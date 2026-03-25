using MemoryGraph.Abstractions;
using MemoryGraph.Infrastructure.AMS;

namespace MemoryCtl;

internal static class ChatIngestor
{
    public sealed record IngestResult(
        ChatCursor NewCursor,
        IReadOnlyList<Guid> CreatedCardIds,
        int EventsRead,
        int EventsIngested,
        int LinesMalformed);

    public static IngestResult Ingest(
        string dbPath,
        string chatlogPath,
        string cursorPath,
        int maxEvents,
        int gapMinutes,
        bool applyMaintenance,
        ICommandRuntimeFactory runtimeFactory,
        string? rawLlmPath = null)
    {
        ArgumentNullException.ThrowIfNull(runtimeFactory);

        var cursor = CursorStore.Load(cursorPath);

        var events = new List<(int lineNo, ChatEvent evt)>();
        int totalRead = 0;
        int malformed = 0;

        foreach (var pair in ChatLog.ReadEvents(chatlogPath, onMalformedLine: (lineNo, preview, ex) =>
        {
            malformed++;
            Console.Error.WriteLine($"WARN chatlog malformed line {lineNo}: {ex.GetType().Name}: {ex.Message}");
            Console.Error.WriteLine($"WARN chatlog malformed preview: {preview}");
        }))
        {
            totalRead++;
            if (pair.lineNo <= cursor.LastLineNumber) continue;
            events.Add(pair);
            if (events.Count >= maxEvents) break;
        }

        if (events.Count == 0)
            return new IngestResult(cursor, Array.Empty<Guid>(), totalRead, 0, malformed);

        // Chunk by explicit chat boundaries first, then time gaps within a chat.
        // Combined corpora can contain many channels/chat_ids in one raw file, so
        // a pure gap-based split will incorrectly merge unrelated sessions that
        // happen to be near each other in time.
        var chunks = new List<List<(int lineNo, ChatEvent evt)>>();
        List<(int lineNo, ChatEvent evt)> cur = new();
        DateTimeOffset? lastTs = null;
        string? lastChannel = null;
        string? lastChatId = null;

        foreach (var e in events)
        {
            var crossedChatBoundary = cur.Count > 0
                && (!string.Equals(e.evt.Channel, lastChannel, StringComparison.Ordinal)
                    || !string.Equals(e.evt.ChatId, lastChatId, StringComparison.Ordinal));

            var crossedGapBoundary = cur.Count > 0
                && lastTs != null
                && (e.evt.Ts - lastTs.Value).TotalMinutes >= gapMinutes;

            if (crossedChatBoundary || crossedGapBoundary)
            {
                chunks.Add(cur);
                cur = new();
            }
            cur.Add(e);
            lastTs = e.evt.Ts;
            lastChannel = e.evt.Channel;
            lastChatId = e.evt.ChatId;
        }
        if (cur.Count > 0) chunks.Add(cur);

        // Load rawLlm completions if provided, grouped by (channel, chatId).
        // No cursor tracking — completions are matched into sessions by timestamp window.
        var llmByChat = new Dictionary<(string Channel, string ChatId), List<LlmCompletion>>();
        if (rawLlmPath != null && File.Exists(rawLlmPath))
        {
            foreach (var (_, c) in RawLlmLog.ReadCompletions(rawLlmPath,
                onMalformedLine: (lineNo, preview, ex) =>
                    Console.Error.WriteLine($"WARN rawllm malformed line {lineNo}: {ex.GetType().Name}: {ex.Message}")))
            {
                if (LooksLikeAutomation(c.Text)) continue;
                var chatKey = (c.Channel, c.ChatId);
                if (!llmByChat.TryGetValue(chatKey, out var list))
                    llmByChat[chatKey] = list = new();
                list.Add(c);
            }
        }

        // Load tool events from the chatlog, grouped by (channel, chatId).
        var toolEventsByChat = new Dictionary<(string Channel, string ChatId), List<ToolCallEvent>>();
        foreach (var (_, toolEvt) in ChatLog.ReadToolEvents(chatlogPath,
            onMalformedLine: (lineNo, preview, ex) =>
                Console.Error.WriteLine($"WARN chatlog tool_event malformed line {lineNo}: {ex.GetType().Name}: {ex.Message}")))
        {
            var key = (toolEvt.Channel, toolEvt.ChatId);
            if (!toolEventsByChat.TryGetValue(key, out var list))
                toolEventsByChat[key] = list = new();
            list.Add(toolEvt);
        }

        // Load AMS store and get the AmsGraphStoreAdapter.
        var runtime = runtimeFactory.Load(dbPath);
        var amsStore = runtime.GraphStore as AmsGraphStoreAdapter
            ?? throw new InvalidOperationException(
                "ChatIngestor requires an AmsGraphStoreAdapter. Use AmsCommandRuntimeFactory.");

        var created = new List<Guid>();
        var now = DateTimeOffset.UtcNow;
        var gapSpan = TimeSpan.FromMinutes(gapMinutes);

        foreach (var chunk in chunks)
        {
            var first = chunk.First().evt;
            var last  = chunk.Last().evt;

            // Build merged event list: rawUser events + rawLlm completions within this session's time window.
            var merged = chunk
                .Select(p => (Ts: p.evt.Ts, MsgId: p.evt.MessageId, Dir: p.evt.Direction,
                              Author: p.evt.Author, Text: p.evt.Text,
                              Channel: p.evt.Channel, ChatId: p.evt.ChatId))
                .ToList();

            if (llmByChat.TryGetValue((first.Channel, first.ChatId), out var chatLlm))
            {
                foreach (var c in chatLlm)
                {
                    if (c.Ts >= first.Ts - gapSpan && c.Ts <= last.Ts + gapSpan)
                        merged.Add((c.Ts, c.CompletionMessageId, "out", "Rocky",
                                    c.Text, c.Channel, c.ChatId));
                }
            }

            // Sort by timestamp; user before assistant at equal timestamps.
            var ordered = merged
                .OrderBy(e => e.Ts)
                .ThenBy(e => string.Equals(e.Dir, "out", StringComparison.OrdinalIgnoreCase) ? 1 : 0)
                .ToList();

            // Stable dedupe key based on rawUser line range (stable as long as chatlog is append-only).
            var key = $"{first.Channel}:{first.ChatId}:lines:{chunk.First().lineNo}-{chunk.Last().lineNo}";
            var sessionId = GuidUtil.FromKey("session:" + key);

            var sessionLabel = !string.IsNullOrWhiteSpace(first.Slug) ? first.Slug : first.ChatId;
            var firstUserText = ordered
                .FirstOrDefault(e => string.Equals(e.Dir, "in", StringComparison.OrdinalIgnoreCase)).Text;
            var snippet = string.IsNullOrWhiteSpace(firstUserText) ? "" :
                " | " + (firstUserText.Length > 60 ? firstUserText[..60].TrimEnd() + "…" : firstUserText.Trim());
            var title = $"{sessionLabel}{snippet} ({first.Ts:yyyy-MM-dd}, {ordered.Count} msgs)";

            var participants = ordered
                .GroupBy(e => (Author: e.Author ?? e.Dir, Direction: e.Dir))
                .Select(g => (g.Key.Author, g.Key.Direction))
                .ToList();

            // Accumulate token totals from the rawUser events in this chunk.
            int totalIn           = chunk.Sum(p => p.evt.TokensIn);
            int totalOut          = chunk.Sum(p => p.evt.TokensOut);
            int totalCacheRead    = chunk.Sum(p => p.evt.TokensCacheRead);
            int totalCacheCreate  = chunk.Sum(p => p.evt.TokensCacheCreate);

            amsStore.UpsertChatSession(sessionId, first.Channel, first.ChatId, title,
                ordered[0].Ts, ordered[^1].Ts, participants,
                totalIn, totalOut, totalCacheRead, totalCacheCreate);

            int seq = 0;
            foreach (var ev in ordered)
            {
                // Per-message token counts only available on rawUser events (matched by MsgId).
                var rawEvt = chunk.FirstOrDefault(p => p.evt.MessageId == ev.MsgId).evt;
                int msgIn           = rawEvt?.TokensIn          ?? 0;
                int msgOut          = rawEvt?.TokensOut         ?? 0;
                int msgCacheRead    = rawEvt?.TokensCacheRead   ?? 0;
                int msgCacheCreate  = rawEvt?.TokensCacheCreate ?? 0;
                var msgObjId = GuidUtil.FromKey($"msg:{ev.Channel}:{ev.ChatId}:{ev.MsgId}");
                amsStore.AttachChatMessage(msgObjId, sessionId,
                    ev.MsgId, ev.Ts, ev.Dir, ev.Author, ev.Text, ev.Channel, seq++,
                    msgIn, msgOut, msgCacheRead, msgCacheCreate);
            }

            // Attach tool-call objects to this session.
            if (toolEventsByChat.TryGetValue((first.Channel, first.ChatId), out var sessionToolEvents))
            {
                foreach (var toolEvt in sessionToolEvents)
                {
                    // Only attach tool events within this chunk's time window.
                    if (toolEvt.Ts >= first.Ts - gapSpan && toolEvt.Ts <= last.Ts + gapSpan)
                    {
                        amsStore.AttachToolCall(sessionId, toolEvt.ToolUseId, toolEvt.ToolName,
                            toolEvt.Ts, toolEvt.Channel, toolEvt.ChatId,
                            toolEvt.InputJson, toolEvt.ResultPreview, toolEvt.IsError);
                    }
                }
            }

            // Link session to memAnchors.
            foreach (var (memAnchorId, memAnchorName) in ResolveMemAnchors(first, first.Ts))
            {
                amsStore.UpsertMemAnchorContainer(memAnchorId, memAnchorName);
                amsStore.LinkSessionToMemAnchor(sessionId, memAnchorId,
                    new MemoryLinkMeta(Relevance: 0.8f, Reason: "ingest-chatlog", CreatedAt: now));
            }

            created.Add(sessionId);
        }

        // Persist updated AmsStore to memory.ams.json.
        AmsStateStore.Save(dbPath, amsStore.InnerStore);

        var newCursor = new ChatCursor(
            LastLineNumber: chunks.Last().Last().lineNo,
            LastTs: chunks.Last().Last().evt.Ts,
            LastMessageId: chunks.Last().Last().evt.MessageId);

        CursorStore.Save(cursorPath, newCursor);

        return new IngestResult(newCursor, created, totalRead, events.Count, malformed);
    }

    private static bool LooksLikeAutomation(string text)
    {
        if (string.IsNullOrWhiteSpace(text)) return false;
        var t = text.TrimStart();
        if (t.StartsWith("Ingest chat_", StringComparison.OrdinalIgnoreCase)) return true;
        if (t.StartsWith("Derived transcript updated", StringComparison.OrdinalIgnoreCase)) return true;
        if (t.StartsWith("Telegram ingest job", StringComparison.OrdinalIgnoreCase)) return true;
        return false;
    }

    /// <summary>
    /// Returns deterministic (memAnchorId, memAnchorName) pairs for a chat event.
    /// IDs are stable across runs via GuidUtil.FromKey.
    /// For claude-code channels, produces a 4-level hierarchy:
    ///   Conversations → Project: X → Day: yyyy-MM-dd [X] → Session: slug-or-uuid
    /// For all other channels, preserves original 2-level behavior.
    /// </summary>
    private static IReadOnlyList<(Guid memAnchorId, string memAnchorName)> ResolveMemAnchors(
        ChatEvent first, DateTimeOffset sessionDate)
    {
        var names = new List<string> { "Conversations" };
        var source = ParseSourceFromChannel(first.Channel);
        var project = ParseProjectFromChannel(first.Channel) ?? "unknown";
        var day = sessionDate.ToString("yyyy-MM-dd");
        var sessionLabel = !string.IsNullOrWhiteSpace(first.Slug)
            ? first.Slug
            : (first.ChatId.Length >= 8 ? first.ChatId[..8] : first.ChatId);

        // Source-first SmartList hierarchy.
        names.Add($"Source: {source}");
        names.Add($"Source: {source} | Project: {project}");
        names.Add($"Source: {source} | Project: {project} | Day: {day}");
        names.Add($"Source: {source} | Project: {project} | Day: {day} | Session: {sessionLabel}");

        // Backward-compatible project/day/session anchors.
        names.Add($"Project: {project}");
        names.Add($"Day: {day} [{project}]");
        names.Add($"Session: {sessionLabel}");

        // Agent-only SmartList foundation (phase 1 producer-side only).
        names.Add("Agent Memory");
        names.Add("Agent Memory | Shared");
        names.Add($"Agent Memory | Source: {source}");
        names.Add($"Agent Memory | Source: {source} | Project: {project}");

        var uniqueNames = names
            .Where(n => !string.IsNullOrWhiteSpace(n))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        return uniqueNames.Select(n => (GuidUtil.FromKey("memanchor:" + n), n)).ToArray();
    }

    private static string ParseSourceFromChannel(string channel)
    {
        if (string.IsNullOrWhiteSpace(channel))
            return "other";

        if (channel.StartsWith("claude-code", StringComparison.OrdinalIgnoreCase))
            return "claude";
        if (channel.StartsWith("codex", StringComparison.OrdinalIgnoreCase))
            return "codex";

        var idx = channel.IndexOf('/', StringComparison.Ordinal);
        var head = idx <= 0 ? channel : channel[..idx];
        head = head.Trim();
        return string.IsNullOrWhiteSpace(head) ? "other" : head.ToLowerInvariant();
    }

    private static string? ParseProjectFromChannel(string channel)
    {
        // "<source>/<project>" -> "<project>"; "<source>" -> null
        var idx = channel.IndexOf('/', StringComparison.Ordinal);
        if (idx < 0 || idx == channel.Length - 1)
            return null;
        return channel[(idx + 1)..];
    }
}
