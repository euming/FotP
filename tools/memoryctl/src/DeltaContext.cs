using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CardBinder.Core;
using MemoryGraph.Abstractions;

namespace MemoryCtl;

internal static class DeltaContext
{
    public sealed record Options(
        string Channel,
        string ChatId,
        string Query,
        int Top,
        int MaxChars,
        string? TranscriptTailPath,
        int TailMaxChars);

    public static string Build(string dbPath, Options opts)
    {
        var db = MemoryJsonlReader.Load(dbPath);
        var top = SelectCards(db, opts);
        if (top.Count == 0) return "";

        var tailText = LoadTail(opts.TranscriptTailPath, opts.TailMaxChars);

        var lines = new List<string>();
        lines.Add("DELTA CONTEXT (memory overlay)");
        lines.Add($"Chat: {opts.Channel}:{opts.ChatId}");
        lines.Add($"Query: {opts.Query}");
        lines.Add("");

        foreach (var c in top)
        {
            if (!db.TryGetPayload(c, out var payload) || payload == null)
                continue;

            var title = payload.Title ?? c.Value.ToString();
            var text = payload.Text ?? "";
            var snippet = Snip(text, 240);

            // Basic redundancy filter: if title/snippet already appears in the recent transcript tail, skip.
            if (!string.IsNullOrWhiteSpace(tailText))
            {
                if (tailText.IndexOf(title, StringComparison.OrdinalIgnoreCase) >= 0)
                    continue;
                if (!string.IsNullOrWhiteSpace(snippet) && tailText.IndexOf(Snip(snippet, 80), StringComparison.OrdinalIgnoreCase) >= 0)
                    continue;
            }

            var item = $"- {title}";
            if (!string.IsNullOrWhiteSpace(snippet))
                item += $"\n  {snippet.Replace("\n", " ").Trim()}";
            lines.Add(item);
            lines.Add("");

            if (lines.Sum(x => x.Length + 1) > opts.MaxChars)
                break;
        }

        if (lines.Count <= 4)
            return "";

        return string.Join("\n", lines).TrimEnd();
    }

    public static IReadOnlyList<CardId> SelectCards(MemoryDb db, Options opts)
    {
        var graphStore = new MemoryDbGraphStoreAdapter(db);
        var payloads = db.PayloadByCardId.ToDictionary(
            kvp => kvp.Key,
            kvp => new MemoryCardPayload(kvp.Key, kvp.Value.Title, kvp.Value.Text, kvp.Value.Source, kvp.Value.UpdatedAt));

        return SelectCards(graphStore, payloads, new LegacyScoringQueryEngine(), opts)
            .Select(id => new CardId(id))
            .ToList();
    }

    internal static IReadOnlyList<Guid> SelectCards(
        IMemoryGraphStore graphStore,
        IReadOnlyDictionary<Guid, MemoryCardPayload> payloadByCardId,
        IMemoryQueryEngine queryEngine,
        Options opts)
    {
        // Candidate set: cards in Overlay memAnchors (optionally also scoped to the chat memAnchor).
        var overlayBinders = new[] { "Overlay: Pinned", "Overlay: Missing Context", "Overlay: Session" };

        var memAnchorIds = new List<Guid>();
        foreach (var memAnchorId in graphStore.AllMemAnchors)
        {
            if (!graphStore.TryGetMemAnchorName(memAnchorId, out var name) || string.IsNullOrWhiteSpace(name))
                continue;
            if (overlayBinders.Any(x => string.Equals(x, name, StringComparison.OrdinalIgnoreCase)))
                memAnchorIds.Add(memAnchorId);
        }

        var candidates = new HashSet<Guid>();
        foreach (var memAnchorId in memAnchorIds)
            foreach (var cardId in graphStore.CardsIn(memAnchorId))
                candidates.Add(cardId);

        // If we have a chat memAnchor, intersect.
        var chatBinderName = $"Chat: {opts.Channel}:{opts.ChatId}";
        Guid? chatMemAnchorId = null;
        foreach (var memAnchorId in graphStore.AllMemAnchors)
        {
            if (graphStore.TryGetMemAnchorName(memAnchorId, out var name) && string.Equals(name, chatBinderName, StringComparison.OrdinalIgnoreCase))
            {
                chatMemAnchorId = memAnchorId;
                break;
            }
        }

        if (chatMemAnchorId is Guid chatId)
        {
            var inChat = new HashSet<Guid>(graphStore.CardsIn(chatId));
            candidates.RemoveWhere(c => !inChat.Contains(c));
        }

        if (candidates.Count == 0)
            return Array.Empty<Guid>();

        var scored = queryEngine.Query(opts.Query, graphStore, payloadByCardId, top: graphStore.AllCards.Count)
            .Where(hit => candidates.Contains(hit.CardId))
            .OrderByDescending(hit => hit.TotalScore)
            .Take(Math.Max(1, opts.Top))
            .Select(hit => hit.CardId)
            .ToList();

        return scored;
    }

    private static string Snip(string s, int max)
    {
        if (string.IsNullOrWhiteSpace(s)) return "";
        var t = s.Trim();
        return t.Length <= max ? t : t.Substring(0, max) + "...";
    }

    private static string LoadTail(string? path, int maxChars)
    {
        if (string.IsNullOrWhiteSpace(path)) return "";
        if (!File.Exists(path)) return "";

        var text = File.ReadAllText(path);
        if (text.Length <= maxChars) return text;
        return text.Substring(text.Length - maxChars);
    }
}
