using System.Text.Json;
using AMS.Core;

namespace MemoryCtl.Inspection;

internal sealed record SessionClusterProjection(
    IReadOnlyList<GraphInspectionSession> ClusterSessions,
    IReadOnlyDictionary<string, string> SessionToCanonical,
    bool IndexDriftDetected,
    int ChatSessionContainers,
    int ChatSessionsIndexEntries
);

internal sealed class SessionClusterProjector
{
    private static readonly string[] ContinuationClusterIndexes =
    [
        "continuation-clusters:user",
        "continuation-clusters:memory"
    ];

    private readonly AmsStore _store;

    public SessionClusterProjector(AmsStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
    }

    public SessionClusterProjection Project(Dictionary<string, GraphInspectionSession> sessionItemById)
    {
        ArgumentNullException.ThrowIfNull(sessionItemById);

        var sessionContainers = _store.Containers.Values
            .Where(c => c.ContainerKind == "chat_session")
            .OrderBy(c => ReadStr(c.Metadata, "started_at"), StringComparer.Ordinal)
            .ToList();

        int chatSessionContainers = sessionContainers.Count;
        int chatSessionsIndexEntries = CountChatSessionIndexEntries();
        bool indexDriftDetected = chatSessionContainers != chatSessionsIndexEntries;

        var continuationMetaBySession = BuildContinuationMetaBySession();
        var threadMetaBySession = BuildThreadMetaBySession();
        var semanticMetaBySession = new Dictionary<string, ThreadMeta>(threadMetaBySession, StringComparer.Ordinal);
        foreach (var (sessionId, meta) in continuationMetaBySession)
            semanticMetaBySession[sessionId] = meta;

        var inferredThreadMetaByFallback = BuildInferredThreadMetaByFallbackKey(sessionContainers, semanticMetaBySession);

        var sources = sessionContainers
            .Where(c => sessionItemById.ContainsKey(c.ContainerId))
            .Select(c => BuildSource(c, semanticMetaBySession, inferredThreadMetaByFallback))
            .ToList();

        // Timeline is calendar-first: split by day first, then semantic group within day.
        // This prevents a cross-day semantic continuation cluster from collapsing
        // all sessions into the first day/month/year bucket.
        var grouped = sources
            .GroupBy(BuildTimelineBucketKey, StringComparer.Ordinal)
            .OrderBy(g => g.Min(x => x.StartedAt))
            .ToList();

        var clustered = new List<GraphInspectionSession>();
        var sessionToCanonical = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var group in grouped)
        {
            var memberSessions = group.Select(g => sessionItemById[g.SessionId]).ToList();
            if (memberSessions.Count == 0)
                continue;

            var canonical = memberSessions
                .OrderByDescending(s => s.Messages.Count)
                .ThenByDescending(s => ParseDateLabel(s.DateLabel))
                .First();

            var earliest = group.Min(g => g.StartedAt);
            var threadMeta = group.Select(g => g.ThreadMeta).FirstOrDefault(m => m is not null);
            var semanticGroupKey = group.Select(g => g.GroupKey).FirstOrDefault() ?? string.Empty;
            var isContinuationGroup = semanticGroupKey.StartsWith("continuation-cluster:", StringComparison.Ordinal);

            // Continuation clusters can span many days/months. For timeline rendering we keep
            // the semantic group key, but use local canonical/session-derived titles so each
            // day bucket reflects that day's conversation intent.
            var preferredEnrichedTitle = isContinuationGroup ? null : threadMeta?.EnrichedTitle;
            var preferredThreadTitle = isContinuationGroup ? null : (threadMeta?.BootstrapTitle ?? threadMeta?.Slug);

            var fallbackText = string.Join(' ', canonical.Messages.Select(m => m.Text).Where(t => !string.IsNullOrWhiteSpace(t)).Take(3));
            var (title, titleQuality) = InspectionTitlePolicy.ResolveSessionTitle(
                preferredEnrichedTitle,
                preferredThreadTitle,
                canonical.Title,
                fallbackText,
                earliest,
                memberSessions.Count);

            var clusterSession = canonical with
            {
                Title = title,
                TitleQuality = titleQuality,
                GroupKey = semanticGroupKey,
                DateLabel = earliest == DateTimeOffset.MinValue
                    ? canonical.DateLabel
                    : " · " + earliest.ToString("yyyy-MM-dd HH:mm"),
                SessionCount = memberSessions.Count,
                TokensIn = memberSessions.Sum(s => s.TokensIn),
                TokensOut = memberSessions.Sum(s => s.TokensOut),
                TokensCacheRead = memberSessions.Sum(s => s.TokensCacheRead),
                TokensCacheCreate = memberSessions.Sum(s => s.TokensCacheCreate),
                NodeRole = "semantic"
            };

            clustered.Add(clusterSession);

            foreach (var member in group)
                sessionToCanonical[member.SessionId] = clusterSession.Id;
        }

        return new SessionClusterProjection(
            clustered,
            sessionToCanonical,
            indexDriftDetected,
            chatSessionContainers,
            chatSessionsIndexEntries);
    }

    private SourceSession BuildSource(
        ContainerRecord container,
        IReadOnlyDictionary<string, ThreadMeta> threadMetaBySession,
        IReadOnlyDictionary<string, ThreadMeta> inferredThreadMetaByFallback)
    {
        var sessionId = container.ContainerId;
        var chatId = ReadStr(container.Metadata, "chat_id");
        var startedAt = ParseStartedAt(ReadStr(container.Metadata, "started_at"));
        var normalizedStart = NormalizeMinute(startedAt);
        var fallbackGroupKey = BuildFallbackGroupKey(sessionId, chatId, normalizedStart);

        threadMetaBySession.TryGetValue(sessionId, out var threadMeta);
        if (threadMeta is null && inferredThreadMetaByFallback.TryGetValue(fallbackGroupKey, out var inferredMeta))
            threadMeta = inferredMeta;

        string groupKey;
        if (!string.IsNullOrWhiteSpace(threadMeta?.SemanticGroupKey))
        {
            groupKey = threadMeta.SemanticGroupKey!;
        }
        else
        {
            groupKey = fallbackGroupKey;
        }

        return new SourceSession(sessionId, startedAt, groupKey, threadMeta);
    }

    private IReadOnlyDictionary<string, ThreadMeta> BuildInferredThreadMetaByFallbackKey(
        IReadOnlyList<ContainerRecord> sessionContainers,
        IReadOnlyDictionary<string, ThreadMeta> threadMetaBySession)
    {
        var byFallback = new Dictionary<string, Dictionary<string, ThreadMeta>>(StringComparer.Ordinal);

        foreach (var container in sessionContainers)
        {
            var sessionId = container.ContainerId;
            if (!threadMetaBySession.TryGetValue(sessionId, out var meta) || string.IsNullOrWhiteSpace(meta.SemanticGroupKey))
                continue;

            var chatId = ReadStr(container.Metadata, "chat_id");
            var startedAt = ParseStartedAt(ReadStr(container.Metadata, "started_at"));
            var normalizedStart = NormalizeMinute(startedAt);
            var fallbackKey = BuildFallbackGroupKey(sessionId, chatId, normalizedStart);

            if (!byFallback.TryGetValue(fallbackKey, out var semanticByKey))
            {
                semanticByKey = new Dictionary<string, ThreadMeta>(StringComparer.Ordinal);
                byFallback[fallbackKey] = semanticByKey;
            }

            semanticByKey[meta.SemanticGroupKey!] = meta;
        }

        var inferred = new Dictionary<string, ThreadMeta>(StringComparer.Ordinal);
        foreach (var (fallbackKey, semanticByKey) in byFallback)
        {
            if (semanticByKey.Count == 1)
                inferred[fallbackKey] = semanticByKey.Values.First();
        }

        return inferred;
    }

    private Dictionary<string, ThreadMeta> BuildThreadMetaBySession()
    {
        var result = new Dictionary<string, ThreadMeta>(StringComparer.Ordinal);

        foreach (var thread in _store.Containers.Values.Where(c => c.ContainerKind == "conversation_thread"))
        {
            var semanticGroupKey = !string.IsNullOrWhiteSpace(thread.ContainerId)
                ? thread.ContainerId
                : ReadStr(thread.Metadata, "group_key");

            var meta = new ThreadMeta(
                semanticGroupKey,
                ReadTrustedThreadTitle(thread.Metadata),
                ReadStr(thread.Metadata, "bootstrap_title"),
                ReadStr(thread.Metadata, "slug"));

            var canonicalId = NormalizeSessionId(ReadStr(thread.Metadata, "canonical_session_id"));
            if (!string.IsNullOrWhiteSpace(canonicalId))
                result[canonicalId] = meta;

            foreach (var memberLn in _store.IterateForward(thread.ContainerId))
            {
                _store.Objects.TryGetValue(memberLn.ObjectId, out var memberRef);
                var sessionId = ResolveSessionId(memberLn.ObjectId, memberRef);

                if (!string.IsNullOrWhiteSpace(sessionId))
                    result[sessionId] = meta;
            }
        }

        return result;
    }

    private Dictionary<string, ThreadMeta> BuildContinuationMetaBySession()
    {
        var result = new Dictionary<string, ThreadMeta>(StringComparer.Ordinal);
        var seenClusters = new HashSet<string>(StringComparer.Ordinal);

        foreach (var indexId in ContinuationClusterIndexes)
        {
            if (!_store.Containers.ContainsKey(indexId))
                continue;

            foreach (var refLink in _store.IterateForward(indexId))
            {
                if (!_store.Objects.TryGetValue(refLink.ObjectId, out var refObj))
                    continue;

                var clusterId = ResolveClusterId(refLink.ObjectId, refObj);
                if (string.IsNullOrWhiteSpace(clusterId) || !seenClusters.Add(clusterId))
                    continue;

                if (!_store.Containers.TryGetValue(clusterId, out var cluster))
                    continue;

                var label = ReadStr(cluster.Metadata, "label");
                var meta = new ThreadMeta(
                    cluster.ContainerId,
                    label,
                    label,
                    ReadStr(cluster.Metadata, "cluster_key"));

                var canonicalId = NormalizeSessionId(ReadStr(cluster.Metadata, "canonical_session_id"));
                if (!string.IsNullOrWhiteSpace(canonicalId))
                    result[canonicalId] = meta;

                foreach (var memberLn in _store.IterateForward(cluster.ContainerId))
                {
                    _store.Objects.TryGetValue(memberLn.ObjectId, out var memberRef);
                    var sessionId = ResolveSessionId(memberLn.ObjectId, memberRef);
                    if (!string.IsNullOrWhiteSpace(sessionId))
                        result[sessionId] = meta;
                }
            }
        }

        return result;
    }

    private int CountChatSessionIndexEntries()
    {
        if (!_store.Containers.ContainsKey("chat-sessions"))
            return 0;

        var sessions = new HashSet<string>(StringComparer.Ordinal);
        foreach (var ln in _store.IterateForward("chat-sessions"))
        {
            if (!_store.Objects.TryGetValue(ln.ObjectId, out var refObj))
                continue;

            var sessionId = refObj.SemanticPayload?.Provenance?.TryGetValue("session_id", out var el) == true
                ? el.GetString() ?? string.Empty
                : string.Empty;

            if (!string.IsNullOrWhiteSpace(sessionId))
                sessions.Add(sessionId);
        }

        return sessions.Count;
    }

    private string ResolveSessionId(string memberObjectId, ObjectRecord? memberRef)
    {
        if (TryResolveSessionIdFromProvenance(memberRef?.SemanticPayload?.Provenance, out var fromProvenance))
            return fromProvenance;

        if (memberObjectId.StartsWith("chat-session:", StringComparison.Ordinal))
            return memberObjectId;

        if (memberObjectId.StartsWith("session-ref:", StringComparison.Ordinal))
            return NormalizeSessionId(memberObjectId["session-ref:".Length..]);

        return string.Empty;
    }

    private static string ResolveClusterId(string objectId, ObjectRecord? refObj)
    {
        if (refObj?.SemanticPayload?.Provenance?.TryGetValue("cluster_id", out var clusterEl) == true)
        {
            var clusterId = clusterEl.GetString() ?? string.Empty;
            if (!string.IsNullOrWhiteSpace(clusterId))
                return clusterId.Trim();
        }

        if (objectId.StartsWith("continuation-cluster-ref:", StringComparison.Ordinal))
            return "continuation-cluster:" + objectId["continuation-cluster-ref:".Length..];

        return string.Empty;
    }

    private static bool TryResolveSessionIdFromProvenance(
        Dictionary<string, JsonElement>? provenance,
        out string sessionId)
    {
        sessionId = string.Empty;
        if (provenance is null)
            return false;

        foreach (var key in new[] { "session_id", "session_container_id", "canonical_session_id" })
        {
            if (!provenance.TryGetValue(key, out var el))
                continue;

            var raw = el.GetString() ?? string.Empty;
            var normalized = NormalizeSessionId(raw);
            if (string.IsNullOrWhiteSpace(normalized))
                continue;

            sessionId = normalized;
            return true;
        }

        return false;
    }

    private static DateTimeOffset NormalizeMinute(DateTimeOffset value)
    {
        if (value == DateTimeOffset.MinValue)
            return value;

        var utc = value.ToUniversalTime();
        return new DateTimeOffset(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, 0, TimeSpan.Zero);
    }

    private static DateTimeOffset ParseDateLabel(string dateLabel)
    {
        var trimmed = dateLabel.TrimStart(' ', '·');
        return DateTimeOffset.TryParse(trimmed, out var dt) ? dt : DateTimeOffset.MinValue;
    }

    private static DateTimeOffset ParseStartedAt(string startedAt)
        => DateTimeOffset.TryParse(startedAt, out var dt) ? dt : DateTimeOffset.MinValue;

    private static string BuildFallbackGroupKey(string sessionId, string chatId, DateTimeOffset normalizedStart)
    {
        var keyChat = string.IsNullOrWhiteSpace(chatId) ? sessionId : chatId.Trim();
        return normalizedStart == DateTimeOffset.MinValue
            ? $"{keyChat}|unknown"
            : $"{keyChat}|{normalizedStart:yyyy-MM-ddTHH:mm}";
    }

    private static string BuildTimelineBucketKey(SourceSession source)
    {
        if (source.StartedAt == DateTimeOffset.MinValue)
            return $"unknown|{source.GroupKey}";

        var localDate = source.StartedAt.Date;
        return $"{localDate:yyyy-MM-dd}|{source.GroupKey}";
    }

    private static string NormalizeSessionId(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return string.Empty;

        var value = raw.Trim();
        if (value.StartsWith("chat-session:", StringComparison.Ordinal))
            return value;

        if (value.Contains(':', StringComparison.Ordinal))
            return value;

        return "chat-session:" + value;
    }

    private static string ReadStr(Dictionary<string, JsonElement>? meta, string key)
    {
        if (meta is null)
            return string.Empty;

        return meta.TryGetValue(key, out var el)
            ? el.GetString() ?? string.Empty
            : string.Empty;
    }

    private static string ReadTrustedThreadTitle(Dictionary<string, JsonElement>? meta)
    {
        var enriched = ReadStr(meta, "enriched_title");
        var validation = ReadStr(meta, "title_validation");
        return string.Equals(validation, "accepted", StringComparison.OrdinalIgnoreCase)
            ? enriched
            : string.Empty;
    }

    private sealed record SourceSession(
        string SessionId,
        DateTimeOffset StartedAt,
        string GroupKey,
        ThreadMeta? ThreadMeta);

    private sealed record ThreadMeta(
        string? SemanticGroupKey,
        string? EnrichedTitle,
        string? BootstrapTitle,
        string? Slug);
}
