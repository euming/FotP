using System.Text;
using System.Text.Json;
using CardBinder.Core;
using MemoryGraph.Abstractions;
using MemoryGraph.Application;
using MemoryGraph.Infrastructure.AMS;

namespace MemoryCtl;

internal sealed class CommandRuntime
{
    public required MemoryDb Db { get; init; }
    public required IMemoryGraphStore GraphStore { get; init; }
    public required Dictionary<Guid, MemoryCardPayload> Payloads { get; init; }
    public required IngestService IngestService { get; init; }
    public required RetrievalService RetrievalService { get; init; }
}

internal interface ICommandRuntimeFactory
{
    CommandRuntime Load(string dbPath);
    void SyncPayloadsToDb(CommandRuntime runtime);
    AMS.Core.AmsStore? LoadAmsStore(string dbPath);
}

/// <summary>
/// Legacy rollback runtime factory.
/// Deprecated for default operation after AMS cutover; keep for explicit `--backend legacy` rollback during soak.
/// </summary>
internal sealed class LegacyCommandRuntimeFactory : ICommandRuntimeFactory
{
    public CommandRuntime Load(string dbPath)
    {
        var db = MemoryJsonlReader.Load(dbPath);
        var graphStore = new MemoryDbGraphStoreAdapter(db);
        var payloads = db.PayloadByCardId.ToDictionary(
            kvp => kvp.Key,
            kvp => new MemoryCardPayload(kvp.Key, kvp.Value.Title, kvp.Value.Text, kvp.Value.Source, kvp.Value.UpdatedAt));

        return new CommandRuntime
        {
            Db = db,
            GraphStore = graphStore,
            Payloads = payloads,
            IngestService = new IngestService(graphStore),
            RetrievalService = new RetrievalService(new LegacyScoringQueryEngine())
        };
    }

    public void SyncPayloadsToDb(CommandRuntime runtime)
    {
        foreach (var (cardId, payload) in runtime.Payloads)
        {
            runtime.Db.PayloadByCardId[cardId] = new CardPayload(
                cardId,
                payload.Title,
                payload.Text,
                payload.Source,
                payload.UpdatedAt);
        }
    }

    public AMS.Core.AmsStore? LoadAmsStore(string dbPath) => null;
}

internal sealed class AmsCommandRuntimeFactory : ICommandRuntimeFactory
{
    public CommandRuntime Load(string dbPath)
    {
        // During soak: if memory.ams.json exists, load from it (AMS-native).
        // Otherwise fall back to JSONL loading for backward compatibility with existing fixtures/data.
        if (!File.Exists(AmsStateStore.AmsPath(dbPath)))
            return LoadFromJsonl(dbPath);

        var store = AmsStateStore.Load(dbPath);
        var graphStore = new AmsGraphStoreAdapter(store);

        // Restore memAnchor name cache from container metadata.
        foreach (var c in store.Containers.Values.Where(c => c.ContainerKind == "memanchor"))
        {
            if (c.Metadata != null
                && c.Metadata.TryGetValue("name", out var nameEl)
                && nameEl.ValueKind == JsonValueKind.String
                && Guid.TryParse(c.ContainerId["memanchor:".Length..], out var id))
            {
                graphStore.RestoreMemAnchorName(id, nameEl.GetString()!);
            }
        }

        // Derive payloads from session containers by assembling transcripts.
        var payloads = new Dictionary<Guid, MemoryCardPayload>();
        foreach (var c in store.Containers.Values.Where(c => c.ContainerKind == "chat_session"))
        {
            if (!Guid.TryParse(c.ContainerId["chat-session:".Length..], out var sessionId))
                continue;

            var meta = c.Metadata;
            var title  = TryGetMetaString(meta, "title")  ?? c.ContainerId;
            var source = TryGetMetaString(meta, "source");
            var startedAt = TryGetMetaDate(meta, "started_at");

            // Assemble transcript from ordered message objects via IterateForward.
            var sb = new StringBuilder();
            foreach (var ln in store.IterateForward(c.ContainerId))
            {
                if (!store.Objects.TryGetValue(ln.ObjectId, out var obj)) continue;
                var summary = obj.SemanticPayload?.Summary ?? ln.ObjectId;
                sb.AppendLine(summary);
            }

            payloads[sessionId] = new MemoryCardPayload(
                sessionId, title, sb.ToString().Trim(), source, startedAt);
        }

        // Populate Db.PayloadByCardId so commands that read payload titles via Db (e.g. query) work.
        var db = new MemoryDb(new CardBinderCore());
        foreach (var (id, p) in payloads)
            db.PayloadByCardId[id] = new CardPayload(id, p.Title, p.Text, p.Source, p.UpdatedAt);

        return new CommandRuntime
        {
            Db = db,
            GraphStore = graphStore,
            Payloads = payloads,
            IngestService = new IngestService(graphStore),
            RetrievalService = new RetrievalService(new LegacyScoringQueryEngine())
        };
    }

    // No-op: payloads are derived from AMS graph on load, not persisted to JSONL.
    public void SyncPayloadsToDb(CommandRuntime runtime) { }

    public AMS.Core.AmsStore? LoadAmsStore(string dbPath)
    {
        if (File.Exists(AmsStateStore.AmsPath(dbPath)))
            return AmsStateStore.Load(dbPath);
        return null;
    }

    /// <summary>
    /// Legacy JSONL load path, used as fallback when memory.ams.json has not yet been created.
    /// Mirrors the pre-refactor AmsCommandRuntimeFactory behavior.
    /// </summary>
    private static CommandRuntime LoadFromJsonl(string dbPath)
    {
        var db = MemoryJsonlReader.Load(dbPath);
        var graphStore = new AmsGraphStoreAdapter();

        foreach (var card in db.Core.AllCards)
        {
            var state = db.Core.GetState(card) switch
            {
                CardState.Active => MemoryCardState.Active,
                CardState.Tombstoned => MemoryCardState.Tombstoned,
                CardState.Retracted => MemoryCardState.Retracted,
                _ => MemoryCardState.Active
            };

            graphStore.UpsertCard(card.Value, state, db.Core.GetStateReason(card));
        }

        foreach (var memAnchor in db.Core.AllBinders)
        {
            if (!db.Core.TryGetBinderName(memAnchor, out var name) || string.IsNullOrWhiteSpace(name))
                continue;

            graphStore.UpsertMemAnchor(memAnchor.Value, name);
        }

        foreach (var card in db.Core.AllCards)
        {
            foreach (var memAnchor in db.Core.BindersOf(card))
            {
                MemoryLinkMeta? meta = null;
                if (db.Core.TryGetLinkMeta(card, memAnchor, out var linkMeta))
                    meta = new MemoryLinkMeta(linkMeta.Relevance, linkMeta.Reason, linkMeta.AddedBy, linkMeta.CreatedAt);

                graphStore.Link(card.Value, memAnchor.Value, meta);
            }
        }

        var payloads = db.PayloadByCardId.ToDictionary(
            kvp => kvp.Key,
            kvp => new MemoryCardPayload(kvp.Key, kvp.Value.Title, kvp.Value.Text, kvp.Value.Source, kvp.Value.UpdatedAt));

        return new CommandRuntime
        {
            Db = db,
            GraphStore = graphStore,
            Payloads = payloads,
            IngestService = new IngestService(graphStore),
            RetrievalService = new RetrievalService(new LegacyScoringQueryEngine())
        };
    }

    private static string? TryGetMetaString(Dictionary<string, JsonElement>? meta, string key)
    {
        if (meta == null || !meta.TryGetValue(key, out var el)) return null;
        return el.ValueKind == JsonValueKind.String ? el.GetString() : null;
    }

    private static DateTimeOffset? TryGetMetaDate(Dictionary<string, JsonElement>? meta, string key)
    {
        if (meta == null || !meta.TryGetValue(key, out var el)) return null;
        return el.TryGetDateTimeOffset(out var dt) ? dt : null;
    }
}

/// <summary>
/// Legacy CardBinder-backed store adapter used only by explicit legacy backend rollback path.
/// Keep isolated from default AMS flow to minimize cutover risk during soak.
/// </summary>
internal sealed class MemoryDbGraphStoreAdapter : IMemoryGraphStore
{
    private readonly MemoryDb _db;

    public MemoryDbGraphStoreAdapter(MemoryDb db)
    {
        _db = db ?? throw new ArgumentNullException(nameof(db));
    }

    public bool CardExists(Guid cardId) => _db.Core.CardExists(new CardId(cardId));
    public bool MemAnchorExists(Guid memAnchorId) => _db.Core.BinderExists(new MemAnchorId(memAnchorId));

    public void UpsertCard(Guid cardId, MemoryCardState state = MemoryCardState.Active, string? stateReason = null)
        => _db.Core.UpsertCard(new CardId(cardId), ToLegacyState(state), stateReason);

    public void UpsertMemAnchor(Guid memAnchorId, string name)
        => _db.Core.UpsertBinder(new MemAnchorId(memAnchorId), name);

    public void Link(Guid cardId, Guid memAnchorId, MemoryLinkMeta? meta = null)
    {
        _db.Core.Link(new CardId(cardId), new MemAnchorId(memAnchorId), new TagLinkMeta(
            Relevance: Math.Clamp(meta?.Relevance ?? 0.5f, 0f, 1f),
            Reason: meta?.Reason,
            AddedBy: meta?.AddedBy,
            CreatedAt: meta?.CreatedAt ?? DateTimeOffset.UtcNow));
    }

    public void Unlink(Guid cardId, Guid memAnchorId) => _db.Core.Unlink(new CardId(cardId), new MemAnchorId(memAnchorId));
    public int UnlinkAllFromCard(Guid cardId) => _db.Core.UnlinkAllFromCard(new CardId(cardId));
    public int UnlinkAllFromMemAnchor(Guid memAnchorId) => _db.Core.UnlinkAllFromBinder(new MemAnchorId(memAnchorId));

    public MemoryCardState GetState(Guid cardId) => _db.Core.GetState(new CardId(cardId)) switch
    {
        CardState.Active => MemoryCardState.Active,
        CardState.Tombstoned => MemoryCardState.Tombstoned,
        CardState.Retracted => MemoryCardState.Retracted,
        _ => MemoryCardState.Active
    };

    public void SetState(Guid cardId, MemoryCardState state, string? reason = null)
        => _db.Core.SetState(new CardId(cardId), ToLegacyState(state), reason);

    public string? GetStateReason(Guid cardId) => _db.Core.GetStateReason(new CardId(cardId));

    public IReadOnlyList<Guid> BindersOf(Guid cardId) => _db.Core.BindersOf(new CardId(cardId)).Select(x => x.Value).ToArray();
    public IReadOnlyList<Guid> CardsIn(Guid memAnchorId) => _db.Core.CardsIn(new MemAnchorId(memAnchorId)).Select(x => x.Value).ToArray();

    public bool TryGetLinkMeta(Guid cardId, Guid memAnchorId, out MemoryLinkMeta meta)
    {
        if (_db.Core.TryGetLinkMeta(new CardId(cardId), new MemAnchorId(memAnchorId), out var m))
        {
            meta = new MemoryLinkMeta(m.Relevance, m.Reason, m.AddedBy, m.CreatedAt);
            return true;
        }

        meta = new MemoryLinkMeta();
        return false;
    }

    public bool TryGetMemAnchorName(Guid memAnchorId, out string name) => _db.Core.TryGetBinderName(new MemAnchorId(memAnchorId), out name!);

    public IReadOnlyCollection<Guid> AllCards => _db.Core.AllCards.Select(x => x.Value).ToArray();
    public IReadOnlyCollection<Guid> AllMemAnchors => _db.Core.AllBinders.Select(x => x.Value).ToArray();

    private static CardState ToLegacyState(MemoryCardState state) => state switch
    {
        MemoryCardState.Active => CardState.Active,
        MemoryCardState.Tombstoned => CardState.Tombstoned,
        MemoryCardState.Retracted => CardState.Retracted,
        _ => CardState.Active
    };
}

internal sealed class LegacyScoringQueryEngine : IMemoryQueryEngine
{
    public IReadOnlyList<MemoryQueryHit> Query(string query, IMemoryGraphStore graphStore, IReadOnlyDictionary<Guid, MemoryCardPayload> payloadByCardId, int top = 10)
    {
        var tokens = Scoring.Tokenize(query);
        var scored = new List<MemoryQueryHit>();

        foreach (var cardId in graphStore.AllCards)
        {
            if (graphStore.GetState(cardId) == MemoryCardState.Retracted)
                continue;

            double textScore = 0;
            if (payloadByCardId.TryGetValue(cardId, out var payload))
            {
                var hay = ((payload.Title ?? string.Empty) + "\n" + (payload.Text ?? string.Empty)).ToLowerInvariant();
                foreach (var token in tokens)
                {
                    if (hay.Contains(token, StringComparison.Ordinal))
                        textScore += 1.0;
                }
            }

            double memAnchorScore = 0;
            foreach (var memAnchorId in graphStore.BindersOf(cardId))
            {
                if (!graphStore.TryGetMemAnchorName(memAnchorId, out var name) || string.IsNullOrEmpty(name))
                    continue;

                var lower = name.ToLowerInvariant();
                foreach (var token in tokens)
                {
                    if (lower.Contains(token, StringComparison.Ordinal))
                        memAnchorScore += 0.25;
                }
            }

            double metaScore = 0;
            foreach (var memAnchorId in graphStore.BindersOf(cardId))
            {
                if (graphStore.TryGetLinkMeta(cardId, memAnchorId, out var meta))
                    metaScore += Math.Clamp(meta.Relevance, 0f, 1f) * 0.5;
            }

            var total = textScore + memAnchorScore + metaScore;
            if (total <= 0)
                continue;

            scored.Add(new MemoryQueryHit(cardId, total, textScore, memAnchorScore, metaScore));
        }

        return scored
            .OrderByDescending(x => x.TotalScore)
            .ThenByDescending(x => x.TextScore)
            .ThenBy(x => x.CardId)
            .Take(top)
            .ToList();
    }
}
