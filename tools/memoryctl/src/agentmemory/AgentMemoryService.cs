using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using AMS.Core;
using MemoryGraph.Abstractions;
using MemoryGraph.Application;
using MemoryGraph.Infrastructure.AMS;

namespace MemoryCtl;

internal sealed record AgentMaintainResult(int LessonsSynced, int SummariesBuilt, int SourceLinksPruned);

internal sealed record AgentLessonHit(
    string LessonId,
    string Title,
    double Score,
    double Confidence,
    double EvidenceHealth,
    string FreshnessTier,
    string StereotypeFamilyId,
    string StereotypeVersionId,
    IReadOnlyList<AgentMemoryService.EvidenceSnapshot> EvidenceSnapshots,
    AgentHitExplain? Explain);

internal sealed record AgentShortTermHit(
    string SourceKind,
    string SourceRef,
    string SessionRef,
    string SessionTitle,
    string Snippet,
    double Score,
    double Recency,
    IReadOnlyList<string> MatchedTokens,
    DateTimeOffset? Timestamp,
    string Path);

internal sealed record AgentTokenMatchLocations(
    IReadOnlyList<string> NodeSummaryTokens,
    IReadOnlyList<string> LessonSummaryTokens,
    IReadOnlyList<string> EvidenceSnippetTokens);

internal sealed record AgentScoreBreakdown(
    double TokenOverlap,
    double SemanticContribution,
    double FreshnessContribution,
    double EvidenceContribution,
    double DecayDivisor,
    double FinalScore);

internal sealed record AgentHitExplain(
    string RankingSource,
    IReadOnlyList<string> MatchedTokens,
    AgentTokenMatchLocations TokenMatchLocations,
    AgentScoreBreakdown ScoreBreakdown,
    string Path,
    double RouteCost,
    bool RepresentativeMismatch,
    IReadOnlyList<string> RiskFlags,
    string WhyWon);

internal sealed record AgentQueryDiagnostics(
    string ScoringLane,
    string RoutingDecision,
    IReadOnlyList<string> RoutingFlags,
    string ScopeLens = "global");

internal sealed record AgentLineageScope(
    string Level,
    string ObjectId,
    string NodeId,
    string Title,
    string CurrentStep,
    string NextCommand,
    string? BranchOffAnchor,
    IReadOnlyList<string> ArtifactRefs);

internal sealed record AgentQueryContext(
    IReadOnlyList<AgentLineageScope> Lineage,
    string AgentRole,
    string Mode,
    string? FailureBucket,
    IReadOnlyList<string> ActiveArtifacts,
    int TraversalBudget,
    string Source)
{
    public bool HasLineage => Lineage.Count > 0;
    public AgentLineageScope? Self => Lineage.FirstOrDefault(x => string.Equals(x.Level, "self", StringComparison.Ordinal));
    public AgentLineageScope? Parent => Lineage.FirstOrDefault(x => string.Equals(x.Level, "parent", StringComparison.Ordinal));
    public AgentLineageScope? Grandparent => Lineage.FirstOrDefault(x => string.Equals(x.Level, "grandparent", StringComparison.Ordinal));
}

internal sealed record AgentQueryResult(
    IReadOnlyList<AgentLessonHit> Hits,
    IReadOnlyList<AgentShortTermHit> ShortTermHits,
    bool WeakResult,
    IReadOnlyList<ObjectRecord> FallbackSummaries,
    string Markdown,
    int TouchedLessons,
    int FreshnessAdmissions,
    AgentQueryDiagnostics Diagnostics,
    AgentQueryContext? Context = null,
    int RouteMemoryEpisodesProjected = 0);

internal sealed record FreshnessLaneInfo(
    string Path,
    string ObjectId,
    string Status,
    string TopicKey,
    int MemberCount,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    int Index);

internal sealed record FreshnessObjectPosition(
    string LanePath,
    string LaneStatus,
    string TopicKey,
    int LaneIndex,
    string TemperatureLabel);

internal sealed class AgentMemoryService
{
    internal sealed record EvidenceSnapshot(
        string source_kind,
        string source_id,
        string source_ref,
        string snippet,
        string snippet_hash,
        DateTimeOffset captured_at,
        string link_status,
        DateTimeOffset? missing_since);

    private static readonly string[] DreamKinds = ["topic", "thread", "decision", "invariant"];
    private static readonly string[] TierOrder = ["fresh", "1d", "7d", "30d", "90d", "yearly"];
    private static readonly Regex TokenRx = new("[a-z0-9]{3,}", RegexOptions.Compiled | RegexOptions.IgnoreCase);
    private static readonly HashSet<string> TitleStopwords = new(StringComparer.OrdinalIgnoreCase)
    {
        "the", "and", "for", "with", "from", "that", "this", "into", "onto", "when", "were", "been", "have", "has", "had",
        "you", "your", "our", "their", "about", "after", "before", "under", "over", "between", "using", "user", "claude",
        "need", "open", "next", "done", "doing", "always", "never", "just", "then", "still", "more", "also"
    };
    private static readonly HashSet<string> TitleNoise = new(StringComparer.OrdinalIgnoreCase)
    {
        "topic", "thread", "decision", "invariant", "lesson", "memory", "session", "chat", "message", "messages"
    };

    private static readonly IReadOnlyDictionary<string, double> TierKeepRates = new Dictionary<string, double>(StringComparer.Ordinal)
    {
        ["fresh"] = 1.00, ["1d"] = 0.90, ["7d"] = 0.80, ["30d"] = 0.60, ["90d"] = 0.40, ["yearly"] = 0.20
    };

    private static readonly IReadOnlyDictionary<string, double> TierWeights = new Dictionary<string, double>(StringComparer.Ordinal)
    {
        ["fresh"] = 1.00, ["1d"] = 0.90, ["7d"] = 0.75, ["30d"] = 0.55, ["90d"] = 0.35, ["yearly"] = 0.20
    };

    private const string AgentSummaryIndexContainer = "agent-summary-index";
    private const string AgentMemoryRootContainer = "agent-memory";
    private const string AgentMemoryDecayContainer = "agent-memory:decay-ladder";
    private const string AgentMemorySummaryContainer = "agent-memory:summaries";
    private const string AgentMemoryStereotypeContainer = "agent-memory:stereotypes";
    private const string AgentMemorySourceLinksContainer = "agent-memory:sources";
    private const string AgentMemoryLessonsContainer = "agent-memory:lessons";
    private const string AgentMemorySourceGroupsContainer = "agent-memory:source-groups";
    private const string AgentMemorySemanticContainer = "agent-memory:semantic";
    private const string AgentMemoryFreshnessContainer = "agent-memory:freshness";
    private const string AgentMemoryFreshnessStateObjectId = "agent-memory:freshness-state";
    private const string AgentMemorySmartListContainer = SmartListService.DurableRootContainer;
    private const string AgentMemoryShortTermSmartListContainer = SmartListService.ShortTermRootContainer;
    private const string FreshnessSmartListRootPath = "smartlist/agent-memory/freshness";
    private const string FreshnessLaneRootPath = "smartlist/agent-memory/freshness/lanes";
    private const string FreshnessStatusActive = "active";
    private const string FreshnessStatusHistorical = "historical";
    private const string FreshnessStatusFrozen = "frozen";
    private const int FreshnessHeadLaneMax = 11;
    private static readonly HashSet<string> DeepMemoryTokens = new(StringComparer.OrdinalIgnoreCase)
    {
        "deep", "frozen", "archive", "old"
    };
    private readonly AmsStore _store;
    private readonly IRetrievalGraphStore _retrievalGraphStore;
    private readonly RetrievalGraphProjector _retrievalGraph;
    private readonly RouteMemoryService _routeMemory;
    private readonly SmartListService _smartLists;

    public AgentMemoryService(AmsStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _retrievalGraphStore = new AmsGraphStoreAdapter(store);
        _retrievalGraph = new RetrievalGraphProjector(store);
        _routeMemory = new RouteMemoryService(_retrievalGraphStore);
        _smartLists = new SmartListService(store);
    }

    public AgentMaintainResult Maintain(DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        var lessons = SyncLessons(nowUtc);
        VerifySourceLinks(nowUtc);
        RotateFreshness(nowUtc);
        var pruned = DecimateSourceLinks(nowUtc);
        SyncFreshnessLanes(nowUtc);
        var summaries = BuildSummaries(nowUtc);
        SyncAgentMemoryTree();
        _retrievalGraph.ProjectAgentMemory();
        return new AgentMaintainResult(lessons, summaries, pruned);
    }

    public AgentQueryResult Query(string query, int top, DateTimeOffset nowUtc, bool touch = true, AgentQueryContext? context = null, bool projectRouteMemory = false, RouteMemoryBiasOptions? biasOptions = null)
    {
        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentException("query is required", nameof(query));
        if (top < 1)
            throw new ArgumentOutOfRangeException(nameof(top), "top must be >= 1");

        context ??= TryBuildDefaultContext();
        var freshnessAdmissions = EnsureFreshnessReady(nowUtc);
        var includeFrozen = IsDeepMemoryQuery(query);
        var freshnessPositions = BuildFreshnessObjectPositions(includeFrozen);
        var frozenExclusions = includeFrozen ? new HashSet<string>(StringComparer.Ordinal) : BuildFrozenOnlyObjectIds();
        var graphRoutes = BuildContextGraphRoutes(context, biasOptions);
        var ranking = RankLessons(query, top, context, graphRoutes, freshnessPositions, frozenExclusions);
        var hits = ranking.Hits;
        var shortTermHits = SelectShortTermHits(query, top, nowUtc, context, graphRoutes, freshnessPositions, frozenExclusions);
        var lessonWeak = hits.Count(x => x.Score >= 0.45d) < 2;
        var weak = lessonWeak && shortTermHits.Count == 0;
        var fallback = weak ? SelectFallbackSummaries(query) : Array.Empty<ObjectRecord>();
        var touched = touch ? TouchLessons(hits.Select(x => x.LessonId), nowUtc, hits) : 0;
        if (touch)
        {
            freshnessAdmissions += AdmitObjectsToFreshness(
                hits.Select(x => x.LessonId)
                    .Concat(shortTermHits.Select(x => x.SourceRef))
                    .Where(x => !string.IsNullOrWhiteSpace(x)),
                nowUtc,
                "touch",
                BuildTopicKey(BuildFreeTextTokens(query)));
            freshnessPositions = BuildFreshnessObjectPositions(includeFrozen);
        }
        var markdown = RenderMarkdown(query, hits, shortTermHits, weak, fallback, context);
        var projectedEpisodes = projectRouteMemory
            ? ProjectRouteMemory(query, nowUtc, hits, shortTermHits, weak, fallback, ranking.Diagnostics, context)
            : 0;
        return new AgentQueryResult(hits, shortTermHits, weak, fallback, markdown, touched, freshnessAdmissions, ranking.Diagnostics, context, projectedEpisodes);
    }

    private void EnsureScaffold()
    {
        foreach (var tier in TierOrder)
        {
            var id = TierContainer(tier);
            if (!_store.Containers.ContainsKey(id))
                _store.CreateContainer(id, "container", "lesson_freshness");
            _store.Containers[id].Policies.UniqueMembers = true;
        }

        if (!_store.Containers.ContainsKey(AgentSummaryIndexContainer))
        {
            _store.CreateContainer(AgentSummaryIndexContainer, "container", "agent_summary_index");
            _store.Containers[AgentSummaryIndexContainer].Policies.UniqueMembers = true;
        }

        EnsureUniqueContainer(AgentMemoryRootContainer, "agent_memory_root");
        EnsureUniqueContainer(AgentMemoryDecayContainer, "agent_memory_group");
        EnsureUniqueContainer(AgentMemorySummaryContainer, "agent_memory_group");
        EnsureUniqueContainer(AgentMemoryStereotypeContainer, "agent_memory_group");
        EnsureUniqueContainer(AgentMemorySourceLinksContainer, "agent_memory_group");
        EnsureUniqueContainer(AgentMemoryLessonsContainer, "agent_memory_group");
        EnsureUniqueContainer(AgentMemorySourceGroupsContainer, "agent_memory_group");
        EnsureUniqueContainer(AgentMemorySemanticContainer, "agent_memory_group");
        EnsureUniqueContainer(AgentMemoryFreshnessContainer, "agent_memory_group");
        EnsureUniqueContainer(AgentMemorySmartListContainer, "smartlist_root");
        EnsureUniqueContainer(AgentMemoryShortTermSmartListContainer, "smartlist_root");

        ReplaceMembers(AgentMemoryRootContainer, [
            AgentMemoryDecayContainer,
            AgentMemorySummaryContainer,
            AgentMemoryStereotypeContainer,
            AgentMemorySourceLinksContainer,
            AgentMemorySourceGroupsContainer,
            AgentMemorySemanticContainer,
            AgentMemoryFreshnessContainer,
            AgentMemoryLessonsContainer,
            AgentMemorySmartListContainer,
            AgentMemoryShortTermSmartListContainer
        ]);

        ReplaceMembers(AgentMemoryDecayContainer, TierOrder.Select(TierContainer).ToList());
    }

    private int SyncLessons(DateTimeOffset nowUtc)
    {
        var count = 0;
        var dreams = _store.Objects.Values
            .Where(x => DreamKinds.Contains(x.ObjectKind, StringComparer.Ordinal) && !x.ObjectId.Contains("-members:", StringComparison.OrdinalIgnoreCase))
            .OrderBy(x => x.ObjectKind, StringComparer.Ordinal)
            .ThenBy(x => x.ObjectId, StringComparer.Ordinal)
            .ToList();

        foreach (var dream in dreams)
        {
            SyncOneLesson(dream, nowUtc);
            count++;
        }

        return count;
    }

    private void SyncOneLesson(ObjectRecord dream, DateTimeOffset nowUtc)
    {
        var suffix = Suffix(dream.ObjectId);
        var snapshots = BuildSnapshots(dream, nowUtc);
        var (title, titleQuality) = BuildLessonTitle(dream, snapshots);

        var familyId = $"lesson-stereotype-family:{dream.ObjectKind}:{suffix}";
        _store.UpsertObject(familyId, "lesson_stereotype_family");
        var familyProv = EnsureProv(_store.Objects[familyId]);

        var fingerprint = $"{dream.ObjectKind}|{title}|{string.Join("|", snapshots.Select(x => x.source_ref).OrderBy(x => x, StringComparer.Ordinal))}";
        var versionId = $"lesson-stereotype-version:{dream.ObjectKind}:{suffix}:v:{Hash8(fingerprint)}";
        _store.UpsertObject(versionId, "lesson_stereotype_version");
        var versionProv = EnsureProv(_store.Objects[versionId]);
        versionProv["family_id"] = JsonSerializer.SerializeToElement(familyId);
        versionProv["origin_dream_id"] = JsonSerializer.SerializeToElement(dream.ObjectId);

        var familyMembers = $"lesson-stereotype-family-members:{familyId}";
        EnsureUniqueContainer(familyMembers, "lesson_stereotype_family_members");
        if (!_store.HasMembership(familyMembers, versionId)) _store.AddObject(familyMembers, versionId);
        familyProv["current_version_id"] = JsonSerializer.SerializeToElement(versionId);

        var lessonId = $"lesson:{dream.ObjectKind}:{suffix}";
        _store.UpsertObject(lessonId, "lesson");
        var lesson = _store.Objects[lessonId];
        lesson.SemanticPayload ??= new SemanticPayload();
        lesson.SemanticPayload.Summary = title;
        lesson.SemanticPayload.Tags = ["lesson", dream.ObjectKind];
        var lessonProv = EnsureProv(lesson);

        lessonProv["stereotype_family_id"] = JsonSerializer.SerializeToElement(familyId);
        lessonProv["stereotype_version_id"] = JsonSerializer.SerializeToElement(versionId);
        lessonProv["origin_dream_id"] = JsonSerializer.SerializeToElement(dream.ObjectId);
        lessonProv["origin_kind"] = JsonSerializer.SerializeToElement(dream.ObjectKind);
        lessonProv["title_quality"] = JsonSerializer.SerializeToElement(titleQuality);
        lessonProv["evidence_snapshots"] = JsonSerializer.SerializeToElement(snapshots);
        lessonProv["confidence"] = JsonSerializer.SerializeToElement(ConfidenceFromVote(dream));
        lessonProv["evidence_health"] = JsonSerializer.SerializeToElement(EvidenceHealth(snapshots));
        lessonProv["touch_count"] = JsonSerializer.SerializeToElement(ReadInt(lessonProv, "touch_count", 0));
        lessonProv["last_touched_at"] = JsonSerializer.SerializeToElement(ReadDate(lessonProv, "last_touched_at") ?? nowUtc);
        lessonProv["freshness_tier"] = JsonSerializer.SerializeToElement(ReadString(lessonProv, "freshness_tier") ?? "fresh");
        lessonProv["decay_multiplier"] = JsonSerializer.SerializeToElement(ReadDouble(lessonProv, "decay_multiplier", 1.0d));
        lessonProv["source_project_keys"] = JsonSerializer.SerializeToElement(snapshots
            .Select(x => SourceProjectKey(x.source_ref))
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList());

        var memberContainer = $"lesson-stereotype-members:{versionId}";
        EnsureUniqueContainer(memberContainer, "lesson_stereotype_members");
        if (!_store.HasMembership(memberContainer, lessonId)) _store.AddObject(memberContainer, lessonId);
        foreach (var v in _store.IterateForward(familyMembers).Select(x => x.ObjectId).Where(x => x != versionId).ToList())
        {
            var old = $"lesson-stereotype-members:{v}";
            if (_store.TryGetMembership(old, lessonId, out var link))
                _store.RemoveLinkNode(old, link.LinkNodeId);
        }

        SyncSourceLinks(lessonId, snapshots, nowUtc);
    }

    private void SyncSourceLinks(string lessonId, IReadOnlyList<EvidenceSnapshot> snapshots, DateTimeOffset nowUtc)
    {
        var containerId = $"lesson-sources:{lessonId}";
        EnsureUniqueContainer(containerId, "lesson_sources");
        foreach (var link in _store.IterateForward(containerId).ToList())
            _store.RemoveLinkNode(containerId, link.LinkNodeId);

        foreach (var snapshot in snapshots.OrderBy(x => x.source_ref, StringComparer.Ordinal))
        {
            if (string.IsNullOrWhiteSpace(snapshot.source_ref)) continue;
            if (!_store.Objects.ContainsKey(snapshot.source_ref) && !_store.Containers.ContainsKey(snapshot.source_ref))
                continue;
            var created = _store.AddObject(containerId, snapshot.source_ref);
            created.Metadata = new Dictionary<string, JsonElement>(StringComparer.Ordinal)
            {
                ["captured_at"] = JsonSerializer.SerializeToElement(nowUtc),
                ["link_status"] = JsonSerializer.SerializeToElement(snapshot.link_status)
            };
        }
    }

    private void RotateFreshness(DateTimeOffset nowUtc)
    {
        foreach (var lesson in _store.Objects.Values.Where(x => x.ObjectKind == "lesson").ToList())
        {
            var prov = EnsureProv(lesson);
            var lastTouched = ReadDate(prov, "last_touched_at") ?? lesson.UpdatedAt;
            var decay = Math.Max(1.0d, ReadDouble(prov, "decay_multiplier", 1.0d));
            var age = Math.Max(0d, (nowUtc - lastTouched).TotalDays) * decay;
            var tier = age < 1d ? "fresh" : age < 7d ? "1d" : age < 30d ? "7d" : age < 90d ? "30d" : age < 365d ? "90d" : "yearly";
            MoveToTier(lesson.ObjectId, tier);
            prov["freshness_tier"] = JsonSerializer.SerializeToElement(tier);
        }
    }

    private void VerifySourceLinks(DateTimeOffset nowUtc)
    {
        foreach (var lesson in _store.Objects.Values.Where(x => x.ObjectKind == "lesson").ToList())
        {
            var prov = EnsureProv(lesson);
            var snapshots = ReadSnapshots(prov).ToList();
            if (snapshots.Count > 0)
            {
                for (var i = 0; i < snapshots.Count; i++)
                {
                    var snapshot = snapshots[i];
                    var live = IsResolvable(snapshot.source_ref);
                    snapshots[i] = snapshot with
                    {
                        link_status = live ? "live" : "missing",
                        missing_since = live ? null : (snapshot.missing_since ?? nowUtc)
                    };
                }

                prov["evidence_snapshots"] = JsonSerializer.SerializeToElement(snapshots);
                prov["evidence_health"] = JsonSerializer.SerializeToElement(EvidenceHealth(snapshots));
            }

            var sourceContainerId = $"lesson-sources:{lesson.ObjectId}";
            if (!_store.Containers.ContainsKey(sourceContainerId))
                continue;

            foreach (var link in _store.IterateForward(sourceContainerId).ToList())
            {
                var live = IsResolvable(link.ObjectId);
                link.Metadata ??= new Dictionary<string, JsonElement>(StringComparer.Ordinal);
                link.Metadata["captured_at"] = JsonSerializer.SerializeToElement(nowUtc);
                link.Metadata["link_status"] = JsonSerializer.SerializeToElement(live ? "live" : "missing");
            }
        }
    }

    private int DecimateSourceLinks(DateTimeOffset nowUtc)
    {
        var pruned = 0;
        foreach (var tier in TierOrder.Where(x => x != "fresh"))
        {
            var keepRate = TierKeepRates[tier];
            foreach (var member in _store.IterateForward(TierContainer(tier)).ToList())
            {
                var lessonId = member.ObjectId;
                var sourceContainerId = $"lesson-sources:{lessonId}";
                if (!_store.Containers.ContainsKey(sourceContainerId)) continue;
                var links = _store.IterateForward(sourceContainerId).ToList();
                if (links.Count <= 1) continue;
                if (!_store.Objects.TryGetValue(lessonId, out var lesson)) continue;
                var recency = 1d / (1d + Math.Max(0d, (nowUtc - (ReadDate(lesson.SemanticPayload?.Provenance, "last_touched_at") ?? lesson.UpdatedAt)).TotalDays) / 30d);
                var snaps = ReadSnapshots(lesson.SemanticPayload?.Provenance).ToDictionary(x => x.source_ref, StringComparer.Ordinal);
                var ordered = links.Select(x =>
                {
                    var ev = snaps.TryGetValue(x.ObjectId, out var s) && s.link_status == "live" ? 1d : 0.15d;
                    return (x, score: recency + ev);
                }).OrderByDescending(x => x.score).ThenBy(x => x.x.ObjectId, StringComparer.Ordinal).ToList();
                var keep = Math.Max(1, (int)Math.Ceiling(ordered.Count * keepRate));
                foreach (var loser in ordered.Skip(keep))
                {
                    _store.RemoveLinkNode(sourceContainerId, loser.x.LinkNodeId);
                    pruned++;
                }
            }
        }
        return pruned;
    }

    private int BuildSummaries(DateTimeOffset nowUtc)
    {
        var lessons = _store.Objects.Values.Where(x => x.ObjectKind == "lesson").OrderBy(x => x.ObjectId, StringComparer.Ordinal).ToList();
        var ranked = lessons.OrderByDescending(x =>
        {
            var p = x.SemanticPayload?.Provenance;
            var c = ReadDouble(p, "confidence", 0d);
            var e = ReadDouble(p, "evidence_health", 0d);
            var t = ReadString(p, "freshness_tier") ?? "yearly";
            var w = TierWeights.TryGetValue(t, out var v) ? v : 0.15d;
            return 0.55d * c + 0.30d * e + 0.15d * w;
        }).ThenBy(x => x.ObjectId, StringComparer.Ordinal).ToList();

        var summaryIds = new List<string>();
        WriteSummary("agent_summary:shared", "shared", ranked.Take(8), nowUtc); summaryIds.Add("agent_summary:shared");

        var bySource = ranked.SelectMany(x => ReadStringList(x.SemanticPayload?.Provenance, "source_project_keys")
            .Select(k => (lesson: x, source: SourceFromKey(k))))
            .Where(x => !string.IsNullOrWhiteSpace(x.source))
            .GroupBy(x => x.source!, StringComparer.Ordinal)
            .OrderBy(x => x.Key, StringComparer.Ordinal);

        foreach (var group in bySource)
        {
            var id = $"agent_summary:source:{group.Key}";
            WriteSummary(id, $"source:{group.Key}", group.Select(x => x.lesson).DistinctBy(x => x.ObjectId).Take(8), nowUtc);
            summaryIds.Add(id);
        }

        foreach (var smartListSummaryId in BuildSmartListSummaries(nowUtc))
            summaryIds.Add(smartListSummaryId);

        foreach (var link in _store.IterateForward(AgentSummaryIndexContainer).ToList()) _store.RemoveLinkNode(AgentSummaryIndexContainer, link.LinkNodeId);
        foreach (var id in summaryIds.OrderBy(x => x, StringComparer.Ordinal)) _store.AddObject(AgentSummaryIndexContainer, id);
        return summaryIds.Count;
    }

    private void SyncFreshnessLanes(DateTimeOffset nowUtc)
    {
        EnsureFreshnessReady(nowUtc);
    }

    private int EnsureFreshnessReady(DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        EnsureFreshnessScaffold(nowUtc);

        var lanes = GetFreshnessLanes(includeFrozen: true);
        if (lanes.Count == 0 || !string.Equals(lanes[0].Status, FreshnessStatusActive, StringComparison.Ordinal))
        {
            CreateFreshnessHeadLane(nowUtc, string.Empty, lanes.Count == 0 ? "bootstrap" : "repair");
            lanes = GetFreshnessLanes(includeFrozen: true);
        }

        var unseen = EnumerateFreshnessCandidateObjects()
            .Where(obj => !HasFreshnessMembership(obj.ObjectId))
            .Select(obj => obj.ObjectId)
            .ToList();

        return unseen.Count == 0
            ? 0
            : AdmitObjectsToFreshness(
                unseen,
                nowUtc,
                "ingest",
                BuildTopicKey(unseen
                    .Where(id => _store.Objects.TryGetValue(id, out _))
                    .SelectMany(id => BuildTopicTokensForObject(_store.Objects[id]))
                    .ToList()));
    }

    private void EnsureFreshnessScaffold(DateTimeOffset nowUtc)
    {
        _smartLists.CreateBucket(FreshnessSmartListRootPath, durable: true, "agent-memory", nowUtc);
        _smartLists.CreateBucket(FreshnessLaneRootPath, durable: true, "agent-memory", nowUtc);
        _smartLists.UpdateBucketFields(FreshnessSmartListRootPath, new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [SmartListService.RetrievalVisibilityKey] = SmartListService.RetrievalVisibilitySuppressed
        }, nowUtc);
        _smartLists.UpdateBucketFields(FreshnessLaneRootPath, new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [SmartListService.RetrievalVisibilityKey] = SmartListService.RetrievalVisibilitySuppressed
        }, nowUtc);

        _store.UpsertObject(AgentMemoryFreshnessStateObjectId, "agent_memory_state");
        var state = _store.Objects[AgentMemoryFreshnessStateObjectId];
        state.SemanticPayload ??= new SemanticPayload();
        state.SemanticPayload.Summary = "agent memory freshness lanes";
        var prov = EnsureProv(state);
        prov["current_lane_path"] = JsonSerializer.SerializeToElement(ReadString(prov, "current_lane_path") ?? string.Empty);
        prov["current_topic_key"] = JsonSerializer.SerializeToElement(ReadString(prov, "current_topic_key") ?? string.Empty);
        prov["last_rotation_reason"] = JsonSerializer.SerializeToElement(ReadString(prov, "last_rotation_reason") ?? string.Empty);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);
    }

    private void WriteFreshnessState(string lanePath, string topicKey, string reason, DateTimeOffset nowUtc)
    {
        _store.UpsertObject(AgentMemoryFreshnessStateObjectId, "agent_memory_state");
        var state = _store.Objects[AgentMemoryFreshnessStateObjectId];
        var prov = EnsureProv(state);
        prov["current_lane_path"] = JsonSerializer.SerializeToElement(lanePath);
        prov["current_topic_key"] = JsonSerializer.SerializeToElement(topicKey);
        prov["last_rotation_reason"] = JsonSerializer.SerializeToElement(reason);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);
        state.UpdatedAt = nowUtc;
    }

    private IReadOnlyList<FreshnessLaneInfo> GetFreshnessLanes(bool includeFrozen)
    {
        var lanes = new List<FreshnessLaneInfo>();
        foreach (var laneObjectId in _store.IterateForward(AgentMemoryFreshnessContainer).Select(x => x.ObjectId))
        {
            if (!_store.Objects.TryGetValue(laneObjectId, out var obj) || obj.ObjectKind != SmartListService.BucketObjectKind)
                continue;

            var prov = obj.SemanticPayload?.Provenance;
            var path = ReadString(prov, "path") ?? string.Empty;
            if (!IsFreshnessLanePath(path))
                continue;

            var status = ReadString(prov, "status") ?? FreshnessStatusHistorical;
            if (!includeFrozen && string.Equals(status, FreshnessStatusFrozen, StringComparison.Ordinal))
                continue;

            lanes.Add(new FreshnessLaneInfo(
                path,
                obj.ObjectId,
                status,
                ReadString(prov, "topic_key") ?? string.Empty,
                CountFreshnessLaneMembers(path),
                ReadDate(prov, "created_at") ?? obj.CreatedAt,
                ReadDate(prov, "updated_at") ?? obj.UpdatedAt,
                lanes.Count));
        }

        return lanes;
    }

    private IReadOnlyDictionary<string, FreshnessObjectPosition> BuildFreshnessObjectPositions(bool includeFrozen)
    {
        var positions = new Dictionary<string, FreshnessObjectPosition>(StringComparer.Ordinal);
        foreach (var lane in GetFreshnessLanes(includeFrozen))
        {
            var label = FreshnessTemperatureLabel(lane.Status, lane.Index);
            foreach (var memberId in _smartLists.GetBucketMemberObjectIds(lane.Path))
            {
                if (positions.ContainsKey(memberId) || IsFreshnessInternalObject(memberId))
                    continue;

                positions[memberId] = new FreshnessObjectPosition(
                    lane.Path,
                    lane.Status,
                    lane.TopicKey,
                    lane.Index,
                    label);
            }
        }

        return positions;
    }

    private HashSet<string> BuildFrozenOnlyObjectIds()
    {
        var accessible = BuildFreshnessObjectPositions(includeFrozen: false).Keys.ToHashSet(StringComparer.Ordinal);
        var frozenOnly = new HashSet<string>(StringComparer.Ordinal);
        foreach (var lane in GetFreshnessLanes(includeFrozen: true).Where(x => string.Equals(x.Status, FreshnessStatusFrozen, StringComparison.Ordinal)))
        {
            foreach (var memberId in _smartLists.GetBucketMemberObjectIds(lane.Path))
            {
                if (!accessible.Contains(memberId))
                    frozenOnly.Add(memberId);
            }
        }

        return frozenOnly;
    }

    private int AdmitObjectsToFreshness(IEnumerable<string> objectIds, DateTimeOffset nowUtc, string admissionReason, string topicKey)
    {
        EnsureFreshnessScaffold(nowUtc);
        var admitted = 0;
        foreach (var objectId in objectIds
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal))
        {
            admitted += AdmitObjectToFreshness(objectId, nowUtc, admissionReason, topicKey);
        }

        return admitted;
    }

    private int AdmitObjectToFreshness(string objectId, DateTimeOffset nowUtc, string admissionReason, string topicKey)
    {
        if ((!_store.Objects.ContainsKey(objectId) && !_store.Containers.ContainsKey(objectId)) || IsFreshnessInternalObject(objectId))
            return 0;

        var head = EnsureFreshnessHeadLane(nowUtc, topicKey, admissionReason, objectId);
        var headMembers = _smartLists.GetBucketMemberObjectIds(head.Path);
        if (headMembers.Contains(objectId, StringComparer.Ordinal))
            return 0;

        _smartLists.Attach(head.Path, objectId, "agent-memory", nowUtc);
        UpdateFreshnessLaneFields(
            head.Path,
            FreshnessStatusActive,
            string.IsNullOrWhiteSpace(topicKey) ? head.TopicKey : topicKey,
            admissionReason,
            nowUtc);
        WriteFreshnessState(head.Path, string.IsNullOrWhiteSpace(topicKey) ? head.TopicKey : topicKey, admissionReason, nowUtc);
        return 1;
    }

    private FreshnessLaneInfo EnsureFreshnessHeadLane(DateTimeOffset nowUtc, string topicKey, string reason, string? incomingObjectId = null)
    {
        EnsureFreshnessScaffold(nowUtc);
        var lanes = GetFreshnessLanes(includeFrozen: true);
        if (lanes.Count == 0)
            return CreateFreshnessHeadLane(nowUtc, topicKey, reason);

        var head = lanes[0];
        var headMembers = _smartLists.GetBucketMemberObjectIds(head.Path);
        var incomingDistinct = !string.IsNullOrWhiteSpace(incomingObjectId)
            && !headMembers.Contains(incomingObjectId, StringComparer.Ordinal);

        if (string.Equals(head.Status, FreshnessStatusFrozen, StringComparison.Ordinal)
            || string.Equals(reason, "topic-shift", StringComparison.Ordinal)
            || string.Equals(reason, "user-shelve", StringComparison.Ordinal)
            || (incomingDistinct && head.MemberCount >= FreshnessHeadLaneMax)
            || ShouldRotateForTopicShift(head.TopicKey, topicKey, head.MemberCount))
        {
            var rotateReason = string.Equals(reason, "topic-shift", StringComparison.Ordinal) || string.Equals(reason, "user-shelve", StringComparison.Ordinal)
                ? reason
                : incomingDistinct && head.MemberCount >= FreshnessHeadLaneMax
                    ? "overflow"
                    : "topic-shift";
            return CreateFreshnessHeadLane(nowUtc, topicKey, rotateReason);
        }

        if (!string.Equals(head.Status, FreshnessStatusActive, StringComparison.Ordinal))
        {
            UpdateFreshnessLaneFields(head.Path, FreshnessStatusActive, string.IsNullOrWhiteSpace(topicKey) ? head.TopicKey : topicKey, reason, nowUtc);
            head = GetFreshnessLanes(includeFrozen: true).First(x => string.Equals(x.Path, head.Path, StringComparison.Ordinal));
        }

        return head;
    }

    private FreshnessLaneInfo CreateFreshnessHeadLane(DateTimeOffset nowUtc, string topicKey, string reason)
    {
        EnsureFreshnessScaffold(nowUtc);

        var currentLanes = GetFreshnessLanes(includeFrozen: true);
        if (currentLanes.Count > 0 && string.Equals(currentLanes[0].Status, FreshnessStatusActive, StringComparison.Ordinal))
        {
            UpdateFreshnessLaneFields(
                currentLanes[0].Path,
                FreshnessStatusHistorical,
                currentLanes[0].TopicKey,
                reason,
                nowUtc);
        }

        var laneSuffix = Guid.NewGuid().ToString("N")[..8];
        var laneId = $"lane-{nowUtc:yyyyMMddHHmmss}-{laneSuffix}";
        var path = $"{FreshnessLaneRootPath}/{laneId}";
        var bucket = _smartLists.CreateBucket(path, durable: true, "agent-memory", nowUtc);
        UpdateFreshnessLaneFields(path, FreshnessStatusActive, topicKey, reason, nowUtc);

        var orderedLaneIds = new[] { bucket.ObjectId }
            .Concat(_store.IterateForward(AgentMemoryFreshnessContainer).Select(x => x.ObjectId))
            .Where(id => _store.Objects.TryGetValue(id, out var obj)
                && obj.ObjectKind == SmartListService.BucketObjectKind
                && IsFreshnessLanePath(ReadString(obj.SemanticPayload?.Provenance, "path")))
            .Distinct(StringComparer.Ordinal)
            .ToList();
        ReplaceMembers(AgentMemoryFreshnessContainer, orderedLaneIds);
        WriteFreshnessState(path, topicKey, reason, nowUtc);
        return GetFreshnessLanes(includeFrozen: true).First(x => string.Equals(x.Path, path, StringComparison.Ordinal));
    }

    private void UpdateFreshnessLaneFields(string path, string status, string topicKey, string admissionReason, DateTimeOffset nowUtc)
    {
        var laneId = path[(path.LastIndexOf('/') + 1)..];
        var existingCreatedAt = _store.Objects.TryGetValue($"smartlist-bucket:{path}", out var obj)
            ? ReadDate(obj.SemanticPayload?.Provenance, "created_at") ?? obj.CreatedAt
            : nowUtc;
        _smartLists.UpdateBucketFields(path, new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["lane_id"] = laneId,
            ["status"] = status,
            ["topic_key"] = topicKey,
            ["admission_reason"] = admissionReason,
            ["member_count"] = CountFreshnessLaneMembers(path).ToString(),
            [SmartListService.RetrievalVisibilityKey] = SmartListService.RetrievalVisibilitySuppressed,
            ["updated_at"] = nowUtc.ToString("O"),
            ["created_at"] = existingCreatedAt.ToString("O")
        }, nowUtc);
    }

    private bool ShouldRotateForTopicShift(string currentTopicKey, string incomingTopicKey, int memberCount)
    {
        if (memberCount < 4 || string.IsNullOrWhiteSpace(currentTopicKey) || string.IsNullOrWhiteSpace(incomingTopicKey))
            return false;

        var currentTokens = currentTopicKey.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var incomingTokens = incomingTopicKey.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return !currentTokens.Intersect(incomingTokens, StringComparer.Ordinal).Any();
    }

    private int CountFreshnessLaneMembers(string path)
        => _smartLists.GetBucketMemberObjectIds(path).Count(id => !IsFreshnessInternalObject(id));

    private IEnumerable<ObjectRecord> EnumerateFreshnessCandidateObjects()
    {
        return _store.Objects.Values
            .Where(IsFreshnessCandidateObject)
            .OrderByDescending(x => x.UpdatedAt)
            .ThenByDescending(x => x.CreatedAt)
            .ThenBy(x => x.ObjectId, StringComparer.Ordinal);
    }

    private bool IsFreshnessCandidateObject(ObjectRecord obj)
    {
        if (IsFreshnessInternalObject(obj.ObjectId))
            return false;

        if (obj.ObjectKind is "lesson" or "chat_message" or "task_thread" or "task_checkpoint" or "task_artifact"
            or SmartListService.BucketObjectKind or SmartListService.NoteObjectKind or SmartListService.RollupObjectKind)
        {
            if (obj.ObjectKind == SmartListService.BucketObjectKind)
            {
                var path = ReadString(obj.SemanticPayload?.Provenance, "path");
                return !IsFreshnessInternalPath(path);
            }

            return true;
        }

        return string.Equals(obj.ObjectKind, "container", StringComparison.Ordinal)
            && _store.Containers.TryGetValue(obj.ObjectId, out var container)
            && string.Equals(container.ContainerKind, "chat_session", StringComparison.Ordinal);
    }

    private bool HasFreshnessMembership(string objectId)
    {
        return _store.ContainersForMemberObject(objectId)
            .Any(containerId => containerId.StartsWith($"smartlist-members:{FreshnessLaneRootPath}/", StringComparison.Ordinal));
    }

    private IReadOnlyList<string> BuildTopicTokensForObject(ObjectRecord obj)
    {
        var prov = obj.SemanticPayload?.Provenance;
        var inputs = new List<string>
        {
            obj.SemanticPayload?.Summary ?? string.Empty,
            ReadString(prov, "title") ?? string.Empty,
            ReadString(prov, "text") ?? string.Empty,
            ReadString(prov, "path") ?? string.Empty,
            obj.ObjectId
        };

        if (string.Equals(obj.ObjectKind, "container", StringComparison.Ordinal)
            && _store.Containers.TryGetValue(obj.ObjectId, out var container)
            && string.Equals(container.ContainerKind, "chat_session", StringComparison.Ordinal))
        {
            inputs.Add(ReadString(container.Metadata, "title") ?? string.Empty);
        }

        return BuildFreeTextTokens(string.Join(' ', inputs));
    }

    private static string BuildTopicKey(IReadOnlyList<string> tokens)
        => string.Join(' ', tokens.Take(6));

    private static bool IsDeepMemoryQuery(string query)
        => BuildFreeTextTokens(query).Any(DeepMemoryTokens.Contains);

    private static bool IsFreshnessLanePath(string? path)
        => !string.IsNullOrWhiteSpace(path)
            && path.StartsWith(FreshnessLaneRootPath + "/", StringComparison.Ordinal);

    private static bool IsFreshnessInternalPath(string? path)
        => !string.IsNullOrWhiteSpace(path)
            && (string.Equals(path, FreshnessSmartListRootPath, StringComparison.Ordinal)
                || path.StartsWith(FreshnessSmartListRootPath + "/", StringComparison.Ordinal));

    private bool IsFreshnessInternalObject(string objectId)
    {
        if (string.Equals(objectId, AgentMemoryFreshnessStateObjectId, StringComparison.Ordinal))
            return true;

        if (_store.Objects.TryGetValue(objectId, out var obj))
        {
            var prov = obj.SemanticPayload?.Provenance;
            var path = obj.ObjectKind switch
            {
                var kind when kind == SmartListService.BucketObjectKind => ReadString(prov, "path"),
                var kind when kind == SmartListService.RollupObjectKind => ReadString(prov, "bucket_path"),
                _ => null
            };
            if (IsFreshnessInternalPath(path))
                return true;
        }

        return objectId.StartsWith($"smartlist-members:{FreshnessSmartListRootPath}", StringComparison.Ordinal);
    }

    private static string FreshnessTemperatureLabel(string status, int laneIndex)
    {
        if (string.Equals(status, FreshnessStatusFrozen, StringComparison.Ordinal))
            return "frozen";
        if (laneIndex == 0)
            return "hot";
        return laneIndex <= 2 ? "warm" : "cold";
    }

    private static double FreshnessLaneBoost(FreshnessObjectPosition? position)
    {
        if (position is null)
            return 0d;

        return position.TemperatureLabel switch
        {
            "hot" => 0.22d,
            "warm" => 0.12d,
            "cold" => 0.06d,
            "frozen" => 0.03d,
            _ => 0d
        };
    }

    private void SyncAgentMemoryTree()
    {
        var lessonIds = _store.Objects.Values
            .Where(x => x.ObjectKind == "lesson")
            .Select(x => x.ObjectId)
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();
        ReplaceMembers(AgentMemoryLessonsContainer, lessonIds);

        var summaryIds = _store.IterateForward(AgentSummaryIndexContainer)
            .Select(x => x.ObjectId)
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();
        ReplaceMembers(AgentMemorySummaryContainer, summaryIds);

        var stereotypeContainers = _store.Containers.Keys
            .Where(x => x.StartsWith("lesson-stereotype-members:", StringComparison.Ordinal))
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();
        ReplaceMembers(AgentMemoryStereotypeContainer, stereotypeContainers);

        var sourceLinkContainers = _store.Containers.Keys
            .Where(x => x.StartsWith("lesson-sources:", StringComparison.Ordinal))
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();
        ReplaceMembers(AgentMemorySourceLinksContainer, sourceLinkContainers);

        var bySource = lessonIds
            .Select(id => _store.Objects.TryGetValue(id, out var lesson) ? (id, source: ReadSource(lesson.SemanticPayload?.Provenance)) : (id, source: string.Empty))
            .Where(x => !string.IsNullOrWhiteSpace(x.source))
            .GroupBy(x => x.source!, StringComparer.Ordinal)
            .OrderBy(x => x.Key, StringComparer.Ordinal)
            .ToList();

        var sourceGroupContainers = new List<string>();
        foreach (var group in bySource)
        {
            var groupId = $"agent-memory:source:{group.Key}";
            EnsureUniqueContainer(groupId, "agent_memory_source_group");
            ReplaceMembers(groupId, group.Select(x => x.id).OrderBy(x => x, StringComparer.Ordinal).ToList());
            sourceGroupContainers.Add(groupId);
        }

        ReplaceMembers(AgentMemorySourceGroupsContainer, sourceGroupContainers.OrderBy(x => x, StringComparer.Ordinal).ToList());
        SyncSemanticHierarchy(lessonIds);
    }

    private void SyncSemanticHierarchy(IReadOnlyList<string> lessonIds)
    {
        EnsureUniqueContainer(AgentMemorySemanticContainer, "agent_memory_group");

        var kindContainers = DreamKinds
            .Select(kind =>
            {
                var containerId = $"agent-memory:semantic:{kind}";
                EnsureUniqueContainer(containerId, "agent_memory_semantic_kind");
                return (kind, containerId);
            })
            .ToDictionary(x => x.kind, x => x.containerId, StringComparer.Ordinal);

        var groups = new Dictionary<string, SemanticNodeGroup>(StringComparer.Ordinal);
        foreach (var lessonId in lessonIds)
        {
            if (!_store.Objects.TryGetValue(lessonId, out var lesson))
                continue;

            var kind = SemanticKindFromLesson(lesson);
            var label = BuildSemanticClusterLabel(lesson.SemanticPayload?.Summary, kind, lesson.ObjectId);
            var slug = BuildSemanticSlug(label, lesson.ObjectId);
            var key = $"{kind}:{slug}";

            if (!groups.TryGetValue(key, out var group))
            {
                group = new SemanticNodeGroup(kind, label);
                groups[key] = group;
            }

            group.LessonIds.Add(lessonId);
            var sourceContainerId = $"lesson-sources:{lessonId}";
            if (_store.Containers.ContainsKey(sourceContainerId))
                group.SourceContainerIds.Add(sourceContainerId);
        }

        var nodeContainersByKind = DreamKinds.ToDictionary(
            kind => kind,
            _ => new List<string>(),
            StringComparer.Ordinal);
        var themeGroups = new Dictionary<string, SemanticThemeGroup>(StringComparer.Ordinal);

        foreach (var entry in groups.OrderBy(x => x.Key, StringComparer.Ordinal))
        {
            var key = entry.Key;
            var group = entry.Value;
            var capKind = Capitalize(group.Kind);

            var nodeObjectId = $"lesson-semantic:{key}";
            var nodeContainerId = $"lesson-semantic-node:{key}";
            var membersContainerId = $"lesson-semantic-members:{key}";
            var sourcesContainerId = $"lesson-semantic-sources:{key}";

            _store.UpsertObject(nodeObjectId, "lesson_semantic_node");
            var nodeObject = _store.Objects[nodeObjectId];
            nodeObject.SemanticPayload ??= new SemanticPayload();
            nodeObject.SemanticPayload.Summary = $"{capKind}: {group.Label}";
            nodeObject.SemanticPayload.Tags = ["lesson_semantic_node", group.Kind];
            var nodeProv = EnsureProv(nodeObject);
            nodeProv["kind"] = JsonSerializer.SerializeToElement(group.Kind);
            nodeProv["semantic_slug"] = JsonSerializer.SerializeToElement(key[(group.Kind.Length + 1)..]);
            nodeProv["node_container_id"] = JsonSerializer.SerializeToElement(nodeContainerId);
            nodeProv["members_container_id"] = JsonSerializer.SerializeToElement(membersContainerId);
            nodeProv["sources_container_id"] = JsonSerializer.SerializeToElement(sourcesContainerId);
            nodeProv["lesson_count"] = JsonSerializer.SerializeToElement(group.LessonIds.Count);
            nodeProv["representative_lesson_id"] = JsonSerializer.SerializeToElement(group.LessonIds.OrderBy(x => x, StringComparer.Ordinal).FirstOrDefault() ?? string.Empty);

            EnsureUniqueContainer(nodeContainerId, "lesson_semantic_node");
            EnsureUniqueContainer(membersContainerId, "lesson_semantic_members");
            EnsureUniqueContainer(sourcesContainerId, "lesson_semantic_sources");

            ReplaceMembers(
                membersContainerId,
                group.LessonIds.OrderBy(x => x, StringComparer.Ordinal).ToList());
            ReplaceMembers(
                sourcesContainerId,
                group.SourceContainerIds.OrderBy(x => x, StringComparer.Ordinal).ToList());
            ReplaceMembers(nodeContainerId, [nodeObjectId, membersContainerId, sourcesContainerId]);

            nodeContainersByKind[group.Kind].Add(nodeContainerId);

            var themeLabel = BuildSemanticThemeLabel(group.Label, group.Kind);
            var themeSlug = BuildSemanticSlug(themeLabel, key);
            var themeKey = $"{group.Kind}:{themeSlug}";
            if (!themeGroups.TryGetValue(themeKey, out var theme))
            {
                theme = new SemanticThemeGroup(group.Kind, themeLabel);
                themeGroups[themeKey] = theme;
            }
            theme.NodeContainerIds.Add(nodeContainerId);
        }

        var themeContainersByKind = DreamKinds.ToDictionary(
            kind => kind,
            _ => new List<string>(),
            StringComparer.Ordinal);
        foreach (var entry in themeGroups.OrderBy(x => x.Key, StringComparer.Ordinal))
        {
            var themeKey = entry.Key;
            var theme = entry.Value;
            var capKind = Capitalize(theme.Kind);
            var themeObjectId = $"lesson-semantic-theme-object:{themeKey}";
            var themeContainerId = $"lesson-semantic-theme:{themeKey}";

            _store.UpsertObject(themeObjectId, "lesson_semantic_theme");
            var themeObject = _store.Objects[themeObjectId];
            themeObject.SemanticPayload ??= new SemanticPayload();
            themeObject.SemanticPayload.Summary = $"{capKind} Theme: {theme.Label}";
            themeObject.SemanticPayload.Tags = ["lesson_semantic_theme", theme.Kind];
            var themeProv = EnsureProv(themeObject);
            themeProv["kind"] = JsonSerializer.SerializeToElement(theme.Kind);
            themeProv["theme_slug"] = JsonSerializer.SerializeToElement(themeKey[(theme.Kind.Length + 1)..]);
            themeProv["theme_container_id"] = JsonSerializer.SerializeToElement(themeContainerId);
            themeProv["node_count"] = JsonSerializer.SerializeToElement(theme.NodeContainerIds.Count);

            EnsureUniqueContainer(themeContainerId, "lesson_semantic_theme");
            ReplaceMembers(
                themeContainerId,
                [themeObjectId, .. theme.NodeContainerIds.OrderBy(x => x, StringComparer.Ordinal)]);

            themeContainersByKind[theme.Kind].Add(themeContainerId);
        }

        foreach (var kind in DreamKinds)
        {
            var kindContainerId = kindContainers[kind];
            var themeContainers = themeContainersByKind[kind].OrderBy(x => x, StringComparer.Ordinal).ToList();
            if (themeContainers.Count == 0)
            {
                ReplaceMembers(kindContainerId, nodeContainersByKind[kind].OrderBy(x => x, StringComparer.Ordinal).ToList());
                continue;
            }

            ReplaceMembers(kindContainerId, themeContainers);
        }

        ReplaceMembers(
            AgentMemorySemanticContainer,
            DreamKinds.Select(kind => kindContainers[kind]).ToList());
    }

    private void WriteSummary(string id, string scope, IEnumerable<ObjectRecord> lessons, DateTimeOffset nowUtc)
    {
        var lines = lessons.Select(x => $"- {x.SemanticPayload?.Summary ?? x.ObjectId}").ToList();
        _store.UpsertObject(id, "agent_summary");
        var obj = _store.Objects[id];
        obj.SemanticPayload ??= new SemanticPayload();
        obj.SemanticPayload.Summary = lines.Count == 0 ? $"Scope: {scope}{Environment.NewLine}No ranked lessons available." : $"Scope: {scope}{Environment.NewLine}{string.Join(Environment.NewLine, lines)}";
        var prov = EnsureProv(obj);
        prov["scope"] = JsonSerializer.SerializeToElement(scope);
        prov["generated_at"] = JsonSerializer.SerializeToElement(nowUtc);
    }

    private IReadOnlyList<string> BuildSmartListSummaries(DateTimeOffset nowUtc)
    {
        var summaryIds = new List<string>();
        var bucketPathsWithRollups = _store.Objects.Values
            .Where(x => x.ObjectKind == SmartListService.RollupObjectKind
                && string.Equals(ReadString(x.SemanticPayload?.Provenance, "durability"), "durable", StringComparison.Ordinal))
            .Select(x => ReadString(x.SemanticPayload?.Provenance, "bucket_path"))
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .ToHashSet(StringComparer.Ordinal);

        var smartListObjects = _store.Objects.Values
            .Where(x => (x.ObjectKind == SmartListService.BucketObjectKind || x.ObjectKind == SmartListService.NoteObjectKind || x.ObjectKind == SmartListService.RollupObjectKind)
                && string.Equals(ReadString(x.SemanticPayload?.Provenance, "durability"), "durable", StringComparison.Ordinal))
            .Where(x => string.Equals(
                SmartListService.ReadRetrievalVisibility(x.SemanticPayload?.Provenance),
                SmartListService.RetrievalVisibilityDefault,
                StringComparison.Ordinal))
            .Where(x => x.ObjectKind != SmartListService.BucketObjectKind
                || !bucketPathsWithRollups.Contains(ReadString(x.SemanticPayload?.Provenance, "path") ?? string.Empty))
            .OrderBy(x => x.ObjectId, StringComparer.Ordinal)
            .ToList();

        foreach (var obj in smartListObjects)
        {
            var id = $"agent_summary:smartlist:{Hash8(obj.ObjectId)}";
            var scope = obj.ObjectKind switch
            {
                SmartListService.BucketObjectKind => $"smartlist:{ReadString(obj.SemanticPayload?.Provenance, "path") ?? obj.ObjectId}",
                SmartListService.RollupObjectKind => $"smartlist:{ReadString(obj.SemanticPayload?.Provenance, "bucket_path") ?? obj.ObjectId}",
                _ => $"smartlist-note:{obj.ObjectId}"
            };
            WriteSummary(id, scope, [obj], nowUtc);
            summaryIds.Add(id);
        }

        return summaryIds;
    }

    private RankedLessons RankLessons(
        string query,
        int top,
        AgentQueryContext? context,
        IReadOnlyDictionary<Guid, ContextRoute>? graphRoutes,
        IReadOnlyDictionary<string, FreshnessObjectPosition> freshnessPositions,
        ISet<string> excludedObjectIds)
    {
        var tokens = Scoring.Tokenize(query);
        if (tokens.Count == 0)
        {
            return new RankedLessons(
                [],
                new AgentQueryDiagnostics(
                    "none",
                    "none",
                    [],
                    ScopeLens(context)));
        }

        var semanticHits = RankLessonsFromSemanticNodes(tokens, top, context, graphRoutes, freshnessPositions, excludedObjectIds);
        var rawHits = RankLessonsRaw(tokens, top, context, graphRoutes, freshnessPositions, excludedObjectIds);
        var routingFlags = new HashSet<string>(StringComparer.Ordinal);

        if (semanticHits.Count == 0)
        {
            return new RankedLessons(
                rawHits,
                new AgentQueryDiagnostics(
                    "raw-lesson",
                    "semantic-empty",
                    [],
                    ScopeLens(context)));
        }

        var bestSemantic = semanticHits[0];
        var bestRaw = rawHits.FirstOrDefault();
        var weakSemantic = IsWeakSemanticHit(bestSemantic, tokens.Count);
        if (weakSemantic)
        {
            foreach (var risk in bestSemantic.Explain?.RiskFlags ?? [])
                routingFlags.Add(risk);
        }

        var rawMoreSpecific = bestRaw?.Explain?.MatchedTokens.Count > bestSemantic.Explain?.MatchedTokens.Count;
        var rawMateriallyBetter = bestRaw is not null && bestRaw.Score >= (bestSemantic.Score + 0.05d);
        var shouldReroute = weakSemantic && bestRaw is not null && (rawMoreSpecific || rawMateriallyBetter);
        if (shouldReroute)
        {
            if (rawMoreSpecific)
                routingFlags.Add("raw-more-specific");
            if (rawMateriallyBetter)
                routingFlags.Add("raw-score-stronger");

            return new RankedLessons(
                rawHits,
                new AgentQueryDiagnostics(
                    "raw-lesson",
                    "rerouted-from-semantic-node-first",
                    routingFlags.OrderBy(x => x, StringComparer.Ordinal).ToList(),
                    ScopeLens(context)));
        }

        return new RankedLessons(
            semanticHits,
            new AgentQueryDiagnostics(
                "semantic-node-first",
                "semantic-node-first",
                routingFlags.OrderBy(x => x, StringComparer.Ordinal).ToList(),
                ScopeLens(context)));
    }

    private List<AgentLessonHit> RankLessonsRaw(
        IReadOnlyList<string> tokens,
        int top,
        AgentQueryContext? context,
        IReadOnlyDictionary<Guid, ContextRoute>? graphRoutes,
        IReadOnlyDictionary<string, FreshnessObjectPosition> freshnessPositions,
        ISet<string> excludedObjectIds)
    {
        return _store.Objects.Values
            .Where(x => x.ObjectKind == "lesson")
            .Where(x => !excludedObjectIds.Contains(x.ObjectId))
            .Select(lesson =>
            {
                var score = ScoreLesson(lesson, tokens, freshnessPositions);
                var prov = lesson.SemanticPayload?.Provenance;
                var matchLocations = BuildTokenMatchLocations(null, lesson, score.Snapshots, tokens);
                var matchedTokens = MatchTokens(matchLocations);
                if (matchedTokens.Count == 0)
                    return null;

                var contextRoute = ResolveContextRoute(
                    context,
                    lesson.SemanticPayload?.Summary,
                    score.Snapshots.Select(x => $"{x.snippet}\n{x.source_ref}"),
                    BuildLessonAnchors(lesson, score.Snapshots),
                    BuildCandidateRetrievalNodeIds(lesson),
                    graphRoutes);
                if (ShouldSuppressWeakScopedLesson(context, tokens.Count, matchedTokens.Count, contextRoute, score.Semantic))
                    return null;
                var finalScore = score.Score + contextRoute.Boost;

                var riskFlags = new List<string>();
                if (matchedTokens.Count <= Math.Max(1, tokens.Count / 4))
                    riskFlags.Add("low-specificity");
                if (score.Semantic < 0.35d && (score.FreshnessContribution + score.EvidenceContribution) > score.SemanticContribution)
                    riskFlags.Add("freshness-evidence-boosted");
                if (context is not null && context.HasLineage && contextRoute.Boost <= 0d)
                    riskFlags.Add("global-fallback");

                return new AgentLessonHit(
                    lesson.ObjectId,
                    lesson.SemanticPayload?.Summary ?? lesson.ObjectId,
                    finalScore,
                    score.Confidence,
                    score.Evidence,
                    score.Tier,
                    ReadString(prov, "stereotype_family_id") ?? string.Empty,
                    ReadString(prov, "stereotype_version_id") ?? string.Empty,
                    score.Snapshots,
                    new AgentHitExplain(
                        "raw-lesson",
                        matchedTokens,
                        matchLocations,
                        new AgentScoreBreakdown(
                            score.Semantic,
                            score.SemanticContribution,
                            score.FreshnessContribution,
                            score.EvidenceContribution,
                            score.Decay,
                            finalScore),
                        $"raw-lesson -> {lesson.ObjectId} -> {(ReadString(prov, "stereotype_family_id") ?? "(none)")} -> {(ReadString(prov, "stereotype_version_id") ?? "(none)")} -> {contextRoute.Label}",
                        contextRoute.Cost,
                        false,
                        riskFlags,
                        BuildWhyWonSentence(
                            "raw-lesson",
                            matchedTokens.Count,
                            tokens.Count,
                            false,
                            riskFlags) + ContextWhy(contextRoute)));
            })
            .Where(x => x is not null && x.Score > 0d)
            .Select(x => x!)
            .Select(hit => (hit, position: freshnessPositions.TryGetValue(hit.LessonId, out var position) ? position : null))
            .OrderBy(x => x.position is null ? 1 : 0)
            .ThenBy(x => x.position?.LaneIndex ?? int.MaxValue)
            .ThenByDescending(x => x.hit.Score)
            .ThenBy(x => x.hit.LessonId, StringComparer.Ordinal)
            .Take(top)
            .Select(x => x.hit)
            .ToList();
    }

    private List<AgentLessonHit> RankLessonsFromSemanticNodes(
        IReadOnlyList<string> tokens,
        int top,
        AgentQueryContext? context,
        IReadOnlyDictionary<Guid, ContextRoute>? graphRoutes,
        IReadOnlyDictionary<string, FreshnessObjectPosition> freshnessPositions,
        ISet<string> excludedObjectIds)
    {
        var nodes = _store.Objects.Values
            .Where(x => x.ObjectKind == "lesson_semantic_node")
            .OrderBy(x => x.ObjectId, StringComparer.Ordinal)
            .ToList();

        if (nodes.Count == 0)
            return [];

        var hits = new List<AgentLessonHit>();
        foreach (var node in nodes)
        {
            var nodeProv = node.SemanticPayload?.Provenance;
            var membersContainerId = ReadString(nodeProv, "members_container_id");
            if (string.IsNullOrWhiteSpace(membersContainerId) || !_store.Containers.ContainsKey(membersContainerId))
                continue;

            var members = _store.IterateForward(membersContainerId)
                .Select(x => x.ObjectId)
                .Where(id => _store.Objects.TryGetValue(id, out var obj) && obj.ObjectKind == "lesson" && !excludedObjectIds.Contains(id))
                .Select(id => _store.Objects[id])
                .OrderBy(x => x.ObjectId, StringComparer.Ordinal)
                .ToList();
            if (members.Count == 0)
                continue;

            var rankedMembers = members
                .Select(lesson => (lesson, score: ScoreLesson(lesson, tokens, freshnessPositions)))
                .OrderByDescending(x => x.score.Score)
                .ThenBy(x => x.lesson.ObjectId, StringComparer.Ordinal)
                .ToList();

            var best = rankedMembers[0];
            var bestProv = best.lesson.SemanticPayload?.Provenance;
            var nodeHay = (node.SemanticPayload?.Summary ?? string.Empty) + "\n" + string.Join('\n', members.Select(x => x.SemanticPayload?.Summary ?? string.Empty));
            var nodeMatchedTokens = tokens.Where(t => nodeHay.Contains(t, StringComparison.OrdinalIgnoreCase)).Distinct(StringComparer.Ordinal).OrderBy(x => x, StringComparer.Ordinal).ToList();
            var nodeSemantic = (double)nodeMatchedTokens.Count / tokens.Count;
            var semantic = Math.Max(nodeSemantic, best.score.Semantic);
            var memberCount = members.Count;
            var representativeMismatch = TitlesMateriallyDiffer(node.SemanticPayload?.Summary, best.lesson.SemanticPayload?.Summary);
            var semanticContribution = 0.80d * semantic;
            var freshnessContribution = (0.10d * best.score.Freshness)
                + (freshnessPositions.TryGetValue(best.lesson.ObjectId, out var lanePosition) ? FreshnessLaneBoost(lanePosition) : 0d);
            var evidenceContribution = 0.10d * best.score.Evidence;
            var broadPenalty = memberCount >= 4 ? 0.12d : memberCount == 3 ? 0.06d : 0.00d;
            var lowSpecificityPenalty = nodeMatchedTokens.Count <= Math.Max(1, tokens.Count / 4) ? 0.06d : 0.00d;
            var representativePenalty = representativeMismatch ? 0.08d : 0.00d;
            var preDecay = Math.Max(0d, semanticContribution + freshnessContribution + evidenceContribution - broadPenalty - lowSpecificityPenalty - representativePenalty);
            var contextRoute = ResolveContextRoute(
                context,
                node.SemanticPayload?.Summary,
                members.Select(x => x.SemanticPayload?.Summary ?? string.Empty).Concat(best.score.Snapshots.Select(x => $"{x.snippet}\n{x.source_ref}")),
                BuildSemanticNodeAnchors(node, best.lesson, best.score.Snapshots),
                BuildCandidateRetrievalNodeIds(node, best.lesson),
                graphRoutes);
            var matchLocations = BuildTokenMatchLocations(node, best.lesson, best.score.Snapshots, tokens);
            var matchedTokens = MatchTokens(matchLocations);
            if (matchedTokens.Count == 0)
                continue;
            if (ShouldSuppressWeakScopedLesson(context, tokens.Count, matchedTokens.Count, contextRoute, semantic))
                continue;

            var score = (preDecay / best.score.Decay) + contextRoute.Boost;
            if (score <= 0d)
                continue;

            var riskFlags = new List<string>();
            if (memberCount >= 3)
                riskFlags.Add("broad-node");
            if (matchedTokens.Count <= Math.Max(1, tokens.Count / 4))
                riskFlags.Add("low-specificity");
            if (representativeMismatch)
                riskFlags.Add("representative-mismatch");
            if (semantic < 0.35d && (freshnessContribution + evidenceContribution) > semanticContribution)
                riskFlags.Add("freshness-evidence-boosted");
            if (context is not null && context.HasLineage && contextRoute.Boost <= 0d)
                riskFlags.Add("global-fallback");

            hits.Add(new AgentLessonHit(
                best.lesson.ObjectId,
                node.SemanticPayload?.Summary ?? best.lesson.SemanticPayload?.Summary ?? best.lesson.ObjectId,
                score,
                best.score.Confidence,
                best.score.Evidence,
                best.score.Tier,
                ReadString(bestProv, "stereotype_family_id") ?? string.Empty,
                ReadString(bestProv, "stereotype_version_id") ?? string.Empty,
                best.score.Snapshots,
                new AgentHitExplain(
                    "semantic-node-first",
                    matchedTokens,
                    matchLocations,
                    new AgentScoreBreakdown(
                        semantic,
                        semanticContribution,
                        freshnessContribution,
                        evidenceContribution,
                        best.score.Decay,
                        score),
                    $"{node.ObjectId} -> {best.lesson.ObjectId} -> {(ReadString(bestProv, "stereotype_family_id") ?? "(none)")} -> {(ReadString(bestProv, "stereotype_version_id") ?? "(none)")} -> {contextRoute.Label}",
                    contextRoute.Cost,
                    representativeMismatch,
                    riskFlags,
                    BuildWhyWonSentence(
                        "semantic-node-first",
                        matchedTokens.Count,
                        tokens.Count,
                        representativeMismatch,
                        riskFlags) + ContextWhy(contextRoute))));
        }

        return hits
            .Select(hit => (hit, position: freshnessPositions.TryGetValue(hit.LessonId, out var position) ? position : null))
            .OrderBy(x => x.position is null ? 1 : 0)
            .ThenBy(x => x.position?.LaneIndex ?? int.MaxValue)
            .ThenByDescending(x => x.hit.Score)
            .ThenBy(x => x.hit.LessonId, StringComparer.Ordinal)
            .Take(top)
            .Select(x => x.hit)
            .ToList();
    }

    private LessonScore ScoreLesson(ObjectRecord lesson, IReadOnlyList<string> tokens, IReadOnlyDictionary<string, FreshnessObjectPosition> freshnessPositions)
    {
        var prov = lesson.SemanticPayload?.Provenance;
        var snapshots = ReadSnapshots(prov);
        var hay = ((lesson.SemanticPayload?.Summary ?? string.Empty) + "\n" + string.Join('\n', snapshots.Select(s => s.snippet + "\n" + s.source_ref))).ToLowerInvariant();
        var semantic = (double)tokens.Count(t => hay.Contains(t, StringComparison.Ordinal)) / tokens.Count;
        var tier = ReadString(prov, "freshness_tier") ?? "yearly";
        var freshness = TierWeights.TryGetValue(tier, out var w) ? w : 0.15d;
        var laneBoost = freshnessPositions.TryGetValue(lesson.ObjectId, out var position) ? FreshnessLaneBoost(position) : 0d;
        var confidence = ReadDouble(prov, "confidence", 0d);
        var evidence = ReadDouble(prov, "evidence_health", 0d);
        var decay = Math.Max(1.0d, ReadDouble(prov, "decay_multiplier", 1.0d));
        var semanticContribution = 0.65d * semantic;
        var freshnessContribution = (0.20d * freshness) + laneBoost;
        var evidenceContribution = 0.15d * evidence;
        var score = (semanticContribution + freshnessContribution + evidenceContribution) / decay;
        return new LessonScore(score, semantic, freshness, evidence, confidence, decay, tier, snapshots, semanticContribution, freshnessContribution, evidenceContribution);
    }

    private IReadOnlyList<ObjectRecord> SelectFallbackSummaries(string query)
    {
        var tokens = Scoring.Tokenize(query);
        return _store.IterateForward(AgentSummaryIndexContainer).Select(x => _store.Objects[x.ObjectId]).Select(x => (obj: x, score: tokens.Count(t => (x.SemanticPayload?.Summary ?? string.Empty).ToLowerInvariant().Contains(t, StringComparison.Ordinal)) + (x.ObjectId == "agent_summary:shared" ? 1 : 0))).OrderByDescending(x => x.score).ThenBy(x => x.obj.ObjectId, StringComparer.Ordinal).Take(5).Select(x => x.obj).ToList();
    }

    private int TouchLessons(IEnumerable<string> seedIds, DateTimeOffset nowUtc, IReadOnlyList<AgentLessonHit> hits)
    {
        var seeds = seedIds.ToHashSet(StringComparer.Ordinal);
        var touched = new HashSet<string>(seeds, StringComparer.Ordinal);
        foreach (var hit in hits)
        {
            var members = $"lesson-stereotype-members:{hit.StereotypeVersionId}";
            if (_store.Containers.ContainsKey(members))
            {
                var ordered = _store.IterateForward(members).Select(x => x.ObjectId).ToList();
                var i = ordered.IndexOf(hit.LessonId);
                if (i > 0) touched.Add(ordered[i - 1]);
                if (i >= 0 && i < ordered.Count - 1) touched.Add(ordered[i + 1]);
            }
        }

        var lessons = _store.Objects.Values.Where(x => x.ObjectKind == "lesson").ToList();
        foreach (var seed in seeds)
        {
            var seedObj = lessons.FirstOrDefault(x => x.ObjectId == seed);
            if (seedObj is null) continue;
            var keys = ReadStringList(seedObj.SemanticPayload?.Provenance, "source_project_keys");
            foreach (var sibling in lessons.Where(x => x.ObjectId != seed && ReadStringList(x.SemanticPayload?.Provenance, "source_project_keys").Any(k => keys.Contains(k, StringComparer.Ordinal))).OrderBy(x => x.ObjectId, StringComparer.Ordinal).Take(3))
                touched.Add(sibling.ObjectId);
        }

        foreach (var id in touched.OrderBy(x => x, StringComparer.Ordinal))
        {
            if (!_store.Objects.TryGetValue(id, out var lesson) || lesson.ObjectKind != "lesson") continue;
            var prov = EnsureProv(lesson);
            var snapshots = ReadSnapshots(prov).ToList();
            var anyMissing = false;
            for (var i = 0; i < snapshots.Count; i++)
            {
                var s = snapshots[i];
                var ok = IsResolvable(s.source_ref);
                if (!ok) anyMissing = true;
                snapshots[i] = s with
                {
                    link_status = ok ? "live" : "missing",
                    missing_since = ok ? null : (s.missing_since ?? nowUtc)
                };
            }
            prov["touch_count"] = JsonSerializer.SerializeToElement(ReadInt(prov, "touch_count", 0) + 1);
            prov["last_touched_at"] = JsonSerializer.SerializeToElement(nowUtc);
            prov["freshness_tier"] = JsonSerializer.SerializeToElement("fresh");
            prov["decay_multiplier"] = JsonSerializer.SerializeToElement(anyMissing ? 2.0d : 1.0d);
            prov["evidence_snapshots"] = JsonSerializer.SerializeToElement(snapshots);
            prov["evidence_health"] = JsonSerializer.SerializeToElement(EvidenceHealth(snapshots));
            MoveToTier(id, "fresh");
        }
        return touched.Count;
    }

    private int ProjectRouteMemory(
        string query,
        DateTimeOffset nowUtc,
        IReadOnlyList<AgentLessonHit> hits,
        IReadOnlyList<AgentShortTermHit> shortTermHits,
        bool weak,
        IReadOnlyList<ObjectRecord> fallback,
        AgentQueryDiagnostics diagnostics,
        AgentQueryContext? context)
    {
        var frame = BuildRouteMemoryFrameFingerprint(context, diagnostics);
        var frameNode = _routeMemory.UpsertFrameNode(frame, updatedAt: nowUtc);

        var route = BuildRouteMemoryRoute(hits, diagnostics);
        var routeNode = _routeMemory.UpsertRouteNode(route, updatedAt: nowUtc);

        var candidateNodeIds = BuildRouteMemoryCandidateNodeIds(hits, shortTermHits);
        var winningTargetRef = hits.FirstOrDefault()?.LessonId
            ?? shortTermHits.FirstOrDefault()?.SourceRef
            ?? fallback.FirstOrDefault()?.ObjectId;
        var winningNodeId = ResolveRouteMemoryWinningNodeId(hits, shortTermHits);
        var topTargetRefs = hits.Select(x => x.LessonId)
            .Concat(shortTermHits.Select(x => x.SourceRef))
            .Concat(fallback.Select(x => x.ObjectId))
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Take(5)
            .ToList();

        _routeMemory.RecordEpisode(
            new RetrievalEpisodeRecord(
                QueryText: query,
                OccurredAt: nowUtc,
                WeakResult: weak,
                UsedFallback: weak && fallback.Count > 0,
                WinningTargetRef: winningTargetRef,
                TopTargetRefs: topTargetRefs),
            frameNode.NodeId,
            routeNode.NodeId,
            candidateNodeIds,
            winningNodeId);

        return 1;
    }

    private static RetrievalFrameFingerprint BuildRouteMemoryFrameFingerprint(AgentQueryContext? context, AgentQueryDiagnostics diagnostics)
        => BuildRouteMemoryFrameFingerprint(context, diagnostics.ScopeLens);

    private static RetrievalFrameFingerprint BuildRouteMemoryFrameFingerprint(AgentQueryContext? context, string scopeLens)
    {
        var lineage = context?.Lineage
            .Select(x => x.NodeId)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();
        var artifacts = (context?.Lineage.SelectMany(x => x.ArtifactRefs) ?? Enumerable.Empty<string>())
            .Concat(context?.ActiveArtifacts ?? [])
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        return new RetrievalFrameFingerprint(
            scopeLens,
            context?.AgentRole,
            context?.Mode,
            context?.FailureBucket,
            lineage,
            artifacts);
    }

    private static RetrievalRouteDescriptor BuildRouteMemoryRoute(IReadOnlyList<AgentLessonHit> hits, AgentQueryDiagnostics diagnostics)
    {
        var explain = hits.FirstOrDefault()?.Explain;
        if (explain is not null)
        {
            return new RetrievalRouteDescriptor(
                explain.RankingSource,
                explain.Path,
                explain.RouteCost,
                explain.RiskFlags);
        }

        var fallbackPath = string.IsNullOrWhiteSpace(diagnostics.RoutingDecision)
            ? diagnostics.ScoringLane
            : $"{diagnostics.ScoringLane} -> {diagnostics.RoutingDecision}";
        return new RetrievalRouteDescriptor(
            diagnostics.ScoringLane,
            fallbackPath,
            0d,
            diagnostics.RoutingFlags);
    }

    private IReadOnlyList<Guid> BuildRouteMemoryCandidateNodeIds(IReadOnlyList<AgentLessonHit> hits, IReadOnlyList<AgentShortTermHit> shortTermHits)
    {
        var nodeIds = new List<Guid>();
        foreach (var hit in hits)
        {
            var nodeId = TryGetRetrievalNodeIdByRef(hit.LessonId);
            if (nodeId.HasValue)
                nodeIds.Add(nodeId.Value);
        }

        foreach (var hit in shortTermHits)
        {
            var nodeId = TryGetRetrievalNodeIdByRef(hit.SourceRef);
            if (nodeId.HasValue)
                nodeIds.Add(nodeId.Value);
        }

        return nodeIds.Distinct().ToList();
    }

    private Guid? ResolveRouteMemoryWinningNodeId(IReadOnlyList<AgentLessonHit> hits, IReadOnlyList<AgentShortTermHit> shortTermHits)
    {
        var lessonWinner = hits.FirstOrDefault();
        if (lessonWinner is not null)
            return TryGetRetrievalNodeIdByRef(lessonWinner.LessonId);

        var shortTermWinner = shortTermHits.FirstOrDefault();
        if (shortTermWinner is not null)
            return TryGetRetrievalNodeIdByRef(shortTermWinner.SourceRef);

        return null;
    }

    private string RenderMarkdown(
        string query,
        IReadOnlyList<AgentLessonHit> hits,
        IReadOnlyList<AgentShortTermHit> shortTermHits,
        bool weak,
        IReadOnlyList<ObjectRecord> fallback,
        AgentQueryContext? context)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# AGENT MEMORY");
        sb.AppendLine();
        sb.AppendLine($"Query: {query}");
        if (context is not null && (context.HasLineage || !string.IsNullOrWhiteSpace(context.FailureBucket)))
        {
            sb.AppendLine();
            sb.AppendLine("## Context");
            sb.AppendLine($"- scope_lens: {ScopeLens(context)}");
            sb.AppendLine($"- source: {context.Source}");
            if (!string.IsNullOrWhiteSpace(context.AgentRole) || !string.IsNullOrWhiteSpace(context.Mode))
                sb.AppendLine($"- agent: role={context.AgentRole} mode={context.Mode}");
            if (!string.IsNullOrWhiteSpace(context.FailureBucket))
                sb.AppendLine($"- failure_bucket: {context.FailureBucket}");
            foreach (var scope in context.Lineage)
            {
                sb.AppendLine($"- {scope.Level}: {scope.NodeId} | {scope.Title}");
                if (!string.IsNullOrWhiteSpace(scope.CurrentStep))
                    sb.AppendLine($"  step: {scope.CurrentStep}");
                if (!string.IsNullOrWhiteSpace(scope.NextCommand))
                    sb.AppendLine($"  next: {scope.NextCommand}");
            }
        }
        sb.AppendLine();
        sb.AppendLine("## Lessons");
        if (hits.Count == 0) sb.AppendLine("- No lesson hits.");
        for (var i = 0; i < hits.Count; i++)
        {
            var h = hits[i];
            sb.AppendLine($"{i + 1}. {h.Title} (score={h.Score:0.00}, confidence={h.Confidence:0.00}, evidence={h.EvidenceHealth:0.00}, tier={h.FreshnessTier})");
            sb.AppendLine($"- stereotype: family={h.StereotypeFamilyId} version={h.StereotypeVersionId}");
            foreach (var ev in h.EvidenceSnapshots.Take(3))
                sb.AppendLine($"- evidence [{ev.link_status}]: {ev.snippet} (ref={ev.source_ref})");
        }
        if (shortTermHits.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine("## Short-Term Memory");
            for (var i = 0; i < shortTermHits.Count; i++)
            {
                var hit = shortTermHits[i];
                var timestamp = hit.Timestamp is null ? string.Empty : $" @ {hit.Timestamp:yyyy-MM-dd HH:mm}";
                sb.AppendLine($"{i + 1}. [{hit.SourceKind}] {hit.SessionTitle} (score={hit.Score:0.00}, recency={hit.Recency:0.00}, matched={hit.MatchedTokens.Count}){timestamp}");
                sb.AppendLine($"- snippet: {hit.Snippet}");
                sb.AppendLine($"- ref: {hit.SourceRef} (session={hit.SessionRef})");
            }
        }
        if (weak)
        {
            sb.AppendLine();
            sb.AppendLine("## Cross-Agent Summaries (fallback)");
            if (fallback.Count == 0) sb.AppendLine("- No fallback summaries available.");
            foreach (var summary in fallback)
            {
                sb.AppendLine($"- {summary.ObjectId}");
                if (!string.IsNullOrWhiteSpace(summary.SemanticPayload?.Summary))
                    sb.AppendLine($"  {summary.SemanticPayload.Summary.Replace(Environment.NewLine, " ")}");
            }
        }
        return sb.ToString().TrimEnd();
    }

    private IReadOnlyList<AgentShortTermHit> SelectShortTermHits(
        string query,
        int top,
        DateTimeOffset nowUtc,
        AgentQueryContext? context,
        IReadOnlyDictionary<Guid, ContextRoute>? graphRoutes,
        IReadOnlyDictionary<string, FreshnessObjectPosition> freshnessPositions,
        ISet<string> excludedObjectIds)
    {
        var tokens = Scoring.Tokenize(query);
        if (tokens.Count == 0)
            return [];

        var sessionMap = BuildSessionMessageIndex();
        var turnHits = _store.Objects.Values
            .Where(x => x.ObjectKind == "chat_message")
            .Select(msg =>
            {
                var provenance = msg.SemanticPayload?.Provenance;
                var text = ReadString(provenance, "text") ?? msg.SemanticPayload?.Summary ?? string.Empty;
                if (string.IsNullOrWhiteSpace(text))
                    return null;

                var matchedTokens = MatchTokens(text, tokens);
                if (matchedTokens.Count == 0)
                    return null;

                if (!sessionMap.TryGetValue(msg.ObjectId, out var sessionInfo))
                {
                    sessionInfo = (
                        SessionRef: string.Empty,
                        SessionTitle: "Current session",
                        Timestamp: ReadDate(provenance, "ts"));
                }

                var effectiveTs = ReadDate(provenance, "ts") ?? sessionInfo.Timestamp;
                var recency = ShortTermRecencyScore(nowUtc, effectiveTs);
                var semantic = (double)matchedTokens.Count / Math.Max(1, tokens.Count);
                var contextRoute = ResolveContextRoute(
                    context,
                    sessionInfo.SessionTitle,
                    [text],
                    BuildObjectAnchors(msg, sessionInfo.SessionRef),
                    BuildCandidateRetrievalNodeIds(msg, sessionInfo.SessionRef),
                    graphRoutes);
                var score = (semantic * 0.85d) + (recency * 0.15d) + contextRoute.Boost;

                return new AgentShortTermHit(
                    "turn",
                    msg.ObjectId,
                    sessionInfo.SessionRef,
                    sessionInfo.SessionTitle,
                    TruncateSnippet(text, 220),
                    score,
                    recency,
                    matchedTokens,
                    effectiveTs,
                    $"short-term:turn -> {msg.ObjectId} -> {contextRoute.Label}");
            })
            .Where(x => x is not null)
            .Select(x => x!)
            .OrderByDescending(x => x.Score)
            .ThenByDescending(x => x.Timestamp ?? DateTimeOffset.MinValue)
            .ThenBy(x => x.SourceRef, StringComparer.Ordinal)
            .ToList();

        var manualHits = SelectManualSmartListHits(tokens, nowUtc, context, graphRoutes);
        var taskGraphHits = SelectTaskGraphHits(tokens, nowUtc, context, graphRoutes);
        var sessionHits = _store.Containers.Values
            .Where(x => x.ContainerKind == "chat_session")
            .Select(session =>
            {
                var title = ReadString(session.Metadata, "title");
                if (string.IsNullOrWhiteSpace(title))
                    title = session.ContainerId;

                var matchedTokens = MatchTokens(title, tokens);
                if (matchedTokens.Count == 0)
                    return null;

                var ts = ReadDate(session.Metadata, "ended_at") ?? ReadDate(session.Metadata, "started_at");
                var recency = ShortTermRecencyScore(nowUtc, ts);
                var semantic = (double)matchedTokens.Count / Math.Max(1, tokens.Count);
                var contextRoute = ResolveContextRoute(
                    context,
                    title,
                    [ResolveSnippet(session.ContainerId)],
                    BuildContainerAnchors(session),
                    BuildCandidateRetrievalNodeIds(session),
                    graphRoutes);
                var score = (semantic * 0.80d) + (recency * 0.20d) + contextRoute.Boost;

                return new AgentShortTermHit(
                    "session",
                    session.ContainerId,
                    session.ContainerId,
                    title,
                    TruncateSnippet(ResolveSnippet(session.ContainerId), 220),
                    score,
                    recency,
                    matchedTokens,
                    ts,
                    $"short-term:session -> {session.ContainerId} -> {contextRoute.Label}");
            })
            .Where(x => x is not null)
            .Select(x => x!)
            .OrderByDescending(x => x.Score)
            .ThenByDescending(x => x.Timestamp ?? DateTimeOffset.MinValue)
            .ThenBy(x => x.SourceRef, StringComparer.Ordinal)
            .ToList();

        var combined = turnHits
            .Concat(taskGraphHits)
            .Concat(manualHits)
            .Concat(sessionHits)
            .Where(x => !excludedObjectIds.Contains(x.SourceRef))
            .GroupBy(x => x.SourceRef, StringComparer.Ordinal)
            .Select(group => group
                .OrderByDescending(x => x.Score)
                .ThenByDescending(x => x.Timestamp ?? DateTimeOffset.MinValue)
                .ThenBy(x => x.SourceRef, StringComparer.Ordinal)
                .First())
            .ToList();

        var freshnessHits = combined
            .Select(hit =>
            {
                if (!freshnessPositions.TryGetValue(hit.SourceRef, out var position))
                    return null;

                return new
                {
                    hit = hit with
                    {
                        Score = hit.Score + FreshnessLaneBoost(position),
                        Path = $"{hit.Path} -> freshness:{position.TemperatureLabel}"
                    },
                    position
                };
            })
            .Where(x => x is not null)
            .Select(x => x!)
            .OrderBy(x => x.position.LaneIndex)
            .ThenByDescending(x => x.hit.Score)
            .ThenByDescending(x => x.hit.Timestamp ?? DateTimeOffset.MinValue)
            .ThenBy(x => x.hit.SourceRef, StringComparer.Ordinal)
            .Select(x => x.hit)
            .ToList();

        var freshnessRefs = freshnessHits.Select(x => x.SourceRef).ToHashSet(StringComparer.Ordinal);
        var fallbackHits = combined
            .Where(x => !freshnessRefs.Contains(x.SourceRef))
            .OrderByDescending(x => x.Score)
            .ThenByDescending(x => x.Timestamp ?? DateTimeOffset.MinValue)
            .ThenBy(x => x.SourceRef, StringComparer.Ordinal)
            .ToList();

        return freshnessHits
            .Concat(fallbackHits)
            .Take(top)
            .ToList();
    }

    private IReadOnlyList<AgentShortTermHit> SelectManualSmartListHits(
        IReadOnlyList<string> tokens,
        DateTimeOffset nowUtc,
        AgentQueryContext? context,
        IReadOnlyDictionary<Guid, ContextRoute>? graphRoutes)
    {
        return _store.Objects.Values
            .Where(x => x.ObjectKind == SmartListService.BucketObjectKind || x.ObjectKind == SmartListService.NoteObjectKind)
            .Where(x => IsSmartListVisibleForQuery(x, context))
            .Select(obj =>
            {
                var provenance = obj.SemanticPayload?.Provenance;
                var title = obj.ObjectKind == SmartListService.BucketObjectKind
                    ? ReadString(provenance, "path") ?? obj.SemanticPayload?.Summary ?? obj.ObjectId
                    : ReadString(provenance, "title") ?? obj.SemanticPayload?.Summary ?? obj.ObjectId;
                var snippet = BuildManualSmartListSnippet(obj);
                var haystack = $"{title}\n{snippet}";
                var matchedTokens = MatchTokens(haystack, tokens);
                if (matchedTokens.Count == 0)
                    return null;

                var ts = ReadDate(provenance, "updated_at") ?? ReadDate(provenance, "created_at") ?? obj.UpdatedAt;
                var recency = ShortTermRecencyScore(nowUtc, ts);
                var semantic = (double)matchedTokens.Count / Math.Max(1, tokens.Count);
                var contextRoute = ResolveContextRoute(
                    context,
                    title,
                    [snippet],
                    BuildObjectAnchors(obj),
                    BuildCandidateRetrievalNodeIds(obj),
                    graphRoutes);
                var score = (semantic * 0.80d) + (recency * 0.20d) + contextRoute.Boost;
                var sourceKind = obj.ObjectKind == SmartListService.BucketObjectKind ? "smartlist_bucket" : "smartlist_note";

                return new AgentShortTermHit(
                    sourceKind,
                    obj.ObjectId,
                    ReadString(provenance, "path") ?? obj.ObjectId,
                    title,
                    TruncateSnippet(snippet, 220),
                    score,
                    recency,
                    matchedTokens,
                    ts,
                    $"short-term:{sourceKind} -> {obj.ObjectId} -> {contextRoute.Label}");
            })
            .Where(x => x is not null)
            .Select(x => x!)
            .OrderByDescending(x => x.Score)
            .ThenByDescending(x => x.Timestamp ?? DateTimeOffset.MinValue)
            .ThenBy(x => x.SourceRef, StringComparer.Ordinal)
            .ToList();
    }

    private bool IsSmartListVisibleForQuery(ObjectRecord obj, AgentQueryContext? context)
    {
        if (obj.ObjectKind == SmartListService.BucketObjectKind && IsFreshnessInternalPath(ReadString(obj.SemanticPayload?.Provenance, "path")))
            return false;
        if (obj.ObjectKind == SmartListService.RollupObjectKind && IsFreshnessInternalPath(ReadString(obj.SemanticPayload?.Provenance, "bucket_path")))
            return false;

        var visibility = SmartListService.ReadRetrievalVisibility(obj.SemanticPayload?.Provenance);
        return visibility switch
        {
            SmartListService.RetrievalVisibilityDefault => true,
            SmartListService.RetrievalVisibilityScoped => !HasActiveSmartListScope(context) || IsSmartListInActiveScope(obj, context),
            SmartListService.RetrievalVisibilitySuppressed => false,
            _ => true
        };
    }

    private static bool HasActiveSmartListScope(AgentQueryContext? context)
    {
        if (context is null)
            return false;

        return context.Lineage
            .Select(x => x.BranchOffAnchor)
            .Append(context.FailureBucket)
            .Any(IsSmartListPath);
    }

    private bool IsSmartListInActiveScope(ObjectRecord obj, AgentQueryContext? context)
    {
        if (context is null)
            return false;

        var objectPaths = GetSmartListScopePaths(obj);
        if (objectPaths.Count == 0)
            return false;

        var scopePaths = context.Lineage
            .Select(x => x.BranchOffAnchor)
            .Append(context.FailureBucket)
            .Where(IsSmartListPath)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        return objectPaths.Any(path => scopePaths.Any(scopePath => SmartListPathsOverlap(path, scopePath!)));
    }

    private IReadOnlyList<string> GetSmartListScopePaths(ObjectRecord obj)
    {
        var prov = obj.SemanticPayload?.Provenance;
        var paths = obj.ObjectKind switch
        {
            var kind when kind == SmartListService.BucketObjectKind =>
                ReadString(prov, "path") is string path && !string.IsNullOrWhiteSpace(path) ? [path] : [],
            var kind when kind == SmartListService.RollupObjectKind =>
                ReadString(prov, "bucket_path") is string bucketPath && !string.IsNullOrWhiteSpace(bucketPath) ? [bucketPath] : [],
            var kind when kind == SmartListService.NoteObjectKind =>
                _store.ContainersForMemberObject(obj.ObjectId)
                    .Where(x => x.StartsWith("smartlist-members:", StringComparison.Ordinal))
                    .Select(x => x["smartlist-members:".Length..])
                    .Distinct(StringComparer.Ordinal)
                    .ToList(),
            _ => []
        };

        return paths
            .Where(path => !IsFreshnessInternalPath(path))
            .ToList();
    }

    private static bool SmartListPathsOverlap(string left, string right)
    {
        if (string.Equals(left, right, StringComparison.Ordinal))
            return true;

        return left.StartsWith(right + "/", StringComparison.Ordinal)
            || right.StartsWith(left + "/", StringComparison.Ordinal);
    }

    private IReadOnlyList<AgentShortTermHit> SelectTaskGraphHits(
        IReadOnlyList<string> tokens,
        DateTimeOffset nowUtc,
        AgentQueryContext? context,
        IReadOnlyDictionary<Guid, ContextRoute>? graphRoutes)
    {
        var taskObjects = (context is not null && context.HasLineage
                ? context.Lineage.SelectMany(scope => EnumerateTaskGraphObjects(scope.ObjectId)).Distinct(StringComparer.Ordinal)
                : _store.Objects.Values
                    .Where(x => x.ObjectKind is "task_thread" or "task_checkpoint" or "task_artifact")
                    .Select(x => x.ObjectId))
            .Where(id => _store.Objects.ContainsKey(id))
            .Select(id => _store.Objects[id])
            .ToList();

        return taskObjects
            .Select(obj =>
            {
                var provenance = obj.SemanticPayload?.Provenance;
                var title = obj.SemanticPayload?.Summary ?? obj.ObjectId;
                var threadId = ReadString(provenance, "thread_id") ?? Suffix(obj.ObjectId);
                var snippet = obj.ObjectKind switch
                {
                    "task_thread" => string.Join(" | ", new[] { ReadString(provenance, "current_step"), ReadString(provenance, "next_command") }.Where(x => !string.IsNullOrWhiteSpace(x))),
                    "task_checkpoint" => $"{ReadString(provenance, "current_step") ?? title} | next: {ReadString(provenance, "next_command") ?? string.Empty}",
                    "task_artifact" => ReadString(provenance, "artifact_ref") ?? title,
                    _ => title
                };
                var haystack = $"{title}\n{snippet}\n{threadId}\n{obj.ObjectId}";
                var matchedTokens = MatchTokens(haystack, tokens);
                if (matchedTokens.Count == 0)
                    return null;

                var ts = ReadDate(provenance, "updated_at") ?? ReadDate(provenance, "created_at") ?? obj.UpdatedAt;
                var recency = ShortTermRecencyScore(nowUtc, ts);
                var semantic = (double)matchedTokens.Count / Math.Max(1, tokens.Count);
                var contextRoute = ResolveContextRoute(
                    context,
                    title,
                    [snippet, threadId, obj.ObjectId],
                    BuildObjectAnchors(obj),
                    BuildCandidateRetrievalNodeIds(obj),
                    graphRoutes);
                var score = (semantic * 0.70d) + (recency * 0.10d) + 0.10d + contextRoute.Boost;

                return new AgentShortTermHit(
                    obj.ObjectKind,
                    obj.ObjectId,
                    threadId,
                    title,
                    TruncateSnippet(snippet, 220),
                    score,
                    recency,
                    matchedTokens,
                    ts,
                    $"short-term:{obj.ObjectKind} -> {obj.ObjectId} -> {contextRoute.Label}");
            })
            .Where(x => x is not null)
            .Select(x => x!)
            .OrderByDescending(x => x.Score)
            .ThenByDescending(x => x.Timestamp ?? DateTimeOffset.MinValue)
            .ThenBy(x => x.SourceRef, StringComparer.Ordinal)
            .ToList();
    }

    private IEnumerable<string> EnumerateTaskGraphObjects(string threadObjectId)
    {
        yield return threadObjectId;

        if (!_store.Objects.TryGetValue(threadObjectId, out var obj) || obj.ObjectKind != "task_thread")
            yield break;

        var threadId = ReadString(obj.SemanticPayload?.Provenance, "thread_id") ?? Suffix(threadObjectId);
        var checkpointsContainerId = $"task-thread:{threadId}:checkpoints";
        if (_store.Containers.ContainsKey(checkpointsContainerId))
        {
            foreach (var checkpoint in _store.IterateForward(checkpointsContainerId).Select(x => x.ObjectId))
                yield return checkpoint;
        }

        var artifactsContainerId = $"task-thread:{threadId}:artifacts";
        if (_store.Containers.ContainsKey(artifactsContainerId))
        {
            foreach (var artifact in _store.IterateForward(artifactsContainerId).Select(x => x.ObjectId))
                yield return artifact;
        }
    }

    private Dictionary<string, (string SessionRef, string SessionTitle, DateTimeOffset? Timestamp)> BuildSessionMessageIndex()
    {
        var map = new Dictionary<string, (string SessionRef, string SessionTitle, DateTimeOffset? Timestamp)>(StringComparer.Ordinal);

        foreach (var session in _store.Containers.Values.Where(x => x.ContainerKind == "chat_session"))
        {
            var title = ReadString(session.Metadata, "title");
            if (string.IsNullOrWhiteSpace(title))
                title = session.ContainerId;
            var ts = ReadDate(session.Metadata, "ended_at") ?? ReadDate(session.Metadata, "started_at");

            foreach (var member in _store.IterateForward(session.ContainerId))
            {
                if (!_store.Objects.TryGetValue(member.ObjectId, out var obj) || obj.ObjectKind != "chat_message")
                    continue;

                map[obj.ObjectId] = (session.ContainerId, title, ts);
            }
        }

        return map;
    }

    private List<EvidenceSnapshot> BuildSnapshots(ObjectRecord dream, DateTimeOffset nowUtc)
    {
        var list = new List<EvidenceSnapshot>();
        var memberContainer = $"{dream.ObjectKind}-members:{Suffix(dream.ObjectId)}";
        if (_store.Containers.ContainsKey(memberContainer))
            list.AddRange(_store.IterateForward(memberContainer).Select(x => SnapshotFromRef(SourceRef(x.ObjectId), nowUtc)));
        if (dream.SemanticPayload?.Provenance?.TryGetValue("evidence", out var ev) == true && ev.ValueKind == JsonValueKind.Array)
            list.AddRange(ev.EnumerateArray().Where(x => x.ValueKind == JsonValueKind.String).Select(x => SnapshotFromRef(SourceRef(x.GetString() ?? string.Empty), nowUtc)));
        return list.Where(x => !string.IsNullOrWhiteSpace(x.source_ref)).GroupBy(x => x.source_ref, StringComparer.Ordinal).Select(x => x.First()).OrderBy(x => x.source_ref, StringComparer.Ordinal).Take(3).ToList();
    }

    private EvidenceSnapshot SnapshotFromRef(string sourceRef, DateTimeOffset nowUtc)
    {
        var snippet = ResolveSnippet(sourceRef);
        if (snippet.Length > 220) snippet = snippet[..220].TrimEnd() + "...";
        var live = IsResolvable(sourceRef);
        return new EvidenceSnapshot(Kind(sourceRef), Suffix(sourceRef), sourceRef, snippet, Hash8(snippet), nowUtc, live ? "live" : "missing", live ? null : nowUtc);
    }

    private static (string Title, string Quality) BuildLessonTitle(ObjectRecord dream, IReadOnlyList<EvidenceSnapshot> snapshots)
    {
        var kindPrefix = char.ToUpperInvariant(dream.ObjectKind.FirstOrDefault()) + dream.ObjectKind[1..];

        if (TryBuildPhrase(dream.SemanticPayload?.Summary, out var fromSummary))
            return ($"{kindPrefix}: {fromSummary}", "high");

        foreach (var snapshot in snapshots)
        {
            if (TryBuildPhrase(snapshot.snippet, out var fromSnippet))
                return ($"{kindPrefix}: {fromSnippet}", "fallback");
        }

        var aggregate = string.Join(' ', snapshots.Select(x => x.snippet));
        if (TryBuildPhrase(aggregate, out var fromEvidence))
            return ($"{kindPrefix}: {fromEvidence}", "fallback");

        var suffix = Suffix(dream.ObjectId);
        var shortSuffix = suffix.Length > 8 ? suffix[..8] : suffix;
        return ($"{kindPrefix}: {shortSuffix}", "invalid");
    }

    private static bool TryBuildPhrase(string? text, out string phrase)
    {
        phrase = string.Empty;
        if (string.IsNullOrWhiteSpace(text))
            return false;

        var cleaned = text.Replace("User:", string.Empty, StringComparison.OrdinalIgnoreCase)
            .Replace("Claude:", string.Empty, StringComparison.OrdinalIgnoreCase)
            .Replace("Implement the following plan", string.Empty, StringComparison.OrdinalIgnoreCase)
            .Replace('#', ' ')
            .Replace('*', ' ');
        cleaned = Regex.Replace(cleaned, "\\s+", " ").Trim();
        if (cleaned.Length == 0)
            return false;

        var tokens = TokenRx.Matches(cleaned)
            .Select(x => x.Value.ToLowerInvariant())
            .Where(x => !TitleStopwords.Contains(x))
            .Where(x => !TitleNoise.Contains(x))
            .Where(x => !IsLikelyIdentifierToken(x))
            .Take(8)
            .ToList();

        if (tokens.Count < 2)
            return false;

        phrase = string.Join(' ', tokens);
        if (phrase.Length > 72)
            phrase = phrase[..72].TrimEnd();
        return !string.IsNullOrWhiteSpace(phrase);
    }

    private static bool IsLikelyIdentifierToken(string token)
    {
        if (string.IsNullOrWhiteSpace(token))
            return true;

        if (token.All(char.IsDigit))
            return true;

        var hasDigit = token.Any(char.IsDigit);
        if (!hasDigit)
            return false;

        var isHex = token.All(c => (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
        if (isHex)
            return true;

        var digitCount = token.Count(char.IsDigit);
        return token.Length >= 8 && ((double)digitCount / token.Length) >= 0.45d;
    }

    private static string SemanticKindFromLesson(ObjectRecord lesson)
    {
        var kind = ReadString(lesson.SemanticPayload?.Provenance, "origin_kind");
        if (!string.IsNullOrWhiteSpace(kind))
            return kind!;

        if (lesson.ObjectId.StartsWith("lesson:", StringComparison.Ordinal))
        {
            var rest = lesson.ObjectId["lesson:".Length..];
            var idx = rest.IndexOf(':', StringComparison.Ordinal);
            if (idx > 0)
                return rest[..idx];
        }

        return "topic";
    }

    private static string BuildSemanticClusterLabel(string? lessonTitle, string kind, string fallbackId)
    {
        var title = lessonTitle ?? string.Empty;
        var capKind = Capitalize(kind) + ":";
        if (title.StartsWith(capKind, StringComparison.OrdinalIgnoreCase))
            title = title[capKind.Length..];

        var cleaned = Regex.Replace(title, "[^a-zA-Z0-9 ]+", " ");
        cleaned = Regex.Replace(cleaned, "\\s+", " ").Trim();

        var tokens = TokenRx.Matches(cleaned)
            .Select(x => x.Value.ToLowerInvariant())
            .Where(x => !TitleStopwords.Contains(x))
            .Where(x => !TitleNoise.Contains(x))
            .Where(x => !IsLikelyIdentifierToken(x))
            .Take(6)
            .ToList();

        if (tokens.Count < 2)
        {
            tokens = TokenRx.Matches(cleaned)
                .Select(x => x.Value.ToLowerInvariant())
                .Where(x => !IsLikelyIdentifierToken(x))
                .Take(4)
                .ToList();
        }

        if (tokens.Count == 0)
            return $"{kind} memory {Hash8(fallbackId)}";

        return string.Join(' ', tokens);
    }

    private static string BuildSemanticSlug(string label, string fallbackSeed)
    {
        var tokens = TokenRx.Matches(label)
            .Select(x => x.Value.ToLowerInvariant())
            .Where(x => !IsLikelyIdentifierToken(x))
            .Take(6)
            .ToList();

        var slug = string.Join('-', tokens);
        if (string.IsNullOrWhiteSpace(slug))
            slug = Hash8(fallbackSeed);

        if (slug.Length > 64)
            slug = slug[..64].Trim('-');

        return string.IsNullOrWhiteSpace(slug) ? Hash8(fallbackSeed) : slug;
    }

    private static string BuildSemanticThemeLabel(string nodeLabel, string kind)
    {
        var tokens = TokenRx.Matches(nodeLabel ?? string.Empty)
            .Select(x => x.Value.ToLowerInvariant())
            .Where(x => !TitleStopwords.Contains(x))
            .Where(x => !TitleNoise.Contains(x))
            .Where(x => !IsLikelyIdentifierToken(x))
            .Take(3)
            .ToList();

        if (tokens.Count == 0)
            return $"{kind} theme";

        return string.Join(' ', tokens);
    }

    private static string Capitalize(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return value;
        return char.ToUpperInvariant(value[0]) + value[1..];
    }

    private static bool IsWeakSemanticHit(AgentLessonHit hit, int queryTokenCount)
    {
        if (hit.Explain is null || !string.Equals(hit.Explain.RankingSource, "semantic-node-first", StringComparison.Ordinal))
            return false;

        if (hit.Explain.RepresentativeMismatch)
            return true;

        if (hit.Explain.MatchedTokens.Count <= Math.Max(1, queryTokenCount / 4))
            return true;

        return hit.Explain.RiskFlags.Any(x => string.Equals(x, "broad-node", StringComparison.Ordinal));
    }

    private static AgentTokenMatchLocations BuildTokenMatchLocations(ObjectRecord? node, ObjectRecord lesson, IReadOnlyList<EvidenceSnapshot> snapshots, IReadOnlyList<string> tokens)
    {
        var nodeText = node?.SemanticPayload?.Summary ?? string.Empty;
        var lessonText = lesson.SemanticPayload?.Summary ?? string.Empty;
        var evidenceText = string.Join('\n', snapshots.Select(x => $"{x.snippet}\n{x.source_ref}"));

        return new AgentTokenMatchLocations(
            MatchTokens(nodeText, tokens),
            MatchTokens(lessonText, tokens),
            MatchTokens(evidenceText, tokens));
    }

    private static IReadOnlyList<string> MatchTokens(string haystack, IReadOnlyList<string> tokens)
    {
        if (string.IsNullOrWhiteSpace(haystack) || tokens.Count == 0)
            return [];

        return tokens
            .Where(t => haystack.Contains(t, StringComparison.OrdinalIgnoreCase))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();
    }

    private static IReadOnlyList<string> MatchTokens(AgentTokenMatchLocations locations)
    {
        return locations.NodeSummaryTokens
            .Concat(locations.LessonSummaryTokens)
            .Concat(locations.EvidenceSnippetTokens)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();
    }

    private static bool TitlesMateriallyDiffer(string? nodeTitle, string? lessonTitle)
    {
        var nodeTokens = NormalizeTitleTokens(nodeTitle);
        var lessonTokens = NormalizeTitleTokens(lessonTitle);
        if (nodeTokens.Count == 0 || lessonTokens.Count == 0)
            return false;

        if (nodeTokens.SetEquals(lessonTokens))
            return false;

        var overlap = nodeTokens.Intersect(lessonTokens, StringComparer.Ordinal).Count();
        return overlap < Math.Min(nodeTokens.Count, lessonTokens.Count);
    }

    private static HashSet<string> NormalizeTitleTokens(string? title)
    {
        if (string.IsNullOrWhiteSpace(title))
            return new HashSet<string>(StringComparer.Ordinal);

        var cleaned = Regex.Replace(title, "[^a-zA-Z0-9 ]+", " ");
        return TokenRx.Matches(cleaned)
            .Select(x => x.Value.ToLowerInvariant())
            .Where(x => !TitleStopwords.Contains(x))
            .Where(x => !TitleNoise.Contains(x))
            .Where(x => !IsLikelyIdentifierToken(x))
            .ToHashSet(StringComparer.Ordinal);
    }

    private static string BuildWhyWonSentence(string source, int matchedTokenCount, int queryTokenCount, bool representativeMismatch, IReadOnlyList<string> riskFlags)
    {
        var noun = source == "semantic-node-first" ? "semantic node" : "lesson";
        var matched = $"{matchedTokenCount}/{queryTokenCount} query tokens";
        if (riskFlags.Count == 0)
            return $"{noun} matched {matched} without routing risk flags.";

        var riskText = string.Join(", ", riskFlags);
        if (representativeMismatch)
            return $"{noun} matched {matched}, but the representative title differs from the best matching lesson ({riskText}).";

        return $"{noun} matched {matched}; diagnostics flagged {riskText}.";
    }

    private sealed class SemanticNodeGroup
    {
        public string Kind { get; }
        public string Label { get; }
        public HashSet<string> LessonIds { get; } = new(StringComparer.Ordinal);
        public HashSet<string> SourceContainerIds { get; } = new(StringComparer.Ordinal);

        public SemanticNodeGroup(string kind, string label)
        {
            Kind = kind;
            Label = label;
        }
    }

    private sealed class SemanticThemeGroup
    {
        public string Kind { get; }
        public string Label { get; }
        public HashSet<string> NodeContainerIds { get; } = new(StringComparer.Ordinal);

        public SemanticThemeGroup(string kind, string label)
        {
            Kind = kind;
            Label = label;
        }
    }

    private sealed record GraphAnchor(string Type, string Value, double SeedCost = 0d);
    private sealed record ContextRoute(
        string Label,
        double Boost,
        double Cost = double.PositiveInfinity,
        double RouteMemoryBias = 0d);
    private sealed record GraphSeed(Guid NodeId, string Label, double SeedCost);
    private sealed record GraphNeighbor(Guid NodeId, double StepCost, string StepLabel);

    private AgentQueryContext? TryBuildDefaultContext()
    {
        if (!_store.Containers.ContainsKey("task-graph:active"))
            return null;

        var activeObjectId = _store.IterateForward("task-graph:active")
            .Select(x => x.ObjectId)
            .FirstOrDefault();
        if (string.IsNullOrWhiteSpace(activeObjectId))
            return null;

        var lineage = new List<AgentLineageScope>();
        var currentObjectId = activeObjectId;
        var levelNames = new[] { "self", "parent", "grandparent" };
        for (var i = 0; i < levelNames.Length && !string.IsNullOrWhiteSpace(currentObjectId); i++)
        {
            var scope = TryBuildLineageScope(currentObjectId!, levelNames[i]);
            if (scope is null)
                break;

            lineage.Add(scope);
            currentObjectId = string.IsNullOrWhiteSpace(scope.BranchOffAnchor)
                ? ResolveParentThreadObjectId(scope.ObjectId)
                : ResolveParentThreadObjectId(scope.ObjectId);
        }

        if (lineage.Count == 0)
            return null;

        var artifacts = lineage
            .SelectMany(x => x.ArtifactRefs)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .Take(8)
            .ToList();

        return new AgentQueryContext(
            lineage,
            "implementer",
            "build",
            null,
            artifacts,
            3,
            "active-task-graph");
    }

    private AgentLineageScope? TryBuildLineageScope(string objectId, string level)
    {
        if (!_store.Objects.TryGetValue(objectId, out var obj) || obj.ObjectKind != "task_thread")
            return null;

        var prov = obj.SemanticPayload?.Provenance;
        var nodeId = ReadString(prov, "thread_id") ?? Suffix(objectId);
        return new AgentLineageScope(
            level,
            objectId,
            nodeId,
            obj.SemanticPayload?.Summary ?? nodeId,
            ReadString(prov, "current_step") ?? string.Empty,
            ReadString(prov, "next_command") ?? string.Empty,
            EmptyToNull(ReadString(prov, "branch_off_anchor")),
            ReadArtifactRefs(nodeId));
    }

    private string? ResolveParentThreadObjectId(string threadObjectId)
    {
        if (!_store.Objects.TryGetValue(threadObjectId, out var obj))
            return null;

        var parentThreadId = EmptyToNull(ReadString(obj.SemanticPayload?.Provenance, "parent_thread_id"));
        return string.IsNullOrWhiteSpace(parentThreadId) ? null : $"task-thread:{parentThreadId}";
    }

    private IReadOnlyList<string> ReadArtifactRefs(string threadId)
    {
        var containerId = $"task-thread:{threadId}:artifacts";
        if (!_store.Containers.ContainsKey(containerId))
            return [];

        return _store.IterateForward(containerId)
            .Select(x => x.ObjectId)
            .Where(x => _store.Objects.ContainsKey(x))
            .Select(x => ReadString(_store.Objects[x].SemanticPayload?.Provenance, "artifact_ref") ?? string.Empty)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();
    }

    private IReadOnlyDictionary<Guid, ContextRoute>? BuildContextGraphRoutes(AgentQueryContext? context, RouteMemoryBiasOptions? biasOptions = null)
    {
        if (context is null || (!context.HasLineage && string.IsNullOrWhiteSpace(context.FailureBucket)))
            return null;

        var seeds = BuildContextGraphSeeds(context)
            .Where(seed => _retrievalGraphStore.RetrievalNodeExists(seed.NodeId))
            .GroupBy(seed => seed.NodeId)
            .Select(group => group.OrderBy(seed => seed.SeedCost).First())
            .ToList();
        if (seeds.Count == 0)
            return null;

        var budget = Math.Max(1d, context.TraversalBudget + 0.75d);
        var best = new Dictionary<Guid, ContextRoute>();
        var frontier = new PriorityQueue<(Guid NodeId, double Cost, string Label), double>();

        foreach (var seed in seeds)
        {
            frontier.Enqueue((seed.NodeId, seed.SeedCost, $"retrieval-graph:{seed.Label}"), seed.SeedCost);
        }

        while (frontier.TryDequeue(out var current, out _))
        {
            var currentRoute = new ContextRoute(current.Label, DistanceToContextBoost(current.Cost), current.Cost);
            if (best.TryGetValue(current.NodeId, out var seen) && !IsBetterRoute(currentRoute, seen))
                continue;

            best[current.NodeId] = currentRoute;
            foreach (var neighbor in EnumerateRetrievalNeighbors(current.NodeId))
            {
                var nextCost = current.Cost + neighbor.StepCost;
                if (nextCost > budget)
                    continue;

                var nextRoute = new ContextRoute(
                    $"{current.Label} -> {neighbor.StepLabel}",
                    DistanceToContextBoost(nextCost),
                    nextCost);
                if (best.TryGetValue(neighbor.NodeId, out var existing) && !IsBetterRoute(nextRoute, existing))
                    continue;

                frontier.Enqueue((neighbor.NodeId, nextCost, nextRoute.Label), nextCost);
            }
        }

        ApplyRouteMemoryBiases(best, context, biasOptions);
        return best;
    }

    private void ApplyRouteMemoryBiases(IDictionary<Guid, ContextRoute> routes, AgentQueryContext context, RouteMemoryBiasOptions? biasOptions = null)
    {
        if (routes.Count == 0)
            return;

        var frame = BuildRouteMemoryFrameFingerprint(context, ScopeLens(context));
        var biases = _routeMemory.GetTargetBiases(frame, biasOptions);
        if (biases.Count == 0)
            return;

        foreach (var pair in biases)
        {
            if (!routes.TryGetValue(pair.Key, out var route))
                continue;

            var bias = pair.Value;
            if (Math.Abs(bias.Bias) < 0.0001d)
                continue;

            var direction = bias.Bias >= 0d ? "reuse" : "suppress";
            var labelSuffix = $"route-memory:{direction}(strong={bias.StrongWins},weak={bias.WeakWins},miss={bias.CandidateMisses})";
            routes[pair.Key] = route with
            {
                Label = $"{route.Label} -> {labelSuffix}",
                Boost = route.Boost + bias.Bias,
                RouteMemoryBias = bias.Bias
            };
        }
    }

    private IReadOnlyList<GraphSeed> BuildContextGraphSeeds(AgentQueryContext context)
    {
        var seeds = new List<GraphSeed>();
        for (var i = 0; i < context.Lineage.Count; i++)
        {
            var scope = context.Lineage[i];
            var depthLabel = scope.Level switch
            {
                "self" => "self-thread",
                "parent" => "parent-thread",
                "grandparent" => "grandparent-thread",
                _ => $"lineage-{i}"
            };

            AddGraphSeed(seeds, RetrievalNodeKinds.TaskThread, scope.ObjectId, depthLabel, i);

            if (!string.IsNullOrWhiteSpace(scope.BranchOffAnchor))
                AddGraphSeed(seeds, RetrievalNodeKinds.SmartListBucket, $"smartlist-bucket:{scope.BranchOffAnchor}", $"{depthLabel}-anchor", i + 0.35d);

            foreach (var artifactNodeId in ResolveTaskArtifactNodeIds(scope.ArtifactRefs))
                seeds.Add(new GraphSeed(artifactNodeId, $"{depthLabel}-artifact", i + 0.55d));
        }

        if (!string.IsNullOrWhiteSpace(context.FailureBucket) && IsSmartListPath(context.FailureBucket!))
            AddGraphSeed(seeds, RetrievalNodeKinds.SmartListBucket, $"smartlist-bucket:{context.FailureBucket}", "failure-bucket", 0.85d);

        foreach (var artifactNodeId in ResolveTaskArtifactNodeIds(context.ActiveArtifacts))
            seeds.Add(new GraphSeed(artifactNodeId, "active-artifact", 0.75d));

        foreach (var projectRef in ResolveWorkspaceProjectRefs())
            AddGraphSeed(seeds, RetrievalNodeKinds.ProjectContext, projectRef, "workspace-project", 1.25d);

        return seeds;
    }

    private IEnumerable<GraphNeighbor> EnumerateRetrievalNeighbors(Guid nodeId)
    {
        foreach (var edge in _retrievalGraphStore.OutboundRetrievalEdges(nodeId))
            yield return new GraphNeighbor(edge.ToNodeId, Math.Max(0d, edge.Meta.Cost), edge.Meta.EdgeKind);

        foreach (var edge in _retrievalGraphStore.InboundRetrievalEdges(nodeId))
        {
            var reversePenalty = edge.Meta.Directed ? 0.15d : 0d;
            yield return new GraphNeighbor(edge.FromNodeId, Math.Max(0d, edge.Meta.Cost) + reversePenalty, $"{edge.Meta.EdgeKind}:reverse");
        }
    }

    private static bool IsBetterRoute(ContextRoute candidate, ContextRoute existing)
    {
        if (candidate.Cost < existing.Cost - 0.0001d)
            return true;
        return Math.Abs(candidate.Cost - existing.Cost) < 0.0001d && candidate.Boost > existing.Boost;
    }

    private void AddGraphSeed(List<GraphSeed> seeds, string nodeKind, string targetRef, string label, double seedCost)
    {
        var nodeId = RetrievalGraphConventions.BuildNodeId(nodeKind, targetRef);
        seeds.Add(new GraphSeed(nodeId, label, seedCost));
    }

    private static IEnumerable<string> ResolveWorkspaceProjectRefs()
    {
        var name = ResolveWorkspaceProjectName();
        if (string.IsNullOrWhiteSpace(name))
            yield break;

        yield return BuildProjectRef(name);
    }

    private static string BuildProjectRef(string raw)
    {
        var normalized = raw.Trim().Replace('\\', '-').Replace('/', '-').Replace(' ', '-').ToLowerInvariant();
        return $"project:{normalized}";
    }

    private static string? ResolveWorkspaceProjectName()
    {
        var cursor = new DirectoryInfo(Directory.GetCurrentDirectory());
        while (cursor is not null)
        {
            if (cursor.GetDirectories(".git").Length > 0 || cursor.GetFiles(".git").Length > 0)
                return cursor.Name;
            cursor = cursor.Parent;
        }

        var cwd = Directory.GetCurrentDirectory();
        return Path.GetFileName(cwd.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
    }

    private IEnumerable<Guid> ResolveTaskArtifactNodeIds(IEnumerable<string> artifactRefs)
    {
        var wanted = artifactRefs
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(NormalizeArtifactRef)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        if (wanted.Count == 0)
            yield break;

        foreach (var artifact in _store.Objects.Values.Where(x => x.ObjectKind == "task_artifact"))
        {
            var normalized = NormalizeArtifactRef(ReadString(artifact.SemanticPayload?.Provenance, "artifact_ref"));
            if (!wanted.Contains(normalized))
                continue;

            var nodeId = RetrievalGraphConventions.BuildNodeId(RetrievalNodeKinds.TaskArtifact, artifact.ObjectId);
            if (_retrievalGraphStore.RetrievalNodeExists(nodeId))
                yield return nodeId;
        }
    }

    private ContextRoute ResolveContextRoute(
        AgentQueryContext? context,
        string? title,
        IEnumerable<string> evidenceTexts,
        IReadOnlyList<GraphAnchor>? candidateAnchors = null,
        IReadOnlyList<Guid>? candidateNodeIds = null,
        IReadOnlyDictionary<Guid, ContextRoute>? graphRoutes = null)
    {
        if (context is null || (!context.HasLineage && string.IsNullOrWhiteSpace(context.FailureBucket)))
            return new ContextRoute("global", 0d);

        var graphRoute = ResolveGraphContextRoute(candidateNodeIds, graphRoutes);
        if (graphRoute.Boost > 0d)
            return graphRoute;

        var structured = ResolveStructuredContextRoute(context, candidateAnchors ?? []);
        if (structured.Boost > 0d)
            return structured;

        var haystack = string.Join('\n', new[] { title ?? string.Empty }.Concat(evidenceTexts ?? []));
        if (string.IsNullOrWhiteSpace(haystack) || !context.HasLineage)
            return new ContextRoute("global", 0d);

        var best = new ContextRoute("global", 0d);
        foreach (var scope in context.Lineage)
        {
            var tokens = BuildScopeTokens(scope);
            if (tokens.Count == 0)
                continue;

            var matched = tokens.Count(token => haystack.Contains(token, StringComparison.OrdinalIgnoreCase));
            if (matched == 0)
                continue;

            var depth = scope.Level switch
            {
                "self" => 0d,
                "parent" => 1d,
                "grandparent" => 2d,
                _ => 3d
            };

            var boost = DistanceToContextBoost(depth) + Math.Min(0.10d, matched * 0.02d);
            if (boost > best.Boost)
                best = new ContextRoute($"text-lineage:{scope.Level}", boost, depth);
        }

        if (!string.IsNullOrWhiteSpace(context.FailureBucket))
        {
            var failureMatches = BuildFreeTextTokens(context.FailureBucket!)
                .Count(token => haystack.Contains(token, StringComparison.OrdinalIgnoreCase));
            if (failureMatches > 0)
                best = new ContextRoute(best.Label, best.Boost + Math.Min(0.04d, failureMatches * 0.02d), best.Cost);
        }

        return best;
    }

    private static ContextRoute ResolveGraphContextRoute(
        IReadOnlyList<Guid>? candidateNodeIds,
        IReadOnlyDictionary<Guid, ContextRoute>? graphRoutes)
    {
        if (candidateNodeIds is null || candidateNodeIds.Count == 0 || graphRoutes is null || graphRoutes.Count == 0)
            return new ContextRoute("global", 0d);

        ContextRoute? best = null;
        foreach (var nodeId in candidateNodeIds)
        {
            if (!graphRoutes.TryGetValue(nodeId, out var route))
                continue;

            best = PickBetter(best, route);
        }

        return best ?? new ContextRoute("global", 0d);
    }

    private ContextRoute ResolveStructuredContextRoute(AgentQueryContext context, IReadOnlyList<GraphAnchor> candidateAnchors)
    {
        if (candidateAnchors.Count == 0)
            return new ContextRoute("global", 0d);

        var budget = Math.Max(1d, context.TraversalBudget + 0.75d);
        ContextRoute? best = null;
        foreach (var seed in BuildContextAnchors(context))
        {
            foreach (var candidate in candidateAnchors)
            {
                if (!TryMeasureAnchorDistance(seed, candidate, out var cost, out var label))
                    continue;
                if (cost > budget)
                    continue;

                var route = new ContextRoute(label, DistanceToContextBoost(cost), cost);
                best = PickBetter(best, route);
            }
        }

        return best ?? new ContextRoute("global", 0d);
    }

    private static ContextRoute? PickBetter(ContextRoute? best, ContextRoute candidate)
    {
        if (best is null)
            return candidate;
        if (candidate.Cost < best.Cost - 0.0001d)
            return candidate;
        if (Math.Abs(candidate.Cost - best.Cost) < 0.0001d && candidate.Boost > best.Boost)
            return candidate;
        return best;
    }

    private static bool ShouldSuppressWeakScopedLesson(
        AgentQueryContext? context,
        int totalTokenCount,
        int matchedTokenCount,
        ContextRoute route,
        double semanticScore)
    {
        if (context is null || !context.HasLineage)
            return false;
        if (totalTokenCount < 3)
            return false;
        if (matchedTokenCount > Math.Max(1, totalTokenCount / 4))
            return false;
        if (semanticScore >= 0.45d)
            return false;
        if (route.RouteMemoryBias > 0d &&
            route.Label.Contains("project-context", StringComparison.Ordinal) &&
            !HasStrongNonProjectScopeSignal(route.Label))
        {
            return true;
        }
        if (HasDirectScopeSignal(route.Label))
            return false;

        return route.RouteMemoryBias > 0d ||
               route.Label.Contains("workspace-project", StringComparison.Ordinal) ||
               string.Equals(route.Label, "global", StringComparison.Ordinal);
    }

    private static bool HasDirectScopeSignal(string routeLabel)
    {
        if (string.IsNullOrWhiteSpace(routeLabel))
            return false;

        return routeLabel.Contains("self-thread", StringComparison.Ordinal) ||
               routeLabel.Contains("parent-thread", StringComparison.Ordinal) ||
               routeLabel.Contains("grandparent-thread", StringComparison.Ordinal) ||
               routeLabel.Contains("active-artifact", StringComparison.Ordinal) ||
               routeLabel.Contains("failure-bucket", StringComparison.Ordinal) ||
               routeLabel.Contains("text-lineage:self", StringComparison.Ordinal) ||
               routeLabel.Contains("text-lineage:parent", StringComparison.Ordinal) ||
               routeLabel.Contains("text-lineage:grandparent", StringComparison.Ordinal);
    }

    private static bool HasStrongNonProjectScopeSignal(string routeLabel)
    {
        if (string.IsNullOrWhiteSpace(routeLabel))
            return false;

        return routeLabel.Contains("anchor", StringComparison.Ordinal) ||
               routeLabel.Contains("artifact", StringComparison.Ordinal) ||
               routeLabel.Contains("failure-bucket", StringComparison.Ordinal) ||
               routeLabel.Contains("text-lineage:self", StringComparison.Ordinal) ||
               routeLabel.Contains("text-lineage:parent", StringComparison.Ordinal) ||
               routeLabel.Contains("text-lineage:grandparent", StringComparison.Ordinal);
    }

    private IReadOnlyList<GraphAnchor> BuildContextAnchors(AgentQueryContext context)
    {
        var anchors = new List<GraphAnchor>();
        for (var i = 0; i < context.Lineage.Count; i++)
        {
            var scope = context.Lineage[i];
            var seedCost = i;
            anchors.Add(new GraphAnchor("task-thread", scope.ObjectId, seedCost));
            anchors.Add(new GraphAnchor("thread-id", scope.NodeId, seedCost));

            if (!string.IsNullOrWhiteSpace(scope.BranchOffAnchor))
            {
                anchors.Add(new GraphAnchor(
                    IsSmartListPath(scope.BranchOffAnchor!) ? "smartlist-bucket" : "branch-anchor",
                    scope.BranchOffAnchor!,
                    seedCost + 0.35d));
            }

            foreach (var artifact in scope.ArtifactRefs.Where(x => !string.IsNullOrWhiteSpace(x)).Distinct(StringComparer.Ordinal))
                anchors.Add(new GraphAnchor("artifact", NormalizeArtifactRef(artifact), seedCost + 0.55d));
        }

        if (!string.IsNullOrWhiteSpace(context.FailureBucket))
        {
            anchors.Add(new GraphAnchor(
                IsSmartListPath(context.FailureBucket!) ? "smartlist-bucket" : "failure-bucket",
                context.FailureBucket!,
                0.85d));
        }

        foreach (var artifact in context.ActiveArtifacts.Where(x => !string.IsNullOrWhiteSpace(x)).Distinct(StringComparer.Ordinal))
            anchors.Add(new GraphAnchor("artifact", NormalizeArtifactRef(artifact), 0.75d));

        return anchors
            .GroupBy(x => (x.Type, x.Value))
            .Select(g => g.OrderBy(x => x.SeedCost).First())
            .ToList();
    }

    private IReadOnlyList<GraphAnchor> BuildLessonAnchors(ObjectRecord lesson, IReadOnlyList<EvidenceSnapshot> snapshots)
    {
        var anchors = new List<GraphAnchor> { new("lesson", lesson.ObjectId) };
        var prov = lesson.SemanticPayload?.Provenance;
        AddIfValue(anchors, "stereotype-family", ReadString(prov, "stereotype_family_id"));
        AddIfValue(anchors, "stereotype-version", ReadString(prov, "stereotype_version_id"));
        AddIfValue(anchors, "origin", ReadString(prov, "origin_dream_id"));

        foreach (var snapshot in snapshots)
        {
            AddIfValue(anchors, "source-ref", snapshot.source_ref);
            anchors.AddRange(BuildAnchorsFromSourceRef(snapshot.source_ref));
        }

        anchors.AddRange(BuildAnchorsFromMemberships(lesson.ObjectId));
        return DistinctAnchors(anchors);
    }

    private IReadOnlyList<GraphAnchor> BuildSemanticNodeAnchors(ObjectRecord node, ObjectRecord lesson, IReadOnlyList<EvidenceSnapshot> snapshots)
        => DistinctAnchors(BuildObjectAnchors(node).Concat(BuildLessonAnchors(lesson, snapshots)).ToList());

    private IReadOnlyList<Guid> BuildCandidateRetrievalNodeIds(params ObjectRecord[] objects)
        => objects
            .Select(TryGetRetrievalNodeId)
            .Where(x => x.HasValue)
            .Select(x => x!.Value)
            .Distinct()
            .ToList();

    private IReadOnlyList<Guid> BuildCandidateRetrievalNodeIds(ObjectRecord obj, string? relatedContainerId)
    {
        var nodeIds = BuildCandidateRetrievalNodeIds(obj).ToList();
        if (!string.IsNullOrWhiteSpace(relatedContainerId) && _store.Containers.TryGetValue(relatedContainerId, out var container))
        {
            var containerNodeId = TryGetRetrievalNodeId(container);
            if (containerNodeId.HasValue)
                nodeIds.Add(containerNodeId.Value);
        }

        return nodeIds.Distinct().ToList();
    }

    private IReadOnlyList<Guid> BuildCandidateRetrievalNodeIds(ContainerRecord container)
    {
        var nodeId = TryGetRetrievalNodeId(container);
        return nodeId.HasValue ? [nodeId.Value] : [];
    }

    private IReadOnlyList<GraphAnchor> BuildContainerAnchors(ContainerRecord container)
    {
        var anchors = new List<GraphAnchor>
        {
            new("container", container.ContainerId)
        };

        if (container.ContainerKind == "chat_session")
            anchors.Add(new GraphAnchor("chat-session", container.ContainerId));

        return anchors;
    }

    private IReadOnlyList<GraphAnchor> BuildObjectAnchors(ObjectRecord obj, string? relatedContainerId = null)
    {
        var prov = obj.SemanticPayload?.Provenance;
        var anchors = new List<GraphAnchor>
        {
            new("object", obj.ObjectId)
        };

        switch (obj.ObjectKind)
        {
            case "task_thread":
                AddIfValue(anchors, "task-thread", obj.ObjectId);
                AddIfValue(anchors, "thread-id", ReadString(prov, "thread_id"));
                AddIfValue(anchors, "thread-id", ReadString(prov, "parent_thread_id"));
                AddAnchorValue(anchors, ReadString(prov, "branch_off_anchor"));
                foreach (var artifact in ReadArtifactRefs(ReadString(prov, "thread_id") ?? Suffix(obj.ObjectId)))
                    AddIfValue(anchors, "artifact", NormalizeArtifactRef(artifact));
                break;
            case "task_checkpoint":
                AddAnchorValue(anchors, ReadString(prov, "branch_off_anchor"));
                AddIfValue(anchors, "artifact", NormalizeArtifactRef(ReadString(prov, "artifact_ref")));
                break;
            case "task_artifact":
                AddIfValue(anchors, "artifact", NormalizeArtifactRef(ReadString(prov, "artifact_ref")));
                break;
            case SmartListService.BucketObjectKind:
                var bucketPath = ReadString(prov, "path");
                if (!IsFreshnessInternalPath(bucketPath))
                    AddIfValue(anchors, "smartlist-bucket", bucketPath);
                break;
            case SmartListService.NoteObjectKind:
                break;
        }

        if (!string.IsNullOrWhiteSpace(relatedContainerId))
            AddIfValue(anchors, "chat-session", relatedContainerId);

        anchors.AddRange(BuildAnchorsFromMemberships(obj.ObjectId));
        return DistinctAnchors(anchors);
    }

    private Guid? TryGetRetrievalNodeId(ObjectRecord obj)
    {
        var nodeKind = obj.ObjectKind switch
        {
            "task_thread" => RetrievalNodeKinds.TaskThread,
            "task_checkpoint" => RetrievalNodeKinds.TaskCheckpoint,
            "task_artifact" => RetrievalNodeKinds.TaskArtifact,
            SmartListService.BucketObjectKind => RetrievalNodeKinds.SmartListBucket,
            SmartListService.NoteObjectKind => RetrievalNodeKinds.SmartListNote,
            SmartListService.RollupObjectKind => RetrievalNodeKinds.SmartListRollup,
            "lesson" => RetrievalNodeKinds.Lesson,
            "lesson_semantic_node" => RetrievalNodeKinds.LessonSemanticNode,
            "chat_message" => RetrievalNodeKinds.ChatMessage,
            "lesson_stereotype_family" => RetrievalNodeKinds.StereotypeFamily,
            "lesson_stereotype_version" => RetrievalNodeKinds.StereotypeVersion,
            _ => null
        };
        if (nodeKind is null)
            return null;

        var nodeId = RetrievalGraphConventions.BuildNodeId(nodeKind, obj.ObjectId);
        return _retrievalGraphStore.RetrievalNodeExists(nodeId) ? nodeId : null;
    }

    private Guid? TryGetRetrievalNodeIdByRef(string? targetRef)
    {
        if (string.IsNullOrWhiteSpace(targetRef))
            return null;

        if (_store.Objects.TryGetValue(targetRef, out var obj))
            return TryGetRetrievalNodeId(obj);

        if (_store.Containers.TryGetValue(targetRef, out var container))
            return TryGetRetrievalNodeId(container);

        return null;
    }

    private Guid? TryGetRetrievalNodeId(ContainerRecord container)
    {
        var nodeKind = container.ContainerKind switch
        {
            "chat_session" => RetrievalNodeKinds.ChatSession,
            _ => null
        };
        if (nodeKind is null)
            return null;

        var nodeId = RetrievalGraphConventions.BuildNodeId(nodeKind, container.ContainerId);
        return _retrievalGraphStore.RetrievalNodeExists(nodeId) ? nodeId : null;
    }

    private IReadOnlyList<GraphAnchor> BuildAnchorsFromSourceRef(string? sourceRef)
    {
        if (string.IsNullOrWhiteSpace(sourceRef))
            return [];

        var anchors = new List<GraphAnchor> { new("source-ref", sourceRef!) };
        if (_store.Objects.TryGetValue(sourceRef!, out var obj))
            anchors.AddRange(BuildObjectAnchors(obj));
        else if (_store.Containers.TryGetValue(sourceRef!, out var container))
            anchors.AddRange(BuildContainerAnchors(container));
        return DistinctAnchors(anchors);
    }

    private IReadOnlyList<GraphAnchor> BuildAnchorsFromMemberships(string objectId)
    {
        var anchors = new List<GraphAnchor>();
        foreach (var containerId in _store.ContainersForMemberObject(objectId))
        {
            if (containerId.StartsWith("smartlist-members:", StringComparison.Ordinal))
            {
                var path = containerId["smartlist-members:".Length..];
                if (!IsFreshnessInternalPath(path))
                    anchors.Add(new GraphAnchor("smartlist-bucket", path));
                continue;
            }

            if (containerId.StartsWith("lesson-stereotype-members:", StringComparison.Ordinal))
            {
                anchors.Add(new GraphAnchor("stereotype-version", containerId["lesson-stereotype-members:".Length..]));
                continue;
            }

            if (containerId.StartsWith("chat-session:", StringComparison.Ordinal))
                anchors.Add(new GraphAnchor("chat-session", containerId));
        }

        return anchors;
    }

    private static IReadOnlyList<GraphAnchor> DistinctAnchors(IReadOnlyList<GraphAnchor> anchors)
        => anchors
            .Where(x => !string.IsNullOrWhiteSpace(x.Value))
            .GroupBy(x => (x.Type, x.Value))
            .Select(g => g.OrderBy(x => x.SeedCost).First())
            .ToList();

    private static void AddIfValue(List<GraphAnchor> anchors, string type, string? value)
    {
        if (!string.IsNullOrWhiteSpace(value))
            anchors.Add(new GraphAnchor(type, value!.Trim()));
    }

    private static void AddAnchorValue(List<GraphAnchor> anchors, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return;

        anchors.Add(new GraphAnchor(IsSmartListPath(value) ? "smartlist-bucket" : "branch-anchor", value.Trim()));
    }

    private static bool TryMeasureAnchorDistance(GraphAnchor seed, GraphAnchor candidate, out double cost, out string label)
    {
        cost = double.PositiveInfinity;
        label = "global";

        if (seed.Type == "task-thread" && candidate.Type == "task-thread" && string.Equals(seed.Value, candidate.Value, StringComparison.Ordinal))
        {
            cost = seed.SeedCost;
            label = "graph-distance:task-thread";
            return true;
        }

        if ((seed.Type == "task-thread" || seed.Type == "thread-id") && (candidate.Type == "task-thread" || candidate.Type == "thread-id"))
        {
            var seedId = Suffix(seed.Value);
            var candidateId = Suffix(candidate.Value);
            if (string.Equals(seedId, candidateId, StringComparison.Ordinal))
            {
                cost = seed.SeedCost + 0.10d;
                label = "graph-distance:lineage-thread";
                return true;
            }
        }

        if (seed.Type == "artifact" && candidate.Type == "artifact" && string.Equals(seed.Value, candidate.Value, StringComparison.OrdinalIgnoreCase))
        {
            cost = seed.SeedCost + 0.70d;
            label = "graph-distance:artifact";
            return true;
        }

        if (seed.Type == "source-ref" && candidate.Type == "source-ref" && string.Equals(seed.Value, candidate.Value, StringComparison.Ordinal))
        {
            cost = seed.SeedCost + 0.95d;
            label = "graph-distance:source-ref";
            return true;
        }

        if (seed.Type == "stereotype-version" && candidate.Type == "stereotype-version" && string.Equals(seed.Value, candidate.Value, StringComparison.Ordinal))
        {
            cost = seed.SeedCost + 1.10d;
            label = "graph-distance:stereotype-version";
            return true;
        }

        if (seed.Type == "stereotype-family" && candidate.Type == "stereotype-family" && string.Equals(seed.Value, candidate.Value, StringComparison.Ordinal))
        {
            cost = seed.SeedCost + 1.40d;
            label = "graph-distance:stereotype-family";
            return true;
        }

        if (seed.Type == "smartlist-bucket" && candidate.Type == "smartlist-bucket"
            && TryMeasureSmartListDistance(seed.Value, candidate.Value, out var smartListCost, out var smartListLabel))
        {
            cost = seed.SeedCost + smartListCost;
            label = smartListLabel;
            return true;
        }

        if ((seed.Type == "branch-anchor" || seed.Type == "failure-bucket")
            && candidate.Type == "smartlist-bucket"
            && TryMeasureSmartListDistance(seed.Value, candidate.Value, out smartListCost, out smartListLabel))
        {
            cost = seed.SeedCost + smartListCost + 0.20d;
            label = smartListLabel;
            return true;
        }

        if (string.Equals(seed.Type, candidate.Type, StringComparison.Ordinal)
            && string.Equals(seed.Value, candidate.Value, StringComparison.OrdinalIgnoreCase))
        {
            cost = seed.SeedCost + 1.25d;
            label = $"graph-distance:{seed.Type}";
            return true;
        }

        return false;
    }

    private static bool TryMeasureSmartListDistance(string left, string right, out double cost, out string label)
    {
        cost = double.PositiveInfinity;
        label = "graph-distance:smartlist";
        if (!IsSmartListPath(left) || !IsSmartListPath(right))
            return false;

        var leftParts = left.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var rightParts = right.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var common = 0;
        while (common < leftParts.Length && common < rightParts.Length
            && string.Equals(leftParts[common], rightParts[common], StringComparison.OrdinalIgnoreCase))
        {
            common++;
        }

        if (common == 0)
            return false;

        var segmentDelta = (leftParts.Length - common) + (rightParts.Length - common);
        cost = 0.35d + (segmentDelta * 0.55d);
        label = segmentDelta == 0
            ? "graph-distance:smartlist-exact"
            : leftParts.Length == common || rightParts.Length == common
                ? "graph-distance:smartlist-ancestor"
                : "graph-distance:smartlist-lateral";
        return true;
    }

    private static double DistanceToContextBoost(double cost)
        => 0.22d / (1d + Math.Max(0d, cost));

    private static bool IsSmartListPath(string? value)
        => !string.IsNullOrWhiteSpace(value) && value.StartsWith("smartlist/", StringComparison.OrdinalIgnoreCase);

    private static string NormalizeArtifactRef(string? artifactRef)
    {
        if (string.IsNullOrWhiteSpace(artifactRef))
            return string.Empty;

        var trimmed = artifactRef.Trim();
        return (Path.GetFileName(trimmed) ?? trimmed).ToLowerInvariant();
    }

    private static IReadOnlyList<string> BuildScopeTokens(AgentLineageScope scope)
    {
        var inputs = new[]
        {
            scope.NodeId,
            scope.Title,
            scope.CurrentStep,
            scope.NextCommand,
            scope.BranchOffAnchor ?? string.Empty,
            string.Join(' ', scope.ArtifactRefs.Select(x => Path.GetFileName(x) ?? x))
        };
        return BuildFreeTextTokens(string.Join(' ', inputs));
    }

    private static IReadOnlyList<string> BuildFreeTextTokens(string text)
    {
        return TokenRx.Matches(text ?? string.Empty)
            .Select(x => x.Value.ToLowerInvariant())
            .Where(x => !TitleStopwords.Contains(x))
            .Where(x => !TitleNoise.Contains(x))
            .Where(x => !IsLikelyIdentifierToken(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();
    }

    private static string ScopeLens(AgentQueryContext? context)
        => context is not null && context.HasLineage ? "local-first-lineage" : "global";

    private static string ContextWhy(ContextRoute route)
    {
        if (route.Boost <= 0d && Math.Abs(route.RouteMemoryBias) < 0.0001d)
            return string.Empty;

        var parts = new List<string>();
        if (route.Boost > 0d)
        {
            parts.Add(double.IsFinite(route.Cost)
                ? $" Context route: {route.Label} (cost={route.Cost:0.##})."
                : $" Context route: {route.Label}.");
        }
        else
        {
            parts.Add($" Context route: {route.Label}.");
        }

        if (Math.Abs(route.RouteMemoryBias) >= 0.0001d)
            parts.Add($" Route-memory bias={route.RouteMemoryBias:+0.##;-0.##}.");

        return string.Concat(parts);
    }

    private bool IsResolvable(string sourceRef) => !string.IsNullOrWhiteSpace(sourceRef) && (_store.Objects.ContainsKey(sourceRef) || _store.Containers.ContainsKey(sourceRef));

    private string BuildManualSmartListSnippet(ObjectRecord obj)
    {
        var prov = obj.SemanticPayload?.Provenance;
        if (obj.ObjectKind == SmartListService.BucketObjectKind)
        {
            var path = ReadString(prov, "path") ?? obj.ObjectId;
            var rollupId = $"smartlist-rollup:{path}";
            if (_store.Objects.TryGetValue(rollupId, out var rollup) && rollup.ObjectKind == SmartListService.RollupObjectKind)
            {
                var rollupProv = rollup.SemanticPayload?.Provenance;
                var summary = ReadString(rollupProv, "summary") ?? string.Empty;
                var scope = ReadString(rollupProv, "scope") ?? string.Empty;
                var stopHint = ReadString(rollupProv, "stop_hint") ?? string.Empty;
                var childHighlights = string.Empty;
                if (rollupProv is not null
                    && rollupProv.TryGetValue("child_highlights", out var childEl)
                    && childEl.ValueKind == JsonValueKind.Array)
                {
                    var parts = childEl.EnumerateArray()
                        .Where(x => x.ValueKind == JsonValueKind.Object)
                        .Select(x =>
                        {
                            var childPath = x.TryGetProperty("path", out var pathEl) ? pathEl.ToString() ?? string.Empty : string.Empty;
                            var childSummary = x.TryGetProperty("summary", out var summaryEl) ? summaryEl.ToString() ?? string.Empty : string.Empty;
                            return string.IsNullOrWhiteSpace(childSummary) ? childPath : $"{childPath}: {childSummary}";
                        })
                        .Where(x => !string.IsNullOrWhiteSpace(x))
                        .ToList();
                    if (parts.Count > 0)
                        childHighlights = " children: " + string.Join("; ", parts);
                }

                return $"bucket {path}; summary: {summary}"
                    + (string.IsNullOrWhiteSpace(stopHint) ? string.Empty : $"; stop_hint: {stopHint}")
                    + (string.IsNullOrWhiteSpace(scope) ? string.Empty : $"; scope: {scope}")
                    + childHighlights;
            }

            var membersContainerId = ReadString(prov, "members_container_id");
            var members = string.IsNullOrWhiteSpace(membersContainerId) || !_store.Containers.ContainsKey(membersContainerId)
                ? []
                : _store.IterateForward(membersContainerId)
                    .Select(x => x.ObjectId)
                    .Select(id =>
                    {
                        if (!_store.Objects.TryGetValue(id, out var member))
                            return id;

                        var memberProv = member.SemanticPayload?.Provenance;
                        return member.ObjectKind switch
                        {
                            SmartListService.BucketObjectKind => ReadString(memberProv, "path") ?? id,
                            SmartListService.NoteObjectKind => ReadString(memberProv, "title") ?? member.SemanticPayload?.Summary ?? id,
                            _ => member.SemanticPayload?.Summary ?? id
                        };
                    })
                    .Take(6)
                    .ToList();

            return members.Count == 0
                ? $"bucket {path}"
                : $"bucket {path}; members: {string.Join(", ", members)}";
        }

        var text = ReadString(prov, "text") ?? obj.SemanticPayload?.Summary ?? obj.ObjectId;
        var bucketPaths = _store.ContainersForMemberObject(obj.ObjectId)
            .Where(x => x.StartsWith("smartlist-members:", StringComparison.Ordinal))
            .Select(x => x["smartlist-members:".Length..])
            .Distinct(StringComparer.Ordinal)
            .OrderBy(x => x, StringComparer.Ordinal)
            .Take(6)
            .ToList();

        return bucketPaths.Count == 0
            ? text
            : $"{text} buckets: {string.Join(", ", bucketPaths)}";
    }

    private string ResolveSnippet(string sourceRef)
    {
        if (string.IsNullOrWhiteSpace(sourceRef))
            return string.Empty;

        if (_store.Objects.TryGetValue(sourceRef, out var obj))
        {
            var text = ReadString(obj.SemanticPayload?.Provenance, "text");
            if (!string.IsNullOrWhiteSpace(text))
                return text;

            if (!string.IsNullOrWhiteSpace(obj.SemanticPayload?.Summary))
                return obj.SemanticPayload.Summary!;
        }

        if (_store.Containers.TryGetValue(sourceRef, out var container))
        {
            var title = ReadString(container.Metadata, "title");
            if (!string.IsNullOrWhiteSpace(title))
                return title;

            if (container.ContainerKind == "chat_session")
            {
                var fallback = ResolveSessionSnippet(container.ContainerId);
                if (!string.IsNullOrWhiteSpace(fallback))
                    return fallback;
            }

            return container.ContainerId;
        }

        return sourceRef;
    }

    private string? ResolveSessionSnippet(string sessionId)
    {
        foreach (var link in _store.IterateForward(sessionId))
        {
            if (!_store.Objects.TryGetValue(link.ObjectId, out var msg))
                continue;

            var text = ReadString(msg.SemanticPayload?.Provenance, "text");
            if (!string.IsNullOrWhiteSpace(text))
                return text;

            if (!string.IsNullOrWhiteSpace(msg.SemanticPayload?.Summary))
                return msg.SemanticPayload.Summary;
        }

        return null;
    }

    private static string SourceRef(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return string.Empty;

        var value = raw.Trim();
        if (value.StartsWith("Turn:", StringComparison.Ordinal))
            return NormalizeWithPrefix(value["Turn:".Length..], "chat-msg:");

        if (value.StartsWith("Conversation:", StringComparison.Ordinal))
            return NormalizeConversationRef(value["Conversation:".Length..]);

        if (value.StartsWith("Segment:", StringComparison.Ordinal))
            return NormalizeConversationRef(value["Segment:".Length..]);

        if (value.StartsWith("session-ref:", StringComparison.Ordinal))
            return "chat-session:" + value["session-ref:".Length..];

        if (value.StartsWith("chat-session:chat-session:", StringComparison.Ordinal))
            return "chat-session:" + value["chat-session:chat-session:".Length..];

        if (value.StartsWith("chat-msg:chat-msg:", StringComparison.Ordinal))
            return "chat-msg:" + value["chat-msg:chat-msg:".Length..];

        return value.Contains(':', StringComparison.Ordinal) ? value : "chat-session:" + value;
    }

    private static string NormalizeConversationRef(string raw)
    {
        var value = raw.Trim();
        if (value.StartsWith("session-ref:", StringComparison.Ordinal))
            return "chat-session:" + value["session-ref:".Length..];
        return NormalizeWithPrefix(value, "chat-session:");
    }

    private static string NormalizeWithPrefix(string raw, string prefix)
    {
        var value = raw.Trim();
        if (value.StartsWith(prefix, StringComparison.Ordinal))
            return value;
        return prefix + value;
    }

    private static string Kind(string sourceRef) => sourceRef.StartsWith("chat-msg:", StringComparison.Ordinal) ? "turn" : sourceRef.StartsWith("chat-session:", StringComparison.Ordinal) ? "session" : "reference";
    private static string TruncateSnippet(string text, int maxChars) => string.IsNullOrWhiteSpace(text) || text.Length <= maxChars ? text : text[..maxChars].TrimEnd() + "...";
    private static double ShortTermRecencyScore(DateTimeOffset nowUtc, DateTimeOffset? timestamp)
    {
        if (timestamp is null)
            return 0.50d;

        var ageDays = Math.Max(0d, (nowUtc - timestamp.Value).TotalDays);
        return 1d / (1d + (ageDays / 7d));
    }
    private static double ConfidenceFromVote(ObjectRecord dream) => Math.Clamp(ReadDouble(dream.SemanticPayload?.Provenance, "vote_score", 0d) / 10d, 0.20d, 1d);
    private static double EvidenceHealth(IReadOnlyList<EvidenceSnapshot> snapshots) => snapshots.Count == 0 ? 0.25d : Math.Clamp((double)snapshots.Count(x => x.link_status == "live") / snapshots.Count, 0d, 1d);
    private void EnsureUniqueContainer(string id, string kind) { if (!_store.Containers.ContainsKey(id)) _store.CreateContainer(id, "container", kind); _store.Containers[id].Policies.UniqueMembers = true; }
    private void ReplaceMembers(string containerId, IReadOnlyList<string> memberIds) { foreach (var link in _store.IterateForward(containerId).ToList()) _store.RemoveLinkNode(containerId, link.LinkNodeId); foreach (var memberId in memberIds.Where(x => !string.IsNullOrWhiteSpace(x)).Distinct(StringComparer.Ordinal)) if (_store.Objects.ContainsKey(memberId) || _store.Containers.ContainsKey(memberId)) _store.AddObject(containerId, memberId); }
    private void MoveToTier(string lessonId, string tier) { foreach (var t in TierOrder) { var cid = TierContainer(t); if (_store.TryGetMembership(cid, lessonId, out var link)) _store.RemoveLinkNode(cid, link.LinkNodeId); } _store.AddObject(TierContainer(tier), lessonId); }
    private static string TierContainer(string tier) => $"lesson-freshness:{tier}";
    private static string Hash8(string text) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text ?? string.Empty))).ToLowerInvariant()[..8];
    private static string Suffix(string id) { var i = id.IndexOf(':', StringComparison.Ordinal); return i >= 0 ? id[(i + 1)..] : id; }
    private static Dictionary<string, JsonElement> EnsureProv(ObjectRecord obj) { obj.SemanticPayload ??= new SemanticPayload(); obj.SemanticPayload.Provenance ??= new Dictionary<string, JsonElement>(StringComparer.Ordinal); return obj.SemanticPayload.Provenance; }
    private static List<EvidenceSnapshot> ReadSnapshots(IReadOnlyDictionary<string, JsonElement>? p) => p is not null && p.TryGetValue("evidence_snapshots", out var el) ? (el.Deserialize<List<EvidenceSnapshot>>() ?? []) : [];
    private static string? ReadString(IReadOnlyDictionary<string, JsonElement>? p, string k) => p is not null && p.TryGetValue(k, out var e) ? (e.ValueKind == JsonValueKind.String ? e.GetString() : e.ToString()) : null;
    private static double ReadDouble(IReadOnlyDictionary<string, JsonElement>? p, string k, double d) => p is not null && p.TryGetValue(k, out var e) && (e.TryGetDouble(out var x) || (e.ValueKind == JsonValueKind.String && double.TryParse(e.GetString(), out x))) ? x : d;
    private static int ReadInt(IReadOnlyDictionary<string, JsonElement>? p, string k, int d) => p is not null && p.TryGetValue(k, out var e) && (e.TryGetInt32(out var x) || (e.ValueKind == JsonValueKind.String && int.TryParse(e.GetString(), out x))) ? x : d;
    private static DateTimeOffset? ReadDate(IReadOnlyDictionary<string, JsonElement>? p, string k) => p is not null && p.TryGetValue(k, out var e) && (e.TryGetDateTimeOffset(out var x) || (e.ValueKind == JsonValueKind.String && DateTimeOffset.TryParse(e.GetString(), out x))) ? x : null;
    private static IReadOnlyList<string> ReadStringList(IReadOnlyDictionary<string, JsonElement>? p, string k) => p is not null && p.TryGetValue(k, out var e) && e.ValueKind == JsonValueKind.Array ? e.EnumerateArray().Where(x => x.ValueKind == JsonValueKind.String).Select(x => x.GetString() ?? string.Empty).Where(x => !string.IsNullOrWhiteSpace(x)).ToList() : [];
    private static string? EmptyToNull(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;
    private static string ReadSource(IReadOnlyDictionary<string, JsonElement>? p) { foreach (var key in ReadStringList(p, "source_project_keys")) { var source = SourceFromKey(key); if (!string.IsNullOrWhiteSpace(source)) return source; } return string.Empty; }
    private string? SourceProjectKey(string sourceRef) { string? channel = null; if (sourceRef.StartsWith("chat-session:", StringComparison.Ordinal) && _store.Containers.TryGetValue(sourceRef, out var c)) channel = ReadString(c.Metadata, "channel"); else if (sourceRef.StartsWith("chat-msg:", StringComparison.Ordinal) && _store.Objects.TryGetValue(sourceRef, out var o)) channel = ReadString(o.SemanticPayload?.Provenance, "channel"); if (string.IsNullOrWhiteSpace(channel)) return null; var source = channel.StartsWith("claude-code", StringComparison.OrdinalIgnoreCase) ? "claude" : channel.StartsWith("codex", StringComparison.OrdinalIgnoreCase) ? "codex" : (channel.Contains('/') ? channel[..channel.IndexOf('/', StringComparison.Ordinal)] : channel).Trim().ToLowerInvariant(); var project = channel.Contains('/') ? channel[(channel.IndexOf('/', StringComparison.Ordinal) + 1)..] : "unknown"; return $"source:{source}|project:{project}"; }
    private static string SourceFromKey(string key) { foreach (var part in key.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) if (part.StartsWith("source:", StringComparison.Ordinal)) return part["source:".Length..]; return string.Empty; }

    private sealed record RankedLessons(
        IReadOnlyList<AgentLessonHit> Hits,
        AgentQueryDiagnostics Diagnostics);

    private sealed record LessonScore(
        double Score,
        double Semantic,
        double Freshness,
        double Evidence,
        double Confidence,
        double Decay,
        string Tier,
        IReadOnlyList<EvidenceSnapshot> Snapshots,
        double SemanticContribution,
        double FreshnessContribution,
        double EvidenceContribution);
}
