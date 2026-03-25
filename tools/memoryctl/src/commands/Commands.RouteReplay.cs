using AMS.Core;
using MemoryGraph.Abstractions;
using MemoryGraph.Application;
using MemoryGraph.Infrastructure.AMS;
using System.Text.Json;

namespace MemoryCtl;

/// <summary>
/// Offline route-replay lane.
///
/// For each <see cref="RouteReplayRecord"/> in a JSONL input file:
///   1. Deserialise a clean copy of the AMS store (isolation — no bleed between cases).
///   2. Run <c>baseline</c> query without any pre-injected route-memory episodes.
///   3. Inject the pre-recorded episodes from the record into the retrieval graph.
///   4. Run <c>replay</c> query — the injected episodes now steer routing.
///   5. Write a <see cref="RouteReplayOutput"/> line to the output JSONL file.
/// </summary>
internal sealed partial class GraphCommandModule
{
    private static readonly JsonSerializerOptions RouteReplayJsonOptions = new()
    {
        WriteIndented = false
    };

    public int RouteReplay(string dbPath, string inputPath, string outPath, int defaultTop)
    {
        var store = _runtimeFactory.LoadAmsStore(dbPath);
        if (store is null)
        {
            Console.Error.WriteLine("error: route-replay requires the AMS backend.");
            return 1;
        }

        if (!File.Exists(inputPath))
        {
            Console.Error.WriteLine($"error: input file not found: {inputPath}");
            return 1;
        }

        var lines = File.ReadAllLines(inputPath);
        var records = new List<RouteReplayRecord>();
        foreach (var line in lines)
        {
            var trimmed = line.Trim();
            if (string.IsNullOrEmpty(trimmed) || trimmed.StartsWith("//", StringComparison.Ordinal))
                continue;
            var record = JsonSerializer.Deserialize<RouteReplayRecord>(trimmed, RouteReplayJsonOptions);
            if (record is not null)
                records.Add(record);
        }

        if (records.Count == 0)
        {
            Console.Error.WriteLine("warning: no records found in input file.");
            return 0;
        }

        // Serialise once — each case deserialises a pristine copy for isolation.
        var storeSnapshot = AmsPersistence.Serialize(store);

        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(outPath)) ?? ".");
        using var writer = File.CreateText(outPath);

        var improved = 0;
        var regressed = 0;
        var unchanged = 0;

        for (var i = 0; i < records.Count; i++)
        {
            var record = records[i];
            var top = record.Top > 0 ? record.Top : defaultTop;

            // Fresh isolated store for this case.
            var freshStore = AmsPersistence.Deserialize(storeSnapshot);
            var service = new AgentMemoryService(freshStore);

            // Materialise lesson nodes into the retrieval graph so candidate
            // node IDs can be resolved during episode injection.
            service.Maintain(DateTimeOffset.UtcNow);

            var context = BuildAgentQueryContext(
                freshStore,
                record.CurrentNode,
                record.ParentNode,
                record.GrandparentNode,
                record.Role,
                record.Mode,
                failureBucket: null,
                activeArtifacts: [],
                traversalBudget: 3,
                noActiveThreadContext: record.NoActiveThreadContext);

            // ── Baseline (no episodes injected) ───────────────────────────
            var baseline = service.Query(record.Query, top, DateTimeOffset.UtcNow, touch: false, context, projectRouteMemory: false);

            // ── Inject pre-recorded episodes ───────────────────────────────
            var graphAdapter = new AmsGraphStoreAdapter(freshStore);
            var routeMemory = new RouteMemoryService(graphAdapter);
            InjectReplayEpisodes(routeMemory, graphAdapter, record.Episodes);

            // ── Replay (episodes now steer routing bias) ───────────────────
            var replay = service.Query(record.Query, top, DateTimeOffset.UtcNow, touch: false, context, projectRouteMemory: true);

            var baselineHits = baseline.Hits.Select(h => h.LessonId).ToList();
            var replayHits = replay.Hits.Select(h => h.LessonId).ToList();
            var baselineSurface = baselineHits
                .Concat(baseline.ShortTermHits.Select(h => h.SourceRef))
                .Distinct(StringComparer.Ordinal)
                .ToList();
            // Full measured surface: lesson hits + short-term scoped hits (sourceRef).
            // Acceptance checks, top-1 deltas, and inspectability should all use this
            // same surface so short-term winners are not invisible to replay analysis.
            var replaySurface = replayHits
                .Concat(replay.ShortTermHits.Select(h => h.SourceRef))
                .Distinct(StringComparer.Ordinal)
                .ToList();

            var top1Baseline = baselineSurface.Count > 0 ? baselineSurface[0] : null;
            var top1Replay = replaySurface.Count > 0 ? replaySurface[0] : null;
            var top1Changed = !string.Equals(top1Baseline, top1Replay, StringComparison.Ordinal);

            var expectedRefs = record.ExpectedRefs is { Count: > 0 } ? record.ExpectedRefs : null;
            bool? expectedRefsHit = expectedRefs is not null
                ? replaySurface.Any(r => expectedRefs.Contains(r, StringComparer.Ordinal))
                : null;

            var delta = ComputeReplayDelta(baseline.WeakResult, replay.WeakResult, top1Baseline, top1Replay, expectedRefs);

            // Inspectability contract: capture route paths from the full measured surface.
            // Lesson hits contribute Explain.Path; short-term hits contribute their Path.
            var replayExplainPaths = replay.Hits
                .Select(h => h.Explain?.Path ?? string.Empty)
                .Concat(replay.ShortTermHits.Select(h => h.Path))
                .Where(p => !string.IsNullOrEmpty(p))
                .Distinct(StringComparer.Ordinal)
                .ToList();
            var routeMemorySignalPresent = replayExplainPaths.Any(p =>
                p.Contains("route-memory:reuse", StringComparison.Ordinal) ||
                p.Contains("route-memory:suppress", StringComparison.Ordinal) ||
                p.Contains("episode-result:", StringComparison.Ordinal) ||
                p.Contains("episode-candidate", StringComparison.Ordinal));

            if (delta.StartsWith("top1-promoted", StringComparison.Ordinal) || delta == "improved-weak")
                improved++;
            else if (delta.StartsWith("top1-demoted", StringComparison.Ordinal) || delta == "regressed-weak")
                regressed++;
            else
                unchanged++;

            var output = new RouteReplayOutput(
                CaseIndex: i,
                Query: record.Query,
                BaselineHits: baselineHits,
                ReplayHits: replayHits,
                ReplaySurface: replaySurface,
                BaselineWeak: baseline.WeakResult,
                ReplayWeak: replay.WeakResult,
                BaselineScopeLens: baseline.Diagnostics.ScopeLens,
                ReplayScopeLens: replay.Diagnostics.ScopeLens,
                Top1Changed: top1Changed,
                Top1Baseline: top1Baseline,
                Top1Replay: top1Replay,
                Delta: delta,
                ExpectedRefsHit: expectedRefsHit,
                ReplayExplainPaths: replayExplainPaths,
                RouteMemorySignalPresent: routeMemorySignalPresent);

            writer.WriteLine(JsonSerializer.Serialize(output, RouteReplayJsonOptions));
        }

        writer.Flush();
        Console.WriteLine($"route-replay: {records.Count} cases → improved={improved} regressed={regressed} unchanged={unchanged}");
        Console.WriteLine($"output: {Path.GetFullPath(outPath)}");
        return 0;
    }

    private static void InjectReplayEpisodes(
        RouteMemoryService routeMemory,
        AmsGraphStoreAdapter graphAdapter,
        IReadOnlyList<RouteReplayEpisodeEntry> episodes)
    {
        foreach (var entry in episodes)
        {
            var frame = new RetrievalFrameFingerprint(
                ScopeLens: entry.Frame.ScopeLens,
                AgentRole: entry.Frame.AgentRole,
                Mode: entry.Frame.Mode,
                LineageNodeIds: entry.Frame.LineageNodeIds,
                ArtifactRefs: entry.Frame.ArtifactRefs ?? []);

            var route = new RetrievalRouteDescriptor(
                RankingSource: entry.Route.RankingSource,
                Path: entry.Route.Path,
                Cost: entry.Route.Cost,
                RiskFlags: entry.Route.RiskFlags ?? []);

            DateTimeOffset occurredAt = DateTimeOffset.TryParse(entry.Episode.OccurredAt, out var parsed)
                ? parsed
                : DateTimeOffset.UtcNow;

            var episodeRecord = new RetrievalEpisodeRecord(
                QueryText: entry.Episode.QueryText,
                OccurredAt: occurredAt,
                WeakResult: entry.Episode.WeakResult,
                UsedFallback: entry.Episode.UsedFallback,
                WinningTargetRef: entry.Episode.WinningTargetRef,
                TopTargetRefs: entry.Episode.TopTargetRefs);

            var frameNode = routeMemory.UpsertFrameNode(frame);
            var routeNode = routeMemory.UpsertRouteNode(route);

            // Resolve candidate node IDs from target refs (deterministic hash).
            var candidateNodeIds = entry.CandidateTargetRefs
                .Select(r => RetrievalGraphConventions.BuildNodeId(RetrievalNodeKinds.Lesson, r))
                .Where(id => graphAdapter.RetrievalNodeExists(id))
                .ToList();

            var winningNodeId = RetrievalGraphConventions.BuildNodeId(RetrievalNodeKinds.Lesson, entry.WinningTargetRef);
            Guid? winnerArg = graphAdapter.RetrievalNodeExists(winningNodeId) ? winningNodeId : null;

            routeMemory.RecordEpisode(episodeRecord, frameNode.NodeId, routeNode.NodeId, candidateNodeIds, winnerArg);
        }
    }

    private static string ComputeReplayDelta(
        bool baselineWeak,
        bool replayWeak,
        string? top1Baseline,
        string? top1Replay,
        IReadOnlyList<string>? expectedRefs)
    {
        var top1Changed = !string.Equals(top1Baseline, top1Replay, StringComparison.Ordinal);

        if (baselineWeak && !replayWeak)
            return "improved-weak";
        if (!baselineWeak && replayWeak)
            return "regressed-weak";

        if (top1Changed && top1Replay is not null && top1Baseline is not null)
        {
            if (expectedRefs is { Count: > 0 })
            {
                var replayTop1InExpected = expectedRefs.Contains(top1Replay, StringComparer.Ordinal);
                var baselineTop1InExpected = expectedRefs.Contains(top1Baseline, StringComparer.Ordinal);
                if (replayTop1InExpected && !baselineTop1InExpected)
                    return "top1-promoted";
                if (!replayTop1InExpected && baselineTop1InExpected)
                    return "top1-demoted";
                // Both in or both out — direction unknown, treat as reorder.
                return "reorder";
            }
            // No expected_refs: direction unknown, but top1 changed.
            return "reorder";
        }

        if (top1Changed)
            return "reorder";
        return "no-change";
    }
}
