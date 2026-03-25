using AMS.Core;
using System.Text;
using MemoryGraph.Abstractions;
using MemoryGraph.Application;
using MemoryGraph.Infrastructure.AMS;
using MemoryCtl.Viewer;

namespace MemoryCtl;

internal sealed partial class GraphCommandModule
{
    private readonly ICommandRuntimeFactory _runtimeFactory;

    public GraphCommandModule(ICommandRuntimeFactory runtimeFactory)
    {
        _runtimeFactory = runtimeFactory ?? throw new ArgumentNullException(nameof(runtimeFactory));
    }

    private AmsStore? RequireAmsStore(string dbPath, string commandName)
    {
        var store = _runtimeFactory.LoadAmsStore(dbPath);
        if (store is not null)
            return store;

        if (_runtimeFactory is LegacyCommandRuntimeFactory)
        {
            Console.Error.WriteLine($"error: {commandName} requires the AMS backend. Rerun with '--backend ams'.");
            return null;
        }

        Console.Error.WriteLine(
            $"error: {commandName} requires AMS state at '{AmsStateStore.AmsPath(dbPath)}'. " +
            "Ingest or sync the corpus first.");
        return null;
    }

    public int Add(string dbPath, string title, string text, IReadOnlyList<string> memAnchors, string? source, string? key)
    {
        MemoryDb? existingDb = null;
        if (File.Exists(dbPath))
            existingDb = MemoryJsonlReader.Load(dbPath);

        var memAnchorNames = (memAnchors.Count == 0) ? new[] { "Conversations" } : memAnchors;
        var resolved = existingDb != null
            ? MemoryJsonlWriter.ResolveBinders(existingDb, memAnchorNames)
            : memAnchorNames.Select(n => (Guid.NewGuid(), n)).ToList();

        var now = DateTimeOffset.Now;
        var cardId = !string.IsNullOrWhiteSpace(key)
            ? GuidUtil.FromKey("card:" + key)
            : Guid.NewGuid();

        if (existingDb is not null)
        {
            // Route through application service abstractions before persisting.
            var runtime = _runtimeFactory.Load(dbPath);
            runtime.IngestService.UpsertCard(cardId, runtime.Payloads, title, text, source, now);
            foreach (var (memAnchorId, memAnchorName) in resolved)
            {
                runtime.IngestService.UpsertMemAnchor(memAnchorId, memAnchorName);
                runtime.IngestService.LinkCardToMemAnchor(cardId, memAnchorId);
            }
        }

        // Preserve existing append-only JSONL persistence behavior.
        MemoryJsonlWriter.AppendCard(dbPath, cardId, title, text, source, now, resolved);

        Console.WriteLine(cardId);
        return 0;
    }

    public int Query(string dbPath, string query, int top, string? memAnchorFilter, bool explain)
    {
        var runtime = _runtimeFactory.Load(dbPath);

        var candidateSet = BuildCandidateSet(runtime, memAnchorFilter);
        var hits = runtime.RetrievalService.Query(query, runtime.GraphStore, runtime.Payloads, top: runtime.GraphStore.AllCards.Count)
            .Where(hit => candidateSet is null || candidateSet.Contains(hit.CardId))
            .Take(top)
            .ToList();

        foreach (var hit in hits)
        {
            runtime.Db.TryGetPayload(new CardBinder.Core.CardId(hit.CardId), out var payload);
            var title = payload?.Title ?? hit.CardId.ToString();
            Console.WriteLine($"- {title}  (score={hit.TotalScore:0.##})");

            if (!explain)
                continue;

            Console.WriteLine($"    text={hit.TextScore:0.##} memAnchor={hit.MemAnchorScore:0.##} meta={hit.MetaScore:0.##}");
            var memAnchors = runtime.GraphStore.BindersOf(hit.CardId)
                .Select(id => runtime.GraphStore.TryGetMemAnchorName(id, out var name) ? name : id.ToString())
                .ToList();
            if (memAnchors.Count > 0)
                Console.WriteLine($"    memAnchors: {string.Join(", ", memAnchors)}");
        }

        return 0;
    }

    public int AgentMaintain(string dbPath)
    {
        var store = RequireAmsStore(dbPath, "agent-maintain");
        if (store is null)
            return 1;

        var service = new AgentMemoryService(store);
        var result = service.Maintain(DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);

        Console.WriteLine($"lessons={result.LessonsSynced} summaries={result.SummariesBuilt} pruned_links={result.SourceLinksPruned}");
        return 0;
    }

    public int RetrievalGraphMaterialize(string dbPath)
    {
        var store = RequireAmsStore(dbPath, "retrieval-graph-materialize");
        if (store is null)
            return 1;

        var projector = new RetrievalGraphProjector(store);
        projector.ProjectAll();
        AmsStateStore.Save(dbPath, store);

        var graphStore = new AmsGraphStoreAdapter(store);
        var nodeCount = graphStore.AllRetrievalNodes.Count;
        var edgeCount = graphStore.AllRetrievalNodes.Sum(nodeId => graphStore.OutboundRetrievalEdges(nodeId).Count);
        Console.WriteLine($"retrieval_nodes={nodeCount}");
        Console.WriteLine($"retrieval_edges={edgeCount}");
        return 0;
    }

    public int AgentQuery(
        string dbPath,
        string query,
        int top,
        bool explain,
        bool recordRoute,
        string? currentNodeId,
        string? parentNodeId,
        string? grandparentNodeId,
        string? agentRole,
        string? mode,
        string? failureBucket,
        IReadOnlyList<string> activeArtifacts,
        int traversalBudget,
        bool noActiveThreadContext,
        RouteMemoryBiasOptions? biasOptions = null)
    {
        var store = RequireAmsStore(dbPath, "agent-query");
        if (store is null)
            return 1;

        var service = new AgentMemoryService(store);
        var context = BuildAgentQueryContext(
            store,
            currentNodeId,
            parentNodeId,
            grandparentNodeId,
            agentRole,
            mode,
            failureBucket,
            activeArtifacts,
            traversalBudget,
            noActiveThreadContext);
        var result = service.Query(query, top, DateTimeOffset.UtcNow, touch: true, context, projectRouteMemory: recordRoute, biasOptions: biasOptions);
        Console.WriteLine(result.Markdown);

        if (explain)
        {
            Console.WriteLine();
            Console.WriteLine("## Explain");
            for (var i = 0; i < result.Hits.Count; i++)
            {
                var hit = result.Hits[i];
                var explainHit = hit.Explain;
                if (explainHit is null)
                    continue;

                Console.WriteLine($"{i + 1}. {hit.Title}");
                Console.WriteLine($"- source={explainHit.RankingSource}");
                Console.WriteLine($"- matched_tokens: {(explainHit.MatchedTokens.Count == 0 ? "(none)" : string.Join(", ", explainHit.MatchedTokens))}");
                Console.WriteLine(
                    $"- token_locations: node=[{string.Join(", ", explainHit.TokenMatchLocations.NodeSummaryTokens)}] " +
                    $"lesson=[{string.Join(", ", explainHit.TokenMatchLocations.LessonSummaryTokens)}] " +
                    $"evidence=[{string.Join(", ", explainHit.TokenMatchLocations.EvidenceSnippetTokens)}]");
                Console.WriteLine(
                    $"- score_breakdown: overlap={explainHit.ScoreBreakdown.TokenOverlap:0.00} " +
                    $"semantic={explainHit.ScoreBreakdown.SemanticContribution:0.00} " +
                    $"freshness={explainHit.ScoreBreakdown.FreshnessContribution:0.00} " +
                    $"evidence={explainHit.ScoreBreakdown.EvidenceContribution:0.00} " +
                    $"decay={explainHit.ScoreBreakdown.DecayDivisor:0.00} " +
                    $"final={explainHit.ScoreBreakdown.FinalScore:0.00}");
                Console.WriteLine($"- path: {explainHit.Path}");
                Console.WriteLine($"- why: {explainHit.WhyWon}");
                Console.WriteLine($"- risk: {(explainHit.RiskFlags.Count == 0 ? "none" : string.Join(", ", explainHit.RiskFlags))}");
            }

            if (result.ShortTermHits.Count > 0)
            {
                for (var i = 0; i < result.ShortTermHits.Count; i++)
                {
                    var hit = result.ShortTermHits[i];
                    Console.WriteLine($"{result.Hits.Count + i + 1}. [short-term] {hit.SessionTitle}");
                    Console.WriteLine($"- source={hit.SourceKind}");
                    Console.WriteLine($"- matched_tokens: {(hit.MatchedTokens.Count == 0 ? "(none)" : string.Join(", ", hit.MatchedTokens))}");
                    Console.WriteLine($"- score_breakdown: recency={hit.Recency:0.00} final={hit.Score:0.00}");
                    Console.WriteLine($"- path: {hit.Path}");
                }
            }
        }

        Console.WriteLine();
        Console.WriteLine($"# Diagnostics");
        Console.WriteLine(
            $"weak_result={result.WeakResult.ToString().ToLowerInvariant()} " +
            $"touched={result.TouchedLessons} freshness_admissions={result.FreshnessAdmissions} lesson_hits={result.Hits.Count} short_term_hits={result.ShortTermHits.Count} " +
            $"lane={result.Diagnostics.ScoringLane} reroute={result.Diagnostics.RoutingDecision} scope_lens={result.Diagnostics.ScopeLens}");
        if (result.Diagnostics.RoutingFlags.Count > 0)
            Console.WriteLine($"routing_flags={string.Join(", ", result.Diagnostics.RoutingFlags)}");
        if (result.RouteMemoryEpisodesProjected > 0 || result.TouchedLessons > 0 || result.FreshnessAdmissions > 0)
            AmsStateStore.Save(dbPath, store);
        return 0;
    }

    public int SmartListCreate(string dbPath, string path, bool durable, string createdBy)
    {
        var store = RequireAmsStore(dbPath, "smartlist-create");
        if (store is null)
            return 1;

        var service = new SmartListService(store);
        var bucket = service.CreateBucket(path, durable, createdBy, DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);

        Console.WriteLine($"path={bucket.Path}");
        Console.WriteLine($"object_id={bucket.ObjectId}");
        Console.WriteLine($"durability={bucket.Durability}");
        return 0;
    }

    public int SmartListNote(string dbPath, string title, string text, IReadOnlyList<string> bucketPaths, bool durable, string createdBy)
    {
        var store = RequireAmsStore(dbPath, "smartlist-note");
        if (store is null)
            return 1;

        var service = new SmartListService(store);
        var note = service.CreateNote(title, text, bucketPaths, durable, createdBy, DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);

        Console.WriteLine($"note_id={note.NoteId}");
        Console.WriteLine($"title={note.Title}");
        Console.WriteLine($"durability={note.Durability}");
        Console.WriteLine($"bucket_paths={(note.BucketPaths.Count == 0 ? "(root)" : string.Join(", ", note.BucketPaths))}");
        return 0;
    }

    public int SmartListAttach(string dbPath, string path, string member, string createdBy)
    {
        var store = RequireAmsStore(dbPath, "smartlist-attach");
        if (store is null)
            return 1;

        var service = new SmartListService(store);
        var bucket = service.Attach(path, member, createdBy, DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);

        Console.WriteLine($"path={bucket.Path}");
        Console.WriteLine($"attached={member}");
        return 0;
    }

    public int SmartListInspect(string dbPath, string path, int depth)
    {
        var store = RequireAmsStore(dbPath, "smartlist-inspect");
        if (store is null)
            return 1;

        var result = new SmartListService(store).Inspect(path, depth);
        Console.WriteLine($"path={result.Path}");
        Console.WriteLine($"title={result.Title}");
        Console.WriteLine($"durability={result.Durability}");
        Console.WriteLine($"retrieval_visibility={result.RetrievalVisibility}");
        foreach (var entry in result.Entries)
        {
            var indent = new string(' ', entry.Depth * 2);
            Console.WriteLine($"{indent}- [{entry.MemberKind}] {entry.PathOrId} title={entry.Title} durability={entry.Durability} retrieval_visibility={entry.RetrievalVisibility}");
        }

        return 0;
    }

    public int SmartListRemember(string dbPath, string? path, string? objectId)
    {
        var store = RequireAmsStore(dbPath, "smartlist-remember");
        if (store is null)
            return 1;

        var result = new SmartListService(store).Remember(path, objectId, DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);

        Console.WriteLine($"buckets_promoted={result.BucketsPromoted}");
        Console.WriteLine($"notes_promoted={result.NotesPromoted}");
        Console.WriteLine($"promoted={string.Join(", ", result.PromotedObjectIds)}");
        return 0;
    }

    public int SmartListRollup(
        string dbPath,
        string path,
        string summary,
        string scope,
        string? stopHint,
        IReadOnlyList<SmartListRollupChild> children,
        bool durable,
        string createdBy)
    {
        var store = RequireAmsStore(dbPath, "smartlist-rollup");
        if (store is null)
            return 1;

        var rollup = new SmartListService(store).SetRollup(path, summary, scope, stopHint, children, durable, createdBy, DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);

        Console.WriteLine($"bucket_path={rollup.BucketPath}");
        Console.WriteLine($"rollup_id={rollup.RollupId}");
        Console.WriteLine($"title={rollup.Title}");
        Console.WriteLine($"durability={rollup.Durability}");
        Console.WriteLine($"child_highlights={rollup.ChildHighlights.Count}");
        return 0;
    }

    public int SmartListRollupShow(string dbPath, string path)
    {
        var store = RequireAmsStore(dbPath, "smartlist-rollup-show");
        if (store is null)
            return 1;

        var rollup = new SmartListService(store).GetRollup(path);
        if (rollup is null)
        {
            Console.Error.WriteLine($"error: no rollup found for '{path}'.");
            return 1;
        }

        Console.WriteLine($"bucket_path={rollup.BucketPath}");
        Console.WriteLine($"rollup_id={rollup.RollupId}");
        Console.WriteLine($"title={rollup.Title}");
        Console.WriteLine($"summary={rollup.Summary}");
        Console.WriteLine($"scope={rollup.Scope}");
        Console.WriteLine($"stop_hint={rollup.StopHint ?? string.Empty}");
        Console.WriteLine($"durability={rollup.Durability}");
        Console.WriteLine($"retrieval_visibility={rollup.RetrievalVisibility}");
        foreach (var child in rollup.ChildHighlights)
            Console.WriteLine($"child={child.Path}::{child.Summary}");
        return 0;
    }

    public int SmartListVisibility(
        string dbPath,
        string path,
        string visibility,
        bool recursive,
        bool includeNotes,
        bool includeRollups)
    {
        var store = RequireAmsStore(dbPath, "smartlist-visibility");
        if (store is null)
            return 1;

        var result = new SmartListService(store).SetRetrievalVisibility(path, visibility, recursive, includeNotes, includeRollups, DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);

        Console.WriteLine($"path={result.Path}");
        Console.WriteLine($"retrieval_visibility={result.RetrievalVisibility}");
        Console.WriteLine($"buckets_updated={result.BucketsUpdated}");
        Console.WriteLine($"notes_updated={result.NotesUpdated}");
        Console.WriteLine($"rollups_updated={result.RollupsUpdated}");
        return 0;
    }

    public int BugReportCreate(
        string dbPath,
        string sourceAgent,
        string parentAgent,
        string errorOutput,
        string stackContext,
        IReadOnlyList<string>? attemptedFixes,
        IReadOnlyList<string>? reproductionSteps,
        string? recommendedFixPlan,
        string severity,
        bool durable,
        string createdBy)
    {
        var store = RequireAmsStore(dbPath, "bugreport-create");
        if (store is null)
            return 1;

        var service = new BugReportService(store);
        var report = service.CreateBugReport(
            sourceAgent, parentAgent, errorOutput, stackContext,
            attemptedFixes, reproductionSteps, recommendedFixPlan,
            severity, durable, createdBy, DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);

        WriteBugReport(report);
        return 0;
    }

    public int BugReportUpdateStatus(string dbPath, string bugId, string status)
    {
        var store = RequireAmsStore(dbPath, "bugreport-update-status");
        if (store is null)
            return 1;

        var service = new BugReportService(store);
        var report = service.UpdateStatus(bugId, status, DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);

        WriteBugReport(report);
        return 0;
    }

    public int BugReportShow(string dbPath, string bugId)
    {
        var store = RequireAmsStore(dbPath, "bugreport-show");
        if (store is null)
            return 1;

        var report = new BugReportService(store).GetBugReport(bugId);
        if (report is null)
        {
            Console.Error.WriteLine($"Bug report '{bugId}' not found.");
            return 1;
        }

        WriteBugReport(report);
        return 0;
    }

    public int BugReportList(string dbPath, string? statusFilter)
    {
        var store = RequireAmsStore(dbPath, "bugreport-list");
        if (store is null)
            return 1;

        var reports = new BugReportService(store).ListBugReports(statusFilter);
        Console.WriteLine($"count={reports.Count}");
        foreach (var report in reports)
        {
            Console.WriteLine($"---");
            Console.WriteLine($"bug_id={report.BugId}");
            Console.WriteLine($"severity={report.Severity}");
            Console.WriteLine($"status={report.Status}");
            Console.WriteLine($"source_agent={report.SourceAgent}");
            Console.WriteLine($"error={BugReportService.TruncateForSummary(report.ErrorOutput)}");
            Console.WriteLine($"created_at={report.CreatedAt:O}");
        }
        return 0;
    }

    public int BugReportSearch(string dbPath, string query, string? statusFilter)
    {
        var store = RequireAmsStore(dbPath, "bugreport-search");
        if (store is null)
            return 1;

        var results = new BugReportService(store).SearchBugReports(query, statusFilter);
        Console.WriteLine($"query={query}");
        Console.WriteLine($"count={results.Count}");
        foreach (var report in results)
        {
            Console.WriteLine($"---");
            Console.WriteLine($"bug_id={report.BugId}");
            Console.WriteLine($"severity={report.Severity}");
            Console.WriteLine($"status={report.Status}");
            Console.WriteLine($"source_agent={report.SourceAgent}");
            Console.WriteLine($"error={BugReportService.TruncateForSummary(report.ErrorOutput)}");
            Console.WriteLine($"fix_plan={BugReportService.TruncateForSummary(report.RecommendedFixPlan)}");
            Console.WriteLine($"created_at={report.CreatedAt:O}");
            if (report.ResolvedAt.HasValue)
                Console.WriteLine($"resolved_at={report.ResolvedAt.Value:O}");
        }
        return 0;
    }

    public int BugFixCreate(
        string dbPath,
        string title,
        string description,
        string fixRecipe,
        string? linkedBugReportId,
        bool durable,
        string createdBy)
    {
        var store = RequireAmsStore(dbPath, "bugfix-create");
        if (store is null)
            return 1;

        var service = new BugReportService(store);
        var fix = service.CreateBugFix(
            title, description, fixRecipe, linkedBugReportId,
            durable, createdBy, DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);

        WriteBugFix(fix);
        return 0;
    }

    public int BugFixShow(string dbPath, string fixId)
    {
        var store = RequireAmsStore(dbPath, "bugfix-show");
        if (store is null)
            return 1;

        var fix = new BugReportService(store).GetBugFix(fixId);
        if (fix is null)
        {
            Console.Error.WriteLine($"Bug fix '{fixId}' not found.");
            return 1;
        }

        WriteBugFix(fix);
        return 0;
    }

    public int BugFixList(string dbPath)
    {
        var store = RequireAmsStore(dbPath, "bugfix-list");
        if (store is null)
            return 1;

        var fixes = new BugReportService(store).ListBugFixes();
        Console.WriteLine($"count={fixes.Count}");
        foreach (var fix in fixes)
        {
            Console.WriteLine($"---");
            Console.WriteLine($"fix_id={fix.FixId}");
            Console.WriteLine($"title={fix.Title}");
            Console.WriteLine($"status={fix.Status}");
            Console.WriteLine($"linked_bugreports={string.Join(", ", fix.LinkedBugReportIds)}");
            Console.WriteLine($"created_at={fix.CreatedAt:O}");
        }
        return 0;
    }

    public int BugFixLink(string dbPath, string bugReportId, string bugFixId, string createdBy)
    {
        var store = RequireAmsStore(dbPath, "bugfix-link");
        if (store is null)
            return 1;

        var service = new BugReportService(store);
        service.LinkBugReportToFix(bugReportId, bugFixId, createdBy, DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);

        Console.WriteLine($"linked_bugreport={bugReportId}");
        Console.WriteLine($"linked_bugfix={bugFixId}");
        return 0;
    }

    private static void WriteBugFix(BugFixInfo fix)
    {
        Console.WriteLine($"fix_id={fix.FixId}");
        Console.WriteLine($"title={fix.Title}");
        Console.WriteLine($"description={fix.Description}");
        Console.WriteLine($"fix_recipe={fix.FixRecipe}");
        Console.WriteLine($"status={fix.Status}");
        Console.WriteLine($"linked_bugreport_ids={string.Join(", ", fix.LinkedBugReportIds)}");
        Console.WriteLine($"durability={fix.Durability}");
        Console.WriteLine($"bucket_paths={string.Join(", ", fix.BucketPaths)}");
        Console.WriteLine($"created_at={fix.CreatedAt:O}");
    }

    private static void WriteBugReport(BugReportInfo report)
    {
        Console.WriteLine($"bug_id={report.BugId}");
        Console.WriteLine($"source_agent={report.SourceAgent}");
        Console.WriteLine($"parent_agent={report.ParentAgent}");
        Console.WriteLine($"severity={report.Severity}");
        Console.WriteLine($"status={report.Status}");
        Console.WriteLine($"error_output={report.ErrorOutput}");
        Console.WriteLine($"stack_context={report.StackContext}");
        Console.WriteLine($"attempted_fixes={string.Join(" | ", report.AttemptedFixes)}");
        Console.WriteLine($"reproduction_steps={string.Join(" | ", report.ReproductionSteps)}");
        Console.WriteLine($"recommended_fix_plan={report.RecommendedFixPlan}");
        Console.WriteLine($"durability={report.Durability}");
        Console.WriteLine($"retrieval_visibility={report.RetrievalVisibility}");
        Console.WriteLine($"bucket_paths={string.Join(", ", report.BucketPaths)}");
        Console.WriteLine($"created_at={report.CreatedAt:O}");
        Console.WriteLine($"resolved_at={report.ResolvedAt?.ToString("O") ?? ""}");
    }

    public int ThreadStatus(string dbPath)
    {
        var store = RequireAmsStore(dbPath, "thread-status");
        if (store is null)
            return 1;

        var overview = new TaskGraphService(store).Inspect();
        WriteThreadStatus(overview);
        return 0;
    }

    public int ThreadStart(string dbPath, string title, string currentStep, string nextCommand, string? threadId, string? branchOffAnchor, string? artifactRef)
    {
        var store = RequireAmsStore(dbPath, "thread-start");
        if (store is null)
            return 1;

        var service = new TaskGraphService(store);
        var result = service.StartThread(title, currentStep, nextCommand, threadId, branchOffAnchor, artifactRef, DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);
        TryMirrorActiveThreadMarkdown(service);
        WriteThreadAction("thread-start", result);
        return 0;
    }

    public int ThreadPushTangent(string dbPath, string title, string currentStep, string nextCommand, string? threadId, string? branchOffAnchor, string? artifactRef)
    {
        var store = RequireAmsStore(dbPath, "thread-push-tangent");
        if (store is null)
            return 1;

        var service = new TaskGraphService(store);
        var result = service.PushTangent(title, currentStep, nextCommand, threadId, branchOffAnchor, artifactRef, DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);
        TryMirrorActiveThreadMarkdown(service);
        WriteThreadAction("thread-push-tangent", result);
        return 0;
    }

    public int ThreadCheckpoint(string dbPath, string currentStep, string nextCommand, string? branchOffAnchor, string? artifactRef)
    {
        var store = RequireAmsStore(dbPath, "thread-checkpoint");
        if (store is null)
            return 1;

        var service = new TaskGraphService(store);
        var result = service.CheckpointActiveThread(currentStep, nextCommand, branchOffAnchor, artifactRef, DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);
        TryMirrorActiveThreadMarkdown(service);
        WriteThreadAction("thread-checkpoint", result);
        return 0;
    }

    public int ThreadPop(string dbPath)
    {
        var store = RequireAmsStore(dbPath, "thread-pop");
        if (store is null)
            return 1;

        var service = new TaskGraphService(store);
        var result = service.PopThread(DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);
        TryMirrorActiveThreadMarkdown(service);
        WriteThreadAction("thread-pop", result);
        return 0;
    }

    public int ThreadArchive(string dbPath, string? threadId)
    {
        var store = RequireAmsStore(dbPath, "thread-archive");
        if (store is null)
            return 1;

        var service = new TaskGraphService(store);
        var result = service.ArchiveThread(threadId, DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);
        TryMirrorActiveThreadMarkdown(service);
        WriteThreadAction("thread-archive", result);
        return 0;
    }

    public int ThreadList(string dbPath)
    {
        var store = RequireAmsStore(dbPath, "thread-list");
        if (store is null)
            return 1;

        var overview = new TaskGraphService(store).Inspect();
        Console.WriteLine("# TASK THREADS");
        foreach (var thread in overview.AllThreads.OrderByDescending(x => string.Equals(x.Status, "active", StringComparison.OrdinalIgnoreCase)).ThenByDescending(x => x.UpdatedAt))
        {
            var marker = string.Equals(thread.Status, "active", StringComparison.OrdinalIgnoreCase) ? "*" : "-";
            var parent = string.IsNullOrWhiteSpace(thread.ParentThreadId) ? string.Empty : $" parent={thread.ParentThreadId}";
            Console.WriteLine($"{marker} {thread.Status.ToUpperInvariant()} {thread.ThreadId} {thread.Title}{parent}");
            Console.WriteLine($"  step: {thread.CurrentStep}");
            Console.WriteLine($"  next: {thread.NextCommand}");
            Console.WriteLine($"  checkpoints={thread.Checkpoints.Count} children={thread.ChildThreadIds.Count} artifacts={thread.Artifacts.Count}");
        }

        return 0;
    }

    public int AgentCapabilityUpsert(
        string dbPath,
        string agent,
        string capabilityKey,
        string state,
        string problemKey,
        string equivalenceGroupKey,
        string? summary,
        string? notes,
        string createdBy)
    {
        var store = RequireAmsStore(dbPath, "agent-capability-upsert");
        if (store is null)
            return 1;

        var entry = new AgentCapabilityService(store).Upsert(
            agent,
            capabilityKey,
            state,
            problemKey,
            equivalenceGroupKey,
            summary,
            notes,
            createdBy,
            DateTimeOffset.UtcNow);
        AmsStateStore.Save(dbPath, store);
        WriteAgentCapability(entry);
        return 0;
    }

    public int AgentCapabilityShow(string dbPath, string? entryId, string? agent, string? capabilityKey)
    {
        var store = RequireAmsStore(dbPath, "agent-capability-show");
        if (store is null)
            return 1;

        var service = new AgentCapabilityService(store);
        AgentCapabilityEntryInfo? entry;
        if (!string.IsNullOrWhiteSpace(entryId))
        {
            entry = service.GetById(entryId!);
        }
        else
        {
            if (string.IsNullOrWhiteSpace(agent) || string.IsNullOrWhiteSpace(capabilityKey))
            {
                Console.Error.WriteLine("error: agent-capability-show requires either --id or both --agent and --capability-key.");
                return 1;
            }

            entry = service.GetByPair(agent!, capabilityKey!);
        }

        if (entry is null)
        {
            Console.Error.WriteLine("error: agent capability entry was not found.");
            return 1;
        }

        WriteAgentCapability(entry);
        return 0;
    }

    public int AgentCapabilityList(string dbPath, string? agent, string? problemKey, string? equivalenceGroupKey)
    {
        var store = RequireAmsStore(dbPath, "agent-capability-list");
        if (store is null)
            return 1;

        var selected = new[]
        {
            !string.IsNullOrWhiteSpace(agent),
            !string.IsNullOrWhiteSpace(problemKey),
            !string.IsNullOrWhiteSpace(equivalenceGroupKey)
        }.Count(x => x);

        if (selected != 1)
        {
            Console.Error.WriteLine("error: agent-capability-list requires exactly one of --agent, --problem, or --group.");
            return 1;
        }

        var service = new AgentCapabilityService(store);
        IReadOnlyList<AgentCapabilityEntryInfo> entries;
        string filterKind;
        string filterValue;
        if (!string.IsNullOrWhiteSpace(agent))
        {
            filterKind = "agent";
            filterValue = agent!;
            entries = service.ListByAgent(agent!);
        }
        else if (!string.IsNullOrWhiteSpace(problemKey))
        {
            filterKind = "problem";
            filterValue = problemKey!;
            entries = service.ListByProblem(problemKey!);
        }
        else
        {
            filterKind = "group";
            filterValue = equivalenceGroupKey!;
            entries = service.ListByGroup(equivalenceGroupKey!);
        }

        Console.WriteLine("# AGENT CAPABILITIES");
        Console.WriteLine($"filter={filterKind}:{filterValue}");
        Console.WriteLine($"count={entries.Count}");
        foreach (var entry in entries)
            Console.WriteLine($"- {entry.Agent} {entry.CapabilityKey} state={entry.State} problem={entry.ProblemKey} group={entry.EquivalenceGroupKey} id={entry.EntryId}");
        return 0;
    }

    public int Prompt(
        string dbPath,
        string query,
        int top,
        IReadOnlyList<string> memAnchorFilters,
        IReadOnlyList<string> containerSeeds,
        IReadOnlyList<string> objectSeeds,
        int maxObjects,
        ContextObjectOrdering ordering)
    {
        var runtime = _runtimeFactory.Load(dbPath);

        HashSet<Guid>? candidateSet = null;
        if (memAnchorFilters is { Count: > 0 })
        {
            var filterSet = new HashSet<string>(memAnchorFilters.Select(f => f.ToLowerInvariant()));
            candidateSet = new HashSet<Guid>();

            foreach (var memAnchorId in runtime.GraphStore.AllMemAnchors)
            {
                if (!runtime.GraphStore.TryGetMemAnchorName(memAnchorId, out var name) || string.IsNullOrWhiteSpace(name))
                    continue;
                if (!filterSet.Contains(name.ToLowerInvariant()))
                    continue;

                foreach (var cardId in runtime.GraphStore.CardsIn(memAnchorId))
                    candidateSet.Add(cardId);
            }
        }

        var topCards = runtime.RetrievalService.Query(query, runtime.GraphStore, runtime.Payloads, top: runtime.GraphStore.AllCards.Count)
            .Where(hit => candidateSet is null || candidateSet.Contains(hit.CardId))
            .Take(top)
            .Select(hit => hit.CardId)
            .ToList();

        var isRoadmap = memAnchorFilters.Any(b =>
            b != null && b.Equals("Topic: roadmap", StringComparison.OrdinalIgnoreCase));

        if (isRoadmap)
        {
            using var sw = new StringWriter();
            sw.WriteLine("# Roadmap");
            sw.WriteLine($"Query: {query}");
            sw.WriteLine();

            var openItems = new List<(string Title, string Area, string Description)>();
            var inProgressItems = new List<(string Title, string Area, string Description)>();
            var doneItems = new List<(string Title, string Area, string Description)>();

            foreach (var cardId in topCards)
            {
                if (!runtime.Payloads.TryGetValue(cardId, out var payload))
                    continue;

                var title = payload.Title ?? cardId.ToString();
                var text = payload.Text ?? string.Empty;
                var (status, area, desc) = Commands.ParseRoadmapFields(text, title);
                var item = (Title: title, Area: area, Description: desc);

                switch (status)
                {
                    case "in_progress": inProgressItems.Add(item); break;
                    case "done": doneItems.Add(item); break;
                    default: openItems.Add(item); break;
                }
            }

            WriteRoadmapBucket(sw, "Open", openItems);
            WriteRoadmapBucket(sw, "In progress", inProgressItems);
            WriteRoadmapBucket(sw, "Done", doneItems);

            Console.Write(sw.ToString());
            return 0;
        }

        var amsStore = BuildAmsStoreFromRuntime(runtime);

        var compiled = ContextCompiler.CompileContext(amsStore, new ContextCompileInput
        {
            ContainerIds = containerSeeds,
            ObjectIds = objectSeeds.Count > 0 ? objectSeeds : topCards.Select(x => x.ToString("D")).ToArray(),
            MaxObjects = Math.Max(1, maxObjects),
            Ordering = ordering
        });

        Console.WriteLine(compiled.MemoryMarkdown);
        return 0;
    }

    private static AmsStore BuildAmsStoreFromRuntime(CommandRuntime runtime)
    {
        var store = new AmsStore();

        foreach (var cardId in runtime.GraphStore.AllCards)
        {
            runtime.Payloads.TryGetValue(cardId, out var payload);
            store.UpsertObject(
                cardId.ToString("D"),
                "card",
                payload?.Source,
                payload is null
                    ? null
                    : new SemanticPayload { Summary = payload.Title });
        }

        foreach (var memAnchorId in runtime.GraphStore.AllMemAnchors)
        {
            var containerId = memAnchorId.ToString("D");
            store.CreateContainer(containerId, "card", "memAnchor");
            if (runtime.GraphStore.TryGetMemAnchorName(memAnchorId, out var name) && !string.IsNullOrWhiteSpace(name))
                store.Containers[containerId].ExpectationMetadata.Interpretation = name;

            foreach (var cardId in runtime.GraphStore.CardsIn(memAnchorId))
            {
                if (store.Objects.ContainsKey(cardId.ToString("D")))
                    store.AddObject(containerId, cardId.ToString("D"));
            }
        }

        return store;
    }

    public int MakeMemAnchor(string dbPath, string memAnchorName, string query, int top, string? memAnchorFilter, float relevance, string reason)
    {
        var runtime = _runtimeFactory.Load(dbPath);

        var candidateSet = BuildCandidateSet(runtime, memAnchorFilter);
        var topCards = runtime.RetrievalService.Query(query, runtime.GraphStore, runtime.Payloads, top: runtime.GraphStore.AllCards.Count)
            .Where(hit => candidateSet is null || candidateSet.Contains(hit.CardId))
            .Take(top)
            .Select(hit => hit.CardId)
            .ToList();

        var binderCache = MemoryJsonlWriter.CreateBinderCache(runtime.Db);
        var resolved = MemoryJsonlWriter.ResolveBinders(binderCache, new[] { memAnchorName });

        var linked = 0;
        foreach (var cardId in topCards)
        {
            MemoryJsonlWriter.AppendLinks(dbPath, cardId, DateTimeOffset.Now, resolved, relevance, reason);
            linked++;
        }

        Console.WriteLine($"MemAnchor '{memAnchorName}' linked to {linked} card(s).");
        return 0;
    }

    public int Maintain(string dbPath, Guid cardId, int top, bool apply, string reason, float relevance)
    {
        var db = MemoryJsonlReader.Load(dbPath);
        var seed = new CardBinder.Core.CardId(cardId);

        if (!db.Core.CardExists(seed))
            throw new ArgumentException($"Card not found: {cardId}");

        if (!db.TryGetPayload(seed, out _))
            throw new ArgumentException($"Card has no payload text: {cardId}. (maintain needs card_payload)");

        Func<CardBinder.Core.MemAnchorId, string?> binderName = b => db.Core.TryGetBinderName(b, out var n) ? n : null;

        var related = Maintenance.FindRelatedCards(db, seed, top);
        Console.WriteLine("RELATED CARDS");
        foreach (var r in related)
            Console.WriteLine($"- {r.Title}  (score={r.Score:0.##})  id={r.CardId.Value}");

        Console.WriteLine();
        Console.WriteLine("SUGGESTED BINDERS (from related cards)");
        var binderSuggestions = Maintenance.SuggestBindersFromRelated(db, related, top);
        foreach (var (name, score) in binderSuggestions)
            Console.WriteLine($"- {name}  (score={score:0.##})");

        if (!apply)
            return 0;

        var already = new HashSet<CardBinder.Core.MemAnchorId>(db.Core.BindersOf(seed));
        var toAdd = new List<(Guid Id, string Name)>();

        foreach (var (name, _) in binderSuggestions)
        {
            var match = db.Core.AllBinders.FirstOrDefault(b => string.Equals(binderName(b), name, StringComparison.OrdinalIgnoreCase));
            if (match.Value == Guid.Empty || already.Contains(match))
                continue;
            toAdd.Add((match.Value, name));
        }

        if (toAdd.Count == 0)
        {
            Console.WriteLine();
            Console.WriteLine("APPLY: nothing to link (already linked or no resolvable memAnchors)." );
            return 0;
        }

        var runtime = _runtimeFactory.Load(dbPath);
        foreach (var (id, _) in toAdd)
        {
            runtime.IngestService.LinkCardToMemAnchor(cardId, id, new MemoryLinkMeta(
                Relevance: relevance,
                Reason: reason,
                CreatedAt: DateTimeOffset.Now));
        }

        var resolved = toAdd.Select(x => (x.Id, x.Name)).ToList();
        MemoryJsonlWriter.AppendLinks(dbPath, cardId, DateTimeOffset.Now, resolved, relevance, reason);

        Console.WriteLine();
        Console.WriteLine($"APPLY: linked {cardId} to {resolved.Count} memAnchor(s)." );
        return 0;
    }

    public int AtlasPage(string dbPath, string pageId)
    {
        var store = RequireAmsStore(dbPath, "atlas-page");
        if (store is null) return 1;

        // Special synthetic page: atlas:0 — not yet implemented (Phase 3 Rust port)
        if (string.Equals(pageId, "atlas:0", StringComparison.OrdinalIgnoreCase))
        {
            Console.Error.WriteLine("error: atlas:0 multi-resolution summary is not yet implemented (planned for Phase 3).");
            return 1;
        }

        // Resolve the object: try exact ID first, then container lookup
        ObjectRecord? obj = null;
        ContainerRecord? container = null;

        if (store.Objects.TryGetValue(pageId, out obj))
        {
            store.Containers.TryGetValue(pageId, out container);
        }
        else
        {
            // Try prefix match across objects
            var objMatches = store.Objects.Keys
                .Where(k => k.StartsWith(pageId, StringComparison.OrdinalIgnoreCase))
                .ToList();
            if (objMatches.Count == 1)
            {
                obj = store.Objects[objMatches[0]];
                store.Containers.TryGetValue(objMatches[0], out container);
            }
            else if (objMatches.Count > 1)
            {
                Console.Error.WriteLine($"error: ambiguous page-id prefix '{pageId}' matches {objMatches.Count} objects.");
                foreach (var m in objMatches.Take(10))
                    Console.Error.WriteLine($"  {m}");
                return 1;
            }
            else
            {
                Console.Error.WriteLine($"error: no object found for page-id '{pageId}'.");
                return 1;
            }
        }

        // Header
        Console.WriteLine($"=== {obj.ObjectId} ===");
        Console.WriteLine($"kind:       {obj.ObjectKind}");
        Console.WriteLine($"created:    {obj.CreatedAt:yyyy-MM-dd HH:mm:ss}");
        Console.WriteLine($"updated:    {obj.UpdatedAt:yyyy-MM-dd HH:mm:ss}");

        if (!string.IsNullOrWhiteSpace(obj.InSituRef))
            Console.WriteLine($"in-situ:    {obj.InSituRef}");

        if (obj.SemanticPayload is { } sp)
        {
            if (!string.IsNullOrWhiteSpace(sp.Summary))
                Console.WriteLine($"summary:    {sp.Summary}");
            if (sp.Tags is { Count: > 0 })
                Console.WriteLine($"tags:       {string.Join(", ", sp.Tags)}");
            if (sp.Provenance is { } prov && prov.Count > 0)
            {
                Console.WriteLine("provenance:");
                foreach (var (k, v) in prov)
                {
                    var val = v.ValueKind == System.Text.Json.JsonValueKind.String ? v.GetString() ?? "" : v.ToString();
                    if (val.Length > 200) val = val[..200] + "…";
                    Console.WriteLine($"  {k}: {val}");
                }
            }
        }

        // Container view: metadata + linked objects
        if (container is not null)
        {
            Console.WriteLine($"container:  {container.ContainerKind}");
            if (container.Metadata is { } meta && meta.Count > 0)
            {
                Console.WriteLine("metadata:");
                foreach (var (k, v) in meta)
                {
                    var val = v.ValueKind == System.Text.Json.JsonValueKind.String ? v.GetString() ?? "" : v.ToString();
                    Console.WriteLine($"  {k}: {val}");
                }
            }

            // Walk the link chain and print members
            int memberCount = 0;
            var cur = container.HeadLinknodeId;
            int guard = 0;
            var members = new List<(string ObjectId, string Kind, string? Summary)>();
            while (cur != null && guard < 5000)
            {
                if (!store.LinkNodes.TryGetValue(cur, out var ln)) break;
                if (store.Objects.TryGetValue(ln.ObjectId, out var memberObj))
                {
                    members.Add((memberObj.ObjectId, memberObj.ObjectKind, memberObj.SemanticPayload?.Summary));
                }
                cur = ln.NextLinknodeId;
                guard++;
                memberCount++;
            }

            Console.WriteLine($"members ({memberCount}):");
            foreach (var (id, kind, summary) in members.Take(20))
            {
                var summaryStr = string.IsNullOrWhiteSpace(summary) ? "" : $"  — {summary[..Math.Min(80, summary.Length)]}";
                Console.WriteLine($"  [{kind}] {id}{summaryStr}");
            }
            if (members.Count > 20)
                Console.WriteLine($"  … and {members.Count - 20} more");
        }

        return 0;
    }

    public int AtlasSearch(string dbPath, string query, int top = 20)
    {
        var store = RequireAmsStore(dbPath, "atlas-search");
        if (store is null) return 1;

        // Tokenize query for simple keyword matching
        var tokens = query.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(t => t.ToLowerInvariant())
            .ToHashSet();

        static int ScoreObject(ObjectRecord obj, HashSet<string> tokens)
        {
            int score = 0;
            if (obj.SemanticPayload?.Summary is { } s)
            {
                var lc = s.ToLowerInvariant();
                foreach (var t in tokens) if (lc.Contains(t)) score++;
            }
            if (obj.SemanticPayload?.Tags is { } tags)
            {
                foreach (var tag in tags)
                    foreach (var t in tokens)
                        if (tag.ToLowerInvariant().Contains(t)) score++;
            }
            if (obj.ObjectId.ToLowerInvariant().Contains(tokens.First())) score++;
            return score;
        }

        var hits = store.Objects.Values
            .Select(obj => (Obj: obj, Score: ScoreObject(obj, tokens)))
            .Where(x => x.Score > 0)
            .OrderByDescending(x => x.Score)
            .Take(top)
            .ToList();

        if (hits.Count == 0)
        {
            Console.WriteLine("(no results)");
            return 0;
        }

        foreach (var (obj, score) in hits)
        {
            var summary = obj.SemanticPayload?.Summary ?? "";
            if (summary.Length > 100) summary = summary[..100] + "…";
            Console.WriteLine($"[score={score}] {obj.ObjectId}  ({obj.ObjectKind})");
            if (!string.IsNullOrWhiteSpace(summary))
                Console.WriteLine($"  {summary}");
        }

        return 0;
    }

    public int AtlasExpand(string dbPath, string refId)
    {
        var store = RequireAmsStore(dbPath, "atlas-expand");
        if (store is null) return 1;

        // Resolve to an exact or prefix match
        string? resolvedId = null;
        if (store.Objects.ContainsKey(refId))
        {
            resolvedId = refId;
        }
        else
        {
            var matches = store.Objects.Keys
                .Where(k => k.StartsWith(refId, StringComparison.OrdinalIgnoreCase))
                .ToList();
            if (matches.Count == 1)
                resolvedId = matches[0];
            else if (matches.Count > 1)
            {
                Console.Error.WriteLine($"error: ambiguous ref-id prefix '{refId}' matches {matches.Count} objects.");
                foreach (var m in matches.Take(10))
                    Console.Error.WriteLine($"  {m}");
                return 1;
            }
            else
            {
                Console.Error.WriteLine($"error: no object found for ref-id '{refId}'.");
                return 1;
            }
        }

        var obj = store.Objects[resolvedId];
        Console.WriteLine($"=== expand: {resolvedId} ({obj.ObjectKind}) ===");

        // Show containers this object belongs to
        var parentContainers = store.Containers.Values
            .Where(c =>
            {
                var cur = c.HeadLinknodeId;
                int g = 0;
                while (cur != null && g < 5000)
                {
                    if (!store.LinkNodes.TryGetValue(cur, out var ln)) break;
                    if (ln.ObjectId == resolvedId) return true;
                    cur = ln.NextLinknodeId;
                    g++;
                }
                return false;
            })
            .ToList();

        Console.WriteLine($"member-of ({parentContainers.Count}):");
        foreach (var c in parentContainers.Take(20))
        {
            var cSummary = store.Objects.TryGetValue(c.ContainerId, out var cObj) && cObj.SemanticPayload?.Summary is { } s
                ? $"  — {s[..Math.Min(80, s.Length)]}"
                : "";
            Console.WriteLine($"  [{c.ContainerKind}] {c.ContainerId}{cSummary}");
        }

        // If this object is itself a container, show its direct children
        if (store.Containers.TryGetValue(resolvedId, out var selfContainer))
        {
            var children = new List<string>();
            var cur = selfContainer.HeadLinknodeId;
            int guard = 0;
            while (cur != null && guard < 200)
            {
                if (!store.LinkNodes.TryGetValue(cur, out var ln)) break;
                children.Add(ln.ObjectId);
                cur = ln.NextLinknodeId;
                guard++;
            }

            Console.WriteLine($"children ({children.Count}):");
            foreach (var childId in children.Take(20))
            {
                var childKind = store.Objects.TryGetValue(childId, out var childObj) ? childObj.ObjectKind : "?";
                Console.WriteLine($"  [{childKind}] {childId}");
            }
            if (children.Count > 20)
                Console.WriteLine($"  … and {children.Count - 20} more");
        }

        return 0;
    }

    public int ListSessions(string dbPath, string? since, int n)
    {
        var runtime = _runtimeFactory.Load(dbPath);

        if (runtime.GraphStore is not AmsGraphStoreAdapter amsAdapter)
        {
            Console.Error.WriteLine("error: list-sessions requires the AMS backend.");
            return 1;
        }

        var innerStore = amsAdapter.InnerStore;

        static string ReadStr(System.Collections.Generic.Dictionary<string, System.Text.Json.JsonElement>? meta, string key)
        {
            if (meta is null) return string.Empty;
            return meta.TryGetValue(key, out var el) ? el.GetString() ?? string.Empty : string.Empty;
        }

        DateTimeOffset sinceDate = DateTimeOffset.MinValue;
        if (!string.IsNullOrWhiteSpace(since))
        {
            if (!DateTimeOffset.TryParse(since, out sinceDate))
                throw new ArgumentException($"Invalid --since date: '{since}'");
        }

        var sessions = innerStore.Containers.Values
            .Where(c => c.ContainerKind == "chat_session")
            .Select(c =>
            {
                var startedAt = ReadStr(c.Metadata, "started_at");
                DateTimeOffset.TryParse(startedAt, out var dt);
                var title = ReadStr(c.Metadata, "title");
                var guid = c.ContainerId.Contains(':')
                    ? c.ContainerId[(c.ContainerId.IndexOf(':') + 1)..]
                    : c.ContainerId;

                // Count messages by walking link chain (up to 500 nodes)
                int msgCount = 0;
                var cur = c.HeadLinknodeId;
                int guard = 0;
                while (cur != null && guard < 500)
                {
                    if (!innerStore.LinkNodes.TryGetValue(cur, out var ln)) break;
                    msgCount++;
                    cur = ln.NextLinknodeId;
                    guard++;
                }

                return (Dt: dt, Guid: guid, Title: title, MsgCount: msgCount);
            })
            .Where(s => s.Dt >= sinceDate)
            .OrderByDescending(s => s.Dt)
            .Take(n)
            .ToList();

        foreach (var s in sessions)
        {
            var date   = s.Dt == DateTimeOffset.MinValue ? "????-??-??" : s.Dt.ToString("yyyy-MM-dd");
            var guid8  = s.Guid.Length >= 8 ? s.Guid[..8] : s.Guid;
            var title  = string.IsNullOrWhiteSpace(s.Title) ? "(untitled)" : s.Title;
            if (title.Length > 50) title = title[..47] + "…";
            Console.WriteLine($"{date}  {guid8}  {title,-52}  ({s.MsgCount} msgs)");
        }

        return 0;
    }

    public int ShowSession(string dbPath, string idPrefix, string? htmlPath = null)
    {
        var runtime = _runtimeFactory.Load(dbPath);

        if (runtime.GraphStore is not AmsGraphStoreAdapter amsAdapter)
        {
            Console.Error.WriteLine("error: show-session requires the AMS backend.");
            return 1;
        }

        var innerStore = amsAdapter.InnerStore;

        // Find matching container by full GUID or prefix
        var matches = innerStore.Containers.Values
            .Where(c => c.ContainerKind == "chat_session")
            .Where(c =>
            {
                var guid = c.ContainerId.Contains(':')
                    ? c.ContainerId[(c.ContainerId.IndexOf(':') + 1)..]
                    : c.ContainerId;
                return guid.StartsWith(idPrefix, StringComparison.OrdinalIgnoreCase);
            })
            .ToList();

        if (matches.Count == 0)
        {
            Console.Error.WriteLine($"error: no chat session found with id prefix '{idPrefix}'.");
            return 1;
        }
        if (matches.Count > 1)
        {
            Console.Error.WriteLine($"error: ambiguous prefix '{idPrefix}' matches {matches.Count} sessions. Use a longer prefix.");
            foreach (var m in matches)
                Console.Error.WriteLine($"  {m.ContainerId}");
            return 1;
        }

        var container = matches[0];

        // Open the HTML browser to this session if --html was supplied.
        // Regenerate the HTML with this session's ancestors pre-opened so the
        // browser lands directly on it without any JS timing issues.
        if (!string.IsNullOrWhiteSpace(htmlPath))
        {
            var rawId  = container.ContainerId.Contains(':')
                ? container.ContainerId[(container.ContainerId.IndexOf(':') + 1)..]
                : container.ContainerId;
            var anchor = $"chat-session-{rawId}";
            var html   = new HtmlRenderer().Render(new AmsViewModelProjector(amsAdapter.InnerStore, amsAdapter).Project());
            var absHtmlPath = Path.GetFullPath(htmlPath);
            File.WriteAllText(absHtmlPath, html, System.Text.Encoding.UTF8);
            var uri = new Uri(absHtmlPath).AbsoluteUri + "#" + anchor;
            System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo(uri) { UseShellExecute = true });
            Console.Error.WriteLine($"opened: {uri}");
        }

        const int maxMsgLen = 2000;

        var cur = container.HeadLinknodeId;
        int guard = 0;
        while (cur != null && guard < 10000)
        {
            if (!innerStore.LinkNodes.TryGetValue(cur, out var ln)) break;

            if (innerStore.Objects.TryGetValue(ln.ObjectId, out var obj))
            {
                var prov = obj.SemanticPayload?.Provenance;
                if (prov != null)
                {
                    string direction = prov.TryGetValue("direction", out var dirEl) ? dirEl.GetString() ?? "" : "";
                    string text = prov.TryGetValue("text", out var textEl) ? textEl.GetString() ?? "" : "";
                    string ts = prov.TryGetValue("ts", out var tsEl) ? tsEl.GetString() ?? "" : "";

                    string timeStr = "";
                    if (!string.IsNullOrWhiteSpace(ts) && DateTimeOffset.TryParse(ts, out var dt))
                        timeStr = $"[{dt:HH:mm}] ";

                    string role = direction == "in" ? "USER" : "CLAUDE";
                    if (text.Length > maxMsgLen)
                        text = text[..maxMsgLen] + "…";

                    Console.WriteLine($"{timeStr}{role}: {text}");
                    Console.WriteLine();
                }
            }

            cur = ln.NextLinknodeId;
            guard++;
        }

        return 0;
    }

    public int Dream(string dbPath, int topicK, int threadK, int decisionK, int invariantK, bool dryRun)
    {
        Console.Error.WriteLine("error: dream is unavailable in the current AMS.Core-only build.");
        return 1;
    }

    public int DreamRelax(string dbPath, int maxSteps, int maxAccepted, double temperature, int seed, bool dryRun)
    {
        Console.Error.WriteLine("error: dream-relax is unavailable in the current AMS.Core-only build.");
        return 1;
    }

    public int DebugAms(string dbPath, string? outPath, string? openAnchor = null)
    {
        var runtime = _runtimeFactory.Load(dbPath);

        if (runtime.GraphStore is not AmsGraphStoreAdapter amsStore)
        {
            Console.Error.WriteLine("error: debug-ams requires the AMS backend (--backend ams).");
            return 1;
        }

        Console.Write(amsStore.RenderTextInspector());

        if (!string.IsNullOrWhiteSpace(openAnchor) && string.IsNullOrWhiteSpace(outPath))
        {
            Console.Error.WriteLine("error: debug-ams --open-anchor requires --out <path>.");
            return 1;
        }

        if (outPath is not null)
        {
            var vm   = new AmsViewModelProjector(amsStore.InnerStore, amsStore, runtime.Payloads).Project();
            var html = new HtmlRenderer().Render(vm);
            var absOutPath = Path.GetFullPath(outPath);
            File.WriteAllText(absOutPath, html, System.Text.Encoding.UTF8);
            Console.Error.WriteLine($"HTML inspector written to: {absOutPath}");

            if (!string.IsNullOrWhiteSpace(openAnchor))
            {
                var uri = new Uri(absOutPath).AbsoluteUri + "#" + openAnchor.Trim();
                System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo(uri) { UseShellExecute = true });
                Console.Error.WriteLine($"opened: {uri}");
            }
        }

        return 0;
    }

    private static void WriteThreadStatus(TaskGraphOverview overview)
    {
        Console.WriteLine("# TASK GRAPH");
        if (overview.ActiveThread is null)
        {
            Console.WriteLine("active_thread=(none)");
            Console.WriteLine($"parked={overview.ParkedThreads.Count}");
            return;
        }

        Console.WriteLine($"active_thread={overview.ActiveThread.ThreadId}");
        Console.WriteLine($"title={overview.ActiveThread.Title}");
        Console.WriteLine($"active_path={string.Join(" -> ", overview.ActivePath.Select(x => x.ThreadId))}");
        Console.WriteLine($"current_step={overview.ActiveThread.CurrentStep}");
        Console.WriteLine($"next_command={overview.ActiveThread.NextCommand}");
        Console.WriteLine($"parked={overview.ParkedThreads.Count}");
        Console.WriteLine($"checkpoints={overview.ActiveThread.Checkpoints.Count}");
        Console.WriteLine($"artifacts={overview.ActiveThread.Artifacts.Count}");
    }

    private static void WriteThreadAction(string action, TaskGraphCommandResult result)
    {
        Console.WriteLine($"action={action}");
        Console.WriteLine($"thread_id={result.Thread.ThreadId}");
        Console.WriteLine($"title={result.Thread.Title}");
        Console.WriteLine($"status={result.Thread.Status}");
        Console.WriteLine($"current_step={result.Thread.CurrentStep}");
        Console.WriteLine($"next_command={result.Thread.NextCommand}");
        if (result.Checkpoint is not null)
            Console.WriteLine($"checkpoint={result.Checkpoint.CheckpointObjectId}");
        if (result.ResumedCheckpoint is not null)
        {
            Console.WriteLine($"resumed_checkpoint={result.ResumedCheckpoint.CheckpointObjectId}");
            Console.WriteLine($"resumed_step={result.ResumedCheckpoint.CurrentStep}");
            Console.WriteLine($"resumed_next={result.ResumedCheckpoint.NextCommand}");
        }
        WriteThreadStatus(result.Overview);
    }

    private static void WriteAgentCapability(AgentCapabilityEntryInfo entry)
    {
        Console.WriteLine($"entry_id={entry.EntryId}");
        Console.WriteLine($"agent={entry.Agent}");
        Console.WriteLine($"capability_key={entry.CapabilityKey}");
        Console.WriteLine($"state={entry.State}");
        Console.WriteLine($"problem_key={entry.ProblemKey}");
        Console.WriteLine($"equivalence_group_key={entry.EquivalenceGroupKey}");
        Console.WriteLine($"group_object_id={entry.GroupObjectId}");
        Console.WriteLine($"summary={entry.Summary}");
        Console.WriteLine($"notes={entry.Notes}");
        Console.WriteLine($"created_at={entry.CreatedAt:O}");
        Console.WriteLine($"updated_at={entry.UpdatedAt:O}");
    }

    private static void TryMirrorActiveThreadMarkdown(TaskGraphService service)
    {
        var repoRoot = Environment.CurrentDirectory;
        var activeThreadPath = Path.Combine(repoRoot, "docs", "architecture", "active-thread.md");
        if (File.Exists(activeThreadPath))
            service.MirrorActiveThreadMarkdown(repoRoot);
    }


    private static HashSet<Guid>? BuildCandidateSet(CommandRuntime runtime, string? memAnchorFilter)
    {
        if (string.IsNullOrWhiteSpace(memAnchorFilter))
            return null;

        var filterLower = memAnchorFilter.ToLowerInvariant();
        var set = new HashSet<Guid>();
        foreach (var memAnchorId in runtime.GraphStore.AllMemAnchors)
        {
            if (!runtime.GraphStore.TryGetMemAnchorName(memAnchorId, out var name))
                continue;
            if (!name.ToLowerInvariant().Contains(filterLower))
                continue;

            foreach (var cardId in runtime.GraphStore.CardsIn(memAnchorId))
                set.Add(cardId);
        }

        return set;
    }

    private static AgentQueryContext? BuildAgentQueryContext(
        AmsStore store,
        string? currentNodeId,
        string? parentNodeId,
        string? grandparentNodeId,
        string? agentRole,
        string? mode,
        string? failureBucket,
        IReadOnlyList<string> activeArtifacts,
        int traversalBudget,
        bool noActiveThreadContext)
    {
        var lineage = new List<AgentLineageScope>();

        if (!string.IsNullOrWhiteSpace(currentNodeId) || !string.IsNullOrWhiteSpace(parentNodeId) || !string.IsNullOrWhiteSpace(grandparentNodeId))
        {
            TryAddExplicitScope(store, lineage, "self", currentNodeId, activeArtifacts);
            TryAddExplicitScope(store, lineage, "parent", parentNodeId, []);
            TryAddExplicitScope(store, lineage, "grandparent", grandparentNodeId, []);
        }
        else if (!noActiveThreadContext)
        {
            lineage.AddRange(BuildActiveThreadLineage(store, activeArtifacts));
        }

        var normalizedArtifacts = activeArtifacts
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();
        if (lineage.Count == 0 && string.IsNullOrWhiteSpace(agentRole) && string.IsNullOrWhiteSpace(mode) && string.IsNullOrWhiteSpace(failureBucket) && normalizedArtifacts.Count == 0)
            return null;

        return new AgentQueryContext(
            lineage,
            agentRole ?? "implementer",
            mode ?? "build",
            failureBucket,
            normalizedArtifacts,
            Math.Max(1, traversalBudget),
            lineage.Count == 0 ? "explicit" : (!string.IsNullOrWhiteSpace(currentNodeId) ? "explicit" : "active-task-graph"));
    }

    private static List<AgentLineageScope> BuildActiveThreadLineage(AmsStore store, IReadOnlyList<string> activeArtifacts)
    {
        var lineage = new List<AgentLineageScope>();
        if (!store.Containers.ContainsKey("task-graph:active"))
            return lineage;

        var activeObjectId = store.IterateForward("task-graph:active")
            .Select(x => x.ObjectId)
            .FirstOrDefault();
        if (string.IsNullOrWhiteSpace(activeObjectId))
            return lineage;

        var current = activeObjectId;
        var levels = new[] { "self", "parent", "grandparent" };
        for (var i = 0; i < levels.Length && !string.IsNullOrWhiteSpace(current); i++)
        {
            var scope = BuildScopeFromNode(store, current!, levels[i], i == 0 ? activeArtifacts : []);
            if (scope is null)
                break;

            lineage.Add(scope);
            current = ResolveParentThreadObjectId(store, current!);
        }

        return lineage;
    }

    private static void TryAddExplicitScope(AmsStore store, List<AgentLineageScope> lineage, string level, string? rawNodeId, IReadOnlyList<string> extraArtifacts)
    {
        if (string.IsNullOrWhiteSpace(rawNodeId))
            return;

        var scope = BuildScopeFromNode(store, rawNodeId.Trim(), level, extraArtifacts);
        if (scope is not null)
            lineage.Add(scope);
    }

    private static AgentLineageScope? BuildScopeFromNode(AmsStore store, string rawNodeId, string level, IReadOnlyList<string> extraArtifacts)
    {
        var objectId = ResolveThreadObjectId(store, rawNodeId);
        if (store.Objects.TryGetValue(objectId, out var obj) && obj.ObjectKind == "task_thread")
        {
            var prov = obj.SemanticPayload?.Provenance;
            var threadId = ReadString(prov, "thread_id") ?? Suffix(objectId);
            var artifacts = ReadArtifactRefs(store, threadId)
                .Concat(extraArtifacts)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Distinct(StringComparer.Ordinal)
                .ToList();
            return new AgentLineageScope(
                level,
                objectId,
                threadId,
                obj.SemanticPayload?.Summary ?? threadId,
                ReadString(prov, "current_step") ?? string.Empty,
                ReadString(prov, "next_command") ?? string.Empty,
                EmptyToNull(ReadString(prov, "branch_off_anchor")),
                artifacts);
        }

        return new AgentLineageScope(
            level,
            objectId,
            rawNodeId,
            rawNodeId,
            string.Empty,
            string.Empty,
            null,
            extraArtifacts.Where(x => !string.IsNullOrWhiteSpace(x)).Distinct(StringComparer.Ordinal).ToList());
    }

    private static string ResolveThreadObjectId(AmsStore store, string rawNodeId)
    {
        if (store.Objects.ContainsKey(rawNodeId))
            return rawNodeId;

        var candidate = $"task-thread:{rawNodeId}";
        return store.Objects.ContainsKey(candidate) ? candidate : rawNodeId;
    }

    private static string? ResolveParentThreadObjectId(AmsStore store, string threadObjectId)
    {
        if (!store.Objects.TryGetValue(threadObjectId, out var obj))
            return null;

        var parentThreadId = EmptyToNull(ReadString(obj.SemanticPayload?.Provenance, "parent_thread_id"));
        return string.IsNullOrWhiteSpace(parentThreadId) ? null : $"task-thread:{parentThreadId}";
    }

    private static IReadOnlyList<string> ReadArtifactRefs(AmsStore store, string threadId)
    {
        var containerId = $"task-thread:{threadId}:artifacts";
        if (!store.Containers.ContainsKey(containerId))
            return [];

        return store.IterateForward(containerId)
            .Select(x => x.ObjectId)
            .Where(x => store.Objects.ContainsKey(x))
            .Select(x => ReadString(store.Objects[x].SemanticPayload?.Provenance, "artifact_ref") ?? string.Empty)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();
    }

    private static string Suffix(string id)
        => id.Contains(':', StringComparison.Ordinal) ? id[(id.IndexOf(':', StringComparison.Ordinal) + 1)..] : id;

    private static string? EmptyToNull(string? value)
        => string.IsNullOrWhiteSpace(value) ? null : value;

    private static string? ReadString(IReadOnlyDictionary<string, System.Text.Json.JsonElement>? metadata, string key)
        => metadata is not null && metadata.TryGetValue(key, out var element)
            ? (element.ValueKind == System.Text.Json.JsonValueKind.String ? element.GetString() : element.ToString())
            : null;

    private static void WriteRoadmapBucket(StringWriter sw, string heading, List<(string Title, string Area, string Description)> items)
    {
        if (items.Count == 0)
            return;

        sw.WriteLine($"## {heading}");
        foreach (var item in items)
        {
            var areaText = string.IsNullOrWhiteSpace(item.Area) ? string.Empty : $" (area: {item.Area})";
            sw.WriteLine($"- **{item.Title}**{areaText} – {item.Description}");
        }
        sw.WriteLine();
    }
}

