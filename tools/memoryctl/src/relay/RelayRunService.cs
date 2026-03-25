using System.Text;
using AMS.Core;

namespace MemoryCtl;

internal sealed record RelayRunResult(
    string TaskThreadId,
    string RunArtifactRef,
    string Verdict,
    string StopReason,
    string? RepairThreadId,
    IReadOnlyList<string> ArchiveRefs);

internal sealed class RelayRunService
{
    private const string RelayFactoryRootPath = "smartlist/architecture/agent-mirroring/triple-buffer-relay-factory";
    private const string RelayFactoryTemplatePath = "smartlist/architecture/agent-mirroring/triple-buffer-relay-factory/05-smartlist-bucket-template";
    private const string RelayHandoffFactoryPath = "smartlist/architecture/agent-mirroring/cross-agent-handoff-factory";
    private const string RelayRunsRootPath = "smartlist/execution-plan/relay-runs";
    private const string ArchiveRootPath = "smartlist/archive/relay-repairs";
    private const string CreatedBy = "relay-run";

    private readonly AmsStore _store;
    private readonly TaskGraphService _taskGraph;
    private readonly SmartListService _smartLists;

    public RelayRunService(AmsStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _taskGraph = new TaskGraphService(store);
        _smartLists = new SmartListService(store);
    }

    public RelayRunResult Run(string taskThreadId, string? injectHandoffFailureMode, DateTimeOffset nowUtc)
    {
        if (string.IsNullOrWhiteSpace(taskThreadId))
            throw new ArgumentException("task thread id is required", nameof(taskThreadId));

        var overview = _taskGraph.Inspect();
        if (string.Equals(overview.ActiveThread?.ThreadId, taskThreadId, StringComparison.Ordinal))
            throw new InvalidOperationException("relay-run cannot target the active thread; use a disposable task thread instead.");

        var existing = _taskGraph.FindThread(taskThreadId);
        var proofThread = _taskGraph.EnsureParkedThread(
            existing?.Title ?? BuildTitle(taskThreadId),
            existing?.CurrentStep ?? "Run bounded triple-buffer relay proof on the disposable archive-freeze task graph.",
            existing?.NextCommand ?? $"memoryctl relay-run --db <path> --task-thread-id {taskThreadId}",
            taskThreadId,
            existing?.BranchOffAnchor ?? RelayFactoryRootPath,
            existing?.ParentThreadId,
            null,
            nowUtc);

        var runId = BuildRunId(nowUtc);
        var runBucketPath = $"{RelayRunsRootPath}/{NormalizePathSegment(taskThreadId)}/{runId}";
        var managerBucketPath = $"{runBucketPath}/01-manager-requirements";
        var implementerBucketPath = $"{runBucketPath}/02-implementer-receipt";
        var judgeBucketPath = $"{runBucketPath}/03-judge-verdict";
        var stopBucketPath = $"{runBucketPath}/04-stop-reason";
        var archiveBucketPath = $"{runBucketPath}/05-archive-refs";
        var repairBucketPath = $"{runBucketPath}/06-repair-thread";

        EnsureFactoryBuckets(nowUtc, RelayFactoryRootPath, RelayFactoryTemplatePath, RelayHandoffFactoryPath);
        CreateRunBuckets(runBucketPath, managerBucketPath, implementerBucketPath, judgeBucketPath, stopBucketPath, archiveBucketPath, repairBucketPath, nowUtc);
        _taskGraph.AttachArtifact(proofThread.ThreadId, RelayFactoryRootPath, nowUtc);
        _taskGraph.AttachArtifact(proofThread.ThreadId, RelayFactoryTemplatePath, nowUtc);

        _smartLists.CreateNote(
            "Manager requirements",
            BuildManagerRequirements(proofThread.ThreadId, proofThread.Title, runId),
            [managerBucketPath],
            durable: true,
            CreatedBy,
            nowUtc);

        var failureSpec = ResolveFailureSpec(injectHandoffFailureMode);
        _smartLists.CreateNote(
            "Implementer receipt",
            BuildImplementerReceipt(proofThread.ThreadId, runId, failureSpec),
            [implementerBucketPath],
            durable: true,
            CreatedBy,
            nowUtc);

        string verdict;
        string stopReason;
        string? repairThreadId = null;
        var archiveRefs = new List<string>();

        if (failureSpec is null)
        {
            verdict = "success";
            stopReason = "completed";
        }
        else
        {
            EnsureFactoryBuckets(nowUtc, failureSpec.HandoffFactoryPath, failureSpec.RelayFactoryPath);
            verdict = "handoff-factory defect";
            stopReason = $"handoff-contract-failed:{failureSpec.Mode}";

            var suppressedArchiveBucketPath = $"{ArchiveRootPath}/{NormalizePathSegment(taskThreadId)}/{runId}";
            _smartLists.CreateBucket(suppressedArchiveBucketPath, durable: true, CreatedBy, nowUtc);
            _smartLists.CreateNote(
                "Archived handoff lineage",
                BuildArchivedLineageNote(runId, failureSpec),
                [suppressedArchiveBucketPath],
                durable: true,
                CreatedBy,
                nowUtc);
            _smartLists.Attach(suppressedArchiveBucketPath, failureSpec.HandoffFactoryPath, CreatedBy, nowUtc);
            _smartLists.Attach(suppressedArchiveBucketPath, failureSpec.RelayFactoryPath, CreatedBy, nowUtc);
            _smartLists.SetRetrievalVisibility(
                suppressedArchiveBucketPath,
                SmartListService.RetrievalVisibilitySuppressed,
                recursive: true,
                includeNotes: true,
                includeRollups: true,
                nowUtc);

            repairThreadId = BuildRepairThreadId(taskThreadId, runId);
            var repairThread = _taskGraph.EnsureParkedThread(
                $"{proofThread.Title} Repair {failureSpec.Label}",
                $"Repair {failureSpec.ContractSlot} handoff contract for relay run {runId}.",
                $"Inspect {suppressedArchiveBucketPath} and update {failureSpec.HandoffFactoryPath} plus {failureSpec.RelayFactoryPath}.",
                repairThreadId,
                failureSpec.HandoffFactoryPath,
                proofThread.ThreadId,
                suppressedArchiveBucketPath,
                nowUtc);
            _taskGraph.AttachArtifact(repairThread.ThreadId, failureSpec.HandoffFactoryPath, nowUtc);
            _taskGraph.AttachArtifact(repairThread.ThreadId, failureSpec.RelayFactoryPath, nowUtc);
            _taskGraph.AttachArtifact(repairThread.ThreadId, runBucketPath, nowUtc);

            _taskGraph.AttachArtifact(proofThread.ThreadId, suppressedArchiveBucketPath, nowUtc);
            _taskGraph.AttachArtifact(proofThread.ThreadId, $"task-thread:{repairThread.ThreadId}", nowUtc);

            archiveRefs.Add(suppressedArchiveBucketPath);
            archiveRefs.Add(failureSpec.HandoffFactoryPath);
            archiveRefs.Add(failureSpec.RelayFactoryPath);

            _smartLists.CreateNote(
                "Archive refs",
                BuildArchiveRefsNote(suppressedArchiveBucketPath, failureSpec),
                [archiveBucketPath],
                durable: true,
                CreatedBy,
                nowUtc);
            _smartLists.CreateNote(
                "Repair thread",
                BuildRepairThreadNote(repairThread.ThreadId, failureSpec, suppressedArchiveBucketPath),
                [repairBucketPath],
                durable: true,
                CreatedBy,
                nowUtc);
        }

        _smartLists.CreateNote(
            "Judge verdict",
            BuildJudgeVerdict(proofThread.ThreadId, runId, verdict, stopReason, failureSpec),
            [judgeBucketPath],
            durable: true,
            CreatedBy,
            nowUtc);
        _smartLists.CreateNote(
            "Stop reason",
            BuildStopReason(stopReason, repairThreadId),
            [stopBucketPath],
            durable: true,
            CreatedBy,
            nowUtc);

        _smartLists.SetRollup(
            runBucketPath,
            verdict == "success"
                ? $"Bounded relay run completed successfully for {proofThread.ThreadId}."
                : $"Bounded relay run stopped after a {verdict} classification for {proofThread.ThreadId}.",
            failureSpec is null
                ? "Stores the manager, implementer, judge, and stop receipts for the proof run."
                : "Stores the proof-run receipts plus the archive and repair-thread escalation triggered by the judge.",
            failureSpec is null
                ? "Stop here for the proof verdict. Descend only when you need the detailed receipts."
                : "Stop here for the failure verdict. Descend when you need the archive lineage or repair thread details.",
            BuildRunHighlights(managerBucketPath, implementerBucketPath, judgeBucketPath, stopBucketPath, failureSpec is null ? null : archiveBucketPath),
            durable: true,
            CreatedBy,
            nowUtc);

        var finalCurrentStep = failureSpec is null
            ? $"Relay proof run {runId} completed successfully."
            : $"Relay proof run {runId} stopped after handoff failure {failureSpec.Mode}.";
        var finalNextCommand = failureSpec is null
            ? $"Inspect {runBucketPath} for the durable run receipts."
            : $"Inspect repair thread {repairThreadId} and archive refs in {archiveRefs[0]}.";
        _taskGraph.CheckpointThread(proofThread.ThreadId, finalCurrentStep, finalNextCommand, proofThread.BranchOffAnchor, runBucketPath, nowUtc);

        return new RelayRunResult(proofThread.ThreadId, runBucketPath, verdict, stopReason, repairThreadId, archiveRefs);
    }

    private void CreateRunBuckets(
        string runBucketPath,
        string managerBucketPath,
        string implementerBucketPath,
        string judgeBucketPath,
        string stopBucketPath,
        string archiveBucketPath,
        string repairBucketPath,
        DateTimeOffset nowUtc)
    {
        _smartLists.CreateBucket(runBucketPath, durable: true, CreatedBy, nowUtc);
        _smartLists.CreateBucket(managerBucketPath, durable: true, CreatedBy, nowUtc);
        _smartLists.CreateBucket(implementerBucketPath, durable: true, CreatedBy, nowUtc);
        _smartLists.CreateBucket(judgeBucketPath, durable: true, CreatedBy, nowUtc);
        _smartLists.CreateBucket(stopBucketPath, durable: true, CreatedBy, nowUtc);
        _smartLists.CreateBucket(archiveBucketPath, durable: true, CreatedBy, nowUtc);
        _smartLists.CreateBucket(repairBucketPath, durable: true, CreatedBy, nowUtc);
    }

    private void EnsureFactoryBuckets(DateTimeOffset nowUtc, params string[] paths)
    {
        foreach (var path in paths.Where(x => !string.IsNullOrWhiteSpace(x)).Distinct(StringComparer.Ordinal))
            _smartLists.CreateBucket(path, durable: true, CreatedBy, nowUtc);
    }

    private static List<SmartListRollupChild> BuildRunHighlights(
        string managerBucketPath,
        string implementerBucketPath,
        string judgeBucketPath,
        string stopBucketPath,
        string? archiveBucketPath)
    {
        var highlights = new List<SmartListRollupChild>
        {
            new(managerBucketPath, "manager requirements and proof target"),
            new(implementerBucketPath, "implementer receipt for the bounded relay attempt"),
            new(judgeBucketPath, "judge verdict and failure classification"),
            new(stopBucketPath, "stop reason recorded for the run")
        };
        if (!string.IsNullOrWhiteSpace(archiveBucketPath))
            highlights.Add(new SmartListRollupChild(archiveBucketPath!, "archive refs used to seed the repair thread"));
        return highlights;
    }

    private static string BuildManagerRequirements(string threadId, string title, string runId)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"run_id: {runId}");
        sb.AppendLine($"task_thread_id: {threadId}");
        sb.AppendLine($"task_title: {title}");
        sb.AppendLine("proof_target: bounded relay run on disposable archive-freeze task graph");
        sb.AppendLine("roles: manager -> implementer -> judge/controller");
        sb.AppendLine($"factory_root: {RelayFactoryRootPath}");
        sb.AppendLine($"template_root: {RelayFactoryTemplatePath}");
        sb.AppendLine("success_contract: write durable receipts, judge verdict, and stop reason without touching the live active thread");
        return sb.ToString().TrimEnd();
    }

    private static string BuildImplementerReceipt(string threadId, string runId, RelayFailureSpec? failureSpec)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"run_id: {runId}");
        sb.AppendLine($"task_thread_id: {threadId}");
        if (failureSpec is null)
        {
            sb.AppendLine("execution_mode: success-path proof");
            sb.AppendLine("handoff_status: implementer consumed the manager contract and produced a complete receipt");
            sb.AppendLine("delivery: ready for judge verification");
        }
        else
        {
            sb.AppendLine("execution_mode: injected-handoff-failure");
            sb.AppendLine($"failure_mode: {failureSpec.Mode}");
            sb.AppendLine($"contract_slot: {failureSpec.ContractSlot}");
            sb.AppendLine("delivery: stopped before blind continuation so the judge can classify the transfer defect");
        }

        return sb.ToString().TrimEnd();
    }

    private static string BuildJudgeVerdict(string threadId, string runId, string verdict, string stopReason, RelayFailureSpec? failureSpec)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"run_id: {runId}");
        sb.AppendLine($"task_thread_id: {threadId}");
        sb.AppendLine($"verdict: {verdict}");
        sb.AppendLine($"stop_reason: {stopReason}");
        sb.AppendLine("supported_failure_classes: implementation defect | handoff-factory defect | benchmark/oracle defect | evaluation blind spot");
        if (failureSpec is not null)
        {
            sb.AppendLine($"winning_classification: handoff-factory defect");
            sb.AppendLine($"contract_slot: {failureSpec.ContractSlot}");
            sb.AppendLine($"handoff_factory_ref: {failureSpec.HandoffFactoryPath}");
            sb.AppendLine($"relay_factory_ref: {failureSpec.RelayFactoryPath}");
        }
        else
        {
            sb.AppendLine("winning_classification: success");
            sb.AppendLine("transfer_oracle: satisfied");
            sb.AppendLine("acceptance_oracle: satisfied");
        }

        return sb.ToString().TrimEnd();
    }

    private static string BuildStopReason(string stopReason, string? repairThreadId)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"stop_reason: {stopReason}");
        sb.AppendLine($"repair_thread_id: {(string.IsNullOrWhiteSpace(repairThreadId) ? "none" : repairThreadId)}");
        sb.AppendLine("loop_budget: bounded-phase-1-single-run");
        return sb.ToString().TrimEnd();
    }

    private static string BuildArchiveRefsNote(string archiveBucketPath, RelayFailureSpec failureSpec)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"archive_bucket_path: {archiveBucketPath}");
        sb.AppendLine($"handoff_factory_ref: {failureSpec.HandoffFactoryPath}");
        sb.AppendLine($"relay_factory_ref: {failureSpec.RelayFactoryPath}");
        sb.AppendLine("purpose: explicit on-demand archive context for handoff repair");
        return sb.ToString().TrimEnd();
    }

    private static string BuildRepairThreadNote(string repairThreadId, RelayFailureSpec failureSpec, string archiveBucketPath)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"repair_thread_id: {repairThreadId}");
        sb.AppendLine($"contract_slot: {failureSpec.ContractSlot}");
        sb.AppendLine($"archive_bucket_path: {archiveBucketPath}");
        sb.AppendLine($"next_command: Inspect {archiveBucketPath} and update {failureSpec.HandoffFactoryPath} plus {failureSpec.RelayFactoryPath}.");
        return sb.ToString().TrimEnd();
    }

    private static string BuildArchivedLineageNote(string runId, RelayFailureSpec failureSpec)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"archive_marker: archived-lineage-{runId}");
        sb.AppendLine($"failure_mode: {failureSpec.Mode}");
        sb.AppendLine($"contract_slot: {failureSpec.ContractSlot}");
        sb.AppendLine($"handoff_factory_ref: {failureSpec.HandoffFactoryPath}");
        sb.AppendLine($"relay_factory_ref: {failureSpec.RelayFactoryPath}");
        sb.AppendLine("visibility: suppressed from default retrieval; inspect explicitly or follow judge-generated refs");
        return sb.ToString().TrimEnd();
    }

    private static RelayFailureSpec? ResolveFailureSpec(string? injectHandoffFailureMode)
    {
        if (string.IsNullOrWhiteSpace(injectHandoffFailureMode))
            return null;

        return injectHandoffFailureMode.Trim().ToLowerInvariant() switch
        {
            "missing-read-manifest" => new RelayFailureSpec(
                "missing-read-manifest",
                "Read Manifest",
                "required-read-manifest",
                "smartlist/architecture/agent-mirroring/cross-agent-handoff-factory/00-read-protocol",
                "smartlist/architecture/agent-mirroring/triple-buffer-relay-factory/02-relay-cycle/02-implementer-executes-handoff"),
            "missing-acceptance-oracle" => new RelayFailureSpec(
                "missing-acceptance-oracle",
                "Acceptance Oracle",
                "acceptance-oracle",
                "smartlist/architecture/agent-mirroring/cross-agent-handoff-factory/01-handoff-schema/03-acceptance-oracle",
                "smartlist/architecture/agent-mirroring/triple-buffer-relay-factory/06-evaluation-contract/01-acceptance-oracle"),
            _ => throw new ArgumentException(
                $"Unsupported --inject-handoff-failure '{injectHandoffFailureMode}'. Supported values: missing-read-manifest, missing-acceptance-oracle.")
        };
    }

    private static string BuildRunId(DateTimeOffset nowUtc)
        => $"run-{nowUtc:yyyyMMddHHmmssfff}-{Guid.NewGuid():N}"[..32];

    private static string BuildRepairThreadId(string taskThreadId, string runId)
        => $"{NormalizePathSegment(taskThreadId)}-repair-{runId[^8..]}";

    private static string BuildTitle(string threadId)
    {
        var parts = threadId
            .Split('-', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(Capitalize)
            .ToArray();
        return parts.Length == 0 ? "Relay Proof Thread" : string.Join(' ', parts);
    }

    private static string Capitalize(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;
        return char.ToUpperInvariant(value[0]) + value[1..];
    }

    private static string NormalizePathSegment(string value)
        => string.Concat(value.Trim().ToLowerInvariant().Select(ch => char.IsLetterOrDigit(ch) ? ch : '-')).Trim('-');

    private sealed record RelayFailureSpec(
        string Mode,
        string Label,
        string ContractSlot,
        string HandoffFactoryPath,
        string RelayFactoryPath);
}
