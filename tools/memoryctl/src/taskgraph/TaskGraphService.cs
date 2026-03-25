using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using AMS.Core;

namespace MemoryCtl;

internal sealed record TaskCheckpointInfo(
    string CheckpointObjectId,
    string Summary,
    string CurrentStep,
    string NextCommand,
    string? BranchOffAnchor,
    string? ArtifactRef,
    DateTimeOffset CreatedAt);

internal sealed record TaskArtifactInfo(
    string ArtifactObjectId,
    string Label,
    string ArtifactRef,
    DateTimeOffset CreatedAt);

internal sealed record TaskThreadInfo(
    string ThreadObjectId,
    string ThreadId,
    string Title,
    string Status,
    string? ParentThreadId,
    string? BranchOffAnchor,
    string CurrentStep,
    string NextCommand,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    IReadOnlyList<string> ChildThreadIds,
    IReadOnlyList<TaskCheckpointInfo> Checkpoints,
    IReadOnlyList<TaskArtifactInfo> Artifacts);

internal sealed record TaskGraphOverview(
    TaskThreadInfo? ActiveThread,
    IReadOnlyList<TaskThreadInfo> ActivePath,
    IReadOnlyList<TaskThreadInfo> ParkedThreads,
    IReadOnlyList<TaskThreadInfo> AllThreads);

internal sealed record TaskGraphCommandResult(
    TaskThreadInfo Thread,
    TaskGraphOverview Overview,
    TaskCheckpointInfo? Checkpoint = null,
    TaskCheckpointInfo? ResumedCheckpoint = null);

internal sealed class TaskGraphService
{
    private const string TaskGraphRootContainer = "task-graph";
    private const string TaskGraphActiveContainer = "task-graph:active";
    private const string TaskGraphParkedContainer = "task-graph:parked";
    private static readonly Regex IdRx = new("[^a-z0-9-]+", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private readonly AmsStore _store;
    private readonly RetrievalGraphProjector _retrievalGraph;

    public TaskGraphService(AmsStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _retrievalGraph = new RetrievalGraphProjector(store);
    }

    public TaskGraphCommandResult StartThread(
        string title,
        string currentStep,
        string nextCommand,
        string? threadId,
        string? branchOffAnchor,
        string? artifactRef,
        DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        var active = GetActiveThread();
        var checkpoint = AutoCheckpoint(active, nowUtc);

        if (active is not null)
            ParkThread(active, nowUtc);

        var thread = ResolveOrCreateThread(title, currentStep, nextCommand, threadId, branchOffAnchor, null, artifactRef, nowUtc);
        ActivateThread(thread, nowUtc);
        return new TaskGraphCommandResult(thread, Inspect(), checkpoint);
    }

    public TaskGraphCommandResult PushTangent(
        string title,
        string currentStep,
        string nextCommand,
        string? threadId,
        string? branchOffAnchor,
        string? artifactRef,
        DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        var active = GetActiveThread() ?? throw new InvalidOperationException("thread-push-tangent requires an active thread.");
        var parentCheckpoint = AutoCheckpoint(active, nowUtc);
        ParkThread(active, nowUtc);

        var tangent = ResolveOrCreateThread(title, currentStep, nextCommand, threadId, branchOffAnchor, active.ThreadId, artifactRef, nowUtc);
        LinkChild(active, tangent);
        ActivateThread(tangent, nowUtc);
        return new TaskGraphCommandResult(tangent, Inspect(), parentCheckpoint);
    }

    public TaskGraphCommandResult CheckpointActiveThread(
        string currentStep,
        string nextCommand,
        string? branchOffAnchor,
        string? artifactRef,
        DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        var active = GetActiveThread() ?? throw new InvalidOperationException("thread-checkpoint requires an active thread.");
        UpdateThread(active, active.Title, currentStep, nextCommand, branchOffAnchor ?? active.BranchOffAnchor, active.ParentThreadId, nowUtc);
        var checkpoint = CreateCheckpoint(active.ThreadId, currentStep, nextCommand, branchOffAnchor ?? active.BranchOffAnchor, artifactRef, nowUtc);
        var refreshed = GetThread(active.ThreadId)!;
        return new TaskGraphCommandResult(refreshed, Inspect(), checkpoint);
    }

    public TaskGraphCommandResult PopThread(DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        var active = GetActiveThread() ?? throw new InvalidOperationException("thread-pop requires an active tangent.");
        if (string.IsNullOrWhiteSpace(active.ParentThreadId))
            throw new InvalidOperationException("thread-pop requires the active thread to have a parent tangent root.");

        var activeCheckpoint = AutoCheckpoint(active, nowUtc);
        ParkThread(active, nowUtc);

        var parent = GetThread(active.ParentThreadId!) ?? throw new InvalidOperationException($"parent thread '{active.ParentThreadId}' was not found.");
        ActivateThread(parent, nowUtc);

        var resumedCheckpoint = parent.Checkpoints
            .OrderByDescending(x => x.CreatedAt)
            .ThenByDescending(x => x.CheckpointObjectId, StringComparer.Ordinal)
            .FirstOrDefault();

        return new TaskGraphCommandResult(parent, Inspect(), activeCheckpoint, resumedCheckpoint);
    }

    public TaskGraphCommandResult ArchiveThread(string? threadId, DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        TaskThreadInfo? thread = null;
        if (!string.IsNullOrWhiteSpace(threadId))
            thread = GetThread(threadId!);
        else
            thread = GetActiveThread();

        if (thread is null)
            throw new InvalidOperationException("thread-archive requires either an existing --id or an active thread.");

        RemoveMembership(TaskGraphActiveContainer, thread.ThreadObjectId);
        RemoveMembership(TaskGraphParkedContainer, thread.ThreadObjectId);
        UpdateThreadStatus(thread.ThreadId, "archived", nowUtc);
        return new TaskGraphCommandResult(GetThread(thread.ThreadId)!, Inspect());
    }

    public TaskGraphOverview Inspect()
    {
        EnsureScaffold();
        var threads = LoadThreads().OrderBy(x => x.Title, StringComparer.OrdinalIgnoreCase).ThenBy(x => x.ThreadId, StringComparer.Ordinal).ToList();
        var byId = threads.ToDictionary(x => x.ThreadId, StringComparer.Ordinal);
        var active = GetActiveThread();
        var activePath = new List<TaskThreadInfo>();

        if (active is not null)
        {
            var cursor = active;
            var seen = new HashSet<string>(StringComparer.Ordinal);
            while (cursor is not null && seen.Add(cursor.ThreadId))
            {
                activePath.Add(cursor);
                cursor = !string.IsNullOrWhiteSpace(cursor.ParentThreadId) && byId.TryGetValue(cursor.ParentThreadId!, out var parent)
                    ? parent
                    : null;
            }

            activePath.Reverse();
        }

        var parked = threads
            .Where(x => string.Equals(x.Status, "parked", StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(x => x.UpdatedAt)
            .ThenBy(x => x.ThreadId, StringComparer.Ordinal)
            .ToList();

        return new TaskGraphOverview(active, activePath, parked, threads);
    }

    public TaskThreadInfo? FindThread(string threadId)
    {
        EnsureScaffold();
        return GetThread(threadId);
    }

    public TaskThreadInfo EnsureParkedThread(
        string title,
        string currentStep,
        string nextCommand,
        string? threadId,
        string? branchOffAnchor,
        string? parentThreadId,
        string? artifactRef,
        DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        var thread = ResolveOrCreateThread(title, currentStep, nextCommand, threadId, branchOffAnchor, parentThreadId, artifactRef, nowUtc);
        if (!string.Equals(thread.Status, "active", StringComparison.OrdinalIgnoreCase))
        {
            if (!_store.HasMembership(TaskGraphParkedContainer, thread.ThreadObjectId))
                _store.AddObject(TaskGraphParkedContainer, thread.ThreadObjectId);
            UpdateThreadStatus(thread.ThreadId, "parked", nowUtc);
        }

        if (!string.IsNullOrWhiteSpace(parentThreadId))
        {
            var parent = GetThread(parentThreadId!) ?? throw new InvalidOperationException($"parent thread '{parentThreadId}' was not found.");
            LinkChild(parent, thread);
        }

        return GetThread(thread.ThreadId)!;
    }

    public TaskCheckpointInfo CheckpointThread(
        string threadId,
        string currentStep,
        string nextCommand,
        string? branchOffAnchor,
        string? artifactRef,
        DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        var thread = GetThread(threadId) ?? throw new InvalidOperationException($"thread '{threadId}' was not found.");
        UpdateThread(thread, thread.Title, currentStep, nextCommand, branchOffAnchor ?? thread.BranchOffAnchor, thread.ParentThreadId, nowUtc);
        return CreateCheckpoint(thread.ThreadId, currentStep, nextCommand, branchOffAnchor ?? thread.BranchOffAnchor, artifactRef, nowUtc);
    }

    public TaskArtifactInfo AttachArtifact(string threadId, string artifactRef, DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        var thread = GetThread(threadId) ?? throw new InvalidOperationException($"thread '{threadId}' was not found.");
        return AddArtifact(thread.ThreadId, artifactRef, nowUtc);
    }

    public void MirrorActiveThreadMarkdown(string repoRoot)
    {
        var target = Path.Combine(repoRoot, "docs", "architecture", "active-thread.md");
        if (!Directory.Exists(Path.GetDirectoryName(target)!))
            return;

        var overview = Inspect();
        var sb = new StringBuilder();
        sb.AppendLine("# Active Thread Checkpoint");
        sb.AppendLine();
        sb.AppendLine("Date: " + DateTimeOffset.Now.ToString("yyyy-MM-dd (PT)"));
        sb.AppendLine("Status: Generated from AMS task graph");
        sb.AppendLine();
        sb.AppendLine("## Purpose");
        sb.AppendLine();
        sb.AppendLine("Compatibility mirror of the AMS SmartList task graph. The SmartList task graph is the source of truth during rollout.");
        sb.AppendLine();
        sb.AppendLine("## Active Thread");
        sb.AppendLine();

        if (overview.ActiveThread is null)
        {
            sb.AppendLine("No active thread.");
        }
        else
        {
            sb.AppendLine($"Thread ID: `{overview.ActiveThread.ThreadId}`");
            sb.AppendLine($"Goal: {overview.ActiveThread.Title}");
            if (!string.IsNullOrWhiteSpace(overview.ActiveThread.BranchOffAnchor))
                sb.AppendLine($"Branch-Off Anchor: `{overview.ActiveThread.BranchOffAnchor}`");
            sb.AppendLine($"Current Step: {overview.ActiveThread.CurrentStep}");
            sb.AppendLine($"Next Command: `{overview.ActiveThread.NextCommand}`");
            if (overview.ActivePath.Count > 1)
                sb.AppendLine($"Active Path: `{string.Join(" -> ", overview.ActivePath.Select(x => x.ThreadId))}`");
        }

        sb.AppendLine();
        sb.AppendLine("## Parked Tangents");
        sb.AppendLine();
        if (overview.ParkedThreads.Count == 0)
        {
            sb.AppendLine("None.");
        }
        else
        {
            var index = 1;
            foreach (var thread in overview.ParkedThreads)
            {
                sb.AppendLine($"{index}. `{thread.ThreadId}`");
                sb.AppendLine($"Status: {thread.Status.ToUpperInvariant()}");
                if (!string.IsNullOrWhiteSpace(thread.ParentThreadId))
                    sb.AppendLine($"Parent: `{thread.ParentThreadId}`");
                sb.AppendLine($"Title: {thread.Title}");
                sb.AppendLine($"Current Step: {thread.CurrentStep}");
                sb.AppendLine($"Next Command: `{thread.NextCommand}`");
                sb.AppendLine();
                index++;
            }
        }

        File.WriteAllText(target, sb.ToString().TrimEnd() + Environment.NewLine, Encoding.UTF8);
    }

    private void EnsureScaffold()
    {
        EnsureContainer(TaskGraphRootContainer, "task_graph");
        EnsureContainer(TaskGraphActiveContainer, "task_graph_bucket");
        EnsureContainer(TaskGraphParkedContainer, "task_graph_bucket");
        ReplaceMembers(TaskGraphRootContainer, [TaskGraphActiveContainer, TaskGraphParkedContainer]);
    }

    private TaskThreadInfo ResolveOrCreateThread(
        string title,
        string currentStep,
        string nextCommand,
        string? threadId,
        string? branchOffAnchor,
        string? parentThreadId,
        string? artifactRef,
        DateTimeOffset nowUtc)
    {
        if (string.IsNullOrWhiteSpace(threadId))
        {
            threadId = BuildThreadId(title);
        }

        var existing = GetThread(threadId);
        if (existing is not null)
        {
            UpdateThread(existing, string.IsNullOrWhiteSpace(title) ? existing.Title : title, currentStep, nextCommand, branchOffAnchor ?? existing.BranchOffAnchor, parentThreadId ?? existing.ParentThreadId, nowUtc);
            if (!string.IsNullOrWhiteSpace(artifactRef))
                AddArtifact(threadId, artifactRef!, nowUtc);
            return GetThread(threadId)!;
        }

        var objectId = ThreadObjectId(threadId);
        _store.UpsertObject(objectId, "task_thread");
        var thread = _store.Objects[objectId];
        thread.SemanticPayload ??= new SemanticPayload();
        thread.SemanticPayload.Summary = title;
        thread.SemanticPayload.Tags = ["task_thread", string.IsNullOrWhiteSpace(parentThreadId) ? "root" : "tangent"];

        var prov = EnsureProv(thread);
        prov["thread_id"] = JsonSerializer.SerializeToElement(threadId);
        prov["status"] = JsonSerializer.SerializeToElement("parked");
        prov["parent_thread_id"] = JsonSerializer.SerializeToElement(parentThreadId ?? string.Empty);
        prov["branch_off_anchor"] = JsonSerializer.SerializeToElement(branchOffAnchor ?? string.Empty);
        prov["current_step"] = JsonSerializer.SerializeToElement(currentStep);
        prov["next_command"] = JsonSerializer.SerializeToElement(nextCommand);
        prov["created_at"] = JsonSerializer.SerializeToElement(nowUtc);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);
        prov["children_container_id"] = JsonSerializer.SerializeToElement(ChildrenContainerId(threadId));
        prov["checkpoints_container_id"] = JsonSerializer.SerializeToElement(CheckpointsContainerId(threadId));
        prov["artifacts_container_id"] = JsonSerializer.SerializeToElement(ArtifactsContainerId(threadId));

        EnsureContainer(ChildrenContainerId(threadId), "task_thread_children");
        EnsureContainer(CheckpointsContainerId(threadId), "task_thread_checkpoints");
        EnsureContainer(ArtifactsContainerId(threadId), "task_thread_artifacts");

        if (!string.IsNullOrWhiteSpace(artifactRef))
            AddArtifact(threadId, artifactRef!, nowUtc);

        _retrievalGraph.ProjectTaskThread(objectId);
        return GetThread(threadId)!;
    }

    private TaskCheckpointInfo? AutoCheckpoint(TaskThreadInfo? thread, DateTimeOffset nowUtc)
    {
        if (thread is null)
            return null;

        return CreateCheckpoint(
            thread.ThreadId,
            thread.CurrentStep,
            thread.NextCommand,
            thread.BranchOffAnchor,
            null,
            nowUtc);
    }

    private TaskCheckpointInfo CreateCheckpoint(
        string threadId,
        string currentStep,
        string nextCommand,
        string? branchOffAnchor,
        string? artifactRef,
        DateTimeOffset nowUtc)
    {
        var objectId = $"task-checkpoint:{threadId}:{nowUtc:yyyyMMddHHmmssfff}:{Guid.NewGuid():N}";
        _store.UpsertObject(objectId, "task_checkpoint");
        var checkpoint = _store.Objects[objectId];
        checkpoint.SemanticPayload ??= new SemanticPayload();
        checkpoint.SemanticPayload.Summary = currentStep;
        var prov = EnsureProv(checkpoint);
        prov["thread_id"] = JsonSerializer.SerializeToElement(threadId);
        prov["current_step"] = JsonSerializer.SerializeToElement(currentStep);
        prov["next_command"] = JsonSerializer.SerializeToElement(nextCommand);
        prov["branch_off_anchor"] = JsonSerializer.SerializeToElement(branchOffAnchor ?? string.Empty);
        prov["artifact_ref"] = JsonSerializer.SerializeToElement(artifactRef ?? string.Empty);
        prov["created_at"] = JsonSerializer.SerializeToElement(nowUtc);

        _store.AddObject(CheckpointsContainerId(threadId), objectId);

        if (!string.IsNullOrWhiteSpace(artifactRef))
            AddArtifact(threadId, artifactRef!, nowUtc);

        _retrievalGraph.ProjectTaskCheckpoint(objectId);
        _retrievalGraph.ProjectTaskThread(ThreadObjectId(threadId));
        return new TaskCheckpointInfo(objectId, currentStep, currentStep, nextCommand, branchOffAnchor, artifactRef, nowUtc);
    }

    private TaskArtifactInfo AddArtifact(string threadId, string artifactRef, DateTimeOffset nowUtc)
    {
        var objectId = $"task-artifact:{threadId}:{Hash8(artifactRef)}:{Guid.NewGuid():N}";
        _store.UpsertObject(objectId, "task_artifact");
        var artifact = _store.Objects[objectId];
        artifact.SemanticPayload ??= new SemanticPayload();
        artifact.SemanticPayload.Summary = BuildArtifactLabel(artifactRef);
        var prov = EnsureProv(artifact);
        prov["thread_id"] = JsonSerializer.SerializeToElement(threadId);
        prov["artifact_ref"] = JsonSerializer.SerializeToElement(artifactRef);
        prov["created_at"] = JsonSerializer.SerializeToElement(nowUtc);
        _store.AddObject(ArtifactsContainerId(threadId), objectId);
        _retrievalGraph.ProjectTaskArtifact(objectId);
        _retrievalGraph.ProjectTaskThread(ThreadObjectId(threadId));
        return ParseArtifact(objectId)!;
    }

    private void ActivateThread(TaskThreadInfo thread, DateTimeOffset nowUtc)
    {
        ReplaceMembers(TaskGraphActiveContainer, [thread.ThreadObjectId]);
        RemoveMembership(TaskGraphParkedContainer, thread.ThreadObjectId);
        UpdateThreadStatus(thread.ThreadId, "active", nowUtc);
    }

    private void ParkThread(TaskThreadInfo thread, DateTimeOffset nowUtc)
    {
        RemoveMembership(TaskGraphActiveContainer, thread.ThreadObjectId);
        if (!_store.HasMembership(TaskGraphParkedContainer, thread.ThreadObjectId))
            _store.AddObject(TaskGraphParkedContainer, thread.ThreadObjectId);
        UpdateThreadStatus(thread.ThreadId, "parked", nowUtc);
    }

    private void LinkChild(TaskThreadInfo parent, TaskThreadInfo child)
    {
        if (!_store.HasMembership(ChildrenContainerId(parent.ThreadId), child.ThreadObjectId))
            _store.AddObject(ChildrenContainerId(parent.ThreadId), child.ThreadObjectId);
        _retrievalGraph.LinkTaskChild(parent.ThreadObjectId, child.ThreadObjectId);
        _retrievalGraph.ProjectTaskThread(parent.ThreadObjectId);
        _retrievalGraph.ProjectTaskThread(child.ThreadObjectId);
    }

    private TaskThreadInfo? GetActiveThread()
    {
        EnsureScaffold();
        var active = _store.IterateForward(TaskGraphActiveContainer).Select(x => x.ObjectId).FirstOrDefault();
        if (string.IsNullOrWhiteSpace(active))
            return null;

        return ParseThreadObject(active);
    }

    private TaskThreadInfo? GetThread(string threadId)
        => ParseThreadObject(ThreadObjectId(threadId));

    private List<TaskThreadInfo> LoadThreads()
    {
        return _store.Objects.Values
            .Where(x => x.ObjectKind == "task_thread")
            .Select(x => ParseThreadObject(x.ObjectId))
            .Where(x => x is not null)
            .Select(x => x!)
            .ToList();
    }

    private TaskThreadInfo? ParseThreadObject(string objectId)
    {
        if (!_store.Objects.TryGetValue(objectId, out var obj) || obj.ObjectKind != "task_thread")
            return null;

        var prov = obj.SemanticPayload?.Provenance;
        var threadId = ReadString(prov, "thread_id") ?? Suffix(objectId);
        var childrenContainerId = ChildrenContainerId(threadId);
        var checkpointsContainerId = CheckpointsContainerId(threadId);
        var artifactsContainerId = ArtifactsContainerId(threadId);

        var checkpoints = _store.Containers.ContainsKey(checkpointsContainerId)
            ? _store.IterateForward(checkpointsContainerId)
                .Select(x => ParseCheckpoint(x.ObjectId))
                .Where(x => x is not null)
                .Select(x => x!)
                .OrderByDescending(x => x.CreatedAt)
                .ThenByDescending(x => x.CheckpointObjectId, StringComparer.Ordinal)
                .ToList()
            : new List<TaskCheckpointInfo>();

        var artifacts = _store.Containers.ContainsKey(artifactsContainerId)
            ? _store.IterateForward(artifactsContainerId)
                .Select(x => ParseArtifact(x.ObjectId))
                .Where(x => x is not null)
                .Select(x => x!)
                .OrderByDescending(x => x.CreatedAt)
                .ThenByDescending(x => x.ArtifactObjectId, StringComparer.Ordinal)
                .ToList()
            : new List<TaskArtifactInfo>();

        var children = _store.Containers.ContainsKey(childrenContainerId)
            ? _store.IterateForward(childrenContainerId)
                .Select(x => Suffix(x.ObjectId))
                .OrderBy(x => x, StringComparer.Ordinal)
                .ToList()
            : new List<string>();

        return new TaskThreadInfo(
            objectId,
            threadId,
            obj.SemanticPayload?.Summary ?? threadId,
            ReadString(prov, "status") ?? "parked",
            EmptyToNull(ReadString(prov, "parent_thread_id")),
            EmptyToNull(ReadString(prov, "branch_off_anchor")),
            ReadString(prov, "current_step") ?? string.Empty,
            ReadString(prov, "next_command") ?? string.Empty,
            ReadDate(prov, "created_at") ?? obj.CreatedAt,
            ReadDate(prov, "updated_at") ?? obj.UpdatedAt,
            children,
            checkpoints,
            artifacts);
    }

    private TaskCheckpointInfo? ParseCheckpoint(string objectId)
    {
        if (!_store.Objects.TryGetValue(objectId, out var obj) || obj.ObjectKind != "task_checkpoint")
            return null;

        var prov = obj.SemanticPayload?.Provenance;
        var currentStep = ReadString(prov, "current_step") ?? obj.SemanticPayload?.Summary ?? objectId;
        return new TaskCheckpointInfo(
            objectId,
            obj.SemanticPayload?.Summary ?? currentStep,
            currentStep,
            ReadString(prov, "next_command") ?? string.Empty,
            EmptyToNull(ReadString(prov, "branch_off_anchor")),
            EmptyToNull(ReadString(prov, "artifact_ref")),
            ReadDate(prov, "created_at") ?? obj.CreatedAt);
    }

    private TaskArtifactInfo? ParseArtifact(string objectId)
    {
        if (!_store.Objects.TryGetValue(objectId, out var obj) || obj.ObjectKind != "task_artifact")
            return null;

        var prov = obj.SemanticPayload?.Provenance;
        return new TaskArtifactInfo(
            objectId,
            obj.SemanticPayload?.Summary ?? objectId,
            ReadString(prov, "artifact_ref") ?? string.Empty,
            ReadDate(prov, "created_at") ?? obj.CreatedAt);
    }

    private void UpdateThread(TaskThreadInfo thread, string title, string currentStep, string nextCommand, string? branchOffAnchor, string? parentThreadId, DateTimeOffset nowUtc)
    {
        var obj = _store.Objects[thread.ThreadObjectId];
        obj.SemanticPayload ??= new SemanticPayload();
        obj.SemanticPayload.Summary = title;
        var prov = EnsureProv(obj);
        prov["current_step"] = JsonSerializer.SerializeToElement(currentStep);
        prov["next_command"] = JsonSerializer.SerializeToElement(nextCommand);
        prov["branch_off_anchor"] = JsonSerializer.SerializeToElement(branchOffAnchor ?? string.Empty);
        prov["parent_thread_id"] = JsonSerializer.SerializeToElement(parentThreadId ?? string.Empty);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);
        _retrievalGraph.ProjectTaskThread(thread.ThreadObjectId);
    }

    private void UpdateThreadStatus(string threadId, string status, DateTimeOffset nowUtc)
    {
        var obj = _store.Objects[ThreadObjectId(threadId)];
        var prov = EnsureProv(obj);
        prov["status"] = JsonSerializer.SerializeToElement(status);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);
    }

    private void RemoveMembership(string containerId, string memberId)
    {
        if (_store.TryGetMembership(containerId, memberId, out var link))
            _store.RemoveLinkNode(containerId, link.LinkNodeId);
    }

    private void EnsureContainer(string id, string kind)
    {
        if (!_store.Containers.ContainsKey(id))
            _store.CreateContainer(id, "container", kind);
        _store.Containers[id].Policies.UniqueMembers = true;
    }

    private void ReplaceMembers(string containerId, IReadOnlyList<string> memberIds)
    {
        foreach (var link in _store.IterateForward(containerId).ToList())
            _store.RemoveLinkNode(containerId, link.LinkNodeId);

        foreach (var memberId in memberIds.Distinct(StringComparer.Ordinal))
        {
            if (_store.Objects.ContainsKey(memberId) || _store.Containers.ContainsKey(memberId))
                _store.AddObject(containerId, memberId);
        }
    }

    private static string BuildThreadId(string title)
    {
        var normalized = (title ?? string.Empty).Trim().ToLowerInvariant();
        normalized = IdRx.Replace(normalized, "-").Trim('-');
        if (string.IsNullOrWhiteSpace(normalized))
            normalized = "thread";
        if (normalized.Length > 40)
            normalized = normalized[..40].Trim('-');
        return $"{normalized}-{Hash8(title ?? string.Empty)}";
    }

    private static string BuildArtifactLabel(string artifactRef)
    {
        if (string.IsNullOrWhiteSpace(artifactRef))
            return "Artifact";

        try
        {
            return Path.GetFileName(artifactRef.Trim()) switch
            {
                { Length: > 0 } name => name,
                _ => artifactRef.Trim()
            };
        }
        catch
        {
            return artifactRef.Trim();
        }
    }

    private static string ThreadObjectId(string threadId) => $"task-thread:{threadId}";
    private static string ChildrenContainerId(string threadId) => $"task-thread:{threadId}:children";
    private static string CheckpointsContainerId(string threadId) => $"task-thread:{threadId}:checkpoints";
    private static string ArtifactsContainerId(string threadId) => $"task-thread:{threadId}:artifacts";
    private static string Suffix(string id) => id.Contains(':', StringComparison.Ordinal) ? id[(id.IndexOf(':', StringComparison.Ordinal) + 1)..] : id;
    private static string? EmptyToNull(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;
    private static string Hash8(string text) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text ?? string.Empty))).ToLowerInvariant()[..8];
    private static Dictionary<string, JsonElement> EnsureProv(ObjectRecord obj) { obj.SemanticPayload ??= new SemanticPayload(); obj.SemanticPayload.Provenance ??= new Dictionary<string, JsonElement>(StringComparer.Ordinal); return obj.SemanticPayload.Provenance; }
    private static string? ReadString(IReadOnlyDictionary<string, JsonElement>? p, string k) => p is not null && p.TryGetValue(k, out var e) ? (e.ValueKind == JsonValueKind.String ? e.GetString() : e.ToString()) : null;
    private static DateTimeOffset? ReadDate(IReadOnlyDictionary<string, JsonElement>? p, string k) => p is not null && p.TryGetValue(k, out var e) && (e.TryGetDateTimeOffset(out var x) || (e.ValueKind == JsonValueKind.String && DateTimeOffset.TryParse(e.GetString(), out x))) ? x : null;
}
