using System.Text.Json;
using AMS.Core;
using MemoryGraph.Abstractions;
using MemoryGraph.Application;
using MemoryGraph.Infrastructure.AMS;

namespace MemoryCtl;

internal sealed class RetrievalGraphProjector
{
    private readonly AmsStore _store;
    private readonly IRetrievalGraphStore _graphStore;
    private readonly RetrievalGraphService _service;

    public RetrievalGraphProjector(AmsStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _graphStore = new AmsGraphStoreAdapter(store);
        _service = new RetrievalGraphService(_graphStore);
    }

    public void ProjectTaskThread(string threadObjectId)
    {
        if (!_store.Objects.TryGetValue(threadObjectId, out var obj) || obj.ObjectKind != "task_thread")
            return;

        var node = EnsureObjectNode(threadObjectId);
        if (node is null)
            return;

        var desired = new List<DesiredEdge>();
        var prov = obj.SemanticPayload?.Provenance;
        var parentThreadId = ReadString(prov, "parent_thread_id");
        if (!string.IsNullOrWhiteSpace(parentThreadId))
        {
            var parentObjectId = $"task-thread:{parentThreadId}";
            var parentNode = EnsureObjectNode(parentObjectId);
            if (parentNode is not null)
                desired.Add(new DesiredEdge(parentNode.NodeId, RetrievalEdgeKinds.Parent, Evidence: parentObjectId));
        }

        var branchOffAnchor = ReadString(prov, "branch_off_anchor");
        if (!string.IsNullOrWhiteSpace(branchOffAnchor))
        {
            var anchorNode = EnsureSmartListBucketNode(branchOffAnchor!);
            if (anchorNode is not null)
                desired.Add(new DesiredEdge(anchorNode.NodeId, RetrievalEdgeKinds.BranchAnchor, Evidence: branchOffAnchor));
        }

        foreach (var artifactId in EnumerateContainerMembers(ReadString(prov, "artifacts_container_id")))
        {
            var artifactNode = EnsureObjectNode(artifactId);
            if (artifactNode is not null)
                desired.Add(new DesiredEdge(artifactNode.NodeId, RetrievalEdgeKinds.Artifact, Evidence: artifactId));
        }

        foreach (var childId in EnumerateContainerMembers(ReadString(prov, "children_container_id")))
        {
            var childNode = EnsureObjectNode(childId);
            if (childNode is not null)
                desired.Add(new DesiredEdge(childNode.NodeId, RetrievalEdgeKinds.Child, Evidence: childId));
        }

        foreach (var checkpointId in EnumerateContainerMembers(ReadString(prov, "checkpoints_container_id")))
        {
            var checkpointNode = EnsureObjectNode(checkpointId);
            if (checkpointNode is not null)
                desired.Add(new DesiredEdge(checkpointNode.NodeId, RetrievalEdgeKinds.Child, Evidence: checkpointId));
        }

        AddProjectEdges(desired, ResolveWorkspaceProjectRefs());
        ReconcileOutbound(node.NodeId, desired);
    }

    public void LinkTaskChild(string parentThreadObjectId, string childThreadObjectId)
    {
        var parentNode = EnsureObjectNode(parentThreadObjectId);
        var childNode = EnsureObjectNode(childThreadObjectId);
        if (parentNode is null || childNode is null)
            return;

        _service.LinkEdge(parentNode.NodeId, childNode.NodeId, RetrievalEdgeKinds.Child, evidence: childThreadObjectId, addedBy: "taskgraph");
        _service.LinkEdge(childNode.NodeId, parentNode.NodeId, RetrievalEdgeKinds.Parent, evidence: parentThreadObjectId, addedBy: "taskgraph");
    }

    public void ProjectTaskCheckpoint(string checkpointObjectId)
    {
        if (!_store.Objects.TryGetValue(checkpointObjectId, out var obj) || obj.ObjectKind != "task_checkpoint")
            return;

        var node = EnsureObjectNode(checkpointObjectId);
        if (node is null)
            return;

        var desired = new List<DesiredEdge>();
        var prov = obj.SemanticPayload?.Provenance;
        var threadId = ReadString(prov, "thread_id");
        if (!string.IsNullOrWhiteSpace(threadId))
        {
            var threadNode = EnsureObjectNode($"task-thread:{threadId}");
            if (threadNode is not null)
                desired.Add(new DesiredEdge(threadNode.NodeId, RetrievalEdgeKinds.Parent, Evidence: threadId));
        }

        var branchOffAnchor = ReadString(prov, "branch_off_anchor");
        if (!string.IsNullOrWhiteSpace(branchOffAnchor))
        {
            var anchorNode = EnsureSmartListBucketNode(branchOffAnchor!);
            if (anchorNode is not null)
                desired.Add(new DesiredEdge(anchorNode.NodeId, RetrievalEdgeKinds.BranchAnchor, Evidence: branchOffAnchor));
        }

        var artifactRef = ReadString(prov, "artifact_ref");
        if (!string.IsNullOrWhiteSpace(artifactRef))
        {
            var artifactThreadId = ReadString(prov, "thread_id");
            if (!string.IsNullOrWhiteSpace(artifactThreadId))
            {
                var artifactNode = FindTaskArtifactNode(artifactThreadId!, artifactRef!);
                if (artifactNode is not null)
                    desired.Add(new DesiredEdge(artifactNode.NodeId, RetrievalEdgeKinds.Artifact, Evidence: artifactRef));
            }
        }

        AddProjectEdges(desired, ResolveWorkspaceProjectRefs());
        ReconcileOutbound(node.NodeId, desired);
    }

    public void ProjectTaskArtifact(string artifactObjectId)
    {
        if (!_store.Objects.TryGetValue(artifactObjectId, out var obj) || obj.ObjectKind != "task_artifact")
            return;

        var node = EnsureObjectNode(artifactObjectId);
        if (node is null)
            return;

        var desired = new List<DesiredEdge>();
        var threadId = ReadString(obj.SemanticPayload?.Provenance, "thread_id");
        if (!string.IsNullOrWhiteSpace(threadId))
        {
            var threadNode = EnsureObjectNode($"task-thread:{threadId}");
            if (threadNode is not null)
                desired.Add(new DesiredEdge(threadNode.NodeId, RetrievalEdgeKinds.Parent, Evidence: threadId));
        }

        AddProjectEdges(desired, ResolveWorkspaceProjectRefs());
        ReconcileOutbound(node.NodeId, desired);
    }

    public void ProjectSmartListBucket(string path)
    {
        var node = EnsureSmartListBucketNode(path);
        if (node is null)
            return;

        var bucketObjectId = SmartListBucketObjectId(path);
        var desired = new List<DesiredEdge>();
        if (_store.Objects.TryGetValue(bucketObjectId, out var obj))
        {
            var parentPath = ReadString(obj.SemanticPayload?.Provenance, "parent_path");
            if (!string.IsNullOrWhiteSpace(parentPath))
            {
                var parentNode = EnsureSmartListBucketNode(parentPath!);
                if (parentNode is not null)
                    desired.Add(new DesiredEdge(parentNode.NodeId, RetrievalEdgeKinds.BucketParent, Evidence: parentPath));
            }
        }

        AddProjectEdges(desired, ResolveWorkspaceProjectRefs());
        ReconcileOutbound(node.NodeId, desired);
    }

    public void ProjectSmartListNote(string noteObjectId)
    {
        var node = EnsureObjectNode(noteObjectId);
        if (node is null)
            return;

        var desired = _store.ContainersForMemberObject(noteObjectId)
            .Where(id => id.StartsWith("smartlist-members:", StringComparison.Ordinal))
            .Select(id => id["smartlist-members:".Length..])
            .Distinct(StringComparer.Ordinal)
            .Select(path => EnsureSmartListBucketNode(path))
            .Where(bucketNode => bucketNode is not null)
            .Select(bucketNode => new DesiredEdge(bucketNode!.NodeId, RetrievalEdgeKinds.InBucket, Evidence: bucketNode.TargetRef))
            .ToList();

        AddProjectEdges(desired, ResolveWorkspaceProjectRefs());
        ReconcileOutbound(node.NodeId, desired);
    }

    public void ProjectSmartListRollup(string rollupObjectId)
    {
        if (!_store.Objects.TryGetValue(rollupObjectId, out var obj) || obj.ObjectKind != SmartListService.RollupObjectKind)
            return;

        var node = EnsureObjectNode(rollupObjectId);
        if (node is null)
            return;

        var desired = new List<DesiredEdge>();
        var prov = obj.SemanticPayload?.Provenance;
        var bucketPath = ReadString(prov, "bucket_path");
        if (!string.IsNullOrWhiteSpace(bucketPath))
        {
            var bucketNode = EnsureSmartListBucketNode(bucketPath!);
            if (bucketNode is not null)
                desired.Add(new DesiredEdge(bucketNode.NodeId, RetrievalEdgeKinds.InBucket, Evidence: bucketPath));
        }

        if (prov is not null
            && prov.TryGetValue("child_highlights", out var childEl)
            && childEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var child in childEl.EnumerateArray().Where(x => x.ValueKind == JsonValueKind.Object))
            {
                var childPath = child.TryGetProperty("path", out var pathEl) ? pathEl.ToString() : null;
                if (string.IsNullOrWhiteSpace(childPath))
                    continue;
                var childNode = EnsureSmartListBucketNode(childPath!);
                if (childNode is not null)
                    desired.Add(new DesiredEdge(childNode.NodeId, RetrievalEdgeKinds.Rollup, Evidence: childPath));
            }
        }

        AddProjectEdges(desired, ResolveWorkspaceProjectRefs());
        ReconcileOutbound(node.NodeId, desired);
    }

    public void ProjectChatSession(string sessionContainerId)
    {
        var node = EnsureContainerNode(sessionContainerId);
        if (node is null || !_store.Containers.TryGetValue(sessionContainerId, out var container))
            return;

        var desired = new List<DesiredEdge>();
        foreach (var messageId in EnumerateContainerMembers(sessionContainerId))
        {
            var messageNode = EnsureObjectNode(messageId);
            if (messageNode is not null)
                desired.Add(new DesiredEdge(messageNode.NodeId, RetrievalEdgeKinds.Child, Evidence: messageId));
        }

        AddProjectEdges(desired, ResolveProjectRefs(container));
        ReconcileOutbound(node.NodeId, desired);
    }

    public void ProjectChatMessage(string messageObjectId)
    {
        if (!_store.Objects.TryGetValue(messageObjectId, out var obj) || obj.ObjectKind != "chat_message")
            return;

        var node = EnsureObjectNode(messageObjectId);
        if (node is null)
            return;

        var desired = new List<DesiredEdge>();
        var sessionId = ReadString(obj.SemanticPayload?.Provenance, "session_id");
        if (!string.IsNullOrWhiteSpace(sessionId))
        {
            var sessionContainerId = sessionId!.StartsWith("chat-session:", StringComparison.Ordinal)
                ? sessionId!
                : $"chat-session:{sessionId}";
            var sessionNode = EnsureContainerNode(sessionContainerId);
            if (sessionNode is not null)
                desired.Add(new DesiredEdge(sessionNode.NodeId, RetrievalEdgeKinds.Parent, Evidence: sessionContainerId));
        }

        AddProjectEdges(desired, ResolveProjectRefs(obj));
        ReconcileOutbound(node.NodeId, desired);
    }

    public void ProjectAgentMemory()
    {
        foreach (var lesson in _store.Objects.Values.Where(x => x.ObjectKind == "lesson").OrderBy(x => x.ObjectId, StringComparer.Ordinal))
            ProjectLesson(lesson.ObjectId);

        foreach (var semanticNode in _store.Objects.Values.Where(x => x.ObjectKind == "lesson_semantic_node").OrderBy(x => x.ObjectId, StringComparer.Ordinal))
            ProjectSemanticNode(semanticNode.ObjectId);
    }

    public void ProjectAll()
    {
        foreach (var thread in _store.Objects.Values.Where(x => x.ObjectKind == "task_thread").OrderBy(x => x.ObjectId, StringComparer.Ordinal))
            ProjectTaskThread(thread.ObjectId);

        foreach (var checkpoint in _store.Objects.Values.Where(x => x.ObjectKind == "task_checkpoint").OrderBy(x => x.ObjectId, StringComparer.Ordinal))
            ProjectTaskCheckpoint(checkpoint.ObjectId);

        foreach (var artifact in _store.Objects.Values.Where(x => x.ObjectKind == "task_artifact").OrderBy(x => x.ObjectId, StringComparer.Ordinal))
            ProjectTaskArtifact(artifact.ObjectId);

        foreach (var bucket in _store.Objects.Values.Where(x => x.ObjectKind == SmartListService.BucketObjectKind).OrderBy(x => x.ObjectId, StringComparer.Ordinal))
        {
            var path = ReadString(bucket.SemanticPayload?.Provenance, "path");
            if (!string.IsNullOrWhiteSpace(path))
                ProjectSmartListBucket(path);
        }

        foreach (var note in _store.Objects.Values.Where(x => x.ObjectKind == SmartListService.NoteObjectKind).OrderBy(x => x.ObjectId, StringComparer.Ordinal))
            ProjectSmartListNote(note.ObjectId);

        foreach (var rollup in _store.Objects.Values.Where(x => x.ObjectKind == SmartListService.RollupObjectKind).OrderBy(x => x.ObjectId, StringComparer.Ordinal))
            ProjectSmartListRollup(rollup.ObjectId);

        foreach (var session in _store.Containers.Values.Where(x => x.ContainerKind == "chat_session").OrderBy(x => x.ContainerId, StringComparer.Ordinal))
            ProjectChatSession(session.ContainerId);

        foreach (var message in _store.Objects.Values.Where(x => x.ObjectKind == "chat_message").OrderBy(x => x.ObjectId, StringComparer.Ordinal))
            ProjectChatMessage(message.ObjectId);

        ProjectAgentMemory();
    }

    private void ProjectLesson(string lessonObjectId)
    {
        if (!_store.Objects.TryGetValue(lessonObjectId, out var obj) || obj.ObjectKind != "lesson")
            return;

        var node = EnsureObjectNode(lessonObjectId);
        if (node is null)
            return;

        var desired = new List<DesiredEdge>();
        var prov = obj.SemanticPayload?.Provenance;

        foreach (var containerId in _store.ContainersForMemberObject(lessonObjectId))
        {
            if (!containerId.StartsWith("lesson-semantic-members:", StringComparison.Ordinal))
                continue;
            var key = containerId["lesson-semantic-members:".Length..];
            var semanticNode = EnsureObjectNode($"lesson-semantic:{key}");
            if (semanticNode is not null)
                desired.Add(new DesiredEdge(semanticNode.NodeId, RetrievalEdgeKinds.SemanticMember, Evidence: key));
        }

        foreach (var sourceRef in EnumerateContainerMembers($"lesson-sources:{lessonObjectId}"))
        {
            if (_store.Objects.TryGetValue(sourceRef, out var sourceObj) && sourceObj.ObjectKind == "chat_message")
                ProjectChatMessage(sourceRef);
            else if (_store.Containers.TryGetValue(sourceRef, out var sourceContainer) && sourceContainer.ContainerKind == "chat_session")
                ProjectChatSession(sourceRef);

            var sourceNode = EnsureReferenceNode(sourceRef);
            if (sourceNode is not null)
                desired.Add(new DesiredEdge(sourceNode.NodeId, RetrievalEdgeKinds.SourceRef, Evidence: sourceRef));
        }

        var versionId = ReadString(prov, "stereotype_version_id");
        if (!string.IsNullOrWhiteSpace(versionId))
        {
            var versionNode = EnsureObjectNode(versionId!);
            if (versionNode is not null)
                desired.Add(new DesiredEdge(versionNode.NodeId, RetrievalEdgeKinds.SameStereotypeVersion, Evidence: versionId));
        }

        var familyId = ReadString(prov, "stereotype_family_id");
        if (!string.IsNullOrWhiteSpace(familyId))
        {
            var familyNode = EnsureObjectNode(familyId!);
            if (familyNode is not null)
                desired.Add(new DesiredEdge(familyNode.NodeId, RetrievalEdgeKinds.SameStereotypeFamily, Evidence: familyId));
        }

        AddProjectEdges(desired, ResolveLessonProjectRefs(obj, EnumerateContainerMembers($"lesson-sources:{lessonObjectId}")));
        ReconcileOutbound(node.NodeId, desired);
    }

    private void ProjectSemanticNode(string semanticNodeObjectId)
    {
        if (!_store.Objects.TryGetValue(semanticNodeObjectId, out var obj) || obj.ObjectKind != "lesson_semantic_node")
            return;

        var node = EnsureObjectNode(semanticNodeObjectId);
        if (node is null)
            return;

        var desired = new List<DesiredEdge>();
        var membersContainerId = ReadString(obj.SemanticPayload?.Provenance, "members_container_id");
        foreach (var lessonId in EnumerateContainerMembers(membersContainerId))
        {
            var lessonNode = EnsureObjectNode(lessonId);
            if (lessonNode is not null)
                desired.Add(new DesiredEdge(lessonNode.NodeId, RetrievalEdgeKinds.SemanticMember, Evidence: lessonId));
        }

        ReconcileOutbound(node.NodeId, desired);
    }

    private RetrievalGraphNode? FindTaskArtifactNode(string threadId, string artifactRef)
    {
        var containerId = $"task-thread:{threadId}:artifacts";
        foreach (var artifactId in EnumerateContainerMembers(containerId))
        {
            if (!_store.Objects.TryGetValue(artifactId, out var artifact))
                continue;
            var actualRef = ReadString(artifact.SemanticPayload?.Provenance, "artifact_ref");
            if (string.Equals(actualRef, artifactRef, StringComparison.OrdinalIgnoreCase))
                return EnsureObjectNode(artifactId);
        }

        return null;
    }

    private RetrievalGraphNode? EnsureSmartListBucketNode(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return null;

        var objectId = SmartListBucketObjectId(path);
        return EnsureObjectNode(objectId);
    }

    private RetrievalGraphNode? EnsureReferenceNode(string sourceRef)
    {
        if (string.IsNullOrWhiteSpace(sourceRef))
            return null;

        if (_store.Objects.ContainsKey(sourceRef))
            return EnsureObjectNode(sourceRef);
        if (_store.Containers.ContainsKey(sourceRef))
            return EnsureContainerNode(sourceRef);
        return null;
    }

    private void AddProjectEdges(List<DesiredEdge> desired, IEnumerable<string> projectRefs)
    {
        foreach (var projectRef in projectRefs.Where(x => !string.IsNullOrWhiteSpace(x)).Distinct(StringComparer.Ordinal))
        {
            var projectNode = EnsureProjectNode(projectRef);
            if (projectNode is not null)
                desired.Add(new DesiredEdge(projectNode.NodeId, RetrievalEdgeKinds.ProjectContext, Evidence: projectRef));
        }
    }

    private RetrievalGraphNode? EnsureProjectNode(string projectRef)
    {
        if (string.IsNullOrWhiteSpace(projectRef))
            return null;

        var normalized = projectRef.Trim();
        var title = normalized.StartsWith("project:", StringComparison.Ordinal)
            ? normalized["project:".Length..]
            : normalized;
        return _service.UpsertNode(
            RetrievalGraphConventions.BuildNodeId(RetrievalNodeKinds.ProjectContext, normalized),
            RetrievalNodeKinds.ProjectContext,
            normalized,
            title,
            DateTimeOffset.UtcNow);
    }

    private IEnumerable<string> ResolveWorkspaceProjectRefs()
    {
        var name = ResolveWorkspaceProjectName();
        if (string.IsNullOrWhiteSpace(name))
            yield break;

        yield return BuildProjectRef(name);
    }

    private IEnumerable<string> ResolveLessonProjectRefs(ObjectRecord lesson, IEnumerable<string> sourceRefs)
    {
        foreach (var projectRef in ResolveProjectRefs(lesson))
            yield return projectRef;

        foreach (var sourceRef in sourceRefs)
        {
            if (_store.Objects.TryGetValue(sourceRef, out var sourceObj))
            {
                foreach (var projectRef in ResolveProjectRefs(sourceObj))
                    yield return projectRef;
                continue;
            }

            if (_store.Containers.TryGetValue(sourceRef, out var sourceContainer))
            {
                foreach (var projectRef in ResolveProjectRefs(sourceContainer))
                    yield return projectRef;
            }
        }
    }

    private IEnumerable<string> ResolveProjectRefs(ObjectRecord obj)
    {
        var prov = obj.SemanticPayload?.Provenance;
        foreach (var projectRef in ReadProjectRefs(prov, "source_project_keys"))
            yield return projectRef;

        var channel = ReadString(prov, "channel");
        if (!string.IsNullOrWhiteSpace(channel))
        {
            var projectRef = ProjectRefFromChannel(channel);
            if (!string.IsNullOrWhiteSpace(projectRef))
                yield return projectRef!;
        }

        switch (obj.ObjectKind)
        {
            case "task_thread":
            case "task_checkpoint":
            case "task_artifact":
            case SmartListService.BucketObjectKind:
            case SmartListService.NoteObjectKind:
            case SmartListService.RollupObjectKind:
                foreach (var projectRef in ResolveWorkspaceProjectRefs())
                    yield return projectRef;
                break;
        }
    }

    private IEnumerable<string> ResolveProjectRefs(ContainerRecord container)
    {
        foreach (var projectRef in ReadProjectRefs(container.Metadata, "source_project_keys"))
            yield return projectRef;

        var channel = ReadString(container.Metadata, "channel");
        if (!string.IsNullOrWhiteSpace(channel))
        {
            var projectRef = ProjectRefFromChannel(channel);
            if (!string.IsNullOrWhiteSpace(projectRef))
                yield return projectRef!;
        }
    }

    private static IEnumerable<string> ReadProjectRefs(IReadOnlyDictionary<string, JsonElement>? metadata, string key)
    {
        if (metadata is null || !metadata.TryGetValue(key, out var value) || value.ValueKind != JsonValueKind.Array)
            yield break;

        foreach (var element in value.EnumerateArray().Where(x => x.ValueKind == JsonValueKind.String))
        {
            var projectRef = ProjectRefFromSourceKey(element.GetString());
            if (!string.IsNullOrWhiteSpace(projectRef))
                yield return projectRef!;
        }
    }

    private static string? ProjectRefFromSourceKey(string? sourceKey)
    {
        if (string.IsNullOrWhiteSpace(sourceKey))
            return null;

        foreach (var part in sourceKey.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (part.StartsWith("project:", StringComparison.Ordinal))
                return BuildProjectRef(part["project:".Length..]);
        }

        return null;
    }

    private static string? ProjectRefFromChannel(string? channel)
    {
        if (string.IsNullOrWhiteSpace(channel))
            return null;

        var value = channel.Trim();
        var slash = value.IndexOf('/', StringComparison.Ordinal);
        if (slash < 0 || slash + 1 >= value.Length)
            return null;

        return BuildProjectRef(value[(slash + 1)..]);
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

    private RetrievalGraphNode? EnsureContainerNode(string containerId)
    {
        if (!_store.Containers.TryGetValue(containerId, out var container))
            return null;

        string? nodeKind = container.ContainerKind switch
        {
            "chat_session" => RetrievalNodeKinds.ChatSession,
            _ => null
        };
        if (nodeKind is null)
            return null;

        var title = ReadString(container.Metadata, "title") ?? containerId;
        var updatedAt = ReadDate(container.Metadata, "ended_at") ?? ReadDate(container.Metadata, "started_at");
        return _service.UpsertNode(
            RetrievalGraphConventions.BuildNodeId(nodeKind, containerId),
            nodeKind,
            containerId,
            title,
            updatedAt);
    }

    private RetrievalGraphNode? EnsureObjectNode(string objectId)
    {
        if (!_store.Objects.TryGetValue(objectId, out var obj))
            return null;

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

        var title = obj.SemanticPayload?.Summary ?? objectId;
        return _service.UpsertNode(
            RetrievalGraphConventions.BuildNodeId(nodeKind, objectId),
            nodeKind,
            objectId,
            title,
            obj.UpdatedAt);
    }

    private void ReconcileOutbound(Guid fromNodeId, IReadOnlyList<DesiredEdge> desiredEdges)
    {
        var desiredByTarget = desiredEdges
            .GroupBy(x => x.ToNodeId)
            .ToDictionary(g => g.Key, g => g.First());

        var existing = _service.Outbound(fromNodeId);
        foreach (var edge in existing)
        {
            if (!desiredByTarget.ContainsKey(edge.ToNodeId))
                _graphStore.UnlinkRetrievalEdge(fromNodeId, edge.ToNodeId);
        }

        foreach (var desired in desiredByTarget.Values)
        {
            _service.LinkEdge(fromNodeId, desired.ToNodeId, desired.EdgeKind, desired.Cost, evidence: desired.Evidence, addedBy: "phase2-projector");
        }
    }

    private IEnumerable<string> EnumerateContainerMembers(string? containerId)
    {
        if (string.IsNullOrWhiteSpace(containerId) || !_store.Containers.ContainsKey(containerId))
            yield break;

        foreach (var member in _store.IterateForward(containerId).Select(x => x.ObjectId))
            yield return member;
    }

    private static string SmartListBucketObjectId(string path) => $"smartlist-bucket:{path}";

    private static string? ReadString(IReadOnlyDictionary<string, JsonElement>? p, string key)
        => p is not null && p.TryGetValue(key, out var el) ? (el.ValueKind == JsonValueKind.String ? el.GetString() : el.ToString()) : null;

    private static DateTimeOffset? ReadDate(IReadOnlyDictionary<string, JsonElement>? p, string key)
        => p is not null && p.TryGetValue(key, out var el) && (el.TryGetDateTimeOffset(out var x) || (el.ValueKind == JsonValueKind.String && DateTimeOffset.TryParse(el.GetString(), out x))) ? x : null;

    private sealed record DesiredEdge(Guid ToNodeId, string EdgeKind, double? Cost = null, string? Evidence = null);
}
