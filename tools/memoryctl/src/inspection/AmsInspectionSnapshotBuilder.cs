using AMS.Core;
using MemoryGraph.Abstractions;
using MemoryGraph.Infrastructure.AMS;

namespace MemoryCtl.Inspection;

/// <summary>
/// Reads AMS data and produces a <see cref="GraphInspectionSnapshot"/>.
/// Knows about AMS types; produces no HTML.
/// </summary>
internal sealed class AmsInspectionSnapshotBuilder
{
    private readonly AmsStore _store;
    private readonly AmsGraphStoreAdapter? _adapter;
    private readonly Dictionary<Guid, MemoryCardPayload>? _payloads;

    public AmsInspectionSnapshotBuilder(
        AmsStore store,
        AmsGraphStoreAdapter? adapter = null,
        Dictionary<Guid, MemoryCardPayload>? payloads = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _adapter = adapter;
        _payloads = payloads;
    }

    public GraphInspectionSnapshot Build()
    {
        var (atlasTopMap, atlasThreads, atlasPinned) = LoadAtlasNav();

        var sections = new List<GraphInspectionSection>();

        var sessionData = BuildSessionMaps();
        SessionClusterProjection? clusterProjection = null;

        var timelineSection = BuildTimelineSection(sessionData.SessionItemById, out clusterProjection);
        if (timelineSection is not null)
            sections.Add(timelineSection);

        if (clusterProjection is not null)
        {
            sections.Add(new GraphInspectionDiagnosticsSection(
                "Grouping Diagnostics",
                clusterProjection.IndexDriftDetected,
                clusterProjection.ChatSessionContainers,
                clusterProjection.ChatSessionsIndexEntries));
        }

        var dreamSection = BuildDreamNodesSection(
            sessionData.SessionTitleById,
            sessionData.MsgToSessionId,
            clusterProjection?.SessionToCanonical ?? new Dictionary<string, string>(StringComparer.Ordinal));
        if (dreamSection is not null)
            sections.Add(dreamSection);

        BuildTaskGraphSections(sections);
        BuildSmartListSections(sections);
        BuildStructuralRootSections(sections);
        BuildGenericSections(sections, sessionData.SessionItemById);

        int totalSessions = _store.Containers.Values.Count(c => c.ContainerKind == "chat_session");
        int totalCards = _adapter?.AllMemAnchors.Sum(id => _adapter.CardsIn(id).Count) ?? 0;
        int totalAnchors = _adapter?.AllMemAnchors.Count() ?? 0;

        return new GraphInspectionSnapshot(
            "Memory Graph",
            totalSessions,
            totalCards,
            totalAnchors,
            atlasTopMap,
            atlasThreads,
            atlasPinned,
            sections);
    }

    private (
        IReadOnlyList<GraphInspectionNavEntry> TopMap,
        IReadOnlyList<GraphInspectionNavEntry> Threads,
        IReadOnlyList<GraphInspectionNavEntry> Pinned
    ) LoadAtlasNav()
    {
        // Atlas tool integration lives in the external AMS.Atlas package.
        // In this repository we run against AMS.Core only, so nav sections are omitted.
        return ([], [], []);
    }

    private GraphInspectionTimelineSection? BuildTimelineSection(
        Dictionary<string, GraphInspectionSession> sessionItemById,
        out SessionClusterProjection? projection)
    {
        projection = null;
        if (sessionItemById.Count == 0)
            return null;

        projection = new SessionClusterProjector(_store).Project(sessionItemById);
        if (projection.ClusterSessions.Count == 0)
            return null;

        var groupedSessions = projection.ClusterSessions
            .Select(session =>
            {
                var dt = DateTimeOffset.TryParse(session.DateLabel.TrimStart(' ', '·'), out var parsed)
                    ? parsed
                    : DateTimeOffset.MinValue;
                return (Session: session, Dt: dt);
            })
            .GroupBy(x => x.Dt.Year)
            .OrderBy(g => g.Key)
            .Select(yg => new GraphInspectionTimelineYear(
                yg.Key,
                yg.GroupBy(x => x.Dt.Month)
                  .OrderBy(mg => mg.Key)
                  .Select(mg => new GraphInspectionTimelineMonth(
                      yg.Key,
                      mg.Key,
                      mg.GroupBy(x => x.Dt.Day)
                        .OrderBy(dg => dg.Key)
                        .Select(dg =>
                        {
                            var entries = dg.OrderBy(x => x.Dt)
                                .Select(x => (GraphInspectionDayEntry)new GraphInspectionSingleSessionEntry(x.Session))
                                .ToList();

                            return new GraphInspectionTimelineDay(
                                yg.Key,
                                mg.Key,
                                dg.Key,
                                BuildTimelineDaySummary(entries),
                                entries);
                        })
                        .ToList()))
                  .ToList()))
            .ToList();

        return new GraphInspectionTimelineSection("Chat Sessions", groupedSessions);
    }

    private static string BuildTimelineDaySummary(IReadOnlyList<GraphInspectionDayEntry> entries)
    {
        var ranked = new Dictionary<string, (int Weight, bool HighQuality)>(StringComparer.OrdinalIgnoreCase);

        static void AddCandidate(
            Dictionary<string, (int Weight, bool HighQuality)> target,
            string title,
            int weight,
            bool highQuality)
        {
            var normalized = title.Trim();
            if (string.IsNullOrWhiteSpace(normalized))
                return;

            if (target.TryGetValue(normalized, out var existing))
                target[normalized] = (existing.Weight + Math.Max(1, weight), existing.HighQuality || highQuality);
            else
                target[normalized] = (Math.Max(1, weight), highQuality);
        }

        foreach (var entry in entries)
        {
            switch (entry)
            {
                case GraphInspectionSingleSessionEntry single:
                    AddCandidate(
                        ranked,
                        single.Session.Title,
                        single.Session.SessionCount,
                        string.Equals(single.Session.TitleQuality, "high", StringComparison.Ordinal));
                    break;
                case GraphInspectionSegmentGroupEntry grouped:
                    foreach (var session in grouped.Group.Sessions)
                    {
                        AddCandidate(
                            ranked,
                            session.Title,
                            session.SessionCount,
                            string.Equals(session.TitleQuality, "high", StringComparison.Ordinal));
                    }
                    break;
            }
        }

        if (ranked.Count == 0)
            return "Session activity";

        var ordered = ranked
            .OrderByDescending(x => x.Value.HighQuality)
            .ThenByDescending(x => x.Value.Weight)
            .ThenBy(x => x.Key, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var primary = TruncateSummaryText(ordered[0].Key);
        if (ordered.Count == 1)
            return primary;

        return $"{primary} (+{ordered.Count - 1} more)";
    }

    private static string TruncateSummaryText(string text, int maxLen = 72)
    {
        var normalized = text.Trim();
        if (normalized.Length <= maxLen)
            return normalized;

        return normalized[..(maxLen - 3)].TrimEnd() + "...";
    }

    private GraphInspectionDreamNodesSection? BuildDreamNodesSection(
        Dictionary<string, string> sessionTitleById,
        Dictionary<string, string> msgToSessionId,
        IReadOnlyDictionary<string, string> sessionToCanonical)
    {
        static bool IsDreamKind(string kind) => kind is "topic" or "thread" or "decision" or "invariant";

        static double ReadVoteScore(Dictionary<string, System.Text.Json.JsonElement>? prov)
            => prov is not null && prov.TryGetValue("vote_score", out var el) && el.TryGetDouble(out var score)
                ? score
                : 0;

        var groups = _store.Objects.Values
            .Where(o => IsDreamKind(o.ObjectKind))
            .Where(o => !InspectionTitlePolicy.IsStructuralMembersId(o.ObjectId))
            .GroupBy(o => o.ObjectKind)
            .OrderBy(g => DreamKindOrder(g.Key))
            .Select(g => new GraphInspectionDreamGroup(
                g.Key,
                g.OrderByDescending(o => ReadVoteScore(o.SemanticPayload?.Provenance))
                 .Select(o => BuildDreamNode(o, sessionTitleById, msgToSessionId, sessionToCanonical))
                 .ToList()))
            .ToList();

        return groups.Count == 0
            ? null
            : new GraphInspectionDreamNodesSection("Dream Overview", groups);
    }

    private GraphInspectionDreamNode BuildDreamNode(
        ObjectRecord obj,
        Dictionary<string, string> sessionTitleById,
        Dictionary<string, string> msgToSessionId,
        IReadOnlyDictionary<string, string> sessionToCanonical)
    {
        var provenance = obj.SemanticPayload?.Provenance;
        var score = provenance is not null && provenance.TryGetValue("vote_score", out var scoreEl) && scoreEl.TryGetDouble(out var vote)
            ? vote
            : 0;

        var (label, titleQuality) = InspectionTitlePolicy.ResolveDreamLabel(
            obj.SemanticPayload?.Summary,
            obj.ObjectId,
            obj.ObjectKind);

        var links = BuildDreamMemberLinks(obj, sessionTitleById, msgToSessionId, sessionToCanonical);
        var nodeRole = ClassifyNodeRole(obj.ObjectId);
        var groupKey = obj.ObjectId.Contains(':', StringComparison.Ordinal)
            ? obj.ObjectId[(obj.ObjectId.IndexOf(':', StringComparison.Ordinal) + 1)..]
            : obj.ObjectId;

        return new GraphInspectionDreamNode(
            obj.ObjectId,
            obj.ObjectId.Replace(':', '-'),
            label,
            score,
            links,
            nodeRole,
            titleQuality,
            groupKey);
    }

    private List<GraphInspectionMemberLink> BuildDreamMemberLinks(
        ObjectRecord obj,
        Dictionary<string, string> sessionTitleById,
        Dictionary<string, string> msgToSessionId,
        IReadOnlyDictionary<string, string> sessionToCanonical)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var links = new List<GraphInspectionMemberLink>();

        var suffix = obj.ObjectId.Contains(':', StringComparison.Ordinal)
            ? obj.ObjectId[(obj.ObjectId.IndexOf(':', StringComparison.Ordinal) + 1)..]
            : obj.ObjectId;
        var membersContainerId = $"{obj.ObjectKind}-members:{suffix}";

        void AddSession(string sessionId)
        {
            var canonicalSessionId = sessionToCanonical.TryGetValue(sessionId, out var canonical)
                ? canonical
                : sessionId;

            if (!seen.Add(canonicalSessionId))
                return;

            var title = sessionTitleById.TryGetValue(canonicalSessionId, out var direct)
                ? direct
                : sessionTitleById.TryGetValue(sessionId, out var fallback)
                    ? fallback
                    : canonicalSessionId;

            links.Add(new GraphInspectionMemberLink(title, canonicalSessionId.Replace(':', '-')));
        }

        if (_store.Containers.ContainsKey(membersContainerId))
        {
            foreach (var ln in _store.IterateForward(membersContainerId))
            {
                if (msgToSessionId.TryGetValue(ln.ObjectId, out var sessionId))
                    AddSession(sessionId);
            }
        }

        if (obj.SemanticPayload?.Provenance?.TryGetValue("evidence", out var evidenceEl) == true)
        {
            foreach (var evidence in evidenceEl.EnumerateArray())
            {
                var reference = evidence.GetString() ?? string.Empty;
                var colon = reference.IndexOf(':', StringComparison.Ordinal);
                if (colon < 0)
                    continue;

                var kind = reference[..colon];
                var id = reference[(colon + 1)..];

                if (kind is "Conversation" or "Segment")
                    AddSession(id);
                else if (kind == "Turn" && msgToSessionId.TryGetValue(id, out var turnSessionId))
                    AddSession(turnSessionId);
            }
        }

        return links;
    }

    private void BuildGenericSections(
        List<GraphInspectionSection> sections,
        IReadOnlyDictionary<string, GraphInspectionSession> sessionItemById)
    {
        if (_adapter is null)
            return;

        var chatSessionGuids = new HashSet<Guid>(
            _store.Containers.Values
                .Where(c => c.ContainerKind == "chat_session")
                .Select(c => Guid.Parse(c.ContainerId["chat-session:".Length..])));

        var anchors = _adapter.AllMemAnchors
            .Select(id =>
            {
                _adapter.TryGetMemAnchorName(id, out var name);
                var cards = _adapter.CardsIn(id);
                return (Id: id, Name: name ?? id.ToString(), Cards: cards);
            })
            .OrderBy(a => a.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (anchors.Count > 0)
            BuildMemAnchorSections(anchors, sections, BuildSessionReferenceLookup(sessionItemById));

        var linkedCards = new HashSet<Guid>(anchors.SelectMany(a => a.Cards));
        var payloadsForOrphans = _payloads;
        var orphans = _adapter.AllCards
            .Where(id => !chatSessionGuids.Contains(id) && !linkedCards.Contains(id))
            .Select(id =>
            {
                MemoryCardPayload? payload = null;
                payloadsForOrphans?.TryGetValue(id, out payload);
                return new GraphInspectionGenericItem(id.ToString(), "card", payload?.Title ?? id.ToString(), null, null);
            })
            .OrderBy(item => item.Label, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (orphans.Count > 0)
            sections.Add(new GraphInspectionGenericContainerSection(
                $"Untagged cards ({orphans.Count})",
                "orphaned-cards",
                orphans,
                Family: "memory_cards",
                NavGroup: "Memory Cards",
                NavLabel: $"Untagged cards ({orphans.Count})",
                Path: "Untagged cards"));
    }

    private void BuildMemAnchorSections(
        IReadOnlyList<(Guid Id, string Name, IReadOnlyList<Guid> Cards)> anchors,
        List<GraphInspectionSection> sections,
        IReadOnlyDictionary<string, SessionReferenceInfo> sessionLookup)
    {
        var root = new MemAnchorTreeNode(string.Empty, string.Empty);
        foreach (var (id, name, cards) in anchors)
        {
            var parts = CanonicalizeMemAnchorSegments(name, sessionLookup);
            var current = root;
            var currentPath = new List<string>();
            foreach (var part in parts)
            {
                currentPath.Add(part);
                var path = string.Join(" / ", currentPath);
                if (!current.Children.TryGetValue(part, out var child))
                {
                    child = new MemAnchorTreeNode(part, path);
                    current.Children[part] = child;
                }

                current = child;
            }

            current.AnchorId ??= "memanchor-" + id.ToString("D");
            current.Cards.AddRange(cards
                .Select(cardId =>
                {
                    MemoryCardPayload? payload = null;
                    _payloads?.TryGetValue(cardId, out payload);
                    return new GraphInspectionGenericItem(
                        cardId.ToString(),
                        "card",
                        payload?.Title ?? cardId.ToString(),
                        null,
                        string.IsNullOrWhiteSpace(payload?.Text) ? null : payload.Text.Trim());
                }));
        }

        foreach (var child in root.Children.Values.OrderBy(x => x.Segment, StringComparer.OrdinalIgnoreCase))
            AddMemAnchorSections(child, sections, 0);
    }

    private void AddMemAnchorSections(MemAnchorTreeNode node, List<GraphInspectionSection> sections, int depth)
    {
        var anchorId = string.IsNullOrWhiteSpace(node.AnchorId)
            ? "memanchor-group-" + SlugifyAnchor(node.Path)
            : node.AnchorId;

        var items = new List<GraphInspectionGenericItem>();
        foreach (var child in node.Children.Values.OrderBy(x => x.Segment, StringComparer.OrdinalIgnoreCase))
        {
            var childAnchor = string.IsNullOrWhiteSpace(child.AnchorId)
                ? "memanchor-group-" + SlugifyAnchor(child.Path)
                : child.AnchorId;
            items.Add(new GraphInspectionGenericItem(
                "memanchor-group:" + child.Path,
                "container",
                ResolveMemAnchorHeading(child),
                "anchor:" + childAnchor,
                null));
        }

        if (node.Cards.Count > 0)
            items.AddRange(node.Cards);

        var includeInNav = depth == 0;
        var displayHeading = ResolveMemAnchorHeading(node);
        sections.Add(new GraphInspectionGenericContainerSection(
            displayHeading,
            anchorId,
            items,
            Family: "memory_cards",
            NavGroup: "Memory Cards",
            NavLabel: displayHeading,
            Path: $"Memory Cards / {ResolveMemAnchorPath(node, displayHeading)}",
            IncludeInNav: includeInNav));

        foreach (var child in node.Children.Values.OrderBy(x => x.Segment, StringComparer.OrdinalIgnoreCase))
            AddMemAnchorSections(child, sections, depth + 1);
    }

    private void BuildTaskGraphSections(List<GraphInspectionSection> sections)
    {
        var overview = new TaskGraphService(_store).Inspect();
        if (overview.AllThreads.Count == 0)
            return;

        var rootItems = new List<GraphInspectionGenericItem>();
        if (overview.ActiveThread is not null)
        {
            rootItems.Add(new GraphInspectionGenericItem(
                "task-graph:active-path",
                "meta",
                "Active Path",
                null,
                string.Join(" -> ", overview.ActivePath.Select(x => $"{x.ThreadId} ({x.Title})"))));
            rootItems.Add(new GraphInspectionGenericItem(
                overview.ActiveThread.ThreadObjectId,
                "container",
                $"Active Thread | {overview.ActiveThread.Title}",
                "anchor:" + ThreadAnchor(overview.ActiveThread.ThreadId),
                null));
        }
        else
        {
            rootItems.Add(new GraphInspectionGenericItem("task-graph:no-active", "meta", "Active Thread", null, "No active thread."));
        }

        if (overview.ParkedThreads.Count > 0)
        {
            rootItems.Add(new GraphInspectionGenericItem(
                "task-graph:parked",
                "container",
                $"Parked Threads ({overview.ParkedThreads.Count})",
                "anchor:task-graph-parked",
                null));
        }

        sections.Add(new GraphInspectionGenericContainerSection(
            "Overview",
            "task-graph",
            rootItems,
            Family: "task_graph",
            NavGroup: "Task Graph",
            NavLabel: "Overview",
            Path: "Task Graph"));

        if (overview.ParkedThreads.Count > 0)
        {
            sections.Add(new GraphInspectionGenericContainerSection(
                "Parked Threads",
                "task-graph-parked",
                overview.ParkedThreads
                    .OrderByDescending(x => x.UpdatedAt)
                    .ThenBy(x => x.ThreadId, StringComparer.Ordinal)
                    .Select(parked => new GraphInspectionGenericItem(
                        parked.ThreadObjectId,
                        "container",
                        parked.Title,
                        "anchor:" + ThreadAnchor(parked.ThreadId),
                        null))
                    .ToList(),
                Family: "task_graph",
                NavGroup: "Task Graph",
                NavLabel: "Parked Threads",
                Path: "Task Graph / Parked Threads"));
        }

        foreach (var thread in overview.AllThreads.OrderByDescending(x => string.Equals(x.Status, "active", StringComparison.OrdinalIgnoreCase)).ThenByDescending(x => x.UpdatedAt))
        {
            var isParked = string.Equals(thread.Status, "parked", StringComparison.OrdinalIgnoreCase);
            var threadPathPrefix = isParked
                ? $"Task Graph / Parked Threads / {thread.Title}"
                : $"Task Graph / {thread.Title}";

            var detailItems = new List<GraphInspectionGenericItem>
            {
                new($"task-meta:{thread.ThreadId}:status", "meta", "Status", null, thread.Status),
                new($"task-meta:{thread.ThreadId}:step", "meta", "Current Step", null, thread.CurrentStep),
                new($"task-meta:{thread.ThreadId}:next", "meta", "Next Command", null, thread.NextCommand),
                new($"task-meta:{thread.ThreadId}:updated", "meta", "Updated At", null, thread.UpdatedAt.ToString("o"))
            };

            if (!string.IsNullOrWhiteSpace(thread.ParentThreadId))
            {
                detailItems.Add(new GraphInspectionGenericItem(
                    $"task-meta:{thread.ThreadId}:parent",
                    "container",
                    $"Parent | {thread.ParentThreadId}",
                    "anchor:" + ThreadAnchor(thread.ParentThreadId),
                    null));
            }

            if (!string.IsNullOrWhiteSpace(thread.BranchOffAnchor))
                detailItems.Add(new GraphInspectionGenericItem($"task-meta:{thread.ThreadId}:anchor", "meta", "Branch-Off Anchor", null, thread.BranchOffAnchor));

            if (thread.Checkpoints.Count > 0)
            {
                detailItems.Add(new GraphInspectionGenericItem(
                    $"task-meta:{thread.ThreadId}:checkpoints",
                    "container",
                    $"Checkpoints ({thread.Checkpoints.Count})",
                    "anchor:" + ThreadCheckpointAnchor(thread.ThreadId),
                    null));
            }

            if (thread.ChildThreadIds.Count > 0)
            {
                detailItems.Add(new GraphInspectionGenericItem(
                    $"task-meta:{thread.ThreadId}:children",
                    "container",
                    $"Child Tangents ({thread.ChildThreadIds.Count})",
                    "anchor:" + ThreadChildrenAnchor(thread.ThreadId),
                    null));
            }

            if (thread.Artifacts.Count > 0)
            {
                detailItems.Add(new GraphInspectionGenericItem(
                    $"task-meta:{thread.ThreadId}:artifacts",
                    "container",
                    $"Artifacts ({thread.Artifacts.Count})",
                    "anchor:" + ThreadArtifactsAnchor(thread.ThreadId),
                    null));
            }

            sections.Add(new GraphInspectionGenericContainerSection(
                thread.Title,
                ThreadAnchor(thread.ThreadId),
                detailItems,
                Family: "task_graph",
                NavGroup: "Task Graph",
                NavLabel: thread.Title,
                Path: threadPathPrefix,
                IncludeInNav: !isParked));

            if (thread.Checkpoints.Count > 0)
            {
                sections.Add(new GraphInspectionGenericContainerSection(
                    "Checkpoints",
                    ThreadCheckpointAnchor(thread.ThreadId),
                    thread.Checkpoints.Select(checkpoint => new GraphInspectionGenericItem(
                        checkpoint.CheckpointObjectId,
                        "task_checkpoint",
                        checkpoint.CreatedAt.ToString("yyyy-MM-dd HH:mm:ss") + " | " + checkpoint.Summary,
                        null,
                        BuildCheckpointBody(checkpoint))).ToList(),
                    Family: "task_graph",
                    NavGroup: "Task Graph",
                    NavLabel: $"{thread.Title} / Checkpoints",
                    Path: $"{threadPathPrefix} / Checkpoints",
                    IncludeInNav: !isParked));
            }

            if (thread.ChildThreadIds.Count > 0)
            {
                sections.Add(new GraphInspectionGenericContainerSection(
                    "Child Tangents",
                    ThreadChildrenAnchor(thread.ThreadId),
                    thread.ChildThreadIds.Select(childId => new GraphInspectionGenericItem(
                        ThreadObjectId(childId),
                        "container",
                        ChildThreadLabel(childId, overview.AllThreads),
                        "anchor:" + ThreadAnchor(childId),
                        null)).ToList(),
                    Family: "task_graph",
                    NavGroup: "Task Graph",
                    NavLabel: $"{thread.Title} / Child Tangents",
                    Path: $"{threadPathPrefix} / Child Tangents",
                    IncludeInNav: !isParked));
            }

            if (thread.Artifacts.Count > 0)
            {
                sections.Add(new GraphInspectionGenericContainerSection(
                    "Artifacts",
                    ThreadArtifactsAnchor(thread.ThreadId),
                    thread.Artifacts.Select(artifact => new GraphInspectionGenericItem(
                        artifact.ArtifactObjectId,
                        "task_artifact",
                        artifact.Label,
                        null,
                        $"{artifact.ArtifactRef}{Environment.NewLine}created_at: {artifact.CreatedAt:o}")).ToList(),
                    Family: "task_graph",
                    NavGroup: "Task Graph",
                    NavLabel: $"{thread.Title} / Artifacts",
                    Path: $"{threadPathPrefix} / Artifacts",
                    IncludeInNav: !isParked));
            }
        }
    }

    private void BuildStructuralRootSections(List<GraphInspectionSection> sections)
    {
        var roots = _store.Containers.Values
            .Where(c => c.ContainerKind.EndsWith("_root", StringComparison.Ordinal))
            .OrderBy(c => c.ContainerId, StringComparer.Ordinal)
            .ToList();

        foreach (var root in roots)
        {
            var items = BuildStructureItems(root.ContainerId);
            sections.Add(new GraphInspectionGenericContainerSection(
                FormatStructureHeading(root.ContainerId),
                "structure-" + root.ContainerId.Replace(':', '-'),
                items,
                Family: "structure",
                NavGroup: "Structures",
                NavLabel: FormatStructureNavLabel(root.ContainerId, root.ContainerId),
                Path: $"Structures / {FormatStructureHeading(root.ContainerId)}"));
            AddNestedStructureSections(root.ContainerId, root.ContainerId, sections, 1, new HashSet<string>(StringComparer.Ordinal));
        }
    }

    private void BuildSmartListSections(List<GraphInspectionSection> sections)
    {
        var service = new SmartListService(_store);
        AddSmartListRootSections(sections, service, SmartListService.DurableRootContainer, "Durable");
        AddSmartListRootSections(sections, service, SmartListService.ShortTermRootContainer, "Short-Term");
    }

    private void AddSmartListRootSections(
        List<GraphInspectionSection> sections,
        SmartListService service,
        string rootContainerId,
        string rootLabel)
    {
        if (!_store.Containers.ContainsKey(rootContainerId))
            return;

        var rootItems = BuildSmartListMemberItems(
            rootContainerId,
            service,
            $"SmartLists / {rootLabel}",
            parentBucketPath: null,
            rootAnchorId: SmartListRootAnchor(rootLabel));
        if (rootItems.Count == 0)
            return;

        sections.Add(new GraphInspectionGenericContainerSection(
            rootLabel,
            SmartListRootAnchor(rootLabel),
            rootItems,
            Family: "smartlist",
            NavGroup: "SmartLists",
            NavLabel: rootLabel,
            Path: $"SmartLists / {rootLabel}",
            Synopsis: $"{rootLabel} SmartList buckets"));

        foreach (var bucket in EnumerateSmartListBuckets(rootContainerId, service))
            AddSmartListBucketSections(sections, service, bucket.Path, rootLabel, new HashSet<string>(StringComparer.Ordinal));
    }

    private void AddSmartListBucketSections(
        List<GraphInspectionSection> sections,
        SmartListService service,
        string bucketPath,
        string rootLabel,
        HashSet<string> visited)
    {
        if (!visited.Add(bucketPath))
            return;

        var bucket = service.GetBucket(bucketPath);
        if (bucket is null)
            return;

        var items = BuildSmartListMemberItems(
            MembersContainerId(bucket.Path),
            service,
            $"SmartLists / {rootLabel} / {bucket.Path}",
            bucket.Path,
            SmartListRootAnchor(rootLabel));

        if (!string.IsNullOrWhiteSpace(bucket.ParentPath))
        {
            var parentLabel = bucket.ParentPath.Split('/')[^1];
            items.Add(new GraphInspectionGenericItem(
                $"smartlist-parent:{bucket.Path}",
                "container",
                $"Parent | {HumanizeToken(parentLabel)}",
                "anchor:" + SmartListBucketAnchor(bucket.ParentPath),
                null));
        }
        else
        {
            items.Add(new GraphInspectionGenericItem(
                $"smartlist-root:{bucket.Path}",
                "container",
                $"Back to {rootLabel}",
                "anchor:" + SmartListRootAnchor(rootLabel),
                null));
        }

        items.Add(new GraphInspectionGenericItem(
            $"smartlist-meta:{bucket.Path}",
            "meta",
            "Bucket Metadata",
            null,
            BuildSmartListBucketBody(bucket)));

        sections.Add(new GraphInspectionGenericContainerSection(
            HumanizeToken(bucket.DisplayName),
            SmartListBucketAnchor(bucket.Path),
            items,
            Family: "smartlist",
            NavGroup: "SmartLists",
            NavLabel: HumanizeToken(bucket.DisplayName),
            Path: $"SmartLists / {rootLabel} / {bucket.Path}",
            Synopsis: service.GetRollup(bucket.Path)?.Summary));

        foreach (var child in EnumerateSmartListBuckets(MembersContainerId(bucket.Path), service))
            AddSmartListBucketSections(sections, service, child.Path, rootLabel, visited);
    }

    private List<GraphInspectionGenericItem> BuildSmartListMemberItems(
        string containerId,
        SmartListService service,
        string sectionPath,
        string? parentBucketPath,
        string rootAnchorId)
    {
        var bucketItems = new List<GraphInspectionGenericItem>();
        var rollupItems = new List<GraphInspectionGenericItem>();
        var noteItems = new List<GraphInspectionGenericItem>();
        var otherItems = new List<GraphInspectionGenericItem>();
        var childHighlightMap = parentBucketPath is null
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            : service.GetRollup(parentBucketPath)?.ChildHighlights
                .Where(x => !string.IsNullOrWhiteSpace(x.Path))
                .ToDictionary(x => x.Path, x => x.Summary, StringComparer.Ordinal)
                ?? new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var member in _store.IterateForward(containerId))
        {
            if (!_store.Objects.TryGetValue(member.ObjectId, out var obj))
                continue;

            if (obj.ObjectKind == SmartListService.BucketObjectKind)
            {
                var bucketPath = ReadStr(obj.SemanticPayload?.Provenance, "path") ?? member.ObjectId;
                var bucket = service.GetBucket(bucketPath);
                if (bucket is null)
                    continue;

                var bucketPreview = childHighlightMap.TryGetValue(bucket.Path, out var childHighlight)
                    ? childHighlight
                    : service.GetRollup(bucket.Path)?.Summary;
                bucketItems.Add(new GraphInspectionGenericItem(
                    bucket.ObjectId,
                    "container",
                    HumanizeToken(bucket.DisplayName),
                    "anchor:" + SmartListBucketAnchor(bucket.Path),
                    string.IsNullOrWhiteSpace(bucketPreview) ? null : bucketPreview.Trim()));
                continue;
            }

            if (obj.ObjectKind == SmartListService.NoteObjectKind)
            {
                var note = service.GetNote(member.ObjectId);
                if (note is null)
                    continue;

                noteItems.Add(new GraphInspectionGenericItem(
                    note.NoteId,
                    SmartListService.NoteObjectKind,
                    note.Title,
                    BuildSmartListNoteSummary(note, parentBucketPath),
                    BuildSmartListNoteBody(note, sectionPath)));
                continue;
            }

            if (obj.ObjectKind == SmartListService.RollupObjectKind)
            {
                var rollup = service.GetRollup(ReadStr(obj.SemanticPayload?.Provenance, "bucket_path") ?? member.ObjectId);
                if (rollup is null)
                    continue;

                rollupItems.Add(new GraphInspectionGenericItem(
                    rollup.RollupId,
                    SmartListService.RollupObjectKind,
                    "Rollup Summary",
                    TruncateSummaryText(rollup.Summary, 120),
                    BuildSmartListRollupBody(rollup)));
                continue;
            }

            otherItems.Add(BuildObjectItem(member.ObjectId));
        }

        return rollupItems
            .OrderBy(x => x.Label, StringComparer.OrdinalIgnoreCase)
            .Concat(bucketItems
            .OrderBy(x => x.Label, StringComparer.OrdinalIgnoreCase)
            .Concat(noteItems.OrderBy(x => x.Label, StringComparer.OrdinalIgnoreCase))
            .Concat(otherItems.OrderBy(x => x.Label, StringComparer.OrdinalIgnoreCase)))
            .ToList();
    }

    private List<SmartListBucketInfo> EnumerateSmartListBuckets(string containerId, SmartListService service)
    {
        return _store.IterateForward(containerId)
            .Select(x => x.ObjectId)
            .Where(id => _store.Objects.TryGetValue(id, out var obj) && obj.ObjectKind == SmartListService.BucketObjectKind)
            .Select(id => service.GetBucket(ReadStr(_store.Objects[id].SemanticPayload?.Provenance, "path") ?? id))
            .Where(x => x is not null)
            .Cast<SmartListBucketInfo>()
            .OrderBy(x => x.Path, StringComparer.Ordinal)
            .ToList();
    }

    private void AddNestedStructureSections(
        string rootId,
        string containerId,
        List<GraphInspectionSection> sections,
        int depth,
        HashSet<string> visited)
    {
        const int maxDepth = 4;
        if (depth > maxDepth || !visited.Add(containerId))
            return;

        foreach (var member in _store.IterateForward(containerId))
        {
            if (!_store.Containers.ContainsKey(member.ObjectId))
                continue;

            var memberId = member.ObjectId;
            if (ShouldSkipStructureContainer(rootId, memberId))
                continue;

            if (ShouldExposeStructureSection(rootId, memberId, depth))
            {
                var memberItems = BuildContainerMemberItems(rootId, memberId);
                sections.Add(new GraphInspectionGenericContainerSection(
                    FormatStructureHeading(memberId),
                    "structure-" + rootId.Replace(':', '-') + "-" + memberId.Replace(':', '-'),
                    memberItems,
                    Family: "structure",
                    NavGroup: "Structures",
                    NavLabel: FormatStructureNavLabel(rootId, memberId),
                    Path: $"Structures / {FormatStructureHeading(rootId)} / {FormatStructureHeading(memberId)}"));
            }

            AddNestedStructureSections(rootId, memberId, sections, depth + 1, visited);
        }

        visited.Remove(containerId);
    }

    private static bool ShouldExposeStructureSection(string rootId, string containerId, int depth)
    {
        if (depth <= 1)
            return true;

        if (!string.Equals(rootId, "agent-memory", StringComparison.Ordinal))
            return depth <= 2;

        if (containerId.StartsWith("lesson-freshness:", StringComparison.Ordinal))
            return true;

        if (containerId.StartsWith("agent-memory:semantic:", StringComparison.Ordinal))
            return true;

        if (containerId.StartsWith("lesson-semantic-theme:", StringComparison.Ordinal))
            return true;

        if (containerId.StartsWith("agent-memory:source:", StringComparison.Ordinal))
            return true;

        return false;
    }

    private List<GraphInspectionGenericItem> BuildStructureItems(string rootContainerId)
    {
        var items = new List<GraphInspectionGenericItem>();
        foreach (var link in _store.IterateForward(rootContainerId))
        {
            var memberId = link.ObjectId;
            if (_store.Containers.TryGetValue(memberId, out var childContainer))
            {
                if (ShouldSkipStructureContainer(rootContainerId, memberId))
                    continue;

                var label = BuildContainerLabel(memberId, childContainer.ContainerKind);
                var childAnchor = StructureAnchor(rootContainerId, memberId);
                items.Add(new GraphInspectionGenericItem(memberId, "container", label, "anchor:" + childAnchor, null));
                continue;
            }

            items.Add(BuildObjectItem(memberId));
        }

        return items;
    }

    private List<GraphInspectionGenericItem> BuildContainerMemberItems(string rootId, string containerId)
    {
        var items = new List<GraphInspectionGenericItem>();
        foreach (var member in _store.IterateForward(containerId))
        {
            var memberId = member.ObjectId;
            if (_store.Containers.TryGetValue(memberId, out var child))
            {
                if (ShouldSkipStructureContainer(rootId, memberId))
                    continue;

                var label = BuildContainerLabel(memberId, child.ContainerKind);
                var childAnchor = StructureAnchor(rootId, memberId);
                items.Add(new GraphInspectionGenericItem(memberId, "container", label, "anchor:" + childAnchor, null));
                continue;
            }

            items.Add(BuildObjectItem(memberId));
        }

        return items;
    }

    private bool ShouldSkipStructureContainer(string rootId, string containerId)
    {
        if (!string.Equals(rootId, "agent-memory", StringComparison.Ordinal))
            return false;

        return containerId.StartsWith("lesson-freshness:", StringComparison.Ordinal)
            && CountMembers(containerId) == 0;
    }

    private static string SlugifyAnchor(string value)
    {
        var slug = value.ToLowerInvariant();
        slug = System.Text.RegularExpressions.Regex.Replace(slug, "[^a-z0-9]+", "-");
        return slug.Trim('-');
    }

    private static IReadOnlyDictionary<string, SessionReferenceInfo> BuildSessionReferenceLookup(
        IReadOnlyDictionary<string, GraphInspectionSession> sessionItemById)
    {
        var lookup = new Dictionary<string, SessionReferenceInfo>(StringComparer.OrdinalIgnoreCase);
        foreach (var (sessionId, session) in sessionItemById)
        {
            var rawId = sessionId.StartsWith("chat-session:", StringComparison.Ordinal)
                ? sessionId["chat-session:".Length..]
                : sessionId;
            var day = TryExtractSessionDay(session.DateLabel);
            var info = new SessionReferenceInfo(session.Title, day);

            if (!string.IsNullOrWhiteSpace(rawId))
            {
                lookup.TryAdd(rawId, info);
                if (rawId.Length >= 8)
                    lookup.TryAdd(rawId[..8], info);
            }
        }

        return lookup;
    }

    private static IReadOnlyList<string> CanonicalizeMemAnchorSegments(
        string rawName,
        IReadOnlyDictionary<string, SessionReferenceInfo> sessionLookup)
    {
        var parsed = ParseMemAnchorSegments(rawName);
        if (parsed.Count == 0)
            return ["Other"];

        var path = new List<string>();
        var index = 0;

        if (parsed[index].IsBare && string.Equals(parsed[index].Value, "Agent Memory", StringComparison.OrdinalIgnoreCase))
        {
            path.Add("Agent Memory");
            index++;
        }

        if (index < parsed.Count && parsed[index].IsBare && string.Equals(parsed[index].Value, "Shared", StringComparison.OrdinalIgnoreCase))
        {
            path.Add("Shared");
            index++;
        }

        var remaining = parsed.Skip(index).ToList();
        var hasExplicitDay = remaining.Any(x => string.Equals(x.Key, "day", StringComparison.OrdinalIgnoreCase));

        foreach (var segment in remaining)
        {
            if (segment.IsBare)
            {
                path.Add(HumanizeToken(segment.Value));
                continue;
            }

            switch (segment.Key?.ToLowerInvariant())
            {
                case "source":
                    EnsureAnchorGroup(path, "Agent Memory", "Sources");
                    path.Add(segment.Value.Trim().ToLowerInvariant());
                    break;
                case "project":
                    if (path.Count == 0 || (path.Count == 1 && string.Equals(path[0], "Agent Memory", StringComparison.Ordinal)))
                        EnsureAnchorGroup(path, "Agent Memory", "Projects");
                    path.Add(segment.Value.Trim());
                    break;
                case "day":
                    if (path.Count == 0 || (path.Count == 1 && string.Equals(path[0], "Agent Memory", StringComparison.Ordinal)))
                        EnsureAnchorGroup(path, "Agent Memory", "Days");
                    path.Add(segment.Value.Trim());
                    break;
                case "session":
                    if (path.Count == 0 || (path.Count == 1 && string.Equals(path[0], "Agent Memory", StringComparison.Ordinal)))
                        EnsureAnchorGroup(path, "Agent Memory", "Sessions");

                    var session = ResolveSessionReference(segment.Value, sessionLookup);
                    if (!hasExplicitDay
                        && session.Day is not null
                        && IsSessionGroupPath(path))
                    {
                        path.Add(session.Day);
                    }

                    path.Add(session.Label);
                    break;
                default:
                    path.Add($"{HumanizeToken(segment.Key ?? string.Empty)}: {segment.Value.Trim()}");
                    break;
            }
        }

        return path.Count == 0 ? ["Other"] : path;
    }

    private static List<MemAnchorSegment> ParseMemAnchorSegments(string rawName)
    {
        var segments = new List<MemAnchorSegment>();
        foreach (var part in rawName.Split(" | ", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var colon = part.IndexOf(':', StringComparison.Ordinal);
            if (colon <= 0 || colon == part.Length - 1)
            {
                segments.Add(new MemAnchorSegment(null, part.Trim(), IsBare: true));
                continue;
            }

            var key = part[..colon].Trim();
            var value = part[(colon + 1)..].Trim();
            if (string.IsNullOrWhiteSpace(key) || string.IsNullOrWhiteSpace(value))
            {
                segments.Add(new MemAnchorSegment(null, part.Trim(), IsBare: true));
                continue;
            }

            segments.Add(new MemAnchorSegment(key, value, IsBare: false));
        }

        return segments;
    }

    private static void EnsureAnchorGroup(List<string> path, string rootLabel, string groupLabel)
    {
        if (path.Count == 0)
        {
            path.Add(groupLabel);
            return;
        }

        if (path.Count == 1 && string.Equals(path[0], rootLabel, StringComparison.Ordinal))
        {
            path.Add(groupLabel);
            return;
        }

        if (!string.Equals(path[^1], groupLabel, StringComparison.Ordinal))
            return;
    }

    private static SessionReferenceInfo ResolveSessionReference(
        string rawValue,
        IReadOnlyDictionary<string, SessionReferenceInfo> sessionLookup)
    {
        var key = rawValue.Trim();
        if (sessionLookup.TryGetValue(key, out var match))
            return match;

        return new SessionReferenceInfo(key, null);
    }

    private static bool IsSessionGroupPath(IReadOnlyList<string> path)
        => path.Count switch
        {
            1 => string.Equals(path[0], "Sessions", StringComparison.Ordinal),
            2 => string.Equals(path[0], "Agent Memory", StringComparison.Ordinal)
                 && string.Equals(path[1], "Sessions", StringComparison.Ordinal),
            _ => false
        };

    private static string? TryExtractSessionDay(string? dateLabel)
    {
        if (string.IsNullOrWhiteSpace(dateLabel))
            return null;

        var match = System.Text.RegularExpressions.Regex.Match(dateLabel, "\\d{4}-\\d{2}-\\d{2}");
        return match.Success ? match.Value : null;
    }

    private static string ResolveMemAnchorHeading(MemAnchorTreeNode node)
    {
        if (!LooksLikeOpaqueAnchorSegment(node.Segment))
            return node.Segment;

        foreach (var card in node.Cards)
        {
            if (TryExtractCardHeading(card.Label, out var heading))
                return heading;
        }

        return node.Segment;
    }

    private static bool LooksLikeOpaqueAnchorSegment(string value)
        => System.Text.RegularExpressions.Regex.IsMatch(value.Trim(), "^[0-9a-f]{8}$", System.Text.RegularExpressions.RegexOptions.IgnoreCase);

    private static bool TryExtractCardHeading(string rawLabel, out string heading)
    {
        heading = string.Empty;
        if (string.IsNullOrWhiteSpace(rawLabel))
            return false;

        var separator = rawLabel.IndexOf(" | ", StringComparison.Ordinal);
        if (separator < 0 || separator + 3 >= rawLabel.Length)
            return false;

        var candidate = rawLabel[(separator + 3)..].Trim();
        if (string.IsNullOrWhiteSpace(candidate))
            return false;

        heading = Short(candidate, 72);
        return true;
    }

    private static string ResolveMemAnchorPath(MemAnchorTreeNode node, string displayHeading)
    {
        var separator = node.Path.LastIndexOf(" / ", StringComparison.Ordinal);
        return separator < 0
            ? displayHeading
            : node.Path[..separator] + " / " + displayHeading;
    }

    private int CountMembers(string containerId)
        => _store.IterateForward(containerId).Count();

    private static string StructureAnchor(string rootId, string containerId)
        => "structure-" + rootId.Replace(':', '-') + "-" + containerId.Replace(':', '-');

    private static string ThreadAnchor(string threadId) => "task-thread-" + threadId.Replace(':', '-');
    private static string ThreadCheckpointAnchor(string threadId) => ThreadAnchor(threadId) + "-checkpoints";
    private static string ThreadChildrenAnchor(string threadId) => ThreadAnchor(threadId) + "-children";
    private static string ThreadArtifactsAnchor(string threadId) => ThreadAnchor(threadId) + "-artifacts";
    private static string ThreadObjectId(string threadId) => "task-thread:" + threadId;
    private static string SmartListRootAnchor(string rootLabel) => "smartlists-" + SlugifyAnchor(rootLabel);
    private static string SmartListBucketAnchor(string bucketPath) => "smartlist-" + SlugifyAnchor(bucketPath);
    private static string MembersContainerId(string path) => $"smartlist-members:{path}";

    private static string BuildCheckpointBody(TaskCheckpointInfo checkpoint)
    {
        var lines = new List<string>
        {
            $"current_step: {checkpoint.CurrentStep}",
            $"next_command: {checkpoint.NextCommand}"
        };

        if (!string.IsNullOrWhiteSpace(checkpoint.BranchOffAnchor))
            lines.Add($"branch_off_anchor: {checkpoint.BranchOffAnchor}");
        if (!string.IsNullOrWhiteSpace(checkpoint.ArtifactRef))
            lines.Add($"artifact_ref: {checkpoint.ArtifactRef}");

        return string.Join(Environment.NewLine, lines);
    }

    private static string ChildThreadLabel(string childId, IReadOnlyList<TaskThreadInfo> allThreads)
    {
        var child = allThreads.FirstOrDefault(x => string.Equals(x.ThreadId, childId, StringComparison.Ordinal));
        return child is null ? childId : $"{child.Title} [{child.Status}]";
    }

    private static string FormatStructureHeading(string containerId)
    {
        if (string.Equals(containerId, "agent-memory", StringComparison.Ordinal))
            return "Agent Memory";

        if (containerId.StartsWith("lesson-freshness:", StringComparison.Ordinal))
            return HumanizeToken(containerId["lesson-freshness:".Length..]);

        if (containerId.StartsWith("agent-memory:semantic:", StringComparison.Ordinal))
            return HumanizeToken(containerId["agent-memory:semantic:".Length..]);

        if (containerId.StartsWith("lesson-semantic-theme:", StringComparison.Ordinal))
        {
            var parts = containerId.Split(':', StringSplitOptions.RemoveEmptyEntries);
            return parts.Length >= 3
                ? $"{HumanizeToken(parts[1])}: {HumanizeToken(parts[2])}"
                : HumanizeToken(containerId);
        }

        if (containerId.StartsWith("agent-memory:source:", StringComparison.Ordinal))
            return $"Source: {HumanizeToken(containerId["agent-memory:source:".Length..])}";

        if (containerId.StartsWith("agent-memory:", StringComparison.Ordinal))
            return HumanizeToken(containerId["agent-memory:".Length..]);

        return HumanizeToken(containerId);
    }

    private static string FormatStructureNavLabel(string rootId, string containerId)
    {
        if (string.Equals(rootId, containerId, StringComparison.Ordinal))
            return FormatStructureHeading(containerId);

        var rootLabel = FormatStructureHeading(rootId);
        var label = FormatStructureHeading(containerId);
        return string.Equals(rootId, "agent-memory", StringComparison.Ordinal)
            ? $"{rootLabel} / {label}"
            : $"{rootLabel} / {label}";
    }

    private static string HumanizeToken(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return value;

        var token = value.Trim();
        token = token.Replace('-', ' ').Replace('_', ' ');
        token = token.Replace(":", ": ");
        token = System.Text.RegularExpressions.Regex.Replace(token, "\\s+", " ").Trim();

        return token.Length switch
        {
            0 => token,
            _ when token.Equals("1d", StringComparison.OrdinalIgnoreCase) => "1d",
            _ when token.Equals("7d", StringComparison.OrdinalIgnoreCase) => "7d",
            _ when token.Equals("30d", StringComparison.OrdinalIgnoreCase) => "30d",
            _ when token.Equals("90d", StringComparison.OrdinalIgnoreCase) => "90d",
            _ => string.Join(" ", token
                .Split(' ', StringSplitOptions.RemoveEmptyEntries)
                .Select(CapitalizeToken))
        };
    }

    private static string CapitalizeToken(string token)
    {
        if (token.Length == 0)
            return token;
        if (token.All(char.IsUpper))
            return token;
        return char.ToUpperInvariant(token[0]) + token[1..];
    }

    private sealed class MemAnchorTreeNode(string segment, string path)
    {
        public string Segment { get; } = segment;
        public string Path { get; } = path;
        public Dictionary<string, MemAnchorTreeNode> Children { get; } = new(StringComparer.OrdinalIgnoreCase);
        public string? AnchorId { get; set; }
        public List<GraphInspectionGenericItem> Cards { get; set; } = [];
    }

    private sealed record MemAnchorSegment(string? Key, string Value, bool IsBare);
    private sealed record SessionReferenceInfo(string Label, string? Day);

    private string BuildContainerLabel(string containerId, string containerKind)
    {
        if (containerId.StartsWith("lesson-semantic-node:", StringComparison.Ordinal))
        {
            var semanticTitle = _store.IterateForward(containerId)
                .Select(x => x.ObjectId)
                .Select(id => _store.Objects.TryGetValue(id, out var obj) ? obj : null)
                .Where(x => x is not null && x.ObjectKind == "lesson_semantic_node")
                .Select(x => x!.SemanticPayload?.Summary)
                .FirstOrDefault(x => !string.IsNullOrWhiteSpace(x));

            if (!string.IsNullOrWhiteSpace(semanticTitle))
                return $"{semanticTitle} [{containerKind}] ({CountMembers(containerId)} members)";
        }

        if (containerId.StartsWith("lesson-semantic-theme:", StringComparison.Ordinal))
        {
            var themeTitle = _store.IterateForward(containerId)
                .Select(x => x.ObjectId)
                .Select(id => _store.Objects.TryGetValue(id, out var obj) ? obj : null)
                .Where(x => x is not null && x.ObjectKind == "lesson_semantic_theme")
                .Select(x => x!.SemanticPayload?.Summary)
                .FirstOrDefault(x => !string.IsNullOrWhiteSpace(x));

            if (!string.IsNullOrWhiteSpace(themeTitle))
                return $"{themeTitle} [{containerKind}] ({CountMembers(containerId)} members)";
        }

        return $"{containerId} [{containerKind}] ({CountMembers(containerId)} members)";
    }

    private GraphInspectionGenericItem BuildObjectItem(string objectId)
    {
        if (!_store.Objects.TryGetValue(objectId, out var obj))
            return new GraphInspectionGenericItem(objectId, "object", objectId, null, null);

        var label = BuildObjectLabel(objectId);
        var body = BuildObjectBody(obj);
        return new GraphInspectionGenericItem(objectId, obj.ObjectKind, label, null, body);
    }

    private string BuildObjectLabel(string objectId)
    {
        if (!_store.Objects.TryGetValue(objectId, out var obj))
            return objectId;

        var summary = obj.SemanticPayload?.Summary;
        if (string.IsNullOrWhiteSpace(summary))
            return objectId;

        if (obj.ObjectKind is "lesson" or "lesson_semantic_node")
            return Short(summary, 120);
        if (obj.ObjectKind == SmartListService.RollupObjectKind)
            return $"Rollup - {Short(ReadStr(obj.SemanticPayload?.Provenance, "bucket_path") ?? summary, 120)}";

        return $"{objectId} - {Short(summary, 120)}";
    }

    private string? BuildObjectBody(ObjectRecord obj)
    {
        if (obj.ObjectKind == "lesson")
            return BuildLessonBody(obj);
        if (obj.ObjectKind == "lesson_semantic_node")
            return BuildSemanticNodeBody(obj);
        if (obj.ObjectKind == SmartListService.RollupObjectKind)
            return BuildSmartListRollupBody(obj);

        var summary = obj.SemanticPayload?.Summary;
        if (!string.IsNullOrWhiteSpace(summary))
            return summary.Trim();

        return null;
    }

    private static string BuildSmartListBucketBody(SmartListBucketInfo bucket)
    {
        var lines = new List<string>
        {
            $"path: {bucket.Path}",
            $"object_id: {bucket.ObjectId}",
            $"durability: {bucket.Durability}"
        };

        if (!string.IsNullOrWhiteSpace(bucket.ParentPath))
            lines.Add($"parent_path: {bucket.ParentPath}");

        lines.Add($"created_at: {bucket.CreatedAt:o}");
        lines.Add($"updated_at: {bucket.UpdatedAt:o}");
        return string.Join(Environment.NewLine, lines);
    }

    private static string BuildSmartListNoteSummary(SmartListNoteInfo note, string? currentBucketPath)
    {
        var otherBuckets = note.BucketPaths
            .Where(path => !string.Equals(path, currentBucketPath, StringComparison.Ordinal))
            .ToList();
        if (otherBuckets.Count == 0)
            return $"attached in {note.BucketPaths.Count} bucket";

        return "also in: " + string.Join(", ", otherBuckets);
    }

    private static string BuildSmartListNoteBody(SmartListNoteInfo note, string sectionPath)
    {
        var lines = new List<string>
        {
            note.Text.Trim(),
            string.Empty,
            $"note_id: {note.NoteId}",
            $"durability: {note.Durability}",
            $"section_path: {sectionPath}",
            "bucket_paths:"
        };

        lines.AddRange(note.BucketPaths.Select(path => $"- {path}"));
        lines.Add($"created_at: {note.CreatedAt:o}");
        lines.Add($"updated_at: {note.UpdatedAt:o}");
        return string.Join(Environment.NewLine, lines.Where(line => line is not null));
    }

    private static string BuildSmartListRollupBody(SmartListRollupInfo rollup)
    {
        var lines = new List<string>
        {
            $"title: {rollup.Title}",
            $"summary: {rollup.Summary}",
            $"scope: {rollup.Scope}"
        };

        if (!string.IsNullOrWhiteSpace(rollup.StopHint))
            lines.Add($"stop_hint: {rollup.StopHint}");

        if (rollup.ChildHighlights.Count > 0)
        {
            lines.Add("child_highlights:");
            lines.AddRange(rollup.ChildHighlights.Select(child => $"- {child.Path}: {child.Summary}"));
        }

        lines.Add($"source_mode: {rollup.SourceMode}");
        lines.Add($"bucket_path: {rollup.BucketPath}");
        lines.Add($"durability: {rollup.Durability}");
        lines.Add($"created_at: {rollup.CreatedAt:o}");
        lines.Add($"updated_at: {rollup.UpdatedAt:o}");
        return string.Join(Environment.NewLine, lines);
    }

    private static string BuildSmartListRollupBody(ObjectRecord obj)
    {
        var prov = obj.SemanticPayload?.Provenance;
        var lines = new List<string>
        {
            $"title: {ReadStr(prov, "title")}",
            $"summary: {ReadStr(prov, "summary")}",
            $"scope: {ReadStr(prov, "scope")}"
        };

        var stopHint = ReadStr(prov, "stop_hint");
        if (!string.IsNullOrWhiteSpace(stopHint))
            lines.Add($"stop_hint: {stopHint}");

        if (prov is not null
            && prov.TryGetValue("child_highlights", out var childEl)
            && childEl.ValueKind == System.Text.Json.JsonValueKind.Array)
        {
            lines.Add("child_highlights:");
            foreach (var child in childEl.EnumerateArray())
            {
                if (child.ValueKind != System.Text.Json.JsonValueKind.Object)
                    continue;
                var path = child.TryGetProperty("path", out var pathEl) ? pathEl.ToString() : string.Empty;
                var summary = child.TryGetProperty("summary", out var summaryEl) ? summaryEl.ToString() : string.Empty;
                lines.Add($"- {path}: {summary}");
            }
        }

        lines.Add($"bucket_path: {ReadStr(prov, "bucket_path")}");
        lines.Add($"durability: {ReadStr(prov, "durability")}");
        return string.Join(Environment.NewLine, lines);
    }

    private string BuildLessonBody(ObjectRecord lesson)
    {
        var lines = new List<string>();
        var prov = lesson.SemanticPayload?.Provenance;

        lines.Add($"id: {lesson.ObjectId}");
        lines.Add($"title: {lesson.SemanticPayload?.Summary ?? lesson.ObjectId}");
        lines.Add($"title_quality: {ReadStr(prov, "title_quality")}");
        lines.Add($"freshness_tier: {ReadStr(prov, "freshness_tier")}");
        lines.Add($"confidence: {ReadStr(prov, "confidence")}");
        lines.Add($"evidence_health: {ReadStr(prov, "evidence_health")}");
        lines.Add($"touch_count: {ReadStr(prov, "touch_count")}");
        lines.Add($"last_touched_at: {ReadStr(prov, "last_touched_at")}");
        lines.Add($"decay_multiplier: {ReadStr(prov, "decay_multiplier")}");
        lines.Add($"stereotype_family_id: {ReadStr(prov, "stereotype_family_id")}");
        lines.Add($"stereotype_version_id: {ReadStr(prov, "stereotype_version_id")}");
        lines.Add($"origin_dream_id: {ReadStr(prov, "origin_dream_id")}");

        var sourceContainerId = $"lesson-sources:{lesson.ObjectId}";
        if (_store.Containers.ContainsKey(sourceContainerId))
        {
            var sourceLinks = _store.IterateForward(sourceContainerId).ToList();
            lines.Add($"source_links ({sourceLinks.Count}):");
            foreach (var sourceLink in sourceLinks.Take(8))
            {
                var status = ReadStr(sourceLink.Metadata, "link_status");
                var capturedAt = ReadStr(sourceLink.Metadata, "captured_at");
                var statusLabel = string.IsNullOrWhiteSpace(status) ? "unknown" : status;
                lines.Add($"- [{statusLabel}] {sourceLink.ObjectId}");
                if (!string.IsNullOrWhiteSpace(capturedAt))
                    lines.Add($"  captured_at: {capturedAt}");
            }
            if (sourceLinks.Count > 8)
                lines.Add($"- ... +{sourceLinks.Count - 8} more");
        }

        if (prov is not null && prov.TryGetValue("evidence_snapshots", out var snapshotsEl) && snapshotsEl.ValueKind == System.Text.Json.JsonValueKind.Array)
        {
            var snapshots = snapshotsEl.EnumerateArray().ToList();
            lines.Add($"evidence_snapshots ({snapshots.Count}):");
            foreach (var snapshot in snapshots.Take(8))
            {
                if (snapshot.ValueKind != System.Text.Json.JsonValueKind.Object)
                    continue;
                var status = snapshot.TryGetProperty("link_status", out var statusEl) ? statusEl.ToString() : "unknown";
                var sourceRef = snapshot.TryGetProperty("source_ref", out var refEl) ? refEl.ToString() : string.Empty;
                var snippet = snapshot.TryGetProperty("snippet", out var snippetEl) ? snippetEl.ToString() : string.Empty;
                lines.Add($"- [{status}] {sourceRef}");
                if (!string.IsNullOrWhiteSpace(snippet))
                    lines.Add($"  {Short(snippet, 180)}");
            }
            if (snapshots.Count > 8)
                lines.Add($"- ... +{snapshots.Count - 8} more");
        }

        return string.Join(Environment.NewLine, lines.Where(x => !string.IsNullOrWhiteSpace(x)));
    }

    private string BuildSemanticNodeBody(ObjectRecord node)
    {
        var lines = new List<string>();
        var prov = node.SemanticPayload?.Provenance;
        lines.Add($"id: {node.ObjectId}");
        lines.Add($"title: {node.SemanticPayload?.Summary ?? node.ObjectId}");
        lines.Add($"kind: {ReadStr(prov, "kind")}");
        lines.Add($"lesson_count: {ReadStr(prov, "lesson_count")}");
        lines.Add($"representative_lesson_id: {ReadStr(prov, "representative_lesson_id")}");
        lines.Add($"members_container_id: {ReadStr(prov, "members_container_id")}");
        lines.Add($"sources_container_id: {ReadStr(prov, "sources_container_id")}");
        return string.Join(Environment.NewLine, lines.Where(x => !string.IsNullOrWhiteSpace(x)));
    }

    private static string Short(string value, int maxLen)
        => value.Length <= maxLen ? value : value[..(maxLen - 3)].TrimEnd() + "...";

    private SessionMaps BuildSessionMaps()
    {
        var sessionTitleById = new Dictionary<string, string>(StringComparer.Ordinal);
        var msgToSessionId = new Dictionary<string, string>(StringComparer.Ordinal);
        var sessionItemById = new Dictionary<string, GraphInspectionSession>(StringComparer.Ordinal);
        var sessionSourceById = BuildSessionSourceMap();

        var chatSessions = _store.Containers.Values
            .Where(c => c.ContainerKind == "chat_session")
            .OrderBy(c => ReadStr(c.Metadata, "started_at"), StringComparer.Ordinal)
            .ToList();

        if (chatSessions.Count == 0)
            return new SessionMaps(sessionItemById, sessionTitleById, msgToSessionId);

        foreach (var session in chatSessions)
        {
            var title = ReadStr(session.Metadata, "title");
            sessionTitleById[session.ContainerId] = string.IsNullOrWhiteSpace(title)
                ? session.ContainerId
                : title;

            foreach (var ln in _store.IterateForward(session.ContainerId))
                msgToSessionId.TryAdd(ln.ObjectId, session.ContainerId);

            sessionItemById[session.ContainerId] = BuildSessionItem(session, sessionSourceById);
        }

        return new SessionMaps(sessionItemById, sessionTitleById, msgToSessionId);
    }


    private Dictionary<string, string> BuildSessionSourceMap()
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var anchor in _store.Containers.Values.Where(c => c.ContainerKind == "memanchor"))
        {
            var name = ReadStr(anchor.Metadata, "name");
            var source = ParseSourceFromMemAnchorName(name);
            if (string.IsNullOrWhiteSpace(source))
                continue;

            foreach (var ln in _store.IterateForward(anchor.ContainerId))
            {
                if (!ln.ObjectId.StartsWith("session-ref:", StringComparison.Ordinal))
                    continue;

                var sessionId = "chat-session:" + ln.ObjectId["session-ref:".Length..];
                if (!map.ContainsKey(sessionId))
                    map[sessionId] = source;
            }
        }

        return map;
    }

    private static string? ParseSourceFromMemAnchorName(string? name)
    {
        if (string.IsNullOrWhiteSpace(name) || !name.StartsWith("Source:", StringComparison.OrdinalIgnoreCase))
            return null;

        var rest = name["Source:".Length..].Trim();
        if (string.IsNullOrWhiteSpace(rest))
            return null;

        var sep = rest.IndexOf(" | ", StringComparison.Ordinal);
        var source = (sep >= 0 ? rest[..sep] : rest).Trim().ToLowerInvariant();
        return string.IsNullOrWhiteSpace(source) ? null : source;
    }

    private GraphInspectionSession BuildSessionItem(
        ContainerRecord container,
        IReadOnlyDictionary<string, string> sessionSourceById)
    {
        var sessionGuid = container.ContainerId["chat-session:".Length..];
        var anchorId = "chat-session-" + sessionGuid;
        var title = ReadStr(container.Metadata, "title");
        var channel = ReadStr(container.Metadata, "channel");
        var source = sessionSourceById.TryGetValue(container.ContainerId, out var structuralSource)
            ? structuralSource
            : ClassifySessionSource(channel);
        var startedAt = ReadStr(container.Metadata, "started_at");

        var parsedStarted = DateTimeOffset.TryParse(startedAt, out var parsed)
            ? parsed
            : DateTimeOffset.MinValue;

        var dateLabel = parsedStarted == DateTimeOffset.MinValue
            ? string.Empty
            : " · " + parsedStarted.ToString("yyyy-MM-dd HH:mm");

        var messages = _store.IterateForward(container.ContainerId)
            .Select(ln => _store.Objects.TryGetValue(ln.ObjectId, out var obj) ? obj : null)
            .Where(obj => obj is not null)
            .Select(obj => BuildMessage(obj!))
            .ToList();

        string? synopsis = messages.Count == 0
            ? null
            : ExtractSynopsis(string.Join(Environment.NewLine, messages.Select(m => m.Text)));

        var participants = new List<string>();
        if (container.Metadata?.TryGetValue("participants", out var partsEl) == true)
        {
            foreach (var p in partsEl.EnumerateArray())
            {
                var author = p.TryGetProperty("author", out var a) ? a.GetString() ?? string.Empty : string.Empty;
                var direction = p.TryGetProperty("direction", out var d) ? d.GetString() ?? string.Empty : string.Empty;
                if (!string.IsNullOrWhiteSpace(author))
                    participants.Add($"{author} ({direction})");
            }
        }

        var defaultTitle = string.IsNullOrWhiteSpace(channel)
            ? "session"
            : $"{channel} session";

        return new GraphInspectionSession(
            container.ContainerId,
            anchorId,
            string.IsNullOrWhiteSpace(title) ? defaultTitle : title,
            dateLabel,
            string.IsNullOrWhiteSpace(channel) ? null : channel,
            synopsis,
            participants,
            messages,
            ReadNum(container.Metadata, "tokens_in"),
            ReadNum(container.Metadata, "tokens_out"),
            ReadNum(container.Metadata, "tokens_cache_read"),
            ReadNum(container.Metadata, "tokens_cache_create"),
            Source: source);
    }

    private static string ClassifySessionSource(string? channel)
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

    private static GraphInspectionMessage BuildMessage(ObjectRecord obj)
    {
        var prov = obj.SemanticPayload?.Provenance;
        var direction = ReadStr(prov, "direction");
        var author = ReadStr(prov, "author");
        if (string.IsNullOrWhiteSpace(author))
            author = direction;

        var ts = ReadStr(prov, "ts");
        var text = ReadStr(prov, "text");
        if (string.IsNullOrWhiteSpace(text))
            text = obj.SemanticPayload?.Summary ?? string.Empty;

        var timestamp = DateTimeOffset.TryParse(ts, out var parsed)
            ? parsed.ToString("HH:mm")
            : (string.IsNullOrWhiteSpace(ts) ? null : ts);

        return new GraphInspectionMessage(author, direction, text, timestamp);
    }

    private static int DreamKindOrder(string kind) => kind switch
    {
        "topic" => 0,
        "thread" => 1,
        "decision" => 2,
        "invariant" => 3,
        _ => 99
    };

    private static string ClassifyNodeRole(string pageId)
        => InspectionTitlePolicy.IsStructuralMembersId(pageId) ? "structural" : "semantic";

    private static string InferKindFromPageId(string pageId)
    {
        if (string.IsNullOrWhiteSpace(pageId))
            return "node";

        var colon = pageId.IndexOf(':', StringComparison.Ordinal);
        if (colon > 0)
            return pageId[..colon];

        var dash = pageId.IndexOf('-', StringComparison.Ordinal);
        if (dash > 0)
            return pageId[..dash];

        return "node";
    }

    private static string ReadStr(Dictionary<string, System.Text.Json.JsonElement>? meta, string key)
    {
        if (meta is null)
            return string.Empty;

        return meta.TryGetValue(key, out var el)
            ? el.ValueKind == System.Text.Json.JsonValueKind.String
                ? el.GetString() ?? string.Empty
                : el.ToString()
            : string.Empty;
    }

    private static int ReadNum(Dictionary<string, System.Text.Json.JsonElement>? meta, string key)
    {
        if (meta is null)
            return 0;

        return meta.TryGetValue(key, out var el) && el.TryGetInt32(out var v)
            ? v
            : 0;
    }

    private static List<(string Label, string PageId)> ParseAtlasBullets(string markdown, string sectionHeader)
    {
        var result = new List<(string Label, string PageId)>();
        var inSection = false;

        foreach (var line in markdown.Split('\n'))
        {
            var trimmed = line.TrimEnd();

            if (trimmed.StartsWith("## ", StringComparison.Ordinal))
            {
                inSection = string.Equals(trimmed, $"## {sectionHeader}", StringComparison.Ordinal);
                continue;
            }

            if (!inSection || !trimmed.StartsWith("- ", StringComparison.Ordinal))
                continue;

            var raw = trimmed[2..];
            const string pageMarker = "(page: ";
            var pageIdx = raw.LastIndexOf(pageMarker, StringComparison.Ordinal);
            if (pageIdx < 0)
                continue;

            var pageId = raw[(pageIdx + pageMarker.Length)..].TrimEnd(')').Trim();
            var label = raw[..pageIdx].Trim();
            if (string.IsNullOrWhiteSpace(pageId))
                continue;

            result.Add((label, pageId));
        }

        return result;
    }

    private static string? ExtractSynopsis(string markdown)
    {
        var inSynopsis = false;
        foreach (var line in markdown.Split('\n'))
        {
            var trimmed = line.TrimEnd();
            if (trimmed == "## Synopsis")
            {
                inSynopsis = true;
                continue;
            }

            if (trimmed.StartsWith("## ", StringComparison.Ordinal))
            {
                if (inSynopsis)
                    break;
                continue;
            }

            if (!inSynopsis)
                continue;

            if (!string.IsNullOrWhiteSpace(trimmed) && !trimmed.StartsWith("_No ", StringComparison.Ordinal))
                return trimmed;
        }

        return null;
    }

    private sealed record SessionMaps(
        Dictionary<string, GraphInspectionSession> SessionItemById,
        Dictionary<string, string> SessionTitleById,
        Dictionary<string, string> MsgToSessionId);
}





