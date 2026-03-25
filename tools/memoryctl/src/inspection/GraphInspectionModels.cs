namespace MemoryCtl.Inspection;

internal sealed record GraphInspectionSnapshot(
    string Title,
    int TotalSessions,
    int TotalCards,
    int TotalAnchors,
    IReadOnlyList<GraphInspectionNavEntry> AtlasTopMap,
    IReadOnlyList<GraphInspectionNavEntry> AtlasThreads,
    IReadOnlyList<GraphInspectionNavEntry> AtlasPinned,
    IReadOnlyList<GraphInspectionSection> Sections
);

internal sealed record GraphInspectionNavEntry(
    string Label,
    string PageId,
    string NodeRole = "semantic"
);

internal abstract record GraphInspectionSection(string Heading);

internal sealed record GraphInspectionTimelineSection(
    string Heading,
    IReadOnlyList<GraphInspectionTimelineYear> Groups
) : GraphInspectionSection(Heading);

internal sealed record GraphInspectionDreamNodesSection(
    string Heading,
    IReadOnlyList<GraphInspectionDreamGroup> Kinds
) : GraphInspectionSection(Heading);

internal sealed record GraphInspectionDiagnosticsSection(
    string Heading,
    bool IndexDriftDetected,
    int ChatSessionContainers,
    int ChatSessionsIndexEntries
) : GraphInspectionSection(Heading);

internal sealed record GraphInspectionGenericContainerSection(
    string Heading,
    string AnchorId,
    IReadOnlyList<GraphInspectionGenericItem> Items,
    string Family = "memory_cards",
    string NavGroup = "Memory Cards",
    string? NavLabel = null,
    string? Path = null,
    string? Synopsis = null,
    bool IncludeInNav = true
) : GraphInspectionSection(Heading);

internal sealed record GraphInspectionTimelineYear(
    int Year,
    IReadOnlyList<GraphInspectionTimelineMonth> Months
);

internal sealed record GraphInspectionTimelineMonth(
    int Year,
    int Month,
    IReadOnlyList<GraphInspectionTimelineDay> Days
);

internal sealed record GraphInspectionTimelineDay(
    int Year,
    int Month,
    int Day,
    string Summary,
    IReadOnlyList<GraphInspectionDayEntry> Entries
);

internal abstract record GraphInspectionDayEntry;

internal sealed record GraphInspectionSingleSessionEntry(
    GraphInspectionSession Session
) : GraphInspectionDayEntry;

internal sealed record GraphInspectionSegmentGroupEntry(
    GraphInspectionSegmentGroup Group
) : GraphInspectionDayEntry;

internal sealed record GraphInspectionSegmentGroup(
    int Count,
    string Duration,
    string TimeRange,
    IReadOnlyList<GraphInspectionSession> Sessions
);

internal sealed record GraphInspectionSession(
    string Id,
    string AnchorId,
    string Title,
    string DateLabel,
    string? Channel,
    string? Synopsis,
    IReadOnlyList<string> Participants,
    IReadOnlyList<GraphInspectionMessage> Messages,
    int TokensIn = 0,
    int TokensOut = 0,
    int TokensCacheRead = 0,
    int TokensCacheCreate = 0,
    int SessionCount = 1,
    string NodeRole = "semantic",
    string TitleQuality = "high",
    string GroupKey = "",
    string Source = "other"
);

internal sealed record GraphInspectionMessage(
    string Author,
    string Direction,
    string Text,
    string? Timestamp
);

internal sealed record GraphInspectionDreamGroup(
    string Kind,
    IReadOnlyList<GraphInspectionDreamNode> Nodes
);

internal sealed record GraphInspectionDreamNode(
    string Id,
    string AnchorId,
    string Label,
    double Score,
    IReadOnlyList<GraphInspectionMemberLink> MemberLinks,
    string NodeRole = "semantic",
    string TitleQuality = "high",
    string GroupKey = ""
);

internal sealed record GraphInspectionMemberLink(
    string Title,
    string AnchorId
);

internal sealed record GraphInspectionGenericItem(
    string Id,
    string Kind,
    string Label,
    string? Summary,
    string? BodyText = null
);
