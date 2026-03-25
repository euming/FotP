namespace MemoryCtl.Viewer;

// Root
internal sealed record RootViewModel(
    string Title,
    int TotalSessions,
    int TotalCards,
    int TotalAnchors,
    IReadOnlyList<(string Label, string PageId, string NodeRole)> AtlasTopMap,
    IReadOnlyList<(string Label, string PageId, string NodeRole)> AtlasThreads,
    IReadOnlyList<(string Label, string PageId, string NodeRole)> AtlasPinned,
    IReadOnlyList<SectionViewModel> Sections
);

// Sections
internal abstract record SectionViewModel(string Heading);

internal sealed record TimelineSection(
    string Heading,
    IReadOnlyList<TimelineGroupViewModel> Groups
) : SectionViewModel(Heading);

internal sealed record DreamNodesSection(
    string Heading,
    IReadOnlyList<DreamGroupViewModel> Kinds
) : SectionViewModel(Heading);

internal sealed record DiagnosticsSection(
    string Heading,
    bool IndexDriftDetected,
    int ChatSessionContainers,
    int ChatSessionsIndexEntries
) : SectionViewModel(Heading);

internal sealed record GenericContainerSection(
    string Heading,
    string AnchorId,
    IReadOnlyList<GenericItemViewModel> Items,
    string Family = "memory_cards",
    string NavGroup = "Memory Cards",
    string? NavLabel = null,
    string? Path = null,
    string? Synopsis = null,
    bool IncludeInNav = true
) : SectionViewModel(Heading);

// Timeline
internal sealed record TimelineGroupViewModel(
    int Year,
    IReadOnlyList<TimelineMonthViewModel> Months
);

internal sealed record TimelineMonthViewModel(
    int Year,
    int Month,
    IReadOnlyList<TimelineDayViewModel> Days
);

internal sealed record TimelineDayViewModel(
    int Year,
    int Month,
    int Day,
    string Summary,
    IReadOnlyList<DayEntryViewModel> Entries
);

// Discriminated union for day entries
internal abstract record DayEntryViewModel;
internal sealed record SingleSessionEntry(SessionItemViewModel Session) : DayEntryViewModel;
internal sealed record SegmentGroupEntry(SegmentGroupViewModel Group) : DayEntryViewModel;

internal sealed record SegmentGroupViewModel(
    int Count,
    string Duration,
    string TimeRange,
    IReadOnlyList<SessionItemViewModel> Sessions
);

internal sealed record SessionItemViewModel(
    string Id,
    string AnchorId,
    string Title,
    string DateLabel,
    string? Channel,
    string? Synopsis,
    IReadOnlyList<string> Participants,
    IReadOnlyList<MessageViewModel> Messages,
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

internal sealed record MessageViewModel(
    string Author,
    string Direction,
    string Text,
    string? Timestamp
);

// Dream nodes
internal sealed record DreamGroupViewModel(
    string Kind,
    IReadOnlyList<DreamNodeViewModel> Nodes
);

internal sealed record DreamNodeViewModel(
    string Id,
    string AnchorId,
    string Label,
    double Score,
    IReadOnlyList<(string Title, string AnchorId)> MemberLinks,
    string NodeRole = "semantic",
    string TitleQuality = "high",
    string GroupKey = ""
);

// Generic
internal sealed record GenericItemViewModel(
    string Id,
    string Kind,
    string Label,
    string? Summary,
    string? BodyText = null
);
