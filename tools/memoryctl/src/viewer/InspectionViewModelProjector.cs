using MemoryCtl.Inspection;

namespace MemoryCtl.Viewer;

/// <summary>
/// Projects a backend-neutral inspection snapshot into HTML view models.
/// </summary>
internal sealed class InspectionViewModelProjector
{
    public RootViewModel Project(GraphInspectionSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        return new RootViewModel(
            snapshot.Title,
            snapshot.TotalSessions,
            snapshot.TotalCards,
            snapshot.TotalAnchors,
            snapshot.AtlasTopMap.Select(x => (x.Label, x.PageId, x.NodeRole)).ToList(),
            snapshot.AtlasThreads.Select(x => (x.Label, x.PageId, x.NodeRole)).ToList(),
            snapshot.AtlasPinned.Select(x => (x.Label, x.PageId, x.NodeRole)).ToList(),
            snapshot.Sections.Select(ProjectSection).ToList());
    }

    private static SectionViewModel ProjectSection(GraphInspectionSection section)
    {
        return section switch
        {
            GraphInspectionTimelineSection timeline => new TimelineSection(
                timeline.Heading,
                timeline.Groups.Select(ProjectTimelineYear).ToList()),
            GraphInspectionDreamNodesSection dream => new DreamNodesSection(
                dream.Heading,
                dream.Kinds.Select(ProjectDreamGroup).ToList()),
            GraphInspectionDiagnosticsSection diagnostics => new DiagnosticsSection(
                diagnostics.Heading,
                diagnostics.IndexDriftDetected,
                diagnostics.ChatSessionContainers,
                diagnostics.ChatSessionsIndexEntries),
            GraphInspectionGenericContainerSection generic => new GenericContainerSection(
                generic.Heading,
                generic.AnchorId,
                generic.Items.Select(ProjectGenericItem).ToList(),
                generic.Family,
                generic.NavGroup,
                generic.NavLabel,
                generic.Path,
                generic.Synopsis,
                generic.IncludeInNav),
            _ => throw new InvalidOperationException($"Unsupported inspection section type: {section.GetType().Name}")
        };
    }

    private static TimelineGroupViewModel ProjectTimelineYear(GraphInspectionTimelineYear year)
        => new(year.Year, year.Months.Select(ProjectTimelineMonth).ToList());

    private static TimelineMonthViewModel ProjectTimelineMonth(GraphInspectionTimelineMonth month)
        => new(month.Year, month.Month, month.Days.Select(ProjectTimelineDay).ToList());

    private static TimelineDayViewModel ProjectTimelineDay(GraphInspectionTimelineDay day)
        => new(day.Year, day.Month, day.Day, day.Summary, day.Entries.Select(ProjectDayEntry).ToList());

    private static DayEntryViewModel ProjectDayEntry(GraphInspectionDayEntry entry)
    {
        return entry switch
        {
            GraphInspectionSingleSessionEntry single => new SingleSessionEntry(ProjectSession(single.Session)),
            GraphInspectionSegmentGroupEntry group => new SegmentGroupEntry(ProjectSegmentGroup(group.Group)),
            _ => throw new InvalidOperationException($"Unsupported day entry type: {entry.GetType().Name}")
        };
    }

    private static SegmentGroupViewModel ProjectSegmentGroup(GraphInspectionSegmentGroup group)
        => new(group.Count, group.Duration, group.TimeRange, group.Sessions.Select(ProjectSession).ToList());

    private static SessionItemViewModel ProjectSession(GraphInspectionSession session)
        => new(
            session.Id,
            session.AnchorId,
            session.Title,
            session.DateLabel,
            session.Channel,
            session.Synopsis,
            session.Participants,
            session.Messages.Select(ProjectMessage).ToList(),
            session.TokensIn,
            session.TokensOut,
            session.TokensCacheRead,
            session.TokensCacheCreate,
            session.SessionCount,
            session.NodeRole,
            session.TitleQuality,
            session.GroupKey,
            session.Source);

    private static MessageViewModel ProjectMessage(GraphInspectionMessage message)
        => new(message.Author, message.Direction, message.Text, message.Timestamp);

    private static DreamGroupViewModel ProjectDreamGroup(GraphInspectionDreamGroup group)
        => new(group.Kind, group.Nodes.Select(ProjectDreamNode).ToList());

    private static DreamNodeViewModel ProjectDreamNode(GraphInspectionDreamNode node)
        => new(
            node.Id,
            node.AnchorId,
            node.Label,
            node.Score,
            node.MemberLinks.Select(x => (x.Title, x.AnchorId)).ToList(),
            node.NodeRole,
            node.TitleQuality,
            node.GroupKey);

    private static GenericItemViewModel ProjectGenericItem(GraphInspectionGenericItem item)
        => new(item.Id, item.Kind, item.Label, item.Summary, item.BodyText);
}
