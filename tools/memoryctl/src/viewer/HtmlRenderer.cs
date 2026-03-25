using System.Reflection;
using System.Text;

namespace MemoryCtl.Viewer;

/// <summary>
/// Renders a <see cref="RootViewModel"/> to a self-contained HTML string.
/// Knows about HTML and ViewModels; has no AMS knowledge.
/// </summary>
internal sealed class HtmlRenderer
{
    public string Render(RootViewModel vm)
    {
        var css = LoadResource("viewer.css");
        var js = LoadResource("viewer.js");

        var sb = new StringBuilder();
        sb.AppendLine("<!DOCTYPE html>");
        sb.AppendLine("<html lang=\"en\">");
        sb.AppendLine($"<head><meta charset=\"utf-8\"><title>{H(vm.Title)}</title><style>");
        sb.Append(css);
        sb.AppendLine("</style></head><body>");

        sb.AppendLine($"<h1>{H(vm.Title)}</h1>");
        sb.Append("<p class=\"stats\">");
        if (vm.TotalSessions > 0)
            sb.Append($"<strong>{vm.TotalSessions}</strong> chat sessions &nbsp;Ã‚Â·&nbsp; ");
        sb.AppendLine($"<strong>{vm.TotalCards}</strong> cards &nbsp;Ã‚Â·&nbsp; <strong>{vm.TotalAnchors}</strong> memAnchors</p>");

        RenderDiagnosticsBanner(sb, vm);

        sb.AppendLine("<div id=\"atlas-layout\">");
        sb.AppendLine("<nav id=\"atlas-nav\">");
        sb.AppendLine("<div class=\"atlas-nav-title\">Atlas Navigator</div>");

        var timelineSections = vm.Sections.OfType<TimelineSection>().ToList();
        var genericSections = vm.Sections.OfType<GenericContainerSection>().ToList();
        var sidebarGroups = genericSections
            .Where(x => x.IncludeInNav && !string.IsNullOrWhiteSpace(x.NavGroup))
            .GroupBy(x => x.NavGroup)
            .Select(g => (Group: g.Key, Sections: g.ToList()))
            .ToList();

        bool anySidebar = timelineSections.Count > 0 || vm.AtlasTopMap.Count > 0 || vm.AtlasThreads.Count > 0 || vm.AtlasPinned.Count > 0 || sidebarGroups.Count > 0;

        if (!anySidebar)
        {
            sb.AppendLine("<div class=\"atlas-empty\">No navigation data</div>");
        }
        else
        {
            if (timelineSections.Count > 0)
            {
                sb.AppendLine("<div class=\"atlas-nav-section\">Timeline</div>");
                sb.AppendLine("<ul class=\"atlas-nav-list\">");
                foreach (var tl in timelineSections)
                {
                    foreach (var grp in tl.Groups)
                    {
                        var count = grp.Months.Sum(m => m.Days.Sum(d => d.Entries.Sum(e => e is SingleSessionEntry ? 1 : ((SegmentGroupEntry)e).Group.Sessions.Count)));
                        sb.AppendLine($"<li><a href=\"#timeline-{grp.Year}\">{grp.Year} <span class=\"tl-badge\">{count}</span></a></li>");
                    }
                }
                sb.AppendLine("</ul>");
            }

            if (vm.AtlasTopMap.Count > 0)
            {
                sb.AppendLine("<div class=\"atlas-nav-section\">Top Map</div>");
                sb.AppendLine("<ul class=\"atlas-nav-list\">");
                foreach (var (label, pageId, _) in vm.AtlasTopMap)
                    sb.AppendLine($"<li><a href=\"#{pageId.Replace(":", "-")}\" title=\"{H(label)}\">{H(label)}</a></li>");
                sb.AppendLine("</ul>");
            }

            if (vm.AtlasPinned.Count > 0)
            {
                sb.AppendLine("<div class=\"atlas-nav-section\">Pinned</div>");
                sb.AppendLine("<ul class=\"atlas-nav-list\">");
                foreach (var (label, pageId, _) in vm.AtlasPinned)
                    sb.AppendLine($"<li><a href=\"#{pageId.Replace(":", "-")}\" title=\"{H(label)}\">{H(label)}</a></li>");
                sb.AppendLine("</ul>");
            }

            foreach (var sidebarGroup in sidebarGroups)
            {
                sb.AppendLine($"<div class=\"atlas-nav-section\">{H(sidebarGroup.Group)}</div>");
                sb.AppendLine("<ul class=\"atlas-nav-list\">");
                foreach (var section in sidebarGroup.Sections)
                {
                    var navLabel = section.NavLabel ?? section.Heading;
                    var navTitle = section.Path ?? navLabel;
                    sb.AppendLine($"<li><a href=\"#{H(section.AnchorId)}\" title=\"{H(navTitle)}\">{H(navLabel)}</a></li>");
                }
                sb.AppendLine("</ul>");
            }
        }
        sb.AppendLine("</nav>");

        sb.AppendLine("<div id=\"atlas-main\">");

        bool memoryCardsHeadingShown = false;
        bool taskGraphHeadingShown = false;
        bool structuresHeadingShown = false;
        foreach (var section in vm.Sections)
        {
            switch (section)
            {
                case DreamNodesSection dns:
                    RenderDreamNodes(sb, dns);
                    break;
                case TimelineSection tl:
                    RenderTimeline(sb, tl);
                    break;
                case DiagnosticsSection ds:
                    RenderDiagnosticsSection(sb, ds);
                    break;
                case GenericContainerSection gcs:
                    var isStructure = string.Equals(gcs.Family, "structure", StringComparison.Ordinal);
                    var isTaskGraph = string.Equals(gcs.Family, "task_graph", StringComparison.Ordinal);
                    var isMemoryCard = string.Equals(gcs.Family, "memory_cards", StringComparison.Ordinal);
                    if (isTaskGraph && !taskGraphHeadingShown)
                    {
                        sb.AppendLine("<h2>Task Graph</h2>");
                        taskGraphHeadingShown = true;
                    }
                    if (isMemoryCard && !memoryCardsHeadingShown)
                    {
                        if (vm.TotalSessions > 0)
                            sb.AppendLine("<h2>Memory Cards</h2>");
                        memoryCardsHeadingShown = true;
                    }
                    if (isStructure && !structuresHeadingShown)
                    {
                        sb.AppendLine("<h2>Structures</h2>");
                        structuresHeadingShown = true;
                    }
                    RenderGenericContainer(sb, gcs);
                    break;
            }
        }

        sb.AppendLine("</div>");
        sb.AppendLine("</div>");

        sb.AppendLine("<script>");
        sb.Append(js);
        sb.AppendLine("</script>");
        sb.AppendLine("</body></html>");
        return sb.ToString();
    }

    private static void RenderDiagnosticsBanner(StringBuilder sb, RootViewModel vm)
    {
        var diagnostics = vm.Sections.OfType<DiagnosticsSection>().FirstOrDefault();
        if (diagnostics is null || !diagnostics.IndexDriftDetected)
            return;

        sb.AppendLine($"<div class=\"atlas-synopsis\"><strong>Index drift detected:</strong> chat_session={diagnostics.ChatSessionContainers}, chat-sessions index={diagnostics.ChatSessionsIndexEntries}</div>");
    }

    private static void RenderDiagnosticsSection(StringBuilder sb, DiagnosticsSection diagnostics)
    {
        sb.AppendLine($"<details class=\"anchor\" id=\"{BuildAnchorId(diagnostics.Heading)}\">");
        sb.AppendLine($"<summary class=\"anchor-hd\">{H(diagnostics.Heading)}</summary>");
        sb.AppendLine("<ul class=\"card-list\">");
        sb.AppendLine($"<li class=\"card-plain\">indexDriftDetected: {diagnostics.IndexDriftDetected.ToString().ToLowerInvariant()}</li>");
        sb.AppendLine($"<li class=\"card-plain\">chatSessionContainers: {diagnostics.ChatSessionContainers}</li>");
        sb.AppendLine($"<li class=\"card-plain\">chatSessionsIndexEntries: {diagnostics.ChatSessionsIndexEntries}</li>");
        sb.AppendLine("</ul></details>");
    }

    private static string BuildAnchorId(string heading)
    {
        var lowered = heading.ToLowerInvariant();
        var slug = System.Text.RegularExpressions.Regex.Replace(lowered, "[^a-z0-9]+", "-");
        return slug.Trim('-');
    }

    private static void RenderTimeline(StringBuilder sb, TimelineSection tl)
    {
        if (tl.Groups.Count == 0)
            return;

        sb.AppendLine($"<h2>{H(tl.Heading)}</h2>");
        sb.AppendLine("<div class=\"tl-source-filters\" role=\"group\" aria-label=\"Timeline source filter\">");
        sb.AppendLine("<button type=\"button\" class=\"tl-src-btn is-active\" data-filter=\"all\">All</button>");
        sb.AppendLine("<button type=\"button\" class=\"tl-src-btn\" data-filter=\"claude\">Claude</button>");
        sb.AppendLine("<button type=\"button\" class=\"tl-src-btn\" data-filter=\"codex\">Codex</button>");
        sb.AppendLine("<button type=\"button\" class=\"tl-src-btn\" data-filter=\"other\">Other</button>");
        sb.AppendLine("</div>");
        foreach (var grp in tl.Groups)
        {
            int yearCount = grp.Months.Sum(m => m.Days.Sum(d => d.Entries.Sum(e => e is SingleSessionEntry ? 1 : ((SegmentGroupEntry)e).Group.Sessions.Count)));
            sb.AppendLine($"<details class=\"tl-year\" id=\"timeline-{grp.Year}\">");
            sb.AppendLine($"<summary class=\"tl-year-hd\">{grp.Year} <span class=\"tl-badge\">{yearCount}</span></summary>");
            foreach (var month in grp.Months)
            {
                var monthName = new DateTime(grp.Year, month.Month, 1).ToString("MMMM");
                int monthCount = month.Days.Sum(d => d.Entries.Sum(e => e is SingleSessionEntry ? 1 : ((SegmentGroupEntry)e).Group.Sessions.Count));
                string monthId = $"timeline-{grp.Year}-{month.Month:D2}";
                sb.AppendLine($"<details class=\"tl-month\" id=\"{monthId}\">");
                sb.AppendLine($"<summary class=\"tl-month-hd\">{monthName} <span class=\"tl-badge\">{monthCount}</span></summary>");
                foreach (var day in month.Days)
                {
                    var dayLabel = new DateTime(grp.Year, month.Month, day.Day).ToString("ddd MMM d");
                    string dayId = $"timeline-{grp.Year}-{month.Month:D2}-{day.Day:D2}";
                    var daySummary = string.IsNullOrWhiteSpace(day.Summary)
                        ? dayLabel
                        : $"{dayLabel} - {day.Summary}";
                    int dayCount = day.Entries.Sum(e => e is SingleSessionEntry ? 1 : ((SegmentGroupEntry)e).Group.Sessions.Count);
                    sb.AppendLine($"<details class=\"tl-day\" id=\"{dayId}\">");
                    sb.AppendLine($"<summary class=\"tl-day-hd\">{H(daySummary)} <span class=\"tl-badge\">{dayCount}</span></summary>");
                    sb.AppendLine("<div class=\"tl-sessions\">");
                    foreach (var entry in day.Entries)
                    {
                        if (entry is SingleSessionEntry single)
                            RenderSession(sb, single.Session);
                        else if (entry is SegmentGroupEntry grouped)
                            RenderSegmentGroup(sb, grouped.Group);
                    }
                    sb.AppendLine("</div></details>");
                }
                sb.AppendLine("</details>");
            }
            sb.AppendLine("</details>");
        }
    }

    private static string FormatTokenCount(int n)
    {
        if (n >= 1_000_000) return $"{n / 1_000_000.0:F1}M tkn";
        if (n >= 1_000) return $"{n / 1_000}k tkn";
        return $"{n} tkn";
    }

    private static void RenderSession(StringBuilder sb, SessionItemViewModel session)
    {
        var source = NormalizeSource(session.Source);
        var totalTokens = session.TokensIn + session.TokensOut;
        var tokenBadge = totalTokens > 0
            ? $" <span class=\"badge tokens\" title=\"in:{session.TokensIn} out:{session.TokensOut} cache_r:{session.TokensCacheRead} cache_w:{session.TokensCacheCreate}\">{FormatTokenCount(totalTokens)}</span>"
            : string.Empty;
        var sessionsBadge = session.SessionCount > 1
            ? $" <span class=\"badge sessions\" title=\"{session.SessionCount} grouped sessions\">{session.SessionCount} sessions</span>"
            : string.Empty;
        var sourceBadge = $" <span class=\"badge source\" title=\"source\">{H(source)}</span>";
        var titleQualityBadge = session.TitleQuality != "high"
            ? $" <span class=\"badge\" title=\"title quality\">{H(session.TitleQuality)}</span>"
            : string.Empty;

        sb.AppendLine($"<details class=\"session\" id=\"{session.AnchorId}\" data-group-key=\"{H(session.GroupKey)}\" data-title-quality=\"{H(session.TitleQuality)}\" data-source=\"{H(source)}\">");
        sb.AppendLine($"<summary class=\"session-hd\">{H(session.Title)}{H(session.DateLabel)}{sourceBadge} <span class=\"badge chat\">{session.Messages.Count} msgs</span>{sessionsBadge}{tokenBadge}{titleQualityBadge}</summary>");

        if (!string.IsNullOrWhiteSpace(session.Synopsis))
            sb.AppendLine($"<div class=\"atlas-synopsis\">{H(session.Synopsis)}</div>");

        if (session.Participants.Count > 0 || !string.IsNullOrEmpty(session.Channel))
        {
            sb.Append("<div class=\"session-meta\">");
            if (!string.IsNullOrEmpty(session.Channel))
                sb.Append($"channel: <strong>{H(session.Channel)}</strong>");
            if (session.Participants.Count > 0)
                sb.Append($"&nbsp; participants: {H(string.Join(", ", session.Participants))}");
            sb.AppendLine("</div>");
        }

        sb.AppendLine("<div class=\"chat-window\">");
        foreach (var msg in session.Messages)
            RenderMessage(sb, msg);
        sb.AppendLine("</div></details>");
    }

    private static void RenderSegmentGroup(StringBuilder sb, SegmentGroupViewModel group)
    {
        sb.AppendLine("<details class=\"tl-segment\">");
        sb.AppendLine($"<summary class=\"tl-segment-hd\">{group.Count} sessions: {H(group.Duration)} <span class=\"tl-time-range\">{H(group.TimeRange)}</span></summary>");
        foreach (var session in group.Sessions)
            RenderSession(sb, session);
        sb.AppendLine("</details>");
    }

    private static void RenderMessage(StringBuilder sb, MessageViewModel msg)
    {
        var bubbleClass = msg.Direction switch
        {
            "user" or "in" => "user",
            "assistant" or "out" => "assistant",
            _ => "unknown"
        };

        sb.AppendLine($"<div class=\"bubble {bubbleClass}\">");
        var tsLabel = string.IsNullOrEmpty(msg.Timestamp) ? string.Empty : " Ã‚Â· " + H(msg.Timestamp);
        sb.AppendLine($"<div class=\"bub-meta\">{H(msg.Author)}{tsLabel}</div>");
        sb.AppendLine($"<div class=\"bub-text\">{H(msg.Text)}</div>");
        sb.AppendLine("</div>");
    }

    private static void RenderDreamNodes(StringBuilder sb, DreamNodesSection dns)
    {
        sb.AppendLine($"<h2>{H(dns.Heading)}</h2>");
        foreach (var grp in dns.Kinds)
        {
            sb.AppendLine($"<div class=\"dream-kind-label\">{H(DreamKindLabel(grp.Kind))}</div>");
            foreach (var node in grp.Nodes)
                RenderDreamNode(sb, node);
        }
    }

    private static void RenderDreamNode(StringBuilder sb, DreamNodeViewModel node)
    {
        var qualityBadge = node.TitleQuality != "high"
            ? $" <span class=\"badge\">{H(node.TitleQuality)}</span>"
            : string.Empty;

        sb.AppendLine($"<details class=\"session dream-node\" id=\"{node.AnchorId}\" data-group-key=\"{H(node.GroupKey)}\" data-title-quality=\"{H(node.TitleQuality)}\">");
        sb.AppendLine($"<summary class=\"session-hd\"><span class=\"badge dream-kind\">{H(node.Id.Split(':')[0])}</span> {H(node.Label)}{qualityBadge} <span class=\"badge dream-score\">score {node.Score:F2}</span></summary>");
        sb.AppendLine("<div class=\"chat-window dream-members\">");

        if (node.MemberLinks.Count > 0)
        {
            sb.AppendLine("<strong>Related conversations:</strong><ul>");
            foreach (var (convTitle, convAnchor) in node.MemberLinks)
                sb.AppendLine($"<li><a href=\"#{convAnchor}\" data-highlight=\"{H(node.Label)}\">{H(convTitle)}</a></li>");
            sb.AppendLine("</ul>");
        }
        else
        {
            sb.AppendLine("<em>No linked conversations.</em>");
        }

        sb.AppendLine("</div></details>");
    }

    private static void RenderGenericContainer(StringBuilder sb, GenericContainerSection gcs)
    {
        sb.AppendLine($"<details class=\"anchor\" id=\"{gcs.AnchorId}\">");
        sb.AppendLine($"<summary class=\"anchor-hd\" title=\"{H(gcs.Path ?? gcs.Heading)}\">{H(gcs.Heading)} <span class=\"badge\">{gcs.Items.Count}</span></summary>");
        if (!string.IsNullOrWhiteSpace(gcs.Synopsis))
            sb.AppendLine($"<div class=\"atlas-synopsis\">{H(gcs.Synopsis)}</div>");
        sb.AppendLine("<ul class=\"card-list\">");

        foreach (var item in gcs.Items)
        {
            if (item.Kind == "container" && TryGetAnchorLink(item.Summary, out var targetAnchor))
            {
                sb.AppendLine("<li class=\"card-plain\">");
                sb.AppendLine($"<a href=\"#{H(targetAnchor)}\">{H(item.Label)}</a>");
                if (!string.IsNullOrWhiteSpace(item.BodyText))
                    sb.AppendLine($"<div>{H(item.BodyText)}</div>");
                sb.AppendLine("</li>");
            }
            else if (!string.IsNullOrEmpty(item.BodyText))
            {
                sb.AppendLine("<li><details class=\"card\">");
                sb.AppendLine($"<summary class=\"card-hd\">{H(item.Label)}</summary>");
                sb.AppendLine($"<div class=\"card-body\">{H(item.BodyText)}</div>");
                sb.AppendLine("</details></li>");
            }
            else
            {
                sb.AppendLine($"<li class=\"card-plain\">{H(item.Label)}</li>");
            }
        }

        sb.AppendLine("</ul></details>");
    }

    private static string DreamKindLabel(string kind) => kind switch
    {
        "topic" => "Topics",
        "thread" => "Threads",
        "decision" => "Decisions",
        "invariant" => "Invariants",
        _ => kind
    };

    private static string NormalizeSource(string? source)
    {
        if (string.IsNullOrWhiteSpace(source))
            return "other";
        return source.Trim().ToLowerInvariant();
    }

    private static bool TryGetAnchorLink(string? summary, out string anchor)
    {
        const string prefix = "anchor:";
        if (!string.IsNullOrWhiteSpace(summary) && summary.StartsWith(prefix, StringComparison.Ordinal))
        {
            anchor = summary[prefix.Length..];
            return !string.IsNullOrWhiteSpace(anchor);
        }

        anchor = string.Empty;
        return false;
    }

    private static string H(string? s) => System.Net.WebUtility.HtmlEncode(s ?? string.Empty);

    private static string LoadResource(string name)
    {
        var assembly = typeof(HtmlRenderer).Assembly;
        using var stream = assembly.GetManifestResourceStream(name)
            ?? throw new InvalidOperationException($"Embedded resource not found: {name}");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}



