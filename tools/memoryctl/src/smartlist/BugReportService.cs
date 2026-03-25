using System.Text.Json;
using AMS.Core;

namespace MemoryCtl;

internal sealed record BugReportInfo(
    string BugId,
    string SourceAgent,
    string ParentAgent,
    string ErrorOutput,
    string StackContext,
    IReadOnlyList<string> AttemptedFixes,
    IReadOnlyList<string> ReproductionSteps,
    string RecommendedFixPlan,
    string Severity,
    string Status,
    string Durability,
    string RetrievalVisibility,
    IReadOnlyList<string> BucketPaths,
    DateTimeOffset CreatedAt,
    DateTimeOffset? ResolvedAt);

/// <summary>
/// Stub record for a reusable BugFix recipe.  Cross-referenced with BugReport nodes
/// so FEP can later learn which fixes work for which bug patterns.
/// </summary>
internal sealed record BugFixInfo(
    string FixId,
    string Title,
    string Description,
    string FixRecipe,
    IReadOnlyList<string> LinkedBugReportIds,
    string Status,
    string Durability,
    IReadOnlyList<string> BucketPaths,
    DateTimeOffset CreatedAt);

internal sealed class BugReportService
{
    internal const string BugReportObjectKind = "smartlist_bugreport";
    internal const string BugFixObjectKind = "smartlist_bugfix";
    internal const string DefaultBucketPath = "smartlist/bug-reports";
    internal const string BugFixBucketPath = "smartlist/bug-fixes";

    internal const string StatusOpen = "open";
    internal const string StatusInRepair = "in-repair";
    internal const string StatusResolved = "resolved";

    internal const string SeverityCritical = "critical";
    internal const string SeverityHigh = "high";
    internal const string SeverityMedium = "medium";
    internal const string SeverityLow = "low";

    private static readonly HashSet<string> ValidStatuses = new(StringComparer.OrdinalIgnoreCase)
        { StatusOpen, StatusInRepair, StatusResolved };

    private static readonly HashSet<string> ValidSeverities = new(StringComparer.OrdinalIgnoreCase)
        { SeverityCritical, SeverityHigh, SeverityMedium, SeverityLow };

    private readonly AmsStore _store;
    private readonly SmartListService _smartList;
    private readonly RetrievalGraphProjector _retrievalGraph;

    public BugReportService(AmsStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _smartList = new SmartListService(store);
        _retrievalGraph = new RetrievalGraphProjector(store);
    }

    public BugReportInfo CreateBugReport(
        string sourceAgent,
        string parentAgent,
        string errorOutput,
        string stackContext,
        IReadOnlyList<string>? attemptedFixes,
        IReadOnlyList<string>? reproductionSteps,
        string? recommendedFixPlan,
        string severity,
        bool durable,
        string createdBy,
        DateTimeOffset nowUtc)
    {
        var normalizedSeverity = NormalizeSeverity(severity);
        attemptedFixes ??= Array.Empty<string>();
        reproductionSteps ??= Array.Empty<string>();
        recommendedFixPlan ??= string.Empty;

        // Ensure the global registry bucket exists
        _smartList.CreateBucket(DefaultBucketPath, durable, createdBy, nowUtc);

        var bugId = $"smartlist-bugreport:{Guid.NewGuid():N}";
        _store.UpsertObject(bugId, BugReportObjectKind);
        var obj = _store.Objects[bugId];
        obj.SemanticPayload ??= new SemanticPayload();
        obj.SemanticPayload.Summary = $"Bug: {TruncateForSummary(errorOutput)}";
        var durability = durable ? "durable" : "short_term";
        obj.SemanticPayload.Tags = ["smartlist_bugreport", durability, normalizedSeverity, StatusOpen];

        var prov = EnsureProv(obj);
        prov["bug_id"] = JsonSerializer.SerializeToElement(bugId);
        prov["source_agent"] = JsonSerializer.SerializeToElement(sourceAgent.Trim());
        prov["parent_agent"] = JsonSerializer.SerializeToElement(parentAgent.Trim());
        prov["error_output"] = JsonSerializer.SerializeToElement(errorOutput.Trim());
        prov["stack_context"] = JsonSerializer.SerializeToElement(stackContext.Trim());
        prov["attempted_fixes"] = JsonSerializer.SerializeToElement(attemptedFixes);
        prov["reproduction_steps"] = JsonSerializer.SerializeToElement(reproductionSteps);
        prov["recommended_fix_plan"] = JsonSerializer.SerializeToElement(recommendedFixPlan.Trim());
        prov["severity"] = JsonSerializer.SerializeToElement(normalizedSeverity);
        prov["status"] = JsonSerializer.SerializeToElement(StatusOpen);
        prov["durability"] = JsonSerializer.SerializeToElement(durability);
        prov[SmartListService.RetrievalVisibilityKey] = JsonSerializer.SerializeToElement(SmartListService.RetrievalVisibilityDefault);
        prov["created_by"] = JsonSerializer.SerializeToElement(createdBy);
        prov["created_at"] = JsonSerializer.SerializeToElement(nowUtc);
        prov["resolved_at"] = JsonSerializer.SerializeToElement((string?)null);

        // Attach to the global bug-reports bucket
        _smartList.Attach(DefaultBucketPath, bugId, createdBy, nowUtc);

        _retrievalGraph.ProjectSmartListNote(bugId);
        return GetBugReport(bugId)!;
    }

    public BugReportInfo UpdateStatus(string bugId, string newStatus, DateTimeOffset nowUtc)
    {
        var normalizedStatus = NormalizeStatus(newStatus);
        if (!_store.Objects.TryGetValue(bugId, out var obj) || obj.ObjectKind != BugReportObjectKind)
            throw new InvalidOperationException($"Unknown bug report '{bugId}'.");

        var prov = EnsureProv(obj);
        prov["status"] = JsonSerializer.SerializeToElement(normalizedStatus);

        if (string.Equals(normalizedStatus, StatusResolved, StringComparison.Ordinal))
            prov["resolved_at"] = JsonSerializer.SerializeToElement(nowUtc);

        obj.SemanticPayload!.Tags = (obj.SemanticPayload.Tags ?? [])
            .Where(t => !ValidStatuses.Contains(t))
            .Append(normalizedStatus)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        obj.UpdatedAt = nowUtc;
        _retrievalGraph.ProjectSmartListNote(bugId);
        return GetBugReport(bugId)!;
    }

    public BugReportInfo? GetBugReport(string bugId)
    {
        if (!_store.Objects.TryGetValue(bugId, out var obj) || obj.ObjectKind != BugReportObjectKind)
            return null;

        var prov = obj.SemanticPayload?.Provenance;
        return new BugReportInfo(
            bugId,
            ReadString(prov, "source_agent") ?? string.Empty,
            ReadString(prov, "parent_agent") ?? string.Empty,
            ReadString(prov, "error_output") ?? string.Empty,
            ReadString(prov, "stack_context") ?? string.Empty,
            ReadStringList(prov, "attempted_fixes"),
            ReadStringList(prov, "reproduction_steps"),
            ReadString(prov, "recommended_fix_plan") ?? string.Empty,
            ReadString(prov, "severity") ?? SeverityMedium,
            ReadString(prov, "status") ?? StatusOpen,
            ReadString(prov, "durability") ?? "short_term",
            SmartListService.ReadRetrievalVisibility(prov),
            BucketPathsForBugReport(bugId),
            ReadDate(prov, "created_at") ?? obj.CreatedAt,
            ReadDate(prov, "resolved_at"));
    }

    public IReadOnlyList<BugReportInfo> ListBugReports(string? statusFilter)
    {
        var results = new List<BugReportInfo>();
        var bucketPath = DefaultBucketPath;
        try
        {
            var memberIds = _smartList.GetBucketMemberObjectIds(bucketPath);
            foreach (var memberId in memberIds)
            {
                if (!_store.Objects.TryGetValue(memberId, out var obj) || obj.ObjectKind != BugReportObjectKind)
                    continue;

                var report = GetBugReport(memberId);
                if (report is null)
                    continue;

                if (statusFilter is not null && !string.Equals(report.Status, statusFilter, StringComparison.OrdinalIgnoreCase))
                    continue;

                results.Add(report);
            }
        }
        catch (InvalidOperationException)
        {
            // Bucket doesn't exist yet — no bug reports
        }

        return results;
    }

    public IReadOnlyList<BugReportInfo> SearchBugReports(string query, string? statusFilter)
    {
        var tokens = query.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(t => t.ToLowerInvariant())
            .Where(t => t.Length > 1)
            .ToList();

        if (tokens.Count == 0)
            return ListBugReports(statusFilter);

        var all = ListBugReports(statusFilter);
        var scored = new List<(BugReportInfo Report, int Score)>();

        foreach (var report in all)
        {
            var searchable = string.Join(' ',
                report.ErrorOutput,
                report.StackContext,
                report.RecommendedFixPlan,
                string.Join(' ', report.AttemptedFixes),
                string.Join(' ', report.ReproductionSteps),
                report.SourceAgent,
                report.ParentAgent).ToLowerInvariant();

            var score = tokens.Count(t => searchable.Contains(t, StringComparison.Ordinal));
            if (score > 0)
                scored.Add((report, score));
        }

        return scored.OrderByDescending(x => x.Score).Select(x => x.Report).ToList();
    }

    private IReadOnlyList<string> BucketPathsForBugReport(string bugId)
    {
        return _store.ContainersForMemberObject(bugId)
            .Where(id => id.StartsWith("smartlist-members:", StringComparison.Ordinal))
            .Select(id => id["smartlist-members:".Length..])
            .Distinct(StringComparer.Ordinal)
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();
    }

    private static string NormalizeStatus(string status)
    {
        var trimmed = (status ?? string.Empty).Trim().ToLowerInvariant();
        if (!ValidStatuses.Contains(trimmed))
            throw new ArgumentException($"Invalid status '{status}'. Expected: {string.Join(", ", ValidStatuses)}.");
        return trimmed;
    }

    private static string NormalizeSeverity(string severity)
    {
        var trimmed = (severity ?? string.Empty).Trim().ToLowerInvariant();
        if (!ValidSeverities.Contains(trimmed))
            throw new ArgumentException($"Invalid severity '{severity}'. Expected: {string.Join(", ", ValidSeverities)}.");
        return trimmed;
    }

    internal static string TruncateForSummary(string text)
    {
        if (string.IsNullOrWhiteSpace(text)) return "(empty)";
        var firstLine = text.Split('\n', 2)[0].Trim();
        return firstLine.Length > 120 ? firstLine[..117] + "..." : firstLine;
    }

    private static Dictionary<string, JsonElement> EnsureProv(ObjectRecord obj)
    {
        obj.SemanticPayload ??= new SemanticPayload();
        obj.SemanticPayload.Provenance ??= new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        return obj.SemanticPayload.Provenance;
    }

    private static string? ReadString(IReadOnlyDictionary<string, JsonElement>? p, string key)
        => p is not null && p.TryGetValue(key, out var el) ? (el.ValueKind == JsonValueKind.String ? el.GetString() : el.ToString()) : null;

    private static DateTimeOffset? ReadDate(IReadOnlyDictionary<string, JsonElement>? p, string key)
        => p is not null && p.TryGetValue(key, out var el) && (el.TryGetDateTimeOffset(out var x) || (el.ValueKind == JsonValueKind.String && DateTimeOffset.TryParse(el.GetString(), out x))) ? x : null;

    private static IReadOnlyList<string> ReadStringList(IReadOnlyDictionary<string, JsonElement>? p, string key)
    {
        if (p is null || !p.TryGetValue(key, out var el) || el.ValueKind != JsonValueKind.Array)
            return Array.Empty<string>();

        return el.EnumerateArray()
            .Where(x => x.ValueKind == JsonValueKind.String)
            .Select(x => x.GetString() ?? string.Empty)
            .ToList();
    }

    // ── BugFix stubs ───────────────────────────────────────────────────────
    // These define the schema, cross-reference linkage, and assignment interface.
    // Actual FEP learning (success rates, pattern matching) is deferred to a future plan.

    /// <summary>
    /// Create a BugFix recipe on the global registry and optionally link it to a BugReport.
    /// </summary>
    public BugFixInfo CreateBugFix(
        string title,
        string description,
        string fixRecipe,
        string? linkedBugReportId,
        bool durable,
        string createdBy,
        DateTimeOffset nowUtc)
    {
        _smartList.CreateBucket(BugFixBucketPath, durable, createdBy, nowUtc);

        var fixId = $"smartlist-bugfix:{Guid.NewGuid():N}";
        _store.UpsertObject(fixId, BugFixObjectKind);
        var obj = _store.Objects[fixId];
        obj.SemanticPayload ??= new SemanticPayload();
        obj.SemanticPayload.Summary = $"Fix: {TruncateForSummary(title)}";
        var durability = durable ? "durable" : "short_term";
        obj.SemanticPayload.Tags = ["smartlist_bugfix", durability];

        var prov = EnsureProv(obj);
        prov["fix_id"] = JsonSerializer.SerializeToElement(fixId);
        prov["title"] = JsonSerializer.SerializeToElement(title.Trim());
        prov["description"] = JsonSerializer.SerializeToElement(description.Trim());
        prov["fix_recipe"] = JsonSerializer.SerializeToElement(fixRecipe.Trim());
        prov["status"] = JsonSerializer.SerializeToElement("available");
        prov["durability"] = JsonSerializer.SerializeToElement(durability);
        prov["created_by"] = JsonSerializer.SerializeToElement(createdBy);
        prov["created_at"] = JsonSerializer.SerializeToElement(nowUtc);

        var linkedIds = new List<string>();
        if (!string.IsNullOrWhiteSpace(linkedBugReportId))
        {
            linkedIds.Add(linkedBugReportId);
            LinkBugReportToFix(linkedBugReportId, fixId, createdBy, nowUtc);
        }
        prov["linked_bugreport_ids"] = JsonSerializer.SerializeToElement(linkedIds);

        _smartList.Attach(BugFixBucketPath, fixId, createdBy, nowUtc);
        _retrievalGraph.ProjectSmartListNote(fixId);

        return GetBugFix(fixId)!;
    }

    /// <summary>
    /// Cross-reference a BugReport with a BugFix by storing the link on both sides.
    /// </summary>
    public void LinkBugReportToFix(string bugReportId, string bugFixId, string createdBy, DateTimeOffset nowUtc)
    {
        // Store link on the BugFix side
        if (_store.Objects.TryGetValue(bugFixId, out var fixObj) && fixObj.ObjectKind == BugFixObjectKind)
        {
            var prov = EnsureProv(fixObj);
            var existing = ReadStringList(prov, "linked_bugreport_ids").ToList();
            if (!existing.Contains(bugReportId, StringComparer.Ordinal))
            {
                existing.Add(bugReportId);
                prov["linked_bugreport_ids"] = JsonSerializer.SerializeToElement(existing);
                fixObj.UpdatedAt = nowUtc;
            }
        }

        // Store link on the BugReport side
        if (_store.Objects.TryGetValue(bugReportId, out var reportObj) && reportObj.ObjectKind == BugReportObjectKind)
        {
            var prov = EnsureProv(reportObj);
            var existing = ReadStringList(prov, "linked_bugfix_ids").ToList();
            if (!existing.Contains(bugFixId, StringComparer.Ordinal))
            {
                existing.Add(bugFixId);
                prov["linked_bugfix_ids"] = JsonSerializer.SerializeToElement(existing);
                reportObj.UpdatedAt = nowUtc;
            }
        }
    }

    /// <summary>
    /// Retrieve a BugFix by its ID.
    /// </summary>
    public BugFixInfo? GetBugFix(string fixId)
    {
        if (!_store.Objects.TryGetValue(fixId, out var obj) || obj.ObjectKind != BugFixObjectKind)
            return null;

        var prov = obj.SemanticPayload?.Provenance;
        return new BugFixInfo(
            fixId,
            ReadString(prov, "title") ?? string.Empty,
            ReadString(prov, "description") ?? string.Empty,
            ReadString(prov, "fix_recipe") ?? string.Empty,
            ReadStringList(prov, "linked_bugreport_ids"),
            ReadString(prov, "status") ?? "available",
            ReadString(prov, "durability") ?? "short_term",
            BucketPathsForBugFix(fixId),
            ReadDate(prov, "created_at") ?? obj.CreatedAt);
    }

    /// <summary>
    /// List all BugFix nodes in the global registry.
    /// </summary>
    public IReadOnlyList<BugFixInfo> ListBugFixes()
    {
        var results = new List<BugFixInfo>();
        try
        {
            foreach (var memberId in _smartList.GetBucketMemberObjectIds(BugFixBucketPath))
            {
                var fix = GetBugFix(memberId);
                if (fix is not null)
                    results.Add(fix);
            }
        }
        catch (InvalidOperationException)
        {
            // Bucket doesn't exist yet
        }
        return results;
    }

    /// <summary>
    /// Get linked BugFix IDs for a given BugReport, enabling repair agent assignment.
    /// </summary>
    public IReadOnlyList<string> GetLinkedFixesForBugReport(string bugReportId)
    {
        if (!_store.Objects.TryGetValue(bugReportId, out var obj) || obj.ObjectKind != BugReportObjectKind)
            return Array.Empty<string>();
        return ReadStringList(obj.SemanticPayload?.Provenance, "linked_bugfix_ids");
    }

    private IReadOnlyList<string> BucketPathsForBugFix(string fixId)
    {
        return _store.ContainersForMemberObject(fixId)
            .Where(id => id.StartsWith("smartlist-members:", StringComparison.Ordinal))
            .Select(id => id["smartlist-members:".Length..])
            .Distinct(StringComparer.Ordinal)
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();
    }
}
