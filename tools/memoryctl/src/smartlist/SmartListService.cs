using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using AMS.Core;

namespace MemoryCtl;

internal sealed record SmartListBucketInfo(
    string Path,
    string ObjectId,
    string DisplayName,
    string? ParentPath,
    string Durability,
    string RetrievalVisibility,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);

internal sealed record SmartListNoteInfo(
    string NoteId,
    string Title,
    string Text,
    string Durability,
    string RetrievalVisibility,
    IReadOnlyList<string> BucketPaths,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);

internal sealed record SmartListRollupChild(
    string Path,
    string Summary);

internal sealed record SmartListRollupInfo(
    string RollupId,
    string BucketPath,
    string Title,
    string Summary,
    string Scope,
    string? StopHint,
    string Durability,
    string RetrievalVisibility,
    string SourceMode,
    IReadOnlyList<SmartListRollupChild> ChildHighlights,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);

internal sealed record SmartListInspectEntry(
    int Depth,
    string MemberKind,
    string PathOrId,
    string Title,
    string Durability,
    string RetrievalVisibility);

internal sealed record SmartListInspectResult(
    string Path,
    string Title,
    string Durability,
    string RetrievalVisibility,
    IReadOnlyList<SmartListInspectEntry> Entries);

internal sealed record SmartListRememberResult(
    int BucketsPromoted,
    int NotesPromoted,
    IReadOnlyList<string> PromotedObjectIds);

internal sealed record SmartListVisibilityResult(
    string Path,
    string RetrievalVisibility,
    int BucketsUpdated,
    int NotesUpdated,
    int RollupsUpdated);

internal sealed class SmartListService
{
    internal const string ShortTermRootContainer = "agent-memory:short-term:smartlists";
    internal const string DurableRootContainer = "agent-memory:smartlists";
    internal const string BucketObjectKind = "smartlist_bucket";
    internal const string NoteObjectKind = "smartlist_note";
    internal const string RollupObjectKind = "smartlist_rollup";
    internal const string RetrievalVisibilityKey = "retrieval_visibility";
    internal const string RetrievalVisibilityDefault = "default";
    internal const string RetrievalVisibilityScoped = "scoped";
    internal const string RetrievalVisibilitySuppressed = "suppressed";

    private const string ShortTermDurability = "short_term";
    private const string DurableDurability = "durable";
    private static readonly Regex PathPartRx = new("[^a-z0-9-]+", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private readonly AmsStore _store;
    private readonly RetrievalGraphProjector _retrievalGraph;

    public SmartListService(AmsStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _retrievalGraph = new RetrievalGraphProjector(store);
    }

    public SmartListBucketInfo CreateBucket(string path, bool durable, string createdBy, DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        var canonical = NormalizePath(path);
        if (string.Equals(canonical, "smartlist", StringComparison.Ordinal))
            throw new ArgumentException("smartlist root is reserved; create a bucket under smartlist/<name>.", nameof(path));

        return EnsureBucketPath(canonical, durable ? DurableDurability : ShortTermDurability, createdBy, nowUtc);
    }

    public SmartListNoteInfo CreateNote(string title, string text, IReadOnlyList<string> bucketPaths, bool durable, string createdBy, DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        if (string.IsNullOrWhiteSpace(title))
            throw new ArgumentException("title is required", nameof(title));
        if (string.IsNullOrWhiteSpace(text))
            throw new ArgumentException("text is required", nameof(text));

        var noteId = $"smartlist-note:{Guid.NewGuid():N}";
        var durability = durable ? DurableDurability : ShortTermDurability;
        _store.UpsertObject(noteId, NoteObjectKind);
        var note = _store.Objects[noteId];
        note.SemanticPayload ??= new SemanticPayload();
        note.SemanticPayload.Summary = title.Trim();
        note.SemanticPayload.Tags = ["smartlist_note", durability];

        var prov = EnsureProv(note);
        prov["title"] = JsonSerializer.SerializeToElement(title.Trim());
        prov["text"] = JsonSerializer.SerializeToElement(text.Trim());
        prov["durability"] = JsonSerializer.SerializeToElement(durability);
        prov[RetrievalVisibilityKey] = JsonSerializer.SerializeToElement(ReadString(prov, RetrievalVisibilityKey) ?? RetrievalVisibilityDefault);
        prov["created_by"] = JsonSerializer.SerializeToElement(createdBy);
        prov["created_at"] = JsonSerializer.SerializeToElement(nowUtc);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);
        prov["source"] = JsonSerializer.SerializeToElement("manual");

        var normalizedPaths = bucketPaths
            .Select(NormalizePath)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        if (normalizedPaths.Count == 0)
        {
            EnsureAttached(RootContainerId(durable), noteId);
        }
        else
        {
            foreach (var bucketPath in normalizedPaths)
            {
                var bucket = EnsureBucketPath(bucketPath, durability, createdBy, nowUtc);
                EnsureAttached(MembersContainerId(bucket.Path), noteId);
            }
        }

        _retrievalGraph.ProjectSmartListNote(noteId);
        return GetNote(noteId)!;
    }

    public SmartListBucketInfo Attach(string path, string memberRef, string createdBy, DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        var bucket = EnsureBucketPath(NormalizePath(path), ShortTermDurability, createdBy, nowUtc);
        var memberObjectId = ResolveMemberObject(memberRef, createdBy, nowUtc);
        EnsureAttached(MembersContainerId(bucket.Path), memberObjectId);
        _retrievalGraph.ProjectSmartListBucket(bucket.Path);
        ProjectSmartListMemberIfSupported(memberObjectId);
        return GetBucket(bucket.Path)!;
    }

    public SmartListInspectResult Inspect(string path, int depth)
    {
        EnsureScaffold();
        depth = Math.Max(0, depth);
        var canonical = NormalizePath(path);
        var entries = new List<SmartListInspectEntry>();

        if (string.Equals(canonical, "smartlist", StringComparison.Ordinal))
        {
            AddRootEntries(entries, false);
            AddRootEntries(entries, true);
            return new SmartListInspectResult("smartlist", "smartlist", "mixed", RetrievalVisibilityDefault, entries);
        }

        var bucket = GetBucket(canonical) ?? throw new InvalidOperationException($"Unknown SmartList bucket '{canonical}'.");
        CollectEntries(bucket.Path, 0, depth, entries, new HashSet<string>(StringComparer.Ordinal));
        return new SmartListInspectResult(bucket.Path, bucket.DisplayName, bucket.Durability, bucket.RetrievalVisibility, entries);
    }

    public SmartListRememberResult Remember(string? path, string? objectId, DateTimeOffset nowUtc)
    {
        EnsureScaffold();

        if (!string.IsNullOrWhiteSpace(path))
        {
            var bucket = GetBucket(NormalizePath(path)) ?? throw new InvalidOperationException($"Unknown SmartList bucket '{path}'.");
            var promoted = new HashSet<string>(StringComparer.Ordinal);
            PromoteBucketTree(bucket.Path, nowUtc, promoted);
            return new SmartListRememberResult(
                promoted.Count(x => _store.Objects.TryGetValue(x, out var obj) && obj.ObjectKind == BucketObjectKind),
                promoted.Count(x => _store.Objects.TryGetValue(x, out var obj) && obj.ObjectKind == NoteObjectKind),
                promoted.OrderBy(x => x, StringComparer.Ordinal).ToList());
        }

        if (string.IsNullOrWhiteSpace(objectId))
            throw new ArgumentException("Either path or objectId is required.");

        if (!_store.Objects.TryGetValue(objectId, out var obj))
            throw new InvalidOperationException($"Unknown object '{objectId}'.");

        if (obj.ObjectKind == BucketObjectKind)
        {
            var bucket = GetBucket(ReadString(obj.SemanticPayload?.Provenance, "path") ?? objectId)
                ?? throw new InvalidOperationException($"Unknown SmartList bucket '{objectId}'.");
            var promoted = new HashSet<string>(StringComparer.Ordinal);
            PromoteBucketTree(bucket.Path, nowUtc, promoted);
            return new SmartListRememberResult(
                promoted.Count(x => _store.Objects.TryGetValue(x, out var promotedObj) && promotedObj.ObjectKind == BucketObjectKind),
                promoted.Count(x => _store.Objects.TryGetValue(x, out var promotedObj) && promotedObj.ObjectKind == NoteObjectKind),
                promoted.OrderBy(x => x, StringComparer.Ordinal).ToList());
        }

        if (obj.ObjectKind != NoteObjectKind)
            throw new InvalidOperationException($"Object '{objectId}' is not a SmartList note or bucket.");

        PromoteNote(objectId, nowUtc);
        return new SmartListRememberResult(0, 1, [objectId]);
    }

    public SmartListRollupInfo SetRollup(
        string path,
        string summary,
        string scope,
        string? stopHint,
        IReadOnlyList<SmartListRollupChild> childHighlights,
        bool durable,
        string createdBy,
        DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        if (string.IsNullOrWhiteSpace(summary))
            throw new ArgumentException("summary is required", nameof(summary));
        if (string.IsNullOrWhiteSpace(scope))
            throw new ArgumentException("scope is required", nameof(scope));

        var bucket = EnsureBucketPath(NormalizePath(path), durable ? DurableDurability : ShortTermDurability, createdBy, nowUtc);
        var rollupId = RollupObjectId(bucket.Path);
        _store.UpsertObject(rollupId, RollupObjectKind);
        var rollup = _store.Objects[rollupId];
        rollup.SemanticPayload ??= new SemanticPayload();
        rollup.SemanticPayload.Summary = bucket.DisplayName;
        rollup.SemanticPayload.Tags = ["smartlist_rollup", bucket.Durability];

        var prov = EnsureProv(rollup);
        prov["bucket_path"] = JsonSerializer.SerializeToElement(bucket.Path);
        prov["title"] = JsonSerializer.SerializeToElement(bucket.DisplayName);
        prov["summary"] = JsonSerializer.SerializeToElement(summary.Trim());
        prov["scope"] = JsonSerializer.SerializeToElement(scope.Trim());
        prov["stop_hint"] = JsonSerializer.SerializeToElement((stopHint ?? string.Empty).Trim());
        prov["durability"] = JsonSerializer.SerializeToElement(bucket.Durability);
        prov[RetrievalVisibilityKey] = JsonSerializer.SerializeToElement(ReadString(prov, RetrievalVisibilityKey) ?? bucket.RetrievalVisibility);
        prov["source_mode"] = JsonSerializer.SerializeToElement("manual");
        prov["created_by"] = JsonSerializer.SerializeToElement(ReadString(prov, "created_by") ?? createdBy);
        prov["created_at"] = JsonSerializer.SerializeToElement(ReadDate(prov, "created_at") ?? nowUtc);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);
        prov["child_highlights"] = JsonSerializer.SerializeToElement(
            childHighlights
                .Select(x => new { path = NormalizePath(x.Path), summary = x.Summary.Trim() })
                .ToArray());

        EnsureAttached(MembersContainerId(bucket.Path), rollupId);
        _retrievalGraph.ProjectSmartListRollup(rollupId);
        _retrievalGraph.ProjectSmartListBucket(bucket.Path);
        return GetRollup(bucket.Path)!;
    }

    public SmartListVisibilityResult SetRetrievalVisibility(
        string path,
        string visibility,
        bool recursive,
        bool includeNotes,
        bool includeRollups,
        DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        var canonical = NormalizePath(path);
        var bucket = GetBucket(canonical) ?? throw new InvalidOperationException($"Unknown SmartList bucket '{path}'.");
        var normalizedVisibility = NormalizeRetrievalVisibility(visibility);

        var bucketCount = 0;
        var noteCount = 0;
        var rollupCount = 0;

        ApplyBucketVisibility(bucket.Path, normalizedVisibility, recursive, includeNotes, includeRollups, nowUtc, ref bucketCount, ref noteCount, ref rollupCount);

        return new SmartListVisibilityResult(bucket.Path, normalizedVisibility, bucketCount, noteCount, rollupCount);
    }

    public SmartListRollupInfo? GetRollup(string path)
    {
        var canonical = NormalizePath(path);
        var rollupId = RollupObjectId(canonical);
        if (!_store.Objects.TryGetValue(rollupId, out var obj) || obj.ObjectKind != RollupObjectKind)
            return null;

        var prov = obj.SemanticPayload?.Provenance;
        var childHighlights = new List<SmartListRollupChild>();
        if (prov is not null
            && prov.TryGetValue("child_highlights", out var childEl)
            && childEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var child in childEl.EnumerateArray())
            {
                if (child.ValueKind != JsonValueKind.Object)
                    continue;

                var childPath = child.TryGetProperty("path", out var pathEl) ? pathEl.ToString() ?? string.Empty : string.Empty;
                var childSummary = child.TryGetProperty("summary", out var summaryEl) ? summaryEl.ToString() ?? string.Empty : string.Empty;
                if (string.IsNullOrWhiteSpace(childPath))
                    continue;
                childHighlights.Add(new SmartListRollupChild(childPath, childSummary));
            }
        }

        return new SmartListRollupInfo(
            rollupId,
            ReadString(prov, "bucket_path") ?? canonical,
            ReadString(prov, "title") ?? obj.SemanticPayload?.Summary ?? LastSegment(canonical),
            ReadString(prov, "summary") ?? string.Empty,
            ReadString(prov, "scope") ?? string.Empty,
            EmptyToNull(ReadString(prov, "stop_hint")),
            ReadString(prov, "durability") ?? ShortTermDurability,
            ReadRetrievalVisibility(prov),
            ReadString(prov, "source_mode") ?? "manual",
            childHighlights,
            ReadDate(prov, "created_at") ?? obj.CreatedAt,
            ReadDate(prov, "updated_at") ?? obj.UpdatedAt);
    }

    public SmartListBucketInfo? GetBucket(string path)
    {
        var canonical = NormalizePath(path);
        if (string.Equals(canonical, "smartlist", StringComparison.Ordinal))
            return null;

        var objectId = BucketObjectId(canonical);
        if (!_store.Objects.TryGetValue(objectId, out var obj) || obj.ObjectKind != BucketObjectKind)
            return null;

        var prov = obj.SemanticPayload?.Provenance;
        return new SmartListBucketInfo(
            ReadString(prov, "path") ?? canonical,
            objectId,
            obj.SemanticPayload?.Summary ?? LastSegment(canonical),
            EmptyToNull(ReadString(prov, "parent_path")),
            ReadString(prov, "durability") ?? ShortTermDurability,
            ReadRetrievalVisibility(prov),
            ReadDate(prov, "created_at") ?? obj.CreatedAt,
            ReadDate(prov, "updated_at") ?? obj.UpdatedAt);
    }

    public SmartListNoteInfo? GetNote(string noteId)
    {
        if (!_store.Objects.TryGetValue(noteId, out var obj) || obj.ObjectKind != NoteObjectKind)
            return null;

        var prov = obj.SemanticPayload?.Provenance;
        return new SmartListNoteInfo(
            noteId,
            ReadString(prov, "title") ?? obj.SemanticPayload?.Summary ?? noteId,
            ReadString(prov, "text") ?? string.Empty,
            ReadString(prov, "durability") ?? ShortTermDurability,
            ReadRetrievalVisibility(prov),
            BucketPathsForNote(noteId),
            ReadDate(prov, "created_at") ?? obj.CreatedAt,
            ReadDate(prov, "updated_at") ?? obj.UpdatedAt);
    }

    public SmartListBucketInfo UpdateBucketFields(string path, IReadOnlyDictionary<string, string?> fields, DateTimeOffset nowUtc)
    {
        EnsureScaffold();
        var bucket = GetBucket(NormalizePath(path)) ?? throw new InvalidOperationException($"Unknown SmartList bucket '{path}'.");
        var obj = _store.Objects[bucket.ObjectId];
        var prov = EnsureProv(obj);

        foreach (var field in fields)
        {
            if (string.IsNullOrWhiteSpace(field.Key))
                continue;

            prov[field.Key.Trim()] = JsonSerializer.SerializeToElement(field.Value ?? string.Empty);
        }

        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);
        obj.UpdatedAt = nowUtc;
        ProjectSmartListMemberIfSupported(bucket.ObjectId);
        return GetBucket(bucket.Path)!;
    }

    public IReadOnlyList<string> GetBucketMemberObjectIds(string path)
    {
        var bucket = GetBucket(NormalizePath(path)) ?? throw new InvalidOperationException($"Unknown SmartList bucket '{path}'.");
        return _store.IterateForward(MembersContainerId(bucket.Path))
            .Select(x => x.ObjectId)
            .ToList();
    }

    private void EnsureScaffold()
    {
        EnsureContainer(ShortTermRootContainer, "smartlist_root");
        EnsureContainer(DurableRootContainer, "smartlist_root");
    }

    private SmartListBucketInfo EnsureBucketPath(string canonicalPath, string durability, string createdBy, DateTimeOffset nowUtc)
    {
        if (!canonicalPath.StartsWith("smartlist/", StringComparison.Ordinal))
            throw new ArgumentException($"SmartList paths must live under the smartlist namespace. Got '{canonicalPath}'.", nameof(canonicalPath));

        var segments = canonicalPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 2)
            throw new ArgumentException($"SmartList paths must include at least one bucket name. Got '{canonicalPath}'.", nameof(canonicalPath));

        string? parentPath = null;
        SmartListBucketInfo? current = null;
        for (var i = 1; i < segments.Length; i++)
        {
            var path = string.Join('/', segments.Take(i + 1));
            current = EnsureBucket(path, parentPath, durability, createdBy, nowUtc);
            parentPath = path;
        }

        return current!;
    }

    private SmartListBucketInfo EnsureBucket(string path, string? parentPath, string durability, string createdBy, DateTimeOffset nowUtc)
    {
        var objectId = BucketObjectId(path);
        var created = !_store.Objects.ContainsKey(objectId);
        _store.UpsertObject(objectId, BucketObjectKind);
        var bucket = _store.Objects[objectId];
        bucket.SemanticPayload ??= new SemanticPayload();
        bucket.SemanticPayload.Summary = LastSegment(path);
        bucket.SemanticPayload.Tags = ["smartlist_bucket", durability];

        var prov = EnsureProv(bucket);
        prov["path"] = JsonSerializer.SerializeToElement(path);
        prov["display_name"] = JsonSerializer.SerializeToElement(LastSegment(path));
        prov["parent_path"] = JsonSerializer.SerializeToElement(parentPath ?? string.Empty);
        prov["durability"] = JsonSerializer.SerializeToElement(ReadString(prov, "durability") ?? durability);
        prov[RetrievalVisibilityKey] = JsonSerializer.SerializeToElement(ReadString(prov, RetrievalVisibilityKey) ?? RetrievalVisibilityDefault);
        prov["created_by"] = JsonSerializer.SerializeToElement(ReadString(prov, "created_by") ?? createdBy);
        prov["created_at"] = JsonSerializer.SerializeToElement(ReadDate(prov, "created_at") ?? nowUtc);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);
        prov["source"] = JsonSerializer.SerializeToElement("manual");
        prov["members_container_id"] = JsonSerializer.SerializeToElement(MembersContainerId(path));

        EnsureContainer(MembersContainerId(path), "smartlist_members");

        if (parentPath is null)
        {
            EnsureAttached(RootContainerId(string.Equals(durability, DurableDurability, StringComparison.Ordinal)), objectId);
        }
        else
        {
            var parent = EnsureBucket(parentPath, ParentPath(parentPath), durability, createdBy, nowUtc);
            EnsureAttached(MembersContainerId(parent.Path), objectId);
        }

        if (!created)
        {
            var effectiveDurability = ReadString(prov, "durability") ?? durability;
            EnsureRootMembership(path, effectiveDurability);
        }

        _retrievalGraph.ProjectSmartListBucket(path);
        return GetBucket(path)!;
    }

    private void PromoteBucketTree(string path, DateTimeOffset nowUtc, HashSet<string> promoted)
    {
        var bucket = GetBucket(path) ?? throw new InvalidOperationException($"Unknown SmartList bucket '{path}'.");
        if (promoted.Add(bucket.ObjectId))
            PromoteObject(bucket.ObjectId, DurableDurability, nowUtc);

        EnsureRootMembership(bucket.Path, DurableDurability);

        foreach (var member in _store.IterateForward(MembersContainerId(bucket.Path)).Select(x => x.ObjectId).ToList())
        {
            if (!_store.Objects.TryGetValue(member, out var obj))
                continue;

            if (obj.ObjectKind == BucketObjectKind)
            {
                var childPath = ReadString(obj.SemanticPayload?.Provenance, "path") ?? member;
                PromoteBucketTree(childPath, nowUtc, promoted);
                continue;
            }

            if (obj.ObjectKind == RollupObjectKind && promoted.Add(member))
            {
                PromoteObject(member, DurableDurability, nowUtc);
                continue;
            }

            if (obj.ObjectKind == NoteObjectKind && promoted.Add(member))
                PromoteNote(member, nowUtc);
        }
    }

    private void PromoteNote(string noteId, DateTimeOffset nowUtc)
    {
        PromoteObject(noteId, DurableDurability, nowUtc);

        foreach (var containerId in _store.ContainersForMemberObject(noteId))
        {
            if (string.Equals(containerId, ShortTermRootContainer, StringComparison.Ordinal))
            {
                RemoveMembership(ShortTermRootContainer, noteId);
                EnsureAttached(DurableRootContainer, noteId);
            }
        }
    }

    private void PromoteObject(string objectId, string durability, DateTimeOffset nowUtc)
    {
        var obj = _store.Objects[objectId];
        obj.SemanticPayload ??= new SemanticPayload();
        obj.SemanticPayload.Tags = (obj.SemanticPayload.Tags ?? [])
            .Where(x => !string.Equals(x, ShortTermDurability, StringComparison.OrdinalIgnoreCase) && !string.Equals(x, DurableDurability, StringComparison.OrdinalIgnoreCase))
            .Append(durability)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        var prov = EnsureProv(obj);
        prov["durability"] = JsonSerializer.SerializeToElement(durability);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);
        obj.UpdatedAt = nowUtc;
    }

    private void EnsureRootMembership(string path, string durability)
    {
        var objectId = BucketObjectId(path);
        var desiredRoot = RootContainerId(string.Equals(durability, DurableDurability, StringComparison.Ordinal));
        var otherRoot = string.Equals(desiredRoot, DurableRootContainer, StringComparison.Ordinal) ? ShortTermRootContainer : DurableRootContainer;

        if (ParentPath(path) is not null)
            return;

        RemoveMembership(otherRoot, objectId);
        EnsureAttached(desiredRoot, objectId);
    }

    private void AddRootEntries(List<SmartListInspectEntry> entries, bool durable)
    {
        var rootContainerId = RootContainerId(durable);
        foreach (var member in _store.IterateForward(rootContainerId))
        {
            if (!_store.Objects.TryGetValue(member.ObjectId, out var obj))
                continue;

            if (obj.ObjectKind == BucketObjectKind)
            {
                var bucket = GetBucket(ReadString(obj.SemanticPayload?.Provenance, "path") ?? member.ObjectId);
                if (bucket is not null)
                    entries.Add(new SmartListInspectEntry(0, "bucket", bucket.Path, bucket.DisplayName, bucket.Durability, bucket.RetrievalVisibility));
            }
            else if (obj.ObjectKind == RollupObjectKind)
            {
                var rollup = GetRollup(ReadString(obj.SemanticPayload?.Provenance, "bucket_path") ?? member.ObjectId);
                if (rollup is not null)
                    entries.Add(new SmartListInspectEntry(0, "rollup", rollup.BucketPath, rollup.Title, rollup.Durability, rollup.RetrievalVisibility));
            }
            else if (obj.ObjectKind == NoteObjectKind)
            {
                var note = GetNote(member.ObjectId);
                if (note is not null)
                    entries.Add(new SmartListInspectEntry(0, "note", note.NoteId, note.Title, note.Durability, note.RetrievalVisibility));
            }
        }
    }

    private void CollectEntries(string bucketPath, int depth, int maxDepth, List<SmartListInspectEntry> entries, HashSet<string> seenBuckets)
    {
        if (!seenBuckets.Add(bucketPath))
            return;

        foreach (var member in _store.IterateForward(MembersContainerId(bucketPath)))
        {
            if (!_store.Objects.TryGetValue(member.ObjectId, out var obj))
                continue;

            if (obj.ObjectKind == BucketObjectKind)
            {
                var childPath = ReadString(obj.SemanticPayload?.Provenance, "path") ?? member.ObjectId;
                var bucket = GetBucket(childPath);
                if (bucket is null)
                    continue;

                entries.Add(new SmartListInspectEntry(depth + 1, "bucket", bucket.Path, bucket.DisplayName, bucket.Durability, bucket.RetrievalVisibility));
                if (depth < maxDepth)
                    CollectEntries(bucket.Path, depth + 1, maxDepth, entries, seenBuckets);
            }
            else if (obj.ObjectKind == RollupObjectKind)
            {
                var rollup = GetRollup(ReadString(obj.SemanticPayload?.Provenance, "bucket_path") ?? member.ObjectId);
                if (rollup is not null)
                    entries.Add(new SmartListInspectEntry(depth + 1, "rollup", rollup.BucketPath, rollup.Title, rollup.Durability, rollup.RetrievalVisibility));
            }
            else if (obj.ObjectKind == NoteObjectKind)
            {
                var note = GetNote(member.ObjectId);
                if (note is not null)
                    entries.Add(new SmartListInspectEntry(depth + 1, "note", note.NoteId, note.Title, note.Durability, note.RetrievalVisibility));
            }
        }
    }

    private void ApplyBucketVisibility(
        string bucketPath,
        string visibility,
        bool recursive,
        bool includeNotes,
        bool includeRollups,
        DateTimeOffset nowUtc,
        ref int bucketCount,
        ref int noteCount,
        ref int rollupCount)
    {
        UpdateObjectVisibility(BucketObjectId(bucketPath), visibility, nowUtc);
        bucketCount++;

        var rollupId = RollupObjectId(bucketPath);
        if (includeRollups && _store.Objects.TryGetValue(rollupId, out var rollup) && rollup.ObjectKind == RollupObjectKind)
        {
            UpdateObjectVisibility(rollupId, visibility, nowUtc);
            rollupCount++;
        }

        foreach (var memberId in _store.IterateForward(MembersContainerId(bucketPath)).Select(x => x.ObjectId).ToList())
        {
            if (!_store.Objects.TryGetValue(memberId, out var obj))
                continue;

            if (obj.ObjectKind == NoteObjectKind)
            {
                if (includeNotes)
                {
                    UpdateObjectVisibility(memberId, visibility, nowUtc);
                    noteCount++;
                }
                continue;
            }

            if (obj.ObjectKind != BucketObjectKind || !recursive)
                continue;

            var childPath = ReadString(obj.SemanticPayload?.Provenance, "path") ?? memberId;
            ApplyBucketVisibility(childPath, visibility, true, includeNotes, includeRollups, nowUtc, ref bucketCount, ref noteCount, ref rollupCount);
        }
    }

    private void UpdateObjectVisibility(string objectId, string visibility, DateTimeOffset nowUtc)
    {
        if (!_store.Objects.TryGetValue(objectId, out var obj))
            return;

        var prov = EnsureProv(obj);
        prov[RetrievalVisibilityKey] = JsonSerializer.SerializeToElement(visibility);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);
        obj.UpdatedAt = nowUtc;

        ProjectSmartListMemberIfSupported(objectId);
    }

    private string ResolveMemberObject(string memberRef, string createdBy, DateTimeOffset nowUtc)
    {
        if (_store.Objects.ContainsKey(memberRef))
            return memberRef;

        var normalized = NormalizePath(memberRef);
        if (normalized.StartsWith("smartlist/", StringComparison.Ordinal))
            return EnsureBucketPath(normalized, ShortTermDurability, createdBy, nowUtc).ObjectId;

        throw new InvalidOperationException($"Unknown SmartList member '{memberRef}'.");
    }

    private IReadOnlyList<string> BucketPathsForNote(string noteId)
    {
        return _store.ContainersForMemberObject(noteId)
            .Select(ContainerIdToBucketPath)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList()!;
    }

    private void EnsureAttached(string containerId, string objectId)
    {
        if (!_store.HasMembership(containerId, objectId))
            _store.AddObject(containerId, objectId);
    }

    private void ProjectSmartListMemberIfSupported(string objectId)
    {
        if (!_store.Objects.TryGetValue(objectId, out var obj))
            return;

        switch (obj.ObjectKind)
        {
            case BucketObjectKind:
                var path = ReadString(obj.SemanticPayload?.Provenance, "path");
                if (!string.IsNullOrWhiteSpace(path))
                    _retrievalGraph.ProjectSmartListBucket(path);
                break;
            case NoteObjectKind:
                _retrievalGraph.ProjectSmartListNote(objectId);
                break;
            case RollupObjectKind:
                _retrievalGraph.ProjectSmartListRollup(objectId);
                break;
        }
    }

    private void RemoveMembership(string containerId, string objectId)
    {
        if (_store.TryGetMembership(containerId, objectId, out var membership))
            _store.RemoveLinkNode(containerId, membership.LinkNodeId);
    }

    private void EnsureContainer(string containerId, string containerKind)
    {
        if (!_store.Containers.ContainsKey(containerId))
            _store.CreateContainer(containerId, "container", containerKind);
        _store.Containers[containerId].Policies.UniqueMembers = true;
    }

    private static string RootContainerId(bool durable) => durable ? DurableRootContainer : ShortTermRootContainer;
    private static string BucketObjectId(string path) => $"smartlist-bucket:{path}";
    private static string RollupObjectId(string path) => $"smartlist-rollup:{path}";
    private static string MembersContainerId(string path) => $"smartlist-members:{path}";

    private static string NormalizePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("path is required", nameof(path));

        var normalized = path.Replace('\\', '/').Trim();
        var parts = normalized
            .Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(NormalizePathPart)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .ToList();

        if (parts.Count == 0)
            throw new ArgumentException("path must contain at least one segment", nameof(path));

        if (!string.Equals(parts[0], "smartlist", StringComparison.Ordinal))
            parts.Insert(0, "smartlist");

        return string.Join('/', parts);
    }

    private static string NormalizePathPart(string segment)
    {
        var normalized = PathPartRx.Replace(segment.Trim().ToLowerInvariant(), "-").Trim('-');
        if (string.IsNullOrWhiteSpace(normalized))
            throw new ArgumentException($"Invalid SmartList path segment '{segment}'.", nameof(segment));
        return normalized;
    }

    private static string? ParentPath(string path)
    {
        var slash = path.LastIndexOf('/');
        if (slash <= "smartlist".Length)
            return null;
        return path[..slash];
    }

    private static string LastSegment(string path)
    {
        var slash = path.LastIndexOf('/');
        return slash >= 0 ? path[(slash + 1)..] : path;
    }

    private static string? ContainerIdToBucketPath(string containerId)
    {
        if (!containerId.StartsWith("smartlist-members:", StringComparison.Ordinal))
            return null;
        return containerId["smartlist-members:".Length..];
    }

    private static Dictionary<string, JsonElement> EnsureProv(ObjectRecord obj)
    {
        obj.SemanticPayload ??= new SemanticPayload();
        obj.SemanticPayload.Provenance ??= new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        return obj.SemanticPayload.Provenance;
    }

    internal static string ReadRetrievalVisibility(IReadOnlyDictionary<string, JsonElement>? p)
        => NormalizeRetrievalVisibility(ReadString(p, RetrievalVisibilityKey));

    internal static string NormalizeRetrievalVisibility(string? value)
    {
        return (value ?? RetrievalVisibilityDefault).Trim().ToLowerInvariant() switch
        {
            RetrievalVisibilityDefault => RetrievalVisibilityDefault,
            RetrievalVisibilityScoped => RetrievalVisibilityScoped,
            RetrievalVisibilitySuppressed => RetrievalVisibilitySuppressed,
            _ => throw new ArgumentException($"Invalid retrieval visibility '{value}'. Expected default, scoped, or suppressed.")
        };
    }

    private static string? ReadString(IReadOnlyDictionary<string, JsonElement>? p, string key)
        => p is not null && p.TryGetValue(key, out var el) ? (el.ValueKind == JsonValueKind.String ? el.GetString() : el.ToString()) : null;

    private static DateTimeOffset? ReadDate(IReadOnlyDictionary<string, JsonElement>? p, string key)
        => p is not null && p.TryGetValue(key, out var el) && (el.TryGetDateTimeOffset(out var x) || (el.ValueKind == JsonValueKind.String && DateTimeOffset.TryParse(el.GetString(), out x))) ? x : null;

    private static string? EmptyToNull(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;
}
