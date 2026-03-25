using System.Text.Json;
using AMS.Core;

namespace MemoryCtl;

/// <summary>
/// Manages conversation thread note lifecycle: archive/freeze, active authority pointer,
/// and supersession/historical markers. Designed for disposable archive-freeze task graphs.
/// </summary>
internal sealed record ThreadNoteInfo(
    string NoteObjectId,
    string ThreadId,
    string Title,
    string Text,
    string Status,
    string RetrievalVisibility,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);

internal sealed record ThreadAuthorityState(
    string ThreadId,
    string? ActiveNoteObjectId,
    IReadOnlyList<ThreadNoteInfo> AllNotes,
    IReadOnlyList<ThreadNoteInfo> HistoricalNotes,
    bool IsFrozen);

internal sealed class ConversationThreadBuilder
{
    internal const string ThreadNoteObjectKind = "conversation_thread_note";
    internal const string StatusActive = "active";
    internal const string StatusSuperseded = "superseded";
    internal const string StatusHistorical = "historical";

    private readonly AmsStore _store;
    private readonly SmartListService _smartLists;

    public ConversationThreadBuilder(AmsStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _smartLists = new SmartListService(store);
    }

    /// <summary>
    /// Adds a note to a thread and promotes it to active authority.
    /// Any previously active note on this thread is superseded and suppressed.
    /// </summary>
    public ThreadNoteInfo AddNote(
        string threadId,
        string title,
        string text,
        string createdBy,
        DateTimeOffset nowUtc)
    {
        if (string.IsNullOrWhiteSpace(threadId))
            throw new ArgumentException("threadId is required", nameof(threadId));
        if (ReadFrozenFlag(threadId))
            throw new InvalidOperationException($"Cannot add a note to frozen thread '{threadId}'.");
        if (string.IsNullOrWhiteSpace(title))
            throw new ArgumentException("title is required", nameof(title));
        if (string.IsNullOrWhiteSpace(text))
            throw new ArgumentException("text is required", nameof(text));

        EnsureThreadContainer(threadId);
        SupersedeCurrentActive(threadId, nowUtc);

        var noteObjectId = BuildNoteObjectId(threadId, nowUtc);
        _store.UpsertObject(noteObjectId, ThreadNoteObjectKind);
        var obj = _store.Objects[noteObjectId];
        obj.SemanticPayload ??= new SemanticPayload();
        obj.SemanticPayload.Summary = title.Trim();
        obj.SemanticPayload.Tags = [ThreadNoteObjectKind, StatusActive];

        var prov = EnsureProv(obj);
        prov["thread_id"] = JsonSerializer.SerializeToElement(threadId);
        prov["title"] = JsonSerializer.SerializeToElement(title.Trim());
        prov["text"] = JsonSerializer.SerializeToElement(text.Trim());
        prov["status"] = JsonSerializer.SerializeToElement(StatusActive);
        prov[SmartListService.RetrievalVisibilityKey] = JsonSerializer.SerializeToElement(SmartListService.RetrievalVisibilityDefault);
        prov["created_by"] = JsonSerializer.SerializeToElement(createdBy);
        prov["created_at"] = JsonSerializer.SerializeToElement(nowUtc);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);

        _store.AddObject(NotesContainerId(threadId), noteObjectId);
        SetActivePointer(threadId, noteObjectId, nowUtc);

        return ParseNote(noteObjectId)!;
    }

    /// <summary>
    /// Freezes a thread: all notes become historical, active authority pointer is cleared.
    /// Frozen threads suppress all notes from default retrieval.
    /// </summary>
    public ThreadAuthorityState Freeze(string threadId, DateTimeOffset nowUtc)
    {
        if (string.IsNullOrWhiteSpace(threadId))
            throw new ArgumentException("threadId is required", nameof(threadId));

        EnsureThreadContainer(threadId);

        foreach (var noteObjectId in EnumerateNoteObjectIds(threadId))
        {
            SetNoteStatus(noteObjectId, StatusHistorical, SmartListService.RetrievalVisibilitySuppressed, nowUtc);
        }

        ClearActivePointer(threadId, nowUtc);
        SetFrozenFlag(threadId, true, nowUtc);

        return GetAuthorityState(threadId);
    }

    /// <summary>
    /// Returns the current authority state for a thread: active note, all notes, and frozen status.
    /// </summary>
    public ThreadAuthorityState GetAuthorityState(string threadId)
    {
        if (string.IsNullOrWhiteSpace(threadId))
            throw new ArgumentException("threadId is required", nameof(threadId));

        var containerId = NotesContainerId(threadId);
        if (!_store.Containers.ContainsKey(containerId))
            return new ThreadAuthorityState(threadId, null, [], [], false);

        var allNotes = EnumerateNoteObjectIds(threadId)
            .Select(ParseNote)
            .Where(n => n is not null)
            .Select(n => n!)
            .OrderByDescending(n => n.CreatedAt)
            .ToList();

        var activeNoteId = ReadActivePointer(threadId);
        var historicalNotes = allNotes
            .Where(n => n.Status is StatusSuperseded or StatusHistorical)
            .ToList();

        var isFrozen = ReadFrozenFlag(threadId);

        return new ThreadAuthorityState(threadId, activeNoteId, allNotes, historicalNotes, isFrozen);
    }

    // ── internals ──────────────────────────────────────────────────────────

    private void SupersedeCurrentActive(string threadId, DateTimeOffset nowUtc)
    {
        var currentActiveId = ReadActivePointer(threadId);
        if (string.IsNullOrWhiteSpace(currentActiveId))
            return;

        SetNoteStatus(currentActiveId, StatusSuperseded, SmartListService.RetrievalVisibilitySuppressed, nowUtc);
    }

    private void SetNoteStatus(string noteObjectId, string status, string visibility, DateTimeOffset nowUtc)
    {
        if (!_store.Objects.TryGetValue(noteObjectId, out var obj))
            return;

        obj.SemanticPayload ??= new SemanticPayload();
        obj.SemanticPayload.Tags = (obj.SemanticPayload.Tags ?? [])
            .Where(t => t != StatusActive && t != StatusSuperseded && t != StatusHistorical)
            .Append(status)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        var prov = EnsureProv(obj);
        prov["status"] = JsonSerializer.SerializeToElement(status);
        prov[SmartListService.RetrievalVisibilityKey] = JsonSerializer.SerializeToElement(visibility);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);
        obj.UpdatedAt = nowUtc;
    }

    private void EnsureThreadContainer(string threadId)
    {
        var containerId = NotesContainerId(threadId);
        if (!_store.Containers.ContainsKey(containerId))
            _store.CreateContainer(containerId, "container", "conversation_thread_notes");
        _store.Containers[containerId].Policies.UniqueMembers = true;

        var metaContainerId = MetaContainerId(threadId);
        if (!_store.Containers.ContainsKey(metaContainerId))
            _store.CreateContainer(metaContainerId, "container", "conversation_thread_meta");
        _store.Containers[metaContainerId].Policies.UniqueMembers = true;
    }

    private void SetActivePointer(string threadId, string noteObjectId, DateTimeOffset nowUtc)
    {
        var metaContainerId = MetaContainerId(threadId);
        var pointerId = ActivePointerObjectId(threadId);
        _store.UpsertObject(pointerId, "conversation_thread_active_pointer");
        var obj = _store.Objects[pointerId];
        obj.SemanticPayload ??= new SemanticPayload();
        obj.SemanticPayload.Summary = $"Active authority for {threadId}";

        var prov = EnsureProv(obj);
        prov["thread_id"] = JsonSerializer.SerializeToElement(threadId);
        prov["active_note_object_id"] = JsonSerializer.SerializeToElement(noteObjectId);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);

        if (!_store.HasMembership(metaContainerId, pointerId))
            _store.AddObject(metaContainerId, pointerId);
    }

    private void ClearActivePointer(string threadId, DateTimeOffset nowUtc)
    {
        var pointerId = ActivePointerObjectId(threadId);
        if (!_store.Objects.TryGetValue(pointerId, out var obj))
            return;

        var prov = EnsureProv(obj);
        prov["active_note_object_id"] = JsonSerializer.SerializeToElement(string.Empty);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);
    }

    private string? ReadActivePointer(string threadId)
    {
        var pointerId = ActivePointerObjectId(threadId);
        if (!_store.Objects.TryGetValue(pointerId, out var obj))
            return null;

        var value = ReadString(obj.SemanticPayload?.Provenance, "active_note_object_id");
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }

    private void SetFrozenFlag(string threadId, bool frozen, DateTimeOffset nowUtc)
    {
        var metaContainerId = MetaContainerId(threadId);
        var frozenId = FrozenFlagObjectId(threadId);
        _store.UpsertObject(frozenId, "conversation_thread_frozen_flag");
        var obj = _store.Objects[frozenId];
        obj.SemanticPayload ??= new SemanticPayload();
        obj.SemanticPayload.Summary = frozen ? $"Thread {threadId} is frozen" : $"Thread {threadId} is active";

        var prov = EnsureProv(obj);
        prov["thread_id"] = JsonSerializer.SerializeToElement(threadId);
        prov["frozen"] = JsonSerializer.SerializeToElement(frozen);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);

        if (!_store.HasMembership(metaContainerId, frozenId))
            _store.AddObject(metaContainerId, frozenId);
    }

    private bool ReadFrozenFlag(string threadId)
    {
        var frozenId = FrozenFlagObjectId(threadId);
        if (!_store.Objects.TryGetValue(frozenId, out var obj))
            return false;

        var prov = obj.SemanticPayload?.Provenance;
        if (prov is null || !prov.TryGetValue("frozen", out var el))
            return false;

        return el.ValueKind == JsonValueKind.True || (el.ValueKind == JsonValueKind.String && bool.TryParse(el.GetString(), out var b) && b);
    }

    private IReadOnlyList<string> EnumerateNoteObjectIds(string threadId)
    {
        var containerId = NotesContainerId(threadId);
        if (!_store.Containers.ContainsKey(containerId))
            return [];

        return _store.IterateForward(containerId)
            .Select(x => x.ObjectId)
            .Where(id => _store.Objects.TryGetValue(id, out var obj) && obj.ObjectKind == ThreadNoteObjectKind)
            .ToList();
    }

    private ThreadNoteInfo? ParseNote(string noteObjectId)
    {
        if (!_store.Objects.TryGetValue(noteObjectId, out var obj) || obj.ObjectKind != ThreadNoteObjectKind)
            return null;

        var prov = obj.SemanticPayload?.Provenance;
        return new ThreadNoteInfo(
            noteObjectId,
            ReadString(prov, "thread_id") ?? string.Empty,
            ReadString(prov, "title") ?? obj.SemanticPayload?.Summary ?? noteObjectId,
            ReadString(prov, "text") ?? string.Empty,
            ReadString(prov, "status") ?? StatusActive,
            SmartListService.ReadRetrievalVisibility(prov),
            ReadDate(prov, "created_at") ?? obj.CreatedAt,
            ReadDate(prov, "updated_at") ?? obj.UpdatedAt);
    }

    private static string NotesContainerId(string threadId) => $"conversation-thread-notes:{threadId}";
    private static string MetaContainerId(string threadId) => $"conversation-thread-meta:{threadId}";
    private static string ActivePointerObjectId(string threadId) => $"conversation-thread-active-pointer:{threadId}";
    private static string FrozenFlagObjectId(string threadId) => $"conversation-thread-frozen:{threadId}";

    private static string BuildNoteObjectId(string threadId, DateTimeOffset nowUtc)
        => $"conversation-thread-note:{threadId}:{nowUtc:yyyyMMddHHmmssfff}:{Guid.NewGuid():N}";

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
}
