using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using AMS.Core;

namespace MemoryCtl;

internal sealed record AgentCapabilityEntryInfo(
    string EntryId,
    string Agent,
    string CapabilityKey,
    string State,
    string ProblemKey,
    string EquivalenceGroupKey,
    string GroupObjectId,
    string Summary,
    string Notes,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);

internal sealed class AgentCapabilityService
{
    internal const string EntryObjectKind = "agent_capability_entry";
    internal const string GroupObjectKind = "agent_capability_group";

    private static readonly Regex KeyPartRx = new("[^a-z0-9-]+", RegexOptions.Compiled | RegexOptions.IgnoreCase);
    private static readonly HashSet<string> AllowedStates = new(StringComparer.Ordinal)
    {
        "mirrored",
        "partial",
        "workaround",
        "missing",
        "intentional_asymmetry"
    };

    private readonly AmsStore _store;
    private readonly SmartListService _smartLists;

    public AgentCapabilityService(AmsStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _smartLists = new SmartListService(store);
    }

    public AgentCapabilityEntryInfo Upsert(
        string agent,
        string capabilityKey,
        string state,
        string problemKey,
        string equivalenceGroupKey,
        string? summary,
        string? notes,
        string createdBy,
        DateTimeOffset nowUtc)
    {
        var normalizedAgent = NormalizeKey(agent, nameof(agent));
        var normalizedCapabilityKey = NormalizeKey(capabilityKey, nameof(capabilityKey));
        var normalizedState = NormalizeState(state);
        var normalizedProblemKey = NormalizeKey(problemKey, nameof(problemKey));
        var normalizedEquivalenceGroupKey = NormalizeKey(equivalenceGroupKey, nameof(equivalenceGroupKey));

        var entryId = EntryObjectId(normalizedAgent, normalizedCapabilityKey);
        var existing = GetById(entryId);
        var previousProblem = existing?.ProblemKey;
        var previousGroup = existing?.EquivalenceGroupKey;

        var agentBucketPath = AgentBucketPath(normalizedAgent);
        var problemBucketPath = ProblemBucketPath(normalizedProblemKey);
        var groupBucketPath = GroupBucketPath(normalizedEquivalenceGroupKey);

        _smartLists.CreateBucket(agentBucketPath, durable: true, createdBy, nowUtc);
        _smartLists.CreateBucket(problemBucketPath, durable: true, createdBy, nowUtc);
        _smartLists.CreateBucket(groupBucketPath, durable: true, createdBy, nowUtc);

        var groupObjectId = EnsureGroupObject(normalizedEquivalenceGroupKey, createdBy, nowUtc);

        var effectiveSummary = string.IsNullOrWhiteSpace(summary)
            ? $"{normalizedAgent}:{normalizedCapabilityKey}"
            : summary.Trim();

        _store.UpsertObject(entryId, EntryObjectKind);
        var entry = _store.Objects[entryId];
        entry.SemanticPayload ??= new SemanticPayload();
        entry.SemanticPayload.Summary = effectiveSummary;
        entry.SemanticPayload.Tags = ["agent_capability", normalizedState, normalizedAgent];

        var prov = EnsureProv(entry);
        prov["agent"] = JsonSerializer.SerializeToElement(normalizedAgent);
        prov["capability_key"] = JsonSerializer.SerializeToElement(normalizedCapabilityKey);
        prov["state"] = JsonSerializer.SerializeToElement(normalizedState);
        prov["problem_key"] = JsonSerializer.SerializeToElement(normalizedProblemKey);
        prov["equivalence_group_key"] = JsonSerializer.SerializeToElement(normalizedEquivalenceGroupKey);
        prov["summary"] = JsonSerializer.SerializeToElement(effectiveSummary);
        prov["notes"] = JsonSerializer.SerializeToElement(string.IsNullOrWhiteSpace(notes) ? string.Empty : notes.Trim());
        prov["group_object_id"] = JsonSerializer.SerializeToElement(groupObjectId);
        prov["created_by"] = JsonSerializer.SerializeToElement(ReadString(prov, "created_by") ?? createdBy);
        prov["created_at"] = JsonSerializer.SerializeToElement(ReadDate(prov, "created_at") ?? nowUtc);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);

        if (!string.IsNullOrWhiteSpace(previousProblem) && !string.Equals(previousProblem, normalizedProblemKey, StringComparison.Ordinal))
            RemoveMembership(MembersContainerId(ProblemBucketPath(previousProblem!)), entryId);
        if (!string.IsNullOrWhiteSpace(previousGroup) && !string.Equals(previousGroup, normalizedEquivalenceGroupKey, StringComparison.Ordinal))
            RemoveMembership(MembersContainerId(GroupBucketPath(previousGroup!)), entryId);

        _smartLists.Attach(agentBucketPath, entryId, createdBy, nowUtc);
        _smartLists.Attach(problemBucketPath, entryId, createdBy, nowUtc);
        _smartLists.Attach(groupBucketPath, entryId, createdBy, nowUtc);
        _smartLists.Attach(groupBucketPath, groupObjectId, createdBy, nowUtc);

        return GetById(entryId)!;
    }

    public AgentCapabilityEntryInfo? GetByPair(string agent, string capabilityKey)
        => GetById(EntryObjectId(NormalizeKey(agent, nameof(agent)), NormalizeKey(capabilityKey, nameof(capabilityKey))));

    public AgentCapabilityEntryInfo? GetById(string entryId)
    {
        if (!_store.Objects.TryGetValue(entryId, out var obj) || obj.ObjectKind != EntryObjectKind)
            return null;

        var prov = obj.SemanticPayload?.Provenance;
        return new AgentCapabilityEntryInfo(
            entryId,
            ReadString(prov, "agent") ?? string.Empty,
            ReadString(prov, "capability_key") ?? string.Empty,
            ReadString(prov, "state") ?? string.Empty,
            ReadString(prov, "problem_key") ?? string.Empty,
            ReadString(prov, "equivalence_group_key") ?? string.Empty,
            ReadString(prov, "group_object_id") ?? GroupObjectId(ReadString(prov, "equivalence_group_key") ?? string.Empty),
            ReadString(prov, "summary") ?? obj.SemanticPayload?.Summary ?? string.Empty,
            ReadString(prov, "notes") ?? string.Empty,
            ReadDate(prov, "created_at") ?? obj.CreatedAt,
            ReadDate(prov, "updated_at") ?? obj.UpdatedAt);
    }

    public IReadOnlyList<AgentCapabilityEntryInfo> ListByAgent(string agent)
        => ListFromBucket(AgentBucketPath(NormalizeKey(agent, nameof(agent))));

    public IReadOnlyList<AgentCapabilityEntryInfo> ListByProblem(string problemKey)
        => ListFromBucket(ProblemBucketPath(NormalizeKey(problemKey, nameof(problemKey))));

    public IReadOnlyList<AgentCapabilityEntryInfo> ListByGroup(string equivalenceGroupKey)
        => ListFromBucket(GroupBucketPath(NormalizeKey(equivalenceGroupKey, nameof(equivalenceGroupKey))));

    private IReadOnlyList<AgentCapabilityEntryInfo> ListFromBucket(string bucketPath)
    {
        var containerId = MembersContainerId(bucketPath);
        if (!_store.Containers.ContainsKey(containerId))
            return [];

        return _store.IterateForward(containerId)
            .Select(link => GetById(link.ObjectId))
            .Where(entry => entry is not null)
            .Select(entry => entry!)
            .OrderBy(entry => entry.Agent, StringComparer.Ordinal)
            .ThenBy(entry => entry.CapabilityKey, StringComparer.Ordinal)
            .ToList();
    }

    private string EnsureGroupObject(string equivalenceGroupKey, string createdBy, DateTimeOffset nowUtc)
    {
        var objectId = GroupObjectId(equivalenceGroupKey);
        _store.UpsertObject(objectId, GroupObjectKind);
        var obj = _store.Objects[objectId];
        obj.SemanticPayload ??= new SemanticPayload();
        obj.SemanticPayload.Summary = equivalenceGroupKey;
        obj.SemanticPayload.Tags = ["agent_capability_group"];

        var prov = EnsureProv(obj);
        prov["equivalence_group_key"] = JsonSerializer.SerializeToElement(equivalenceGroupKey);
        prov["created_by"] = JsonSerializer.SerializeToElement(ReadString(prov, "created_by") ?? createdBy);
        prov["created_at"] = JsonSerializer.SerializeToElement(ReadDate(prov, "created_at") ?? nowUtc);
        prov["updated_at"] = JsonSerializer.SerializeToElement(nowUtc);
        return objectId;
    }

    private static string NormalizeState(string state)
    {
        if (string.IsNullOrWhiteSpace(state))
            throw new ArgumentException("state is required", nameof(state));

        var normalized = state.Trim().ToLowerInvariant();
        if (!AllowedStates.Contains(normalized))
            throw new ArgumentException($"Invalid agent capability state '{state}'. Allowed values: {string.Join(", ", AllowedStates.OrderBy(x => x, StringComparer.Ordinal))}.");

        return normalized;
    }

    private static string NormalizeKey(string raw, string paramName)
    {
        if (string.IsNullOrWhiteSpace(raw))
            throw new ArgumentException($"{paramName} is required", paramName);

        var normalized = KeyPartRx.Replace(raw.Trim().ToLowerInvariant(), "-").Trim('-');
        if (string.IsNullOrWhiteSpace(normalized))
            throw new ArgumentException($"{paramName} must contain at least one alphanumeric character", paramName);

        return normalized;
    }

    private void RemoveMembership(string containerId, string memberId)
    {
        if (_store.TryGetMembership(containerId, memberId, out var link))
            _store.RemoveLinkNode(containerId, link.LinkNodeId);
    }

    private static string AgentBucketPath(string agent) => $"smartlist/agent-capabilities/agents/{agent}";
    private static string ProblemBucketPath(string problemKey) => $"smartlist/agent-capabilities/problems/{problemKey}";
    private static string GroupBucketPath(string equivalenceGroupKey) => $"smartlist/agent-capabilities/groups/{equivalenceGroupKey}";
    private static string MembersContainerId(string bucketPath) => $"smartlist-members:{bucketPath}";
    private static string GroupObjectId(string equivalenceGroupKey) => $"agent-capability-group:{StableKeyPart(equivalenceGroupKey)}";
    private static string EntryObjectId(string agent, string capabilityKey) => $"agent-capability-entry:{agent}:{StableKeyPart(capabilityKey)}";

    private static string StableKeyPart(string key)
    {
        var hash = Hash8(key);
        var prefix = key.Length > 32 ? key[..32].Trim('-') : key;
        return $"{prefix}-{hash}".Trim('-');
    }

    private static string Hash8(string text)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text ?? string.Empty))).ToLowerInvariant()[..8];

    private static Dictionary<string, JsonElement> EnsureProv(ObjectRecord obj)
    {
        obj.SemanticPayload ??= new SemanticPayload();
        obj.SemanticPayload.Provenance ??= new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        return obj.SemanticPayload.Provenance;
    }

    private static string? ReadString(IReadOnlyDictionary<string, JsonElement>? p, string k)
        => p is not null && p.TryGetValue(k, out var e) ? (e.ValueKind == JsonValueKind.String ? e.GetString() : e.ToString()) : null;

    private static DateTimeOffset? ReadDate(IReadOnlyDictionary<string, JsonElement>? p, string k)
        => p is not null && p.TryGetValue(k, out var e) && (e.TryGetDateTimeOffset(out var x) || (e.ValueKind == JsonValueKind.String && DateTimeOffset.TryParse(e.GetString(), out x))) ? x : null;
}
