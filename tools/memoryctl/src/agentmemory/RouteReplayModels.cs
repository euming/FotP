using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace MemoryCtl;

/// <summary>
/// One case in a route-replay input file (JSONL, one record per line).
/// Captures a query scenario and the pre-recorded route-memory episodes to inject
/// before the replay run.
/// </summary>
internal sealed record RouteReplayRecord(
    [property: JsonPropertyName("query")] string Query,
    [property: JsonPropertyName("top")] int Top,
    [property: JsonPropertyName("current_node")] string? CurrentNode,
    [property: JsonPropertyName("parent_node")] string? ParentNode,
    [property: JsonPropertyName("grandparent_node")] string? GrandparentNode,
    [property: JsonPropertyName("role")] string? Role,
    [property: JsonPropertyName("mode")] string? Mode,
    [property: JsonPropertyName("no_active_thread_context")] bool NoActiveThreadContext,
    [property: JsonPropertyName("episodes")] IReadOnlyList<RouteReplayEpisodeEntry> Episodes,
    /// <summary>
    /// Target refs that the replay result must surface (in any position) to count as a pass.
    /// Used to classify top1 changes as promotions vs. demotions and to populate
    /// <see cref="RouteReplayOutput.ExpectedRefsHit"/>.
    /// When null or empty the acceptance check is skipped and delta direction is unknown.
    /// </summary>
    [property: JsonPropertyName("expected_refs")] IReadOnlyList<string>? ExpectedRefs = null);

/// <summary>One pre-recorded episode to inject for a replay case.</summary>
internal sealed record RouteReplayEpisodeEntry(
    [property: JsonPropertyName("frame")] RouteReplayFrameInput Frame,
    [property: JsonPropertyName("route")] RouteReplayRouteInput Route,
    [property: JsonPropertyName("episode")] RouteReplayEpisodeInput Episode,
    /// <summary>
    /// Target refs of all candidate lessons shown during the original retrieval
    /// (including the winner). Used to record EpisodeCandidate edges.
    /// </summary>
    [property: JsonPropertyName("candidate_target_refs")] IReadOnlyList<string> CandidateTargetRefs,
    /// <summary>Target ref of the lesson that won this episode.</summary>
    [property: JsonPropertyName("winning_target_ref")] string WinningTargetRef);

internal sealed record RouteReplayFrameInput(
    [property: JsonPropertyName("scope_lens")] string ScopeLens,
    [property: JsonPropertyName("agent_role")] string AgentRole,
    [property: JsonPropertyName("mode")] string Mode,
    [property: JsonPropertyName("lineage_node_ids")] IReadOnlyList<string> LineageNodeIds,
    [property: JsonPropertyName("artifact_refs")] IReadOnlyList<string>? ArtifactRefs);

internal sealed record RouteReplayRouteInput(
    [property: JsonPropertyName("ranking_source")] string RankingSource,
    [property: JsonPropertyName("path")] string Path,
    [property: JsonPropertyName("cost")] double Cost,
    [property: JsonPropertyName("risk_flags")] IReadOnlyList<string>? RiskFlags);

internal sealed record RouteReplayEpisodeInput(
    [property: JsonPropertyName("query_text")] string QueryText,
    [property: JsonPropertyName("occurred_at")] string OccurredAt,
    [property: JsonPropertyName("weak_result")] bool WeakResult,
    [property: JsonPropertyName("used_fallback")] bool UsedFallback,
    [property: JsonPropertyName("winning_target_ref")] string WinningTargetRef,
    [property: JsonPropertyName("top_target_refs")] IReadOnlyList<string> TopTargetRefs);

/// <summary>
/// One output line written to the replay output JSONL file.
/// </summary>
internal sealed record RouteReplayOutput(
    [property: JsonPropertyName("case_index")] int CaseIndex,
    [property: JsonPropertyName("query")] string Query,
    [property: JsonPropertyName("baseline_hits")] IReadOnlyList<string> BaselineHits,
    [property: JsonPropertyName("replay_hits")] IReadOnlyList<string> ReplayHits,
    /// <summary>
    /// Full measured retrieval surface from the replay run: lesson IDs from result.Hits
    /// followed by SourceRefs from result.ShortTermHits. Acceptance checks must use this
    /// field, not replay_hits alone, because short-term scoped winners are only visible here.
    /// </summary>
    [property: JsonPropertyName("replay_surface")] IReadOnlyList<string> ReplaySurface,
    [property: JsonPropertyName("baseline_weak")] bool BaselineWeak,
    [property: JsonPropertyName("replay_weak")] bool ReplayWeak,
    [property: JsonPropertyName("baseline_scope_lens")] string BaselineScopeLens,
    [property: JsonPropertyName("replay_scope_lens")] string ReplayScopeLens,
    [property: JsonPropertyName("top1_changed")] bool Top1Changed,
    [property: JsonPropertyName("top1_baseline")] string? Top1Baseline,
    [property: JsonPropertyName("top1_replay")] string? Top1Replay,
    /// <summary>
    /// One of: "no-change", "reorder", "improved-weak", "regressed-weak", "top1-promoted", "top1-demoted".
    /// "top1-promoted" means the replay top1 is in expected_refs (or expected_refs absent and top1 changed).
    /// "top1-demoted" means the replay top1 is NOT in expected_refs but baseline top1 was.
    /// </summary>
    [property: JsonPropertyName("delta")] string Delta,
    /// <summary>
    /// True when at least one ref in replay_surface is also in the record's expected_refs.
    /// Null when expected_refs was not provided in the input record.
    /// </summary>
    [property: JsonPropertyName("expected_refs_hit")] bool? ExpectedRefsHit,
    /// <summary>
    /// Explain paths from the replay hit list (inspectability contract).
    /// Contains "route-memory:reuse(...)" or "route-memory:suppress(...)" when bias was applied.
    /// </summary>
    [property: JsonPropertyName("replay_explain_paths")] IReadOnlyList<string> ReplayExplainPaths,
    /// <summary>
    /// True when at least one replay hit's explain path contains the "route-memory:" signal,
    /// confirming the bias was applied and is observable.
    /// </summary>
    [property: JsonPropertyName("route_memory_signal_present")] bool RouteMemorySignalPresent);
