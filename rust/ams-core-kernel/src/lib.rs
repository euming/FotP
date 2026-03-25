pub mod atlas;
pub mod atlas_multi_scale;
pub mod dream;
pub mod dream_cluster;
pub mod dream_generate_md;
pub mod dream_shortcut;
pub mod resolution;
pub mod bugreport;
pub mod policy;
pub mod cache;
pub mod callstack;
pub mod active_inference;
pub mod agent_pool;
pub mod swarm_dag;
pub mod agent_query;
pub mod fep_bootstrap;
pub mod fep_cache_signal;
pub use fep_cache_signal::{
    cache_signal_stats, cache_signal_cluster_surprise, cache_signal_cluster_surprise_map,
    emit_cache_signal, fep_cache_report,
    CacheSignalStatsResult, ClusterSurpriseEntry, ClusterSurpriseResult, ToolSignalStats,
    FepCacheReport, DreamSchedulePreviewEntry, CacheReportRecommendation,
    CACHE_SIGNAL_KIND, CACHE_SIGNAL_TOOL_NAME, DEFAULT_WINDOW_HOURS,
};
pub mod freshness;
pub mod context;
pub mod corpus;
pub mod corpus_inspect;
pub mod importer;
pub mod inspect;
pub mod invariants;
pub mod lesson_retrieval;
pub mod log;
pub mod model;
pub mod operator_inspect;
pub mod parity;
pub mod persistence;
pub mod projdir;
pub mod knowledge_entry;
pub use knowledge_entry::{
    ke_write, ke_read, ke_search, ke_context, ke_bootstrap, ke_check_freshness,
    KeEntry, KeWriteRequest, KeWriteResult, KeReadResult,
    KeSearchResult, KeSearchHit, KeContextResult, KeBootstrapResult,
    KE_OBJECT_KIND, KE_ROOT_BUCKET, KE_SCOPE_PREFIX, KE_KIND_PREFIX,
    KE_STALE_BUCKET, VALID_KINDS,
};
pub mod retrieval;
pub mod retrieval_inspect;
pub mod route_memory;
pub mod route_replay;
pub mod short_term;
pub mod search_cache;
pub mod swarm_plan_store;
pub use swarm_plan_store::{migrate_swarm_plan_store, SwarmPlanMigrateResult};
pub mod session_gc;
pub use session_gc::{
    create_session_tombstone, format_prune_check_result, session_prune_batch,
    session_prune_check, session_prune_safe, session_tombstone_expire,
    ClusterPruneInfo, SessionPruneBatchResult, SessionPruneCheckResult,
    SessionPruneSafeResult, SessionTombstoneExpireResult, SessionTombstoneResult,
    SESSION_TOMBSTONE_KIND,
};
pub mod shadow;
pub mod smartlist_write;
pub mod store;
pub mod taskgraph_write;
pub mod tool_anomaly;
pub mod tool_outcome;
pub mod write_service;

pub use active_inference::{
    apply_belief_delta, apply_rlhf_belief_delta, cache_prior, compute_belief_delta,
    compute_epistemic_value, compute_expected_free_energy, compute_free_energy,
    compute_hierarchical_free_energy, compute_relevance_free_energy, extract_stereotype_prior,
    hierarchical_belief_update, load_cached_prior, predict, relevance_observation,
    AttributePrior, BeliefDelta, GaussianParam, StereotypePrior,
    AUTOMATIC_PRECISION, RLHF_PRECISION,
};
pub use fep_bootstrap::{
    bootstrap_agent_tool_priors, bootstrap_relevance_priors, classify_session_feedback,
    classify_user_feedback, decay_agent_tool_priors, get_relevance_prior,
    load_agent_tool_priors_from_snapshot, run_fep_bootstrap,
    write_agent_tool_priors_to_snapshot, write_relevance_priors_to_snapshot,
    FepBootstrapReport, UserFeedbackSignal,
    bootstrap_tool_duration_priors, compute_duration_free_energy,
    write_tool_duration_priors_to_snapshot, load_tool_duration_priors_from_snapshot,
    detect_slow_tools, ToolDurationPrior, SlowToolEntry,
};
pub use freshness::{
    build_freshness_positions, build_frozen_only_object_ids, build_object_topic_tokens,
    freshness_lane_boost, is_deep_memory_query, is_freshness_internal_object, is_freshness_internal_path,
    prepare_snapshot_freshness, touch_snapshot_freshness, FreshnessObjectPosition, FreshnessPrepareResult,
    FreshnessTouchResult, FreshnessWriteAction, FRESHNESS_LANE_ROOT_PATH, FRESHNESS_SMARTLIST_ROOT_PATH,
    FRESHNESS_STATUS_ACTIVE, FRESHNESS_STATUS_FROZEN, FRESHNESS_STATUS_HISTORICAL,
};
pub use agent_query::{
    run_agent_query, AgentFallbackSummary, AgentQueryDiagnostics, AgentQueryExplain, AgentQueryHit,
    AgentQueryRequest, AgentQueryResult, AgentQueryScoreBreakdown,
};
pub use context::{build_query_context, render_context, LineageScope, QueryContext, QueryContextOptions};
pub use corpus::{
    import_materialized_corpus, BinderRecord, CardPayloadRecord, CardRecord, CardState,
    MaterializedCorpus, TagLinkMeta, TagLinkRecord,
};
pub use corpus_inspect::{corpus_summary, list_binders, list_cards, show_binder, show_card};
pub use importer::{import_snapshot_file, resolve_snapshot_input_path};
pub use inspect::{
    diff_snapshots, list_containers, list_link_nodes, list_objects, show_container, show_link_node,
    show_memberships, show_object, TraversalDirection,
};
pub use invariants::{validate_invariants, InvariantViolation};
pub use lesson_retrieval::{rank_snapshot_lessons, RankedLessonSurface};
pub use log::{append_log_entry, replay_log, MutationLogEntry};
pub use model::{
    AbsoluteSemantics, AmsSnapshot, ContainerPolicies, ContainerRecord, ExpectationMetadata,
    HypothesisAnnotation, LinkNodeRecord, ObjectRecord, SemanticPayload,
};
pub use operator_inspect::{list_sessions, show_session, smartlist_inspect, thread_status};
pub use parity::{load_parity_cases, run_parity_validation, write_parity_reports, ParityCase, ParityReport};
pub use persistence::{deserialize_snapshot, serialize_snapshot};
pub use retrieval::{parse_binder_filters, parse_seed_card, query_cards, render_query_hits, EffectivePayload, QueryOptions, RetrievalHit, tokenize};
pub use retrieval_inspect::run_query_cards;
pub use route_memory::{
    append_route_episode_entry, build_frame_fingerprint, canonical_target_ref,
    classify_tool_outcome, compute_efe_biases, default_route_memory_path,
    load_route_episode_entries, load_route_replay_records, parse_target_card_id,
    RetrievalFrameFingerprint, RouteMemoryBiasOptions, RouteMemoryStore, RouteMemoryTargetBias,
    RouteReplayEpisodeEntry, RouteReplayEpisodeInput, RouteReplayFrameInput, RouteReplayRecord,
    RouteReplayRouteInput, ToolOutcome, UserFeedback,
};
pub use tool_anomaly::{
    detect_tool_anomalies, emit_anomaly_notes, ToolAnomaly, DEFAULT_ANOMALY_THRESHOLD,
};
pub use tool_outcome::{
    bootstrap_tool_outcome_priors, classify_agent_tool_outcome, compute_tool_outcome_free_energy,
    load_tool_outcome_priors_from_snapshot, predict_tool_outcome, update_tool_outcome_beliefs,
    write_tool_outcome_priors_to_snapshot, ToolOutcomeDistribution, ToolOutcomePrediction,
};
pub use route_replay::{load_and_run_route_replay, run_route_replay, write_route_replay_outputs, RouteReplayOutput};
pub use short_term::{select_short_term_hits, AgentShortTermHit};
pub use shadow::{
    load_shadow_cases, run_shadow_validation, write_shadow_reports, ShadowSurfaceSummary,
    ShadowValidationReport,
};
pub use smartlist_write::{
    attach_member as attach_smartlist_member, attach_member_before as insert_smartlist_member_before,
    attach_to_category, bootstrap_recency_ladder, browse_bucket, browse_category,
    browse_category_by_tier, browse_tier, categorize_inbox,
    create_bucket as create_smartlist_bucket, create_category, create_note as create_smartlist_note,
    default_note_id_for_mutation, detach_member as detach_smartlist_member,
    gc_sweep, get_bucket as get_smartlist_bucket, get_note as get_smartlist_note,
    get_rollup as get_smartlist_rollup, list_categories, list_memberships, list_recency_tiers,
    move_member as move_smartlist_member, normalize_path as normalize_smartlist_path,
    normalize_retrieval_visibility, rotate_recency_tiers,
    set_bucket_fields as set_smartlist_bucket_fields, set_ordering_policy,
    set_retrieval_visibility as set_smartlist_retrieval_visibility, set_rollup as set_smartlist_rollup,
    write_time_attach,
    SmartListAttachResult, SmartListBrowseItem, SmartListBucketInfo, SmartListCategorizationResult,
    SmartListCategoryInfo, SmartListDetachResult, SmartListGcRemoval, SmartListGcResult,
    SmartListMembershipsResult, SmartListMoveResult, SmartListNoteInfo, SmartListRecencyTierInfo,
    SmartListRollupChild, SmartListRollupInfo, SmartListRotationPromotion, SmartListRotationResult,
    SmartListVisibilityResult, BUCKET_OBJECT_KIND, CATEGORY_PREFIX, DURABLE_DURABILITY,
    DURABLE_ROOT_CONTAINER, INBOX_PATH, NOTE_OBJECT_KIND, RECENCY_FROZEN, RECENCY_LONG_TERM,
    RECENCY_MEDIUM_TERM, RECENCY_SHORT_TERM, RECENCY_TIERS, RETRIEVAL_VISIBILITY_DEFAULT,
    RETRIEVAL_VISIBILITY_KEY, RETRIEVAL_VISIBILITY_SCOPED, RETRIEVAL_VISIBILITY_SUPPRESSED,
    ROLLUP_OBJECT_KIND, SHORT_TERM_DURABILITY, SHORT_TERM_ROOT_CONTAINER, VALID_ORDERING_POLICIES,
};
pub use agent_pool::{
    allocate as agent_pool_allocate, release as agent_pool_release, status as agent_pool_status,
    AllocateResult as AgentPoolAllocateResult, ReleaseResult as AgentPoolReleaseResult,
    PoolStatus as AgentPoolStatus, PoolEntry as AgentPoolEntry,
    REGISTRY_PATH as AGENT_POOL_REGISTRY_PATH, FREE_PATH as AGENT_POOL_FREE_PATH,
    ALLOCATED_PATH as AGENT_POOL_ALLOCATED_PATH,
};
pub use cache::{
    create_artifact_object, ensure_source_cache_links_list, ensure_tool_cache_list,
    fresh_cache_entry, ghost_artifact, invalidate_artifact, lookup_exact,
    lookup_source_centric, lookup_tool_centric, new_artifact_id, promote_artifact,
    register_source, register_tool, revalidate_artifact, source_cache_links_path,
    stale_artifact, tool_cache_smartlist_path,
    CacheEntryMetadata, CacheHit, InvocationIdentity, InvalidationRequest,
    PromotionResult, SourceIdentity, ToolIdentity, ValidityState,
    ARTIFACT_OBJECT_KIND, INVOCATION_IDENTITY_OBJECT_KIND, SOURCE_OBJECT_KIND, TOOL_OBJECT_KIND,
};
pub use policy::{
    enforce_add_member_policies, set_container_policy, show_container_policy,
    AddMemberDecision, SetPolicyResult,
};
pub use model::{GraphShape, OverflowPolicy};
pub use atlas::{atlas_expand, atlas_page, atlas_search};
pub use dream::{dream_touch, dream_schedule, shortcut_path_for, DreamTouchResult, DreamScheduleResult, DREAM_SHORTCUTS_ROOT, DREAM_SHORTCUT_KIND, DREAMER_TOOL_ID};
pub use dream_cluster::{dream_cluster, DreamClusterResult, TopicCluster, DREAM_TOPICS_ROOT, DREAM_TOPIC_OBJECT_KIND, DEFAULT_MIN_JACCARD, DEFAULT_MAX_CLUSTERS};
pub use dream_generate_md::{dream_generate_md, DreamGenerateMdResult};
pub use dream_shortcut::{
    dream_shortcut, find_isolated_sessions,
    DreamShortcutResult, EmbeddingEntry, EmbeddingsSidecar, IsolatedSessionsResult,
    SHORTCUT_LINK_KIND, WS_SHORTCUTS_ROOT,
};
pub use resolution::{
    annotate_resolution_state, read_resolution_state, resolve_object, resolve_objects,
    RecoveryPath, ResolutionRequest, ResolutionResult, ResolutionState,
};
pub use atlas_multi_scale::{
    atlas_define, atlas_list, atlas_list_at_scale, atlas_navigate, atlas_show,
    render_atlas_info, render_navigation, render_scale_listing,
    AtlasInfo, AtlasNavigationLevel, AtlasNavigationResult, AtlasScaleEntry, AtlasScaleLevel,
    ATLAS_OBJECT_KIND,
};
pub use bugreport::{
    create_bugreport, create_bugfix, get_bugreport, get_bugfix,
    get_linked_fixes_for_bugreport, link_bugreport_to_fix,
    list_bugreports, list_bugfixes, search_bugreports,
    update_bugreport_status,
    BugFixInfo, BugReportInfo, CreateBugFixParams, CreateBugReportParams,
    BUGFIX_BUCKET_PATH, BUGFIX_OBJECT_KIND, BUGREPORT_OBJECT_KIND, DEFAULT_BUCKET_PATH as BUGREPORT_BUCKET_PATH,
    SEVERITY_CRITICAL, SEVERITY_HIGH, SEVERITY_LOW, SEVERITY_MEDIUM,
    STATUS_IN_REPAIR, STATUS_OPEN, STATUS_RESOLVED,
};
pub use callstack::{
    assert_mode,
    callstack_advance, callstack_complete_node, callstack_delete_node, callstack_enter_edit,
    callstack_enter_execute, callstack_interrupt, callstack_load_plan,
    callstack_move_node, callstack_observe, callstack_observe_at, callstack_pop, callstack_push,
    callstack_quarantined_push, callstack_rename_node, callstack_resume, callstack_set_depends_on,
    callstack_show,
    list_projects, park_project, ready_nodes as callstack_ready_nodes, read_plan_mode,
    record_tool_call, record_tool_call_with_duration, render_context_text, switch_project,
    repair_roots,
    BatchOp, run_batch,
    CallstackContext, CallstackFrame, CallstackOpResult, InterruptParams, PlanNodeDef, ProjectInfo,
    RepairRootsResult,
    EXECUTION_PLAN_ROOT,
};
pub use store::{AmsStore, StoreError};
pub use taskgraph_write::{
    archive_thread as archive_task_thread, checkpoint_active_thread as checkpoint_task_thread,
    claim_thread as claim_task_thread, heartbeat_thread_claim, inspect_task_graph,
    pop_thread as pop_task_thread, push_tangent as push_task_tangent,
    release_thread_claim, start_thread as start_task_thread, thread_list, TaskActiveClaimInfo,
    TaskArtifactInfo, TaskCheckpointInfo, TaskClaimCommandResult, TaskClaimInfo, TaskGraphCommandResult,
    TaskGraphOverview, TaskThreadInfo, TASK_GRAPH_ACTIVE_CONTAINER, TASK_GRAPH_PARKED_CONTAINER,
    TASK_GRAPH_ROOT_CONTAINER,
};
pub use write_service::{
    canonicalize_episodes, default_write_lock_path, default_write_log_path, default_write_state_path,
    ArchiveThreadRequest, AttachSmartListCategoryRequest, AttachSmartListMemberRequest,
    BootstrapRecencyLadderRequest, CategorizeInboxRequest, CheckpointThreadRequest, ClaimThreadRequest,
    CreateSmartListBucketRequest, CreateSmartListCategoryRequest, CreateSmartListNoteRequest,
    DetachSmartListMemberRequest, DreamTouchRequest, GcSweepRequest,
    HeartbeatThreadClaimRequest, InsertSmartListMemberBeforeRequest, MoveSmartListMemberRequest,
    MutationEnvelope, MutationKind, PopThreadRequest, PushTangentRequest, RecordRouteEpisodeRequest,
    ReleaseThreadClaimRequest, resolve_authoritative_snapshot_input, RotateRecencyTiersRequest,
    RouteStateComparison, SetSmartListBucketFieldsRequest, SetSmartListOrderingPolicyRequest,
    SetSmartListRollupRequest, SetSmartListVisibilityRequest,
    StartThreadRequest, WriteApplyResult, WriteBackendManifest, WriteBackendMode, WriteBackendStatus,
    AllocateAgentPoolRequest, ReleaseAgentPoolRequest,
    CreateBugreportRequest, UpdateBugreportStatusRequest, CreateBugfixRequest,
    WriteMutationPayload, WriteRecoveryReport, WriteResourceResult, WriteService, WriteServicePaths,
    WriteServiceState, WriteTimeAttachRequest,
};
pub use search_cache::{
    compute_corpus_version, normalize_query, search_cache_key,
    CorpusVersionResult, SEMANTIC_SEARCH_TOOL_ID,
};
