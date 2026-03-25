use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};

use ams_core_kernel::{
    default_route_memory_path, run_fep_bootstrap,
    bootstrap_agent_tool_priors, decay_agent_tool_priors, write_agent_tool_priors_to_snapshot,
    load_tool_outcome_priors_from_snapshot, predict_tool_outcome,
    append_log_entry, diff_snapshots, import_snapshot_file, list_containers, list_link_nodes, list_objects,
    list_sessions, replay_log, serialize_snapshot, show_container, show_link_node, show_memberships,
    show_object, show_session, smartlist_inspect, thread_list, thread_status, validate_invariants,
};
use ams_core_kernel::{
    default_note_id_for_mutation, load_parity_cases, run_parity_validation, write_parity_reports,
    run_agent_query, AgentQueryRequest, corpus_summary, import_materialized_corpus, list_binders, list_cards,
    load_and_run_route_replay, load_route_episode_entries, load_route_replay_records, show_binder, show_card,
    AmsStore, ArchiveThreadRequest, AttachSmartListCategoryRequest, AttachSmartListMemberRequest,
    BootstrapRecencyLadderRequest, CategorizeInboxRequest, CheckpointThreadRequest, ClaimThreadRequest,
    CreateSmartListBucketRequest, CreateSmartListCategoryRequest, CreateSmartListNoteRequest,
    DetachSmartListMemberRequest, GcSweepRequest,
    HeartbeatThreadClaimRequest, InsertSmartListMemberBeforeRequest, MoveSmartListMemberRequest,
    PopThreadRequest, PushTangentRequest, RecordRouteEpisodeRequest, ReleaseThreadClaimRequest,
    RotateRecencyTiersRequest, SetSmartListBucketFieldsRequest, SetSmartListOrderingPolicyRequest,
    StartThreadRequest,
    AllocateAgentPoolRequest, ReleaseAgentPoolRequest,
    RouteMemoryBiasOptions, RouteMemoryStore, SetSmartListRollupRequest, SetSmartListVisibilityRequest,
    SmartListRollupChild, TaskClaimCommandResult, WriteBackendStatus, WriteService, WriteTimeAttachRequest,
    WriteRecoveryReport, DreamTouchRequest, DreamScheduleResult, dream_schedule,
    dream_cluster, DreamClusterResult,
    dream_generate_md, DreamGenerateMdResult,
    dream_shortcut, find_isolated_sessions, EmbeddingsSidecar,
    resolve_authoritative_snapshot_input,
    FreshnessWriteAction,
    load_shadow_cases, run_shadow_validation, write_shadow_reports,
    run_query_cards, CardState, MutationLogEntry, QueryContextOptions, TraversalDirection,
    detect_tool_anomalies, emit_anomaly_notes, load_agent_tool_priors_from_snapshot,
    classify_agent_tool_outcome, update_tool_outcome_beliefs,
    ToolOutcome, DEFAULT_ANOMALY_THRESHOLD,
    bootstrap_tool_duration_priors,
    write_tool_duration_priors_to_snapshot, load_tool_duration_priors_from_snapshot,
    detect_slow_tools,
};
use ams_core_kernel::{
    browse_bucket, browse_category, browse_category_by_tier, browse_tier,
    list_categories, list_memberships, list_recency_tiers,
};
use ams_core_kernel::{
    get_bugreport, get_bugfix,
    list_bugreports, list_bugfixes, search_bugreports,
    BugFixInfo, BugReportInfo,
    CreateBugreportRequest, UpdateBugreportStatusRequest, CreateBugfixRequest,
};
use ams_core_kernel::{set_container_policy, show_container_policy};
use ams_core_kernel::{
    register_tool as cache_register_tool, register_source as cache_register_source,
    promote_artifact, lookup_tool_centric, lookup_source_centric,
    invalidate_artifact, revalidate_artifact, stale_artifact,
    ToolIdentity, SourceIdentity, InvocationIdentity, InvalidationRequest, ValidityState,
    CacheHit,
};
use ams_core_kernel::{
    assert_mode,
    callstack_advance, callstack_complete_node, callstack_delete_node, callstack_enter_edit,
    callstack_enter_execute, callstack_interrupt, callstack_load_plan,
    callstack_move_node, callstack_observe, callstack_observe_at, callstack_pop, callstack_push,
    callstack_quarantined_push, callstack_rename_node, callstack_resume, callstack_set_depends_on,
    callstack_show,
    list_projects, park_project, record_tool_call, record_tool_call_with_duration,
    render_context_text, run_batch, switch_project,
    BatchOp, CallstackOpResult, InterruptParams, PlanNodeDef,
};
use ams_core_kernel::callstack::ready_nodes;
use ams_core_kernel::{
    create_session_tombstone, format_prune_check_result, session_prune_batch,
    session_prune_check, session_prune_safe, session_tombstone_expire,
};
use ams_core_kernel::model::now_fixed;
use ams_core_kernel::{atlas_expand, atlas_page, atlas_search};
use ams_core_kernel::{
    atlas_define, atlas_list, atlas_list_at_scale, atlas_navigate, atlas_show,
    render_atlas_info, render_navigation, render_scale_listing,
};
use ams_core_kernel::{
    read_resolution_state, resolve_object,
    ResolutionRequest,
};
use ams_core_kernel::projdir::{format_context, format_search, format_stats_table, format_tree, projdir_build_dirs, projdir_build_file_pages, projdir_context, projdir_doc, projdir_ingest, projdir_register_atlas, projdir_search, projdir_stats, projdir_tree};
use ams_core_kernel::knowledge_entry::{ke_write, ke_read, ke_search, ke_bootstrap, ke_context as ke_context_fn, KeWriteRequest};
use ams_core_kernel::{compute_corpus_version, search_cache::search_cache_key, search_cache::SEMANTIC_SEARCH_TOOL_ID, tool_cache_smartlist_path};
use ams_core_kernel::{cache_signal_stats, cache_signal_cluster_surprise, emit_cache_signal, fep_cache_report, CacheReportRecommendation};
use ams_core_kernel::normalize_smartlist_path;
use ams_core_kernel::migrate_swarm_plan_store;
use ams_core_kernel::repair_roots;

#[derive(Debug, Parser)]
#[command(name = "ams-core-kernel")]
#[command(about = "Standalone Rust AMS.Core substrate kernel CLI/importer.")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    ValidateSnapshot {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
    },
    RoundtripSnapshot {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    ReplayLog {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    AppendLog {
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        entry: String,
    },
    Stress {
        #[arg(long, default_value_t = 4)]
        containers: usize,
        #[arg(long, default_value_t = 1000)]
        iterations: usize,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    ListObjects {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        kind: Option<String>,
    },
    ShowObject {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        id: String,
    },
    ListContainers {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        kind: Option<String>,
    },
    ListLinkNodes {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        container_id: Option<String>,
        #[arg(long)]
        object_id: Option<String>,
    },
    ShowContainer {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        id: String,
        #[arg(long, value_enum, default_value_t = CliTraversalDirection::Both)]
        direction: CliTraversalDirection,
    },
    ShowLinkNode {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        id: String,
    },
    Memberships {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        object_id: String,
    },
    ListSessions {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        since: Option<String>,
        #[arg(long, default_value_t = 20)]
        n: usize,
    },
    ShowSession {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        id: String,
    },
    ThreadStatus {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
    },
    ThreadStart {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        title: String,
        #[arg(long)]
        current_step: String,
        #[arg(long)]
        next_command: String,
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        branch_off_anchor: Option<String>,
        #[arg(long)]
        artifact_ref: Option<String>,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    ThreadPushTangent {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        title: String,
        #[arg(long)]
        current_step: String,
        #[arg(long)]
        next_command: String,
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        branch_off_anchor: Option<String>,
        #[arg(long)]
        artifact_ref: Option<String>,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    ThreadCheckpoint {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        current_step: String,
        #[arg(long)]
        next_command: String,
        #[arg(long)]
        branch_off_anchor: Option<String>,
        #[arg(long)]
        artifact_ref: Option<String>,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    ThreadArchive {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    ThreadPop {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    ThreadList {
        #[arg(long)]
        input: PathBuf,
    },
    BackendStatus {
        #[arg(long)]
        input: PathBuf,
    },
    BackendRecoverValidate {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        assert_clean: bool,
    },
    ThreadClaim {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        agent: String,
        #[arg(long, default_value_t = 300)]
        lease_seconds: i64,
        #[arg(long)]
        claim_token: Option<String>,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    ThreadHeartbeat {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        agent: String,
        #[arg(long)]
        claim_token: String,
        #[arg(long, default_value_t = 300)]
        lease_seconds: i64,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    ThreadRelease {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        agent: String,
        #[arg(long)]
        claim_token: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    SmartlistInspect {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        path: String,
        #[arg(long, default_value_t = 3)]
        depth: usize,
    },
    SnapshotDiff {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        left: PathBuf,
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        right: PathBuf,
    },
    CorpusSummary {
        /// Accepts a '.memory.jsonl' store path and materializes the paired AMS snapshot when present.
        #[arg(long)]
        input: PathBuf,
    },
    ListCards {
        /// Accepts a '.memory.jsonl' store path and materializes the paired AMS snapshot when present.
        #[arg(long)]
        input: PathBuf,
        #[arg(long, value_enum)]
        state: Option<CliCardState>,
    },
    ShowCard {
        /// Accepts a '.memory.jsonl' store path and materializes the paired AMS snapshot when present.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        id: String,
    },
    ListBinders {
        /// Accepts a '.memory.jsonl' store path and materializes the paired AMS snapshot when present.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        contains: Option<String>,
    },
    ShowBinder {
        /// Accepts a '.memory.jsonl' store path and materializes the paired AMS snapshot when present.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        id: String,
    },
    QueryCards {
        /// Accepts a '.memory.jsonl' store path and materializes the paired AMS snapshot when present.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        q: String,
        #[arg(long, default_value_t = 10)]
        top: usize,
        #[arg(long)]
        binder: Option<String>,
        #[arg(long)]
        seed_card: Option<String>,
        #[arg(long, value_enum)]
        state: Option<CliCardState>,
        #[arg(long)]
        include_retracted: bool,
        #[arg(long)]
        explain: bool,
        #[arg(long)]
        current_node: Option<String>,
        #[arg(long)]
        parent_node: Option<String>,
        #[arg(long)]
        grandparent_node: Option<String>,
        #[arg(long)]
        role: Option<String>,
        #[arg(long)]
        mode: Option<String>,
        #[arg(long)]
        failure_bucket: Option<String>,
        #[arg(long)]
        artifact: Option<String>,
        #[arg(long, default_value_t = 3)]
        traversal_budget: usize,
        #[arg(long)]
        no_active_thread_context: bool,
        #[arg(long)]
        route_replay: Option<PathBuf>,
        #[arg(long, default_value_t = 1.0)]
        bias_scale: f64,
        #[arg(long, default_value_t = 1)]
        min_strong_wins: usize,
        #[arg(long, default_value_t = 0.0001)]
        min_bias: f64,
        #[arg(long, default_value_t = 16)]
        max_episodes: usize,
    },
    AgentQuery {
        /// Accepts a '.memory.jsonl' store path and materializes the paired AMS snapshot when present.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        q: String,
        #[arg(long, default_value_t = 8)]
        top: usize,
        #[arg(long)]
        explain: bool,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        binder: Option<String>,
        #[arg(long)]
        seed_card: Option<String>,
        #[arg(long, value_enum)]
        state: Option<CliCardState>,
        #[arg(long)]
        include_retracted: bool,
        #[arg(long)]
        current_node: Option<String>,
        #[arg(long)]
        parent_node: Option<String>,
        #[arg(long)]
        grandparent_node: Option<String>,
        #[arg(long)]
        role: Option<String>,
        #[arg(long)]
        mode: Option<String>,
        #[arg(long)]
        failure_bucket: Option<String>,
        #[arg(long)]
        artifact: Option<String>,
        #[arg(long, default_value_t = 3)]
        traversal_budget: usize,
        #[arg(long)]
        no_active_thread_context: bool,
        #[arg(long)]
        route_replay: Option<PathBuf>,
        #[arg(long, default_value_t = 1.0)]
        bias_scale: f64,
        #[arg(long, default_value_t = 1)]
        min_strong_wins: usize,
        #[arg(long, default_value_t = 0.0001)]
        min_bias: f64,
        #[arg(long, default_value_t = 16)]
        max_episodes: usize,
        #[arg(long)]
        record_route: bool,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        no_legacy_route_mirror: bool,
        #[arg(long)]
        include_latent: bool,
    },
    RecordRouteEpisode {
        /// Accepts a '.memory.jsonl' store path and records into the authoritative Rust write log.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        mutation_id: String,
        #[arg(long)]
        actor_id: String,
        #[arg(long)]
        episode_json: Option<String>,
        #[arg(long)]
        episode_file: Option<PathBuf>,
        #[arg(long)]
        expected_version: Option<u64>,
        #[arg(long)]
        mirror_legacy: bool,
    },
    RouteStateCompare {
        /// Accepts a '.memory.jsonl' store path and compares authoritative route state against the legacy sidecar.
        #[arg(long)]
        input: PathBuf,
    },
    SmartlistCreate {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        path: String,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
        #[arg(long)]
        durable: bool,
    },
    SmartlistNote {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        title: String,
        #[arg(long)]
        text: String,
        #[arg(long)]
        buckets: Option<String>,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
        #[arg(long)]
        note_id: Option<String>,
        #[arg(long)]
        durable: bool,
    },
    SmartlistAttach {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        path: String,
        #[arg(long)]
        member_ref: String,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    SmartlistAttachBefore {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        path: String,
        #[arg(long)]
        member_ref: String,
        #[arg(long)]
        before_member_ref: String,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    SmartlistDetach {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        path: String,
        #[arg(long)]
        member_ref: String,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    SmartlistMove {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        source_path: String,
        #[arg(long)]
        target_path: String,
        #[arg(long)]
        member_ref: String,
        #[arg(long)]
        before_member_ref: Option<String>,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    SmartlistBucketSet {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        path: String,
        #[arg(long)]
        field: Vec<String>,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    SmartlistRollup {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        path: String,
        #[arg(long)]
        summary: String,
        #[arg(long)]
        scope: String,
        #[arg(long)]
        stop_hint: Option<String>,
        #[arg(long)]
        child_highlight: Vec<String>,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
        #[arg(long)]
        durable: bool,
    },
    SmartlistVisibility {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        path: String,
        #[arg(long)]
        visibility: String,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        recursive: bool,
        #[arg(long)]
        include_notes: bool,
        #[arg(long)]
        include_rollups: bool,
    },
    /// List all SmartList bucket memberships for an object.
    SmartlistMemberships {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        object_id: String,
    },
    /// Create a category bucket at smartlist/category/<name>.
    SmartlistCategoryCreate {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    /// List all categories.
    SmartlistCategoryList {
        #[arg(long)]
        input: PathBuf,
    },
    /// Attach an object to a category.
    SmartlistCategoryAttach {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        object_id: String,
        #[arg(long)]
        category: String,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    /// Set ordering policy on a bucket.
    SmartlistSetOrdering {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        path: String,
        #[arg(long)]
        policy: String,
        #[arg(long, default_value = "asc")]
        direction: String,
        #[arg(long)]
        tie_breaker: Option<String>,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    /// Browse bucket members with ordering applied.
    SmartlistBrowse {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        category: Option<String>,
        #[arg(long)]
        tier: Option<String>,
    },
    /// Bootstrap or list recency tier buckets.
    SmartlistRecencyTiers {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        bootstrap: bool,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    /// Run rotation sweep across recency tiers.
    SmartlistRotate {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    /// Categorize inbox objects by kind.
    SmartlistCategorize {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    /// Run GC sweep to remove expired memberships.
    SmartlistGc {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        dry_run: bool,
        #[arg(long, default_value_t = 0)]
        default_ttl_hours: u64,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    /// Auto-attach an object to inbox + recency + kind-based categories.
    SmartlistWriteAttach {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        object_id: String,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    /// Atomic dream-touch primitive: promote a focal object to the head of each
    /// of its SmartList memberships and populate a shortcut bucket with all
    /// co-members.  This converts multi-hop System-2 query paths into
    /// single-hop System-1 Atlas lookups.
    DreamTouch {
        /// Snapshot / write-backend input path (same as other write commands).
        #[arg(long)]
        input: PathBuf,
        /// Object ID of the focal object to touch.
        #[arg(long)]
        object_id: String,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    /// Schedule and run dream-touch over all SmartLists that need it, prioritised
    /// by FEP surprise signal.  Skips lists with a valid cached dreamer:v1
    /// artifact; processes cache misses first, then stale artifacts ordered by age.
    DreamSchedule {
        /// Snapshot / write-backend input path (same as other read commands).
        #[arg(long)]
        input: PathBuf,
        /// Maximum number of SmartLists to touch in this run (default: 100).
        #[arg(long, default_value = "100")]
        max_touches: usize,
        #[arg(long)]
        created_by: Option<String>,
    },
    /// Discover topic clusters from relationship topology and materialise them
    /// as Atlas SmartLists.  Sessions that share many SmartList containers are
    /// grouped into clusters; each cluster gets a `dream_topic` anchor object
    /// and a per-cluster SmartList.  A root index SmartList at
    /// `smartlist/dream-topics` lists all topics ranked by cluster size.
    DreamCluster {
        /// Snapshot / write-backend input path.
        #[arg(long)]
        input: PathBuf,
        /// Minimum Jaccard similarity for two sessions to be placed in the
        /// same cluster (default: 0.3).
        #[arg(long, default_value = "0.3")]
        min_jaccard: f64,
        /// Maximum number of clusters to emit (default: 50).
        #[arg(long, default_value = "50")]
        max_clusters: usize,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    /// Find all sessions that are topologically isolated in the dream-topics
    /// Atlas — i.e. they belong to fewer than 2 topic clusters.  Isolated
    /// sessions are the primary targets for Watts-Strogatz shortcut injection.
    /// Prints one session GUID per line to stdout.
    DreamFindIsolated {
        /// Snapshot / write-backend input path (read-only).
        #[arg(long)]
        db: PathBuf,
    },
    /// Watts-Strogatz shortcut linker: for each topologically isolated session,
    /// find its nearest topic cluster by cosine similarity and inject a shortcut
    /// link, collapsing graph diameter from O(N/k) to O(log N).
    /// Reads an embeddings sidecar (.embeddings.json) produced by embed-dream-cards.py.
    DreamShortcut {
        /// Snapshot / write-backend input path (will be mutated and re-saved).
        #[arg(long)]
        input: PathBuf,
        /// Path to the embeddings sidecar JSON (e.g. foo.memory.embeddings.json).
        #[arg(long)]
        embeddings: PathBuf,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    /// Read the topology-based dream cluster Atlas (built by dream-cluster) and
    /// write CLAUDE.local.md.  Replaces the Python generate-claude-md.py script
    /// for topology-based dream output.
    DreamGenerateMd {
        /// Snapshot / write-backend input path.
        #[arg(long)]
        input: PathBuf,
        /// Output path for the generated Markdown file (e.g. CLAUDE.local.md).
        #[arg(long)]
        out: PathBuf,
        /// Maximum number of topic clusters to include (default: 10).
        #[arg(long, default_value = "10")]
        max_topics: usize,
        /// Maximum number of recent sessions to include (default: 20).
        #[arg(long, default_value = "20")]
        max_sessions: usize,
    },
    ParityValidate {
        /// Accepts a '.memory.jsonl' store path and materializes the paired AMS snapshot when present.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        cases: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    ShadowValidate {
        /// Accepts a '.memory.jsonl' store path and materializes the paired AMS snapshot when present.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        cases: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        memoryctl_exe: Option<PathBuf>,
        #[arg(long)]
        assert_match: bool,
    },
    RouteReplay {
        /// Accepts a '.memory.jsonl' store path and materializes the paired AMS snapshot when present.
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        replay: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 10)]
        top: usize,
        #[arg(long, default_value_t = 1.0)]
        bias_scale: f64,
        #[arg(long, default_value_t = 1)]
        min_strong_wins: usize,
        #[arg(long, default_value_t = 0.0001)]
        min_bias: f64,
        #[arg(long, default_value_t = 16)]
        max_episodes: usize,
    },
    /// Bootstrap FEP relevance priors from route episode history into an AMS snapshot.
    FepBootstrap {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Route episodes JSONL file (route-memory format).
        #[arg(long)]
        episodes: PathBuf,
        /// Output snapshot path (defaults to input path with '.bootstrapped' suffix).
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Inspect tool outcome priors from a snapshot.
    ToolOutcomePriors {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
    },
    /// Predict tool outcome for a given context.
    PredictToolOutcome {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Scope lens (e.g. "local-first-lineage").
        #[arg(long)]
        scope_lens: String,
        /// Agent role (e.g. "implementer").
        #[arg(long)]
        agent_role: String,
    },
    /// Bootstrap FEP agent tool-call priors from tool-call objects in a snapshot.
    FepBootstrapAgentTools {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Output snapshot path (defaults to input path with '.agent-tools.ams.json' suffix).
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Detect anomalous tool-call outcomes using FEP free-energy against bootstrapped priors.
    FepDetectToolAnomalies {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// ISO 8601 timestamp; only tool-calls created after this time are evaluated.
        #[arg(long)]
        since: String,
        /// Free-energy threshold above which a tool-call is considered anomalous.
        #[arg(long, default_value_t = DEFAULT_ANOMALY_THRESHOLD)]
        threshold: f64,
        /// Output snapshot path (writes SmartList notes for anomalies).
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Update a tool's FEP outcome belief after a successful repair.
    FepUpdateToolBelief {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Tool name whose prior should be updated (e.g. "Bash", "Read").
        #[arg(long)]
        tool_name: String,
        /// Observed outcome to shift beliefs toward (default: Success).
        #[arg(long, default_value = "Success")]
        outcome: String,
        /// Precision of the belief update (higher = stronger shift). Default: 1.0.
        #[arg(long, default_value_t = 1.0)]
        precision: f64,
        /// Output snapshot path (defaults to overwriting input).
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Batch-update agent tool beliefs from recent tool-call objects.
    ///
    /// Walks all tool-call objects created after --since, classifies each,
    /// and applies Bayesian belief updates to the per-tool prior distributions.
    FepUpdateAgentToolBeliefs {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// ISO 8601 timestamp; only tool-calls created after this time are processed.
        #[arg(long)]
        since: String,
        /// Precision for each belief update (higher = stronger shift). Default: 1.0.
        #[arg(long, default_value_t = 1.0)]
        precision: f64,
        /// Output snapshot path (defaults to overwriting input).
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Apply precision decay to agent tool priors, increasing uncertainty on stale priors.
    FepDecayToolPriors {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Decay rate (0.0-1.0). Higher values decay faster toward default uncertainty.
        #[arg(long, default_value_t = 0.1)]
        decay_rate: f64,
        /// Output snapshot path (defaults to overwriting input).
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Bootstrap per-tool Gaussian duration priors from tool-call objects in a snapshot.
    ///
    /// Walks all `tool-call` objects that have a `duration_s` provenance field,
    /// fits a Gaussian (mean, variance) for each tool, and persists the priors
    /// in the `fep:agent-tool-duration-priors` container.
    FepBootstrapDurationPriors {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Output snapshot path (defaults to input path with '.duration-priors.ams.json' suffix).
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Detect slow tool calls whose duration exceeds the expected Gaussian prior.
    ///
    /// Loads bootstrapped duration priors, then walks recent tool-call objects
    /// and reports any whose observed `duration_s` produces a FEP free energy
    /// (Gaussian surprise) above the given threshold.
    FepDetectSlowTools {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// ISO 8601 timestamp; only tool-calls created after this time are evaluated.
        #[arg(long)]
        since: String,
        /// Free-energy threshold above which a call is considered slow. Default: 3.0.
        #[arg(long, default_value_t = 3.0_f64)]
        threshold: f64,
    },
    AgentPoolAllocate {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        agent_ref: String,
        #[arg(long)]
        task_path: String,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    AgentPoolRelease {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        agent_ref: String,
        #[arg(long)]
        task_path: String,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    AgentPoolStatus {
        #[arg(long)]
        input: PathBuf,
    },
    /// Convenience search: front-path retrieval (no latent), record-route on.
    Search {
        /// Search keywords (positional).
        keywords: Vec<String>,
        /// Corpus to query: all, project, claude, codex.
        #[arg(long, value_enum, default_value_t = CliCorpus::All)]
        corpus: CliCorpus,
        #[arg(long, default_value_t = 8)]
        top: usize,
        #[arg(long)]
        explain: bool,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        actor_id: Option<String>,
        /// Disable route recording.
        #[arg(long)]
        no_record_route: bool,
    },
    /// Convenience recall: latent-inclusive retrieval across corpus + factories.
    Recall {
        /// Search keywords (positional).
        keywords: Vec<String>,
        /// Corpus to query: all, project, claude, codex.
        #[arg(long, value_enum, default_value_t = CliCorpus::All)]
        corpus: CliCorpus,
        #[arg(long, default_value_t = 8)]
        top: usize,
        #[arg(long)]
        explain: bool,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        actor_id: Option<String>,
        /// Disable route recording.
        #[arg(long)]
        no_record_route: bool,
    },

    // ── Bug Report commands ────────────────────────────────────────────
    /// List bug reports, optionally filtered by status.
    BugreportList {
        #[arg(long)]
        input: PathBuf,
        /// Filter by status: open, in-repair, resolved.
        #[arg(long)]
        status: Option<String>,
    },
    /// Show a single bug report by ID.
    BugreportShow {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        id: String,
    },
    /// Create a new bug report.
    BugreportCreate {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        source_agent: String,
        #[arg(long)]
        parent_agent: String,
        #[arg(long)]
        error_output: String,
        #[arg(long, default_value = "")]
        stack_context: String,
        /// Comma-separated list of attempted fixes.
        #[arg(long)]
        attempted_fixes: Option<String>,
        /// Comma-separated list of reproduction steps.
        #[arg(long)]
        reproduction_steps: Option<String>,
        #[arg(long, default_value = "")]
        recommended_fix_plan: String,
        #[arg(long, default_value = "medium")]
        severity: String,
        #[arg(long)]
        durable: bool,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },
    /// Search bug reports by keyword.
    BugreportSearch {
        #[arg(long)]
        input: PathBuf,
        /// Search query.
        #[arg(long)]
        q: String,
        /// Filter by status.
        #[arg(long)]
        status: Option<String>,
    },
    /// Update the status of a bug report.
    BugreportUpdateStatus {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        id: String,
        /// New status: open, in-repair, resolved.
        #[arg(long)]
        status: String,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },

    // ── Bug Fix commands ───────────────────────────────────────────────
    /// List all bug fixes.
    BugfixList {
        #[arg(long)]
        input: PathBuf,
    },
    /// Show a single bug fix by ID.
    BugfixShow {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        id: String,
    },
    /// Create a new bug fix recipe.
    BugfixCreate {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        title: String,
        #[arg(long)]
        description: String,
        #[arg(long)]
        fix_recipe: String,
        /// Link to an existing bug report ID.
        #[arg(long)]
        linked_bugreport_id: Option<String>,
        #[arg(long)]
        durable: bool,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
        #[arg(long)]
        created_by: Option<String>,
    },

    // ── Policy-layer commands ─────────────────────────────────────────────
    /// Set a policy field on a container.
    /// Fields: unique_members, max_members, graph_shape, allow_multi_parent.
    PolicySet {
        #[arg(long)]
        input: PathBuf,
        /// Container ID to update (e.g. `smartlist-members:smartlist/my/list`).
        #[arg(long)]
        container_id: String,
        /// Policy field name: unique_members | max_members | graph_shape | allow_multi_parent.
        #[arg(long)]
        field: String,
        /// New value (true/false for booleans; integer or "none" for max_members;
        /// any/tree/dag for graph_shape).
        #[arg(long)]
        value: String,
        #[arg(long)]
        mutation_id: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Show current policies for a container.
    PolicyShow {
        #[arg(long)]
        input: PathBuf,
        /// Container ID to inspect.
        #[arg(long)]
        container_id: String,
    },

    // ── Swarm-Plan / Callstack commands ──────────────────────────────────
    /// Emit compact execution context (frames + observations) for the active project.
    SwarmPlanContext {
        #[arg(long)]
        input: PathBuf,
        #[arg(long, default_value_t = 2000)]
        max_chars: usize,
        /// Scope to a specific named project root.
        #[arg(long)]
        project: Option<String>,
    },
    /// List all project roots in the execution plan.
    SwarmPlanList {
        #[arg(long)]
        input: PathBuf,
    },
    /// Render the full callstack tree for the active (or named) project.
    SwarmPlanShow {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        project: Option<String>,
    },
    /// Push a new child execution node under the active node.
    SwarmPlanPush {
        #[arg(long)]
        input: PathBuf,
        /// Display name for the new node.
        name: String,
        #[arg(long)]
        description: Option<String>,
        /// Comma-separated sibling titles that must complete before this node is ready.
        #[arg(long)]
        depends_on: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Complete the active work or policy node and return to its parent.
    SwarmPlanPop {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        return_text: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Write an observation note to the active node (or a specific node via --node-path).
    SwarmPlanObserve {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        title: String,
        #[arg(long)]
        text: String,
        #[arg(long)]
        actor_id: Option<String>,
        /// Target a specific node instead of the active node (used by child-done).
        #[arg(long)]
        node_path: Option<String>,
    },
    /// Insert an interrupt before the active work node.
    SwarmPlanInterrupt {
        #[arg(long)]
        input: PathBuf,
        #[arg(long, default_value = "repair")]
        policy: String,
        #[arg(long, default_value = "")]
        reason: String,
        #[arg(long, default_value = "")]
        error_output: String,
        #[arg(long, default_value = "")]
        context: String,
        #[arg(long, default_value = "")]
        attempted_fix: String,
        #[arg(long, default_value = "")]
        repair_hint: String,
        #[arg(long, default_value = "")]
        subtask_hints: String,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Archive the active interrupt and resume its interrupted sibling.
    SwarmPlanResume {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Advance the cursor to the next ready node in the execution tree.
    SwarmPlanAdvance {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Switch the active project (parks all others).
    SwarmPlanSwitch {
        #[arg(long)]
        input: PathBuf,
        /// Name of the project to switch to.
        name: String,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Suspend the active project without completing it.
    SwarmPlanPark {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Complete a specific node by path (for parallel dispatch).
    SwarmPlanCompleteNode {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        node_path: String,
        #[arg(long)]
        return_text: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// List all nodes whose dependencies are satisfied and are ready for dispatch.
    SwarmPlanReadyNodes {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        project: Option<String>,
    },
    /// Load a JSON plan file into the callstack as a new project with dependency-wired nodes.
    SwarmPlanLoadPlan {
        #[arg(long)]
        input: PathBuf,
        /// Path to the JSON plan file ({project, description, nodes:[{title,description,depends_on}]}).
        #[arg(long)]
        file: PathBuf,
        /// Load nodes as children of the currently active node instead of creating a new project.
        #[arg(long, default_value_t = false)]
        into_active: bool,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Execute a batch of swarm-plan operations with a single lock acquisition.
    /// Reads a JSON array of operations from --ops (file path or "-" for stdin).
    SwarmPlanBatch {
        #[arg(long)]
        input: PathBuf,
        /// Path to JSON ops file, or "-" for stdin. Default: stdin.
        #[arg(long, default_value = "-")]
        ops: String,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Retroactively create missing `00-node` sub-buckets for all execution-plan
    /// roots that were loaded via the old `load-plan` path (which never created
    /// `00-node`). Idempotent: safe to run multiple times.
    SwarmPlanRepairRoots {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Migrate all execution-plan objects for a named project from a source
    /// snapshot into a new per-plan destination snapshot.
    /// The source is never modified.
    SwarmPlanMigrate {
        /// Source snapshot path (e.g. factories.memory.ams.json).
        #[arg(long)]
        from: PathBuf,
        /// Destination write-service store path (e.g. swarm-plans/p8-store-migration.memory.jsonl).
        /// Companion .memory.ams.json, empty .ams-write-log.jsonl, and .ams-write-state.json
        /// are created automatically.
        #[arg(long)]
        to: PathBuf,
        /// Execution-plan project name to migrate (e.g. p7-fep-cache-signal).
        #[arg(long)]
        project: String,
    },
    /// Transition the active project from execute mode to edit mode.
    SwarmPlanEnterEdit {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Transition the active project from edit mode to execute mode.
    SwarmPlanEnterExecute {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Push a new ready-state child node without touching the execution cursor (execute mode only).
    SwarmPlanQuarantinedPush {
        #[arg(long)]
        input: PathBuf,
        /// Display name for the new node.
        name: String,
        #[arg(long)]
        description: Option<String>,
        #[arg(long)]
        depends_on: Option<String>,
        #[arg(long)]
        parent_node_path: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Rename a node's title field (edit mode only).
    SwarmPlanRenameNode {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        node_path: String,
        #[arg(long)]
        new_title: String,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Delete a leaf node (no children) — edit mode only.
    SwarmPlanDeleteNode {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        node_path: String,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Set (replace) the depends_on CSV for a node — edit mode only.
    SwarmPlanSetDependsOn {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        node_path: String,
        /// Comma-separated sibling titles that must complete before this node.
        #[arg(long, default_value = "")]
        depends_on: String,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Move a node to a new parent within the same project — edit mode only.
    SwarmPlanMoveNode {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        node_path: String,
        #[arg(long)]
        new_parent_path: String,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Tag a completed swarm-plan to a project knowledge bucket by writing a receipt note.
    ///
    /// The note is written to the shared memory store (--input), not the per-plan store.
    /// It records the plan name, component bucket, and an optional summary so agents can
    /// navigate "what work touched this component?" via atlas-search or browse-bucket.
    SwarmPlanTag {
        /// Path to the shared memory write-service store (e.g. shared-memory/shared.memory.jsonl).
        #[arg(long)]
        input: PathBuf,
        /// Swarm-plan project name to tag (e.g. p3-incremental-dreaming).
        #[arg(long)]
        plan_name: String,
        /// Target SmartList bucket path in the project hierarchy (e.g. smartlist/project/ngm/dreaming).
        #[arg(long)]
        bucket_path: String,
        /// One-line summary of what the plan accomplished. Defaults to the plan name.
        #[arg(long)]
        summary: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },

    // ── Cache CLI commands ────────────────────────────────────────────────
    /// Register or update a Tool Identity Object and its cache SmartList.
    CacheRegisterTool {
        #[arg(long)]
        input: PathBuf,
        /// Stable tool identifier (e.g. "parser:v2").
        #[arg(long)]
        tool_id: String,
        /// Tool version string used for cache compatibility checks.
        #[arg(long)]
        tool_version: String,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Register or update a Source Identity Object and its cache-links SmartList.
    CacheRegisterSource {
        #[arg(long)]
        input: PathBuf,
        /// Stable AMS Object ID for the source.
        #[arg(long)]
        source_id: String,
        /// Optional fingerprint (e.g. content hash, ETag).
        #[arg(long)]
        fingerprint: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Promote a tool output to a cached artifact and attach it to both SmartLists.
    CachePromote {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        tool_id: String,
        #[arg(long)]
        tool_version: String,
        #[arg(long)]
        source_id: String,
        /// Optional source fingerprint.
        #[arg(long)]
        source_fingerprint: Option<String>,
        /// SHA-256 hex of the normalized parameter map, or "none".
        #[arg(long, default_value = "none")]
        param_hash: String,
        /// Optional external file-path reference for the artifact payload.
        #[arg(long)]
        in_situ_ref: Option<String>,
        /// Optional content hash of the artifact payload.
        #[arg(long)]
        artifact_fingerprint: Option<String>,
        #[arg(long)]
        actor_id: Option<String>,
    },
    /// Look up cached artifacts for a tool+source pair.
    CacheLookup {
        #[arg(long)]
        input: PathBuf,
        /// Lookup mode: "tool" (tool-centric) or "source" (source-centric).
        #[arg(long, default_value = "tool")]
        mode: String,
        #[arg(long)]
        tool_id: String,
        #[arg(long)]
        source_id: String,
        /// Optional param_hash to narrow the search.
        #[arg(long)]
        param_hash: Option<String>,
        /// Output format: "text" or "json".
        #[arg(long, default_value = "text")]
        format: String,
    },
    /// Change the validity state of a cached artifact.
    CacheInvalidate {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        artifact_id: String,
        /// New state: "invalidated", "stale", "ghosted", "lost", or "valid".
        #[arg(long)]
        state: String,
        /// Human-readable reason for invalidation.
        #[arg(long)]
        reason: Option<String>,
    },

    // ── Atlas CLI commands ────────────────────────────────────────────────────
    /// Render an Object page: metadata, SemanticPayload, and container members.
    AtlasPage {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Object ID or unambiguous prefix to render.
        #[arg(long)]
        id: String,
    },
    /// Keyword search across object summaries, tags, and IDs.
    AtlasSearch {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Space-separated keyword query.
        #[arg(long)]
        q: String,
        /// Maximum number of results to return.
        #[arg(long, default_value_t = 20)]
        top: usize,
    },
    /// Show containment relationships: containers this object belongs to and its direct children.
    AtlasExpand {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Object ID or unambiguous prefix to expand.
        #[arg(long)]
        id: String,
    },

    // ── Multi-scale Atlas commands ────────────────────────────────────────────
    /// Register or update a named multi-scale atlas.
    ///
    /// Each --scale argument is formatted as "N:path1,path2" where N is the
    /// scale index (0 = coarsest) and the paths are SmartList bucket paths.
    AtlasDefine {
        #[arg(long)]
        input: PathBuf,
        /// Unique atlas name (slug).
        #[arg(long)]
        name: String,
        /// Optional description.
        #[arg(long)]
        description: Option<String>,
        /// One or more scale level specs in the form "N:bucket/path[,bucket/path2]".
        #[arg(long = "scale", required = true)]
        scales: Vec<String>,
    },
    /// Show a registered atlas definition.
    AtlasShow {
        #[arg(long)]
        input: PathBuf,
        /// Atlas name to show.
        #[arg(long)]
        name: String,
    },
    /// List all registered atlases.
    AtlasList {
        #[arg(long)]
        input: PathBuf,
    },
    /// List objects visible at a given scale level of a named atlas.
    AtlasListAtScale {
        #[arg(long)]
        input: PathBuf,
        /// Atlas name.
        #[arg(long)]
        name: String,
        /// Scale level index (0 = coarsest).
        #[arg(long)]
        scale: u32,
    },
    /// Coarse-to-fine navigation: show at which scales an object appears in an atlas.
    AtlasNavigate {
        #[arg(long)]
        input: PathBuf,
        /// Atlas name.
        #[arg(long)]
        name: String,
        /// Object ID or unambiguous prefix.
        #[arg(long)]
        id: String,
    },

    // ── Resolution engine commands ────────────────────────────────────────────
    /// Attempt to resolve an object, using all recovery strategies.
    ///
    /// Outputs: state, resolved_object_id, recovery_path, explanation, revalidated.
    ResolutionResolve {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// The AMS Object ID to resolve.
        #[arg(long)]
        object_id: String,
        /// Tool ID hint for recovery (optional).
        #[arg(long)]
        tool_id: Option<String>,
        /// Source ID hint for recovery (optional).
        #[arg(long)]
        source_id: Option<String>,
        /// Param hash hint for recovery (optional).
        #[arg(long)]
        param_hash: Option<String>,
        /// Disable cache recovery.
        #[arg(long)]
        no_cache: bool,
        /// Disable historical-path recovery.
        #[arg(long)]
        no_historical: bool,
        /// Disable partial-reconstruction recovery.
        #[arg(long)]
        no_partial: bool,
        /// Disable content-addressed recovery.
        #[arg(long)]
        no_content_addressed: bool,
        /// If set, re-mark the artifact as Valid when recovery succeeds.
        #[arg(long)]
        revalidate_on_recovery: bool,
    },
    /// Show the resolution state annotation stored on an object.
    ResolutionShow {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// The AMS Object ID to inspect.
        #[arg(long)]
        object_id: String,
    },

    // ── ProjDir Atlas commands ────────────────────────────────────────────────
    /// Ingest git-tracked files as GNUISNGNU Objects (P4-A1).
    ///
    /// Walks all files returned by `git ls-files` in --repo-root, creates or
    /// updates a file Object for each, and promotes a Layer 4 cache artifact
    /// keyed on (path, mtime, size) so unchanged files are O(1) on re-run.
    ProjdirIngest {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Repository root to scan. Defaults to the current working directory.
        #[arg(long, default_value = ".")]
        repo_root: PathBuf,
    },

    /// Build SmartList directory hierarchy from existing file Objects (P4-A2).
    ///
    /// For each unique directory derived from `file:` Objects in the store:
    /// creates a SmartList bucket at `smartlist/projdir/<dir>`, a directory
    /// Object (`dir:<path>`), attaches file Objects to their parent bucket, and
    /// attaches subdirectory Objects to their parent bucket.
    ProjdirBuildDirs {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
    },

    /// Aggregate extension counts/sizes from file Objects and write stats Object (P4-A3).
    ///
    /// Reads all `file` Objects from the store, aggregates count and total size
    /// by extension, writes a `projdir-stats:overview` Object whose in_situ_ref
    /// is the formatted table, and populates `smartlist/projdir-stats` with one
    /// `ext-stats:<ext>` Object per extension sorted by count descending.
    ProjdirStats {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
    },

    /// Register (or refresh) the `projdir` Atlas with three scale levels (P4-B1/B3).
    ///
    /// Must be run after `projdir-ingest` and `projdir-build-dirs`. Enrolls:
    ///   scale 0 — `smartlist/projdir` (repo overview / root)
    ///   scale 1 — direct children of root (top-level directories)
    ///   scale 2 — per-file SmartList pages (`smartlist/projdir-file/<path>`)
    ///
    /// Internally calls `projdir-build-file-pages` before registering scale 2.
    ProjdirRegisterAtlas {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
    },

    /// Build per-file SmartList page buckets for Atlas scale 2 (P4-B3).
    ///
    /// For each `file:` Object in the store, creates a SmartList bucket at
    /// `smartlist/projdir-file/<path>` and attaches the file Object as its sole
    /// member. Idempotent; safe to run multiple times. Called automatically by
    /// `projdir-register-atlas`, but also available as a standalone command.
    ProjdirBuildFilePages {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
    },

    /// Print stored head content for a file in the projdir index (P4-B3).
    ///
    /// Looks up the file Object by normalized path and prints its `in_situ_ref`
    /// (the first 50 lines captured during `projdir-ingest`). Returns an error
    /// if the file is not in the index. Replaces `ams.bat proj-dir doc <path>`.
    ProjdirDoc {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Repo-relative file path (e.g. `src/lib.rs`).
        #[arg(long)]
        path: String,
    },

    /// Emit a compact onboarding context dump for the projdir Atlas (P4-D2).
    ///
    /// Prints a directory tree (up to `--depth` levels), extension file stats,
    /// and key markdown docs at depth ≤ 2. Replaces `ams.bat proj-dir context`.
    ProjdirContext {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Maximum tree depth (default 2).
        #[arg(long, default_value = "2")]
        depth: usize,
    },

    /// Render an indented directory tree from the projdir Atlas (P4-B2).
    ///
    /// Reads SmartList buckets built by `projdir-build-dirs` and recursively
    /// renders the directory tree up to `--depth` levels. Directories are shown
    /// as `<name>/`, files as `<name> [<size>]`. Replaces `ams.bat proj-dir tree`.
    ProjdirTree {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Repo-relative directory to start from. Defaults to the repo root.
        #[arg(long)]
        path: Option<String>,
        /// Maximum recursion depth (default 3).
        #[arg(long, default_value = "3")]
        depth: usize,
    },

    /// Compute a short stable corpus-version hash (P5-A1).
    ///
    /// Counts all session_ref / session Objects and finds the max created_at
    /// timestamp, then produces the first 12 hex chars of SHA256("{count}:{max_ms}").
    ///
    /// Output lines:
    ///   corpus_version=<12-hex-chars>
    ///   session_count=<N>
    SearchCorpusVersion {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
    },

    /// Look up a cached semantic search result (P5-B1).
    ///
    /// Normalises the query, computes the current corpus-version hash, builds
    /// the Layer 4 cache key (`query:<norm>:<corpus_version>`), and performs a
    /// tool-centric cache lookup for `tool=semantic-search:v1`.
    ///
    /// Output lines on a **cache hit**:
    ///   status=hit
    ///   artifact_id=<id>
    ///   text=<cached result text>
    ///
    /// Output lines on a **cache miss**:
    ///   status=miss
    ///   source_id=<key that was looked up>
    SearchCacheLookup {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Raw (un-normalised) query string.
        #[arg(long)]
        query: String,
    },

    /// Promote a semantic search result into the Layer 4 cache (P5-B2).
    ///
    /// Normalises the query, computes the current corpus-version hash, builds
    /// the Layer 4 source key (`query:<norm>:<corpus_version>`), and promotes
    /// an artifact for `tool=semantic-search:v1` with the given text payload.
    ///
    /// Output lines on success:
    ///   action=search-cache-promote
    ///   artifact_id=<id>
    ///   source_id=<key that was promoted>
    ///   corpus_version=<12-hex-chars>
    ///   snapshot=<path>
    SearchCachePromote {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Raw (un-normalised) query string.
        #[arg(long)]
        query: String,
        /// The result text to store as the cached artifact payload.
        #[arg(long)]
        text: String,
        #[arg(long)]
        actor_id: Option<String>,
    },

    /// Invalidate all semantic search cache entries for a given corpus version (P5-C4).
    ///
    /// Scans the `semantic-search:v1` tool cache SmartList and marks every artifact
    /// whose `source_id` ends with `:<corpus_version>` as stale.  Called by the
    /// ingest pipeline after new sessions are added so that cached search results
    /// produced against the old corpus are not returned to callers.
    ///
    /// Output lines:
    ///   action=search-cache-invalidate
    ///   corpus_version=<corpus_version>
    ///   invalidated=<N>
    SearchCacheInvalidate {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// The corpus version token to invalidate (e.g. the 12-hex-char hash from
        /// `search-corpus-version`).
        #[arg(long)]
        corpus_version: String,
    },

    /// Print aggregate statistics for the semantic search cache (P5-C5).
    ///
    /// Counts all valid artifacts in the `semantic-search:v1` tool cache SmartList
    /// and reports the current corpus version.
    ///
    /// Output lines:
    ///   action=search-cache-stats
    ///   corpus_version=<12-hex-chars>
    ///   session_count=<N>
    ///   cached_entries=<M>
    SearchCacheStats {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
    },

    /// Emit a FEP cache-signal `tool-call` Object recording a search cache hit or miss (P7-A1).
    ///
    /// Called by `semantic-query.py` immediately after a `search-cache-lookup` resolves.
    /// Writes a `tool-call` Object with `signal_kind=search-cache-signal` into the store.
    /// The dream-schedule reads these objects to compute per-cluster miss rates and
    /// prioritises re-dreaming high-miss clusters (P3↔P7 feedback loop).
    ///
    /// Output lines on success:
    ///   action=fep-cache-signal-emit
    ///   object_id=tool-call:<uuid>
    ///   cache_status=hit|miss
    ///   query_normalized=<normalised query>
    ///   corpus_version=<12-hex-chars>
    FepCacheSignalEmit {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Raw (un-normalised) query string that was searched.
        #[arg(long)]
        query: String,
        /// Whether the lookup was a cache hit (`true`) or miss (`false`).
        #[arg(long)]
        is_hit: bool,
        /// Actor ID to record in the signal provenance.
        #[arg(long)]
        actor_id: Option<String>,
    },

    /// Aggregate FEP cache signal statistics over a sliding time window (P7-A2).
    ///
    /// Scans all `tool-call` objects with `signal_kind=search-cache-signal` and
    /// computes per-tool hit/miss statistics.
    ///
    /// Output: one block per tool in key=value format:
    ///   action=fep-cache-signal-stats
    ///   tool=<name> hit_count=N miss_count=M total=K hit_rate=R consecutive_misses=C
    FepCacheSignalStats {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Only show stats for this tool name (default: all tools).
        #[arg(long)]
        tool: Option<String>,
        /// Sliding window in hours (default: 24).
        #[arg(long, default_value = "24")]
        window_hours: u32,
    },

    /// Rank dream-topic clusters by cache-miss surprise score (P7-B1).
    ///
    /// Joins cache signals within the window to cluster membership and computes
    /// a per-cluster surprise score: `miss_rate × ln(1 + miss_count)`.
    ///
    /// Output: one line per cluster (sorted by surprise descending):
    ///   action=fep-cache-signal-cluster-surprise
    ///   cluster_id=<id> sessions=N miss_rate=R surprise=S
    FepCacheSignalClusterSurprise {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Sliding window in hours (default: 24).
        #[arg(long, default_value = "24")]
        window_hours: u32,
        /// Only include clusters with at least this many signals.
        #[arg(long)]
        min_signals: Option<usize>,
    },

    /// Full FEP feedback-loop status report (P7-C3).
    ///
    /// Combines cache signal statistics, cluster surprise ranking, dream-schedule
    /// preview, and operator recommendations into a single human-readable report.
    ///
    /// Output sections:
    ///   === Cache Signal Summary ===
    ///   === Cluster Surprise Ranking ===
    ///   === Dream Schedule Preview ===
    ///   === Recommendations ===
    FepCacheReport {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Sliding window in hours (default: 24).
        #[arg(long, default_value = "24")]
        window_hours: u32,
    },

    /// Emit a tool-call object from a swarm agent's audit trail entry.
    ///
    /// Writes a `tool-call` Object into the store with full provenance including
    /// duration_s, enabling the FEP pipeline to analyse swarm tool performance.
    ///
    /// Output lines on success:
    ///   action=emit-tool-call
    ///   object_id=tool-call:<uuid>
    ///   tool_name=<name>
    ///   duration_s=<float>
    EmitToolCall {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Tool name as recorded in the agent audit trail (e.g. 'Bash: dotnet run ...').
        #[arg(long)]
        tool_name: String,
        /// Whether the tool call resulted in an error (e.g. stall-killed agent).
        #[arg(long, default_value = "false")]
        is_error: bool,
        /// Short result preview (truncated to 200 chars).
        #[arg(long, default_value = "")]
        result_preview: String,
        /// Actor ID for provenance (e.g. swarm-worker:v1).
        #[arg(long, default_value = "swarm-orchestrator")]
        actor_id: String,
        /// Wall-clock duration of the tool call in seconds.
        #[arg(long)]
        duration_s: Option<f64>,
        /// ISO 8601 timestamp; defaults to now if omitted.
        #[arg(long)]
        ts: Option<String>,
    },

    /// Create a session tombstone for the given session object (P6-A1).
    ///
    /// Snapshots the session's cluster memberships and embedding into a lightweight
    /// `session_tombstone` Object so that dream-cluster topology remains stable
    /// after the live session is removed.
    ///
    /// Prints machine-readable key=value lines:
    ///   original_session_id=<id>
    ///   tombstone_object_id=<id>
    ///   embedding_preserved=yes|no
    ///   cluster_memberships=<N>
    SessionTombstoneCreate {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Object ID of the session to ghost.
        #[arg(long)]
        session_id: String,
        /// Actor label to record in the tombstone provenance.
        #[arg(long, default_value = "session-gc")]
        created_by: String,
        /// Output path for the updated snapshot (same as input if omitted).
        #[arg(long)]
        output: Option<PathBuf>,
    },

    /// Check whether a session can be safely pruned without breaking dream-cluster
    /// topology (P6-A2).
    ///
    /// Prints machine-readable key=value lines:
    ///   safe=yes|no
    ///   reason=<...>      (only when safe=no)
    ///   cluster_count=<N>
    ///   cluster=<id> remaining_live=<N> would_isolate=yes|no
    SessionPruneCheck {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Object ID of the session to evaluate.
        #[arg(long)]
        session_id: String,
    },

    /// Safe composite prune: run safety check, create tombstone, ghost original (P6-B1).
    ///
    /// If pruning would isolate a dream-topic cluster, prints:
    ///   status=skipped
    ///   reason=<...>
    ///
    /// If safe, creates a tombstone and ghosts the original session, then prints:
    ///   status=pruned
    ///   tombstone_id=<id>
    ///   clusters_preserved=<N>
    SessionPruneSafe {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Object ID of the session to prune.
        #[arg(long)]
        session_id: String,
        /// Actor label to record in the tombstone provenance.
        #[arg(long, default_value = "session-gc")]
        created_by: String,
        /// Output path for the updated snapshot (same as input if omitted).
        #[arg(long)]
        output: Option<PathBuf>,
    },

    /// Batch safe prune: prune a list of sessions from a file (P6-B1).
    ///
    /// Reads newline-delimited session IDs from --ids-file and calls
    /// session-prune-safe logic for each.  Prints a summary:
    ///   pruned=N skipped=M total=K
    SessionPruneBatch {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Path to a file containing newline-delimited session object IDs.
        #[arg(long)]
        ids_file: PathBuf,
        /// Actor label to record in tombstone provenances.
        #[arg(long, default_value = "session-gc")]
        created_by: String,
        /// Output path for the updated snapshot (same as input if omitted).
        #[arg(long)]
        output: Option<PathBuf>,
    },

    /// Ghost tombstones older than a configurable age threshold (P6-C2).
    ///
    /// Scans all `session_tombstone` objects and ghosts those whose `ghosted_at`
    /// provenance timestamp is older than `--max-age-days` (default 30).
    /// Already-ghosted tombstones are skipped.
    ///
    /// Prints:
    ///   expired=N
    ///   kept=M
    SessionTombstoneExpire {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Ghost tombstones older than this many days (default: 30).
        #[arg(long, default_value = "30")]
        max_age_days: u32,
        /// Output path for the updated snapshot (same as input if omitted).
        #[arg(long)]
        output: Option<PathBuf>,
    },

    /// Search for files and directories in the projdir Atlas by keyword (P4-C1).
    ///
    /// Scores each file/directory Object by how well it matches the query terms:
    ///   exact path match (+10), path contains term (+5), extension match (+1),
    ///   head content contains term (+2). Prints the top-20 results in the same
    /// format as `ams.bat proj-dir search`.
    ProjdirSearch {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// One or more search terms.
        #[arg(long, required = true, num_args = 1..)]
        query: Vec<String>,
    },

    // ── Agent Knowledge Cache (AKC) commands ─────────────────────────────────

    /// Write a knowledge entry into the AMS store.
    KeWrite {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Scope: repo-relative path or concept slug (e.g. `rust/ams-core-kernel/src/projdir.rs`).
        #[arg(long)]
        scope: String,
        /// Entry kind: purpose|api|data-model|failure-modes|decision|prerequisites|test-guide.
        #[arg(long)]
        kind: String,
        /// Prose explanation text.
        #[arg(long)]
        text: String,
        /// Optional short summary.
        #[arg(long)]
        summary: Option<String>,
        /// Tags to attach (repeatable).
        #[arg(long = "tag", num_args = 0..)]
        tag: Vec<String>,
        /// Confidence score [0.0, 1.0] (default 0.8).
        #[arg(long, default_value = "0.8")]
        confidence: f64,
        /// Paths to watch for staleness (repeatable).
        #[arg(long = "watch", num_args = 0..)]
        watch: Vec<String>,
        /// Author agent ID.
        #[arg(long)]
        actor_id: Option<String>,
        /// Bootstrap source path (used when writing via ke-bootstrap).
        #[arg(long)]
        bootstrap_source: Option<String>,
    },

    /// Read all knowledge entries for a given scope.
    KeRead {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Scope to read (repo-relative path or concept slug).
        #[arg(long)]
        scope: String,
        /// Include stale entries in the output.
        #[arg(long, default_value = "false")]
        include_stale: bool,
    },

    /// Search knowledge entries by query terms.
    KeSearch {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// One or more search terms.
        #[arg(long, required = true, num_args = 1..)]
        query: Vec<String>,
        /// Return top N results (default 10).
        #[arg(long, default_value = "10")]
        top: usize,
        /// Restrict to entries whose scope starts with this prefix.
        #[arg(long)]
        scope: Option<String>,
        /// Restrict to entries of this kind.
        #[arg(long)]
        kind: Option<String>,
        /// Include stale entries in the results.
        #[arg(long, default_value = "false")]
        include_stale: bool,
    },

    /// Build a compact knowledge-cache context block for agent injection.
    KeContext {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Restrict to entries whose scope starts with this prefix.
        #[arg(long)]
        scope: Option<String>,
        /// Maximum number of entries to include (default 20).
        #[arg(long, default_value = "20")]
        max_entries: usize,
        /// Maximum output characters (default 3000).
        #[arg(long, default_value = "3000")]
        max_chars: usize,
    },

    /// Bootstrap knowledge entries from markdown files in the repo (depth ≤ 3).
    KeBootstrap {
        /// Accepts either a direct '.ams.json' snapshot path or a '.memory.jsonl' store path.
        #[arg(long)]
        input: PathBuf,
        /// Repository root to scan. Defaults to the current working directory.
        #[arg(long, default_value = ".")]
        repo_root: PathBuf,
        /// Overwrite existing entries (not currently used; reserved for future use).
        #[arg(long, default_value = "false")]
        overwrite: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliTraversalDirection {
    Forward,
    Backward,
    Both,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliCardState {
    Active,
    Tombstoned,
    Retracted,
}

impl From<CliTraversalDirection> for TraversalDirection {
    fn from(value: CliTraversalDirection) -> Self {
        match value {
            CliTraversalDirection::Forward => TraversalDirection::Forward,
            CliTraversalDirection::Backward => TraversalDirection::Backward,
            CliTraversalDirection::Both => TraversalDirection::Both,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliCorpus {
    All,
    Project,
    Claude,
    Codex,
}

impl From<CliCardState> for CardState {
    fn from(value: CliCardState) -> Self {
        match value {
            CliCardState::Active => CardState::Active,
            CliCardState::Tombstoned => CardState::Tombstoned,
            CliCardState::Retracted => CardState::Retracted,
        }
    }
}

fn main() -> Result<()> {
    // The Command enum has 180+ variants.  Clap's debug-mode initialization
    // recurses deeply through them during parse/error-handling, which overflows
    // Windows' default 1 MB thread stack.  Spawn on a 8 MB stack to match the
    // Linux default; this is a no-op in release builds where the frames are
    // eliminated by inlining.
    let result = std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(run_main)
        .expect("failed to spawn main thread")
        .join()
        .expect("main thread panicked");
    result
}

fn run_main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::ValidateSnapshot { input } => validate_snapshot(&input),
        Command::RoundtripSnapshot { input, output } => roundtrip_snapshot(&input, &output),
        Command::ReplayLog { input, output } => replay_log_command(&input, output.as_ref()),
        Command::AppendLog { output, entry } => append_log_command(&output, &entry),
        Command::Stress {
            containers,
            iterations,
            output,
        } => stress_command(containers, iterations, output.as_ref()),
        Command::ListObjects { input, kind } => list_objects_command(&input, kind.as_deref()),
        Command::ShowObject { input, id } => show_object_command(&input, &id),
        Command::ListContainers { input, kind } => list_containers_command(&input, kind.as_deref()),
        Command::ListLinkNodes {
            input,
            container_id,
            object_id,
        } => list_link_nodes_command(&input, container_id.as_deref(), object_id.as_deref()),
        Command::ShowContainer {
            input,
            id,
            direction,
        } => show_container_command(&input, &id, direction.into()),
        Command::ShowLinkNode { input, id } => show_link_node_command(&input, &id),
        Command::Memberships { input, object_id } => memberships_command(&input, &object_id),
        Command::ListSessions { input, since, n } => list_sessions_command(&input, since.as_deref(), n),
        Command::ShowSession { input, id } => show_session_command(&input, &id),
        Command::ThreadStatus { input } => thread_status_command(&input),
        Command::ThreadStart {
            input,
            title,
            current_step,
            next_command,
            id,
            branch_off_anchor,
            artifact_ref,
            mutation_id,
            actor_id,
        } => thread_start_command(
            &input,
            &title,
            &current_step,
            &next_command,
            id.as_deref(),
            branch_off_anchor.as_deref(),
            artifact_ref.as_deref(),
            mutation_id.as_deref(),
            actor_id.as_deref(),
        ),
        Command::ThreadPushTangent {
            input,
            title,
            current_step,
            next_command,
            id,
            branch_off_anchor,
            artifact_ref,
            mutation_id,
            actor_id,
        } => thread_push_tangent_command(
            &input,
            &title,
            &current_step,
            &next_command,
            id.as_deref(),
            branch_off_anchor.as_deref(),
            artifact_ref.as_deref(),
            mutation_id.as_deref(),
            actor_id.as_deref(),
        ),
        Command::ThreadCheckpoint {
            input,
            current_step,
            next_command,
            branch_off_anchor,
            artifact_ref,
            mutation_id,
            actor_id,
        } => thread_checkpoint_command(
            &input,
            &current_step,
            &next_command,
            branch_off_anchor.as_deref(),
            artifact_ref.as_deref(),
            mutation_id.as_deref(),
            actor_id.as_deref(),
        ),
        Command::ThreadArchive {
            input,
            id,
            mutation_id,
            actor_id,
        } => thread_archive_command(&input, id.as_deref(), mutation_id.as_deref(), actor_id.as_deref()),
        Command::ThreadPop {
            input,
            mutation_id,
            actor_id,
        } => thread_pop_command(&input, mutation_id.as_deref(), actor_id.as_deref()),
        Command::ThreadList { input } => thread_list_command(&input),
        Command::BackendStatus { input } => backend_status_command(&input),
        Command::BackendRecoverValidate { input, assert_clean } => {
            backend_recover_validate_command(&input, assert_clean)
        }
        Command::ThreadClaim {
            input,
            id,
            agent,
            lease_seconds,
            claim_token,
            mutation_id,
            actor_id,
        } => thread_claim_command(
            &input,
            id.as_deref(),
            &agent,
            lease_seconds,
            claim_token.as_deref(),
            mutation_id.as_deref(),
            actor_id.as_deref(),
        ),
        Command::ThreadHeartbeat {
            input,
            id,
            agent,
            claim_token,
            lease_seconds,
            mutation_id,
            actor_id,
        } => thread_heartbeat_command(
            &input,
            id.as_deref(),
            &agent,
            &claim_token,
            lease_seconds,
            mutation_id.as_deref(),
            actor_id.as_deref(),
        ),
        Command::ThreadRelease {
            input,
            id,
            agent,
            claim_token,
            reason,
            mutation_id,
            actor_id,
        } => thread_release_command(
            &input,
            id.as_deref(),
            &agent,
            &claim_token,
            reason.as_deref(),
            mutation_id.as_deref(),
            actor_id.as_deref(),
        ),
        Command::SmartlistInspect { input, path, depth } => smartlist_inspect_command(&input, &path, depth),
        Command::SnapshotDiff { left, right } => snapshot_diff_command(&left, &right),
        Command::CorpusSummary { input } => corpus_summary_command(&input),
        Command::ListCards { input, state } => list_cards_command(&input, state.map(Into::into)),
        Command::ShowCard { input, id } => show_card_command(&input, &id),
        Command::ListBinders { input, contains } => list_binders_command(&input, contains.as_deref()),
        Command::ShowBinder { input, id } => show_binder_command(&input, &id),
        Command::QueryCards {
            input,
            q,
            top,
            binder,
            seed_card,
            state,
            include_retracted,
            explain,
            current_node,
            parent_node,
            grandparent_node,
            role,
            mode,
            failure_bucket,
            artifact,
            traversal_budget,
            no_active_thread_context,
            route_replay,
            bias_scale,
            min_strong_wins,
            min_bias,
            max_episodes,
        } => query_cards_command(
            &input,
            &q,
            top,
            binder.as_deref(),
            seed_card.as_deref(),
            state.map(Into::into),
            include_retracted,
            explain,
            QueryContextOptions {
                current_node_id: current_node,
                parent_node_id: parent_node,
                grandparent_node_id: grandparent_node,
                agent_role: role,
                mode,
                failure_bucket,
                active_artifacts: parse_multi_value(artifact.as_deref()),
                traversal_budget,
                no_active_thread_context,
            },
            route_replay.as_ref(),
            RouteMemoryBiasOptions {
                min_strong_wins_to_activate: min_strong_wins,
                bias_scale,
                min_bias_to_apply: min_bias,
                max_episodes,
            },
        ),
        Command::RouteReplay {
            input,
            replay,
            out,
            top,
            bias_scale,
            min_strong_wins,
            min_bias,
            max_episodes,
        } => route_replay_command(
            &input,
            &replay,
            &out,
            top,
            RouteMemoryBiasOptions {
                min_strong_wins_to_activate: min_strong_wins,
                bias_scale,
                min_bias_to_apply: min_bias,
                max_episodes,
            },
        ),
        Command::FepBootstrap {
            input,
            episodes,
            output,
        } => fep_bootstrap_command(&input, &episodes, output.as_ref()),
        Command::ToolOutcomePriors { input } => tool_outcome_priors_command(&input),
        Command::PredictToolOutcome {
            input,
            scope_lens,
            agent_role,
        } => predict_tool_outcome_command(&input, &scope_lens, &agent_role),
        Command::FepBootstrapAgentTools { input, output } => {
            fep_bootstrap_agent_tools_command(&input, output.as_ref())
        }
        Command::FepDetectToolAnomalies {
            input,
            since,
            threshold,
            output,
        } => fep_detect_tool_anomalies_command(&input, &since, threshold, output.as_ref()),
        Command::FepUpdateToolBelief {
            input,
            tool_name,
            outcome,
            precision,
            output,
        } => fep_update_tool_belief_command(&input, &tool_name, &outcome, precision, output.as_ref()),
        Command::FepUpdateAgentToolBeliefs {
            input,
            since,
            precision,
            output,
        } => fep_update_agent_tool_beliefs_command(&input, &since, precision, output.as_ref()),
        Command::FepDecayToolPriors {
            input,
            decay_rate,
            output,
        } => fep_decay_tool_priors_command(&input, decay_rate, output.as_ref()),
        Command::FepBootstrapDurationPriors { input, output } => {
            fep_bootstrap_duration_priors_command(&input, output.as_ref())
        }
        Command::FepDetectSlowTools {
            input,
            since,
            threshold,
        } => fep_detect_slow_tools_command(&input, &since, threshold),
        Command::AgentQuery {
            input,
            q,
            top,
            explain,
            json,
            binder,
            seed_card,
            state,
            include_retracted,
            current_node,
            parent_node,
            grandparent_node,
            role,
            mode,
            failure_bucket,
            artifact,
            traversal_budget,
            no_active_thread_context,
            route_replay,
            bias_scale,
            min_strong_wins,
            min_bias,
            max_episodes,
            record_route,
            actor_id,
            no_legacy_route_mirror,
            include_latent,
        } => agent_query_command(
            &input,
            &q,
            top,
            explain,
            json,
            binder.as_deref(),
            seed_card,
            state.map(Into::into),
            include_retracted,
            QueryContextOptions {
                current_node_id: current_node,
                parent_node_id: parent_node,
                grandparent_node_id: grandparent_node,
                agent_role: role,
                mode,
                failure_bucket,
                active_artifacts: parse_multi_value(artifact.as_deref()),
                traversal_budget,
                no_active_thread_context,
            },
            route_replay.as_ref(),
            RouteMemoryBiasOptions {
                min_strong_wins_to_activate: min_strong_wins,
                bias_scale,
                min_bias_to_apply: min_bias,
                max_episodes,
            },
            record_route,
            actor_id.as_deref(),
            !no_legacy_route_mirror,
            include_latent,
        ),
        Command::RecordRouteEpisode {
            input,
            mutation_id,
            actor_id,
            episode_json,
            episode_file,
            expected_version,
            mirror_legacy,
        } => record_route_episode_command(
            &input,
            &mutation_id,
            &actor_id,
            episode_json.as_deref(),
            episode_file.as_ref(),
            expected_version,
            mirror_legacy,
        ),
        Command::RouteStateCompare { input } => route_state_compare_command(&input),
        Command::SmartlistCreate {
            input,
            path,
            mutation_id,
            actor_id,
            created_by,
            durable,
        } => smartlist_create_command(&input, &path, mutation_id.as_deref(), actor_id.as_deref(), created_by.as_deref(), durable),
        Command::SmartlistNote {
            input,
            title,
            text,
            buckets,
            mutation_id,
            actor_id,
            created_by,
            note_id,
            durable,
        } => smartlist_note_command(
            &input,
            &title,
            &text,
            buckets.as_deref(),
            mutation_id.as_deref(),
            actor_id.as_deref(),
            created_by.as_deref(),
            note_id.as_deref(),
            durable,
        ),
        Command::SmartlistAttach {
            input,
            path,
            member_ref,
            mutation_id,
            actor_id,
            created_by,
        } => smartlist_attach_command(
            &input,
            &path,
            &member_ref,
            mutation_id.as_deref(),
            actor_id.as_deref(),
            created_by.as_deref(),
        ),
        Command::SmartlistAttachBefore {
            input,
            path,
            member_ref,
            before_member_ref,
            mutation_id,
            actor_id,
            created_by,
        } => smartlist_attach_before_command(
            &input,
            &path,
            &member_ref,
            &before_member_ref,
            mutation_id.as_deref(),
            actor_id.as_deref(),
            created_by.as_deref(),
        ),
        Command::SmartlistDetach {
            input,
            path,
            member_ref,
            mutation_id,
            actor_id,
            created_by,
        } => smartlist_detach_command(
            &input,
            &path,
            &member_ref,
            mutation_id.as_deref(),
            actor_id.as_deref(),
            created_by.as_deref(),
        ),
        Command::SmartlistMove {
            input,
            source_path,
            target_path,
            member_ref,
            before_member_ref,
            mutation_id,
            actor_id,
            created_by,
        } => smartlist_move_command(
            &input,
            &source_path,
            &target_path,
            &member_ref,
            before_member_ref.as_deref(),
            mutation_id.as_deref(),
            actor_id.as_deref(),
            created_by.as_deref(),
        ),
        Command::SmartlistBucketSet {
            input,
            path,
            field,
            mutation_id,
            actor_id,
            created_by,
        } => smartlist_bucket_set_command(
            &input,
            &path,
            &field,
            mutation_id.as_deref(),
            actor_id.as_deref(),
            created_by.as_deref(),
        ),
        Command::SmartlistRollup {
            input,
            path,
            summary,
            scope,
            stop_hint,
            child_highlight,
            mutation_id,
            actor_id,
            created_by,
            durable,
        } => smartlist_rollup_command(
            &input,
            &path,
            &summary,
            &scope,
            stop_hint.as_deref(),
            &child_highlight,
            mutation_id.as_deref(),
            actor_id.as_deref(),
            created_by.as_deref(),
            durable,
        ),
        Command::SmartlistVisibility {
            input,
            path,
            visibility,
            mutation_id,
            actor_id,
            recursive,
            include_notes,
            include_rollups,
        } => smartlist_visibility_command(
            &input,
            &path,
            &visibility,
            mutation_id.as_deref(),
            actor_id.as_deref(),
            recursive,
            include_notes,
            include_rollups,
        ),
        Command::SmartlistMemberships { input, object_id } => smartlist_memberships_command(&input, &object_id),
        Command::SmartlistCategoryCreate { input, name, mutation_id, actor_id, created_by } =>
            smartlist_category_create_command(&input, &name, mutation_id.as_deref(), actor_id.as_deref(), created_by.as_deref()),
        Command::SmartlistCategoryList { input } => smartlist_category_list_command(&input),
        Command::SmartlistCategoryAttach { input, object_id, category, mutation_id, actor_id, created_by } =>
            smartlist_category_attach_command(&input, &object_id, &category, mutation_id.as_deref(), actor_id.as_deref(), created_by.as_deref()),
        Command::SmartlistSetOrdering { input, path, policy, direction, tie_breaker, mutation_id, actor_id, created_by } =>
            smartlist_set_ordering_command(&input, &path, &policy, &direction, tie_breaker.as_deref(), mutation_id.as_deref(), actor_id.as_deref(), created_by.as_deref()),
        Command::SmartlistBrowse { input, path, category, tier } =>
            smartlist_browse_command(&input, path.as_deref(), category.as_deref(), tier.as_deref()),
        Command::SmartlistRecencyTiers { input, bootstrap, mutation_id, actor_id, created_by } =>
            smartlist_recency_tiers_command(&input, bootstrap, mutation_id.as_deref(), actor_id.as_deref(), created_by.as_deref()),
        Command::SmartlistRotate { input, dry_run, mutation_id, actor_id, created_by } =>
            smartlist_rotate_command(&input, dry_run, mutation_id.as_deref(), actor_id.as_deref(), created_by.as_deref()),
        Command::SmartlistCategorize { input, dry_run, mutation_id, actor_id, created_by } =>
            smartlist_categorize_command(&input, dry_run, mutation_id.as_deref(), actor_id.as_deref(), created_by.as_deref()),
        Command::SmartlistGc { input, dry_run, default_ttl_hours, mutation_id, actor_id, created_by } =>
            smartlist_gc_command(&input, dry_run, default_ttl_hours, mutation_id.as_deref(), actor_id.as_deref(), created_by.as_deref()),
        Command::SmartlistWriteAttach { input, object_id, mutation_id, actor_id, created_by } =>
            smartlist_write_attach_command(&input, &object_id, mutation_id.as_deref(), actor_id.as_deref(), created_by.as_deref()),
        Command::DreamTouch { input, object_id, mutation_id, actor_id, created_by } =>
            dream_touch_command(&input, &object_id, mutation_id.as_deref(), actor_id.as_deref(), created_by.as_deref()),
        Command::DreamSchedule { input, max_touches, created_by } =>
            dream_schedule_command(&input, max_touches, created_by.as_deref()),
        Command::DreamCluster { input, min_jaccard, max_clusters, mutation_id, actor_id, created_by } =>
            dream_cluster_command(&input, min_jaccard, max_clusters, mutation_id.as_deref(), actor_id.as_deref(), created_by.as_deref()),
        Command::DreamFindIsolated { db } =>
            dream_find_isolated_command(&db),
        Command::DreamShortcut { input, embeddings, actor_id, created_by } =>
            dream_shortcut_command(&input, &embeddings, actor_id.as_deref(), created_by.as_deref()),
        Command::DreamGenerateMd { input, out, max_topics, max_sessions } =>
            dream_generate_md_command(&input, &out, max_topics, max_sessions),
        Command::ParityValidate { input, cases, out } => parity_validate_command(&input, &cases, out.as_ref()),
        Command::ShadowValidate {
            input,
            cases,
            out,
            memoryctl_exe,
            assert_match,
        } => shadow_validate_command(&input, &cases, out.as_ref(), memoryctl_exe.as_ref(), assert_match),
        Command::AgentPoolAllocate {
            input,
            agent_ref,
            task_path,
            mutation_id,
            actor_id,
            created_by,
        } => agent_pool_allocate_command(
            &input,
            &agent_ref,
            &task_path,
            mutation_id.as_deref(),
            actor_id.as_deref(),
            created_by.as_deref(),
        ),
        Command::AgentPoolRelease {
            input,
            agent_ref,
            task_path,
            mutation_id,
            actor_id,
            created_by,
        } => agent_pool_release_command(
            &input,
            &agent_ref,
            &task_path,
            mutation_id.as_deref(),
            actor_id.as_deref(),
            created_by.as_deref(),
        ),
        Command::AgentPoolStatus { input } => agent_pool_status_command(&input),
        Command::Search {
            keywords,
            corpus,
            top,
            explain,
            json,
            actor_id,
            no_record_route,
        } => {
            let input = resolve_corpus_path(corpus)?;
            agent_query_command(
                &input,
                &keywords.join(" "),
                top,
                explain,
                json,
                None,
                None,
                None,
                false,
                QueryContextOptions {
                    current_node_id: None,
                    parent_node_id: None,
                    grandparent_node_id: None,
                    agent_role: None,
                    mode: None,
                    failure_bucket: None,
                    active_artifacts: vec![],
                    traversal_budget: 3,
                    no_active_thread_context: false,
                },
                None,
                RouteMemoryBiasOptions::default(),
                !no_record_route,
                actor_id.as_deref(),
                true,
                false, // include_latent = false for search
            )
        }
        Command::Recall {
            keywords,
            corpus,
            top,
            explain,
            json,
            actor_id,
            no_record_route,
        } => {
            let input = resolve_corpus_path(corpus)?;
            recall_command(&input, &keywords.join(" "), top, explain, json, actor_id.as_deref(), !no_record_route)
        }

        // ── Bug Report commands ────────────────────────────────────────
        Command::BugreportList { input, status } =>
            bugreport_list_command(&input, status.as_deref()),
        Command::BugreportShow { input, id } =>
            bugreport_show_command(&input, &id),
        Command::BugreportCreate {
            input, source_agent, parent_agent, error_output, stack_context,
            attempted_fixes, reproduction_steps, recommended_fix_plan,
            severity, durable, mutation_id, actor_id, created_by,
        } => bugreport_create_command(
            &input, &source_agent, &parent_agent, &error_output, &stack_context,
            attempted_fixes.as_deref(), reproduction_steps.as_deref(),
            &recommended_fix_plan, &severity, durable,
            mutation_id.as_deref(), actor_id.as_deref(), created_by.as_deref(),
        ),
        Command::BugreportSearch { input, q, status } =>
            bugreport_search_command(&input, &q, status.as_deref()),
        Command::BugreportUpdateStatus { input, id, status, mutation_id, actor_id } =>
            bugreport_update_status_command(&input, &id, &status, mutation_id.as_deref(), actor_id.as_deref()),

        // ── Bug Fix commands ───────────────────────────────────────────
        Command::BugfixList { input } =>
            bugfix_list_command(&input),
        Command::BugfixShow { input, id } =>
            bugfix_show_command(&input, &id),
        Command::BugfixCreate {
            input, title, description, fix_recipe, linked_bugreport_id,
            durable, mutation_id, actor_id, created_by,
        } => bugfix_create_command(
            &input, &title, &description, &fix_recipe,
            linked_bugreport_id.as_deref(), durable,
            mutation_id.as_deref(), actor_id.as_deref(), created_by.as_deref(),
        ),

        // ── Policy-layer commands ──────────────────────────────────────
        Command::PolicySet { input, container_id, field, value, mutation_id, actor_id } =>
            policy_set_command(&input, &container_id, &field, &value, mutation_id.as_deref(), actor_id.as_deref()),
        Command::PolicyShow { input, container_id } =>
            policy_show_command(&input, &container_id),

        // ── Swarm-Plan / Callstack commands ───────────────────────────
        Command::SwarmPlanContext { input, max_chars, project } =>
            swarm_plan_context_command(&input, max_chars, project.as_deref()),
        Command::SwarmPlanList { input } =>
            swarm_plan_list_command(&input),
        Command::SwarmPlanShow { input, project } =>
            swarm_plan_show_command(&input, project.as_deref()),
        Command::SwarmPlanPush { input, name, description, depends_on, actor_id } =>
            swarm_plan_push_command(&input, &name, description.as_deref(), depends_on.as_deref(), actor_id.as_deref()),
        Command::SwarmPlanPop { input, return_text, actor_id } =>
            swarm_plan_pop_command(&input, return_text.as_deref(), actor_id.as_deref()),
        Command::SwarmPlanObserve { input, title, text, actor_id, node_path } =>
            swarm_plan_observe_command(&input, &title, &text, actor_id.as_deref(), node_path.as_deref()),
        Command::SwarmPlanInterrupt {
            input, policy, reason, error_output, context, attempted_fix,
            repair_hint, subtask_hints, actor_id,
        } => swarm_plan_interrupt_command(
            &input, &policy, &reason, &error_output, &context,
            &attempted_fix, &repair_hint, &subtask_hints, actor_id.as_deref(),
        ),
        Command::SwarmPlanResume { input, actor_id } =>
            swarm_plan_resume_command(&input, actor_id.as_deref()),
        Command::SwarmPlanAdvance { input, actor_id } =>
            swarm_plan_advance_command(&input, actor_id.as_deref()),
        Command::SwarmPlanSwitch { input, name, actor_id } =>
            swarm_plan_switch_command(&input, &name, actor_id.as_deref()),
        Command::SwarmPlanPark { input, project, actor_id } =>
            swarm_plan_park_command(&input, project.as_deref(), actor_id.as_deref()),
        Command::SwarmPlanCompleteNode { input, node_path, return_text, actor_id } =>
            swarm_plan_complete_node_command(&input, &node_path, return_text.as_deref(), actor_id.as_deref()),
        Command::SwarmPlanReadyNodes { input, project } =>
            swarm_plan_ready_nodes_command(&input, project.as_deref()),
        Command::SwarmPlanLoadPlan { input, file, into_active, actor_id } =>
            swarm_plan_load_plan_command(&input, &file, into_active, actor_id.as_deref()),
        Command::SwarmPlanBatch { input, ops, actor_id } =>
            swarm_plan_batch_command(&input, &ops, actor_id.as_deref()),
        Command::SwarmPlanRepairRoots { input, actor_id } =>
            swarm_plan_repair_roots_command(&input, actor_id.as_deref()),
        Command::SwarmPlanMigrate { from, to, project } => {
            let result = migrate_swarm_plan_store(&from, &to, &project)?;
            println!("{}", result.to_text());
            Ok(())
        }
        Command::SwarmPlanEnterEdit { input, project, actor_id } =>
            swarm_plan_enter_edit_command(&input, project.as_deref(), actor_id.as_deref()),
        Command::SwarmPlanEnterExecute { input, project, actor_id } =>
            swarm_plan_enter_execute_command(&input, project.as_deref(), actor_id.as_deref()),
        Command::SwarmPlanQuarantinedPush { input, name, description, depends_on, parent_node_path, actor_id } =>
            swarm_plan_quarantined_push_command(
                &input, &name, description.as_deref(), depends_on.as_deref(),
                parent_node_path.as_deref(), actor_id.as_deref(),
            ),
        Command::SwarmPlanRenameNode { input, node_path, new_title, actor_id } =>
            swarm_plan_rename_node_command(&input, &node_path, &new_title, actor_id.as_deref()),
        Command::SwarmPlanDeleteNode { input, node_path, actor_id } =>
            swarm_plan_delete_node_command(&input, &node_path, actor_id.as_deref()),
        Command::SwarmPlanSetDependsOn { input, node_path, depends_on, actor_id } =>
            swarm_plan_set_depends_on_command(&input, &node_path, &depends_on, actor_id.as_deref()),
        Command::SwarmPlanMoveNode { input, node_path, new_parent_path, actor_id } =>
            swarm_plan_move_node_command(&input, &node_path, &new_parent_path, actor_id.as_deref()),
        Command::SwarmPlanTag { input, plan_name, bucket_path, summary, actor_id } =>
            swarm_plan_tag_command(&input, &plan_name, &bucket_path, summary.as_deref(), actor_id.as_deref()),

        // ── Cache commands ─────────────────────────────────────────────
        Command::CacheRegisterTool { input, tool_id, tool_version, actor_id } =>
            cache_register_tool_command(&input, &tool_id, &tool_version, actor_id.as_deref()),
        Command::CacheRegisterSource { input, source_id, fingerprint, actor_id } =>
            cache_register_source_command(&input, &source_id, fingerprint.as_deref(), actor_id.as_deref()),
        Command::CachePromote {
            input, tool_id, tool_version, source_id, source_fingerprint,
            param_hash, in_situ_ref, artifact_fingerprint, actor_id,
        } => cache_promote_command(
            &input, &tool_id, &tool_version, &source_id,
            source_fingerprint.as_deref(), &param_hash,
            in_situ_ref.as_deref(), artifact_fingerprint.as_deref(),
            actor_id.as_deref(),
        ),
        Command::CacheLookup { input, mode, tool_id, source_id, param_hash, format } =>
            cache_lookup_command(&input, &mode, &tool_id, &source_id, param_hash.as_deref(), &format),
        Command::CacheInvalidate { input, artifact_id, state, reason } =>
            cache_invalidate_command(&input, &artifact_id, &state, reason.as_deref()),

        // ── Atlas commands ─────────────────────────────────────────────
        Command::AtlasPage { input, id } => atlas_page_command(&input, &id),
        Command::AtlasSearch { input, q, top } => atlas_search_command(&input, &q, top),
        Command::AtlasExpand { input, id } => atlas_expand_command(&input, &id),
        Command::AtlasDefine { input, name, description, scales } => {
            atlas_define_command(&input, &name, description.as_deref(), &scales)
        }
        Command::AtlasShow { input, name } => atlas_show_command(&input, &name),
        Command::AtlasList { input } => atlas_list_command(&input),
        Command::AtlasListAtScale { input, name, scale } => {
            atlas_list_at_scale_command(&input, &name, scale)
        }
        Command::AtlasNavigate { input, name, id } => atlas_navigate_command(&input, &name, &id),

        // ── Resolution commands ─────────────────────────────────────────
        Command::ResolutionResolve {
            input, object_id, tool_id, source_id, param_hash,
            no_cache, no_historical, no_partial, no_content_addressed,
            revalidate_on_recovery,
        } => resolution_resolve_command(
            &input, &object_id,
            tool_id.as_deref(), source_id.as_deref(), param_hash.as_deref(),
            no_cache, no_historical, no_partial, no_content_addressed,
            revalidate_on_recovery,
        ),
        Command::ResolutionShow { input, object_id } =>
            resolution_show_command(&input, &object_id),

        // ── ProjDir Atlas commands ──────────────────────────────────────────
        Command::ProjdirIngest { input, repo_root } =>
            projdir_ingest_command(&input, &repo_root),
        Command::ProjdirBuildDirs { input } =>
            projdir_build_dirs_command(&input),
        Command::ProjdirStats { input } =>
            projdir_stats_command(&input),
        Command::ProjdirRegisterAtlas { input } =>
            projdir_register_atlas_command(&input),
        Command::ProjdirBuildFilePages { input } =>
            projdir_build_file_pages_command(&input),
        Command::ProjdirDoc { input, path } =>
            projdir_doc_command(&input, &path),
        Command::ProjdirContext { input, depth } =>
            projdir_context_command(&input, depth),
        Command::ProjdirTree { input, path, depth } =>
            projdir_tree_command(&input, path.as_deref(), depth),
        Command::ProjdirSearch { input, query } =>
            projdir_search_command(&input, &query),

        // ── Agent Knowledge Cache (AKC) commands ────────────────────────────
        Command::KeWrite { input, scope, kind, text, summary, tag, confidence, watch, actor_id, bootstrap_source } =>
            ke_write_command(&input, &scope, &kind, &text, summary.as_deref(), &tag, confidence, &watch, actor_id.as_deref(), bootstrap_source.as_deref()),
        Command::KeRead { input, scope, include_stale } =>
            ke_read_command(&input, &scope, include_stale),
        Command::KeSearch { input, query, top, scope, kind, include_stale } =>
            ke_search_command(&input, &query, top, scope.as_deref(), kind.as_deref(), include_stale),
        Command::KeContext { input, scope, max_entries, max_chars } =>
            ke_context_command(&input, scope.as_deref(), max_entries, max_chars),
        Command::KeBootstrap { input, repo_root, overwrite: _ } =>
            ke_bootstrap_command(&input, &repo_root),

        // ── Search cache commands (P5) ──────────────────────────────────────
        Command::SearchCorpusVersion { input } =>
            search_corpus_version_command(&input),
        Command::SearchCacheLookup { input, query } =>
            search_cache_lookup_command(&input, &query),
        Command::SearchCachePromote { input, query, text, actor_id } =>
            search_cache_promote_command(&input, &query, &text, actor_id.as_deref()),
        Command::SearchCacheInvalidate { input, corpus_version } =>
            search_cache_invalidate_command(&input, &corpus_version),
        Command::SearchCacheStats { input } =>
            search_cache_stats_command(&input),

        // ── FEP cache signal commands (P7) ─────────────────────────────────
        Command::FepCacheSignalEmit { input, query, is_hit, actor_id } =>
            fep_cache_signal_emit_command(&input, &query, is_hit, actor_id.as_deref()),
        Command::FepCacheSignalStats { input, tool, window_hours } =>
            fep_cache_signal_stats_command(&input, tool.as_deref(), window_hours),
        Command::FepCacheSignalClusterSurprise { input, window_hours, min_signals } =>
            fep_cache_signal_cluster_surprise_command(&input, window_hours, min_signals),
        Command::FepCacheReport { input, window_hours } =>
            fep_cache_report_command(&input, window_hours),
        Command::EmitToolCall { input, tool_name, is_error, result_preview, actor_id, duration_s, ts } =>
            emit_tool_call_command(&input, &tool_name, is_error, &result_preview, &actor_id, duration_s, ts.as_deref()),

        // ── Ghost session recovery commands (P6) ───────────────────────────
        Command::SessionTombstoneCreate { input, session_id, created_by, output } =>
            session_tombstone_create_command(&input, &session_id, &created_by, output.as_deref()),
        Command::SessionPruneCheck { input, session_id } =>
            session_prune_check_command(&input, &session_id),
        Command::SessionPruneSafe { input, session_id, created_by, output } =>
            session_prune_safe_command(&input, &session_id, &created_by, output.as_deref()),
        Command::SessionPruneBatch { input, ids_file, created_by, output } =>
            session_prune_batch_command(&input, &ids_file, &created_by, output.as_deref()),
        Command::SessionTombstoneExpire { input, max_age_days, output } =>
            session_tombstone_expire_command(&input, max_age_days, output.as_deref()),
    }
}

fn validate_snapshot(input: &PathBuf) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());
    print_summary("snapshot", &store);
    emit_invariants(&store)
}

fn roundtrip_snapshot(input: &PathBuf, output: &PathBuf) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    emit_invariants(&store)?;
    let rendered = serialize_snapshot(&store)?;
    fs::write(output, rendered).with_context(|| format!("failed to write snapshot '{}'", output.display()))?;
    println!("snapshot_source={}", resolved.display());
    print_summary("roundtrip", &store);
    Ok(())
}

fn replay_log_command(input: &PathBuf, output: Option<&PathBuf>) -> Result<()> {
    let store = replay_log(input)?;
    emit_invariants(&store)?;
    if let Some(output) = output {
        fs::write(output, serialize_snapshot(&store)?)
            .with_context(|| format!("failed to write snapshot '{}'", output.display()))?;
    }
    print_summary("replay", &store);
    Ok(())
}

fn append_log_command(output: &PathBuf, entry: &str) -> Result<()> {
    let parsed: MutationLogEntry = serde_json::from_str(entry).context("failed to parse --entry as JSON")?;
    append_log_entry(output, &parsed)?;
    println!("appended=1");
    Ok(())
}

fn stress_command(container_count: usize, iterations: usize, output: Option<&PathBuf>) -> Result<()> {
    let mut store = AmsStore::new();
    for i in 0..container_count {
        store.create_container(format!("ctr:{i}"), "container", "smartlist")?;
    }
    for i in 0..(iterations.max(container_count) + 4) {
        store.upsert_object(format!("obj:{i}"), "thing", None, None, None)?;
    }

    for i in 0..iterations {
        let container_id = format!("ctr:{}", i % container_count);
        let object_id = format!("obj:{i}");
        let link_id = format!("ln:{i}");
        store.add_object(&container_id, &object_id, None, Some(link_id.clone()))?;

        if i % 3 == 0 && i > 0 {
            let prior_object = format!("obj:{}", i - 1);
            store.insert_after(
                &container_id,
                &link_id,
                &prior_object,
                None,
                Some(format!("ln:insert:{i}")),
            )?;
        }

        if i % 5 == 0 && i > 1 {
            let remove_id = format!("ln:{}", i - 1);
            let _ = store.remove_linknode(&container_id, &remove_id)?;
        }
    }

    emit_invariants(&store)?;
    if let Some(output) = output {
        fs::write(output, serialize_snapshot(&store)?)
            .with_context(|| format!("failed to write stress snapshot '{}'", output.display()))?;
    }
    print_summary("stress", &store);
    Ok(())
}

fn list_objects_command(input: &PathBuf, kind: Option<&str>) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());
    print!("{}", list_objects(&store, kind));
    Ok(())
}

fn show_object_command(input: &PathBuf, object_id: &str) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());
    print!("{}", show_object(&store, object_id)?);
    Ok(())
}

fn list_containers_command(input: &PathBuf, kind: Option<&str>) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());
    print!("{}", list_containers(&store, kind));
    Ok(())
}

fn list_link_nodes_command(input: &PathBuf, container_id: Option<&str>, object_id: Option<&str>) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());
    print!("{}", list_link_nodes(&store, container_id, object_id));
    Ok(())
}

fn show_container_command(input: &PathBuf, container_id: &str, direction: TraversalDirection) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());
    print!("{}", show_container(&store, container_id, direction)?);
    Ok(())
}

fn show_link_node_command(input: &PathBuf, link_node_id: &str) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());
    print!("{}", show_link_node(&store, link_node_id)?);
    Ok(())
}

fn memberships_command(input: &PathBuf, object_id: &str) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());
    print!("{}", show_memberships(&store, object_id)?);
    Ok(())
}

fn list_sessions_command(input: &PathBuf, since: Option<&str>, n: usize) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    print!("{}", list_sessions(&store, since, n)?);
    Ok(())
}

fn show_session_command(input: &PathBuf, id: &str) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    print!("{}", show_session(&store, id)?);
    Ok(())
}

fn thread_status_command(input: &PathBuf) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    print!("{}", thread_status(&store));
    Ok(())
}

fn thread_start_command(
    input: &PathBuf,
    title: &str,
    current_step: &str,
    next_command: &str,
    thread_id: Option<&str>,
    branch_off_anchor: Option<&str>,
    artifact_ref: Option<&str>,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-thread-write");
    let result = service.start_thread(&StartThreadRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("thread-start:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id,
        corpus_ref: input.display().to_string(),
        expected_version: None,
        title: title.to_string(),
        current_step: current_step.to_string(),
        next_command: next_command.to_string(),
        thread_id: thread_id.map(str::to_string),
        branch_off_anchor: branch_off_anchor.map(str::to_string),
        artifact_ref: artifact_ref.map(str::to_string),
    })?;
    print_thread_action("thread-start", &result.resource);
    Ok(())
}

fn thread_push_tangent_command(
    input: &PathBuf,
    title: &str,
    current_step: &str,
    next_command: &str,
    thread_id: Option<&str>,
    branch_off_anchor: Option<&str>,
    artifact_ref: Option<&str>,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-thread-write");
    let result = service.push_tangent(&PushTangentRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("thread-push-tangent:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id,
        corpus_ref: input.display().to_string(),
        expected_version: None,
        title: title.to_string(),
        current_step: current_step.to_string(),
        next_command: next_command.to_string(),
        thread_id: thread_id.map(str::to_string),
        branch_off_anchor: branch_off_anchor.map(str::to_string),
        artifact_ref: artifact_ref.map(str::to_string),
    })?;
    print_thread_action("thread-push-tangent", &result.resource);
    Ok(())
}

fn thread_checkpoint_command(
    input: &PathBuf,
    current_step: &str,
    next_command: &str,
    branch_off_anchor: Option<&str>,
    artifact_ref: Option<&str>,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-thread-write");
    let result = service.checkpoint_thread(&CheckpointThreadRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("thread-checkpoint:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id,
        corpus_ref: input.display().to_string(),
        expected_version: None,
        current_step: current_step.to_string(),
        next_command: next_command.to_string(),
        branch_off_anchor: branch_off_anchor.map(str::to_string),
        artifact_ref: artifact_ref.map(str::to_string),
    })?;
    print_thread_action("thread-checkpoint", &result.resource);
    Ok(())
}

fn thread_archive_command(
    input: &PathBuf,
    thread_id: Option<&str>,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-thread-write");
    let result = service.archive_thread(&ArchiveThreadRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("thread-archive:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id,
        corpus_ref: input.display().to_string(),
        expected_version: None,
        thread_id: thread_id.map(str::to_string),
    })?;
    print_thread_action("thread-archive", &result.resource);
    Ok(())
}

fn thread_pop_command(input: &PathBuf, mutation_id: Option<&str>, actor_id: Option<&str>) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-thread-write");
    let result = service.pop_thread(&PopThreadRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("thread-pop:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id,
        corpus_ref: input.display().to_string(),
        expected_version: None,
    })?;
    print_thread_action("thread-pop", &result.resource);
    Ok(())
}

fn thread_list_command(input: &PathBuf) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    print!("{}", thread_list(&store)?);
    Ok(())
}

fn backend_status_command(input: &PathBuf) -> Result<()> {
    let service = WriteService::from_input(input);
    print_backend_status(&service.backend_status());
    Ok(())
}

fn backend_recover_validate_command(input: &PathBuf, assert_clean: bool) -> Result<()> {
    let service = WriteService::from_input(input);
    let report = service.validate_recovery()?;
    print_recovery_report(&report);
    if assert_clean
        && (!report.state_matches_log || !report.manifest_matches_paths || report.invariant_violations > 0)
    {
        bail!("backend recovery validation reported one or more mismatches");
    }
    Ok(())
}

fn thread_claim_command(
    input: &PathBuf,
    thread_id: Option<&str>,
    agent_id: &str,
    lease_seconds: i64,
    claim_token: Option<&str>,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-thread-write");
    let result = service.claim_thread(&ClaimThreadRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("thread-claim:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id,
        corpus_ref: input.display().to_string(),
        expected_version: None,
        thread_id: thread_id.map(str::to_string),
        agent_id: agent_id.to_string(),
        lease_seconds,
        claim_token: claim_token.map(str::to_string),
    })?;
    print_claim_action("thread-claim", &result.resource);
    Ok(())
}

fn thread_heartbeat_command(
    input: &PathBuf,
    thread_id: Option<&str>,
    agent_id: &str,
    claim_token: &str,
    lease_seconds: i64,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-thread-write");
    let result = service.heartbeat_thread_claim(&HeartbeatThreadClaimRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("thread-heartbeat:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id,
        corpus_ref: input.display().to_string(),
        expected_version: None,
        thread_id: thread_id.map(str::to_string),
        agent_id: agent_id.to_string(),
        claim_token: claim_token.to_string(),
        lease_seconds,
    })?;
    print_claim_action("thread-heartbeat", &result.resource);
    Ok(())
}

fn thread_release_command(
    input: &PathBuf,
    thread_id: Option<&str>,
    agent_id: &str,
    claim_token: &str,
    reason: Option<&str>,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-thread-write");
    let result = service.release_thread_claim(&ReleaseThreadClaimRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("thread-release:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id,
        corpus_ref: input.display().to_string(),
        expected_version: None,
        thread_id: thread_id.map(str::to_string),
        agent_id: agent_id.to_string(),
        claim_token: claim_token.to_string(),
        release_reason: reason.map(str::to_string),
    })?;
    print_claim_action("thread-release", &result.resource);
    Ok(())
}

fn smartlist_inspect_command(input: &PathBuf, path: &str, depth: usize) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    print!("{}", smartlist_inspect(&store, path, depth)?);
    Ok(())
}

fn snapshot_diff_command(left: &PathBuf, right: &PathBuf) -> Result<()> {
    let (left_store, left_resolved) = import_snapshot_file(left)?;
    let (right_store, right_resolved) = import_snapshot_file(right)?;
    println!("left_snapshot_source={}", left_resolved.display());
    println!("right_snapshot_source={}", right_resolved.display());
    print!("{}", diff_snapshots(&left_store, &right_store)?);
    Ok(())
}

fn corpus_summary_command(input: &PathBuf) -> Result<()> {
    let corpus = import_materialized_corpus(input)?;
    print!("{}", corpus_summary(&corpus));
    Ok(())
}

fn list_cards_command(input: &PathBuf, state: Option<CardState>) -> Result<()> {
    let corpus = import_materialized_corpus(input)?;
    print!("{}", list_cards(&corpus, state));
    Ok(())
}

fn show_card_command(input: &PathBuf, card_id: &str) -> Result<()> {
    let corpus = import_materialized_corpus(input)?;
    print!("{}", show_card(&corpus, card_id)?);
    Ok(())
}

fn list_binders_command(input: &PathBuf, contains: Option<&str>) -> Result<()> {
    let corpus = import_materialized_corpus(input)?;
    print!("{}", list_binders(&corpus, contains));
    Ok(())
}

fn show_binder_command(input: &PathBuf, binder_id: &str) -> Result<()> {
    let corpus = import_materialized_corpus(input)?;
    print!("{}", show_binder(&corpus, binder_id)?);
    Ok(())
}

fn query_cards_command(
    input: &PathBuf,
    query: &str,
    top: usize,
    binder_filters: Option<&str>,
    seed_card: Option<&str>,
    state_filter: Option<CardState>,
    include_retracted: bool,
    explain: bool,
    context_options: QueryContextOptions,
    route_replay: Option<&PathBuf>,
    route_memory_bias_options: RouteMemoryBiasOptions,
) -> Result<()> {
    let corpus = import_materialized_corpus(input)?;
    let route_memory = if let Some(route_replay) = route_replay {
        let replay_records = load_route_replay_records(route_replay)?;
        Some(RouteMemoryStore::from_replay_records(&replay_records))
    } else {
        None
    };
    print!(
        "{}",
        run_query_cards(
            &corpus,
            query,
            top,
            binder_filters,
            seed_card,
            state_filter,
            include_retracted,
            explain,
            context_options,
            route_memory.as_ref(),
            &route_memory_bias_options,
        )?
    );
    Ok(())
}

fn route_replay_command(
    input: &PathBuf,
    replay: &PathBuf,
    out: &PathBuf,
    top: usize,
    route_memory_bias_options: RouteMemoryBiasOptions,
) -> Result<()> {
    let corpus = import_materialized_corpus(input)?;
    let outputs = load_and_run_route_replay(&corpus, replay, out, top, &route_memory_bias_options)?;
    let improved = outputs
        .iter()
        .filter(|output| output.delta == "improved-weak" || output.delta == "top1-promoted")
        .count();
    let regressed = outputs
        .iter()
        .filter(|output| output.delta == "regressed-weak" || output.delta == "top1-demoted")
        .count();
    let unchanged = outputs.len().saturating_sub(improved + regressed);
    println!(
        "route-replay: {} cases -> improved={} regressed={} unchanged={}",
        outputs.len(),
        improved,
        regressed,
        unchanged
    );
    println!("output: {}", out.display());
    Ok(())
}

fn agent_query_command(
    input: &PathBuf,
    query: &str,
    top: usize,
    explain: bool,
    json: bool,
    binder_filters: Option<&str>,
    seed_card: Option<String>,
    state_filter: Option<CardState>,
    include_retracted: bool,
    context_options: QueryContextOptions,
    route_replay: Option<&PathBuf>,
    route_memory_bias_options: RouteMemoryBiasOptions,
    record_route: bool,
    actor_id: Option<&str>,
    legacy_route_mirror: bool,
    include_latent: bool,
) -> Result<()> {
    let mut corpus = import_materialized_corpus(input)?;
    let route_memory = load_agent_route_memory(input, route_replay)?;
    let actor_id = actor_id
        .map(str::to_string)
        .or_else(|| std::env::var("AMS_ACTOR_ID").ok())
        .unwrap_or_else(|| "rust-agent-query".to_string());
    let result = run_agent_query(
        &mut corpus,
        &AgentQueryRequest {
            query: query.to_string(),
            top,
            binder_filters: parse_multi_value(binder_filters),
            seed_card,
            state_filter,
            include_retracted,
            explain,
            context_options,
            route_memory,
            route_memory_bias_options,
            include_latent,
            touch: true,
        },
    )?;
    // Output results first — reads must never be blocked by write-lock contention.
    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        print!("{}", result.markdown);
    }
    // Write side-effects (route recording, freshness) are best-effort.
    // If the write lock is held, skip immediately — do not block the read path.
    let service = WriteService::from_input(input);
    if service.is_locked() {
        eprintln!("WARNING: write lock held — skipping route-memory and freshness writes");
    } else {
        if !result.freshness_actions.is_empty() {
            if let Err(e) = apply_freshness_actions(&service, input, &actor_id, &result.freshness_actions) {
                eprintln!("WARNING: freshness write skipped ({})", e);
            }
        }
        if record_route {
            if let Some(episode) = result.route_episode.as_ref() {
                let mutation_id = format!("route-memory:{}:{}", actor_id, uuid::Uuid::new_v4());
                if let Err(e) = service.try_record_route_episode(&RecordRouteEpisodeRequest {
                mutation_id,
                actor_id: actor_id.clone(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                episode: episode.clone(),
                legacy_mirror_path: if legacy_route_mirror {
                    Some(default_route_memory_path(input))
                } else {
                    None
                },
            }) {
                eprintln!("WARNING: route-memory write skipped ({})", e);
            }
            }
        }
    }
    Ok(())
}

fn current_project_name() -> String {
    if let Ok(value) = std::env::var("AMS_REPO_NAME")
        .or_else(|_| std::env::var("AMS_PROJECT_NAME"))
        .or_else(|_| std::env::var("AMS_PRODUCT_NAME"))
    {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }

    if let Ok(cwd) = std::env::current_dir() {
        if let Some(name) = cwd.file_name().and_then(|n| n.to_str()) {
            let trimmed = name.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }

    "NetworkGraphMemory".to_string()
}

fn unix_storage_slug(name: &str) -> String {
    let mut slug = String::with_capacity(name.len());
    let mut last_dash = false;

    for ch in name.chars() {
        let normalized = if ch.is_ascii_alphanumeric() || ch == '.' || ch == '_' || ch == '-' {
            ch.to_ascii_lowercase()
        } else {
            '-'
        };

        if normalized == '-' {
            if !last_dash && !slug.is_empty() {
                slug.push('-');
            }
            last_dash = true;
            continue;
        }

        slug.push(normalized);
        last_dash = false;
    }

    while slug.ends_with('-') || slug.ends_with('.') {
        slug.pop();
    }

    if slug.is_empty() {
        "networkgraphmemory".to_string()
    } else {
        slug
    }
}

/// Resolve a corpus enum to the `.memory.jsonl` path, checking persistent and legacy locations.
fn resolve_corpus_path(corpus: CliCorpus) -> Result<PathBuf> {
    let project_name = current_project_name();
    let relative = match corpus {
        CliCorpus::All => PathBuf::from("all-agents-sessions").join("all-agents-sessions.memory.jsonl"),
        CliCorpus::Project => PathBuf::from("per-project")
            .join(&project_name)
            .join(format!("{}.memory.jsonl", project_name)),
        CliCorpus::Claude => PathBuf::from("all-claude-projects").join("all-claude-projects.memory.jsonl"),
        CliCorpus::Codex => PathBuf::from("all-codex-sessions").join("all-codex-sessions.memory.jsonl"),
    };

    // Check persistent location first (LOCALAPPDATA or AMS_OUTPUT_ROOT)
    let persistent_root = if let Ok(root) = std::env::var("AMS_OUTPUT_ROOT") {
        PathBuf::from(root)
    } else if let Ok(local_app_data) = std::env::var("LOCALAPPDATA") {
        PathBuf::from(local_app_data).join(&project_name).join("agent-memory")
    } else if let Ok(home) = std::env::var("USERPROFILE").or_else(|_| std::env::var("HOME")) {
        PathBuf::from(home)
            .join(format!(".{}", unix_storage_slug(&project_name)))
            .join("agent-memory")
    } else {
        PathBuf::from(format!(".{}", unix_storage_slug(&project_name))).join("agent-memory")
    };

    let persistent = persistent_root.join(&relative);
    if persistent.exists() {
        return Ok(persistent);
    }

    // Legacy fallback: scripts/output/
    let legacy = PathBuf::from("scripts").join("output").join(&relative);
    if legacy.exists() {
        return Ok(legacy);
    }

    // Return persistent path (will error at import time with a clear message)
    Ok(persistent)
}

/// Resolve the factories DB path.
fn factories_db_path() -> PathBuf {
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()));
    // Relative to repo root
    let repo_path = PathBuf::from("shared-memory/system-memory/factories/factories.memory.jsonl");
    if repo_path.exists() {
        return repo_path;
    }
    if let Some(dir) = exe_dir {
        let candidate = dir.join("../../..").join(&repo_path);
        if candidate.exists() {
            return candidate;
        }
    }
    repo_path
}

/// Recall command: queries both primary corpus and factories, picks best result.
fn recall_command(
    input: &PathBuf,
    query: &str,
    top: usize,
    explain: bool,
    json_output: bool,
    actor_id: Option<&str>,
    record_route: bool,
) -> Result<()> {
    let actor = actor_id
        .map(str::to_string)
        .or_else(|| std::env::var("AMS_ACTOR_ID").ok())
        .unwrap_or_else(|| "rust-recall".to_string());

    let factories = factories_db_path();

    // Query both sources and rank results
    let sources: Vec<(&str, &PathBuf)> = if factories.exists() {
        vec![("corpus", input), ("factories", &factories)]
    } else {
        vec![("corpus", input)]
    };

    let mut best_source = "";
    let mut best_score: f64 = f64::NEG_INFINITY;
    let mut best_result = None;

    for (source_name, source_input) in &sources {
        let corpus_result = import_materialized_corpus(source_input);
        let mut corpus = match corpus_result {
            Ok(c) => c,
            Err(_) => continue,
        };
        let route_memory = load_agent_route_memory(source_input, None).unwrap_or(None);
        let result = run_agent_query(
            &mut corpus,
            &AgentQueryRequest {
                query: query.to_string(),
                top,
                binder_filters: vec![],
                seed_card: None,
                state_filter: None,
                include_retracted: false,
                explain,
                context_options: QueryContextOptions {
                    current_node_id: None,
                    parent_node_id: None,
                    grandparent_node_id: None,
                    agent_role: None,
                    mode: None,
                    failure_bucket: None,
                    active_artifacts: vec![],
                    traversal_budget: 3,
                    no_active_thread_context: false,
                },
                route_memory,
                route_memory_bias_options: RouteMemoryBiasOptions::default(),
                include_latent: true,
                touch: true,
            },
        )?;

        // Rank: hits contribute most, then short-term, then fallback
        let mut score: f64 = 0.0;
        if !result.hits.is_empty() {
            score += 100.0 + result.hits[0].score;
            score += result.hits.len() as f64 * 10.0;
        }
        if !result.short_term.is_empty() {
            score += result.short_term.len() as f64 * 5.0;
        }
        if !result.fallback.is_empty() {
            score += result.fallback.len() as f64 * 3.0;
        }
        if *source_name == "factories" {
            score += 5.0;
        }

        if score > best_score {
            best_score = score;
            best_source = source_name;
            best_result = Some((result, (*source_input).clone()));
        }
    }

    let Some((result, best_input)) = best_result else {
        bail!("no corpus available for recall query");
    };

    // Output results first — reads must never be blocked by write-lock contention.
    if json_output {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        print!("{}", result.markdown);
    }

    if best_source != "corpus" {
        eprintln!("# RecallSource\nsource={best_source}");
    }

    // Write side-effects (route recording, freshness) are best-effort.
    // If the write lock is held, skip immediately — do not block the read path.
    let service = WriteService::from_input(&best_input);
    if service.is_locked() {
        eprintln!("WARNING: write lock held — skipping route-memory and freshness writes");
    } else {
        if record_route {
            if let Some(episode) = result.route_episode.as_ref() {
                let mutation_id = format!("route-memory:{}:{}", actor, uuid::Uuid::new_v4());
                if let Err(e) = service.try_record_route_episode(&RecordRouteEpisodeRequest {
                    mutation_id,
                    actor_id: actor.clone(),
                    corpus_ref: best_input.display().to_string(),
                    expected_version: None,
                    episode: episode.clone(),
                    legacy_mirror_path: Some(default_route_memory_path(&best_input)),
                }) {
                    eprintln!("WARNING: route-memory write skipped ({})", e);
                }
            }
        }

        if !result.freshness_actions.is_empty() {
            if let Err(e) = apply_freshness_actions(&service, &best_input, &actor, &result.freshness_actions) {
                eprintln!("WARNING: freshness write skipped ({})", e);
            }
        }
    }

    Ok(())
}

fn apply_freshness_actions(
    service: &WriteService,
    input: &PathBuf,
    actor_id: &str,
    actions: &[FreshnessWriteAction],
) -> Result<()> {
    for action in actions {
        match action {
            FreshnessWriteAction::CreateBucket { path, durable } => {
                service.create_smartlist_bucket(&CreateSmartListBucketRequest {
                    mutation_id: format!("freshness:{}:{}", actor_id, uuid::Uuid::new_v4()),
                    actor_id: actor_id.to_string(),
                    corpus_ref: input.display().to_string(),
                    expected_version: None,
                    path: path.clone(),
                    durable: *durable,
                    created_by: actor_id.to_string(),
                })?;
            }
            FreshnessWriteAction::SetBucketFields { path, fields } => {
                service.set_smartlist_bucket_fields(&SetSmartListBucketFieldsRequest {
                    mutation_id: format!("freshness:{}:{}", actor_id, uuid::Uuid::new_v4()),
                    actor_id: actor_id.to_string(),
                    corpus_ref: input.display().to_string(),
                    expected_version: None,
                    path: path.clone(),
                    fields: fields.clone(),
                    created_by: actor_id.to_string(),
                })?;
            }
            FreshnessWriteAction::AttachMember { path, member_ref } => {
                service.attach_smartlist_member(&AttachSmartListMemberRequest {
                    mutation_id: format!("freshness:{}:{}", actor_id, uuid::Uuid::new_v4()),
                    actor_id: actor_id.to_string(),
                    corpus_ref: input.display().to_string(),
                    expected_version: None,
                    path: path.clone(),
                    member_ref: member_ref.clone(),
                    created_by: actor_id.to_string(),
                })?;
            }
            FreshnessWriteAction::MoveMember {
                source_path,
                target_path,
                member_ref,
                before_member_ref,
            } => {
                service.move_smartlist_member(&MoveSmartListMemberRequest {
                    mutation_id: format!("freshness:{}:{}", actor_id, uuid::Uuid::new_v4()),
                    actor_id: actor_id.to_string(),
                    corpus_ref: input.display().to_string(),
                    expected_version: None,
                    source_path: source_path.clone(),
                    target_path: target_path.clone(),
                    member_ref: member_ref.clone(),
                    before_member_ref: before_member_ref.clone(),
                    created_by: actor_id.to_string(),
                })?;
            }
        }
    }
    Ok(())
}

fn load_agent_route_memory(input: &PathBuf, route_replay: Option<&PathBuf>) -> Result<Option<RouteMemoryStore>> {
    let mut episodes = Vec::new();
    let write_service = WriteService::from_input(input);
    let authoritative_log_path = write_service.paths().log_path.clone();
    if authoritative_log_path.exists() {
        episodes.extend(write_service.load_route_episodes()?);
    } else {
        let route_memory_path = crate::default_route_memory_path(input);
        if route_memory_path.exists() {
            episodes.extend(load_route_episode_entries(&route_memory_path)?);
        }
    }
    if let Some(route_replay) = route_replay {
        let replay_records = load_route_replay_records(route_replay)?;
        episodes.extend(
            replay_records
                .into_iter()
                .flat_map(|record| record.episodes.into_iter()),
        );
    }
    if episodes.is_empty() {
        Ok(None)
    } else {
        Ok(Some(RouteMemoryStore::from_episodes(episodes)))
    }
}

fn record_route_episode_command(
    input: &PathBuf,
    mutation_id: &str,
    actor_id: &str,
    episode_json: Option<&str>,
    episode_file: Option<&PathBuf>,
    expected_version: Option<u64>,
    mirror_legacy: bool,
) -> Result<()> {
    let episode_raw = match (episode_json, episode_file) {
        (Some(raw), None) => raw.to_string(),
        (None, Some(path)) => fs::read_to_string(path)
            .with_context(|| format!("failed to read --episode-file '{}'", path.display()))?,
        (Some(_), Some(_)) => bail!("pass only one of --episode-json or --episode-file"),
        (None, None) => bail!("one of --episode-json or --episode-file is required"),
    };
    let episode =
        serde_json::from_str(&episode_raw).context("failed to parse route episode input as RouteReplayEpisodeEntry")?;
    let service = WriteService::from_input(input);
    let result = service.record_route_episode(&RecordRouteEpisodeRequest {
        mutation_id: mutation_id.to_string(),
        actor_id: actor_id.to_string(),
        corpus_ref: input.display().to_string(),
        expected_version,
        episode,
        legacy_mirror_path: if mirror_legacy {
            Some(default_route_memory_path(input))
        } else {
            None
        },
    })?;
    println!("applied={}", result.applied);
    println!("version={}", result.version);
    println!("mirrored_legacy={}", result.mirrored_legacy);
    println!("authoritative_log={}", service.paths().log_path.display());
    println!("authoritative_state={}", service.paths().state_path.display());
    Ok(())
}

fn route_state_compare_command(input: &PathBuf) -> Result<()> {
    let service = WriteService::from_input(input);
    let legacy_path = default_route_memory_path(input);
    let comparison = service.compare_with_legacy_route_sidecar(&legacy_path)?;
    println!("authoritative_log={}", service.paths().log_path.display());
    println!("legacy_sidecar={}", legacy_path.display());
    println!("authoritative_events={}", comparison.authoritative_events);
    println!("legacy_events={}", comparison.legacy_events);
    println!("matches={}", comparison.matches);
    Ok(())
}

fn smartlist_create_command(
    input: &PathBuf,
    path: &str,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
    durable: bool,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let result = service.create_smartlist_bucket(&CreateSmartListBucketRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("smartlist-bucket:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        path: path.to_string(),
        durable,
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("path={}", result.resource.path);
    println!("object_id={}", result.resource.object_id);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn smartlist_note_command(
    input: &PathBuf,
    title: &str,
    text: &str,
    buckets: Option<&str>,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
    note_id: Option<&str>,
    durable: bool,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let mutation_id = mutation_id
        .map(str::to_string)
        .unwrap_or_else(|| format!("smartlist-note:{}:{}", actor_id, uuid::Uuid::new_v4()));
    let result = service.create_smartlist_note(&CreateSmartListNoteRequest {
        mutation_id: mutation_id.clone(),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        title: title.to_string(),
        text: text.to_string(),
        bucket_paths: parse_multi_value(buckets),
        durable,
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
        note_id: note_id.map(str::to_string).or_else(|| Some(default_note_id_for_mutation(&mutation_id))),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("note_id={}", result.resource.note_id);
    println!("bucket_paths={}", if result.resource.bucket_paths.is_empty() { "<root>".to_string() } else { result.resource.bucket_paths.join(",") });
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn smartlist_attach_command(
    input: &PathBuf,
    path: &str,
    member_ref: &str,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let result = service.attach_smartlist_member(&AttachSmartListMemberRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("smartlist-attach:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        path: path.to_string(),
        member_ref: member_ref.to_string(),
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("path={}", result.resource.path);
    println!("member_object_id={}", result.resource.member_object_id);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn smartlist_attach_before_command(
    input: &PathBuf,
    path: &str,
    member_ref: &str,
    before_member_ref: &str,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let result = service.insert_smartlist_member_before(&InsertSmartListMemberBeforeRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("smartlist-attach-before:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        path: path.to_string(),
        member_ref: member_ref.to_string(),
        before_member_ref: before_member_ref.to_string(),
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("path={}", result.resource.path);
    println!("member_object_id={}", result.resource.member_object_id);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn smartlist_detach_command(
    input: &PathBuf,
    path: &str,
    member_ref: &str,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let result = service.detach_smartlist_member(&DetachSmartListMemberRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("smartlist-detach:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        path: path.to_string(),
        member_ref: member_ref.to_string(),
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("path={}", result.resource.path);
    println!("member_object_id={}", result.resource.member_object_id);
    println!("removed={}", result.resource.removed);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn smartlist_move_command(
    input: &PathBuf,
    source_path: &str,
    target_path: &str,
    member_ref: &str,
    before_member_ref: Option<&str>,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let result = service.move_smartlist_member(&MoveSmartListMemberRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("smartlist-move:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        source_path: source_path.to_string(),
        target_path: target_path.to_string(),
        member_ref: member_ref.to_string(),
        before_member_ref: before_member_ref.map(str::to_string),
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("source_path={}", result.resource.source_path);
    println!("target_path={}", result.resource.target_path);
    println!("member_object_id={}", result.resource.member_object_id);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn smartlist_bucket_set_command(
    input: &PathBuf,
    path: &str,
    fields: &[String],
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let parsed_fields = parse_fields(fields)?;
    let result = service.set_smartlist_bucket_fields(&SetSmartListBucketFieldsRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("smartlist-bucket-set:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        path: path.to_string(),
        fields: parsed_fields.clone(),
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("path={}", result.resource.path);
    for (key, value) in parsed_fields {
        println!("field_{key}={value}");
    }
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn smartlist_rollup_command(
    input: &PathBuf,
    path: &str,
    summary: &str,
    scope: &str,
    stop_hint: Option<&str>,
    child_highlights: &[String],
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
    durable: bool,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let result = service.set_smartlist_rollup(&SetSmartListRollupRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("smartlist-rollup:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        path: path.to_string(),
        summary: summary.to_string(),
        scope: scope.to_string(),
        stop_hint: stop_hint.map(str::to_string),
        child_highlights: parse_child_highlights(child_highlights)?,
        durable,
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("rollup_id={}", result.resource.rollup_id);
    println!("bucket_path={}", result.resource.bucket_path);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn smartlist_visibility_command(
    input: &PathBuf,
    path: &str,
    visibility: &str,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    recursive: bool,
    include_notes: bool,
    include_rollups: bool,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let result = service.set_smartlist_visibility(&SetSmartListVisibilityRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("smartlist-visibility:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id,
        corpus_ref: input.display().to_string(),
        expected_version: None,
        path: path.to_string(),
        visibility: visibility.to_string(),
        recursive,
        include_notes,
        include_rollups,
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("path={}", result.resource.path);
    println!("retrieval_visibility={}", result.resource.retrieval_visibility);
    println!("buckets_updated={}", result.resource.buckets_updated);
    println!("notes_updated={}", result.resource.notes_updated);
    println!("rollups_updated={}", result.resource.rollups_updated);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn smartlist_memberships_command(input: &PathBuf, object_id: &str) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let result = list_memberships(&store, object_id);
    println!("object_id={}", result.object_id);
    println!("count={}", result.bucket_paths.len());
    for path in &result.bucket_paths {
        println!("bucket_path={}", path);
    }
    Ok(())
}

fn smartlist_category_create_command(
    input: &PathBuf,
    name: &str,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let result = service.create_smartlist_category(&CreateSmartListCategoryRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("smartlist-category-create:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        name: name.to_string(),
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("name={}", result.resource.name);
    println!("bucket_path={}", result.resource.bucket_path);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn smartlist_category_list_command(input: &PathBuf) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let categories = list_categories(&store);
    println!("count={}", categories.len());
    for cat in &categories {
        println!("category={} path={} members={}", cat.name, cat.bucket_path, cat.member_count);
    }
    Ok(())
}

fn smartlist_category_attach_command(
    input: &PathBuf,
    object_id: &str,
    category: &str,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let result = service.attach_to_smartlist_category(&AttachSmartListCategoryRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("smartlist-category-attach:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        object_id: object_id.to_string(),
        category: category.to_string(),
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("path={}", result.resource.path);
    println!("member_object_id={}", result.resource.member_object_id);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn smartlist_set_ordering_command(
    input: &PathBuf,
    path: &str,
    policy: &str,
    direction: &str,
    tie_breaker: Option<&str>,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let result = service.set_smartlist_ordering_policy(&SetSmartListOrderingPolicyRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("smartlist-set-ordering:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        path: path.to_string(),
        policy: policy.to_string(),
        direction: direction.to_string(),
        tie_breaker: tie_breaker.map(str::to_string),
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("path={}", result.resource.path);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn smartlist_browse_command(
    input: &PathBuf,
    path: Option<&str>,
    category: Option<&str>,
    tier: Option<&str>,
) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let items = match (path, category, tier) {
        (Some(p), None, None) => browse_bucket(&store, p)?,
        (None, Some(c), None) => browse_category(&store, c)?,
        (None, None, Some(t)) => browse_tier(&store, t)?,
        (None, Some(c), Some(t)) => browse_category_by_tier(&store, c, t)?,
        _ => bail!("specify exactly one of: --path, --category, --tier, or --category + --tier"),
    };
    println!("count={}", items.len());
    for item in &items {
        println!("object_id={} kind={} name={}", item.object_id, item.object_kind, item.display_name);
    }
    Ok(())
}

fn smartlist_recency_tiers_command(
    input: &PathBuf,
    bootstrap: bool,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    if bootstrap {
        let service = WriteService::from_input(input);
        let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
        let result = service.bootstrap_smartlist_recency_ladder(&BootstrapRecencyLadderRequest {
            mutation_id: mutation_id
                .map(str::to_string)
                .unwrap_or_else(|| format!("smartlist-recency-bootstrap:{}:{}", actor_id, uuid::Uuid::new_v4())),
            actor_id: actor_id.clone(),
            corpus_ref: input.display().to_string(),
            expected_version: None,
            created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
        })?;
        println!("applied={}", result.write.applied);
        println!("version={}", result.write.version);
        for tier in &result.resource {
            println!("tier={} path={} members={} max={} threshold_hours={}",
                tier.tier, tier.bucket_path, tier.member_count, tier.max_members, tier.rotation_threshold_hours);
        }
        println!("snapshot={}", service.paths().snapshot_path.display());
    } else {
        let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
        let tiers = list_recency_tiers(&store);
        println!("count={}", tiers.len());
        for tier in &tiers {
            println!("tier={} path={} members={} max={} threshold_hours={}",
                tier.tier, tier.bucket_path, tier.member_count, tier.max_members, tier.rotation_threshold_hours);
        }
    }
    Ok(())
}

fn smartlist_rotate_command(
    input: &PathBuf,
    dry_run: bool,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let result = service.rotate_smartlist_recency_tiers(&RotateRecencyTiersRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("smartlist-rotate:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        dry_run,
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("dry_run={}", result.resource.dry_run);
    println!("promotions={}", result.resource.promotions.len());
    for p in &result.resource.promotions {
        println!("promote={} from={} to={} reason={}", p.object_id, p.from_tier, p.to_tier, p.reason);
    }
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn smartlist_categorize_command(
    input: &PathBuf,
    dry_run: bool,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let result = service.categorize_smartlist_inbox(&CategorizeInboxRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("smartlist-categorize:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        dry_run,
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("dry_run={}", result.resource.dry_run);
    println!("processed={}", result.resource.processed);
    println!("categorized={}", result.resource.categorized);
    println!("already_categorized={}", result.resource.already_categorized);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn smartlist_gc_command(
    input: &PathBuf,
    dry_run: bool,
    default_ttl_hours: u64,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let result = service.smartlist_gc_sweep(&GcSweepRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("smartlist-gc:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        dry_run,
        default_ttl_hours,
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("dry_run={}", result.resource.dry_run);
    println!("removed={}", result.resource.removed.len());
    println!("restored_to_inbox={}", result.resource.restored_to_inbox);
    for r in &result.resource.removed {
        println!("gc_remove={} bucket={} reason={}", r.object_id, r.bucket_path, r.reason);
    }
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn smartlist_write_attach_command(
    input: &PathBuf,
    object_id: &str,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-smartlist-write");
    let result = service.smartlist_write_time_attach(&WriteTimeAttachRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("smartlist-write-attach:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        object_id: object_id.to_string(),
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    for path in &result.resource {
        println!("attached_to={}", path);
    }
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn dream_touch_command(
    input: &PathBuf,
    object_id: &str,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-dream");
    let result = service.dream_touch(&DreamTouchRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("dream-touch:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        object_id: object_id.to_string(),
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("object_id={}", result.resource.object_id);
    println!("lists_promoted={}", result.resource.lists_promoted);
    println!("shortcuts_added={}", result.resource.shortcuts_added);
    println!("shortcut_path={}", result.resource.shortcut_path);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn dream_schedule_command(
    input: &PathBuf,
    max_touches: usize,
    created_by: Option<&str>,
) -> Result<()> {
    use ams_core_kernel::{model::now_fixed, serialize_snapshot};

    let resolved = resolve_authoritative_snapshot_input(input);
    let (mut store, snapshot_path) = import_snapshot_file(&resolved)?;

    let now = now_fixed();
    let actor = created_by.unwrap_or("rust-dream-schedule");

    let result: DreamScheduleResult = dream_schedule(&mut store, actor, now, max_touches)?;

    // Persist the mutations back to the snapshot.
    let rendered = serialize_snapshot(&store)?;
    std::fs::write(&snapshot_path, rendered)
        .with_context(|| format!("failed to write snapshot '{}'", snapshot_path.display()))?;

    println!("touched={}", result.touched);
    println!("skipped={}", result.skipped);
    println!("stale={}", result.stale);
    println!("snapshot={}", snapshot_path.display());
    Ok(())
}

fn dream_cluster_command(
    input: &PathBuf,
    min_jaccard: f64,
    max_clusters: usize,
    _mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    use ams_core_kernel::{model::now_fixed, serialize_snapshot};

    let resolved = resolve_authoritative_snapshot_input(input);
    let (mut store, snapshot_path) = import_snapshot_file(&resolved)?;

    let now = now_fixed();
    let actor = created_by.unwrap_or_else(|| actor_id.unwrap_or("rust-dream-cluster"));

    let result: DreamClusterResult = dream_cluster(&mut store, min_jaccard, max_clusters, actor, now)?;

    // Persist the mutations back to the snapshot.
    let rendered = serialize_snapshot(&store)?;
    std::fs::write(&snapshot_path, rendered)
        .with_context(|| format!("failed to write snapshot '{}'", snapshot_path.display()))?;

    println!("sessions_scanned={}", result.sessions_scanned);
    println!("clusters_found={}", result.clusters_found);
    println!("index_path={}", result.index_path);
    for cluster in &result.clusters {
        println!(
            "cluster id={} members={} label={}",
            cluster.cluster_id,
            cluster.members.len(),
            cluster.label
        );
    }
    println!("snapshot={}", snapshot_path.display());
    Ok(())
}

fn dream_find_isolated_command(db: &PathBuf) -> Result<()> {
    let resolved = resolve_authoritative_snapshot_input(db);
    let (store, _snapshot_path) = import_snapshot_file(&resolved)?;

    let result = find_isolated_sessions(&store)?;

    // Print diagnostic counters to stderr so stdout stays clean for piping.
    eprintln!("clusters_inspected={}", result.clusters_inspected);
    eprintln!("memberships_counted={}", result.memberships_counted);
    eprintln!("isolated_count={}", result.isolated_session_ids.len());

    // Print one session GUID per line to stdout.
    for id in &result.isolated_session_ids {
        println!("{}", id);
    }

    Ok(())
}

fn dream_shortcut_command(
    input: &PathBuf,
    embeddings_path: &PathBuf,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    use ams_core_kernel::model::now_fixed;

    let resolved = resolve_authoritative_snapshot_input(input);
    let (mut store, snapshot_path) = import_snapshot_file(&resolved)?;

    let actor = created_by.unwrap_or_else(|| actor_id.unwrap_or("rust-dream-shortcut"));
    let now = now_fixed();

    // Load embeddings sidecar.
    let raw = fs::read_to_string(embeddings_path)
        .with_context(|| format!("failed to read embeddings file '{}'", embeddings_path.display()))?;
    let embeddings: EmbeddingsSidecar = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse embeddings file '{}'", embeddings_path.display()))?;

    let result = dream_shortcut(&mut store, &embeddings, actor, now)?;

    // Persist the mutations back to the snapshot.
    let rendered = serialize_snapshot(&store)?;
    std::fs::write(&snapshot_path, rendered)
        .with_context(|| format!("failed to write snapshot '{}'", snapshot_path.display()))?;

    println!("isolated_evaluated={}", result.isolated_evaluated);
    println!("shortcuts_added={}", result.shortcuts_added);
    println!("cache_hits={}", result.cache_hits);
    println!("embeddings_missing={}", result.embeddings_missing);
    println!("snapshot={}", snapshot_path.display());

    Ok(())
}

fn dream_generate_md_command(
    input: &PathBuf,
    out: &PathBuf,
    max_topics: usize,
    max_sessions: usize,
) -> Result<()> {
    use ams_core_kernel::model::now_fixed;

    let resolved = resolve_authoritative_snapshot_input(input);
    let (store, _snapshot_path) = import_snapshot_file(&resolved)?;

    let now = now_fixed();
    let result: DreamGenerateMdResult = dream_generate_md(&store, now, max_topics, max_sessions)?;

    std::fs::write(out, result.markdown.as_bytes())
        .with_context(|| format!("failed to write output '{}'", out.display()))?;

    println!("topics_written={}", result.topics_written);
    println!("sessions_written={}", result.sessions_written);
    println!("out={}", out.display());
    Ok(())
}

fn parity_validate_command(input: &PathBuf, cases: &PathBuf, out: Option<&PathBuf>) -> Result<()> {
    let corpus = import_materialized_corpus(input)?;
    let loaded_cases = load_parity_cases(cases)?;
    let cases_root = cases.parent().unwrap_or_else(|| std::path::Path::new("."));
    let reports = run_parity_validation(&corpus, &loaded_cases, cases_root)?;
    if let Some(out) = out {
        write_parity_reports(out, &reports)?;
    }

    let passed = reports.iter().filter(|report| report.passed).count();
    let failed = reports.len().saturating_sub(passed);
    println!("parity-validate: {} cases -> passed={} failed={}", reports.len(), passed, failed);
    for report in &reports {
        println!(
            "- {} passed={} top_ref={} scope_lens={}",
            report.case_name,
            report.passed,
            report.actual_top_ref.as_deref().unwrap_or("<none>"),
            report.actual_scope_lens
        );
        if !report.failures.is_empty() {
            println!("  failures: {}", report.failures.join(" | "));
        }
    }
    if let Some(out) = out {
        println!("output: {}", out.display());
    }
    Ok(())
}

fn shadow_validate_command(
    input: &PathBuf,
    cases: &PathBuf,
    out: Option<&PathBuf>,
    memoryctl_exe: Option<&PathBuf>,
    assert_match: bool,
) -> Result<()> {
    let loaded_cases = load_shadow_cases(cases)?;
    let cases_root = cases.parent().unwrap_or_else(|| std::path::Path::new("."));
    let reports = run_shadow_validation(input, &loaded_cases, cases_root, memoryctl_exe.map(|path| path.as_path()))?;
    if let Some(out) = out {
        write_shadow_reports(out, &reports)?;
    }

    let passed = reports.iter().filter(|report| report.passed).count();
    let failed = reports.len().saturating_sub(passed);
    println!("shadow-validate: {} cases -> passed={} failed={}", reports.len(), passed, failed);
    for report in &reports {
        println!(
            "- {} passed={} rust_top_lesson={} csharp_top_lesson={}",
            report.case_name,
            report.passed,
            report.rust.top_lesson_title.as_deref().unwrap_or("<none>"),
            report.csharp.top_lesson_title.as_deref().unwrap_or("<none>")
        );
        if !report.differences.is_empty() {
            println!("  differences: {}", report.differences.join(" | "));
        }
    }
    if let Some(out) = out {
        println!("output: {}", out.display());
    }
    if assert_match && failed > 0 {
        bail!(
            "shadow validation detected {} mismatched case(s); inspect the diff report{}",
            failed,
            out.map(|path| format!(" at '{}'", path.display()))
                .unwrap_or_default()
        );
    }
    Ok(())
}

fn parse_multi_value(raw: Option<&str>) -> Vec<String> {
    raw.into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn parse_child_highlights(values: &[String]) -> Result<Vec<SmartListRollupChild>> {
    values
        .iter()
        .map(|value| {
            let Some((path, summary)) = value.split_once('=') else {
                bail!("invalid --child-highlight '{}'; expected '<path>=<summary>'", value);
            };
            Ok(SmartListRollupChild {
                path: path.trim().to_string(),
                summary: summary.trim().to_string(),
            })
        })
        .collect()
}

fn parse_fields(values: &[String]) -> Result<std::collections::BTreeMap<String, String>> {
    values
        .iter()
        .map(|value| {
            let Some((key, field_value)) = value.split_once('=') else {
                bail!("invalid --field '{}'; expected '<key>=<value>'", value);
            };
            Ok((key.trim().to_string(), field_value.to_string()))
        })
        .collect()
}

fn resolve_actor_id(explicit: Option<&str>, fallback: &str) -> String {
    explicit
        .map(str::to_string)
        .or_else(|| std::env::var("AMS_ACTOR_ID").ok())
        .unwrap_or_else(|| fallback.to_string())
}

fn print_thread_action(action: &str, result: &ams_core_kernel::TaskGraphCommandResult) {
    println!("action={action}");
    println!("thread_id={}", result.thread.thread_id);
    println!("title={}", result.thread.title);
    println!("status={}", result.thread.status);
    println!("current_step={}", result.thread.current_step);
    println!("next_command={}", result.thread.next_command);
    if let Some(parent_thread_id) = result.thread.parent_thread_id.as_deref() {
        println!("parent_thread_id={parent_thread_id}");
    }
    if let Some(checkpoint) = result.checkpoint.as_ref() {
        println!("checkpoint_id={}", checkpoint.checkpoint_object_id);
    }
    if let Some(checkpoint) = result.resumed_checkpoint.as_ref() {
        println!("resumed_checkpoint_id={}", checkpoint.checkpoint_object_id);
    }
    println!(
        "active_thread={}",
        result
            .overview
            .active_thread
            .as_ref()
            .map(|thread| thread.thread_id.clone())
            .unwrap_or_else(|| "(none)".to_string())
    );
    println!("parked_threads={}", result.overview.parked_threads.len());
}

fn print_claim_action(action: &str, result: &TaskClaimCommandResult) {
    println!("action={action}");
    println!("thread_id={}", result.thread.thread_id);
    println!("title={}", result.thread.title);
    println!("status={}", result.thread.status);
    if let Some(active_claim) = result.thread.active_claim.as_ref() {
        println!("active_claim_agent={}", active_claim.agent_id);
        println!("active_claim_token={}", active_claim.claim_token);
        println!("active_claim_attempt={}", active_claim.attempt);
        println!("active_claim_lease_until={}", active_claim.lease_until.to_rfc3339());
    } else {
        println!("active_claim_agent=(none)");
    }
    if let Some(claim) = result.claim.as_ref() {
        println!("claim_status={}", claim.status);
        println!("claim_token={}", claim.claim_token);
        println!("claim_attempt={}", claim.attempt);
    }
    println!("claim_events={}", result.thread.claims.len());
    println!(
        "active_thread={}",
        result
            .overview
            .active_thread
            .as_ref()
            .map(|thread| thread.thread_id.clone())
            .unwrap_or_else(|| "(none)".to_string())
    );
    println!("parked_threads={}", result.overview.parked_threads.len());
}

fn print_backend_status(status: &WriteBackendStatus) {
    println!(
        "backend_mode={}",
        match status.backend_mode {
            ams_core_kernel::write_service::WriteBackendMode::LocalSibling => "local_sibling",
            ams_core_kernel::write_service::WriteBackendMode::SharedRoot => "shared_root",
        }
    );
    println!("corpus_key={}", status.corpus_key);
    println!("corpus_ref={}", status.corpus_ref);
    if let Some(root) = status.backend_root.as_ref() {
        println!("backend_root={}", root.display());
    } else {
        println!("backend_root=(none)");
    }
    println!("corpus_dir={}", status.corpus_dir.display());
    println!("snapshot_path={}", status.snapshot_path.display());
    println!("log_path={}", status.log_path.display());
    println!("state_path={}", status.state_path.display());
    println!("lock_path={}", status.lock_path.display());
    if let Some(manifest_path) = status.manifest_path.as_ref() {
        println!("manifest_path={}", manifest_path.display());
    } else {
        println!("manifest_path=(none)");
    }
}

fn print_recovery_report(report: &WriteRecoveryReport) {
    println!(
        "backend_mode={}",
        match report.backend_mode {
            ams_core_kernel::write_service::WriteBackendMode::LocalSibling => "local_sibling",
            ams_core_kernel::write_service::WriteBackendMode::SharedRoot => "shared_root",
        }
    );
    println!("corpus_key={}", report.corpus_key);
    println!("current_version={}", report.current_version);
    println!("log_events={}", report.log_events);
    println!("state_matches_log={}", report.state_matches_log);
    println!("manifest_matches_paths={}", report.manifest_matches_paths);
    println!("snapshot_exists={}", report.snapshot_exists);
    println!("invariant_violations={}", report.invariant_violations);
}

fn emit_invariants(store: &AmsStore) -> Result<()> {
    let violations = validate_invariants(store);
    if violations.is_empty() {
        println!("invariants=ok");
        return Ok(());
    }

    println!("invariants=failed");
    for violation in violations {
        println!("violation={} {}", violation.code, violation.message);
    }
    bail!("one or more invariants failed");
}

fn print_summary(label: &str, store: &AmsStore) {
    println!("mode={label}");
    println!("objects={}", store.objects().len());
    println!("containers={}", store.containers().len());
    println!("link_nodes={}", store.link_nodes().len());
}

#[cfg(test)]
mod tests {
    use std::fs;

    use ams_core_kernel::{
        append_route_episode_entry, default_route_memory_path, RouteReplayEpisodeEntry, RouteReplayEpisodeInput,
        RouteReplayFrameInput, RouteReplayRouteInput,
    };

    use super::*;

    fn make_episode(target_ref: &str) -> RouteReplayEpisodeEntry {
        RouteReplayEpisodeEntry {
            frame: RouteReplayFrameInput {
                scope_lens: "local-first-lineage".to_string(),
                agent_role: "implementer".to_string(),
                mode: "build".to_string(),
                lineage_node_ids: vec!["child-thread".to_string()],
                artifact_refs: None,
                failure_bucket: None,
            },
            route: RouteReplayRouteInput {
                ranking_source: "raw-lesson".to_string(),
                path: "scope_lens:local-first-lineage".to_string(),
                cost: 0.5,
                risk_flags: None,
            },
            episode: RouteReplayEpisodeInput {
                query_text: "search cache".to_string(),
                occurred_at: chrono::DateTime::parse_from_rfc3339("2026-03-13T22:00:00+00:00").unwrap(),
                weak_result: false,
                used_fallback: false,
                winning_target_ref: target_ref.to_string(),
                top_target_refs: vec![target_ref.to_string()],
                user_feedback: None,
                tool_outcome: None,
            },
            candidate_target_refs: vec![target_ref.to_string()],
            winning_target_ref: target_ref.to_string(),
        }
    }

    #[test]
    fn load_agent_route_memory_reads_sidecar_entries() {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("fixture.memory.jsonl");
        fs::write(&input, "").unwrap();
        let route_path = default_route_memory_path(&input);
        append_route_episode_entry(
            &route_path,
            &make_episode("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1"),
        )
        .unwrap();

        let store = load_agent_route_memory(&input, None).unwrap().expect("route store");
        let query_context = ams_core_kernel::QueryContext {
            lineage: vec![ams_core_kernel::LineageScope {
                level: "self".to_string(),
                object_id: "task-thread:child-thread".to_string(),
                node_id: "child-thread".to_string(),
                title: "Child thread".to_string(),
                current_step: String::new(),
                next_command: String::new(),
                branch_off_anchor: None,
                artifact_refs: Vec::new(),
            }],
            agent_role: "implementer".to_string(),
            mode: "build".to_string(),
            failure_bucket: None,
            active_artifacts: Vec::new(),
            traversal_budget: 3,
            source: "explicit".to_string(),
        };

        let biases = store.get_target_biases(&query_context, &RouteMemoryBiasOptions::default());
        assert_eq!(biases.len(), 1);
    }
}

fn fep_bootstrap_command(
    input: &PathBuf,
    episodes_path: &PathBuf,
    output: Option<&PathBuf>,
) -> Result<()> {
    let (mut snapshot, _path) = import_snapshot_file(input)?;
    let episodes =
        load_route_episode_entries(episodes_path).context("failed to load route episodes")?;

    println!(
        "FEP bootstrap: {} episodes, {} containers in snapshot",
        episodes.len(),
        snapshot.containers().len()
    );

    let report = run_fep_bootstrap(&mut snapshot, &episodes);
    println!("{}", report);

    let output_path = if let Some(out) = output {
        out.clone()
    } else {
        let stem = input
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output");
        let stem = stem.strip_suffix(".ams").unwrap_or(stem);
        input.with_file_name(format!("{}.bootstrapped.ams.json", stem))
    };

    let json = serialize_snapshot(&snapshot)?;
    fs::write(&output_path, json)
        .with_context(|| format!("failed to write bootstrapped snapshot to '{}'", output_path.display()))?;
    println!("output: {}", output_path.display());

    Ok(())
}

fn tool_outcome_priors_command(input: &PathBuf) -> Result<()> {
    let (snapshot, _path) = import_snapshot_file(input)?;
    let priors = load_tool_outcome_priors_from_snapshot(&snapshot);

    if priors.is_empty() {
        println!("No tool outcome priors found in snapshot.");
        return Ok(());
    }

    println!("Tool Outcome Priors ({} contexts):\n", priors.len());
    for (ctx_key, dist) in &priors {
        println!("  context: {} (n={})", ctx_key, dist.total_observations);
        for (outcome, param) in &dist.outcome_params {
            println!(
                "    {:<10} mean={:.3} variance={:.3}",
                outcome.to_string(),
                param.mean,
                param.variance
            );
        }
        println!();
    }

    Ok(())
}

fn predict_tool_outcome_command(
    input: &PathBuf,
    scope_lens: &str,
    agent_role: &str,
) -> Result<()> {
    let (snapshot, _path) = import_snapshot_file(input)?;
    let priors = load_tool_outcome_priors_from_snapshot(&snapshot);

    match predict_tool_outcome(&priors, scope_lens, agent_role) {
        Some(prediction) => {
            println!("Tool Outcome Prediction:");
            println!("  context:     {}", prediction.context_key);
            println!("  most likely: {}", prediction.most_likely);
            println!("  P(success):  {:.3}", prediction.success_probability);
            println!("  probabilities:");
            for (outcome, prob) in &prediction.outcome_probabilities {
                println!("    {:<10} {:.3}", outcome.to_string(), prob);
            }
        }
        None => {
            println!(
                "No priors found for context {}:{}",
                scope_lens, agent_role
            );
        }
    }

    Ok(())
}

fn fep_bootstrap_agent_tools_command(
    input: &PathBuf,
    output: Option<&PathBuf>,
) -> Result<()> {
    let (mut snapshot, _path) = import_snapshot_file(input)?;

    let tool_call_count = snapshot
        .objects()
        .values()
        .filter(|o| o.object_kind == "tool-call")
        .count();

    println!(
        "FEP agent-tool bootstrap: {} tool-call objects in snapshot",
        tool_call_count
    );

    let priors = bootstrap_agent_tool_priors(&snapshot);
    let total_calls: usize = priors.values().map(|d| d.total_observations).sum();
    let tools_count = priors.len();

    let keys_written = write_agent_tool_priors_to_snapshot(&mut snapshot, &priors);

    println!("Agent Tool Prior Bootstrap Report:");
    println!("  tools discovered:        {}", tools_count);
    println!("  total calls classified:  {}", total_calls);
    println!("  keys written to store:   {}", keys_written);

    for (tool_name, dist) in &priors {
        println!("\n  tool: {} (n={})", tool_name, dist.total_observations);
        for (outcome, param) in &dist.outcome_params {
            println!(
                "    {:<10} mean={:.3} variance={:.3}",
                outcome.to_string(),
                param.mean,
                param.variance,
            );
        }
    }

    let output_path = if let Some(out) = output {
        out.clone()
    } else {
        let stem = input
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output");
        let stem = stem.strip_suffix(".ams").unwrap_or(stem);
        input.with_file_name(format!("{}.agent-tools.ams.json", stem))
    };

    let json = serialize_snapshot(&snapshot)?;
    fs::write(&output_path, json)
        .with_context(|| format!("failed to write snapshot to '{}'", output_path.display()))?;
    println!("\noutput: {}", output_path.display());

    Ok(())
}

fn fep_detect_tool_anomalies_command(
    input: &PathBuf,
    since: &str,
    threshold: f64,
    output: Option<&PathBuf>,
) -> Result<()> {
    use chrono::{DateTime, FixedOffset, Utc};

    let (mut snapshot, _path) = import_snapshot_file(input)?;

    // Parse the `since` timestamp
    let since_ts: DateTime<FixedOffset> = if since == "last-run" {
        // Default to 24 hours ago when "last-run" is specified
        (Utc::now() - chrono::Duration::hours(24)).fixed_offset()
    } else {
        DateTime::parse_from_rfc3339(since)
            .with_context(|| format!("invalid --since timestamp: '{}'", since))?
    };

    // Load agent-tool priors from the snapshot
    let priors = load_agent_tool_priors_from_snapshot(&snapshot);
    if priors.is_empty() {
        println!("No agent-tool priors found in snapshot. Run fep-bootstrap-agent-tools first.");
        return Ok(());
    }

    println!(
        "Detecting anomalies: threshold={:.2}, since={}, priors for {} tools",
        threshold,
        since_ts.to_rfc3339(),
        priors.len(),
    );

    let anomalies = detect_tool_anomalies(&snapshot, &priors, since_ts, threshold);

    if anomalies.is_empty() {
        println!("No anomalies detected.");
    } else {
        println!("\n{} anomalies detected:\n", anomalies.len());
        for (i, a) in anomalies.iter().enumerate() {
            println!(
                "  {}. {} [{}] outcome={} FE={:.3} (threshold={:.2}, n={})",
                i + 1,
                a.tool_name,
                a.tool_use_id,
                a.outcome,
                a.free_energy,
                a.threshold,
                a.prior_total_observations,
            );
            for (outcome, mean) in &a.prior_outcome_means {
                println!("       prior {:<10} mean={:.3}", outcome, mean);
            }
        }

        // Emit SmartList notes for anomalies
        let now = Utc::now().fixed_offset();
        match emit_anomaly_notes(&mut snapshot, &anomalies, now) {
            Ok(notes) => {
                if !notes.is_empty() {
                    println!("\n  {} SmartList notes emitted.", notes.len());
                }
            }
            Err(e) => {
                eprintln!("  Warning: failed to emit anomaly notes: {}", e);
            }
        }
    }

    // Write output snapshot if requested
    if let Some(out) = output {
        let json = serialize_snapshot(&snapshot)?;
        fs::write(out, json)
            .with_context(|| format!("failed to write snapshot to '{}'", out.display()))?;
        println!("\noutput: {}", out.display());
    }

    Ok(())
}

fn fep_update_tool_belief_command(
    input: &PathBuf,
    tool_name: &str,
    outcome_str: &str,
    precision: f64,
    output: Option<&PathBuf>,
) -> Result<()> {
    let (mut snapshot, _path) = import_snapshot_file(input)?;

    let outcome = match outcome_str {
        "Success" | "success" => ToolOutcome::Success,
        "Weak" | "weak" => ToolOutcome::Weak,
        "Null" | "null" => ToolOutcome::Null,
        "Error" | "error" => ToolOutcome::Error,
        "Wasteful" | "wasteful" => ToolOutcome::Wasteful,
        other => bail!("unknown outcome '{}' (expected Success, Weak, Null, Error, Wasteful)", other),
    };

    let mut priors = load_agent_tool_priors_from_snapshot(&snapshot);

    let Some(distribution) = priors.get_mut(tool_name) else {
        bail!("no prior distribution found for tool '{}'", tool_name);
    };

    let before_mean = distribution
        .outcome_params
        .get(&outcome)
        .map(|p| p.mean)
        .unwrap_or(0.0);

    update_tool_outcome_beliefs(distribution, outcome, precision);

    let after_mean = distribution
        .outcome_params
        .get(&outcome)
        .map(|p| p.mean)
        .unwrap_or(0.0);

    println!(
        "Updated belief for {} toward {}: mean {:.4} -> {:.4} (precision={:.2}, n={})",
        tool_name, outcome_str, before_mean, after_mean, precision, distribution.total_observations,
    );

    // Write updated priors back
    write_agent_tool_priors_to_snapshot(&mut snapshot, &priors);

    let output_path = if let Some(out) = output {
        out.clone()
    } else {
        input.clone()
    };

    let json = serialize_snapshot(&snapshot)?;
    fs::write(&output_path, json)
        .with_context(|| format!("failed to write snapshot to '{}'", output_path.display()))?;
    println!("output: {}", output_path.display());

    Ok(())
}

fn fep_update_agent_tool_beliefs_command(
    input: &PathBuf,
    since: &str,
    precision: f64,
    output: Option<&PathBuf>,
) -> Result<()> {
    use chrono::{DateTime, FixedOffset, Utc};

    let (mut snapshot, _path) = import_snapshot_file(input)?;

    let since_ts: DateTime<FixedOffset> = if since == "last-run" {
        (Utc::now() - chrono::Duration::hours(24)).fixed_offset()
    } else {
        DateTime::parse_from_rfc3339(since)
            .with_context(|| format!("invalid --since timestamp: '{}'", since))?
    };

    // Load existing priors
    let mut priors = load_agent_tool_priors_from_snapshot(&snapshot);
    if priors.is_empty() {
        println!("No agent-tool priors found in snapshot. Run fep-bootstrap-agent-tools first.");
        return Ok(());
    }

    // Walk recent tool-call objects and apply belief updates
    let mut updates: usize = 0;
    let mut skipped_no_prior: usize = 0;
    let mut tool_update_counts: std::collections::BTreeMap<String, usize> =
        std::collections::BTreeMap::new();

    // Collect updates first (can't borrow snapshot mutably while iterating)
    let tool_calls: Vec<(String, ToolOutcome)> = snapshot
        .objects()
        .iter()
        .filter(|(_, obj)| {
            obj.object_kind == "tool-call" && obj.created_at > since_ts
        })
        .filter_map(|(_, obj)| {
            let prov = obj.semantic_payload.as_ref()?.provenance.as_ref()?;
            let tool_name = prov.get("tool_name")?.as_str()?.to_string();
            let is_error = prov
                .get("is_error")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let result_preview = prov
                .get("result_preview")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let outcome = classify_agent_tool_outcome(is_error, result_preview, &tool_name);
            Some((tool_name, outcome))
        })
        .collect();

    for (tool_name, outcome) in &tool_calls {
        if let Some(distribution) = priors.get_mut(tool_name) {
            update_tool_outcome_beliefs(distribution, *outcome, precision);
            updates += 1;
            *tool_update_counts.entry(tool_name.clone()).or_default() += 1;
        } else {
            skipped_no_prior += 1;
        }
    }

    println!(
        "FEP online belief update: {} tool-calls processed, {} updates applied, {} skipped (no prior)",
        tool_calls.len(),
        updates,
        skipped_no_prior,
    );
    for (tool, count) in &tool_update_counts {
        println!("  {}: {} updates", tool, count);
    }

    // Write updated priors back
    write_agent_tool_priors_to_snapshot(&mut snapshot, &priors);

    let output_path = if let Some(out) = output {
        out.clone()
    } else {
        input.clone()
    };

    let json = serialize_snapshot(&snapshot)?;
    fs::write(&output_path, json)
        .with_context(|| format!("failed to write snapshot to '{}'", output_path.display()))?;
    println!("output: {}", output_path.display());

    Ok(())
}

fn fep_decay_tool_priors_command(
    input: &PathBuf,
    decay_rate: f64,
    output: Option<&PathBuf>,
) -> Result<()> {
    let (mut snapshot, _path) = import_snapshot_file(input)?;

    let mut priors = load_agent_tool_priors_from_snapshot(&snapshot);
    if priors.is_empty() {
        println!("No agent-tool priors found in snapshot. Run fep-bootstrap-agent-tools first.");
        return Ok(());
    }

    let tools_count = priors.len();
    let params_decayed = decay_agent_tool_priors(&mut priors, decay_rate);

    println!(
        "FEP precision decay: {} tools, {} outcome params decayed (rate={})",
        tools_count, params_decayed, decay_rate,
    );

    write_agent_tool_priors_to_snapshot(&mut snapshot, &priors);

    let output_path = if let Some(out) = output {
        out.clone()
    } else {
        input.clone()
    };

    let json = serialize_snapshot(&snapshot)?;
    fs::write(&output_path, json)
        .with_context(|| format!("failed to write snapshot to '{}'", output_path.display()))?;
    println!("Decay output: {}", output_path.display());

    Ok(())
}

fn fep_bootstrap_duration_priors_command(
    input: &PathBuf,
    output: Option<&PathBuf>,
) -> Result<()> {
    let (mut snapshot, _path) = import_snapshot_file(input)?;

    let tool_call_count = snapshot
        .objects()
        .values()
        .filter(|o| o.object_kind == "tool-call")
        .count();

    println!(
        "FEP duration bootstrap: {} tool-call objects in snapshot",
        tool_call_count
    );

    let priors = bootstrap_tool_duration_priors(&snapshot);
    let tools_count = priors.len();
    let total_calls: usize = priors.values().map(|p| p.count).sum();
    let keys_written = write_tool_duration_priors_to_snapshot(&mut snapshot, &priors);

    println!("Tool Duration Prior Bootstrap Report:");
    println!("  tools with duration data: {}", tools_count);
    println!("  total calls with duration: {}", total_calls);
    println!("  keys written to store:    {}", keys_written);

    for (tool_name, prior) in &priors {
        println!(
            "\n  tool: {} (n={}) mean={:.2}s stddev={:.2}s",
            tool_name,
            prior.count,
            prior.mean_s,
            prior.variance_s.sqrt(),
        );
    }

    let output_path = if let Some(out) = output {
        out.clone()
    } else {
        let stem = input
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output");
        let stem = stem.strip_suffix(".ams").unwrap_or(stem);
        input.with_file_name(format!("{}.duration-priors.ams.json", stem))
    };

    let json = serialize_snapshot(&snapshot)?;
    fs::write(&output_path, json)
        .with_context(|| format!("failed to write snapshot to '{}'", output_path.display()))?;
    println!("\noutput: {}", output_path.display());

    Ok(())
}

fn fep_detect_slow_tools_command(
    input: &PathBuf,
    since: &str,
    threshold: f64,
) -> Result<()> {
    use chrono::{DateTime, FixedOffset, Utc};

    let (snapshot, _path) = import_snapshot_file(input)?;

    let since_ts: DateTime<FixedOffset> = if since == "last-run" {
        (Utc::now() - chrono::Duration::hours(24)).fixed_offset()
    } else {
        DateTime::parse_from_rfc3339(since)
            .with_context(|| format!("invalid --since timestamp: '{}'", since))?
    };

    let priors = load_tool_duration_priors_from_snapshot(&snapshot);
    if priors.is_empty() {
        println!("No duration priors found in snapshot. Run fep-bootstrap-duration-priors first.");
        return Ok(());
    }

    println!(
        "Detecting slow tools: threshold={:.2}, since={}, priors for {} tools",
        threshold,
        since_ts.to_rfc3339(),
        priors.len(),
    );

    let slow = detect_slow_tools(&snapshot, &priors, since_ts, threshold);

    if slow.is_empty() {
        println!("No slow tools detected.");
    } else {
        println!("\n{} slow tool calls detected:\n", slow.len());
        for (i, s) in slow.iter().enumerate() {
            println!(
                "  {}. {} [{}] observed={:.2}s (prior mean={:.2}s stddev={:.2}s n={}) FE={:.3}",
                i + 1,
                s.tool_name,
                s.tool_use_id,
                s.observed_s,
                s.prior_mean_s,
                s.prior_variance_s.sqrt(),
                s.prior_count,
                s.free_energy,
            );
        }
    }

    Ok(())
}

fn agent_pool_allocate_command(
    input: &PathBuf,
    agent_ref: &str,
    task_path: &str,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-agent-pool");
    let result = service.allocate_agent_pool(&AllocateAgentPoolRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("agent-pool-allocate:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        agent_ref: agent_ref.to_string(),
        task_path: task_path.to_string(),
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("agent_object_id={}", result.resource.agent_object_id);
    println!("task_path={}", result.resource.task_path);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn agent_pool_release_command(
    input: &PathBuf,
    agent_ref: &str,
    task_path: &str,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor_id = resolve_actor_id(actor_id, "rust-agent-pool");
    let result = service.release_agent_pool(&ReleaseAgentPoolRequest {
        mutation_id: mutation_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("agent-pool-release:{}:{}", actor_id, uuid::Uuid::new_v4())),
        actor_id: actor_id.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        agent_ref: agent_ref.to_string(),
        task_path: task_path.to_string(),
        created_by: created_by.unwrap_or(actor_id.as_str()).to_string(),
    })?;
    println!("applied={}", result.write.applied);
    println!("version={}", result.write.version);
    println!("agent_object_id={}", result.resource.agent_object_id);
    println!("task_path={}", result.resource.task_path);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn agent_pool_status_command(input: &PathBuf) -> Result<()> {
    let (store, _resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let status = ams_core_kernel::agent_pool_status(&store)?;
    println!("free_count={}", status.free_count);
    println!("allocated_count={}", status.allocated_count);
    let free_agents: Vec<String> = status.agents.iter()
        .filter(|a| a.state == "free")
        .map(|a| a.object_id.clone())
        .collect();
    println!("free_agents={}", free_agents.join(","));
    let allocated_agents: Vec<String> = status.agents.iter()
        .filter(|a| a.state == "allocated")
        .map(|a| a.object_id.clone())
        .collect();
    println!("allocated_agents={}", allocated_agents.join(","));
    for entry in &status.agents {
        println!("agent={} state={} task_path={}", entry.object_id, entry.state, entry.task_path.as_deref().unwrap_or(""));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Bug Report / Bug Fix commands
// ---------------------------------------------------------------------------

fn bugreport_list_command(input: &PathBuf, status_filter: Option<&str>) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let reports = list_bugreports(&store, status_filter);
    if reports.is_empty() {
        println!("count=0");
        return Ok(());
    }
    println!("count={}", reports.len());
    for r in &reports {
        print_bugreport_summary(r);
    }
    Ok(())
}

fn bugreport_show_command(input: &PathBuf, id: &str) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let report = get_bugreport(&store, id)
        .ok_or_else(|| anyhow::anyhow!("bug report '{}' not found", id))?;
    print_bugreport_detail(&report);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn bugreport_create_command(
    input: &PathBuf,
    source_agent: &str,
    parent_agent: &str,
    error_output: &str,
    stack_context: &str,
    attempted_fixes: Option<&str>,
    reproduction_steps: Option<&str>,
    recommended_fix_plan: &str,
    severity: &str,
    durable: bool,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-bugreport");
    let mid = mutation_id
        .map(str::to_string)
        .unwrap_or_else(|| format!("bugreport:{}:{}", actor, uuid::Uuid::new_v4()));
    let created_by_str = created_by.unwrap_or(actor.as_str()).to_string();

    let result = service.create_bugreport(&CreateBugreportRequest {
        mutation_id: mid,
        actor_id: actor.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        source_agent: source_agent.to_string(),
        parent_agent: parent_agent.to_string(),
        error_output: error_output.to_string(),
        stack_context: stack_context.to_string(),
        attempted_fixes: parse_multi_value(Some(attempted_fixes.unwrap_or(""))),
        reproduction_steps: parse_multi_value(Some(reproduction_steps.unwrap_or(""))),
        recommended_fix_plan: recommended_fix_plan.to_string(),
        severity: severity.to_string(),
        durable,
        created_by: created_by_str,
    })?;

    println!("applied={}", result.write.applied);
    println!("bug_id={}", result.resource.bug_id);
    println!("severity={}", result.resource.severity);
    println!("status={}", result.resource.status);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn bugreport_search_command(input: &PathBuf, query: &str, status_filter: Option<&str>) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let results = search_bugreports(&store, query, status_filter);
    println!("count={}", results.len());
    for r in &results {
        print_bugreport_summary(r);
    }
    Ok(())
}

fn bugreport_update_status_command(
    input: &PathBuf,
    bug_id: &str,
    new_status: &str,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-bugreport");
    let mid = mutation_id
        .map(str::to_string)
        .unwrap_or_else(|| format!("bugreport-status:{}:{}", actor, uuid::Uuid::new_v4()));

    let result = service.update_bugreport_status(&UpdateBugreportStatusRequest {
        mutation_id: mid,
        actor_id: actor,
        corpus_ref: input.display().to_string(),
        expected_version: None,
        bug_id: bug_id.to_string(),
        new_status: new_status.to_string(),
    })?;

    println!("applied={}", result.write.applied);
    println!("bug_id={}", result.resource.bug_id);
    println!("status={}", result.resource.status);
    if let Some(resolved_at) = result.resource.resolved_at {
        println!("resolved_at={}", resolved_at.to_rfc3339());
    }
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn bugfix_list_command(input: &PathBuf) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let fixes = list_bugfixes(&store);
    println!("count={}", fixes.len());
    for f in &fixes {
        print_bugfix_summary(f);
    }
    Ok(())
}

fn bugfix_show_command(input: &PathBuf, id: &str) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let fix = get_bugfix(&store, id)
        .ok_or_else(|| anyhow::anyhow!("bug fix '{}' not found", id))?;
    print_bugfix_detail(&fix);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn bugfix_create_command(
    input: &PathBuf,
    title: &str,
    description: &str,
    fix_recipe: &str,
    linked_bugreport_id: Option<&str>,
    durable: bool,
    mutation_id: Option<&str>,
    actor_id: Option<&str>,
    created_by: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-bugfix");
    let mid = mutation_id
        .map(str::to_string)
        .unwrap_or_else(|| format!("bugfix:{}:{}", actor, uuid::Uuid::new_v4()));
    let created_by_str = created_by.unwrap_or(actor.as_str()).to_string();

    let result = service.create_bugfix(&CreateBugfixRequest {
        mutation_id: mid,
        actor_id: actor,
        corpus_ref: input.display().to_string(),
        expected_version: None,
        title: title.to_string(),
        description: description.to_string(),
        fix_recipe: fix_recipe.to_string(),
        linked_bugreport_id: linked_bugreport_id.map(str::to_string),
        durable,
        created_by: created_by_str,
    })?;

    println!("applied={}", result.write.applied);
    println!("fix_id={}", result.resource.fix_id);
    println!("title={}", result.resource.title);
    if !result.resource.linked_bugreport_ids.is_empty() {
        println!("linked_bugreport_ids={}", result.resource.linked_bugreport_ids.join(","));
    }
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn print_bugreport_summary(r: &BugReportInfo) {
    println!(
        "bug_id={} severity={} status={} source_agent={} error={}",
        r.bug_id,
        r.severity,
        r.status,
        r.source_agent,
        truncate_line(&r.error_output, 80),
    );
}

fn print_bugreport_detail(r: &BugReportInfo) {
    println!("bug_id={}", r.bug_id);
    println!("source_agent={}", r.source_agent);
    println!("parent_agent={}", r.parent_agent);
    println!("severity={}", r.severity);
    println!("status={}", r.status);
    println!("durability={}", r.durability);
    println!("retrieval_visibility={}", r.retrieval_visibility);
    println!("created_at={}", r.created_at.to_rfc3339());
    if let Some(resolved_at) = r.resolved_at {
        println!("resolved_at={}", resolved_at.to_rfc3339());
    }
    println!("error_output={}", r.error_output);
    println!("stack_context={}", r.stack_context);
    if !r.attempted_fixes.is_empty() {
        println!("attempted_fixes={}", r.attempted_fixes.join(" | "));
    }
    if !r.reproduction_steps.is_empty() {
        println!("reproduction_steps={}", r.reproduction_steps.join(" | "));
    }
    if !r.recommended_fix_plan.is_empty() {
        println!("recommended_fix_plan={}", r.recommended_fix_plan);
    }
    if !r.bucket_paths.is_empty() {
        println!("bucket_paths={}", r.bucket_paths.join(","));
    }
}

fn print_bugfix_summary(f: &BugFixInfo) {
    println!(
        "fix_id={} title={} status={} linked={}",
        f.fix_id,
        truncate_line(&f.title, 60),
        f.status,
        f.linked_bugreport_ids.len(),
    );
}

fn print_bugfix_detail(f: &BugFixInfo) {
    println!("fix_id={}", f.fix_id);
    println!("title={}", f.title);
    println!("description={}", f.description);
    println!("fix_recipe={}", f.fix_recipe);
    println!("status={}", f.status);
    println!("durability={}", f.durability);
    println!("created_at={}", f.created_at.to_rfc3339());
    if !f.linked_bugreport_ids.is_empty() {
        println!("linked_bugreport_ids={}", f.linked_bugreport_ids.join(","));
    }
    if !f.bucket_paths.is_empty() {
        println!("bucket_paths={}", f.bucket_paths.join(","));
    }
}

fn truncate_line(text: &str, max: usize) -> String {
    let first = text.split('\n').next().unwrap_or(text).trim();
    if first.len() > max {
        format!("{}...", &first[..max.saturating_sub(3)])
    } else {
        first.to_string()
    }
}

// ── Policy-layer handlers ─────────────────────────────────────────────────────

fn policy_set_command(
    input: &PathBuf,
    container_id: &str,
    field: &str,
    value: &str,
    _mutation_id: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let _actor = resolve_actor_id(actor_id, "rust-policy");
    let cid = container_id.to_string();
    let field_s = field.to_string();
    let value_s = value.to_string();
    let field_echo = field_s.clone();
    let value_echo = value_s.clone();
    service.run_with_store_mut(move |store, _now| {
        set_container_policy(store, &cid, &field_s, &value_s)?;
        Ok(())
    })?;
    println!("policy updated: container='{}' {}={}", container_id, field_echo, value_echo);
    Ok(())
}

fn policy_show_command(input: &PathBuf, container_id: &str) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let policies = show_container_policy(&store, container_id)?;
    println!("{}", serde_json::to_string_pretty(&policies)?);
    Ok(())
}

// ── Swarm-Plan / Callstack handlers ─────────────────────────────────────────

fn print_swarm_plan_result(result: &CallstackOpResult) {
    println!("action={}", result.action);
    for (k, v) in &result.fields {
        println!("{k}={v}");
    }
}

fn swarm_plan_context_command(input: &PathBuf, max_chars: usize, project: Option<&str>) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    match render_context_text(&store, project, max_chars) {
        Some(text) => print!("{text}"),
        None => println!("[AMS Callstack Context]\n(empty — no active swarm-plan project)\n[End callstack context]"),
    }
    Ok(())
}

fn swarm_plan_list_command(input: &PathBuf) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let projects = list_projects(&store);
    if projects.is_empty() {
        println!("(no execution plan roots found)");
        return Ok(());
    }
    println!("Projects:");
    for p in &projects {
        let state = if p.state != "completed" && p.active_node_path.is_none() {
            "parked"
        } else {
            &p.state
        };
        let cursor = p.active_node_path.as_deref()
            .map(|path| {
                let leaf = ams_core_kernel::callstack::last_path_segment(path);
                format!("  cursor={leaf}")
            })
            .unwrap_or_default();
        println!("  {} [{state}]{cursor}", p.name);
    }
    Ok(())
}

fn swarm_plan_show_command(input: &PathBuf, project: Option<&str>) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let lines = callstack_show(&store, project);
    for line in &lines {
        println!("{line}");
    }
    Ok(())
}

fn swarm_plan_push_command(
    input: &PathBuf,
    name: &str,
    description: Option<&str>,
    depends_on: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    service.guard_not_factories("swarm-plan-push")?;
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let name = name.to_string();
    let description = description.map(str::to_string);
    let depends_on = depends_on.map(str::to_string);
    let actor_clone = actor.clone();
    let result = service.run_with_store_mut(move |store, now| {
        assert_mode(store, None, "execute", "swarm-plan-push")?;
        let res = callstack_push(store, &name, description.as_deref(), &actor_clone, depends_on.as_deref(), now)?;
        record_tool_call(store, "swarm-plan-push", false, &res.to_text(), &actor_clone, now)?;
        Ok(res)
    })?;
    print_swarm_plan_result(&result);
    Ok(())
}

fn swarm_plan_pop_command(
    input: &PathBuf,
    return_text: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    service.guard_not_factories("swarm-plan-pop")?;
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let return_text = return_text.map(str::to_string);
    let result = service.run_with_store_mut(move |store, now| {
        assert_mode(store, None, "execute", "swarm-plan-pop")?;
        let res = callstack_pop(store, return_text.as_deref(), &actor, now)?;
        record_tool_call(store, "swarm-plan-pop", false, &res.to_text(), &actor, now)?;
        Ok(res)
    })?;
    print_swarm_plan_result(&result);
    Ok(())
}

fn swarm_plan_observe_command(
    input: &PathBuf,
    title: &str,
    text: &str,
    actor_id: Option<&str>,
    node_path: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    service.guard_not_factories("swarm-plan-observe")?;
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let title = title.to_string();
    let text = text.to_string();
    let node_path = node_path.map(str::to_string);
    let result = service.run_with_store_mut(move |store, now| {
        assert_mode(store, None, "execute", "swarm-plan-observe")?;
        let res = callstack_observe_at(store, &title, &text, &actor, now, node_path.as_deref())?;
        record_tool_call(store, "swarm-plan-observe", false, &res.to_text(), &actor, now)?;
        Ok(res)
    })?;
    print_swarm_plan_result(&result);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn swarm_plan_interrupt_command(
    input: &PathBuf,
    policy: &str,
    reason: &str,
    error_output: &str,
    context: &str,
    attempted_fix: &str,
    repair_hint: &str,
    subtask_hints: &str,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    service.guard_not_factories("swarm-plan-interrupt")?;
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let params_owned = (
        policy.to_string(),
        reason.to_string(),
        error_output.to_string(),
        context.to_string(),
        attempted_fix.to_string(),
        repair_hint.to_string(),
        subtask_hints.to_string(),
    );
    let result = service.run_with_store_mut(move |store, now| {
        assert_mode(store, None, "execute", "swarm-plan-interrupt")?;
        let params = InterruptParams {
            actor_id: &actor,
            policy_kind: &params_owned.0,
            reason: &params_owned.1,
            error_output: &params_owned.2,
            context: &params_owned.3,
            attempted_fix: &params_owned.4,
            repair_hint: &params_owned.5,
            subtask_hints: &params_owned.6,
        };
        let res = callstack_interrupt(store, &params, now)?;
        record_tool_call(store, "swarm-plan-interrupt", false, &res.to_text(), params.actor_id, now)?;
        Ok(res)
    })?;
    print_swarm_plan_result(&result);
    Ok(())
}

fn swarm_plan_resume_command(input: &PathBuf, actor_id: Option<&str>) -> Result<()> {
    let service = WriteService::from_input(input);
    service.guard_not_factories("swarm-plan-resume")?;
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let result = service.run_with_store_mut(move |store, now| {
        assert_mode(store, None, "execute", "swarm-plan-resume")?;
        let res = callstack_resume(store, &actor, now)?;
        record_tool_call(store, "swarm-plan-resume", false, &res.to_text(), &actor, now)?;
        Ok(res)
    })?;
    print_swarm_plan_result(&result);
    Ok(())
}

fn swarm_plan_advance_command(input: &PathBuf, actor_id: Option<&str>) -> Result<()> {
    let service = WriteService::from_input(input);
    service.guard_not_factories("swarm-plan-advance")?;
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let result = service.run_with_store_mut(move |store, now| {
        assert_mode(store, None, "execute", "swarm-plan-advance")?;
        let res = callstack_advance(store, &actor, now)?;
        record_tool_call(store, "swarm-plan-advance", false, &res.to_text(), &actor, now)?;
        Ok(res)
    })?;
    print_swarm_plan_result(&result);
    Ok(())
}

fn swarm_plan_switch_command(input: &PathBuf, name: &str, actor_id: Option<&str>) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let name_owned = name.to_string();
    let name_for_print = name.to_string();
    let active_node_path = service.run_with_store_mut(move |store, now| {
        // switch_project updates the active-project cursor in the registry store (factories).
        // No tool-call record here — registry meta-ops must not write telemetry to factories.
        let path = switch_project(store, &name_owned, &actor, now)?;
        Ok(path)
    })?;
    println!("action=switch\nproject={name_for_print}\nactive_node_path={active_node_path}");
    Ok(())
}

fn swarm_plan_park_command(
    input: &PathBuf,
    project: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let project = project.map(str::to_string);
    let parked = service.run_with_store_mut(move |store, now| {
        // park_project updates the registry store (factories).
        // No tool-call record here — registry meta-ops must not write telemetry to factories.
        let p = park_project(store, project.as_deref(), &actor, now)?;
        Ok(p)
    })?;
    println!("action=park\nproject={}", parked.as_deref().unwrap_or("none"));
    Ok(())
}

fn swarm_plan_complete_node_command(
    input: &PathBuf,
    node_path: &str,
    return_text: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    service.guard_not_factories("swarm-plan-complete-node")?;
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let node_path = node_path.to_string();
    let return_text = return_text.map(str::to_string);
    let result = service.run_with_store_mut(move |store, now| {
        assert_mode(store, None, "execute", "swarm-plan-complete-node")?;
        let res = callstack_complete_node(store, &node_path, return_text.as_deref(), &actor, now)?;
        record_tool_call(store, "swarm-plan-complete-node", false, &res.to_text(), &actor, now)?;
        Ok(res)
    })?;
    print_swarm_plan_result(&result);
    Ok(())
}

fn swarm_plan_ready_nodes_command(input: &PathBuf, project: Option<&str>) -> Result<()> {
    use ams_core_kernel::callstack::{find_active_node, EXECUTION_PLAN_ROOT, bucket_fields, node_meta_path, last_path_segment, read_observations};
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    // Find the root path for the given project (or active)
    let root_path = if let Some(proj) = project {
        format!("{EXECUTION_PLAN_ROOT}/{}", ams_core_kernel::callstack::slugify(proj))
    } else {
        match find_active_node(&store, None) {
            Some(active) => {
                ams_core_kernel::callstack::resolve_runtime_root(&active.node_path, &active.fields)
            }
            None => {
                println!("(no active project)");
                return Ok(());
            }
        }
    };
    let nodes = ready_nodes(&store, &root_path);
    if nodes.is_empty() {
        println!("(no ready nodes — tree may be complete)");
        return Ok(());
    }
    for n in &nodes {
        let fields = bucket_fields(&store, &node_meta_path(n));
        let title = fields.get("title").map(|s| s.as_str()).unwrap_or_else(|| last_path_segment(n));
        let obs = read_observations(&store, n);
        println!("node_path={n}");
        println!("title={title}");
        // A6: emit depends_on and parent_node_path so the Python orchestrator can
        // resolve completed-dependency artifacts for resolution-aware handoffs.
        if let Some(deps) = fields.get("depends_on").filter(|s| !s.trim().is_empty()) {
            println!("depends_on={deps}");
        }
        if let Some(parent) = fields.get("parent_node_path").filter(|s| !s.trim().is_empty()) {
            println!("parent_node_path={parent}");
        }
        if let Some(role) = fields.get("role").filter(|s| !s.trim().is_empty()) {
            println!("role={role}");
        }
        if !obs.is_empty() {
            println!("observations={}", obs.len());
            for o in obs.iter().take(3) {
                let truncated: String = o.chars().take(200).collect();
                println!("  {truncated}");
            }
        }
        println!("---");
    }
    Ok(())
}

fn swarm_plan_load_plan_command(
    input: &PathBuf,
    file: &PathBuf,
    into_active: bool,
    actor_id: Option<&str>,
) -> Result<()> {
    let text = std::fs::read_to_string(file)
        .with_context(|| format!("failed to read plan file '{}'", file.display()))?;
    let plan: serde_json::Value = serde_json::from_str(&text)
        .with_context(|| format!("failed to parse plan file '{}' as JSON", file.display()))?;

    let project_name = plan.get("project")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("plan file must have a 'project' field"))?;
    let project_description = plan.get("description").and_then(serde_json::Value::as_str);

    let raw_nodes = plan.get("nodes")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("plan file must have a 'nodes' array"))?;

    // First pass: collect id→title mapping so depends_on IDs can be resolved.
    let mut id_to_title: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for node in raw_nodes {
        if let (Some(id), Some(title)) = (
            node.get("id").and_then(serde_json::Value::as_str),
            node.get("title").and_then(serde_json::Value::as_str),
        ) {
            id_to_title.insert(id.to_string(), title.to_string());
        }
    }

    let mut node_defs: Vec<PlanNodeDef> = Vec::new();
    for node in raw_nodes {
        let title = node.get("title")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("each node must have a 'title' field"))?
            .to_string();
        let description = node.get("description").and_then(serde_json::Value::as_str).map(str::to_string);
        let depends_on_raw = match node.get("depends_on") {
            Some(serde_json::Value::String(s)) => {
                if s.trim().is_empty() { None } else { Some(s.clone()) }
            }
            Some(serde_json::Value::Array(arr)) => {
                let joined: String = arr.iter()
                    .filter_map(serde_json::Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", ");
                if joined.is_empty() { None } else { Some(joined) }
            }
            _ => None,
        };
        // Resolve depends_on: translate IDs to titles where possible.
        // If a dep token matches an id in the map, use the title; otherwise
        // keep it as-is (it may already be a title for backwards compat).
        let depends_on = depends_on_raw.map(|raw| {
            raw.split(',')
                .map(|t| t.trim())
                .filter(|t| !t.is_empty())
                .map(|t| id_to_title.get(t).map(|s| s.as_str()).unwrap_or(t))
                .collect::<Vec<_>>()
                .join(", ")
        });
        let role = node.get("role").and_then(serde_json::Value::as_str).map(str::to_string);
        node_defs.push(PlanNodeDef { title, description, depends_on, role });
    }

    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let project_name_owned = project_name.to_string();
    let project_desc_owned = project_description.map(str::to_string);
    let result = service.run_with_store_mut(move |store, now| {
        assert_mode(store, None, "edit", "swarm-plan-load-plan")?;
        let res = callstack_load_plan(
            store,
            &project_name_owned,
            project_desc_owned.as_deref(),
            &node_defs,
            &actor,
            into_active,
            now,
        )?;
        let preview = format!("action=load-plan project={} nodes={}", res.project, res.nodes.len());
        record_tool_call(store, "swarm-plan-load-plan", false, &preview, &actor, now)?;
        Ok(res)
    })?;

    println!("action=load-plan");
    println!("project={}", result.project);
    println!("root={}", result.root);
    println!("nodes={}", result.nodes.len());
    for (title, path) in &result.nodes {
        println!("  {title} -> {path}");
    }
    Ok(())
}

fn swarm_plan_repair_roots_command(input: &PathBuf, actor_id: Option<&str>) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "repair-roots");
    let result = service.run_with_store_mut(move |store, now| {
        repair_roots(store, &actor, now)
    })?;
    println!("{}", result.to_text());
    Ok(())
}

fn swarm_plan_enter_edit_command(
    input: &PathBuf,
    project: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let project_owned = project.map(str::to_string);
    let result = service.run_with_store_mut(move |store, now| {
        let res = callstack_enter_edit(store, project_owned.as_deref(), &actor, now)?;
        record_tool_call(store, "swarm-plan-enter-edit", false, &res.to_text(), &actor, now)?;
        Ok(res)
    })?;
    print_swarm_plan_result(&result);
    Ok(())
}

fn swarm_plan_enter_execute_command(
    input: &PathBuf,
    project: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let project_owned = project.map(str::to_string);
    let result = service.run_with_store_mut(move |store, now| {
        let res = callstack_enter_execute(store, project_owned.as_deref(), &actor, now)?;
        record_tool_call(store, "swarm-plan-enter-execute", false, &res.to_text(), &actor, now)?;
        Ok(res)
    })?;
    print_swarm_plan_result(&result);
    Ok(())
}

fn swarm_plan_quarantined_push_command(
    input: &PathBuf,
    name: &str,
    description: Option<&str>,
    depends_on: Option<&str>,
    parent_node_path: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let name = name.to_string();
    let description = description.map(str::to_string);
    let depends_on = depends_on.map(str::to_string);
    let parent_node_path = parent_node_path.map(str::to_string);
    let result = service.run_with_store_mut(move |store, now| {
        let res = callstack_quarantined_push(
            store,
            &name,
            description.as_deref(),
            parent_node_path.as_deref(),
            depends_on.as_deref(),
            &actor,
            now,
        )?;
        record_tool_call(store, "swarm-plan-quarantined-push", false, &res.to_text(), &actor, now)?;
        Ok(res)
    })?;
    print_swarm_plan_result(&result);
    Ok(())
}

fn swarm_plan_rename_node_command(
    input: &PathBuf,
    node_path: &str,
    new_title: &str,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let node_path = node_path.to_string();
    let new_title = new_title.to_string();
    let result = service.run_with_store_mut(move |store, now| {
        assert_mode(store, None, "edit", "swarm-plan-rename-node")?;
        let res = callstack_rename_node(store, &node_path, &new_title, &actor, now)?;
        record_tool_call(store, "swarm-plan-rename-node", false, &res.to_text(), &actor, now)?;
        Ok(res)
    })?;
    print_swarm_plan_result(&result);
    Ok(())
}

fn swarm_plan_delete_node_command(
    input: &PathBuf,
    node_path: &str,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let node_path = node_path.to_string();
    let result = service.run_with_store_mut(move |store, now| {
        assert_mode(store, None, "edit", "swarm-plan-delete-node")?;
        let res = callstack_delete_node(store, &node_path, &actor, now)?;
        record_tool_call(store, "swarm-plan-delete-node", false, &res.to_text(), &actor, now)?;
        Ok(res)
    })?;
    print_swarm_plan_result(&result);
    Ok(())
}

fn swarm_plan_set_depends_on_command(
    input: &PathBuf,
    node_path: &str,
    depends_on: &str,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let node_path = node_path.to_string();
    let depends_on = depends_on.to_string();
    let result = service.run_with_store_mut(move |store, now| {
        assert_mode(store, None, "edit", "swarm-plan-set-depends-on")?;
        let res = callstack_set_depends_on(store, &node_path, &depends_on, &actor, now)?;
        record_tool_call(store, "swarm-plan-set-depends-on", false, &res.to_text(), &actor, now)?;
        Ok(res)
    })?;
    print_swarm_plan_result(&result);
    Ok(())
}

fn swarm_plan_move_node_command(
    input: &PathBuf,
    node_path: &str,
    new_parent_path: &str,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let node_path = node_path.to_string();
    let new_parent_path = new_parent_path.to_string();
    let result = service.run_with_store_mut(move |store, now| {
        assert_mode(store, None, "edit", "swarm-plan-move-node")?;
        let res = callstack_move_node(store, &node_path, &new_parent_path, &actor, now)?;
        record_tool_call(store, "swarm-plan-move-node", false, &res.to_text(), &actor, now)?;
        Ok(res)
    })?;
    print_swarm_plan_result(&result);
    Ok(())
}

fn swarm_plan_batch_command(
    input: &PathBuf,
    ops_source: &str,
    actor_id: Option<&str>,
) -> Result<()> {
    use std::io::Read;

    let ops_json = if ops_source == "-" {
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)
            .context("failed to read batch ops from stdin")?;
        buf
    } else {
        std::fs::read_to_string(ops_source)
            .with_context(|| format!("failed to read batch ops from '{ops_source}'"))?
    };

    let ops: Vec<BatchOp> = serde_json::from_str(&ops_json)
        .context("failed to parse batch ops JSON array")?;

    if ops.is_empty() {
        println!("[]");
        return Ok(());
    }

    let service = WriteService::from_input(input);
    service.guard_not_factories("swarm-plan-batch")?;
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let results = service.run_with_store_mut(move |store, now| {
        run_batch(store, &ops, &actor, now)
    })?;

    let json = serde_json::to_string(&results)
        .context("failed to serialize batch results")?;
    println!("{json}");
    Ok(())
}

fn swarm_plan_tag_command(
    input: &PathBuf,
    plan_name: &str,
    bucket_path: &str,
    summary: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-swarm-plan");
    let title = format!("plan-receipt: {plan_name}");
    let body = summary.unwrap_or(plan_name).to_string();
    let buckets_arg = bucket_path.to_string();
    let mutation_id = format!("swarm-plan-tag:{actor}:{}", uuid::Uuid::new_v4());
    let result = service.create_smartlist_note(&CreateSmartListNoteRequest {
        mutation_id,
        actor_id: actor.clone(),
        corpus_ref: input.display().to_string(),
        expected_version: None,
        title: title.clone(),
        text: body,
        bucket_paths: vec![buckets_arg.clone()],
        durable: true,
        created_by: actor.clone(),
        note_id: None,
    })?;
    println!("action=swarm-plan-tag");
    println!("plan_name={plan_name}");
    println!("bucket_path={buckets_arg}");
    println!("title={title}");
    println!("note_id={}", result.resource.note_id);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

// ── Cache command handlers ────────────────────────────────────────────────────

fn cache_register_tool_command(
    input: &PathBuf,
    tool_id: &str,
    tool_version: &str,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let _actor = resolve_actor_id(actor_id, "rust-cache");
    let tool_id_owned = tool_id.to_string();
    let tool_version_owned = tool_version.to_string();
    let identity = service.run_with_store_mut(move |store, now| {
        cache_register_tool(store, &tool_id_owned, &tool_version_owned, Some(now))
    })?;
    println!("action=cache-register-tool");
    println!("tool_id={}", identity.tool_id);
    println!("tool_version={}", identity.tool_version);
    println!("object_id={}", identity.object_id);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn cache_register_source_command(
    input: &PathBuf,
    source_id: &str,
    fingerprint: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let _actor = resolve_actor_id(actor_id, "rust-cache");
    let source_id_owned = source_id.to_string();
    let fingerprint_owned = fingerprint.map(str::to_string);
    let identity = service.run_with_store_mut(move |store, now| {
        cache_register_source(store, &source_id_owned, fingerprint_owned.as_deref(), Some(now))
    })?;
    println!("action=cache-register-source");
    println!("source_id={}", identity.source_id);
    if let Some(fp) = &identity.fingerprint {
        println!("fingerprint={fp}");
    }
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn cache_promote_command(
    input: &PathBuf,
    tool_id: &str,
    tool_version: &str,
    source_id: &str,
    source_fingerprint: Option<&str>,
    param_hash: &str,
    in_situ_ref: Option<&str>,
    artifact_fingerprint: Option<&str>,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-cache");

    let tool = ToolIdentity {
        tool_id: tool_id.to_string(),
        tool_version: tool_version.to_string(),
        object_id: format!("cache-tool:{tool_id}"),
    };
    let source = SourceIdentity {
        source_id: source_id.to_string(),
        fingerprint: source_fingerprint.map(str::to_string),
    };
    let invocation = InvocationIdentity::new(&tool, &source, param_hash);
    let in_situ_ref_owned = in_situ_ref.map(str::to_string);
    let artifact_fingerprint_owned = artifact_fingerprint.map(str::to_string);

    let result = service.run_with_store_mut(move |store, now| {
        promote_artifact(
            store, &tool, &source, &invocation,
            in_situ_ref_owned.as_deref(),
            artifact_fingerprint_owned.as_deref(),
            &actor, Some(now),
        )
    })?;

    println!("action=cache-promote");
    println!("artifact_id={}", result.artifact_id);
    println!("tool_cache_path={}", result.tool_cache_path);
    println!("source_cache_links_path={}", result.source_cache_links_path);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn cache_lookup_command(
    input: &PathBuf,
    mode: &str,
    tool_id: &str,
    source_id: &str,
    param_hash: Option<&str>,
    format: &str,
) -> Result<()> {
    let (store, _) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;

    let hits: Vec<CacheHit> = match mode {
        "source" => lookup_source_centric(&store, source_id, Some(tool_id), param_hash),
        _ => lookup_tool_centric(&store, tool_id, source_id, param_hash),
    };

    if format == "json" {
        println!("{}", serde_json::to_string(&hits).context("failed to serialize hits")?);
        return Ok(());
    }

    println!("action=cache-lookup");
    println!("mode={mode}");
    println!("hits={}", hits.len());
    for hit in &hits {
        let exact_label = if hit.exact { "exact" } else { "compatible" };
        println!(
            "  artifact_id={} match={} validity={} param_hash={} created_at={}",
            hit.artifact_id,
            exact_label,
            hit.metadata.validity_state,
            hit.metadata.param_hash,
            hit.metadata.created_at.to_rfc3339(),
        );
        if let Some(ref text) = hit.text {
            println!("  text={}", text);
        }
    }
    Ok(())
}

fn cache_invalidate_command(
    input: &PathBuf,
    artifact_id: &str,
    state: &str,
    reason: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let artifact_id_owned = artifact_id.to_string();
    let new_state = match state {
        "invalidated" => ValidityState::Invalidated,
        "stale" => ValidityState::Stale,
        "ghosted" => ValidityState::Ghosted,
        "lost" => ValidityState::Lost,
        "valid" => ValidityState::Valid,
        other => anyhow::bail!("unknown validity state '{}'; expected: invalidated, stale, ghosted, lost, valid", other),
    };
    let reason_owned = reason.map(str::to_string);

    if new_state == ValidityState::Valid {
        service.run_with_store_mut(move |store, _now| {
            revalidate_artifact(store, &artifact_id_owned)
        })?;
    } else {
        service.run_with_store_mut(move |store, _now| {
            invalidate_artifact(store, &InvalidationRequest {
                artifact_id: artifact_id_owned,
                new_state,
                reason: reason_owned,
            })
        })?;
    }

    println!("action=cache-invalidate");
    println!("artifact_id={artifact_id}");
    println!("state={state}");
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

// ── Atlas command handlers ─────────────────────────────────────────────────────

fn atlas_page_command(input: &PathBuf, id: &str) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());
    let page = atlas_page(&store, id)?;
    print!("{page}");
    Ok(())
}

fn atlas_search_command(input: &PathBuf, query: &str, top: usize) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());
    let results = atlas_search(&store, query, top);
    print!("{results}");
    Ok(())
}

fn atlas_expand_command(input: &PathBuf, id: &str) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());
    let expansion = atlas_expand(&store, id)?;
    print!("{expansion}");
    Ok(())
}

// ── Multi-scale Atlas command handlers ────────────────────────────────────────

/// Parse a scale spec string "N:path1,path2" into (N, Vec<path>).
fn parse_scale_spec(spec: &str) -> Result<(u32, Vec<String>)> {
    let (n_str, paths_str) = spec
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("invalid scale spec '{spec}'; expected format 'N:path[,path2]'"))?;
    let scale: u32 = n_str
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid scale index '{n_str}' in spec '{spec}'"))?;
    let paths: Vec<String> = paths_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if paths.is_empty() {
        anyhow::bail!("scale spec '{spec}' has no bucket paths");
    }
    Ok((scale, paths))
}

fn atlas_define_command(
    input: &PathBuf,
    name: &str,
    description: Option<&str>,
    scale_specs: &[String],
) -> Result<()> {
    let mut scale_levels: Vec<(u32, Vec<String>)> = scale_specs
        .iter()
        .map(|s| parse_scale_spec(s))
        .collect::<Result<_>>()?;
    scale_levels.sort_by_key(|(n, _)| *n);

    let service = WriteService::from_input(input);
    let name_owned = name.to_string();
    let desc_owned = description.map(str::to_string);
    let levels_owned = scale_levels.clone();
    let info = service.run_with_store_mut(move |store, _now| {
        atlas_define(store, &name_owned, desc_owned.as_deref(), &levels_owned)
    })?;

    println!("action=atlas-define");
    println!("atlas_name={}", info.atlas_name);
    println!("scales={}", info.scales.len());
    for sl in &info.scales {
        println!("  scale_{}: {}", sl.scale, sl.bucket_paths.join(","));
    }
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn atlas_show_command(input: &PathBuf, name: &str) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());
    let info = atlas_show(&store, name)?;
    print!("{}", render_atlas_info(&info));
    Ok(())
}

fn atlas_list_command(input: &PathBuf) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());
    let atlases = atlas_list(&store);
    println!("atlases={}", atlases.len());
    for info in &atlases {
        print!("{}", render_atlas_info(info));
    }
    Ok(())
}

fn atlas_list_at_scale_command(input: &PathBuf, name: &str, scale: u32) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());
    let entries = atlas_list_at_scale(&store, name, scale)?;
    print!("{}", render_scale_listing(name, scale, &entries));
    Ok(())
}

fn atlas_navigate_command(input: &PathBuf, name: &str, id: &str) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());
    let nav = atlas_navigate(&store, name, id)?;
    print!("{}", render_navigation(&nav));
    Ok(())
}

// ── Resolution engine commands ─────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn resolution_resolve_command(
    input: &PathBuf,
    object_id: &str,
    tool_id: Option<&str>,
    source_id: Option<&str>,
    param_hash: Option<&str>,
    no_cache: bool,
    no_historical: bool,
    no_partial: bool,
    no_content_addressed: bool,
    revalidate_on_recovery: bool,
) -> Result<()> {
    let (mut store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());

    let req = ResolutionRequest {
        object_id: object_id.to_string(),
        tool_id: tool_id.map(str::to_string),
        source_id: source_id.map(str::to_string),
        param_hash: param_hash.map(str::to_string),
        try_cache: !no_cache,
        try_historical: !no_historical,
        try_partial_reconstruction: !no_partial,
        try_content_addressed: !no_content_addressed,
        revalidate_on_recovery,
    };

    let result = resolve_object(&mut store, &req);

    println!("object_id={}", result.requested_object_id);
    println!("state={}", result.state);
    if let Some(ref rid) = result.resolved_object_id {
        println!("resolved_object_id={}", rid);
    } else {
        println!("resolved_object_id=");
    }
    println!("recovery_path={}", result.recovery_path.as_str());
    println!("revalidated={}", result.revalidated);
    println!("explanation={}", result.explanation);
    Ok(())
}

fn resolution_show_command(input: &PathBuf, object_id: &str) -> Result<()> {
    let (store, resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    println!("snapshot_source={}", resolved.display());

    match read_resolution_state(&store, object_id) {
        Some(state) => {
            println!("object_id={}", object_id);
            println!("resolution_state={}", state);
        }
        None => {
            println!("object_id={}", object_id);
            println!("resolution_state=(none)");
        }
    }
    Ok(())
}

// ── ProjDir Atlas command implementations ─────────────────────────────────────

fn projdir_ingest_command(input: &PathBuf, repo_root: &PathBuf) -> Result<()> {
    let service = WriteService::from_input(input);
    let repo_root = repo_root.clone();
    let result = service.run_with_store_mut(move |store, _now| {
        projdir_ingest(store, &repo_root)
    })?;
    println!("action=projdir-ingest");
    println!("ingested={}", result.ingested);
    println!("skipped={}", result.skipped);
    println!("total={}", result.total);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn projdir_build_dirs_command(input: &PathBuf) -> Result<()> {
    let service = WriteService::from_input(input);
    let result = service.run_with_store_mut(|store, _now| {
        projdir_build_dirs(store)
    })?;
    println!("action=projdir-build-dirs");
    println!("dirs_created={}", result.dirs_created);
    println!("files_attached={}", result.files_attached);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn projdir_stats_command(input: &PathBuf) -> Result<()> {
    let service = WriteService::from_input(input);
    let stats = service.run_with_store_mut(|store, _now| {
        projdir_stats(store)
    })?;
    println!("{}", format_stats_table(&stats));
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn projdir_register_atlas_command(input: &PathBuf) -> Result<()> {
    let service = WriteService::from_input(input);
    let result = service.run_with_store_mut(|store, _now| {
        projdir_register_atlas(store)
    })?;
    println!("action=projdir-register-atlas");
    println!("atlas_name={}", result.atlas_name);
    println!("scale_count={}", result.scale_count);
    println!("scale0_buckets={}", result.scale0_buckets.join(","));
    println!("scale1_buckets={}", result.scale1_buckets.join(","));
    println!("scale2_bucket_count={}", result.scale2_buckets.len());
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn projdir_build_file_pages_command(input: &PathBuf) -> Result<()> {
    let service = WriteService::from_input(input);
    let result = service.run_with_store_mut(|store, _now| {
        projdir_build_file_pages(store)
    })?;
    println!("action=projdir-build-file-pages");
    println!("pages_created={}", result.pages_created);
    println!("files_attached={}", result.files_attached);
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

fn projdir_doc_command(input: &PathBuf, path: &str) -> Result<()> {
    let (store, _resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let result = projdir_doc(&store, path)?;
    println!("path={}", result.path);
    match result.content {
        Some(content) => println!("{}", content),
        None => println!("(binary file — no stored head content)"),
    }
    Ok(())
}

fn projdir_context_command(input: &PathBuf, depth: usize) -> Result<()> {
    let (store, _resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let result = projdir_context(&store, depth);
    print!("{}", format_context(&result, depth));
    Ok(())
}

fn projdir_tree_command(input: &PathBuf, path: Option<&str>, depth: usize) -> Result<()> {
    let (store, _resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let result = projdir_tree(&store, path, depth);
    println!("{}", format_tree(&result));
    Ok(())
}

fn projdir_search_command(input: &PathBuf, query: &[String]) -> Result<()> {
    let (store, _resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let query_refs: Vec<&str> = query.iter().map(String::as_str).collect();
    let result = projdir_search(&store, &query_refs);
    println!("hits={}", result.hits.len());
    println!("{}", format_search(&result));
    Ok(())
}

// ── Agent Knowledge Cache (AKC) command implementations ──────────────────────

#[allow(clippy::too_many_arguments)]
fn ke_write_command(
    input: &PathBuf,
    scope: &str,
    kind: &str,
    text: &str,
    summary: Option<&str>,
    tags: &[String],
    confidence: f64,
    watch: &[String],
    actor_id: Option<&str>,
    bootstrap_source: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    // Knowledge entries must never go into the factories store.
    // Factories is for SmartList templates only.
    // Use the dedicated KE store: shared-memory/system-memory/ke/ke.memory.jsonl
    service.guard_not_factories("ke-write")?;
    let req = KeWriteRequest {
        scope: scope.to_string(),
        kind: kind.to_string(),
        text: text.to_string(),
        summary: summary.map(str::to_string),
        tags: tags.to_vec(),
        confidence,
        author_agent_id: actor_id.unwrap_or("cli").to_string(),
        watch_paths: watch.to_vec(),
        bootstrap_source: bootstrap_source.map(str::to_string),
    };
    let result = service.run_with_store_mut(move |store, _now| {
        ke_write(store, req)
    })?;
    println!("object_id={}", result.object_id);
    println!("scope={}", result.scope);
    println!("kind={}", result.kind);
    println!("was_update={}", result.was_update);
    Ok(())
}

fn ke_read_command(input: &PathBuf, scope: &str, include_stale: bool) -> Result<()> {
    let (store, _resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let result = ke_read(&store, scope, include_stale);
    println!("scope={}", result.scope);
    println!("entries={}", result.entries.len());
    for entry in &result.entries {
        let stale_marker = if entry.is_stale { " [STALE]" } else { "" };
        println!("[{}]{} confidence={:.2} {}", entry.kind, stale_marker, entry.confidence, entry.text);
    }
    Ok(())
}

fn ke_search_command(
    input: &PathBuf,
    query: &[String],
    top: usize,
    scope: Option<&str>,
    kind: Option<&str>,
    _include_stale: bool,
) -> Result<()> {
    let (store, _resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let query_refs: Vec<&str> = query.iter().map(String::as_str).collect();
    let result = ke_search(&store, &query_refs, top, scope, kind);
    println!("hits={}", result.hits.len());
    for hit in &result.hits {
        let stale_marker = if hit.entry.is_stale { " [STALE]" } else { "" };
        println!("score={} [{}/{}]{} {}", hit.score, hit.entry.scope, hit.entry.kind, stale_marker, hit.entry.text);
    }
    Ok(())
}

fn ke_context_command(
    input: &PathBuf,
    scope: Option<&str>,
    max_entries: usize,
    max_chars: usize,
) -> Result<()> {
    let (store, _resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let output = ke_context_fn(&store, scope, max_entries, max_chars);
    if output.is_empty() {
        println!("(no entries)");
    } else {
        println!("{}", output);
    }
    Ok(())
}

fn ke_bootstrap_command(input: &PathBuf, repo_root: &PathBuf) -> Result<()> {
    let service = WriteService::from_input(input);
    let repo_root = repo_root.clone();
    let result = service.run_with_store_mut(move |store, _now| {
        ke_bootstrap(store, &repo_root)
    })?;
    println!("docs_scanned={}", result.docs_scanned);
    println!("entries_written={}", result.entries_written);
    println!("skipped_existing={}", result.skipped_existing);
    Ok(())
}

// ── Search cache commands (P5) ────────────────────────────────────────────────

fn search_corpus_version_command(input: &PathBuf) -> Result<()> {
    let (store, _resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let result = compute_corpus_version(&store);
    println!("corpus_version={}", result.corpus_version);
    println!("session_count={}", result.session_count);
    Ok(())
}

fn search_cache_lookup_command(input: &PathBuf, query: &str) -> Result<()> {
    let (store, _resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;

    // Build the cache key from the normalised query + current corpus version.
    let corpus = compute_corpus_version(&store);
    let source_id = search_cache_key(query, &corpus.corpus_version);

    // Tool-centric lookup: find valid artifacts for semantic-search:v1 + this source.
    let hits = lookup_tool_centric(&store, SEMANTIC_SEARCH_TOOL_ID, &source_id, None);

    // Return the first valid (exact) hit, if any.
    if let Some(hit) = hits.into_iter().find(|h| h.exact) {
        println!("status=hit");
        println!("artifact_id={}", hit.artifact_id);
        if let Some(ref text) = hit.text {
            println!("text={text}");
        }
    } else {
        println!("status=miss");
        println!("source_id={source_id}");
    }
    Ok(())
}

fn search_cache_promote_command(
    input: &PathBuf,
    query: &str,
    text: &str,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "rust-search-cache");

    let tool = ToolIdentity {
        tool_id: SEMANTIC_SEARCH_TOOL_ID.to_string(),
        tool_version: "1".to_string(),
        object_id: format!("cache-tool:{SEMANTIC_SEARCH_TOOL_ID}"),
    };

    // Compute corpus version and cache key inside the write transaction so we
    // read the same store state that we will write into.
    let text_owned = text.to_string();
    let query_owned = query.to_string();

    let result = service.run_with_store_mut(move |store, now| {
        let corpus = compute_corpus_version(store);
        let source_id = search_cache_key(&query_owned, &corpus.corpus_version);

        let source = SourceIdentity {
            source_id: source_id.clone(),
            fingerprint: None,
        };
        let invocation = InvocationIdentity::new(&tool, &source, "none");

        let promotion = promote_artifact(
            store, &tool, &source, &invocation,
            Some(text_owned.as_str()),
            None,
            &actor, Some(now),
        )?;

        Ok((promotion, source_id, corpus.corpus_version))
    })?;

    let (promotion, source_id, corpus_version) = result;
    println!("action=search-cache-promote");
    println!("artifact_id={}", promotion.artifact_id);
    println!("source_id={source_id}");
    println!("corpus_version={corpus_version}");
    println!("snapshot={}", service.paths().snapshot_path.display());
    Ok(())
}

/// Invalidate all search cache entries whose source_id ends with `:<corpus_version>`.
///
/// This is called by the ingest pipeline after new sessions are added.  Every
/// cached search result that was computed against the old corpus version is
/// marked stale so subsequent lookups trigger a fresh computation.
fn search_cache_invalidate_command(input: &PathBuf, corpus_version: &str) -> Result<()> {
    let service = WriteService::from_input(input);
    let corpus_version_owned = corpus_version.to_string();
    let suffix = format!(":{corpus_version}");

    let invalidated = service.run_with_store_mut(move |store, _now| {
        // Walk the semantic-search:v1 tool cache SmartList and find all valid artifacts
        // whose source_id ends with `:<corpus_version>`.  We cannot use lookup_tool_centric
        // here because it requires an exact source_id match; instead we scan directly.
        let raw_path = tool_cache_smartlist_path(SEMANTIC_SEARCH_TOOL_ID);
        let normalized = normalize_smartlist_path(&raw_path).unwrap_or(raw_path);
        let container_id = format!("smartlist-members:{normalized}");

        // Collect matching artifact IDs before mutating the store.
        let mut to_invalidate: Vec<String> = Vec::new();
        for link_node in store.iterate_forward(&container_id) {
            let obj_id = &link_node.object_id;
            let Some(obj) = store.objects().get(obj_id) else { continue };
            let Some(ref sp) = obj.semantic_payload else { continue };
            let Some(ref prov) = sp.provenance else { continue };
            let get_str = |key: &str| prov.get(key).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let source_id = get_str("source_id");
            let validity = get_str("validity_state");
            if validity == "valid" && source_id.ends_with(&suffix) {
                to_invalidate.push(obj_id.clone());
            }
        }

        let count = to_invalidate.len();
        let reason = format!("corpus version changed: {corpus_version_owned}");
        for artifact_id in to_invalidate {
            stale_artifact(store, &artifact_id, &reason)?;
        }
        Ok(count)
    })?;

    println!("action=search-cache-invalidate");
    println!("corpus_version={corpus_version}");
    println!("invalidated={invalidated}");
    Ok(())
}

fn search_cache_stats_command(input: &PathBuf) -> Result<()> {
    let (store, _resolved) = import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;

    // Compute corpus version for context.
    let corpus = compute_corpus_version(&store);

    // Count valid artifacts in the semantic-search:v1 tool cache SmartList.
    let raw_path = tool_cache_smartlist_path(SEMANTIC_SEARCH_TOOL_ID);
    let normalized = normalize_smartlist_path(&raw_path).unwrap_or(raw_path);
    let container_id = format!("smartlist-members:{}", normalized);

    let mut cached_entries: usize = 0;
    for link_node in store.iterate_forward(&container_id) {
        let obj_id = &link_node.object_id;
        let Some(obj) = store.objects().get(obj_id) else { continue };
        let Some(ref sp) = obj.semantic_payload else { continue };
        let Some(ref prov) = sp.provenance else { continue };
        let validity = prov.get("validity_state").and_then(|v| v.as_str()).unwrap_or("");
        if validity == "valid" {
            cached_entries += 1;
        }
    }

    println!("action=search-cache-stats");
    println!("corpus_version={}", corpus.corpus_version);
    println!("session_count={}", corpus.session_count);
    println!("cached_entries={cached_entries}");
    Ok(())
}

// ── P7: FEP cache signal ──────────────────────────────────────────────────────

fn fep_cache_signal_emit_command(
    input: &PathBuf,
    query: &str,
    is_hit: bool,
    actor_id: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let actor = resolve_actor_id(actor_id, "semantic-query");

    let query_owned = query.to_string();
    let (object_id, cache_status, query_normalized, corpus_version) =
        service.run_with_store_mut(move |store, now| {
            let corpus = compute_corpus_version(store);
            emit_cache_signal(store, &query_owned, &corpus.corpus_version, is_hit, &actor, now)?;
            // Collect the last-written tool-call id for reporting.
            let (oid, norm, cv) = {
                use ams_core_kernel::search_cache::normalize_query;
                let norm = normalize_query(&query_owned);
                let cv = corpus.corpus_version.clone();
                // The object_id was assigned inside emit_cache_signal; retrieve the latest one.
                let latest_id = store
                    .objects()
                    .values()
                    .filter(|o| o.object_kind == "tool-call")
                    .filter(|o| {
                        o.semantic_payload
                            .as_ref()
                            .and_then(|sp| sp.provenance.as_ref())
                            .and_then(|p| p.get("signal_kind"))
                            .and_then(|v| v.as_str())
                            == Some(ams_core_kernel::CACHE_SIGNAL_KIND)
                    })
                    .max_by_key(|o| o.created_at)
                    .map(|o| o.object_id.clone())
                    .unwrap_or_default();
                (latest_id, norm, cv)
            };
            Ok((oid, if is_hit { "hit" } else { "miss" }, norm, cv))
        })?;

    println!("action=fep-cache-signal-emit");
    println!("object_id={object_id}");
    println!("cache_status={cache_status}");
    println!("query_normalized={query_normalized}");
    println!("corpus_version={corpus_version}");
    Ok(())
}

fn fep_cache_signal_stats_command(
    input: &PathBuf,
    tool: Option<&str>,
    window_hours: u32,
) -> Result<()> {
    let (store, _resolved) =
        import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let result = cache_signal_stats(&store, tool, window_hours, now_fixed());

    println!("action=fep-cache-signal-stats");
    if result.tools.is_empty() {
        println!("total=0 hit_rate=0.0");
    } else {
        for s in &result.tools {
            println!(
                "tool={} hit_count={} miss_count={} total={} hit_rate={:.3} consecutive_misses={}",
                s.tool_name, s.hit_count, s.miss_count, s.total, s.hit_rate, s.consecutive_misses
            );
        }
    }
    Ok(())
}

fn fep_cache_signal_cluster_surprise_command(
    input: &PathBuf,
    window_hours: u32,
    min_signals: Option<usize>,
) -> Result<()> {
    let (store, _resolved) =
        import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let result = cache_signal_cluster_surprise(&store, window_hours, min_signals, now_fixed());

    println!("action=fep-cache-signal-cluster-surprise");
    for entry in &result.clusters {
        println!(
            "cluster_id={} sessions={} miss_rate={:.3} surprise={:.4}",
            entry.cluster_id, entry.session_count, entry.miss_rate, entry.surprise_score,
        );
    }
    Ok(())
}

fn fep_cache_report_command(input: &PathBuf, window_hours: u32) -> Result<()> {
    let (store, _resolved) =
        import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let report = fep_cache_report(&store, window_hours, now_fixed());

    // Section 1: Cache Signal Summary
    println!("=== Cache Signal Summary ===");
    if report.signal_stats.tools.is_empty() {
        println!("total=0 hit_rate=0.0");
    } else {
        for s in &report.signal_stats.tools {
            println!(
                "tool={} hit_count={} miss_count={} total={} hit_rate={:.3} consecutive_misses={}",
                s.tool_name, s.hit_count, s.miss_count, s.total, s.hit_rate, s.consecutive_misses
            );
        }
    }
    println!();

    // Section 2: Cluster Surprise Ranking
    println!("=== Cluster Surprise Ranking ===");
    if report.top_clusters.is_empty() {
        println!("(no clusters with signals)");
    } else {
        for entry in &report.top_clusters {
            println!(
                "cluster_id={} sessions={} miss_rate={:.3} surprise={:.4}",
                entry.cluster_id, entry.session_count, entry.miss_rate, entry.surprise_score,
            );
        }
    }
    println!();

    // Section 3: Dream Schedule Preview
    println!("=== Dream Schedule Preview ===");
    if report.dream_schedule_preview.is_empty() {
        println!("(no signal data — schedule unchanged)");
    } else {
        for entry in &report.dream_schedule_preview {
            println!(
                "cluster_id={} sessions={} signal_surprise={:.4}",
                entry.cluster_id, entry.session_count, entry.signal_surprise,
            );
        }
    }
    println!();

    // Section 4: Recommendations
    println!("=== Recommendations ===");
    for rec in &report.recommendations {
        match rec {
            CacheReportRecommendation::WarnHighMissRate { cluster_id, miss_rate } => {
                println!(
                    "WARN: cluster {} has high miss rate ({:.1}%) — consider running dream-touch",
                    cluster_id,
                    miss_rate * 100.0,
                );
            }
            CacheReportRecommendation::OkCacheIsWarm => {
                println!("OK: cache is warm");
            }
        }
    }
    if report.recommendations.is_empty() {
        println!("(no data — run searches to generate signals)");
    }

    Ok(())
}

// ── Swarm audit trail tool-call emitter ───────────────────────────────────────

fn emit_tool_call_command(
    input: &PathBuf,
    tool_name: &str,
    is_error: bool,
    result_preview: &str,
    actor_id: &str,
    duration_s: Option<f64>,
    ts: Option<&str>,
) -> Result<()> {
    let service = WriteService::from_input(input);
    let tool_name_owned = tool_name.to_string();
    let result_preview_owned = result_preview.to_string();
    let actor_id_owned = actor_id.to_string();
    let now_override: Option<chrono::DateTime<chrono::FixedOffset>> = ts
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok());

    let object_id = service.run_with_store_mut(move |store, now| {
        let effective_now = now_override.unwrap_or(now);
        use uuid::Uuid;
        let oid = format!("tool-call:{}", Uuid::new_v4().simple());
        record_tool_call_with_duration(
            store,
            &tool_name_owned,
            is_error,
            &result_preview_owned,
            &actor_id_owned,
            effective_now,
            duration_s,
        )?;
        // Return the last written tool-call object id
        let latest_id = store
            .objects()
            .values()
            .filter(|o| o.object_kind == "tool-call")
            .filter(|o| {
                o.semantic_payload
                    .as_ref()
                    .and_then(|sp| sp.provenance.as_ref())
                    .and_then(|p| p.get("signal_kind"))
                    .is_none()
            })
            .max_by_key(|o| o.created_at)
            .map(|o| o.object_id.clone())
            .unwrap_or(oid);
        Ok(latest_id)
    })?;

    println!("action=emit-tool-call");
    println!("object_id={}", object_id);
    println!("tool_name={}", tool_name);
    if let Some(d) = duration_s {
        println!("duration_s={:.3}", d);
    }
    Ok(())
}

// ── P6: Ghost session recovery ────────────────────────────────────────────────

fn session_tombstone_create_command(
    input: &PathBuf,
    session_id: &str,
    created_by: &str,
    output: Option<&Path>,
) -> Result<()> {
    let resolved_input = resolve_authoritative_snapshot_input(input);
    let (mut store, _resolved) = import_snapshot_file(&resolved_input)?;
    let result = create_session_tombstone(&mut store, session_id, created_by, now_fixed())?;

    // Write updated snapshot back to the output path (or in-place).
    let out_path: &Path = output.unwrap_or(resolved_input.as_path());
    fs::write(out_path, serialize_snapshot(&store)?)
        .with_context(|| format!("failed to write snapshot '{}'", out_path.display()))?;

    println!("action=session-tombstone-create");
    println!("original_session_id={}", result.original_session_id);
    println!("tombstone_object_id={}", result.tombstone_object_id);
    println!(
        "embedding_preserved={}",
        if result.embedding_preserved { "yes" } else { "no" }
    );
    println!("cluster_memberships={}", result.cluster_memberships.len());
    println!("output={}", out_path.display());
    Ok(())
}

fn session_prune_check_command(input: &PathBuf, session_id: &str) -> Result<()> {
    let (store, _resolved) =
        import_snapshot_file(&resolve_authoritative_snapshot_input(input))?;
    let result = session_prune_check(&store, session_id)?;
    print!("{}", format_prune_check_result(&result));
    Ok(())
}

fn session_prune_safe_command(
    input: &PathBuf,
    session_id: &str,
    created_by: &str,
    output: Option<&Path>,
) -> Result<()> {
    let resolved_input = resolve_authoritative_snapshot_input(input);
    let (mut store, _resolved) = import_snapshot_file(&resolved_input)?;
    let result = session_prune_safe(&mut store, session_id, created_by, now_fixed())?;

    if result.pruned {
        // Write updated snapshot back.
        let out_path: &Path = output.unwrap_or(resolved_input.as_path());
        fs::write(out_path, serialize_snapshot(&store)?)
            .with_context(|| format!("failed to write snapshot '{}'", out_path.display()))?;
        println!("status=pruned");
        println!(
            "tombstone_id={}",
            result.tombstone_object_id.as_deref().unwrap_or("")
        );
        println!("clusters_preserved={}", result.clusters_preserved);
        println!("output={}", out_path.display());
    } else {
        println!("status=skipped");
        if let Some(ref reason) = result.skip_reason {
            println!("reason={}", reason);
        }
    }
    Ok(())
}

fn session_prune_batch_command(
    input: &PathBuf,
    ids_file: &PathBuf,
    created_by: &str,
    output: Option<&Path>,
) -> Result<()> {
    let ids_text = fs::read_to_string(ids_file)
        .with_context(|| format!("failed to read ids-file '{}'", ids_file.display()))?;
    let session_ids: Vec<String> = ids_text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect();

    let resolved_input = resolve_authoritative_snapshot_input(input);
    let (mut store, _resolved) = import_snapshot_file(&resolved_input)?;
    let result = session_prune_batch(&mut store, &session_ids, created_by, now_fixed());

    if result.pruned > 0 {
        // Write updated snapshot back.
        let out_path: &Path = output.unwrap_or(resolved_input.as_path());
        fs::write(out_path, serialize_snapshot(&store)?)
            .with_context(|| format!("failed to write snapshot '{}'", out_path.display()))?;
        println!("output={}", out_path.display());
    }
    println!("pruned={}", result.pruned);
    println!("skipped={}", result.skipped);
    println!("total={}", result.total);
    Ok(())
}

fn session_tombstone_expire_command(
    input: &PathBuf,
    max_age_days: u32,
    output: Option<&Path>,
) -> Result<()> {
    let resolved_input = resolve_authoritative_snapshot_input(input);
    let (mut store, _resolved) = import_snapshot_file(&resolved_input)?;
    let result = session_tombstone_expire(&mut store, max_age_days, now_fixed())?;

    if result.expired > 0 {
        let out_path: &Path = output.unwrap_or(resolved_input.as_path());
        fs::write(out_path, serialize_snapshot(&store)?)
            .with_context(|| format!("failed to write snapshot '{}'", out_path.display()))?;
        println!("output={}", out_path.display());
    }
    println!("expired={}", result.expired);
    println!("kept={}", result.kept);
    Ok(())
}
