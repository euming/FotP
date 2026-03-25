use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, FixedOffset, Utc};
use serde::{Deserialize, Serialize};

use crate::agent_pool::{self, AllocateResult, ReleaseResult};
use crate::importer::derive_snapshot_input_path;
use crate::invariants::validate_invariants;
use crate::persistence::{deserialize_snapshot, serialize_snapshot};
use crate::route_memory::{
    append_route_episode_entry, load_route_episode_entries, RouteMemoryStore, RouteReplayEpisodeEntry,
};
use crate::smartlist_write::{
    attach_member, attach_member_before, attach_to_category, bootstrap_recency_ladder, categorize_inbox,
    create_bucket, create_category, create_note, default_note_id_for_mutation, detach_member,
    gc_sweep, get_bucket, get_note, get_rollup, list_categories, list_memberships, list_recency_tiers,
    move_member, normalize_path, rotate_recency_tiers, set_bucket_fields, set_ordering_policy,
    set_retrieval_visibility, set_rollup, write_time_attach,
    SmartListAttachResult, SmartListBucketInfo, SmartListCategorizationResult, SmartListCategoryInfo,
    SmartListDetachResult, SmartListGcResult, SmartListMoveResult, SmartListNoteInfo,
    SmartListRecencyTierInfo, SmartListRollupChild, SmartListRollupInfo, SmartListRotationResult,
    SmartListVisibilityResult,
};
use crate::taskgraph_write::{
    archive_thread, checkpoint_active_thread, claim_thread, heartbeat_thread_claim, pop_thread, push_tangent,
    release_thread_claim, start_thread, TaskClaimCommandResult, TaskGraphCommandResult,
};
use crate::bugreport::{
    self, BugFixInfo, BugReportInfo, CreateBugFixParams, CreateBugReportParams,
};
use crate::dream::{dream_touch, DreamTouchResult};
use crate::store::AmsStore;

const LOCK_TIMEOUT: Duration = Duration::from_secs(10);
const LOCK_RETRY_DELAY: Duration = Duration::from_millis(25);
const SHARED_BACKEND_ROOT_ENV: &str = "AMS_WRITE_BACKEND_ROOT";
const SHARED_BACKEND_CORPUS_KEY_ENV: &str = "AMS_WRITE_CORPUS_KEY";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteBackendMode {
    LocalSibling,
    SharedRoot,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteBackendManifest {
    pub backend_mode: WriteBackendMode,
    pub corpus_key: String,
    pub corpus_ref: String,
    pub seed_snapshot_path: PathBuf,
    pub snapshot_path: PathBuf,
    pub log_path: PathBuf,
    pub state_path: PathBuf,
    pub lock_path: PathBuf,
    pub updated_at: DateTime<FixedOffset>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteBackendStatus {
    pub backend_mode: WriteBackendMode,
    pub corpus_key: String,
    pub corpus_ref: String,
    pub backend_root: Option<PathBuf>,
    pub corpus_dir: PathBuf,
    pub snapshot_path: PathBuf,
    pub log_path: PathBuf,
    pub state_path: PathBuf,
    pub lock_path: PathBuf,
    pub manifest_path: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteRecoveryReport {
    pub backend_mode: WriteBackendMode,
    pub corpus_key: String,
    pub current_version: u64,
    pub log_events: usize,
    pub state_matches_log: bool,
    pub manifest_matches_paths: bool,
    pub snapshot_exists: bool,
    pub invariant_violations: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationKind {
    RouteMemoryRecorded,
    SmartListBucketCreated,
    SmartListNoteCreated,
    SmartListMemberAttached,
    SmartListMemberInsertedBefore,
    SmartListMemberDetached,
    SmartListMemberMoved,
    SmartListBucketFieldsSet,
    SmartListRollupSet,
    SmartListVisibilitySet,
    ThreadStarted,
    ThreadTangentPushed,
    ThreadCheckpointed,
    ThreadPopped,
    ThreadArchived,
    ThreadClaimed,
    ThreadClaimHeartbeatRecorded,
    ThreadClaimReleased,
    SmartListCategoryCreated,
    SmartListCategoryAttached,
    SmartListOrderingPolicySet,
    SmartListRecencyBootstrapped,
    SmartListRotationRun,
    SmartListCategorizationRun,
    SmartListGcRun,
    SmartListWriteTimeAttached,
    AgentPoolAllocated,
    AgentPoolReleased,
    BugreportCreated,
    BugreportStatusUpdated,
    BugfixCreated,
    DreamTouchApplied,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WriteMutationPayload {
    RouteMemoryRecorded { episode: RouteReplayEpisodeEntry },
    SmartListBucketCreated { path: String, durability: String },
    SmartListNoteCreated {
        note_id: String,
        title: String,
        text: String,
        bucket_paths: Vec<String>,
        durability: String,
    },
    SmartListMemberAttached { path: String, member_ref: String },
    SmartListMemberInsertedBefore {
        path: String,
        member_ref: String,
        before_member_ref: String,
    },
    SmartListMemberDetached { path: String, member_ref: String },
    SmartListMemberMoved {
        source_path: String,
        target_path: String,
        member_ref: String,
        before_member_ref: Option<String>,
    },
    SmartListBucketFieldsSet {
        path: String,
        fields: BTreeMap<String, String>,
    },
    SmartListRollupSet {
        path: String,
        summary: String,
        scope: String,
        stop_hint: Option<String>,
        child_highlights: Vec<SmartListRollupChild>,
        durability: String,
    },
    SmartListVisibilitySet {
        path: String,
        visibility: String,
        recursive: bool,
        include_notes: bool,
        include_rollups: bool,
    },
    ThreadStarted {
        thread_id: Option<String>,
        title: String,
        current_step: String,
        next_command: String,
        branch_off_anchor: Option<String>,
        artifact_ref: Option<String>,
    },
    ThreadTangentPushed {
        thread_id: Option<String>,
        title: String,
        current_step: String,
        next_command: String,
        branch_off_anchor: Option<String>,
        artifact_ref: Option<String>,
    },
    ThreadCheckpointed {
        current_step: String,
        next_command: String,
        branch_off_anchor: Option<String>,
        artifact_ref: Option<String>,
    },
    ThreadPopped,
    ThreadArchived {
        thread_id: Option<String>,
    },
    ThreadClaimed {
        thread_id: Option<String>,
        agent_id: String,
        lease_seconds: i64,
        claim_token: Option<String>,
    },
    ThreadClaimHeartbeatRecorded {
        thread_id: Option<String>,
        agent_id: String,
        claim_token: String,
        lease_seconds: i64,
    },
    ThreadClaimReleased {
        thread_id: Option<String>,
        agent_id: String,
        claim_token: String,
        release_reason: Option<String>,
    },
    SmartListCategoryCreated { name: String },
    SmartListCategoryAttached { object_id: String, category: String },
    SmartListOrderingPolicySet { path: String, policy: String, direction: String },
    SmartListRecencyBootstrapped,
    SmartListRotationRun { dry_run: bool },
    SmartListCategorizationRun { dry_run: bool },
    SmartListGcRun { dry_run: bool, default_ttl_hours: u64 },
    SmartListWriteTimeAttached { object_id: String },
    AgentPoolAllocated { agent_ref: String, task_path: String },
    AgentPoolReleased { agent_ref: String, task_path: String },
    BugreportCreated { source_agent: String, severity: String },
    BugreportStatusUpdated { bug_id: String, new_status: String },
    BugfixCreated { title: String },
    DreamTouchApplied { object_id: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MutationEnvelope {
    pub mutation_id: String,
    pub mutation_kind: MutationKind,
    pub actor_id: String,
    pub event_time: DateTime<FixedOffset>,
    pub corpus_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<u64>,
    pub payload: WriteMutationPayload,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteServiceState {
    pub current_version: u64,
    #[serde(default)]
    pub applied_mutation_ids: BTreeMap<String, u64>,
    pub updated_at: DateTime<FixedOffset>,
}

impl Default for WriteServiceState {
    fn default() -> Self {
        Self {
            current_version: 0,
            applied_mutation_ids: BTreeMap::new(),
            updated_at: Utc::now().fixed_offset(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteApplyResult {
    pub applied: bool,
    pub version: u64,
    pub mirrored_legacy: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordRouteEpisodeRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub episode: RouteReplayEpisodeEntry,
    pub legacy_mirror_path: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteStateComparison {
    pub authoritative_events: usize,
    pub legacy_events: usize,
    pub matches: bool,
    pub authoritative_canonical: String,
    pub legacy_canonical: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateSmartListBucketRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub path: String,
    pub durable: bool,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateSmartListNoteRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub title: String,
    pub text: String,
    pub bucket_paths: Vec<String>,
    pub durable: bool,
    pub created_by: String,
    pub note_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AttachSmartListMemberRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub path: String,
    pub member_ref: String,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InsertSmartListMemberBeforeRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub path: String,
    pub member_ref: String,
    pub before_member_ref: String,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DetachSmartListMemberRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub path: String,
    pub member_ref: String,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MoveSmartListMemberRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub source_path: String,
    pub target_path: String,
    pub member_ref: String,
    pub before_member_ref: Option<String>,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SetSmartListBucketFieldsRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub path: String,
    pub fields: BTreeMap<String, String>,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SetSmartListRollupRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub path: String,
    pub summary: String,
    pub scope: String,
    pub stop_hint: Option<String>,
    pub child_highlights: Vec<SmartListRollupChild>,
    pub durable: bool,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SetSmartListVisibilityRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub path: String,
    pub visibility: String,
    pub recursive: bool,
    pub include_notes: bool,
    pub include_rollups: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateSmartListCategoryRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub name: String,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AttachSmartListCategoryRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub object_id: String,
    pub category: String,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SetSmartListOrderingPolicyRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub path: String,
    pub policy: String,
    pub direction: String,
    pub tie_breaker: Option<String>,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BootstrapRecencyLadderRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RotateRecencyTiersRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub dry_run: bool,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CategorizeInboxRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub dry_run: bool,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GcSweepRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub dry_run: bool,
    pub default_ttl_hours: u64,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriteTimeAttachRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub object_id: String,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AllocateAgentPoolRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub agent_ref: String,
    pub task_path: String,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReleaseAgentPoolRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub agent_ref: String,
    pub task_path: String,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateBugreportRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub source_agent: String,
    pub parent_agent: String,
    pub error_output: String,
    pub stack_context: String,
    pub attempted_fixes: Vec<String>,
    pub reproduction_steps: Vec<String>,
    pub recommended_fix_plan: String,
    pub severity: String,
    pub durable: bool,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateBugreportStatusRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub bug_id: String,
    pub new_status: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateBugfixRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub title: String,
    pub description: String,
    pub fix_recipe: String,
    pub linked_bugreport_id: Option<String>,
    pub durable: bool,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StartThreadRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub title: String,
    pub current_step: String,
    pub next_command: String,
    pub thread_id: Option<String>,
    pub branch_off_anchor: Option<String>,
    pub artifact_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PushTangentRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub title: String,
    pub current_step: String,
    pub next_command: String,
    pub thread_id: Option<String>,
    pub branch_off_anchor: Option<String>,
    pub artifact_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CheckpointThreadRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub current_step: String,
    pub next_command: String,
    pub branch_off_anchor: Option<String>,
    pub artifact_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PopThreadRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ArchiveThreadRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub thread_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClaimThreadRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub thread_id: Option<String>,
    pub agent_id: String,
    pub lease_seconds: i64,
    pub claim_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HeartbeatThreadClaimRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub thread_id: Option<String>,
    pub agent_id: String,
    pub claim_token: String,
    pub lease_seconds: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReleaseThreadClaimRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    pub thread_id: Option<String>,
    pub agent_id: String,
    pub claim_token: String,
    pub release_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DreamTouchRequest {
    pub mutation_id: String,
    pub actor_id: String,
    pub corpus_ref: String,
    pub expected_version: Option<u64>,
    /// The focal object to touch (promote in its SmartLists and build shortcuts for).
    pub object_id: String,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteResourceResult<T> {
    pub write: WriteApplyResult,
    pub resource: T,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteServicePaths {
    pub backend_mode: WriteBackendMode,
    pub corpus_key: String,
    pub corpus_ref: String,
    pub backend_root: Option<PathBuf>,
    pub corpus_dir: PathBuf,
    pub log_path: PathBuf,
    pub state_path: PathBuf,
    pub lock_path: PathBuf,
    pub snapshot_path: PathBuf,
    pub seed_snapshot_path: PathBuf,
    pub manifest_path: Option<PathBuf>,
    /// True when the input path refers to the factories template store.
    /// Write operations that are not SmartList template management (e.g. plan
    /// execution, node completion) must be rejected when this flag is set.
    pub is_factories_store: bool,
}

#[derive(Clone, Debug)]
pub struct WriteService {
    paths: WriteServicePaths,
}

impl WriteService {
    pub fn from_input(input: &Path) -> Self {
        Self {
            paths: resolve_write_service_paths(input),
        }
    }

    /// Returns true if the write lock file currently exists on disk.
    /// Useful for read paths that want to skip best-effort writes when the
    /// lock is already held, avoiding the 10 s lock-timeout delay.
    pub fn is_locked(&self) -> bool {
        self.paths.lock_path.exists()
    }

    /// Returns an error if this service is backed by the factories store.
    ///
    /// Call this at the top of any operation that is NOT a SmartList template
    /// management operation — in particular, all swarm-plan execution commands
    /// (push, pop, observe, advance, interrupt, resume, complete-node).
    ///
    /// Switch and park are registry meta-ops that intentionally update factories,
    /// so they must NOT call this guard.
    pub fn guard_not_factories(&self, operation: &str) -> Result<()> {
        if self.paths.is_factories_store {
            bail!(
                "factories write-guard: operation '{}' is not permitted on the factories store. \
                 Factories stores only accept SmartList template management operations. \
                 Did you pass the wrong --input path? Use the per-plan store for plan execution.",
                operation
            );
        }
        Ok(())
    }

    #[cfg(test)]
    fn from_paths(paths: WriteServicePaths) -> Self {
        Self { paths }
    }

    pub fn paths(&self) -> &WriteServicePaths {
        &self.paths
    }

    pub fn backend_status(&self) -> WriteBackendStatus {
        WriteBackendStatus {
            backend_mode: self.paths.backend_mode.clone(),
            corpus_key: self.paths.corpus_key.clone(),
            corpus_ref: self.paths.corpus_ref.clone(),
            backend_root: self.paths.backend_root.clone(),
            corpus_dir: self.paths.corpus_dir.clone(),
            snapshot_path: self.paths.snapshot_path.clone(),
            log_path: self.paths.log_path.clone(),
            state_path: self.paths.state_path.clone(),
            lock_path: self.paths.lock_path.clone(),
            manifest_path: self.paths.manifest_path.clone(),
        }
    }

    pub fn validate_recovery(&self) -> Result<WriteRecoveryReport> {
        let state = self.load_or_recover_state()?;
        let envelopes = load_envelopes(&self.paths.log_path)?;
        let rebuilt_state = rebuild_state_from_envelopes(&envelopes);
        let store = self.load_snapshot_store()?;
        let invariant_violations = validate_invariants(&store).len();
        let manifest_matches_paths = match self.load_backend_manifest()? {
            Some(manifest) => manifest.snapshot_path == self.paths.snapshot_path
                && manifest.log_path == self.paths.log_path
                && manifest.state_path == self.paths.state_path
                && manifest.lock_path == self.paths.lock_path
                && manifest.corpus_key == self.paths.corpus_key
                && manifest.corpus_ref == self.paths.corpus_ref
                && manifest.backend_mode == self.paths.backend_mode,
            None => true,
        };

        Ok(WriteRecoveryReport {
            backend_mode: self.paths.backend_mode.clone(),
            corpus_key: self.paths.corpus_key.clone(),
            current_version: state.current_version,
            log_events: envelopes.len(),
            state_matches_log: state.current_version == rebuilt_state.current_version
                && state.applied_mutation_ids == rebuilt_state.applied_mutation_ids,
            manifest_matches_paths,
            snapshot_exists: self.paths.snapshot_path.exists(),
            invariant_violations,
        })
    }

    /// Best-effort variant: fails immediately if the write lock is held.
    pub fn try_record_route_episode(&self, request: &RecordRouteEpisodeRequest) -> Result<WriteApplyResult> {
        let _guard = try_acquire_lock(&self.paths.lock_path)?;
        self.record_route_episode_inner(request)
    }

    pub fn record_route_episode(&self, request: &RecordRouteEpisodeRequest) -> Result<WriteApplyResult> {
        let _guard = acquire_lock(&self.paths.lock_path)?;
        self.record_route_episode_inner(request)
    }

    fn record_route_episode_inner(&self, request: &RecordRouteEpisodeRequest) -> Result<WriteApplyResult> {
        let mut state = self.load_or_recover_state()?;

        if let Some(version) = state.applied_mutation_ids.get(&request.mutation_id).copied() {
            return Ok(WriteApplyResult {
                applied: false,
                version,
                mirrored_legacy: false,
            });
        }

        if let Some(expected_version) = request.expected_version {
            if expected_version != state.current_version {
                bail!(
                    "expected_version mismatch: expected {} but authoritative version is {}",
                    expected_version,
                    state.current_version
                );
            }
        }

        let envelope = MutationEnvelope {
            mutation_id: request.mutation_id.clone(),
            mutation_kind: MutationKind::RouteMemoryRecorded,
            actor_id: request.actor_id.clone(),
            event_time: Utc::now().fixed_offset(),
            corpus_ref: request.corpus_ref.clone(),
            expected_version: request.expected_version,
            payload: WriteMutationPayload::RouteMemoryRecorded {
                episode: request.episode.clone(),
            },
        };

        append_envelope(&self.paths.log_path, &envelope)?;
        state.current_version += 1;
        state
            .applied_mutation_ids
            .insert(request.mutation_id.clone(), state.current_version);
        state.updated_at = envelope.event_time;
        write_state(&self.paths.state_path, &state)?;

        let mut mirrored_legacy = false;
        if let Some(path) = request.legacy_mirror_path.as_ref() {
            append_route_episode_entry(path, &request.episode)?;
            mirrored_legacy = true;
        }

        Ok(WriteApplyResult {
            applied: true,
            version: state.current_version,
            mirrored_legacy,
        })
    }

    pub fn create_smartlist_bucket(
        &self,
        request: &CreateSmartListBucketRequest,
    ) -> Result<WriteResourceResult<SmartListBucketInfo>> {
        let path = normalize_path(&request.path)?;
        let path_for_apply = path.clone();
        let path_for_load = path.clone();
        let durable = if request.durable { "durable" } else { "short_term" }.to_string();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListBucketCreated,
            WriteMutationPayload::SmartListBucketCreated {
                path: path.clone(),
                durability: durable,
            },
            move |store, now_utc| create_bucket(store, &path_for_apply, request.durable, &request.created_by, now_utc),
            move |store| {
                get_bucket(store, &path_for_load)
                    .ok_or_else(|| anyhow!("bucket '{}' not found after mutation", path_for_load))
            },
        )
    }

    pub fn create_smartlist_note(
        &self,
        request: &CreateSmartListNoteRequest,
    ) -> Result<WriteResourceResult<SmartListNoteInfo>> {
        let note_id = request
            .note_id
            .clone()
            .unwrap_or_else(|| default_note_id_for_mutation(&request.mutation_id));
        let note_id_for_apply = note_id.clone();
        let note_id_for_load = note_id.clone();
        let title = request.title.clone();
        let text = request.text.clone();
        let bucket_paths = request.bucket_paths.clone();
        let durable = if request.durable { "durable" } else { "short_term" }.to_string();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListNoteCreated,
            WriteMutationPayload::SmartListNoteCreated {
                note_id: note_id.clone(),
                title: title.clone(),
                text: text.clone(),
                bucket_paths: bucket_paths.clone(),
                durability: durable,
            },
            move |store, now_utc| {
                create_note(
                    store,
                    &title,
                    &text,
                    &bucket_paths,
                    request.durable,
                    &request.created_by,
                    now_utc,
                    Some(&note_id_for_apply),
                )
            },
            move |store| {
                get_note(store, &note_id_for_load)
                    .ok_or_else(|| anyhow!("note '{}' not found after mutation", note_id_for_load))
            },
        )
    }

    pub fn attach_smartlist_member(
        &self,
        request: &AttachSmartListMemberRequest,
    ) -> Result<WriteResourceResult<SmartListAttachResult>> {
        let path = normalize_path(&request.path)?;
        let member_ref = request.member_ref.clone();
        let path_for_apply = path.clone();
        let path_for_load = path.clone();
        let member_ref_for_apply = member_ref.clone();
        let member_ref_for_load = member_ref.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListMemberAttached,
            WriteMutationPayload::SmartListMemberAttached {
                path: path.clone(),
                member_ref: member_ref.clone(),
            },
            move |store, now_utc| attach_member(store, &path_for_apply, &member_ref_for_apply, &request.created_by, now_utc),
            move |store| {
                let bucket = get_bucket(store, &path_for_load)
                    .ok_or_else(|| anyhow!("bucket '{}' not found after attach", path_for_load))?;
                let resolved_member = if store.objects().contains_key(&member_ref_for_load) {
                    member_ref_for_load.clone()
                } else {
                    get_bucket(store, &member_ref_for_load)
                        .map(|info| info.object_id)
                        .unwrap_or_else(|| member_ref_for_load.clone())
                };
                Ok(SmartListAttachResult {
                    path: bucket.path,
                    member_object_id: resolved_member,
                })
            },
        )
    }

    pub fn insert_smartlist_member_before(
        &self,
        request: &InsertSmartListMemberBeforeRequest,
    ) -> Result<WriteResourceResult<SmartListAttachResult>> {
        let path = normalize_path(&request.path)?;
        let member_ref = request.member_ref.clone();
        let before_member_ref = request.before_member_ref.clone();
        let path_for_apply = path.clone();
        let path_for_load = path.clone();
        let member_ref_for_apply = member_ref.clone();
        let member_ref_for_load = member_ref.clone();
        let before_member_ref_for_apply = before_member_ref.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListMemberInsertedBefore,
            WriteMutationPayload::SmartListMemberInsertedBefore {
                path: path.clone(),
                member_ref: member_ref.clone(),
                before_member_ref: before_member_ref.clone(),
            },
            move |store, now_utc| {
                attach_member_before(
                    store,
                    &path_for_apply,
                    &member_ref_for_apply,
                    &before_member_ref_for_apply,
                    &request.created_by,
                    now_utc,
                )
            },
            move |store| {
                let bucket = get_bucket(store, &path_for_load)
                    .ok_or_else(|| anyhow!("bucket '{}' not found after insert-before", path_for_load))?;
                let resolved_member = if store.objects().contains_key(&member_ref_for_load) {
                    member_ref_for_load.clone()
                } else {
                    get_bucket(store, &member_ref_for_load)
                        .map(|info| info.object_id)
                        .unwrap_or_else(|| member_ref_for_load.clone())
                };
                Ok(SmartListAttachResult {
                    path: bucket.path,
                    member_object_id: resolved_member,
                })
            },
        )
    }

    pub fn detach_smartlist_member(
        &self,
        request: &DetachSmartListMemberRequest,
    ) -> Result<WriteResourceResult<SmartListDetachResult>> {
        let path = normalize_path(&request.path)?;
        let member_ref = request.member_ref.clone();
        let path_for_apply = path.clone();
        let path_for_load = path.clone();
        let member_ref_for_apply = member_ref.clone();
        let member_ref_for_load = member_ref.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListMemberDetached,
            WriteMutationPayload::SmartListMemberDetached {
                path: path.clone(),
                member_ref: member_ref.clone(),
            },
            move |store, now_utc| detach_member(store, &path_for_apply, &member_ref_for_apply, &request.created_by, now_utc),
            move |store| {
                let bucket = get_bucket(store, &path_for_load)
                    .ok_or_else(|| anyhow!("bucket '{}' not found after detach", path_for_load))?;
                let resolved_member = if store.objects().contains_key(&member_ref_for_load) {
                    member_ref_for_load.clone()
                } else {
                    get_bucket(store, &member_ref_for_load)
                        .map(|info| info.object_id)
                        .unwrap_or_else(|| member_ref_for_load.clone())
                };
                Ok(SmartListDetachResult {
                    path: bucket.path,
                    member_object_id: resolved_member,
                    removed: true,
                })
            },
        )
    }

    pub fn move_smartlist_member(
        &self,
        request: &MoveSmartListMemberRequest,
    ) -> Result<WriteResourceResult<SmartListMoveResult>> {
        let source_path = normalize_path(&request.source_path)?;
        let target_path = normalize_path(&request.target_path)?;
        let member_ref = request.member_ref.clone();
        let before_member_ref = request.before_member_ref.clone();
        let source_for_apply = source_path.clone();
        let target_for_apply = target_path.clone();
        let source_for_load = source_path.clone();
        let target_for_load = target_path.clone();
        let member_ref_for_apply = member_ref.clone();
        let member_ref_for_load = member_ref.clone();
        let before_for_apply = before_member_ref.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListMemberMoved,
            WriteMutationPayload::SmartListMemberMoved {
                source_path: source_path.clone(),
                target_path: target_path.clone(),
                member_ref: member_ref.clone(),
                before_member_ref: before_member_ref.clone(),
            },
            move |store, now_utc| {
                move_member(
                    store,
                    &source_for_apply,
                    &target_for_apply,
                    &member_ref_for_apply,
                    before_for_apply.as_deref(),
                    &request.created_by,
                    now_utc,
                )
            },
            move |store| {
                let resolved_member = if store.objects().contains_key(&member_ref_for_load) {
                    member_ref_for_load.clone()
                } else {
                    get_bucket(store, &member_ref_for_load)
                        .map(|info| info.object_id)
                        .unwrap_or_else(|| member_ref_for_load.clone())
                };
                Ok(SmartListMoveResult {
                    source_path: source_for_load.clone(),
                    target_path: target_for_load.clone(),
                    member_object_id: resolved_member,
                })
            },
        )
    }

    pub fn set_smartlist_bucket_fields(
        &self,
        request: &SetSmartListBucketFieldsRequest,
    ) -> Result<WriteResourceResult<SmartListBucketInfo>> {
        let path = normalize_path(&request.path)?;
        let fields = request.fields.clone();
        let path_for_apply = path.clone();
        let path_for_load = path.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListBucketFieldsSet,
            WriteMutationPayload::SmartListBucketFieldsSet {
                path: path.clone(),
                fields: fields.clone(),
            },
            move |store, now_utc| set_bucket_fields(store, &path_for_apply, &fields, &request.created_by, now_utc),
            move |store| {
                get_bucket(store, &path_for_load)
                    .ok_or_else(|| anyhow!("bucket '{}' not found after field update", path_for_load))
            },
        )
    }

    pub fn set_smartlist_rollup(
        &self,
        request: &SetSmartListRollupRequest,
    ) -> Result<WriteResourceResult<SmartListRollupInfo>> {
        let path = normalize_path(&request.path)?;
        let summary = request.summary.clone();
        let scope = request.scope.clone();
        let stop_hint = request.stop_hint.clone();
        let child_highlights = request.child_highlights.clone();
        let path_for_apply = path.clone();
        let path_for_load = path.clone();
        let durability = if request.durable { "durable" } else { "short_term" }.to_string();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListRollupSet,
            WriteMutationPayload::SmartListRollupSet {
                path: path.clone(),
                summary: summary.clone(),
                scope: scope.clone(),
                stop_hint: stop_hint.clone(),
                child_highlights: child_highlights.clone(),
                durability,
            },
            move |store, now_utc| {
                set_rollup(
                    store,
                    &path_for_apply,
                    &summary,
                    &scope,
                    stop_hint.as_deref(),
                    &child_highlights,
                    request.durable,
                    &request.created_by,
                    now_utc,
                )
            },
            move |store| {
                get_rollup(store, &path_for_load)
                    .ok_or_else(|| anyhow!("rollup '{}' not found after mutation", path_for_load))
            },
        )
    }

    pub fn set_smartlist_visibility(
        &self,
        request: &SetSmartListVisibilityRequest,
    ) -> Result<WriteResourceResult<SmartListVisibilityResult>> {
        let path = normalize_path(&request.path)?;
        let visibility = request.visibility.clone();
        let path_for_apply = path.clone();
        let path_for_load = path.clone();
        let visibility_for_apply = visibility.clone();
        let visibility_for_load = visibility.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListVisibilitySet,
            WriteMutationPayload::SmartListVisibilitySet {
                path: path.clone(),
                visibility: visibility.clone(),
                recursive: request.recursive,
                include_notes: request.include_notes,
                include_rollups: request.include_rollups,
            },
            move |store, now_utc| {
                set_retrieval_visibility(
                    store,
                    &path_for_apply,
                    &visibility_for_apply,
                    request.recursive,
                    request.include_notes,
                    request.include_rollups,
                    now_utc,
                )
            },
            move |_store| {
                Ok(SmartListVisibilityResult {
                    path: path_for_load.clone(),
                    retrieval_visibility: visibility_for_load.clone(),
                    buckets_updated: 0,
                    notes_updated: 0,
                    rollups_updated: 0,
                })
            },
        )
    }

    pub fn create_smartlist_category(
        &self,
        request: &CreateSmartListCategoryRequest,
    ) -> Result<WriteResourceResult<SmartListCategoryInfo>> {
        let name = request.name.clone();
        let name_for_apply = name.clone();
        let name_for_load = name.clone();
        let created_by = request.created_by.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListCategoryCreated,
            WriteMutationPayload::SmartListCategoryCreated { name: name.clone() },
            move |store, now_utc| create_category(store, &name_for_apply, &created_by, now_utc),
            move |store| {
                let cats = list_categories(store);
                cats.into_iter()
                    .find(|c| c.name == name_for_load || c.bucket_path.ends_with(&name_for_load))
                    .ok_or_else(|| anyhow!("category '{}' not found after creation", name))
            },
        )
    }

    pub fn attach_to_smartlist_category(
        &self,
        request: &AttachSmartListCategoryRequest,
    ) -> Result<WriteResourceResult<SmartListAttachResult>> {
        let object_id = request.object_id.clone();
        let category = request.category.clone();
        let object_id_for_apply = object_id.clone();
        let category_for_apply = category.clone();
        let created_by = request.created_by.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListCategoryAttached,
            WriteMutationPayload::SmartListCategoryAttached { object_id: object_id.clone(), category: category.clone() },
            move |store, now_utc| attach_to_category(store, &object_id_for_apply, &category_for_apply, &created_by, now_utc),
            move |_store| {
                Ok(SmartListAttachResult {
                    path: format!("smartlist/category/{}", category),
                    member_object_id: object_id.clone(),
                })
            },
        )
    }

    pub fn set_smartlist_ordering_policy(
        &self,
        request: &SetSmartListOrderingPolicyRequest,
    ) -> Result<WriteResourceResult<SmartListBucketInfo>> {
        let path = normalize_path(&request.path)?;
        let policy = request.policy.clone();
        let direction = request.direction.clone();
        let tie_breaker = request.tie_breaker.clone();
        let path_for_apply = path.clone();
        let path_for_load = path.clone();
        let created_by = request.created_by.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListOrderingPolicySet,
            WriteMutationPayload::SmartListOrderingPolicySet { path: path.clone(), policy: policy.clone(), direction: direction.clone() },
            move |store, now_utc| set_ordering_policy(store, &path_for_apply, &policy, &direction, tie_breaker.as_deref(), &created_by, now_utc),
            move |store| {
                get_bucket(store, &path_for_load)
                    .ok_or_else(|| anyhow!("bucket '{}' not found after ordering policy set", path_for_load))
            },
        )
    }

    pub fn bootstrap_smartlist_recency_ladder(
        &self,
        request: &BootstrapRecencyLadderRequest,
    ) -> Result<WriteResourceResult<Vec<SmartListRecencyTierInfo>>> {
        let created_by = request.created_by.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListRecencyBootstrapped,
            WriteMutationPayload::SmartListRecencyBootstrapped,
            move |store, now_utc| bootstrap_recency_ladder(store, &created_by, now_utc),
            move |store| Ok(list_recency_tiers(store)),
        )
    }

    pub fn rotate_smartlist_recency_tiers(
        &self,
        request: &RotateRecencyTiersRequest,
    ) -> Result<WriteResourceResult<SmartListRotationResult>> {
        let dry_run = request.dry_run;
        let created_by = request.created_by.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListRotationRun,
            WriteMutationPayload::SmartListRotationRun { dry_run },
            move |store, now_utc| rotate_recency_tiers(store, now_utc, dry_run, &created_by),
            move |_store| Ok(SmartListRotationResult { promotions: Vec::new(), dry_run }),
        )
    }

    pub fn categorize_smartlist_inbox(
        &self,
        request: &CategorizeInboxRequest,
    ) -> Result<WriteResourceResult<SmartListCategorizationResult>> {
        let dry_run = request.dry_run;
        let created_by = request.created_by.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListCategorizationRun,
            WriteMutationPayload::SmartListCategorizationRun { dry_run },
            move |store, now_utc| categorize_inbox(store, now_utc, dry_run, &created_by),
            move |_store| Ok(SmartListCategorizationResult { processed: 0, categorized: 0, already_categorized: 0, dry_run }),
        )
    }

    pub fn smartlist_gc_sweep(
        &self,
        request: &GcSweepRequest,
    ) -> Result<WriteResourceResult<SmartListGcResult>> {
        let dry_run = request.dry_run;
        let default_ttl_hours = request.default_ttl_hours;
        let created_by = request.created_by.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListGcRun,
            WriteMutationPayload::SmartListGcRun { dry_run, default_ttl_hours },
            move |store, now_utc| gc_sweep(store, now_utc, default_ttl_hours, dry_run, &created_by),
            move |_store| Ok(SmartListGcResult { removed: Vec::new(), restored_to_inbox: 0, dry_run }),
        )
    }

    pub fn smartlist_write_time_attach(
        &self,
        request: &WriteTimeAttachRequest,
    ) -> Result<WriteResourceResult<Vec<String>>> {
        let object_id = request.object_id.clone();
        let object_id_for_apply = object_id.clone();
        let created_by = request.created_by.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::SmartListWriteTimeAttached,
            WriteMutationPayload::SmartListWriteTimeAttached { object_id: object_id.clone() },
            move |store, now_utc| write_time_attach(store, &object_id_for_apply, &created_by, now_utc),
            move |store| Ok(list_memberships(store, &object_id).bucket_paths),
        )
    }

    pub fn allocate_agent_pool(
        &self,
        request: &AllocateAgentPoolRequest,
    ) -> Result<WriteResourceResult<AllocateResult>> {
        let agent_ref = request.agent_ref.clone();
        let task_path = request.task_path.clone();
        let created_by = request.created_by.clone();
        let agent_ref_for_apply = agent_ref.clone();
        let task_path_for_apply = task_path.clone();
        let agent_ref_for_load = agent_ref.clone();
        let task_path_for_load = task_path.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::AgentPoolAllocated,
            WriteMutationPayload::AgentPoolAllocated {
                agent_ref: agent_ref.clone(),
                task_path: task_path.clone(),
            },
            move |store, now_utc| {
                agent_pool::allocate(store, &agent_ref_for_apply, &task_path_for_apply, &created_by, now_utc)
            },
            move |_store| {
                Ok(AllocateResult {
                    agent_object_id: agent_ref_for_load.clone(),
                    task_path: task_path_for_load.clone(),
                })
            },
        )
    }

    pub fn release_agent_pool(
        &self,
        request: &ReleaseAgentPoolRequest,
    ) -> Result<WriteResourceResult<ReleaseResult>> {
        let agent_ref = request.agent_ref.clone();
        let task_path = request.task_path.clone();
        let created_by = request.created_by.clone();
        let agent_ref_for_apply = agent_ref.clone();
        let task_path_for_apply = task_path.clone();
        let agent_ref_for_load = agent_ref.clone();
        let task_path_for_load = task_path.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::AgentPoolReleased,
            WriteMutationPayload::AgentPoolReleased {
                agent_ref: agent_ref.clone(),
                task_path: task_path.clone(),
            },
            move |store, now_utc| {
                agent_pool::release(store, &agent_ref_for_apply, &task_path_for_apply, &created_by, now_utc)
            },
            move |_store| {
                Ok(ReleaseResult {
                    agent_object_id: agent_ref_for_load.clone(),
                    task_path: task_path_for_load.clone(),
                })
            },
        )
    }

    pub fn create_bugreport(
        &self,
        request: &CreateBugreportRequest,
    ) -> Result<WriteResourceResult<BugReportInfo>> {
        let source_agent = request.source_agent.clone();
        let parent_agent = request.parent_agent.clone();
        let error_output = request.error_output.clone();
        let stack_context = request.stack_context.clone();
        let attempted_fixes = request.attempted_fixes.clone();
        let reproduction_steps = request.reproduction_steps.clone();
        let recommended_fix_plan = request.recommended_fix_plan.clone();
        let severity = request.severity.clone();
        let durable = request.durable;
        let created_by = request.created_by.clone();
        let severity_for_payload = severity.clone();
        let source_for_payload = source_agent.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::BugreportCreated,
            WriteMutationPayload::BugreportCreated {
                source_agent: source_for_payload,
                severity: severity_for_payload,
            },
            move |store, now_utc| {
                bugreport::create_bugreport(store, &CreateBugReportParams {
                    source_agent: &source_agent,
                    parent_agent: &parent_agent,
                    error_output: &error_output,
                    stack_context: &stack_context,
                    attempted_fixes,
                    reproduction_steps,
                    recommended_fix_plan: &recommended_fix_plan,
                    severity: &severity,
                    durable,
                    created_by: &created_by,
                }, now_utc)
            },
            |_store| bail!("bugreport create is not idempotent-loadable"),
        )
    }

    pub fn update_bugreport_status(
        &self,
        request: &UpdateBugreportStatusRequest,
    ) -> Result<WriteResourceResult<BugReportInfo>> {
        let bug_id = request.bug_id.clone();
        let new_status = request.new_status.clone();
        let bug_id_for_load = bug_id.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::BugreportStatusUpdated,
            WriteMutationPayload::BugreportStatusUpdated {
                bug_id: bug_id.clone(),
                new_status: new_status.clone(),
            },
            move |store, now_utc| {
                bugreport::update_bugreport_status(store, &bug_id, &new_status, now_utc)
            },
            move |store| {
                bugreport::get_bugreport(store, &bug_id_for_load)
                    .ok_or_else(|| anyhow!("bug report '{}' not found", bug_id_for_load))
            },
        )
    }

    pub fn create_bugfix(
        &self,
        request: &CreateBugfixRequest,
    ) -> Result<WriteResourceResult<BugFixInfo>> {
        let title = request.title.clone();
        let description = request.description.clone();
        let fix_recipe = request.fix_recipe.clone();
        let linked_bugreport_id = request.linked_bugreport_id.clone();
        let durable = request.durable;
        let created_by = request.created_by.clone();
        let title_for_payload = title.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::BugfixCreated,
            WriteMutationPayload::BugfixCreated {
                title: title_for_payload,
            },
            move |store, now_utc| {
                bugreport::create_bugfix(store, &CreateBugFixParams {
                    title: &title,
                    description: &description,
                    fix_recipe: &fix_recipe,
                    linked_bugreport_id: linked_bugreport_id.as_deref(),
                    durable,
                    created_by: &created_by,
                }, now_utc)
            },
            |_store| bail!("bugfix create is not idempotent-loadable"),
        )
    }

    /// Apply a dream-touch to the given focal object.
    ///
    /// This is the atomic dreaming primitive: it promotes the object to the head
    /// of each of its SmartList memberships and populates the shortcut bucket
    /// with all co-members, making multi-hop paths accessible as single-hop
    /// Atlas lookups.
    pub fn dream_touch(
        &self,
        request: &DreamTouchRequest,
    ) -> Result<WriteResourceResult<DreamTouchResult>> {
        let object_id = request.object_id.clone();
        let created_by = request.created_by.clone();
        let object_id_for_load = object_id.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::DreamTouchApplied,
            WriteMutationPayload::DreamTouchApplied {
                object_id: object_id.clone(),
            },
            move |store, now_utc| dream_touch(store, &object_id, &created_by, now_utc),
            move |store| {
                // On idempotent re-run, reconstruct a minimal result from the
                // current store state.
                use crate::dream::shortcut_path_for;
                use crate::smartlist_write::list_memberships;
                let memberships = list_memberships(store, &object_id_for_load);
                let shortcut_path = shortcut_path_for(&object_id_for_load)?;
                Ok(DreamTouchResult {
                    object_id: object_id_for_load.clone(),
                    lists_promoted: memberships.bucket_paths.len(),
                    shortcuts_added: 0, // idempotent re-run, no new shortcuts
                    shortcut_path,
                })
            },
        )
    }

    pub fn start_thread(&self, request: &StartThreadRequest) -> Result<WriteResourceResult<TaskGraphCommandResult>> {
        let title = request.title.clone();
        let current_step = request.current_step.clone();
        let next_command = request.next_command.clone();
        let thread_id = request.thread_id.clone();
        let branch_off_anchor = request.branch_off_anchor.clone();
        let artifact_ref = request.artifact_ref.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::ThreadStarted,
            WriteMutationPayload::ThreadStarted {
                thread_id: thread_id.clone(),
                title: title.clone(),
                current_step: current_step.clone(),
                next_command: next_command.clone(),
                branch_off_anchor: branch_off_anchor.clone(),
                artifact_ref: artifact_ref.clone(),
            },
            move |store, now_utc| {
                start_thread(
                    store,
                    &title,
                    &current_step,
                    &next_command,
                    thread_id.as_deref(),
                    branch_off_anchor.as_deref(),
                    artifact_ref.as_deref(),
                    now_utc,
                )
            },
            move |store| {
                let overview = crate::taskgraph_write::inspect_task_graph(store)?;
                let thread = overview
                    .active_thread
                    .clone()
                    .ok_or_else(|| anyhow!("no active thread after thread-start"))?;
                Ok(TaskGraphCommandResult {
                    thread,
                    overview,
                    checkpoint: None,
                    resumed_checkpoint: None,
                })
            },
        )
    }

    pub fn push_tangent(&self, request: &PushTangentRequest) -> Result<WriteResourceResult<TaskGraphCommandResult>> {
        let title = request.title.clone();
        let current_step = request.current_step.clone();
        let next_command = request.next_command.clone();
        let thread_id = request.thread_id.clone();
        let branch_off_anchor = request.branch_off_anchor.clone();
        let artifact_ref = request.artifact_ref.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::ThreadTangentPushed,
            WriteMutationPayload::ThreadTangentPushed {
                thread_id: thread_id.clone(),
                title: title.clone(),
                current_step: current_step.clone(),
                next_command: next_command.clone(),
                branch_off_anchor: branch_off_anchor.clone(),
                artifact_ref: artifact_ref.clone(),
            },
            move |store, now_utc| {
                push_tangent(
                    store,
                    &title,
                    &current_step,
                    &next_command,
                    thread_id.as_deref(),
                    branch_off_anchor.as_deref(),
                    artifact_ref.as_deref(),
                    now_utc,
                )
            },
            move |store| {
                let overview = crate::taskgraph_write::inspect_task_graph(store)?;
                let thread = overview
                    .active_thread
                    .clone()
                    .ok_or_else(|| anyhow!("no active thread after thread-push-tangent"))?;
                Ok(TaskGraphCommandResult {
                    thread,
                    overview,
                    checkpoint: None,
                    resumed_checkpoint: None,
                })
            },
        )
    }

    pub fn checkpoint_thread(
        &self,
        request: &CheckpointThreadRequest,
    ) -> Result<WriteResourceResult<TaskGraphCommandResult>> {
        let current_step = request.current_step.clone();
        let next_command = request.next_command.clone();
        let branch_off_anchor = request.branch_off_anchor.clone();
        let artifact_ref = request.artifact_ref.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::ThreadCheckpointed,
            WriteMutationPayload::ThreadCheckpointed {
                current_step: current_step.clone(),
                next_command: next_command.clone(),
                branch_off_anchor: branch_off_anchor.clone(),
                artifact_ref: artifact_ref.clone(),
            },
            move |store, now_utc| {
                checkpoint_active_thread(
                    store,
                    &current_step,
                    &next_command,
                    branch_off_anchor.as_deref(),
                    artifact_ref.as_deref(),
                    now_utc,
                )
            },
            move |store| {
                let overview = crate::taskgraph_write::inspect_task_graph(store)?;
                let thread = overview
                    .active_thread
                    .clone()
                    .ok_or_else(|| anyhow!("no active thread after thread-checkpoint"))?;
                Ok(TaskGraphCommandResult {
                    thread,
                    overview,
                    checkpoint: None,
                    resumed_checkpoint: None,
                })
            },
        )
    }

    pub fn pop_thread(&self, request: &PopThreadRequest) -> Result<WriteResourceResult<TaskGraphCommandResult>> {
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::ThreadPopped,
            WriteMutationPayload::ThreadPopped,
            move |store, now_utc| pop_thread(store, now_utc),
            move |store| {
                let overview = crate::taskgraph_write::inspect_task_graph(store)?;
                let thread = overview
                    .active_thread
                    .clone()
                    .ok_or_else(|| anyhow!("no active thread after thread-pop"))?;
                Ok(TaskGraphCommandResult {
                    thread,
                    overview,
                    checkpoint: None,
                    resumed_checkpoint: None,
                })
            },
        )
    }

    pub fn archive_thread(
        &self,
        request: &ArchiveThreadRequest,
    ) -> Result<WriteResourceResult<TaskGraphCommandResult>> {
        let thread_id = request.thread_id.clone();
        let thread_id_for_payload = thread_id.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::ThreadArchived,
            WriteMutationPayload::ThreadArchived {
                thread_id: thread_id_for_payload,
            },
            move |store, now_utc| archive_thread(store, thread_id.as_deref(), now_utc),
            move |store| {
                let overview = crate::taskgraph_write::inspect_task_graph(store)?;
                let thread = request
                    .thread_id
                    .as_deref()
                    .map(|id| crate::taskgraph_write::inspect_task_graph(store)?.all_threads.into_iter().find(|thread| thread.thread_id == id)
                        .ok_or_else(|| anyhow!("thread '{}' not found after archive", id)))
                    .transpose()?
                    .or_else(|| overview.active_thread.clone())
                    .or_else(|| overview.all_threads.first().cloned())
                    .ok_or_else(|| anyhow!("no thread available after thread-archive"))?;
                Ok(TaskGraphCommandResult {
                    thread,
                    overview,
                    checkpoint: None,
                    resumed_checkpoint: None,
                })
            },
        )
    }

    pub fn claim_thread(&self, request: &ClaimThreadRequest) -> Result<WriteResourceResult<TaskClaimCommandResult>> {
        let thread_id = request.thread_id.clone();
        let thread_id_for_payload = thread_id.clone();
        let thread_id_for_apply = thread_id.clone();
        let thread_id_for_load = thread_id.clone();
        let agent_id = request.agent_id.clone();
        let lease_seconds = request.lease_seconds;
        let claim_token = request.claim_token.clone();
        let claim_token_for_apply = claim_token.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::ThreadClaimed,
            WriteMutationPayload::ThreadClaimed {
                thread_id: thread_id_for_payload,
                agent_id: agent_id.clone(),
                lease_seconds,
                claim_token: claim_token.clone(),
            },
            move |store, now_utc| {
                claim_thread(
                    store,
                    thread_id_for_apply.as_deref(),
                    &agent_id,
                    lease_seconds,
                    claim_token_for_apply.as_deref(),
                    now_utc,
                )
            },
            move |store| {
                let overview = crate::taskgraph_write::inspect_task_graph(store)?;
                let thread = match thread_id_for_load.as_deref() {
                    Some(id) => overview
                        .all_threads
                        .iter()
                        .find(|thread| thread.thread_id == id)
                        .cloned(),
                    None => overview.active_thread.clone(),
                }
                .ok_or_else(|| anyhow!("no thread available after thread-claim"))?;
                let claim = thread.claims.first().cloned();
                Ok(TaskClaimCommandResult { thread, claim, overview })
            },
        )
    }

    pub fn heartbeat_thread_claim(
        &self,
        request: &HeartbeatThreadClaimRequest,
    ) -> Result<WriteResourceResult<TaskClaimCommandResult>> {
        let thread_id = request.thread_id.clone();
        let thread_id_for_payload = thread_id.clone();
        let thread_id_for_apply = thread_id.clone();
        let thread_id_for_load = thread_id.clone();
        let agent_id = request.agent_id.clone();
        let claim_token = request.claim_token.clone();
        let claim_token_for_apply = claim_token.clone();
        let lease_seconds = request.lease_seconds;
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::ThreadClaimHeartbeatRecorded,
            WriteMutationPayload::ThreadClaimHeartbeatRecorded {
                thread_id: thread_id_for_payload,
                agent_id: agent_id.clone(),
                claim_token: claim_token.clone(),
                lease_seconds,
            },
            move |store, now_utc| {
                heartbeat_thread_claim(
                    store,
                    thread_id_for_apply.as_deref(),
                    &agent_id,
                    &claim_token_for_apply,
                    lease_seconds,
                    now_utc,
                )
            },
            move |store| {
                let overview = crate::taskgraph_write::inspect_task_graph(store)?;
                let thread = match thread_id_for_load.as_deref() {
                    Some(id) => overview
                        .all_threads
                        .iter()
                        .find(|thread| thread.thread_id == id)
                        .cloned(),
                    None => overview.active_thread.clone(),
                }
                .ok_or_else(|| anyhow!("no thread available after thread-heartbeat"))?;
                let claim = thread.claims.first().cloned();
                Ok(TaskClaimCommandResult { thread, claim, overview })
            },
        )
    }

    pub fn release_thread_claim(
        &self,
        request: &ReleaseThreadClaimRequest,
    ) -> Result<WriteResourceResult<TaskClaimCommandResult>> {
        let thread_id = request.thread_id.clone();
        let thread_id_for_payload = thread_id.clone();
        let thread_id_for_apply = thread_id.clone();
        let thread_id_for_load = thread_id.clone();
        let agent_id = request.agent_id.clone();
        let claim_token = request.claim_token.clone();
        let claim_token_for_apply = claim_token.clone();
        let release_reason = request.release_reason.clone();
        let release_reason_for_apply = release_reason.clone();
        self.apply_snapshot_mutation(
            &request.mutation_id,
            &request.actor_id,
            &request.corpus_ref,
            request.expected_version,
            MutationKind::ThreadClaimReleased,
            WriteMutationPayload::ThreadClaimReleased {
                thread_id: thread_id_for_payload,
                agent_id: agent_id.clone(),
                claim_token: claim_token.clone(),
                release_reason: release_reason.clone(),
            },
            move |store, now_utc| {
                release_thread_claim(
                    store,
                    thread_id_for_apply.as_deref(),
                    &agent_id,
                    &claim_token_for_apply,
                    release_reason_for_apply.as_deref(),
                    now_utc,
                )
            },
            move |store| {
                let overview = crate::taskgraph_write::inspect_task_graph(store)?;
                let thread = match thread_id_for_load.as_deref() {
                    Some(id) => overview
                        .all_threads
                        .iter()
                        .find(|thread| thread.thread_id == id)
                        .cloned(),
                    None => overview.active_thread.clone(),
                }
                .ok_or_else(|| anyhow!("no thread available after thread-release"))?;
                let claim = thread.claims.first().cloned();
                Ok(TaskClaimCommandResult { thread, claim, overview })
            },
        )
    }

    pub fn materialize_route_memory_store(&self) -> Result<Option<RouteMemoryStore>> {
        let episodes = self.load_route_episodes()?;
        if episodes.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RouteMemoryStore::from_episodes(episodes)))
        }
    }

    pub fn load_route_episodes(&self) -> Result<Vec<RouteReplayEpisodeEntry>> {
        let envelopes = load_envelopes(&self.paths.log_path)?;
        Ok(envelopes
            .into_iter()
            .filter_map(|envelope| match envelope.payload {
                WriteMutationPayload::RouteMemoryRecorded { episode } => Some(episode),
                _ => None,
            })
            .collect())
    }

    pub fn compare_with_legacy_route_sidecar(&self, legacy_path: &Path) -> Result<RouteStateComparison> {
        let authoritative = self.load_route_episodes()?;
        let legacy = if legacy_path.exists() {
            load_route_episode_entries(legacy_path)?
        } else {
            Vec::new()
        };
        let authoritative_canonical = canonicalize_episodes(&authoritative)?;
        let legacy_canonical = canonicalize_episodes(&legacy)?;
        Ok(RouteStateComparison {
            authoritative_events: authoritative.len(),
            legacy_events: legacy.len(),
            matches: authoritative_canonical == legacy_canonical,
            authoritative_canonical,
            legacy_canonical,
        })
    }

    fn load_or_recover_state(&self) -> Result<WriteServiceState> {
        if self.paths.state_path.exists() {
            let raw = fs::read_to_string(&self.paths.state_path)
                .with_context(|| format!("failed to read write-service state '{}'", self.paths.state_path.display()))?;
            let state: WriteServiceState = serde_json::from_str(&raw)
                .with_context(|| format!("failed to parse write-service state '{}'", self.paths.state_path.display()))?;
            return Ok(state);
        }

        let envelopes = load_envelopes(&self.paths.log_path)?;
        Ok(rebuild_state_from_envelopes(&envelopes))
    }

    fn load_snapshot_store(&self) -> Result<AmsStore> {
        if self.paths.snapshot_path.exists() {
            let raw = fs::read_to_string(&self.paths.snapshot_path)
                .with_context(|| format!("failed to read AMS snapshot '{}'", self.paths.snapshot_path.display()))?;
            return deserialize_snapshot(&raw)
                .with_context(|| format!("failed to deserialize AMS snapshot '{}'", self.paths.snapshot_path.display()));
        }

        if self.paths.seed_snapshot_path != self.paths.snapshot_path && self.paths.seed_snapshot_path.exists() {
            let raw = fs::read_to_string(&self.paths.seed_snapshot_path).with_context(|| {
                format!(
                    "failed to read seed AMS snapshot '{}'",
                    self.paths.seed_snapshot_path.display()
                )
            })?;
            return deserialize_snapshot(&raw).with_context(|| {
                format!(
                    "failed to deserialize seed AMS snapshot '{}'",
                    self.paths.seed_snapshot_path.display()
                )
            });
        }

        Ok(AmsStore::new())
    }

    /// Load the snapshot, run a mutation closure, then save the result.
    /// Acquires the file lock for the duration. Returns the closure's output.
    pub fn run_with_store_mut<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut AmsStore, DateTime<FixedOffset>) -> Result<T>,
    {
        let _guard = acquire_lock(&self.paths.lock_path)?;
        let mut store = self.load_snapshot_store()?;
        let now = Utc::now().fixed_offset();
        let result = f(&mut store, now)?;
        self.write_snapshot_store(&store)?;
        Ok(result)
    }

    fn write_snapshot_store(&self, store: &AmsStore) -> Result<()> {
        if let Some(parent) = self.paths.snapshot_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create AMS snapshot directory '{}'", parent.display()))?;
        }
        fs::write(&self.paths.snapshot_path, serialize_snapshot(store)?)
            .with_context(|| format!("failed to write AMS snapshot '{}'", self.paths.snapshot_path.display()))?;
        self.write_backend_manifest()
    }

    fn apply_snapshot_mutation<T, F, G>(
        &self,
        mutation_id: &str,
        actor_id: &str,
        corpus_ref: &str,
        expected_version: Option<u64>,
        mutation_kind: MutationKind,
        payload: WriteMutationPayload,
        apply: F,
        load_existing: G,
    ) -> Result<WriteResourceResult<T>>
    where
        F: FnOnce(&mut AmsStore, DateTime<FixedOffset>) -> Result<T>,
        G: Fn(&AmsStore) -> Result<T>,
    {
        let _guard = acquire_lock(&self.paths.lock_path)?;
        let mut state = self.load_or_recover_state()?;
        let mut store = self.load_snapshot_store()?;

        if let Some(version) = state.applied_mutation_ids.get(mutation_id).copied() {
            return Ok(WriteResourceResult {
                write: WriteApplyResult {
                    applied: false,
                    version,
                    mirrored_legacy: false,
                },
                resource: load_existing(&store)?,
            });
        }

        if let Some(expected_version) = expected_version {
            if expected_version != state.current_version {
                bail!(
                    "expected_version mismatch: expected {} but authoritative version is {}",
                    expected_version,
                    state.current_version
                );
            }
        }

        let event_time = Utc::now().fixed_offset();
        let resource = apply(&mut store, event_time)?;
        self.write_snapshot_store(&store)?;

        let envelope = MutationEnvelope {
            mutation_id: mutation_id.to_string(),
            mutation_kind,
            actor_id: actor_id.to_string(),
            event_time,
            corpus_ref: corpus_ref.to_string(),
            expected_version,
            payload,
        };
        append_envelope(&self.paths.log_path, &envelope)?;
        state.current_version += 1;
        state.applied_mutation_ids.insert(mutation_id.to_string(), state.current_version);
        state.updated_at = event_time;
        write_state(&self.paths.state_path, &state)?;
        self.write_backend_manifest()?;

        Ok(WriteResourceResult {
            write: WriteApplyResult {
                applied: true,
                version: state.current_version,
                mirrored_legacy: false,
            },
            resource,
        })
    }

    fn write_backend_manifest(&self) -> Result<()> {
        let Some(manifest_path) = self.paths.manifest_path.as_ref() else {
            return Ok(());
        };
        if let Some(parent) = manifest_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create backend manifest directory '{}'", parent.display()))?;
        }
        let manifest = WriteBackendManifest {
            backend_mode: self.paths.backend_mode.clone(),
            corpus_key: self.paths.corpus_key.clone(),
            corpus_ref: self.paths.corpus_ref.clone(),
            seed_snapshot_path: self.paths.seed_snapshot_path.clone(),
            snapshot_path: self.paths.snapshot_path.clone(),
            log_path: self.paths.log_path.clone(),
            state_path: self.paths.state_path.clone(),
            lock_path: self.paths.lock_path.clone(),
            updated_at: Utc::now().fixed_offset(),
        };
        fs::write(manifest_path, serde_json::to_vec_pretty(&manifest)?)
            .with_context(|| format!("failed to write backend manifest '{}'", manifest_path.display()))
    }

    fn load_backend_manifest(&self) -> Result<Option<WriteBackendManifest>> {
        let Some(path) = self.paths.manifest_path.as_ref() else {
            return Ok(None);
        };
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read backend manifest '{}'", path.display()))?;
        let manifest = serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse backend manifest '{}'", path.display()))?;
        Ok(Some(manifest))
    }
}

pub fn default_write_log_path(input: &Path) -> PathBuf {
    with_write_suffix(input, "ams-write-log.jsonl")
}

pub fn default_write_state_path(input: &Path) -> PathBuf {
    with_write_suffix(input, "ams-write-state.json")
}

pub fn default_write_lock_path(input: &Path) -> PathBuf {
    with_write_suffix(input, "ams-write.lock")
}

pub fn resolve_authoritative_snapshot_input(input: &Path) -> PathBuf {
    let paths = resolve_write_service_paths(input);
    if paths.snapshot_path.exists() {
        paths.snapshot_path
    } else {
        paths.seed_snapshot_path
    }
}

fn legacy_route_write_log_path(input: &Path) -> PathBuf {
    with_write_suffix(input, "route-write-log.jsonl")
}

fn legacy_route_write_state_path(input: &Path) -> PathBuf {
    with_write_suffix(input, "route-write-state.json")
}

fn legacy_route_write_lock_path(input: &Path) -> PathBuf {
    with_write_suffix(input, "route-write.lock")
}

fn with_write_suffix(input: &Path, suffix: &str) -> PathBuf {
    let file_name = input
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("ams-write");
    let file_name = if let Some(prefix) = file_name.strip_suffix(".memory.jsonl") {
        format!("{prefix}.{suffix}")
    } else if let Some(prefix) = file_name.strip_suffix(".jsonl") {
        format!("{prefix}.{suffix}")
    } else {
        format!("{file_name}.{suffix}")
    };
    input.with_file_name(file_name)
}

fn resolve_write_service_paths(input: &Path) -> WriteServicePaths {
    let backend_root = std::env::var(SHARED_BACKEND_ROOT_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from);
    let corpus_key = std::env::var(SHARED_BACKEND_CORPUS_KEY_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty());
    resolve_write_service_paths_for_root(input, backend_root.as_deref(), corpus_key.as_deref())
}

fn resolve_write_service_paths_for_root(
    input: &Path,
    backend_root: Option<&Path>,
    corpus_key_override: Option<&str>,
) -> WriteServicePaths {
    let corpus_ref = input.display().to_string();
    let default_snapshot_path = derive_snapshot_input_path(input).unwrap_or_else(|_| input.with_file_name("memory.ams.json"));
    let corpus_key = corpus_key_override
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.to_string())
        .unwrap_or_else(|| default_corpus_key(input));

    // Detect factories store by file stem — "factories.memory.jsonl" or "factories.memory.ams.json".
    let is_factories_store = input
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.starts_with("factories."))
        .unwrap_or(false);

    if let Some(root) = backend_root {
        let root = root.to_path_buf();
        let corpus_dir = root.join(&corpus_key);
        return WriteServicePaths {
            backend_mode: WriteBackendMode::SharedRoot,
            corpus_key,
            corpus_ref,
            backend_root: Some(root),
            corpus_dir: corpus_dir.clone(),
            log_path: corpus_dir.join("ams-write-log.jsonl"),
            state_path: corpus_dir.join("ams-write-state.json"),
            lock_path: corpus_dir.join("ams-write.lock"),
            snapshot_path: corpus_dir.join("memory.ams.json"),
            seed_snapshot_path: default_snapshot_path,
            manifest_path: Some(corpus_dir.join("backend-manifest.json")),
            is_factories_store,
        };
    }

    let default_log_path = default_write_log_path(input);
    let default_state_path = default_write_state_path(input);
    let default_lock_path = default_write_lock_path(input);
    let corpus_dir = input.parent().unwrap_or_else(|| Path::new(".")).to_path_buf();
    WriteServicePaths {
        backend_mode: WriteBackendMode::LocalSibling,
        corpus_key,
        corpus_ref,
        backend_root: None,
        corpus_dir,
        log_path: if !default_log_path.exists() && legacy_route_write_log_path(input).exists() {
            legacy_route_write_log_path(input)
        } else {
            default_log_path
        },
        state_path: if !default_state_path.exists() && legacy_route_write_state_path(input).exists() {
            legacy_route_write_state_path(input)
        } else {
            default_state_path
        },
        lock_path: if !default_lock_path.exists() && legacy_route_write_lock_path(input).exists() {
            legacy_route_write_lock_path(input)
        } else {
            default_lock_path
        },
        snapshot_path: default_snapshot_path.clone(),
        seed_snapshot_path: default_snapshot_path,
        manifest_path: None,
        is_factories_store,
    }
}

fn default_corpus_key(input: &Path) -> String {
    let file_name = input.file_name().and_then(|value| value.to_str()).unwrap_or("corpus");
    let stem = file_name
        .strip_suffix(".memory.jsonl")
        .or_else(|| file_name.strip_suffix(".jsonl"))
        .unwrap_or(file_name);
    let mut normalized = stem
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch.to_ascii_lowercase() } else { '-' })
        .collect::<String>();
    while normalized.contains("--") {
        normalized = normalized.replace("--", "-");
    }
    normalized = normalized.trim_matches('-').to_string();
    if normalized.is_empty() {
        "corpus".to_string()
    } else {
        normalized
    }
}

pub fn canonicalize_episodes(episodes: &[RouteReplayEpisodeEntry]) -> Result<String> {
    let mut lines = episodes
        .iter()
        .map(serde_json::to_string)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("failed to serialize route episodes canonically")?;
    lines.sort();
    Ok(lines.join("\n"))
}

fn rebuild_state_from_envelopes(envelopes: &[MutationEnvelope]) -> WriteServiceState {
    let mut state = WriteServiceState::default();
    for (index, envelope) in envelopes.iter().enumerate() {
        let version = (index as u64) + 1;
        state.current_version = version;
        state
            .applied_mutation_ids
            .insert(envelope.mutation_id.clone(), version);
        state.updated_at = envelope.event_time;
    }
    state
}

fn append_envelope(path: &Path, envelope: &MutationEnvelope) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create write-log directory '{}'", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open authoritative write log '{}'", path.display()))?;
    writeln!(file, "{}", serde_json::to_string(envelope)?)
        .with_context(|| format!("failed to append authoritative write log '{}'", path.display()))
}

fn load_envelopes(path: &Path) -> Result<Vec<MutationEnvelope>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path)
        .with_context(|| format!("failed to open authoritative write log '{}'", path.display()))?;
    let reader = BufReader::new(file);
    let mut envelopes = Vec::new();
    for (index, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("failed to read line {} from '{}'", index + 1, path.display()))?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("//") {
            continue;
        }
        let envelope: MutationEnvelope = serde_json::from_str(trimmed).with_context(|| {
            format!(
                "failed to parse authoritative write mutation on line {} in '{}'",
                index + 1,
                path.display()
            )
        })?;
        envelopes.push(envelope);
    }
    Ok(envelopes)
}

fn write_state(path: &Path, state: &WriteServiceState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create write-state directory '{}'", parent.display()))?;
    }
    let temp_path = path.with_extension("json.tmp");
    fs::write(&temp_path, serde_json::to_vec_pretty(state)?)
        .with_context(|| format!("failed to write temporary write-state '{}'", temp_path.display()))?;
    fs::rename(&temp_path, path)
        .with_context(|| format!("failed to replace write-state '{}'", path.display()))
}

fn acquire_lock(path: &Path) -> Result<FileLockGuard> {
    acquire_lock_with_timeout(path, LOCK_TIMEOUT)
}

/// Try to acquire the lock without waiting.  Returns `Err` immediately if the
/// lock file already exists.  Intended for best-effort write side-effects on
/// read paths (route recording, freshness) where blocking on a held lock would
/// degrade read latency.
fn try_acquire_lock(path: &Path) -> Result<FileLockGuard> {
    acquire_lock_with_timeout(path, Duration::ZERO)
}

fn acquire_lock_with_timeout(path: &Path, timeout: Duration) -> Result<FileLockGuard> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create write-lock directory '{}'", parent.display()))?;
    }

    let start = Instant::now();
    loop {
        match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(mut file) => {
                writeln!(file, "pid={} acquired_at={}", std::process::id(), Utc::now().to_rfc3339()).ok();
                return Ok(FileLockGuard {
                    path: path.to_path_buf(),
                });
            }
            Err(error)
                if error.kind() == ErrorKind::AlreadyExists
                    || error.kind() == ErrorKind::PermissionDenied =>
            {
                if start.elapsed() >= timeout {
                    return Err(anyhow!(
                        "timed out waiting for write-service lock '{}'",
                        path.display()
                    ));
                }
                thread::sleep(LOCK_RETRY_DELAY);
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to acquire write-service lock '{}'", path.display()));
            }
        }
    }
}

struct FileLockGuard {
    path: PathBuf,
}

impl Drop for FileLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::DateTime;
    use tempfile::tempdir;

    use crate::route_memory::{
        RouteReplayEpisodeEntry, RouteReplayEpisodeInput, RouteReplayFrameInput, RouteReplayRouteInput,
    };

    use super::{
        canonicalize_episodes, default_write_log_path, default_write_state_path, ClaimThreadRequest,
        CreateSmartListBucketRequest, CreateSmartListNoteRequest, HeartbeatThreadClaimRequest,
        RecordRouteEpisodeRequest, ReleaseThreadClaimRequest, resolve_write_service_paths_for_root,
        StartThreadRequest, WriteBackendMode, WriteService,
    };

    #[test]
    fn duplicate_mutation_id_is_idempotent() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("all.memory.jsonl");
        let service = WriteService::from_input(&input);
        let episode = make_episode("one");

        let first = service
            .record_route_episode(&RecordRouteEpisodeRequest {
                mutation_id: "mut-1".to_string(),
                actor_id: "agent-a".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                episode: episode.clone(),
                legacy_mirror_path: None,
            })
            .unwrap();
        let second = service
            .record_route_episode(&RecordRouteEpisodeRequest {
                mutation_id: "mut-1".to_string(),
                actor_id: "agent-a".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                episode,
                legacy_mirror_path: None,
            })
            .unwrap();

        assert!(first.applied);
        assert!(!second.applied);
        assert_eq!(first.version, 1);
        assert_eq!(second.version, 1);
        assert_eq!(service.load_route_episodes().unwrap().len(), 1);
        assert!(default_write_log_path(&input).exists());
        assert!(default_write_state_path(&input).exists());
    }

    #[test]
    fn concurrent_writes_do_not_corrupt_authoritative_log() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("all.memory.jsonl");
        let service = Arc::new(WriteService::from_input(&input));

        let handles = (0..16)
            .map(|index| {
                let service = Arc::clone(&service);
                let input = input.clone();
                std::thread::spawn(move || {
                    service
                        .record_route_episode(&RecordRouteEpisodeRequest {
                            mutation_id: format!("mut-{index}"),
                            actor_id: format!("agent-{index}"),
                            corpus_ref: input.display().to_string(),
                            expected_version: None,
                            episode: make_episode(&format!("episode-{index}")),
                            legacy_mirror_path: None,
                        })
                        .unwrap();
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().unwrap();
        }

        let episodes = service.load_route_episodes().unwrap();
        assert_eq!(episodes.len(), 16);
        let canonical = canonicalize_episodes(&episodes).unwrap();
        assert!(canonical.contains("episode-0"));
        assert!(canonical.contains("episode-15"));
    }

    #[test]
    fn replayed_authoritative_state_matches_recorded_episodes() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("all.memory.jsonl");
        let service = WriteService::from_input(&input);
        let expected = vec![make_episode("one"), make_episode("two")];

        for (index, episode) in expected.iter().cloned().enumerate() {
            service
                .record_route_episode(&RecordRouteEpisodeRequest {
                    mutation_id: format!("mut-{index}"),
                    actor_id: "agent-a".to_string(),
                    corpus_ref: input.display().to_string(),
                    expected_version: None,
                    episode,
                    legacy_mirror_path: None,
                })
                .unwrap();
        }

        let actual = service.load_route_episodes().unwrap();
        assert_eq!(
            canonicalize_episodes(&actual).unwrap(),
            canonicalize_episodes(&expected).unwrap()
        );
        assert!(service.materialize_route_memory_store().unwrap().is_some());
    }

    #[test]
    fn compares_authoritative_state_with_legacy_sidecar() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("all.memory.jsonl");
        let legacy_path = crate::route_memory::default_route_memory_path(&input);
        let service = WriteService::from_input(&input);
        let episode = make_episode("one");

        service
            .record_route_episode(&RecordRouteEpisodeRequest {
                mutation_id: "mut-1".to_string(),
                actor_id: "agent-a".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                episode: episode.clone(),
                legacy_mirror_path: Some(legacy_path.clone()),
            })
            .unwrap();

        let comparison = service.compare_with_legacy_route_sidecar(&legacy_path).unwrap();
        assert_eq!(comparison.authoritative_events, 1);
        assert_eq!(comparison.legacy_events, 1);
        assert!(comparison.matches);
    }

    #[test]
    fn smartlist_bucket_write_updates_authoritative_snapshot() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("all.memory.jsonl");
        let service = WriteService::from_input(&input);

        let result = service
            .create_smartlist_bucket(&CreateSmartListBucketRequest {
                mutation_id: "bucket-1".to_string(),
                actor_id: "agent-a".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                path: "architecture/incubation/rust-replatform".to_string(),
                durable: true,
                created_by: "agent-a".to_string(),
            })
            .unwrap();

        assert!(result.write.applied);
        assert!(service.paths().snapshot_path.exists());
        let snapshot = service.load_snapshot_store().unwrap();
        assert!(snapshot
            .objects()
            .contains_key("smartlist-bucket:smartlist/architecture/incubation/rust-replatform"));
    }

    #[test]
    fn smartlist_note_write_is_idempotent_by_mutation_id() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("all.memory.jsonl");
        let service = WriteService::from_input(&input);

        let first = service
            .create_smartlist_note(&CreateSmartListNoteRequest {
                mutation_id: "note-1".to_string(),
                actor_id: "agent-a".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                title: "Title".to_string(),
                text: "Body".to_string(),
                bucket_paths: vec!["smartlist/architecture".to_string()],
                durable: true,
                created_by: "agent-a".to_string(),
                note_id: None,
            })
            .unwrap();
        let second = service
            .create_smartlist_note(&CreateSmartListNoteRequest {
                mutation_id: "note-1".to_string(),
                actor_id: "agent-a".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                title: "Title".to_string(),
                text: "Body".to_string(),
                bucket_paths: vec!["smartlist/architecture".to_string()],
                durable: true,
                created_by: "agent-a".to_string(),
                note_id: None,
            })
            .unwrap();

        assert!(first.write.applied);
        assert!(!second.write.applied);
        assert_eq!(first.resource.note_id, second.resource.note_id);
    }

    #[test]
    fn thread_claim_write_is_idempotent_by_mutation_id() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("all.memory.jsonl");
        let service = WriteService::from_input(&input);

        service
            .start_thread(&StartThreadRequest {
                mutation_id: "thread-start-1".to_string(),
                actor_id: "agent-a".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                title: "Root".to_string(),
                current_step: "Review".to_string(),
                next_command: "read".to_string(),
                thread_id: Some("root".to_string()),
                branch_off_anchor: None,
                artifact_ref: None,
            })
            .unwrap();

        let first = service
            .claim_thread(&ClaimThreadRequest {
                mutation_id: "claim-1".to_string(),
                actor_id: "agent-a".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                thread_id: Some("root".to_string()),
                agent_id: "agent-a".to_string(),
                lease_seconds: 60,
                claim_token: Some("claim-token".to_string()),
            })
            .unwrap();
        let second = service
            .claim_thread(&ClaimThreadRequest {
                mutation_id: "claim-1".to_string(),
                actor_id: "agent-a".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                thread_id: Some("root".to_string()),
                agent_id: "agent-a".to_string(),
                lease_seconds: 60,
                claim_token: Some("claim-token".to_string()),
            })
            .unwrap();

        assert!(first.write.applied);
        assert!(!second.write.applied);
        assert_eq!(
            first.resource.thread.active_claim.as_ref().map(|claim| claim.claim_token.clone()),
            second.resource.thread.active_claim.as_ref().map(|claim| claim.claim_token.clone())
        );
    }

    #[test]
    fn concurrent_claims_allow_single_live_owner() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("all.memory.jsonl");
        let service = Arc::new(WriteService::from_input(&input));
        service
            .start_thread(&StartThreadRequest {
                mutation_id: "thread-start-1".to_string(),
                actor_id: "agent-a".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                title: "Root".to_string(),
                current_step: "Review".to_string(),
                next_command: "read".to_string(),
                thread_id: Some("root".to_string()),
                branch_off_anchor: None,
                artifact_ref: None,
            })
            .unwrap();

        let handles = (0..8)
            .map(|index| {
                let service = Arc::clone(&service);
                let input = input.clone();
                std::thread::spawn(move || {
                    service.claim_thread(&ClaimThreadRequest {
                        mutation_id: format!("claim-{index}"),
                        actor_id: format!("agent-{index}"),
                        corpus_ref: input.display().to_string(),
                        expected_version: None,
                        thread_id: Some("root".to_string()),
                        agent_id: format!("agent-{index}"),
                        lease_seconds: 60,
                        claim_token: Some(format!("claim-token-{index}")),
                    })
                })
            })
            .collect::<Vec<_>>();

        let mut applied = 0;
        for handle in handles {
            match handle.join().unwrap() {
                Ok(result) if result.write.applied => applied += 1,
                Ok(_) => {}
                Err(error) => {
                    assert!(error.to_string().contains("already claimed"));
                }
            }
        }
        assert_eq!(applied, 1);
    }

    #[test]
    fn claim_heartbeat_and_release_roundtrip() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("all.memory.jsonl");
        let service = WriteService::from_input(&input);

        service
            .start_thread(&StartThreadRequest {
                mutation_id: "thread-start-1".to_string(),
                actor_id: "agent-a".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                title: "Root".to_string(),
                current_step: "Review".to_string(),
                next_command: "read".to_string(),
                thread_id: Some("root".to_string()),
                branch_off_anchor: None,
                artifact_ref: None,
            })
            .unwrap();
        let initial = service
            .claim_thread(&ClaimThreadRequest {
                mutation_id: "claim-1".to_string(),
                actor_id: "agent-a".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                thread_id: Some("root".to_string()),
                agent_id: "agent-a".to_string(),
                lease_seconds: 1,
                claim_token: Some("claim-token-1".to_string()),
            })
            .unwrap();
        assert_eq!(initial.resource.thread.active_claim.as_ref().unwrap().agent_id, "agent-a");

        let heartbeat = service
            .heartbeat_thread_claim(&HeartbeatThreadClaimRequest {
                mutation_id: "claim-heartbeat-1".to_string(),
                actor_id: "agent-b".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                thread_id: Some("root".to_string()),
                agent_id: "agent-a".to_string(),
                claim_token: "claim-token-1".to_string(),
                lease_seconds: 60,
            })
            .unwrap();
        assert_eq!(heartbeat.resource.claim.as_ref().unwrap().status, "heartbeat");

        let released = service
            .release_thread_claim(&ReleaseThreadClaimRequest {
                mutation_id: "claim-release-1".to_string(),
                actor_id: "agent-a".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                thread_id: Some("root".to_string()),
                agent_id: "agent-a".to_string(),
                claim_token: "claim-token-1".to_string(),
                release_reason: Some("complete".to_string()),
            })
            .unwrap();
        assert!(released.resource.thread.active_claim.is_none());
        assert_eq!(released.resource.claim.as_ref().unwrap().status, "released");
    }

    #[test]
    fn shared_backend_paths_move_authoritative_state_out_of_input_directory() {
        let dir = tempdir().unwrap();
        let backend_root = dir.path().join("backend");
        let input = dir.path().join("all.memory.jsonl");
        let service = WriteService::from_paths(resolve_write_service_paths_for_root(&input, Some(&backend_root), None));
        assert_eq!(service.paths().backend_mode, WriteBackendMode::SharedRoot);
        assert_eq!(service.paths().backend_root.as_ref(), Some(&backend_root));
        assert_eq!(service.paths().snapshot_path, backend_root.join("all").join("memory.ams.json"));
        assert_eq!(
            service.paths().manifest_path.as_ref(),
            Some(&backend_root.join("all").join("backend-manifest.json"))
        );
    }

    #[test]
    fn shared_backend_state_is_visible_to_new_client_instances() {
        let dir = tempdir().unwrap();
        let backend_root = dir.path().join("backend");
        let input = dir.path().join("all.memory.jsonl");
        let paths = resolve_write_service_paths_for_root(&input, Some(&backend_root), None);
        let service_a = WriteService::from_paths(paths.clone());
        service_a
            .start_thread(&StartThreadRequest {
                mutation_id: "thread-start-1".to_string(),
                actor_id: "agent-a".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                title: "Root".to_string(),
                current_step: "Review".to_string(),
                next_command: "read".to_string(),
                thread_id: Some("root".to_string()),
                branch_off_anchor: None,
                artifact_ref: None,
            })
            .unwrap();

        let service_b = WriteService::from_paths(paths);
        assert_eq!(service_a.paths().snapshot_path, service_b.paths().snapshot_path);
        assert!(service_b.paths().snapshot_path.exists());
        let store = service_b.load_snapshot_store().unwrap();
        let overview = crate::taskgraph_write::inspect_task_graph(&store).unwrap();
        assert_eq!(overview.active_thread.as_ref().map(|thread| thread.thread_id.as_str()), Some("root"));
    }

    #[test]
    fn recovery_validation_passes_for_shared_backend() {
        let dir = tempdir().unwrap();
        let backend_root = dir.path().join("backend");
        let input = dir.path().join("all.memory.jsonl");
        let paths = resolve_write_service_paths_for_root(&input, Some(&backend_root), None);
        let service = WriteService::from_paths(paths);
        service
            .start_thread(&StartThreadRequest {
                mutation_id: "thread-start-1".to_string(),
                actor_id: "agent-a".to_string(),
                corpus_ref: input.display().to_string(),
                expected_version: None,
                title: "Root".to_string(),
                current_step: "Review".to_string(),
                next_command: "read".to_string(),
                thread_id: Some("root".to_string()),
                branch_off_anchor: None,
                artifact_ref: None,
            })
            .unwrap();

        let report = service.validate_recovery().unwrap();
        assert_eq!(report.backend_mode, WriteBackendMode::SharedRoot);
        assert!(report.state_matches_log);
        assert!(report.manifest_matches_paths);
        assert!(report.snapshot_exists);
        assert_eq!(report.invariant_violations, 0);
    }

    // ── factories write-guard tests ────────────────────────────────────────────

    /// Plan execution commands must be rejected when the --input path refers to
    /// the factories store.  The factories store must remain byte-for-byte
    /// identical after the rejected call.
    #[test]
    fn factories_write_guard_rejects_plan_execution_ops() {
        use crate::persistence::serialize_snapshot;

        let dir = tempdir().unwrap();
        // Name the input file exactly as it appears in production.
        let factories_input = dir.path().join("factories.memory.jsonl");
        let factories_snapshot = dir.path().join("factories.memory.ams.json");

        // Write an empty seed so the service has something to load.
        let empty_store = crate::store::AmsStore::new();
        let empty_json = serialize_snapshot(&empty_store).unwrap();
        std::fs::write(&factories_input, &empty_json).unwrap();
        std::fs::write(&factories_snapshot, &empty_json).unwrap();

        let service = WriteService::from_input(&factories_input);
        assert!(service.paths().is_factories_store, "factories store must be detected");

        // Attempting a plan push on the factories store must fail — the guard
        // is called before run_with_store_mut, matching the production code path.
        let push_result = service.guard_not_factories("swarm-plan-push");
        assert!(push_result.is_err(), "push to factories must be rejected");
        let err_msg = push_result.unwrap_err().to_string();
        assert!(err_msg.contains("factories write-guard"), "error must mention write-guard");

        // Other execution ops are also rejected.
        assert!(service.guard_not_factories("swarm-plan-complete-node").is_err());
        assert!(service.guard_not_factories("swarm-plan-pop").is_err());
        assert!(service.guard_not_factories("swarm-plan-advance").is_err());
        assert!(service.guard_not_factories("swarm-plan-observe").is_err());

        // Factories snapshot must be unchanged — plan execution must not have
        // written anything.
        let after_json = std::fs::read_to_string(&factories_snapshot).unwrap_or_default();
        assert_eq!(
            after_json, empty_json,
            "factories snapshot must be unchanged after rejected plan operations"
        );
    }

    /// Switch and park are registry meta-ops that legitimately modify factories.
    /// They must NOT be blocked by the write-guard.
    #[test]
    fn factories_write_guard_allows_non_plan_ops() {
        let dir = tempdir().unwrap();
        let factories_input = dir.path().join("factories.memory.jsonl");

        let service = WriteService::from_input(&factories_input);
        assert!(service.paths().is_factories_store);

        // guard_not_factories is for plan execution ops only.
        // A non-plan op (e.g. creating a SmartList bucket — a template op)
        // must NOT call guard_not_factories and must succeed.
        let result = service.create_smartlist_bucket(&super::CreateSmartListBucketRequest {
            mutation_id: "bucket-1".to_string(),
            actor_id: "agent".to_string(),
            corpus_ref: factories_input.display().to_string(),
            expected_version: None,
            path: "factories/my-template".to_string(),
            durable: false,
            created_by: "test".to_string(),
        });
        assert!(result.is_ok(), "SmartList bucket creation on factories must succeed: {:?}", result.err());
    }

    fn make_episode(label: &str) -> RouteReplayEpisodeEntry {
        RouteReplayEpisodeEntry {
            frame: RouteReplayFrameInput {
                scope_lens: "local-first-lineage".to_string(),
                agent_role: "implementer".to_string(),
                mode: "build".to_string(),
                lineage_node_ids: vec!["node-a".to_string()],
                artifact_refs: Some(vec!["artifact.txt".to_string()]),
                failure_bucket: None,
            },
            route: RouteReplayRouteInput {
                ranking_source: "raw-lesson".to_string(),
                path: "direct".to_string(),
                cost: 0.4,
                risk_flags: None,
            },
            episode: RouteReplayEpisodeInput {
                query_text: format!("query-{label}"),
                occurred_at: DateTime::parse_from_rfc3339("2026-03-15T08:00:00+00:00").unwrap(),
                weak_result: false,
                used_fallback: false,
                winning_target_ref: format!("card-{label}"),
                top_target_refs: vec![format!("card-{label}")],
                user_feedback: None,
                tool_outcome: None,
            },
            candidate_target_refs: vec![format!("card-{label}")],
            winning_target_ref: format!("card-{label}"),
        }
    }
}
