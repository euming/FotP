use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::{JsonMap, ObjectRecord, SemanticPayload};
use crate::store::{AmsStore, StoreError};

pub const TASK_GRAPH_ROOT_CONTAINER: &str = "task-graph";
pub const TASK_GRAPH_ACTIVE_CONTAINER: &str = "task-graph:active";
pub const TASK_GRAPH_PARKED_CONTAINER: &str = "task-graph:parked";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCheckpointInfo {
    pub checkpoint_object_id: String,
    pub summary: String,
    pub current_step: String,
    pub next_command: String,
    pub branch_off_anchor: Option<String>,
    pub artifact_ref: Option<String>,
    pub created_at: DateTime<FixedOffset>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskArtifactInfo {
    pub artifact_object_id: String,
    pub label: String,
    pub artifact_ref: String,
    pub created_at: DateTime<FixedOffset>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskActiveClaimInfo {
    pub agent_id: String,
    pub claim_token: String,
    pub attempt: u64,
    pub lease_until: DateTime<FixedOffset>,
    pub heartbeat_at: DateTime<FixedOffset>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskClaimInfo {
    pub claim_object_id: String,
    pub agent_id: String,
    pub claim_token: String,
    pub status: String,
    pub attempt: u64,
    pub lease_until: Option<DateTime<FixedOffset>>,
    pub heartbeat_at: Option<DateTime<FixedOffset>>,
    pub created_at: DateTime<FixedOffset>,
    pub released_at: Option<DateTime<FixedOffset>>,
    pub release_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskThreadInfo {
    pub thread_object_id: String,
    pub thread_id: String,
    pub title: String,
    pub status: String,
    pub parent_thread_id: Option<String>,
    pub branch_off_anchor: Option<String>,
    pub current_step: String,
    pub next_command: String,
    pub created_at: DateTime<FixedOffset>,
    pub updated_at: DateTime<FixedOffset>,
    pub child_thread_ids: Vec<String>,
    pub checkpoints: Vec<TaskCheckpointInfo>,
    pub artifacts: Vec<TaskArtifactInfo>,
    pub active_claim: Option<TaskActiveClaimInfo>,
    pub claims: Vec<TaskClaimInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskGraphOverview {
    pub active_thread: Option<TaskThreadInfo>,
    pub active_path: Vec<TaskThreadInfo>,
    pub parked_threads: Vec<TaskThreadInfo>,
    pub all_threads: Vec<TaskThreadInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskGraphCommandResult {
    pub thread: TaskThreadInfo,
    pub overview: TaskGraphOverview,
    pub checkpoint: Option<TaskCheckpointInfo>,
    pub resumed_checkpoint: Option<TaskCheckpointInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskClaimCommandResult {
    pub thread: TaskThreadInfo,
    pub claim: Option<TaskClaimInfo>,
    pub overview: TaskGraphOverview,
}

pub fn start_thread(
    store: &mut AmsStore,
    title: &str,
    current_step: &str,
    next_command: &str,
    thread_id: Option<&str>,
    branch_off_anchor: Option<&str>,
    artifact_ref: Option<&str>,
    now_utc: DateTime<FixedOffset>,
) -> Result<TaskGraphCommandResult> {
    ensure_scaffold(store)?;
    let active = get_active_thread(store)?;
    let checkpoint = auto_checkpoint(store, active.as_ref(), now_utc)?;
    if let Some(active) = active.as_ref() {
        park_thread(store, active, now_utc)?;
    }

    let thread = resolve_or_create_thread(
        store,
        title,
        current_step,
        next_command,
        thread_id,
        branch_off_anchor,
        None,
        artifact_ref,
        now_utc,
    )?;
    activate_thread(store, &thread, now_utc)?;
    Ok(TaskGraphCommandResult {
        thread: get_thread(store, &thread.thread_id)?.ok_or_else(|| anyhow!("thread '{}' missing after start", thread.thread_id))?,
        overview: inspect_task_graph(store)?,
        checkpoint,
        resumed_checkpoint: None,
    })
}

pub fn push_tangent(
    store: &mut AmsStore,
    title: &str,
    current_step: &str,
    next_command: &str,
    thread_id: Option<&str>,
    branch_off_anchor: Option<&str>,
    artifact_ref: Option<&str>,
    now_utc: DateTime<FixedOffset>,
) -> Result<TaskGraphCommandResult> {
    ensure_scaffold(store)?;
    let active = get_active_thread(store)?.ok_or_else(|| anyhow!("thread-push-tangent requires an active thread"))?;
    let checkpoint = auto_checkpoint(store, Some(&active), now_utc)?;
    park_thread(store, &active, now_utc)?;

    let tangent = resolve_or_create_thread(
        store,
        title,
        current_step,
        next_command,
        thread_id,
        branch_off_anchor,
        Some(&active.thread_id),
        artifact_ref,
        now_utc,
    )?;
    link_child(store, &active, &tangent)?;
    activate_thread(store, &tangent, now_utc)?;

    Ok(TaskGraphCommandResult {
        thread: get_thread(store, &tangent.thread_id)?.ok_or_else(|| anyhow!("tangent '{}' missing after push", tangent.thread_id))?,
        overview: inspect_task_graph(store)?,
        checkpoint,
        resumed_checkpoint: None,
    })
}

pub fn checkpoint_active_thread(
    store: &mut AmsStore,
    current_step: &str,
    next_command: &str,
    branch_off_anchor: Option<&str>,
    artifact_ref: Option<&str>,
    now_utc: DateTime<FixedOffset>,
) -> Result<TaskGraphCommandResult> {
    ensure_scaffold(store)?;
    let active = get_active_thread(store)?.ok_or_else(|| anyhow!("thread-checkpoint requires an active thread"))?;
    update_thread(
        store,
        &active,
        &active.title,
        current_step,
        next_command,
        branch_off_anchor.or(active.branch_off_anchor.as_deref()),
        active.parent_thread_id.as_deref(),
        now_utc,
    )?;
    let checkpoint = create_checkpoint(
        store,
        &active.thread_id,
        current_step,
        next_command,
        branch_off_anchor.or(active.branch_off_anchor.as_deref()),
        artifact_ref,
        now_utc,
    )?;
    let refreshed = get_thread(store, &active.thread_id)?.ok_or_else(|| anyhow!("thread '{}' missing after checkpoint", active.thread_id))?;
    Ok(TaskGraphCommandResult {
        thread: refreshed,
        overview: inspect_task_graph(store)?,
        checkpoint: Some(checkpoint),
        resumed_checkpoint: None,
    })
}

pub fn pop_thread(store: &mut AmsStore, now_utc: DateTime<FixedOffset>) -> Result<TaskGraphCommandResult> {
    ensure_scaffold(store)?;
    let active = get_active_thread(store)?.ok_or_else(|| anyhow!("thread-pop requires an active tangent"))?;
    let parent_thread_id = active
        .parent_thread_id
        .clone()
        .ok_or_else(|| anyhow!("thread-pop requires the active thread to have a parent tangent root"))?;

    let checkpoint = auto_checkpoint(store, Some(&active), now_utc)?;
    park_thread(store, &active, now_utc)?;

    let parent = get_thread(store, &parent_thread_id)?
        .ok_or_else(|| anyhow!("parent thread '{}' was not found", parent_thread_id))?;
    activate_thread(store, &parent, now_utc)?;
    let resumed_checkpoint = parent.checkpoints.first().cloned();
    let refreshed = get_thread(store, &parent.thread_id)?.ok_or_else(|| anyhow!("thread '{}' missing after pop", parent.thread_id))?;
    Ok(TaskGraphCommandResult {
        thread: refreshed,
        overview: inspect_task_graph(store)?,
        checkpoint,
        resumed_checkpoint,
    })
}

pub fn archive_thread(
    store: &mut AmsStore,
    thread_id: Option<&str>,
    now_utc: DateTime<FixedOffset>,
) -> Result<TaskGraphCommandResult> {
    ensure_scaffold(store)?;
    let thread = if let Some(thread_id) = thread_id {
        get_thread(store, thread_id)?
    } else {
        get_active_thread(store)?
    }
    .ok_or_else(|| anyhow!("thread-archive requires either an existing thread id or an active thread"))?;

    remove_membership(store, TASK_GRAPH_ACTIVE_CONTAINER, &thread.thread_object_id)?;
    remove_membership(store, TASK_GRAPH_PARKED_CONTAINER, &thread.thread_object_id)?;
    update_thread_status(store, &thread.thread_id, "archived", now_utc)?;
    let refreshed = get_thread(store, &thread.thread_id)?.ok_or_else(|| anyhow!("thread '{}' missing after archive", thread.thread_id))?;
    Ok(TaskGraphCommandResult {
        thread: refreshed,
        overview: inspect_task_graph(store)?,
        checkpoint: None,
        resumed_checkpoint: None,
    })
}

pub fn claim_thread(
    store: &mut AmsStore,
    thread_id: Option<&str>,
    agent_id: &str,
    lease_seconds: i64,
    claim_token: Option<&str>,
    now_utc: DateTime<FixedOffset>,
) -> Result<TaskClaimCommandResult> {
    ensure_scaffold(store)?;
    let thread = resolve_claim_target(store, thread_id)?
        .ok_or_else(|| anyhow!("thread-claim requires either an existing thread id or an active thread"))?;
    let lease_seconds = normalize_lease_seconds(lease_seconds);
    if let Some(active_claim) = thread.active_claim.as_ref() {
        if active_claim.lease_until > now_utc && active_claim.agent_id != agent_id {
            bail!(
                "thread '{}' is already claimed by '{}' until {}",
                thread.thread_id,
                active_claim.agent_id,
                active_claim.lease_until.to_rfc3339()
            );
        }
        if active_claim.lease_until <= now_utc {
            expire_claim(store, &thread, active_claim, now_utc)?;
        } else if active_claim.agent_id == agent_id {
            let (thread, claim) =
                heartbeat_claim_internal(store, &thread, active_claim, lease_seconds, now_utc, "heartbeat")?;
            return Ok(TaskClaimCommandResult {
                thread,
                claim: Some(claim),
                overview: inspect_task_graph(store)?,
            });
        }
    }

    let refreshed = get_thread(store, &thread.thread_id)?
        .ok_or_else(|| anyhow!("thread '{}' missing before claim", thread.thread_id))?;
    let next_attempt = refreshed.claims.iter().map(|claim| claim.attempt).max().unwrap_or(0) + 1;
    let claim_token = claim_token
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let claim = record_claim_event(
        store,
        &refreshed,
        &claim_token,
        agent_id,
        "acquired",
        next_attempt,
        Some(now_utc + chrono::Duration::seconds(lease_seconds)),
        Some(now_utc),
        None,
        None,
        now_utc,
    )?;
    set_active_claim(
        store,
        &refreshed.thread_id,
        agent_id,
        &claim_token,
        next_attempt,
        claim.lease_until,
        Some(now_utc),
        now_utc,
    )?;
    let thread = get_thread(store, &refreshed.thread_id)?
        .ok_or_else(|| anyhow!("thread '{}' missing after claim", refreshed.thread_id))?;
    Ok(TaskClaimCommandResult {
        thread,
        claim: Some(claim),
        overview: inspect_task_graph(store)?,
    })
}

pub fn heartbeat_thread_claim(
    store: &mut AmsStore,
    thread_id: Option<&str>,
    agent_id: &str,
    claim_token: &str,
    lease_seconds: i64,
    now_utc: DateTime<FixedOffset>,
) -> Result<TaskClaimCommandResult> {
    ensure_scaffold(store)?;
    let thread = resolve_claim_target(store, thread_id)?
        .ok_or_else(|| anyhow!("thread-heartbeat requires either an existing thread id or an active thread"))?;
    let active_claim = thread
        .active_claim
        .as_ref()
        .ok_or_else(|| anyhow!("thread '{}' has no active claim", thread.thread_id))?;
    if active_claim.claim_token != claim_token {
        bail!("thread '{}' claim token mismatch", thread.thread_id);
    }
    if active_claim.agent_id != agent_id {
        bail!(
            "thread '{}' is claimed by '{}' not '{}'",
            thread.thread_id,
            active_claim.agent_id,
            agent_id
        );
    }
    if active_claim.lease_until <= now_utc {
        expire_claim(store, &thread, active_claim, now_utc)?;
        bail!("thread '{}' claim '{}' has expired", thread.thread_id, claim_token);
    }
    let (thread, claim) =
        heartbeat_claim_internal(store, &thread, active_claim, normalize_lease_seconds(lease_seconds), now_utc, "heartbeat")?;
    Ok(TaskClaimCommandResult {
        thread,
        claim: Some(claim),
        overview: inspect_task_graph(store)?,
    })
}

pub fn release_thread_claim(
    store: &mut AmsStore,
    thread_id: Option<&str>,
    agent_id: &str,
    claim_token: &str,
    release_reason: Option<&str>,
    now_utc: DateTime<FixedOffset>,
) -> Result<TaskClaimCommandResult> {
    ensure_scaffold(store)?;
    let thread = resolve_claim_target(store, thread_id)?
        .ok_or_else(|| anyhow!("thread-release requires either an existing thread id or an active thread"))?;
    let active_claim = thread
        .active_claim
        .as_ref()
        .ok_or_else(|| anyhow!("thread '{}' has no active claim", thread.thread_id))?;
    if active_claim.claim_token != claim_token {
        bail!("thread '{}' claim token mismatch", thread.thread_id);
    }
    if active_claim.agent_id != agent_id {
        bail!(
            "thread '{}' is claimed by '{}' not '{}'",
            thread.thread_id,
            active_claim.agent_id,
            agent_id
        );
    }
    let claim = record_claim_event(
        store,
        &thread,
        claim_token,
        agent_id,
        "released",
        active_claim.attempt,
        Some(active_claim.lease_until),
        Some(active_claim.heartbeat_at),
        Some(now_utc),
        release_reason,
        now_utc,
    )?;
    clear_active_claim(store, &thread.thread_id, now_utc)?;
    let thread = get_thread(store, &thread.thread_id)?
        .ok_or_else(|| anyhow!("thread '{}' missing after release", thread.thread_id))?;
    Ok(TaskClaimCommandResult {
        thread,
        claim: Some(claim),
        overview: inspect_task_graph(store)?,
    })
}

pub fn inspect_task_graph(store: &AmsStore) -> Result<TaskGraphOverview> {
    let threads = load_threads(store)?
        .into_iter()
        .collect::<Vec<_>>();
    let active_thread = get_active_thread(store)?;

    let by_id = threads
        .iter()
        .cloned()
        .map(|thread| (thread.thread_id.clone(), thread))
        .collect::<BTreeMap<_, _>>();

    let mut active_path = Vec::new();
    if let Some(active) = active_thread.clone() {
        let mut cursor = Some(active);
        let mut seen = std::collections::BTreeSet::new();
        while let Some(thread) = cursor {
            if !seen.insert(thread.thread_id.clone()) {
                break;
            }
            let parent_id = thread.parent_thread_id.clone();
            active_path.push(thread);
            cursor = parent_id.and_then(|parent_id| by_id.get(&parent_id).cloned());
        }
        active_path.reverse();
    }

    let mut parked_threads = threads
        .iter()
        .filter(|thread| thread.status.eq_ignore_ascii_case("parked"))
        .cloned()
        .collect::<Vec<_>>();
    parked_threads.sort_by(|left, right| right.updated_at.cmp(&left.updated_at).then_with(|| left.thread_id.cmp(&right.thread_id)));

    let mut all_threads = threads;
    all_threads.sort_by(|left, right| left.title.to_ascii_lowercase().cmp(&right.title.to_ascii_lowercase()).then_with(|| left.thread_id.cmp(&right.thread_id)));

    Ok(TaskGraphOverview {
        active_thread,
        active_path,
        parked_threads,
        all_threads,
    })
}

pub fn thread_list(snapshot: &AmsStore) -> Result<String> {
    let overview = inspect_task_graph(snapshot)?;
    let mut out = String::from("# TASK THREADS\n");
    let mut threads = overview.all_threads;
    threads.sort_by(|left, right| {
        right
            .status
            .eq_ignore_ascii_case("active")
            .cmp(&left.status.eq_ignore_ascii_case("active"))
            .then_with(|| right.updated_at.cmp(&left.updated_at))
    });
    for thread in threads {
        let marker = if thread.status.eq_ignore_ascii_case("active") { "*" } else { "-" };
        let parent = thread
            .parent_thread_id
            .as_ref()
            .map(|value| format!(" parent={value}"))
            .unwrap_or_default();
        out.push_str(&format!(
            "{marker} {} {} {}{parent}\n",
            thread.status.to_ascii_uppercase(),
            thread.thread_id,
            thread.title
        ));
        out.push_str(&format!("  step: {}\n", thread.current_step));
        out.push_str(&format!("  next: {}\n", thread.next_command));
        out.push_str(&format!(
            "  checkpoints={} children={} artifacts={} claims={}\n",
            thread.checkpoints.len(),
            thread.child_thread_ids.len(),
            thread.artifacts.len(),
            thread.claims.len()
        ));
        if let Some(claim) = thread.active_claim.as_ref() {
            out.push_str(&format!(
                "  claim={} token={} attempt={} lease_until={}\n",
                claim.agent_id,
                claim.claim_token,
                claim.attempt,
                claim.lease_until.to_rfc3339()
            ));
        }
    }
    Ok(out)
}

fn ensure_scaffold(store: &mut AmsStore) -> Result<()> {
    ensure_container(store, TASK_GRAPH_ROOT_CONTAINER, "task_graph")?;
    ensure_container(store, TASK_GRAPH_ACTIVE_CONTAINER, "task_graph_bucket")?;
    ensure_container(store, TASK_GRAPH_PARKED_CONTAINER, "task_graph_bucket")?;
    replace_members(store, TASK_GRAPH_ROOT_CONTAINER, &[TASK_GRAPH_ACTIVE_CONTAINER, TASK_GRAPH_PARKED_CONTAINER])?;
    Ok(())
}

fn resolve_or_create_thread(
    store: &mut AmsStore,
    title: &str,
    current_step: &str,
    next_command: &str,
    thread_id: Option<&str>,
    branch_off_anchor: Option<&str>,
    parent_thread_id: Option<&str>,
    artifact_ref: Option<&str>,
    now_utc: DateTime<FixedOffset>,
) -> Result<TaskThreadInfo> {
    let thread_id = empty_to_none(thread_id.map(str::to_string)).unwrap_or_else(|| build_thread_id(title));
    if let Some(existing) = get_thread(store, &thread_id)? {
        update_thread(
            store,
            &existing,
            if title.trim().is_empty() { &existing.title } else { title },
            current_step,
            next_command,
            branch_off_anchor.or(existing.branch_off_anchor.as_deref()),
            parent_thread_id.or(existing.parent_thread_id.as_deref()),
            now_utc,
        )?;
        if let Some(artifact_ref) = artifact_ref.filter(|value| !value.trim().is_empty()) {
            add_artifact(store, &thread_id, artifact_ref, now_utc)?;
        }
        return get_thread(store, &thread_id)?.ok_or_else(|| anyhow!("thread '{}' missing after update", thread_id));
    }

    let object_id = thread_object_id(&thread_id);
    store
        .upsert_object(object_id.clone(), "task_thread", None, None, Some(now_utc))
        .map_err(to_anyhow)?;
    let thread = store
        .objects_mut()
        .get_mut(&object_id)
        .ok_or_else(|| anyhow!("failed to materialize thread '{}'", object_id))?;
    thread.semantic_payload.get_or_insert_with(SemanticPayload::default).summary = Some(title.to_string());
    thread.semantic_payload.as_mut().expect("semantic payload initialized").tags = Some(vec![
        "task_thread".to_string(),
        if parent_thread_id.is_some() {
            "tangent".to_string()
        } else {
            "root".to_string()
        },
    ]);
    thread.created_at = now_utc;
    thread.updated_at = now_utc;
    let prov = ensure_prov(thread);
    prov.insert("thread_id".to_string(), Value::String(thread_id.clone()));
    prov.insert("status".to_string(), Value::String("parked".to_string()));
    prov.insert(
        "parent_thread_id".to_string(),
        Value::String(parent_thread_id.unwrap_or_default().to_string()),
    );
    prov.insert(
        "branch_off_anchor".to_string(),
        Value::String(branch_off_anchor.unwrap_or_default().to_string()),
    );
    prov.insert("current_step".to_string(), Value::String(current_step.to_string()));
    prov.insert("next_command".to_string(), Value::String(next_command.to_string()));
    prov.insert("created_at".to_string(), Value::String(now_utc.to_rfc3339()));
    prov.insert("updated_at".to_string(), Value::String(now_utc.to_rfc3339()));
    prov.insert(
        "children_container_id".to_string(),
        Value::String(children_container_id(&thread_id)),
    );
    prov.insert(
        "checkpoints_container_id".to_string(),
        Value::String(checkpoints_container_id(&thread_id)),
    );
    prov.insert(
        "artifacts_container_id".to_string(),
        Value::String(artifacts_container_id(&thread_id)),
    );
    prov.insert(
        "claims_container_id".to_string(),
        Value::String(claims_container_id(&thread_id)),
    );

    ensure_container(store, &children_container_id(&thread_id), "task_thread_children")?;
    ensure_container(store, &checkpoints_container_id(&thread_id), "task_thread_checkpoints")?;
    ensure_container(store, &artifacts_container_id(&thread_id), "task_thread_artifacts")?;
    ensure_container(store, &claims_container_id(&thread_id), "task_thread_claims")?;

    if let Some(artifact_ref) = artifact_ref.filter(|value| !value.trim().is_empty()) {
        add_artifact(store, &thread_id, artifact_ref, now_utc)?;
    }
    get_thread(store, &thread_id)?.ok_or_else(|| anyhow!("thread '{}' missing after create", thread_id))
}

fn auto_checkpoint(
    store: &mut AmsStore,
    thread: Option<&TaskThreadInfo>,
    now_utc: DateTime<FixedOffset>,
) -> Result<Option<TaskCheckpointInfo>> {
    match thread {
        Some(thread) => Ok(Some(create_checkpoint(
            store,
            &thread.thread_id,
            &thread.current_step,
            &thread.next_command,
            thread.branch_off_anchor.as_deref(),
            None,
            now_utc,
        )?)),
        None => Ok(None),
    }
}

fn create_checkpoint(
    store: &mut AmsStore,
    thread_id: &str,
    current_step: &str,
    next_command: &str,
    branch_off_anchor: Option<&str>,
    artifact_ref: Option<&str>,
    now_utc: DateTime<FixedOffset>,
) -> Result<TaskCheckpointInfo> {
    let object_id = format!("task-checkpoint:{thread_id}:{}:{}", now_utc.format("%Y%m%d%H%M%S%3f"), uuid::Uuid::new_v4().simple());
    store
        .upsert_object(object_id.clone(), "task_checkpoint", None, None, Some(now_utc))
        .map_err(to_anyhow)?;
    let checkpoint = store
        .objects_mut()
        .get_mut(&object_id)
        .ok_or_else(|| anyhow!("failed to materialize checkpoint '{}'", object_id))?;
    checkpoint.semantic_payload.get_or_insert_with(SemanticPayload::default).summary = Some(current_step.to_string());
    checkpoint.updated_at = now_utc;
    checkpoint.created_at = now_utc;
    let prov = ensure_prov(checkpoint);
    prov.insert("thread_id".to_string(), Value::String(thread_id.to_string()));
    prov.insert("current_step".to_string(), Value::String(current_step.to_string()));
    prov.insert("next_command".to_string(), Value::String(next_command.to_string()));
    prov.insert(
        "branch_off_anchor".to_string(),
        Value::String(branch_off_anchor.unwrap_or_default().to_string()),
    );
    prov.insert(
        "artifact_ref".to_string(),
        Value::String(artifact_ref.unwrap_or_default().to_string()),
    );
    prov.insert("created_at".to_string(), Value::String(now_utc.to_rfc3339()));
    ensure_attached(store, &checkpoints_container_id(thread_id), &object_id)?;
    if let Some(artifact_ref) = artifact_ref.filter(|value| !value.trim().is_empty()) {
        add_artifact(store, thread_id, artifact_ref, now_utc)?;
    }
    parse_checkpoint(store, &object_id).ok_or_else(|| anyhow!("checkpoint '{}' missing after create", object_id))
}

fn add_artifact(
    store: &mut AmsStore,
    thread_id: &str,
    artifact_ref: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<TaskArtifactInfo> {
    let object_id = format!(
        "task-artifact:{thread_id}:{}:{}",
        hash8(artifact_ref),
        uuid::Uuid::new_v4().simple()
    );
    store
        .upsert_object(object_id.clone(), "task_artifact", None, None, Some(now_utc))
        .map_err(to_anyhow)?;
    let artifact = store
        .objects_mut()
        .get_mut(&object_id)
        .ok_or_else(|| anyhow!("failed to materialize artifact '{}'", object_id))?;
    artifact.semantic_payload.get_or_insert_with(SemanticPayload::default).summary =
        Some(build_artifact_label(artifact_ref));
    artifact.created_at = now_utc;
    artifact.updated_at = now_utc;
    let prov = ensure_prov(artifact);
    prov.insert("thread_id".to_string(), Value::String(thread_id.to_string()));
    prov.insert("artifact_ref".to_string(), Value::String(artifact_ref.to_string()));
    prov.insert("created_at".to_string(), Value::String(now_utc.to_rfc3339()));
    ensure_attached(store, &artifacts_container_id(thread_id), &object_id)?;
    parse_artifact(store, &object_id).ok_or_else(|| anyhow!("artifact '{}' missing after create", object_id))
}

fn activate_thread(store: &mut AmsStore, thread: &TaskThreadInfo, now_utc: DateTime<FixedOffset>) -> Result<()> {
    replace_members(store, TASK_GRAPH_ACTIVE_CONTAINER, &[thread.thread_object_id.as_str()])?;
    remove_membership(store, TASK_GRAPH_PARKED_CONTAINER, &thread.thread_object_id)?;
    update_thread_status(store, &thread.thread_id, "active", now_utc)
}

fn park_thread(store: &mut AmsStore, thread: &TaskThreadInfo, now_utc: DateTime<FixedOffset>) -> Result<()> {
    remove_membership(store, TASK_GRAPH_ACTIVE_CONTAINER, &thread.thread_object_id)?;
    ensure_attached(store, TASK_GRAPH_PARKED_CONTAINER, &thread.thread_object_id)?;
    update_thread_status(store, &thread.thread_id, "parked", now_utc)
}

fn link_child(store: &mut AmsStore, parent: &TaskThreadInfo, child: &TaskThreadInfo) -> Result<()> {
    ensure_attached(store, &children_container_id(&parent.thread_id), &child.thread_object_id)
}

fn get_active_thread(store: &AmsStore) -> Result<Option<TaskThreadInfo>> {
    if !store.containers().contains_key(TASK_GRAPH_ACTIVE_CONTAINER) {
        return Ok(None);
    }
    let active = store
        .iterate_forward(TASK_GRAPH_ACTIVE_CONTAINER)
        .first()
        .map(|link| link.object_id.clone());
    match active {
        Some(object_id) => Ok(Some(parse_thread_object(store, &object_id)?)),
        None => Ok(None),
    }
}

fn get_thread(store: &AmsStore, thread_id: &str) -> Result<Option<TaskThreadInfo>> {
    parse_thread_object(store, &thread_object_id(thread_id)).map(Some).or_else(|error| {
        if error.to_string().contains("not found") {
            Ok(None)
        } else {
            Err(error)
        }
    })
}

fn load_threads(store: &AmsStore) -> Result<Vec<TaskThreadInfo>> {
    store
        .objects()
        .values()
        .filter(|object| object.object_kind == "task_thread")
        .map(|object| parse_thread_object(store, &object.object_id))
        .collect()
}

fn parse_thread_object(store: &AmsStore, object_id: &str) -> Result<TaskThreadInfo> {
    let obj = store
        .objects()
        .get(object_id)
        .ok_or_else(|| anyhow!("thread object '{}' not found", object_id))?;
    if obj.object_kind != "task_thread" {
        bail!("object '{}' is not a task_thread", object_id);
    }
    let prov = obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
    let thread_id = read_string(prov, "thread_id").unwrap_or_else(|| suffix(object_id));
    let checkpoints = if store.containers().contains_key(&checkpoints_container_id(&thread_id)) {
        let mut values = store
            .iterate_forward(&checkpoints_container_id(&thread_id))
            .iter()
            .filter_map(|link| parse_checkpoint(store, &link.object_id))
            .collect::<Vec<_>>();
        values.sort_by(|left, right| right.created_at.cmp(&left.created_at).then_with(|| right.checkpoint_object_id.cmp(&left.checkpoint_object_id)));
        values
    } else {
        Vec::new()
    };
    let artifacts = if store.containers().contains_key(&artifacts_container_id(&thread_id)) {
        let mut values = store
            .iterate_forward(&artifacts_container_id(&thread_id))
            .iter()
            .filter_map(|link| parse_artifact(store, &link.object_id))
            .collect::<Vec<_>>();
        values.sort_by(|left, right| right.created_at.cmp(&left.created_at).then_with(|| right.artifact_object_id.cmp(&left.artifact_object_id)));
        values
    } else {
        Vec::new()
    };
    let mut child_thread_ids = if store.containers().contains_key(&children_container_id(&thread_id)) {
        store
            .iterate_forward(&children_container_id(&thread_id))
            .iter()
            .map(|link| suffix(&link.object_id))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    child_thread_ids.sort();
    let claims = if store.containers().contains_key(&claims_container_id(&thread_id)) {
        let mut values = store
            .iterate_forward(&claims_container_id(&thread_id))
            .iter()
            .filter_map(|link| parse_claim(store, &link.object_id))
            .collect::<Vec<_>>();
        values.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.claim_object_id.cmp(&left.claim_object_id))
        });
        values
    } else {
        Vec::new()
    };
    let active_claim = parse_active_claim(prov);

    Ok(TaskThreadInfo {
        thread_object_id: object_id.to_string(),
        thread_id,
        title: obj
            .semantic_payload
            .as_ref()
            .and_then(|payload| payload.summary.clone())
            .unwrap_or_else(|| object_id.to_string()),
        status: read_string(prov, "status").unwrap_or_else(|| "parked".to_string()),
        parent_thread_id: empty_to_none(read_string(prov, "parent_thread_id")),
        branch_off_anchor: empty_to_none(read_string(prov, "branch_off_anchor")),
        current_step: read_string(prov, "current_step").unwrap_or_default(),
        next_command: read_string(prov, "next_command").unwrap_or_default(),
        created_at: read_date(prov, "created_at").unwrap_or(obj.created_at),
        updated_at: read_date(prov, "updated_at").unwrap_or(obj.updated_at),
        child_thread_ids,
        checkpoints,
        artifacts,
        active_claim,
        claims,
    })
}

fn parse_checkpoint(store: &AmsStore, object_id: &str) -> Option<TaskCheckpointInfo> {
    let obj = store.objects().get(object_id)?;
    if obj.object_kind != "task_checkpoint" {
        return None;
    }
    let prov = obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
    let current_step = read_string(prov, "current_step")
        .or_else(|| obj.semantic_payload.as_ref().and_then(|payload| payload.summary.clone()))
        .unwrap_or_else(|| object_id.to_string());
    Some(TaskCheckpointInfo {
        checkpoint_object_id: object_id.to_string(),
        summary: obj
            .semantic_payload
            .as_ref()
            .and_then(|payload| payload.summary.clone())
            .unwrap_or_else(|| current_step.clone()),
        current_step,
        next_command: read_string(prov, "next_command").unwrap_or_default(),
        branch_off_anchor: empty_to_none(read_string(prov, "branch_off_anchor")),
        artifact_ref: empty_to_none(read_string(prov, "artifact_ref")),
        created_at: read_date(prov, "created_at").unwrap_or(obj.created_at),
    })
}

fn parse_artifact(store: &AmsStore, object_id: &str) -> Option<TaskArtifactInfo> {
    let obj = store.objects().get(object_id)?;
    if obj.object_kind != "task_artifact" {
        return None;
    }
    let prov = obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
    Some(TaskArtifactInfo {
        artifact_object_id: object_id.to_string(),
        label: obj
            .semantic_payload
            .as_ref()
            .and_then(|payload| payload.summary.clone())
            .unwrap_or_else(|| object_id.to_string()),
        artifact_ref: read_string(prov, "artifact_ref").unwrap_or_default(),
        created_at: read_date(prov, "created_at").unwrap_or(obj.created_at),
    })
}

fn parse_active_claim(prov: Option<&JsonMap>) -> Option<TaskActiveClaimInfo> {
    Some(TaskActiveClaimInfo {
        agent_id: read_string(prov, "claim_agent_id")?,
        claim_token: read_string(prov, "claim_token")?,
        attempt: read_u64(prov, "claim_attempt")?,
        lease_until: read_date(prov, "claim_lease_until")?,
        heartbeat_at: read_date(prov, "claim_heartbeat_at")?,
    })
}

fn parse_claim(store: &AmsStore, object_id: &str) -> Option<TaskClaimInfo> {
    let obj = store.objects().get(object_id)?;
    if obj.object_kind != "task_claim" {
        return None;
    }
    let prov = obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
    Some(TaskClaimInfo {
        claim_object_id: object_id.to_string(),
        agent_id: read_string(prov, "agent_id")?,
        claim_token: read_string(prov, "claim_token")?,
        status: read_string(prov, "status").unwrap_or_else(|| "acquired".to_string()),
        attempt: read_u64(prov, "attempt").unwrap_or(1),
        lease_until: read_date(prov, "lease_until"),
        heartbeat_at: read_date(prov, "heartbeat_at"),
        created_at: read_date(prov, "created_at").unwrap_or(obj.created_at),
        released_at: read_date(prov, "released_at"),
        release_reason: empty_to_none(read_string(prov, "release_reason")),
    })
}

fn update_thread(
    store: &mut AmsStore,
    thread: &TaskThreadInfo,
    title: &str,
    current_step: &str,
    next_command: &str,
    branch_off_anchor: Option<&str>,
    parent_thread_id: Option<&str>,
    now_utc: DateTime<FixedOffset>,
) -> Result<()> {
    let obj = store
        .objects_mut()
        .get_mut(&thread.thread_object_id)
        .ok_or_else(|| anyhow!("thread '{}' was not found", thread.thread_id))?;
    obj.semantic_payload.get_or_insert_with(SemanticPayload::default).summary = Some(title.to_string());
    obj.updated_at = now_utc;
    let prov = ensure_prov(obj);
    prov.insert("current_step".to_string(), Value::String(current_step.to_string()));
    prov.insert("next_command".to_string(), Value::String(next_command.to_string()));
    prov.insert(
        "branch_off_anchor".to_string(),
        Value::String(branch_off_anchor.unwrap_or_default().to_string()),
    );
    prov.insert(
        "parent_thread_id".to_string(),
        Value::String(parent_thread_id.unwrap_or_default().to_string()),
    );
    prov.insert("updated_at".to_string(), Value::String(now_utc.to_rfc3339()));
    Ok(())
}

fn update_thread_status(store: &mut AmsStore, thread_id: &str, status: &str, now_utc: DateTime<FixedOffset>) -> Result<()> {
    let object_id = thread_object_id(thread_id);
    let obj = store
        .objects_mut()
        .get_mut(&object_id)
        .ok_or_else(|| anyhow!("thread '{}' was not found", thread_id))?;
    let prov = ensure_prov(obj);
    prov.insert("status".to_string(), Value::String(status.to_string()));
    prov.insert("updated_at".to_string(), Value::String(now_utc.to_rfc3339()));
    obj.updated_at = now_utc;
    Ok(())
}

fn ensure_container(store: &mut AmsStore, id: &str, kind: &str) -> Result<()> {
    if !store.containers().contains_key(id) {
        store
            .create_container(id.to_string(), "container", kind.to_string())
            .map_err(to_anyhow)?;
    }
    if let Some(container) = store.containers_mut().get_mut(id) {
        container.policies.unique_members = true;
    }
    Ok(())
}

fn replace_members(store: &mut AmsStore, container_id: &str, member_ids: &[&str]) -> Result<()> {
    let existing = store
        .iterate_forward(container_id)
        .iter()
        .map(|link| link.link_node_id.clone())
        .collect::<Vec<_>>();
    for link_node_id in existing {
        store.remove_linknode(container_id, &link_node_id).map_err(to_anyhow)?;
    }
    let mut seen = std::collections::BTreeSet::new();
    for member_id in member_ids {
        if seen.insert((*member_id).to_string())
            && (store.objects().contains_key(*member_id) || store.containers().contains_key(*member_id))
        {
            ensure_attached(store, container_id, member_id)?;
        }
    }
    Ok(())
}

fn ensure_attached(store: &mut AmsStore, container_id: &str, object_id: &str) -> Result<()> {
    if !store.has_membership(container_id, object_id) {
        store.add_object(container_id, object_id, None, None).map_err(to_anyhow)?;
    }
    Ok(())
}

fn remove_membership(store: &mut AmsStore, container_id: &str, member_id: &str) -> Result<()> {
    let link_node_id = store
        .links_for_member_object(member_id)
        .into_iter()
        .find(|link| link.container_id == container_id)
        .map(|link| link.link_node_id.clone());
    if let Some(link_node_id) = link_node_id {
        store.remove_linknode(container_id, &link_node_id).map_err(to_anyhow)?;
    }
    Ok(())
}

fn thread_object_id(thread_id: &str) -> String {
    format!("task-thread:{thread_id}")
}

fn children_container_id(thread_id: &str) -> String {
    format!("task-thread:{thread_id}:children")
}

fn checkpoints_container_id(thread_id: &str) -> String {
    format!("task-thread:{thread_id}:checkpoints")
}

fn artifacts_container_id(thread_id: &str) -> String {
    format!("task-thread:{thread_id}:artifacts")
}

fn claims_container_id(thread_id: &str) -> String {
    format!("task-thread:{thread_id}:claims")
}

fn resolve_claim_target(store: &AmsStore, thread_id: Option<&str>) -> Result<Option<TaskThreadInfo>> {
    match thread_id {
        Some(thread_id) => get_thread(store, thread_id),
        None => get_active_thread(store),
    }
}

fn normalize_lease_seconds(lease_seconds: i64) -> i64 {
    lease_seconds.max(1)
}

fn set_active_claim(
    store: &mut AmsStore,
    thread_id: &str,
    agent_id: &str,
    claim_token: &str,
    attempt: u64,
    lease_until: Option<DateTime<FixedOffset>>,
    heartbeat_at: Option<DateTime<FixedOffset>>,
    now_utc: DateTime<FixedOffset>,
) -> Result<()> {
    let object_id = thread_object_id(thread_id);
    let obj = store
        .objects_mut()
        .get_mut(&object_id)
        .ok_or_else(|| anyhow!("thread '{}' was not found", thread_id))?;
    let prov = ensure_prov(obj);
    prov.insert("claim_agent_id".to_string(), Value::String(agent_id.to_string()));
    prov.insert("claim_token".to_string(), Value::String(claim_token.to_string()));
    prov.insert("claim_attempt".to_string(), Value::Number(attempt.into()));
    prov.insert(
        "claim_lease_until".to_string(),
        Value::String(lease_until.unwrap_or(now_utc).to_rfc3339()),
    );
    prov.insert(
        "claim_heartbeat_at".to_string(),
        Value::String(heartbeat_at.unwrap_or(now_utc).to_rfc3339()),
    );
    prov.insert("updated_at".to_string(), Value::String(now_utc.to_rfc3339()));
    obj.updated_at = now_utc;
    Ok(())
}

fn clear_active_claim(store: &mut AmsStore, thread_id: &str, now_utc: DateTime<FixedOffset>) -> Result<()> {
    let object_id = thread_object_id(thread_id);
    let obj = store
        .objects_mut()
        .get_mut(&object_id)
        .ok_or_else(|| anyhow!("thread '{}' was not found", thread_id))?;
    let prov = ensure_prov(obj);
    for key in [
        "claim_agent_id",
        "claim_token",
        "claim_attempt",
        "claim_lease_until",
        "claim_heartbeat_at",
    ] {
        prov.remove(key);
    }
    prov.insert("updated_at".to_string(), Value::String(now_utc.to_rfc3339()));
    obj.updated_at = now_utc;
    Ok(())
}

fn expire_claim(
    store: &mut AmsStore,
    thread: &TaskThreadInfo,
    active_claim: &TaskActiveClaimInfo,
    now_utc: DateTime<FixedOffset>,
) -> Result<()> {
    record_claim_event(
        store,
        thread,
        &active_claim.claim_token,
        &active_claim.agent_id,
        "expired",
        active_claim.attempt,
        Some(active_claim.lease_until),
        Some(active_claim.heartbeat_at),
        Some(now_utc),
        Some("lease-expired"),
        now_utc,
    )?;
    clear_active_claim(store, &thread.thread_id, now_utc)
}

fn heartbeat_claim_internal(
    store: &mut AmsStore,
    thread: &TaskThreadInfo,
    active_claim: &TaskActiveClaimInfo,
    lease_seconds: i64,
    now_utc: DateTime<FixedOffset>,
    status: &str,
) -> Result<(TaskThreadInfo, TaskClaimInfo)> {
    let lease_until = now_utc + chrono::Duration::seconds(lease_seconds);
    let claim = record_claim_event(
        store,
        thread,
        &active_claim.claim_token,
        &active_claim.agent_id,
        status,
        active_claim.attempt,
        Some(lease_until),
        Some(now_utc),
        None,
        None,
        now_utc,
    )?;
    set_active_claim(
        store,
        &thread.thread_id,
        &active_claim.agent_id,
        &active_claim.claim_token,
        active_claim.attempt,
        Some(lease_until),
        Some(now_utc),
        now_utc,
    )?;
    let refreshed = get_thread(store, &thread.thread_id)?
        .ok_or_else(|| anyhow!("thread '{}' missing after heartbeat", thread.thread_id))?;
    Ok((refreshed, claim))
}

fn record_claim_event(
    store: &mut AmsStore,
    thread: &TaskThreadInfo,
    claim_token: &str,
    agent_id: &str,
    status: &str,
    attempt: u64,
    lease_until: Option<DateTime<FixedOffset>>,
    heartbeat_at: Option<DateTime<FixedOffset>>,
    released_at: Option<DateTime<FixedOffset>>,
    release_reason: Option<&str>,
    now_utc: DateTime<FixedOffset>,
) -> Result<TaskClaimInfo> {
    let object_id = format!(
        "task-claim:{}:{}:{}:{}",
        thread.thread_id,
        attempt,
        status,
        uuid::Uuid::new_v4().simple()
    );
    store
        .upsert_object(object_id.clone(), "task_claim", None, None, Some(now_utc))
        .map_err(to_anyhow)?;
    let claim = store
        .objects_mut()
        .get_mut(&object_id)
        .ok_or_else(|| anyhow!("failed to materialize claim '{}'", object_id))?;
    claim.semantic_payload.get_or_insert_with(SemanticPayload::default).summary =
        Some(format!("{status} {agent_id}"));
    claim.created_at = now_utc;
    claim.updated_at = now_utc;
    let prov = ensure_prov(claim);
    prov.insert("thread_id".to_string(), Value::String(thread.thread_id.clone()));
    prov.insert("agent_id".to_string(), Value::String(agent_id.to_string()));
    prov.insert("claim_token".to_string(), Value::String(claim_token.to_string()));
    prov.insert("status".to_string(), Value::String(status.to_string()));
    prov.insert("attempt".to_string(), Value::Number(attempt.into()));
    if let Some(lease_until) = lease_until {
        prov.insert("lease_until".to_string(), Value::String(lease_until.to_rfc3339()));
    }
    if let Some(heartbeat_at) = heartbeat_at {
        prov.insert("heartbeat_at".to_string(), Value::String(heartbeat_at.to_rfc3339()));
    }
    if let Some(released_at) = released_at {
        prov.insert("released_at".to_string(), Value::String(released_at.to_rfc3339()));
    }
    if let Some(release_reason) = release_reason.filter(|value| !value.trim().is_empty()) {
        prov.insert("release_reason".to_string(), Value::String(release_reason.to_string()));
    }
    prov.insert("created_at".to_string(), Value::String(now_utc.to_rfc3339()));
    ensure_attached(store, &claims_container_id(&thread.thread_id), &object_id)?;
    parse_claim(store, &object_id).ok_or_else(|| anyhow!("claim '{}' missing after create", object_id))
}

fn suffix(id: &str) -> String {
    id.rsplit(':').next().unwrap_or(id).to_string()
}

fn empty_to_none(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn build_thread_id(title: &str) -> String {
    let mut normalized = title.trim().to_ascii_lowercase();
    normalized = normalized
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect::<String>();
    while normalized.contains("--") {
        normalized = normalized.replace("--", "-");
    }
    normalized = normalized.trim_matches('-').to_string();
    if normalized.is_empty() {
        normalized = "thread".to_string();
    }
    if normalized.len() > 40 {
        normalized.truncate(40);
        normalized = normalized.trim_matches('-').to_string();
    }
    format!("{normalized}-{}", hash8(title))
}

fn build_artifact_label(artifact_ref: &str) -> String {
    let trimmed = artifact_ref.trim();
    if trimmed.is_empty() {
        return "Artifact".to_string();
    }
    std::path::Path::new(trimmed)
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .unwrap_or_else(|| trimmed.to_string())
}

fn hash8(text: &str) -> String {
    let mut hash: u32 = 0x811c9dc5;
    for byte in text.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    format!("{hash:08x}")
}

fn ensure_prov(object: &mut ObjectRecord) -> &mut JsonMap {
    object
        .semantic_payload
        .get_or_insert_with(SemanticPayload::default)
        .provenance
        .get_or_insert_with(BTreeMap::new)
}

fn read_string(map: Option<&JsonMap>, key: &str) -> Option<String> {
    match map?.get(key)? {
        Value::String(value) => Some(value.clone()),
        value => Some(value.to_string()),
    }
}

fn read_date(map: Option<&JsonMap>, key: &str) -> Option<DateTime<FixedOffset>> {
    let Value::String(raw) = map?.get(key)? else {
        return None;
    };
    DateTime::parse_from_rfc3339(raw).ok()
}

fn read_u64(map: Option<&JsonMap>, key: &str) -> Option<u64> {
    match map?.get(key)? {
        Value::Number(value) => value.as_u64(),
        Value::String(value) => value.parse::<u64>().ok(),
        _ => None,
    }
}

fn to_anyhow(error: StoreError) -> anyhow::Error {
    anyhow!(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_push_checkpoint_pop_archive_thread_flow() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-15T12:00:00+00:00").unwrap();

        let started = start_thread(
            &mut store,
            "Root thread",
            "Review",
            "read docs",
            Some("root"),
            None,
            Some("docs/architecture.md"),
            now,
        )
        .unwrap();
        assert_eq!(started.thread.thread_id, "root");
        assert_eq!(inspect_task_graph(&store).unwrap().active_thread.unwrap().thread_id, "root");

        let tangent = push_tangent(
            &mut store,
            "Child tangent",
            "Implement",
            "cargo test",
            Some("child"),
            Some("smartlist/execution-plan"),
            None,
            now,
        )
        .unwrap();
        assert_eq!(tangent.thread.parent_thread_id.as_deref(), Some("root"));

        let checkpointed = checkpoint_active_thread(&mut store, "Implement", "cargo test", None, Some("src/lib.rs"), now).unwrap();
        assert!(checkpointed.checkpoint.is_some());

        let popped = pop_thread(&mut store, now).unwrap();
        assert_eq!(popped.thread.thread_id, "root");

        let archived = archive_thread(&mut store, Some("child"), now).unwrap();
        assert_eq!(archived.thread.status, "archived");
    }

    #[test]
    fn claim_heartbeat_release_flow_is_recorded() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-15T12:00:00+00:00").unwrap();
        start_thread(&mut store, "Root", "Review", "read", Some("root"), None, None, now).unwrap();

        let claimed = claim_thread(&mut store, Some("root"), "agent-a", 60, Some("claim-1"), now).unwrap();
        assert_eq!(claimed.thread.active_claim.as_ref().unwrap().agent_id, "agent-a");
        assert_eq!(claimed.claim.as_ref().unwrap().status, "acquired");

        let heartbeat = heartbeat_thread_claim(
            &mut store,
            Some("root"),
            "agent-a",
            "claim-1",
            120,
            now + chrono::Duration::seconds(30),
        )
        .unwrap();
        assert_eq!(heartbeat.claim.as_ref().unwrap().status, "heartbeat");
        assert!(heartbeat.thread.active_claim.as_ref().unwrap().lease_until > now);

        let released = release_thread_claim(
            &mut store,
            Some("root"),
            "agent-a",
            "claim-1",
            Some("complete"),
            now + chrono::Duration::seconds(40),
        )
        .unwrap();
        assert!(released.thread.active_claim.is_none());
        assert_eq!(released.claim.as_ref().unwrap().status, "released");
        assert_eq!(released.thread.claims.len(), 3);
    }

    #[test]
    fn expired_claim_can_be_reclaimed() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-15T12:00:00+00:00").unwrap();
        start_thread(&mut store, "Root", "Review", "read", Some("root"), None, None, now).unwrap();
        claim_thread(&mut store, Some("root"), "agent-a", 10, Some("claim-1"), now).unwrap();

        let reclaimed = claim_thread(
            &mut store,
            Some("root"),
            "agent-b",
            30,
            Some("claim-2"),
            now + chrono::Duration::seconds(20),
        )
        .unwrap();
        assert_eq!(reclaimed.thread.active_claim.as_ref().unwrap().agent_id, "agent-b");
        assert!(reclaimed.thread.claims.iter().any(|claim| claim.status == "expired"));
    }
}
