use anyhow::{bail, Result};
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};

use crate::smartlist_write::{
    attach_member, browse_bucket, detach_member,
    SmartListAttachResult, SmartListDetachResult,
};
use crate::store::AmsStore;

pub const REGISTRY_PATH: &str = "smartlist/agent-pool/registry";
pub const FREE_PATH: &str = "smartlist/agent-pool/free";
pub const ALLOCATED_PATH: &str = "smartlist/agent-pool/allocated";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AllocateResult {
    pub agent_object_id: String,
    pub task_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseResult {
    pub agent_object_id: String,
    pub task_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolEntry {
    pub object_id: String,
    pub state: String,
    pub task_path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolStatus {
    pub free_count: usize,
    pub allocated_count: usize,
    pub agents: Vec<PoolEntry>,
}

/// Allocate an agent from the free pool to a task.
///
/// 1. Detach from free list (fails if agent is not on free list)
/// 2. Attach to allocated list
/// 3. Attach to the task path
pub fn allocate(
    store: &mut AmsStore,
    agent_ref: &str,
    task_path: &str,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<AllocateResult> {
    let detach_result = detach_member(store, FREE_PATH, agent_ref, created_by, now_utc)?;
    if !detach_result.removed {
        bail!(
            "agent '{}' is not on the free list (already allocated?)",
            agent_ref
        );
    }
    let _attach_allocated: SmartListAttachResult =
        attach_member(store, ALLOCATED_PATH, agent_ref, created_by, now_utc)?;
    let _attach_task: SmartListAttachResult =
        attach_member(store, task_path, agent_ref, created_by, now_utc)?;

    Ok(AllocateResult {
        agent_object_id: detach_result.member_object_id,
        task_path: task_path.to_string(),
    })
}

/// Release an agent from a task back to the free pool.
///
/// 1. Detach from allocated list
/// 2. Detach from the task path
/// 3. Attach back to free list
pub fn release(
    store: &mut AmsStore,
    agent_ref: &str,
    task_path: &str,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<ReleaseResult> {
    let detach_allocated: SmartListDetachResult =
        detach_member(store, ALLOCATED_PATH, agent_ref, created_by, now_utc)?;
    let _detach_task: SmartListDetachResult =
        detach_member(store, task_path, agent_ref, created_by, now_utc)?;
    let _attach_free: SmartListAttachResult =
        attach_member(store, FREE_PATH, agent_ref, created_by, now_utc)?;

    Ok(ReleaseResult {
        agent_object_id: detach_allocated.member_object_id,
        task_path: task_path.to_string(),
    })
}

/// Return the current pool status: free/allocated counts and per-agent entries.
pub fn status(store: &AmsStore) -> Result<PoolStatus> {
    let free_members = memberships_for(store, FREE_PATH);
    let allocated_members = memberships_for(store, ALLOCATED_PATH);

    let mut agents = Vec::new();
    for object_id in &free_members {
        agents.push(PoolEntry {
            object_id: object_id.clone(),
            state: "free".to_string(),
            task_path: None,
        });
    }
    for object_id in &allocated_members {
        agents.push(PoolEntry {
            object_id: object_id.clone(),
            state: "allocated".to_string(),
            task_path: None,
        });
    }

    Ok(PoolStatus {
        free_count: free_members.len(),
        allocated_count: allocated_members.len(),
        agents,
    })
}

fn memberships_for(store: &AmsStore, path: &str) -> Vec<String> {
    match browse_bucket(store, path) {
        Ok(items) => items.into_iter().map(|item| item.object_id).collect(),
        Err(_) => Vec::new(),
    }
}
