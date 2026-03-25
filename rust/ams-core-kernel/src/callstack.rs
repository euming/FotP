//! Swarm-plan callstack logic ported from ams.py lines 280-460+.
//!
//! Operates on `AmsStore` to find, create, and manage execution-plan nodes.
//! Path convention: `smartlist/execution-plan/<project>/<node>/...`
//! Node sub-buckets: `00-node` (meta), `10-children`, `20-observations`, `30-receipts`, `90-archive`.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use anyhow::{bail, Result};
use chrono::{DateTime, FixedOffset};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::{JsonMap, SemanticPayload};
use crate::policy::set_container_policy;
use crate::smartlist_write::{
    attach_member_before as smartlist_attach_before,
    create_bucket as create_smartlist_bucket, create_note as create_smartlist_note,
    detach_member as smartlist_detach, move_member as smartlist_move,
    set_bucket_fields as set_smartlist_bucket_fields,
};
use crate::store::AmsStore;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const EXECUTION_PLAN_ROOT: &str = "smartlist/execution-plan";
pub const NODE_BUCKET_SEGMENTS: &[&str] = &[
    "00-node",
    "10-children",
    "20-observations",
    "30-receipts",
    "90-archive",
];

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

pub fn slugify(value: &str) -> String {
    static RX: OnceLock<Regex> = OnceLock::new();
    let rx = RX.get_or_init(|| Regex::new(r"[^A-Za-z0-9]+").expect("valid slugify regex"));
    let slug = rx
        .replace_all(value.trim().to_ascii_lowercase().as_str(), "-")
        .trim_matches('-')
        .to_string();
    if slug.is_empty() { "node".to_string() } else { slug }
}

pub fn last_path_segment(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn bucket_object_id(path: &str) -> String {
    format!("smartlist-bucket:{path}")
}

fn members_container_id(path: &str) -> String {
    format!("smartlist-members:{path}")
}

pub fn node_meta_path(node_path: &str) -> String {
    format!("{node_path}/00-node")
}

pub fn node_children_path(node_path: &str) -> String {
    format!("{node_path}/10-children")
}

pub fn node_observations_path(node_path: &str) -> String {
    format!("{node_path}/20-observations")
}

pub fn node_receipts_path(node_path: &str) -> String {
    format!("{node_path}/30-receipts")
}

#[allow(dead_code)]
pub fn node_archive_path(node_path: &str) -> String {
    format!("{node_path}/90-archive")
}

// ---------------------------------------------------------------------------
// Bucket field reading helpers
// ---------------------------------------------------------------------------

/// Read all provenance fields from a bucket's meta object.
pub fn bucket_fields(store: &AmsStore, path: &str) -> BTreeMap<String, String> {
    let object_id = bucket_object_id(path);
    let Some(obj) = store.objects().get(&object_id) else {
        return BTreeMap::new();
    };
    let Some(prov) = obj
        .semantic_payload
        .as_ref()
        .and_then(|sp| sp.provenance.as_ref())
    else {
        return BTreeMap::new();
    };
    prov.iter()
        .map(|(k, v)| (k.clone(), value_to_string(v)))
        .collect()
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Number(n) => n.to_string(),
        _ => serde_json::to_string(value).unwrap_or_default(),
    }
}

/// Iterate member object IDs of a container in forward (linked-list) order.
fn iter_container_member_ids(store: &AmsStore, container_id: &str) -> Vec<String> {
    store
        .iterate_forward(container_id)
        .into_iter()
        .map(|ln| ln.object_id.clone())
        .collect()
}

/// Return ordered child node paths under `node_path`.
fn iter_children(store: &AmsStore, node_path: &str) -> Vec<String> {
    let container_id = members_container_id(&node_children_path(node_path));
    let member_ids = iter_container_member_ids(store, &container_id);
    let mut children = Vec::new();
    for oid in member_ids {
        if let Some(obj) = store.objects().get(&oid) {
            if let Some(path) = obj
                .semantic_payload
                .as_ref()
                .and_then(|sp| sp.provenance.as_ref())
                .and_then(|prov| prov.get("path"))
                .and_then(|v| v.as_str())
            {
                children.push(path.to_string());
            }
        }
    }
    children
}

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// A frame in the active callstack path.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CallstackFrame {
    pub node_path: String,
    pub fields: BTreeMap<String, String>,
}

/// Result of `find_active_node`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActiveNode {
    pub node_path: String,
    pub fields: BTreeMap<String, String>,
}

/// A project root entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectInfo {
    pub root_path: String,
    pub name: String,
    pub state: String,
    pub active_node_path: Option<String>,
}

/// Rendered callstack context output.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CallstackContext {
    pub frames: Vec<CallstackFrame>,
    pub observations: Vec<String>,
    pub parent_receipt: Option<String>,
    pub has_children: bool,
    pub node_kind: String,
    pub active_node_kind: String,
    pub policy_kind: Option<String>,
    pub repair_hint: Option<String>,
    pub plan_mode: String,
}

// ---------------------------------------------------------------------------
// Execution roots
// ---------------------------------------------------------------------------

/// Find all top-level project roots in the execution plan.
fn execution_roots(store: &AmsStore) -> Vec<(String, BTreeMap<String, String>)> {
    let prefix = format!("{EXECUTION_PLAN_ROOT}/");
    let suffix = "/00-node";
    let mut roots: Vec<(String, BTreeMap<String, String>)> = Vec::new();

    for (oid, obj) in store.objects() {
        if !oid.starts_with("smartlist-bucket:") {
            continue;
        }
        let Some(prov) = obj
            .semantic_payload
            .as_ref()
            .and_then(|sp| sp.provenance.as_ref())
        else {
            continue;
        };
        let Some(path) = prov.get("path").and_then(|v| v.as_str()) else {
            continue;
        };
        if !path.starts_with(&prefix) || !path.ends_with(suffix) {
            continue;
        }
        let fields: BTreeMap<String, String> = prov
            .iter()
            .map(|(k, v)| (k.clone(), value_to_string(v)))
            .collect();
        // Only roots have no parent_node_path
        if !fields.get("parent_node_path").map_or(true, |v| v.is_empty()) {
            continue;
        }
        let node_path = path[..path.len() - suffix.len()].to_string();
        roots.push((node_path, fields));
    }
    roots.sort_by(|a, b| a.0.cmp(&b.0));
    roots
}

// ---------------------------------------------------------------------------
// Mode gates
// ---------------------------------------------------------------------------

/// Read the plan_mode of the active (or named) project root.
/// Returns "execute" if no active root or field absent; "edit" if explicitly set.
pub fn read_plan_mode(store: &AmsStore, project_opt: Option<&str>) -> &'static str {
    let roots = execution_roots(store);
    let slug = project_opt.map(|p| slugify(p));
    let relevant: Vec<&(String, BTreeMap<String, String>)> = if let Some(ref s) = slug {
        roots.iter().filter(|(p, _)| last_path_segment(p) == s.as_str()).collect()
    } else {
        roots.iter().collect()
    };
    for (_, fields) in &relevant {
        let active_path = fields.get("active_node_path").map(|s| s.trim()).unwrap_or("");
        if !active_path.is_empty() {
            let mode = fields.get("plan_mode").map(|s| s.trim()).unwrap_or("");
            return if mode == "edit" { "edit" } else { "execute" };
        }
    }
    "execute"
}

/// Assert that the active plan is in the required mode.
/// Passes silently when there is no active plan (cold start).
pub fn assert_mode(
    store: &AmsStore,
    project_opt: Option<&str>,
    required: &str,
    cmd_name: &str,
) -> Result<()> {
    let roots = execution_roots(store);
    let slug = project_opt.map(|p| slugify(p));
    let relevant: Vec<&(String, BTreeMap<String, String>)> = if let Some(ref s) = slug {
        roots.iter().filter(|(p, _)| last_path_segment(p) == s.as_str()).collect()
    } else {
        roots.iter().collect()
    };
    for (root_path, fields) in &relevant {
        let active_path = fields.get("active_node_path").map(|s| s.trim()).unwrap_or("");
        if !active_path.is_empty() {
            let mode_raw = fields.get("plan_mode").map(|s| s.trim()).unwrap_or("");
            let mode = if mode_raw.is_empty() { "execute" } else { mode_raw };
            if mode != required {
                let plan_name = last_path_segment(root_path);
                let hint_cmd = if required == "execute" {
                    "ams.bat swarm-plan enter-execute"
                } else {
                    "ams.bat swarm-plan enter-edit"
                };
                bail!(
                    "error[MODE_GATE]: command '{}' requires plan_mode={}\n  \
                     but active plan '{}' is in plan_mode={}\n\
                     hint: run `{}`",
                    cmd_name, required, plan_name, mode, hint_cmd
                );
            }
            return Ok(());
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// find_active_node
// ---------------------------------------------------------------------------

/// Find the currently active node, optionally scoped to a project.
pub fn find_active_node(store: &AmsStore, project: Option<&str>) -> Option<ActiveNode> {
    let roots = execution_roots(store);
    let slug = project.map(slugify);

    let filtered_roots: Vec<&(String, BTreeMap<String, String>)> = if let Some(ref s) = slug {
        roots.iter().filter(|(p, _)| last_path_segment(p) == s.as_str()).collect()
    } else {
        roots.iter().collect()
    };

    // First: check root active_node_path pointers
    for (_, root_fields) in &filtered_roots {
        let active_path = root_fields.get("active_node_path").map(|s| s.trim()).unwrap_or("");
        if !active_path.is_empty() {
            let meta = node_meta_path(active_path);
            let fields = bucket_fields(store, &meta);
            if !fields.is_empty() {
                return Some(ActiveNode {
                    node_path: active_path.to_string(),
                    fields,
                });
            }
        }
    }

    // Collect parked root paths
    let parked_root_paths: Vec<&str> = filtered_roots
        .iter()
        .filter(|(_, f)| f.get("active_node_path").map_or(true, |v| v.trim().is_empty()))
        .map(|(p, _)| p.as_str())
        .collect();

    // Fallback: scan all active nodes
    let prefix = format!("{EXECUTION_PLAN_ROOT}/");
    let suffix = "/00-node";
    let mut candidates: Vec<(String, BTreeMap<String, String>)> = Vec::new();

    for (oid, obj) in store.objects() {
        if !oid.starts_with("smartlist-bucket:") {
            continue;
        }
        let Some(prov) = obj
            .semantic_payload
            .as_ref()
            .and_then(|sp| sp.provenance.as_ref())
        else {
            continue;
        };
        let Some(path) = prov.get("path").and_then(|v| v.as_str()) else {
            continue;
        };
        if !path.starts_with(&prefix) || !path.ends_with(suffix) {
            continue;
        }
        let fields: BTreeMap<String, String> = prov
            .iter()
            .map(|(k, v)| (k.clone(), value_to_string(v)))
            .collect();
        if fields.get("state").map(|s| s.as_str()) != Some("active") {
            continue;
        }
        let node_path = path[..path.len() - suffix.len()].to_string();
        if let Some(ref s) = slug {
            let root_segment = node_path[prefix.len()..].split('/').next().unwrap_or("");
            if root_segment != s.as_str() {
                continue;
            }
        }
        // Skip nodes under parked roots
        let under_parked = parked_root_paths.iter().any(|r| {
            node_path == *r || node_path.starts_with(&format!("{r}/"))
        });
        if under_parked {
            continue;
        }
        candidates.push((node_path, fields));
    }
    candidates.sort_by(|a, b| a.0.cmp(&b.0));
    candidates.into_iter().next().map(|(node_path, fields)| ActiveNode { node_path, fields })
}

// ---------------------------------------------------------------------------
// active_path_frames
// ---------------------------------------------------------------------------

/// Walk from `node_path` up to the root via `parent_node_path`, returning frames root-first.
pub fn active_path_frames(store: &AmsStore, node_path: &str) -> Vec<CallstackFrame> {
    let mut frames = Vec::new();
    let mut current = node_path.to_string();
    let mut visited = std::collections::HashSet::new();

    while !current.is_empty() && visited.insert(current.clone()) {
        let meta = node_meta_path(&current);
        let fields = bucket_fields(store, &meta);
        if fields.is_empty() {
            break;
        }
        frames.push(CallstackFrame {
            node_path: current.clone(),
            fields: fields.clone(),
        });
        current = fields
            .get("parent_node_path")
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
    }
    frames.reverse();
    frames
}

// ---------------------------------------------------------------------------
// unique_node_path
// ---------------------------------------------------------------------------

/// Generate a unique node path that doesn't collide with existing buckets.
pub fn unique_node_path(store: &AmsStore, parent_node_path: Option<&str>, name: &str) -> String {
    let base = slugify(name);
    let prefix = match parent_node_path {
        Some(parent) => format!("{}/{base}", node_children_path(parent)),
        None => format!("{EXECUTION_PLAN_ROOT}/{base}"),
    };

    let mut candidate = prefix.clone();
    let mut suffix = 2u32;
    while store.objects().contains_key(&bucket_object_id(&candidate)) {
        candidate = format!("{prefix}-{suffix}");
        suffix += 1;
    }
    candidate
}

// ---------------------------------------------------------------------------
// create_runtime_node
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct CreateNodeParams<'a> {
    pub name: &'a str,
    pub owner: &'a str,
    pub kind: &'a str,
    pub state: &'a str,
    pub next_command: &'a str,
    pub parent_node_path: Option<&'a str>,
    pub root_path: Option<&'a str>,
    pub description: Option<&'a str>,
    pub resume_policy: &'a str,
    pub extra_fields: Option<&'a BTreeMap<String, String>>,
}

/// Create a new runtime node with all sub-buckets, metadata, and optional description note.
pub fn create_runtime_node(
    store: &mut AmsStore,
    params: &CreateNodeParams,
    now: DateTime<FixedOffset>,
) -> Result<String> {
    let node_path = unique_node_path(store, params.parent_node_path, params.name);

    // Create the node bucket and all sub-buckets
    let all_paths: Vec<String> = std::iter::once(node_path.clone())
        .chain(NODE_BUCKET_SEGMENTS.iter().map(|seg| format!("{node_path}/{seg}")))
        .collect();

    for path in &all_paths {
        create_smartlist_bucket(store, path, false, params.owner, now)?;
    }

    // Set meta fields
    let effective_root = params.root_path.unwrap_or(&node_path);
    let mut fields = BTreeMap::from([
        ("kind".to_string(), params.kind.to_string()),
        ("state".to_string(), params.state.to_string()),
        ("owner".to_string(), params.owner.to_string()),
        ("title".to_string(), params.name.to_string()),
        ("next_command".to_string(), params.next_command.to_string()),
        ("resume_policy".to_string(), params.resume_policy.to_string()),
        ("root_path".to_string(), effective_root.to_string()),
        (
            "parent_node_path".to_string(),
            params.parent_node_path.unwrap_or("").to_string(),
        ),
        ("node_path".to_string(), node_path.clone()),
    ]);
    if let Some(extra) = params.extra_fields {
        fields.extend(extra.iter().map(|(k, v)| (k.clone(), v.clone())));
    }
    set_smartlist_bucket_fields(store, &node_meta_path(&node_path), &fields, params.owner, now)?;

    // Write description as an observation note
    if let Some(desc) = params.description {
        if !desc.trim().is_empty() {
            let title = format!("{}:{}", params.kind, params.name);
            create_smartlist_note(
                store,
                &title,
                desc,
                &[node_observations_path(&node_path)],
                false,
                params.owner,
                now,
                None,
            )?;
        }
    }

    Ok(node_path)
}

// ---------------------------------------------------------------------------
// set_node_state / set_node_fields
// ---------------------------------------------------------------------------

/// Set arbitrary fields on a node's meta bucket.
pub fn set_node_fields(
    store: &mut AmsStore,
    node_path: &str,
    fields: &BTreeMap<String, String>,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<()> {
    set_smartlist_bucket_fields(store, &node_meta_path(node_path), fields, actor_id, now)?;
    Ok(())
}

/// Set a node's state field.
pub fn set_node_state(
    store: &mut AmsStore,
    node_path: &str,
    state: &str,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<()> {
    let fields = BTreeMap::from([("state".to_string(), state.to_string())]);
    set_node_fields(store, node_path, &fields, actor_id, now)
}

/// Set the active_node_path pointer on a project root.
pub fn set_root_active_path(
    store: &mut AmsStore,
    root_path: &str,
    active_node_path: &str,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<()> {
    let fields = BTreeMap::from([(
        "active_node_path".to_string(),
        active_node_path.to_string(),
    )]);
    set_node_fields(store, root_path, &fields, actor_id, now)
}

// ---------------------------------------------------------------------------
// read_observations / read_latest_receipt
// ---------------------------------------------------------------------------

/// Read observation texts from a node's 20-observations bucket.
pub fn read_observations(store: &AmsStore, node_path: &str) -> Vec<String> {
    let container_id = members_container_id(&node_observations_path(node_path));
    let member_ids = iter_container_member_ids(store, &container_id);
    let mut texts = Vec::new();
    for oid in member_ids {
        if let Some(obj) = store.objects().get(&oid) {
            let sp = obj.semantic_payload.as_ref();
            let text = sp
                .and_then(|sp| sp.provenance.as_ref())
                .and_then(|prov| prov.get("text"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !text.is_empty() {
                texts.push(text.to_string());
            }
        }
    }
    texts
}

/// Read the latest receipt text from a node's 30-receipts bucket.
pub fn read_latest_receipt(store: &AmsStore, node_path: &str) -> Option<String> {
    let container_id = members_container_id(&node_receipts_path(node_path));
    let member_ids = iter_container_member_ids(store, &container_id);
    let last_oid = member_ids.last()?;
    let obj = store.objects().get(last_oid)?;
    let text = obj
        .semantic_payload
        .as_ref()
        .and_then(|sp| sp.provenance.as_ref())
        .and_then(|prov| prov.get("text"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if text.is_empty() { None } else { Some(text.to_string()) }
}

// ---------------------------------------------------------------------------
// render_context
// ---------------------------------------------------------------------------

/// Build a structured `CallstackContext` for the active node.
pub fn render_context(store: &AmsStore, project: Option<&str>) -> Option<CallstackContext> {
    let active = find_active_node(store, project)?;
    let frames = active_path_frames(store, &active.node_path);
    if frames.is_empty() {
        return None;
    }

    let observations = read_observations(store, &active.node_path);

    let parent_receipt = if frames.len() >= 2 {
        read_latest_receipt(store, &frames[frames.len() - 2].node_path)
    } else {
        None
    };

    let children = iter_children(store, &active.node_path);
    let has_children = !children.is_empty();
    let node_kind = if frames.len() <= 1 {
        "root"
    } else if has_children {
        "branch"
    } else {
        "leaf"
    }
    .to_string();

    let last_fields = &frames.last().unwrap().fields;
    let active_node_kind = last_fields
        .get("kind")
        .cloned()
        .unwrap_or_else(|| "work".to_string());
    let policy_kind = last_fields
        .get("policy_kind")
        .filter(|v| !v.trim().is_empty())
        .cloned();
    let repair_hint = last_fields
        .get("repair_hint")
        .filter(|v| !v.trim().is_empty())
        .cloned();

    let root_meta_fields = bucket_fields(store, &node_meta_path(&frames[0].node_path));
    let plan_mode = root_meta_fields
        .get("plan_mode")
        .filter(|v| !v.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| "execute".to_string());

    Some(CallstackContext {
        frames,
        observations,
        parent_receipt,
        has_children,
        node_kind,
        active_node_kind,
        policy_kind,
        repair_hint,
        plan_mode,
    })
}

/// Render context to the same text format as the Python `callstack_context`.
pub fn render_context_text(store: &AmsStore, project: Option<&str>, max_chars: usize) -> Option<String> {
    let ctx = render_context(store, project)?;
    let mut lines: Vec<String> = vec![
        "[AMS Callstack Context]".to_string(),
        "Frames:".to_string(),
    ];
    for (i, frame) in ctx.frames.iter().enumerate() {
        let title = last_path_segment(&frame.node_path);
        let kind = frame.fields.get("kind").map(|s| s.as_str()).unwrap_or("work");
        let state = frame.fields.get("state").map(|s| s.as_str()).unwrap_or("ready");
        lines.push(format!("{}. {} [{}/{}]", i + 1, title, kind, state));
    }
    if !ctx.observations.is_empty() {
        lines.push("---".to_string());
        lines.push("Active observations:".to_string());
        for obs in &ctx.observations {
            lines.push(format!("- {obs}"));
        }
    }
    if let Some(receipt) = &ctx.parent_receipt {
        lines.push("---".to_string());
        lines.push("Parent receipt:".to_string());
        lines.push(format!("- {receipt}"));
    }
    lines.push("---".to_string());
    lines.push(format!("has_children={}", ctx.has_children));
    lines.push(format!("node_kind={}", ctx.node_kind));
    lines.push(format!("active_node_kind={}", ctx.active_node_kind));
    if let Some(pk) = &ctx.policy_kind {
        lines.push(format!("policy_kind={pk}"));
    }
    if let Some(rh) = &ctx.repair_hint {
        lines.push(format!("repair_hint={rh}"));
    }
    lines.push(format!("plan_mode={}", ctx.plan_mode));
    lines.push("[End callstack context]".to_string());

    let mut output = lines.join("\n");
    if output.len() > max_chars {
        output.truncate(max_chars.saturating_sub(15));
        output.push_str("\n[...truncated]");
    }
    Some(output)
}

// ---------------------------------------------------------------------------
// list_projects
// ---------------------------------------------------------------------------

/// List all execution-plan project roots.
pub fn list_projects(store: &AmsStore) -> Vec<ProjectInfo> {
    execution_roots(store)
        .into_iter()
        .map(|(root_path, fields)| {
            let active_path = fields
                .get("active_node_path")
                .filter(|v| !v.trim().is_empty())
                .cloned();
            let state = if active_path.is_some() {
                fields.get("state").cloned().unwrap_or_else(|| "active".to_string())
            } else {
                let s = fields.get("state").cloned().unwrap_or_else(|| "ready".to_string());
                if s != "completed" { "parked".to_string() } else { s }
            };
            ProjectInfo {
                name: last_path_segment(&root_path).to_string(),
                root_path,
                state,
                active_node_path: active_path,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// switch_project
// ---------------------------------------------------------------------------

/// Switch to a named project, parking all others.
pub fn switch_project(
    store: &mut AmsStore,
    project: &str,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<String> {
    let slug = slugify(project);
    let roots = execution_roots(store);

    let mut target: Option<(String, BTreeMap<String, String>)> = None;
    for (root_path, root_fields) in &roots {
        if last_path_segment(root_path) == slug {
            target = Some((root_path.clone(), root_fields.clone()));
        } else {
            let active_path = root_fields
                .get("active_node_path")
                .map(|s| s.trim())
                .unwrap_or("");
            if !active_path.is_empty() {
                set_node_state(store, active_path, "parked", actor_id, now)?;
                set_root_active_path(store, root_path, "", actor_id, now)?;
            }
        }
    }

    let (root_path, root_fields) = target
        .ok_or_else(|| anyhow::anyhow!("no project root named '{}' found", slug))?;

    let active_path = root_fields
        .get("active_node_path")
        .map(|s| s.trim().to_string())
        .unwrap_or_default();

    if active_path.is_empty() {
        set_root_active_path(store, &root_path, &root_path, actor_id, now)?;
        set_node_state(store, &root_path, "active", actor_id, now)?;
        Ok(root_path)
    } else {
        // Ensure the active node is marked active (might have been parked)
        let fields = bucket_fields(store, &node_meta_path(&active_path));
        if fields.get("state").map(|s| s.as_str()) == Some("parked") {
            set_node_state(store, &active_path, "active", actor_id, now)?;
        }
        Ok(active_path)
    }
}

// ---------------------------------------------------------------------------
// park_project
// ---------------------------------------------------------------------------

/// Park the active project (or a named project).
pub fn park_project(
    store: &mut AmsStore,
    project: Option<&str>,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<Option<String>> {
    if let Some(name) = project {
        let slug = slugify(name);
        let roots = execution_roots(store);
        for (root_path, root_fields) in &roots {
            if last_path_segment(root_path) == slug {
                let active_path = root_fields
                    .get("active_node_path")
                    .map(|s| s.trim())
                    .unwrap_or("");
                if !active_path.is_empty() {
                    set_node_state(store, active_path, "parked", actor_id, now)?;
                }
                set_root_active_path(store, root_path, "", actor_id, now)?;
                return Ok(Some(slug));
            }
        }
        bail!("no project root named '{}' found", slug);
    }

    // Park whatever is currently active
    let active = find_active_node(store, None);
    let Some(active) = active else {
        return Ok(None);
    };
    let root_path = active
        .fields
        .get("root_path")
        .cloned()
        .unwrap_or_else(|| active.node_path.clone());
    set_node_state(store, &active.node_path, "parked", actor_id, now)?;
    set_root_active_path(store, &root_path, "", actor_id, now)?;
    Ok(Some(last_path_segment(&root_path).to_string()))
}

// ---------------------------------------------------------------------------
// resolve_runtime_root
// ---------------------------------------------------------------------------

/// Resolve the project root path for an active node.
pub fn resolve_runtime_root(active_node_path: &str, active_fields: &BTreeMap<String, String>) -> String {
    active_fields
        .get("root_path")
        .filter(|v| !v.is_empty())
        .cloned()
        .unwrap_or_else(|| active_node_path.to_string())
}

// ---------------------------------------------------------------------------
// is_node_ready
// ---------------------------------------------------------------------------

/// Check if a node is ready (state=ready + dependency constraints satisfied).
pub fn is_node_ready(store: &AmsStore, node_path: &str, siblings: Option<&[String]>) -> bool {
    let fields = bucket_fields(store, &node_meta_path(node_path));
    if fields.get("state").map(|s| s.as_str()) != Some("ready") {
        return false;
    }
    let depends_on = fields.get("depends_on").map(|s| s.trim()).unwrap_or("");
    if depends_on.is_empty() {
        return true;
    }
    let dep_titles: Vec<&str> = depends_on.split(',').map(|t| t.trim()).filter(|t| !t.is_empty()).collect();
    let owned_siblings;
    let sibs = match siblings {
        Some(s) => s,
        None => {
            let parent = fields.get("parent_node_path").map(|s| s.trim()).unwrap_or("");
            owned_siblings = if parent.is_empty() {
                Vec::new()
            } else {
                iter_children(store, parent)
            };
            &owned_siblings
        }
    };
    for sib in sibs {
        let sib_fields = bucket_fields(store, &node_meta_path(sib));
        let sib_title = sib_fields.get("title").map(|s| s.as_str()).unwrap_or("");
        if dep_titles.contains(&sib_title) && sib_fields.get("state").map(|s| s.as_str()) != Some("completed") {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// ready_nodes / navigation helpers
// ---------------------------------------------------------------------------

/// Return all children of `parent_path` that are ready (state=ready + deps satisfied).
pub fn ready_nodes(store: &AmsStore, parent_path: &str) -> Vec<String> {
    let children = iter_children(store, parent_path);
    children
        .iter()
        .filter(|c| is_node_ready(store, c, Some(&children)))
        .cloned()
        .collect()
}

/// Return the path of the next ready sibling after `node_path`, or None.
fn find_next_ready_sibling(store: &AmsStore, node_path: &str) -> Option<String> {
    let fields = bucket_fields(store, &node_meta_path(node_path));
    let parent = fields.get("parent_node_path").map(|s| s.trim()).unwrap_or("");
    if parent.is_empty() {
        return None;
    }
    let siblings = iter_children(store, parent);
    let mut found_self = false;
    for sibling in &siblings {
        if sibling == node_path {
            found_self = true;
            continue;
        }
        if found_self && is_node_ready(store, sibling, Some(&siblings)) {
            return Some(sibling.clone());
        }
    }
    None
}

/// Walk down from `node_path` into its first ready child, iteratively.
fn descend_to_first_ready_leaf(store: &AmsStore, node_path: &str) -> String {
    let mut current = node_path.to_string();
    loop {
        let children = iter_children(store, &current);
        let ready_child = children.iter().find(|child| {
            bucket_fields(store, &node_meta_path(child))
                .get("state")
                .map(|s| s.as_str()) == Some("ready")
        });
        match ready_child {
            Some(child) => current = child.clone(),
            None => return current,
        }
    }
}

// ---------------------------------------------------------------------------
// High-level operations: Push, Pop, Observe, Show, Interrupt, Resume, Advance
// ---------------------------------------------------------------------------

/// Result of a callstack operation, suitable for CLI output.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CallstackOpResult {
    pub action: String,
    pub fields: BTreeMap<String, String>,
}

impl CallstackOpResult {
    fn new(action: &str) -> Self {
        Self {
            action: action.to_string(),
            fields: BTreeMap::new(),
        }
    }

    fn field(mut self, key: &str, value: &str) -> Self {
        self.fields.insert(key.to_string(), value.to_string());
        self
    }

    /// Render as `key=value` lines (matching Python output format).
    pub fn to_text(&self) -> String {
        let mut lines = vec![format!("action={}", self.action)];
        for (k, v) in &self.fields {
            lines.push(format!("{k}={v}"));
        }
        lines.join("\n")
    }
}

/// Push a new child node onto the callstack (or create a root if none exists).
pub fn callstack_push(
    store: &mut AmsStore,
    name: &str,
    description: Option<&str>,
    actor_id: &str,
    depends_on: Option<&str>,
    now: DateTime<FixedOffset>,
) -> Result<CallstackOpResult> {
    let active = find_active_node(store, None);

    if active.is_none() {
        // Create a new root
        let mut root_extra = BTreeMap::new();
        root_extra.insert("active_node_path".to_string(), String::new());
        root_extra.insert("plan_mode".to_string(), "execute".to_string());
        let root_path = create_runtime_node(
            store,
            &CreateNodeParams {
                name,
                owner: actor_id,
                kind: "work",
                state: "active",
                next_command: "callstack push",
                parent_node_path: None,
                root_path: None,
                description,
                resume_policy: "next-sibling",
                extra_fields: Some(&root_extra),
            },
            now,
        )?;
        set_root_active_path(store, &root_path, &root_path, actor_id, now)?;
        return Ok(CallstackOpResult::new("push-root").field("node_path", &root_path));
    }

    let active = active.unwrap();
    let current_kind = active.fields.get("kind").map(|s| s.as_str()).unwrap_or("work");
    if current_kind == "interrupt" {
        bail!("callstack push cannot attach a generic work child under an active interrupt");
    }

    let root_path = resolve_runtime_root(&active.node_path, &active.fields);
    set_node_state(store, &active.node_path, "ready", actor_id, now)?;

    let mut extra = BTreeMap::new();
    if let Some(deps) = depends_on {
        extra.insert("depends_on".to_string(), deps.to_string());
    }

    let child_path = create_runtime_node(
        store,
        &CreateNodeParams {
            name,
            owner: actor_id,
            kind: "work",
            state: "active",
            next_command: "callstack pop",
            parent_node_path: Some(&active.node_path),
            root_path: Some(&root_path),
            description,
            resume_policy: "next-sibling",
            extra_fields: if extra.is_empty() { None } else { Some(&extra) },
        },
        now,
    )?;
    set_root_active_path(store, &root_path, &child_path, actor_id, now)?;

    Ok(CallstackOpResult::new("push")
        .field("node_path", &child_path)
        .field("kind", "work")
        .field("parent_path", &active.node_path))
}

/// Pop (complete) the active work or policy node.
pub fn callstack_pop(
    store: &mut AmsStore,
    return_text: Option<&str>,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<CallstackOpResult> {
    let active = find_active_node(store, None)
        .ok_or_else(|| anyhow::anyhow!("no active SmartList callstack frame"))?;

    let kind = active.fields.get("kind").map(|s| s.as_str()).unwrap_or("work");
    if kind != "work" && kind != "policy" {
        bail!("callstack pop only supports work or policy nodes, not '{kind}'");
    }

    let title = active
        .fields
        .get("title")
        .cloned()
        .unwrap_or_else(|| last_path_segment(&active.node_path).to_string());
    let receipt = return_text
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("Completed {title}"));

    create_smartlist_note(
        store,
        &format!("return:{title}"),
        &receipt,
        &[node_receipts_path(&active.node_path)],
        false,
        actor_id,
        now,
        None,
    )?;
    set_node_state(store, &active.node_path, "completed", actor_id, now)?;

    let parent = active
        .fields
        .get("parent_node_path")
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    let root_path = resolve_runtime_root(&active.node_path, &active.fields);

    if !parent.is_empty() {
        set_node_state(store, &parent, "active", actor_id, now)?;
        set_root_active_path(store, &root_path, &parent, actor_id, now)?;
        Ok(CallstackOpResult::new("pop")
            .field("completed", &active.node_path)
            .field("active_node_path", &parent))
    } else {
        set_root_active_path(store, &root_path, "", actor_id, now)?;
        Ok(CallstackOpResult::new("pop-root")
            .field("completed", &active.node_path)
            .field("active_node_path", ""))
    }
}

/// Complete a specific node by path (for parallel dispatch — doesn't require it to be the active cursor).
pub fn callstack_complete_node(
    store: &mut AmsStore,
    node_path: &str,
    return_text: Option<&str>,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<CallstackOpResult> {
    let fields = bucket_fields(store, &node_meta_path(node_path));
    if fields.is_empty() {
        bail!("node not found at {node_path}");
    }

    let title = fields
        .get("title")
        .cloned()
        .unwrap_or_else(|| last_path_segment(node_path).to_string());
    let receipt = return_text
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("Completed {title}"));

    create_smartlist_note(
        store,
        &format!("return:{title}"),
        &receipt,
        &[node_receipts_path(node_path)],
        false,
        actor_id,
        now,
        None,
    )?;
    set_node_state(store, node_path, "completed", actor_id, now)?;

    Ok(CallstackOpResult::new("complete-node")
        .field("completed", node_path)
        .field("title", &title))
}

/// Write an observation note to the active node.
pub fn callstack_observe(
    store: &mut AmsStore,
    title: &str,
    text: &str,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<CallstackOpResult> {
    callstack_observe_at(store, title, text, actor_id, now, None)
}

/// Write an observation to a specific node path (or the active node if None).
pub fn callstack_observe_at(
    store: &mut AmsStore,
    title: &str,
    text: &str,
    actor_id: &str,
    now: DateTime<FixedOffset>,
    target_node_path: Option<&str>,
) -> Result<CallstackOpResult> {
    let node_path = if let Some(path) = target_node_path {
        path.to_string()
    } else {
        let active = find_active_node(store, None)
            .ok_or_else(|| anyhow::anyhow!("no active SmartList callstack frame"))?;
        active.node_path
    };

    create_smartlist_note(
        store,
        title,
        text,
        &[node_observations_path(&node_path)],
        false,
        actor_id,
        now,
        None,
    )?;

    Ok(CallstackOpResult::new("observe")
        .field("node_path", &node_path)
        .field("title", title))
}

/// Render the active callstack as display lines.
pub fn callstack_show(store: &AmsStore, project: Option<&str>) -> Vec<String> {
    let active = match find_active_node(store, project) {
        Some(a) => a,
        None => return vec!["(empty call stack - no active SmartList runtime)".to_string()],
    };
    let frames = active_path_frames(store, &active.node_path);
    if frames.is_empty() {
        return vec!["(empty call stack - no active SmartList runtime)".to_string()];
    }
    frames
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let title = last_path_segment(&f.node_path);
            let kind = f.fields.get("kind").map(|s| s.as_str()).unwrap_or("work");
            let state = f.fields.get("state").map(|s| s.as_str()).unwrap_or("ready");
            format!("{}. {} [{}/{}] {}", i + 1, title, kind, state, f.node_path)
        })
        .collect()
}

/// Parameters for creating an interrupt.
pub struct InterruptParams<'a> {
    pub actor_id: &'a str,
    pub policy_kind: &'a str,
    pub reason: &'a str,
    pub error_output: &'a str,
    pub context: &'a str,
    pub attempted_fix: &'a str,
    pub repair_hint: &'a str,
    pub subtask_hints: &'a str,
}

/// Insert an interrupt before the active work node.
pub fn callstack_interrupt(
    store: &mut AmsStore,
    params: &InterruptParams,
    now: DateTime<FixedOffset>,
) -> Result<CallstackOpResult> {
    let active = find_active_node(store, None)
        .ok_or_else(|| anyhow::anyhow!("no active SmartList callstack frame"))?;

    let current_kind = active.fields.get("kind").map(|s| s.as_str()).unwrap_or("work");
    if current_kind != "work" {
        bail!("callstack interrupt requires an active work node");
    }
    let parent_node_path = active
        .fields
        .get("parent_node_path")
        .map(|s| s.trim())
        .unwrap_or("");
    if parent_node_path.is_empty() {
        bail!("cannot interrupt the root execution node");
    }

    let root_path = resolve_runtime_root(&active.node_path, &active.fields);
    let interrupt_title = format!("interrupt-{}", last_path_segment(&active.node_path));

    let interrupt_description = [
        if params.reason.is_empty() { String::new() } else { format!("reason={}", params.reason) },
        format!("policy_kind={}", params.policy_kind),
        if params.context.is_empty() { String::new() } else { format!("context={}", params.context) },
    ]
    .iter()
    .filter(|l| !l.is_empty())
    .cloned()
    .collect::<Vec<_>>()
    .join("\n");

    let interrupt_path = create_runtime_node(
        store,
        &CreateNodeParams {
            name: &interrupt_title,
            owner: params.actor_id,
            kind: "interrupt",
            state: "active",
            next_command: "callstack resume",
            parent_node_path: Some(parent_node_path),
            root_path: Some(&root_path),
            description: if interrupt_description.is_empty() { None } else { Some(&interrupt_description) },
            resume_policy: "next-sibling",
            extra_fields: Some(&BTreeMap::from([
                ("reason".to_string(), params.reason.to_string()),
                ("context".to_string(), params.context.to_string()),
                ("policy_kind".to_string(), params.policy_kind.to_string()),
                ("interrupted_node_path".to_string(), active.node_path.clone()),
            ])),
        },
        now,
    )?;

    // Reorder: detach interrupt, then insert before interrupted node
    let parent_children = node_children_path(parent_node_path);
    smartlist_detach(store, &parent_children, &interrupt_path, params.actor_id, now)?;
    smartlist_attach_before(store, &parent_children, &interrupt_path, &active.node_path, params.actor_id, now)?;

    set_node_state(store, &active.node_path, "paused", params.actor_id, now)?;
    set_root_active_path(store, &root_path, &interrupt_path, params.actor_id, now)?;

    let mut active_path = interrupt_path.clone();
    let mut policy_path = String::new();

    if params.policy_kind == "repair" {
        set_node_state(store, &interrupt_path, "running-policy", params.actor_id, now)?;
        let policy_description = [
            if params.repair_hint.is_empty() { String::new() } else { format!("repair_hint={}", params.repair_hint) },
            if params.attempted_fix.is_empty() { String::new() } else { format!("attempted_fix={}", params.attempted_fix) },
            if params.error_output.is_empty() { String::new() } else { format!("error_output={}", params.error_output) },
        ]
        .iter()
        .filter(|l| !l.is_empty())
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");

        let desc = if policy_description.is_empty() {
            if !params.repair_hint.is_empty() {
                params.repair_hint.to_string()
            } else {
                format!("Repair work for {}", last_path_segment(&active.node_path))
            }
        } else {
            policy_description
        };

        policy_path = create_runtime_node(
            store,
            &CreateNodeParams {
                name: &format!("repair-{}", last_path_segment(&active.node_path)),
                owner: params.actor_id,
                kind: "policy",
                state: "active",
                next_command: "callstack pop",
                parent_node_path: Some(&interrupt_path),
                root_path: Some(&root_path),
                description: Some(&desc),
                resume_policy: "next-sibling",
                extra_fields: Some(&BTreeMap::from([
                    ("policy_kind".to_string(), "repair".to_string()),
                    ("interrupted_node_path".to_string(), active.node_path.clone()),
                    ("repair_hint".to_string(), params.repair_hint.to_string()),
                    ("attempted_fix".to_string(), params.attempted_fix.to_string()),
                    ("error_output".to_string(), params.error_output.to_string()),
                ])),
            },
            now,
        )?;
        set_root_active_path(store, &root_path, &policy_path, params.actor_id, now)?;
        active_path = policy_path.clone();
    } else if params.policy_kind == "decompose" {
        set_node_state(store, &interrupt_path, "running-policy", params.actor_id, now)?;
        let hints_list: Vec<&str> = params
            .subtask_hints
            .split(',')
            .map(|h| h.trim())
            .filter(|h| !h.is_empty())
            .collect();
        let policy_description = [
            if params.reason.is_empty() { String::new() } else { format!("reason={}", params.reason) },
            if hints_list.is_empty() { String::new() } else { format!("subtask_hints={}", hints_list.join(",")) },
        ]
        .iter()
        .filter(|l| !l.is_empty())
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");

        let desc = if policy_description.is_empty() {
            format!("Decompose {} into subtasks", last_path_segment(&active.node_path))
        } else {
            policy_description
        };

        policy_path = create_runtime_node(
            store,
            &CreateNodeParams {
                name: &format!("decompose-{}", last_path_segment(&active.node_path)),
                owner: params.actor_id,
                kind: "policy",
                state: "active",
                next_command: "callstack pop",
                parent_node_path: Some(&interrupt_path),
                root_path: Some(&root_path),
                description: Some(&desc),
                resume_policy: "next-sibling",
                extra_fields: Some(&BTreeMap::from([
                    ("policy_kind".to_string(), "decompose".to_string()),
                    ("interrupted_node_path".to_string(), active.node_path.clone()),
                    ("subtask_hints".to_string(), hints_list.join(",")),
                    ("reason".to_string(), params.reason.to_string()),
                ])),
            },
            now,
        )?;
        set_root_active_path(store, &root_path, &policy_path, params.actor_id, now)?;
        active_path = policy_path.clone();
    }

    Ok(CallstackOpResult::new("interrupt")
        .field("interrupt_path", &interrupt_path)
        .field("interrupted_node_path", &active.node_path)
        .field("policy_kind", params.policy_kind)
        .field("policy_path", &policy_path)
        .field("active_node_path", &active_path))
}

/// Archive the active interrupt and resume its interrupted sibling.
pub fn callstack_resume(
    store: &mut AmsStore,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<CallstackOpResult> {
    let active = find_active_node(store, None)
        .ok_or_else(|| anyhow::anyhow!("no active SmartList callstack frame"))?;

    if active.fields.get("kind").map(|s| s.as_str()) != Some("interrupt") {
        bail!("callstack resume requires the active node to be an interrupt");
    }

    let parent_node_path = active
        .fields
        .get("parent_node_path")
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    let interrupted_node_path = active
        .fields
        .get("interrupted_node_path")
        .map(|s| s.trim().to_string())
        .unwrap_or_default();

    if parent_node_path.is_empty() || interrupted_node_path.is_empty() {
        bail!("interrupt metadata is incomplete; missing parent_node_path or interrupted_node_path");
    }

    let title = active
        .fields
        .get("title")
        .cloned()
        .unwrap_or_else(|| last_path_segment(&active.node_path).to_string());
    let root_path = resolve_runtime_root(&active.node_path, &active.fields);

    create_smartlist_note(
        store,
        &format!("resume:{title}"),
        &format!("Resolved interrupt and resumed {interrupted_node_path}"),
        &[node_receipts_path(&active.node_path)],
        false,
        actor_id,
        now,
        None,
    )?;
    set_node_state(store, &active.node_path, "archived", actor_id, now)?;

    // Move interrupt to archive
    smartlist_move(
        store,
        &node_children_path(&parent_node_path),
        &node_archive_path(&parent_node_path),
        &active.node_path,
        None,
        actor_id,
        now,
    )?;

    set_node_state(store, &interrupted_node_path, "active", actor_id, now)?;
    set_root_active_path(store, &root_path, &interrupted_node_path, actor_id, now)?;

    Ok(CallstackOpResult::new("resume")
        .field("archived_interrupt_path", &active.node_path)
        .field("active_node_path", &interrupted_node_path))
}

/// Advance the callstack cursor to the next ready node.
pub fn callstack_advance(
    store: &mut AmsStore,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<CallstackOpResult> {
    let active = match find_active_node(store, None) {
        Some(a) => a,
        None => return Ok(CallstackOpResult::new("tree-complete").field("active_node_path", "")),
    };

    let root_path = resolve_runtime_root(&active.node_path, &active.fields);

    // Try children first
    let children = iter_children(store, &active.node_path);
    for child in &children {
        if is_node_ready(store, child, Some(&children)) {
            let target = descend_to_first_ready_leaf(store, child);
            set_node_state(store, &target, "active", actor_id, now)?;
            set_root_active_path(store, &root_path, &target, actor_id, now)?;
            return Ok(CallstackOpResult::new("advance")
                .field("from", &active.node_path)
                .field("active_node_path", &target));
        }
    }

    // Walk up the tree looking for the next ready sibling
    let mut completed_nodes: Vec<String> = Vec::new();
    let mut cursor = active.node_path.clone();
    let mut visited = std::collections::HashSet::new();

    while !cursor.is_empty() && visited.insert(cursor.clone()) {
        if let Some(next_sib) = find_next_ready_sibling(store, &cursor) {
            if cursor != active.node_path {
                set_node_state(store, &cursor, "completed", actor_id, now)?;
                completed_nodes.push(cursor);
            }
            let target = descend_to_first_ready_leaf(store, &next_sib);
            set_node_state(store, &target, "active", actor_id, now)?;
            set_root_active_path(store, &root_path, &target, actor_id, now)?;
            let mut result = CallstackOpResult::new("advance")
                .field("from", &active.node_path)
                .field("active_node_path", &target);
            if !completed_nodes.is_empty() {
                result = result.field("completed_parents", &completed_nodes.join(","));
            }
            return Ok(result);
        }
        let cursor_fields = bucket_fields(store, &node_meta_path(&cursor));
        let parent = cursor_fields
            .get("parent_node_path")
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        if parent.is_empty() {
            break;
        }
        if cursor != active.node_path {
            set_node_state(store, &cursor, "completed", actor_id, now)?;
            completed_nodes.push(cursor.clone());
        }
        cursor = parent;
    }

    // Tree complete
    if !cursor.is_empty() && cursor != active.node_path {
        set_node_state(store, &cursor, "completed", actor_id, now)?;
        completed_nodes.push(cursor);
    }
    set_root_active_path(store, &root_path, "", actor_id, now)?;

    let mut result = CallstackOpResult::new("tree-complete")
        .field("completed_from", &active.node_path)
        .field("active_node_path", "");
    if !completed_nodes.is_empty() {
        result = result.field("completed_parents", &completed_nodes.join(","));
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// callstack_load_plan
// ---------------------------------------------------------------------------

/// A single node definition read from a JSON plan file.
#[derive(Debug)]
pub struct PlanNodeDef {
    pub title: String,
    pub description: Option<String>,
    /// Comma-separated list of sibling titles that must complete before this node.
    pub depends_on: Option<String>,
    /// Dispatch role (e.g. "worker", "decomposer", "repairer"). Stored as a node
    /// field so the orchestrator can read it from ready-nodes output.
    pub role: Option<String>,
}

/// Result returned by `callstack_load_plan`.
#[derive(Debug)]
pub struct LoadPlanResult {
    pub project: String,
    pub root: String,
    /// Ordered list of (title, node_path) for all created nodes.
    pub nodes: Vec<(String, String)>,
}

/// Load a pre-parsed plan into the callstack.
///
/// If `into_active` is true the nodes are created as children of the currently
/// active node; otherwise the current project (if any) is parked first and a
/// new project root is created.
pub fn callstack_load_plan(
    store: &mut AmsStore,
    project_name: &str,
    project_description: Option<&str>,
    node_defs: &[PlanNodeDef],
    actor_id: &str,
    into_active: bool,
    now: DateTime<FixedOffset>,
) -> Result<LoadPlanResult> {
    if node_defs.is_empty() {
        bail!("plan must have at least one node");
    }

    // Validate that depends_on relationships don't form a cycle.
    validate_dag(node_defs)?;

    let (project_root, root_path_value) = if into_active {
        let active = find_active_node(store, None)
            .ok_or_else(|| anyhow::anyhow!("no active callstack node to load plan into"))?;
        let root_path = resolve_runtime_root(&active.node_path, &active.fields);
        (active.node_path.clone(), root_path)
    } else {
        // Park the current project if one is active.
        if let Some(active) = find_active_node(store, None) {
            let root_path = resolve_runtime_root(&active.node_path, &active.fields);
            set_node_state(store, &active.node_path, "parked", actor_id, now)?;
            set_root_active_path(store, &root_path, "", actor_id, now)?;
        }

        // Create a new project root.
        let root_path = create_runtime_node(
            store,
            &CreateNodeParams {
                name: project_name,
                owner: actor_id,
                kind: "work",
                state: "active",
                next_command: "callstack push",
                parent_node_path: None,
                root_path: None,
                description: project_description,
                resume_policy: "next-sibling",
                extra_fields: Some(&BTreeMap::from([(
                    "active_node_path".to_string(),
                    String::new(),
                )])),
            },
            now,
        )?;
        set_root_active_path(store, &root_path, &root_path, actor_id, now)?;

        // Create the 00-node sub-bucket required by execution_roots() so that
        // this plan is visible to `swarm-plan list` and `swarm-plan switch`.
        let meta_path = node_meta_path(&root_path);
        create_smartlist_bucket(store, &meta_path, false, actor_id, now)?;
        let meta_fields = BTreeMap::from([
            ("path".to_string(), meta_path.clone()),
            ("display_name".to_string(), project_name.to_string()),
            ("title".to_string(), project_name.to_string()),
            ("state".to_string(), "active".to_string()),
            ("active_node_path".to_string(), root_path.clone()),
            ("parent_node_path".to_string(), String::new()),
            ("created_by".to_string(), actor_id.to_string()),
            ("node_path".to_string(), root_path.clone()),
            ("plan_mode".to_string(), "edit".to_string()),
        ]);
        set_smartlist_bucket_fields(store, &meta_path, &meta_fields, actor_id, now)?;

        let root_clone = root_path.clone();
        (root_path, root_clone)
    };

    // Create child nodes in "ready" state.
    let mut created: Vec<(String, String)> = Vec::new();
    let mut children_policy_set = false;
    for node_def in node_defs {
        let depends_on = node_def.depends_on.as_deref().unwrap_or("");
        let mut extra: BTreeMap<String, String> = BTreeMap::new();
        if !depends_on.is_empty() {
            extra.insert("depends_on".to_string(), depends_on.to_string());
        }
        if let Some(role) = &node_def.role {
            extra.insert("role".to_string(), role.clone());
        }

        let child_path = create_runtime_node(
            store,
            &CreateNodeParams {
                name: &node_def.title,
                owner: actor_id,
                kind: "work",
                state: "ready",
                next_command: "",
                parent_node_path: Some(&project_root),
                root_path: Some(&root_path_value),
                description: node_def.description.as_deref(),
                resume_policy: "next-sibling",
                extra_fields: if extra.is_empty() { None } else { Some(&extra) },
            },
            now,
        )?;
        created.push((node_def.title.clone(), child_path));

        // After the first child is created the parent's 10-children members
        // container exists; set graph_shape=dag on it exactly once so that
        // any future structural membership cycle is rejected at mutation time.
        if !children_policy_set {
            let children_members_id = format!(
                "smartlist-members:{}",
                node_children_path(&project_root)
            );
            // Ignore errors — the container may not exist in tests that mock
            // the store minimally; the depends_on cycle check above is the
            // primary guard.
            let _ = set_container_policy(store, &children_members_id, "graph_shape", "dag");
            children_policy_set = true;
        }
    }

    Ok(LoadPlanResult {
        project: project_name.to_string(),
        root: project_root,
        nodes: created,
    })
}

// ---------------------------------------------------------------------------
// DAG validation helper (extracted from callstack_load_plan)
// ---------------------------------------------------------------------------

fn validate_dag(node_defs: &[PlanNodeDef]) -> Result<()> {
    use std::collections::{HashMap, HashSet};
    let title_set: HashSet<&str> = node_defs.iter().map(|n| n.title.as_str()).collect();
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for node_def in node_defs {
        let deps: Vec<&str> = node_def
            .depends_on
            .as_deref()
            .unwrap_or("")
            .split(',')
            .map(|t| t.trim())
            .filter(|t| !t.is_empty() && title_set.contains(*t))
            .collect();
        adj.insert(node_def.title.as_str(), deps);
    }
    let mut colour: HashMap<&str, u8> = HashMap::new();
    fn dfs<'a>(
        node: &'a str,
        adj: &HashMap<&'a str, Vec<&'a str>>,
        colour: &mut HashMap<&'a str, u8>,
    ) -> bool {
        if colour.get(node) == Some(&1) {
            return true;
        }
        if colour.get(node) == Some(&2) {
            return false;
        }
        colour.insert(node, 1);
        for &dep in adj.get(node).map(|v| v.as_slice()).unwrap_or(&[]) {
            if dfs(dep, adj, colour) {
                return true;
            }
        }
        colour.insert(node, 2);
        false
    }
    for title in adj.keys().copied() {
        if dfs(title, &adj, &mut colour) {
            bail!(
                "plan has a circular depends_on cycle involving node '{}'; \
                 dag policy requires an acyclic dependency graph",
                title
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Mode transition commands
// ---------------------------------------------------------------------------

/// Transition the active (or named) project from edit mode to execute mode.
pub fn callstack_enter_execute(
    store: &mut AmsStore,
    project_opt: Option<&str>,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<CallstackOpResult> {
    let roots = execution_roots(store);
    let slug = project_opt.map(|p| slugify(p));
    let relevant: Vec<(String, BTreeMap<String, String>)> = if let Some(ref s) = slug {
        roots.into_iter().filter(|(p, _)| last_path_segment(p) == s.as_str()).collect()
    } else {
        roots
    };

    let active_root = relevant.into_iter().find(|(_, f)| {
        !f.get("active_node_path").map(|s| s.trim()).unwrap_or("").is_empty()
    });
    let (root_path, root_fields) = active_root
        .ok_or_else(|| anyhow::anyhow!("no active project to enter execute mode"))?;

    let current_mode = root_fields.get("plan_mode").map(|s| s.trim()).unwrap_or("execute");
    if current_mode != "edit" {
        return Ok(CallstackOpResult::new("no-op")
            .field("plan_mode", "execute")
            .field("root_path", &root_path));
    }

    let mut fields: BTreeMap<String, String> = BTreeMap::new();
    fields.insert("plan_mode".to_string(), "execute".to_string());

    // Restore cursor from pre_edit_cursor if needed
    let active_path = root_fields
        .get("active_node_path")
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    if active_path.is_empty() {
        let pre_cursor = root_fields
            .get("pre_edit_cursor")
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        if !pre_cursor.is_empty() {
            fields.insert("active_node_path".to_string(), pre_cursor.clone());
            set_node_state(store, &pre_cursor, "active", actor_id, now)?;
        } else {
            fields.insert("active_node_path".to_string(), root_path.clone());
            set_node_state(store, &root_path, "active", actor_id, now)?;
        }
    }

    set_node_fields(store, &root_path, &fields, actor_id, now)?;

    Ok(CallstackOpResult::new("enter-execute")
        .field("root_path", &root_path)
        .field("plan_mode", "execute"))
}

/// Transition the active (or named) project from execute mode to edit mode.
/// Parks the execution cursor so the plan can be safely restructured.
pub fn callstack_enter_edit(
    store: &mut AmsStore,
    project_opt: Option<&str>,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<CallstackOpResult> {
    let roots = execution_roots(store);
    let slug = project_opt.map(|p| slugify(p));
    let relevant: Vec<(String, BTreeMap<String, String>)> = if let Some(ref s) = slug {
        roots.into_iter().filter(|(p, _)| last_path_segment(p) == s.as_str()).collect()
    } else {
        roots
    };

    let active_root = relevant.into_iter().find(|(_, f)| {
        !f.get("active_node_path").map(|s| s.trim()).unwrap_or("").is_empty()
    });
    let (root_path, root_fields) = active_root
        .ok_or_else(|| anyhow::anyhow!("no active project to enter edit mode"))?;

    let current_mode = root_fields.get("plan_mode").map(|s| s.trim()).unwrap_or("execute");
    if current_mode == "edit" {
        return Ok(CallstackOpResult::new("no-op")
            .field("plan_mode", "edit")
            .field("root_path", &root_path));
    }

    let active_path = root_fields
        .get("active_node_path")
        .map(|s| s.trim().to_string())
        .unwrap_or_default();

    // Park execution cursor
    if !active_path.is_empty() {
        set_node_state(store, &active_path, "parked", actor_id, now)?;
    }

    let mut fields: BTreeMap<String, String> = BTreeMap::new();
    fields.insert("active_node_path".to_string(), String::new());
    fields.insert("pre_edit_cursor".to_string(), active_path.clone());
    fields.insert("plan_mode".to_string(), "edit".to_string());
    set_node_fields(store, &root_path, &fields, actor_id, now)?;

    let mut result = CallstackOpResult::new("enter-edit")
        .field("root_path", &root_path)
        .field("plan_mode", "edit");
    if !active_path.is_empty() {
        result = result.field("parked_cursor", &active_path);
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Structural edit commands (require edit mode)
// ---------------------------------------------------------------------------

/// Rename a node's title field and update depends_on references in siblings.
/// Note: the path segment is NOT renamed — only the title display field changes.
pub fn callstack_rename_node(
    store: &mut AmsStore,
    node_path: &str,
    new_title: &str,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<CallstackOpResult> {
    let fields = bucket_fields(store, &node_meta_path(node_path));
    if fields.is_empty() {
        bail!("node not found at {node_path}");
    }
    let old_title = fields.get("title").cloned().unwrap_or_default();

    let update = BTreeMap::from([("title".to_string(), new_title.to_string())]);
    set_node_fields(store, node_path, &update, actor_id, now)?;

    // Update depends_on in siblings
    let parent = fields
        .get("parent_node_path")
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    if !parent.is_empty() && !old_title.is_empty() {
        let siblings = iter_children(store, &parent);
        for sib in &siblings {
            if sib == node_path {
                continue;
            }
            let sib_fields = bucket_fields(store, &node_meta_path(sib));
            let dep_csv = sib_fields.get("depends_on").cloned().unwrap_or_default();
            if dep_csv.is_empty() {
                continue;
            }
            let new_dep_csv: String = dep_csv
                .split(',')
                .map(|t| {
                    let t = t.trim();
                    if t == old_title { new_title } else { t }
                })
                .collect::<Vec<_>>()
                .join(", ");
            if new_dep_csv != dep_csv {
                let sib_update = BTreeMap::from([("depends_on".to_string(), new_dep_csv)]);
                set_node_fields(store, sib, &sib_update, actor_id, now)?;
            }
        }
    }

    Ok(CallstackOpResult::new("rename-node")
        .field("node_path", node_path)
        .field("old_title", &old_title)
        .field("new_title", new_title))
}

/// Delete a leaf node (no children). Detaches from parent and soft-deletes.
/// Sweeps siblings' depends_on to remove references to the deleted title.
pub fn callstack_delete_node(
    store: &mut AmsStore,
    node_path: &str,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<CallstackOpResult> {
    let fields = bucket_fields(store, &node_meta_path(node_path));
    if fields.is_empty() {
        bail!("node not found at {node_path}");
    }

    let children = iter_children(store, node_path);
    if !children.is_empty() {
        bail!("cannot delete node with children; delete children first");
    }

    let parent = fields
        .get("parent_node_path")
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    if parent.is_empty() {
        bail!("cannot delete root node");
    }

    let title = fields.get("title").cloned().unwrap_or_default();

    // Detach from parent's 10-children container
    smartlist_detach(store, &node_children_path(&parent), node_path, actor_id, now)?;

    // Soft-delete
    let del_fields = BTreeMap::from([("state".to_string(), "deleted".to_string())]);
    set_node_fields(store, node_path, &del_fields, actor_id, now)?;

    // Sweep siblings' depends_on
    if !title.is_empty() {
        let siblings = iter_children(store, &parent);
        for sib in &siblings {
            let sib_fields = bucket_fields(store, &node_meta_path(sib));
            let dep_csv = sib_fields.get("depends_on").cloned().unwrap_or_default();
            if dep_csv.is_empty() {
                continue;
            }
            let new_deps: Vec<&str> = dep_csv
                .split(',')
                .map(|t| t.trim())
                .filter(|t| !t.is_empty() && *t != title.as_str())
                .collect();
            let new_dep_csv = new_deps.join(", ");
            if new_dep_csv != dep_csv {
                let sib_update = BTreeMap::from([("depends_on".to_string(), new_dep_csv)]);
                set_node_fields(store, sib, &sib_update, actor_id, now)?;
            }
        }
    }

    Ok(CallstackOpResult::new("delete-node")
        .field("node_path", node_path)
        .field("title", &title)
        .field("parent_path", &parent))
}

/// Set (replace) the depends_on CSV for a node.
/// Validates each title exists as a sibling and runs cycle detection.
pub fn callstack_set_depends_on(
    store: &mut AmsStore,
    node_path: &str,
    depends_on_csv: &str,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<CallstackOpResult> {
    let fields = bucket_fields(store, &node_meta_path(node_path));
    if fields.is_empty() {
        bail!("node not found at {node_path}");
    }

    let parent = fields
        .get("parent_node_path")
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    if parent.is_empty() {
        bail!("cannot set depends_on on root node");
    }

    let siblings = iter_children(store, &parent);
    let dep_titles: Vec<&str> = depends_on_csv
        .split(',')
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .collect();

    // Validate dep titles exist as siblings
    for dep in &dep_titles {
        let found = siblings.iter().filter(|s| *s != node_path).any(|sib| {
            let sib_fields = bucket_fields(store, &node_meta_path(sib));
            sib_fields.get("title").map(|t| t == *dep).unwrap_or(false)
        });
        if !found {
            bail!("depends_on title '{}' is not a sibling of {}", dep, node_path);
        }
    }

    // Build node list for cycle detection
    let title = fields
        .get("title")
        .cloned()
        .unwrap_or_else(|| last_path_segment(node_path).to_string());
    let mut all_nodes: Vec<PlanNodeDef> = vec![PlanNodeDef {
        title: title.clone(),
        description: None,
        depends_on: if dep_titles.is_empty() {
            None
        } else {
            Some(dep_titles.join(", "))
        },
        role: None,
    }];
    for sib in &siblings {
        if sib == node_path {
            continue;
        }
        let sib_fields = bucket_fields(store, &node_meta_path(sib));
        let sib_title = sib_fields.get("title").cloned().unwrap_or_default();
        let sib_deps = sib_fields
            .get("depends_on")
            .cloned()
            .filter(|s| !s.is_empty());
        all_nodes.push(PlanNodeDef { title: sib_title, description: None, depends_on: sib_deps, role: None });
    }
    validate_dag(&all_nodes)?;

    let update = BTreeMap::from([("depends_on".to_string(), dep_titles.join(", "))]);
    set_node_fields(store, node_path, &update, actor_id, now)?;

    Ok(CallstackOpResult::new("set-depends-on")
        .field("node_path", node_path)
        .field("depends_on", depends_on_csv))
}

/// Move a node to a new parent within the same project.
/// Clears depends_on on the moved node (old sibling refs are invalid in new context).
pub fn callstack_move_node(
    store: &mut AmsStore,
    node_path: &str,
    new_parent_path: &str,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<CallstackOpResult> {
    let fields = bucket_fields(store, &node_meta_path(node_path));
    if fields.is_empty() {
        bail!("node not found at {node_path}");
    }
    let new_parent_fields = bucket_fields(store, &node_meta_path(new_parent_path));
    if new_parent_fields.is_empty() {
        bail!("new parent node not found at {new_parent_path}");
    }

    let old_parent = fields
        .get("parent_node_path")
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    if old_parent.is_empty() {
        bail!("cannot move root node");
    }
    if old_parent == new_parent_path {
        bail!("node is already a child of {new_parent_path}");
    }

    // Verify same root
    let node_root = fields
        .get("root_path")
        .filter(|v| !v.is_empty())
        .cloned()
        .unwrap_or_else(|| node_path.to_string());
    let parent_root = new_parent_fields
        .get("root_path")
        .filter(|v| !v.is_empty())
        .cloned()
        .unwrap_or_else(|| new_parent_path.to_string());
    if node_root != parent_root {
        bail!("cross-project moves not supported");
    }

    // Cycle guard: walk up from new_parent to check node_path is not an ancestor
    let mut check = new_parent_path.to_string();
    let mut visited = std::collections::HashSet::new();
    loop {
        if check.is_empty() || !visited.insert(check.clone()) {
            break;
        }
        if check == node_path {
            bail!("cannot move node into its own subtree (cycle)");
        }
        let check_fields = bucket_fields(store, &node_meta_path(&check));
        check = check_fields
            .get("parent_node_path")
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
    }

    // Move node from old parent's 10-children to new parent's 10-children
    smartlist_move(
        store,
        &node_children_path(&old_parent),
        &node_children_path(new_parent_path),
        node_path,
        None,
        actor_id,
        now,
    )?;

    // Update parent_node_path and clear depends_on
    let mut update = BTreeMap::new();
    update.insert("parent_node_path".to_string(), new_parent_path.to_string());
    update.insert("depends_on".to_string(), String::new());
    set_node_fields(store, node_path, &update, actor_id, now)?;

    Ok(CallstackOpResult::new("move-node")
        .field("node_path", node_path)
        .field("old_parent", &old_parent)
        .field("new_parent", new_parent_path)
        .field("warning", "depends_on-cleared"))
}

// ---------------------------------------------------------------------------
// Quarantined mutation (safe subset for execute mode)
// ---------------------------------------------------------------------------

/// Push a new child node in "ready" state without touching the execution cursor.
/// Safe to call during live execution — does not advance or change the active node.
/// Logged with tool_name "quarantined:swarm-plan-push" for grep-ability.
pub fn callstack_quarantined_push(
    store: &mut AmsStore,
    name: &str,
    description: Option<&str>,
    parent_node_path_opt: Option<&str>,
    depends_on_opt: Option<&str>,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<CallstackOpResult> {
    assert_mode(store, None, "execute", "swarm-plan-quarantined-push")?;

    let active = find_active_node(store, None)
        .ok_or_else(|| anyhow::anyhow!("no active callstack node for quarantined-push"))?;

    let parent_path = parent_node_path_opt.unwrap_or(&active.node_path);
    let root_path = resolve_runtime_root(&active.node_path, &active.fields);

    let mut extra: BTreeMap<String, String> = BTreeMap::new();
    if let Some(deps) = depends_on_opt {
        if !deps.is_empty() {
            extra.insert("depends_on".to_string(), deps.to_string());
        }
    }

    let child_path = create_runtime_node(
        store,
        &CreateNodeParams {
            name,
            owner: actor_id,
            kind: "work",
            state: "ready",
            next_command: "",
            parent_node_path: Some(parent_path),
            root_path: Some(&root_path),
            description,
            resume_policy: "next-sibling",
            extra_fields: if extra.is_empty() { None } else { Some(&extra) },
        },
        now,
    )?;

    record_tool_call(store, "quarantined:swarm-plan-push", false, &child_path, actor_id, now)?;

    Ok(CallstackOpResult::new("quarantined-push")
        .field("node_path", &child_path)
        .field("parent_path", parent_path)
        .field("kind", "work"))
}

// ---------------------------------------------------------------------------
// Tool-call recording
// ---------------------------------------------------------------------------

/// Record a tool call as an ObjectRecord with `object_kind = "tool-call"`.
///
/// The provenance fields match what `fep_bootstrap.rs` expects:
/// - `tool_name`      — the CLI subcommand name (e.g. `"swarm-plan-push"`)
/// - `is_error`       — bool: whether the call failed
/// - `result_preview` — first 200 chars of the output / error text
/// - `ts`             — ISO 8601 timestamp of the call
/// - `actor_id`       — the acting agent id
pub fn record_tool_call(
    store: &mut AmsStore,
    tool_name: &str,
    is_error: bool,
    result_preview: &str,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<()> {
    record_tool_call_with_duration(store, tool_name, is_error, result_preview, actor_id, now, None)
}

pub fn record_tool_call_with_duration(
    store: &mut AmsStore,
    tool_name: &str,
    is_error: bool,
    result_preview: &str,
    actor_id: &str,
    now: DateTime<FixedOffset>,
    duration_s: Option<f64>,
) -> Result<()> {
    use uuid::Uuid;
    let object_id = format!("tool-call:{}", Uuid::new_v4().simple());
    let preview: String = result_preview.chars().take(200).collect();
    let mut prov: JsonMap = JsonMap::new();
    prov.insert("tool_name".to_string(), serde_json::Value::String(tool_name.to_string()));
    prov.insert("is_error".to_string(), serde_json::Value::Bool(is_error));
    prov.insert("result_preview".to_string(), serde_json::Value::String(preview));
    prov.insert("ts".to_string(), serde_json::Value::String(now.to_rfc3339()));
    prov.insert("actor_id".to_string(), serde_json::Value::String(actor_id.to_string()));
    if let Some(d) = duration_s {
        prov.insert("duration_s".to_string(), serde_json::Value::from(d));
    }
    let sp = SemanticPayload { provenance: Some(prov), ..Default::default() };
    store.upsert_object(object_id, "tool-call", None, Some(sp), Some(now))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Batch operations
// ---------------------------------------------------------------------------

/// A single operation in a batch request.
#[derive(Debug, Deserialize)]
#[serde(tag = "cmd", rename_all = "kebab-case")]
pub enum BatchOp {
    Push {
        name: String,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        depends_on: Option<String>,
    },
    Pop {
        #[serde(default)]
        return_text: Option<String>,
    },
    Observe {
        title: String,
        text: String,
    },
    CompleteNode {
        node_path: String,
        #[serde(default)]
        return_text: Option<String>,
    },
    Advance,
    Resume,
    Switch {
        name: String,
    },
    Park {
        #[serde(default)]
        project: Option<String>,
    },
    Interrupt {
        #[serde(default = "default_repair")]
        policy: String,
        #[serde(default)]
        reason: String,
        #[serde(default)]
        error_output: String,
        #[serde(default)]
        context: String,
        #[serde(default)]
        attempted_fix: String,
        #[serde(default)]
        repair_hint: String,
        #[serde(default)]
        subtask_hints: String,
    },
    QuarantinedPush {
        name: String,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        depends_on: Option<String>,
        #[serde(default)]
        parent_node_path: Option<String>,
    },
}

fn default_repair() -> String {
    "repair".to_string()
}

/// Execute a batch of callstack operations sequentially against a single store.
/// Fail-fast: if any op fails, return the error (prior ops are already applied).
pub fn run_batch(
    store: &mut AmsStore,
    ops: &[BatchOp],
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<Vec<CallstackOpResult>> {
    let mut results = Vec::with_capacity(ops.len());
    for op in ops {
        let res = match op {
            BatchOp::Push { name, description, depends_on } => {
                callstack_push(store, name, description.as_deref(), actor_id, depends_on.as_deref(), now)?
            }
            BatchOp::Pop { return_text } => {
                callstack_pop(store, return_text.as_deref(), actor_id, now)?
            }
            BatchOp::Observe { title, text } => {
                callstack_observe(store, title, text, actor_id, now)?
            }
            BatchOp::CompleteNode { node_path, return_text } => {
                callstack_complete_node(store, node_path, return_text.as_deref(), actor_id, now)?
            }
            BatchOp::Advance => {
                callstack_advance(store, actor_id, now)?
            }
            BatchOp::Resume => {
                callstack_resume(store, actor_id, now)?
            }
            BatchOp::Switch { name } => {
                let active = switch_project(store, name, actor_id, now)?;
                CallstackOpResult::new("switch").field("active_node_path", &active)
            }
            BatchOp::Park { project } => {
                let parked = park_project(store, project.as_deref(), actor_id, now)?;
                CallstackOpResult::new("park")
                    .field("parked", &parked.unwrap_or_default())
            }
            BatchOp::Interrupt {
                policy, reason, error_output, context,
                attempted_fix, repair_hint, subtask_hints,
            } => {
                callstack_interrupt(store, &InterruptParams {
                    actor_id,
                    policy_kind: policy,
                    reason,
                    error_output,
                    context,
                    attempted_fix,
                    repair_hint,
                    subtask_hints,
                }, now)?
            }
            BatchOp::QuarantinedPush { name, description, depends_on, parent_node_path } => {
                callstack_quarantined_push(
                    store,
                    name,
                    description.as_deref(),
                    parent_node_path.as_deref(),
                    depends_on.as_deref(),
                    actor_id,
                    now,
                )?
            }
        };
        record_tool_call(store, &format!("swarm-plan-batch:{}", res.action), false, &res.to_text(), actor_id, now)?;
        results.push(res);
    }
    Ok(results)
}

// ---------------------------------------------------------------------------
// repair_roots
// ---------------------------------------------------------------------------

/// Result of `repair_roots`.
#[derive(Debug)]
pub struct RepairRootsResult {
    /// Number of plans whose `00-node` was newly created.
    pub repaired: usize,
    /// Number of plans that already had a `00-node` (no action taken).
    pub already_ok: usize,
}

impl RepairRootsResult {
    pub fn to_text(&self) -> String {
        format!("repaired={} already_ok={}", self.repaired, self.already_ok)
    }
}

/// Scan the store for all top-level execution-plan roots that are missing a
/// `00-node` sub-bucket and create it retroactively.
///
/// Plans loaded via the old `load-plan` path may not have a `00-node` bucket,
/// making them invisible to `execution_roots()` and therefore to
/// `swarm-plan list` and `swarm-plan switch`.
///
/// A "top-level root" is any SmartList bucket at depth exactly 2 under
/// `smartlist/execution-plan/` (i.e. `smartlist/execution-plan/<name>` with
/// no further slashes in the `<name>` segment).
pub fn repair_roots(
    store: &mut AmsStore,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<RepairRootsResult> {
    let prefix = format!("{EXECUTION_PLAN_ROOT}/");
    let mut plan_names: Vec<String> = Vec::new();

    // Collect all top-level plan bucket paths.
    for (oid, obj) in store.objects() {
        if !oid.starts_with("smartlist-bucket:") {
            continue;
        }
        let Some(prov) = obj
            .semantic_payload
            .as_ref()
            .and_then(|sp| sp.provenance.as_ref())
        else {
            continue;
        };
        let Some(path) = prov.get("path").and_then(|v| v.as_str()) else {
            continue;
        };
        if !path.starts_with(&prefix) {
            continue;
        }
        // Depth-2: strip prefix, the remainder must be exactly one segment (no '/')
        let rest = &path[prefix.len()..];
        if rest.is_empty() || rest.contains('/') {
            continue;
        }
        plan_names.push(rest.to_string());
    }

    plan_names.sort();
    plan_names.dedup();

    let mut repaired = 0usize;
    let mut already_ok = 0usize;

    for name in &plan_names {
        let root_path = format!("{EXECUTION_PLAN_ROOT}/{name}");
        let meta_path = node_meta_path(&root_path);

        // Check if the 00-node bucket already exists.
        let existing = bucket_fields(store, &meta_path);
        if !existing.is_empty() {
            already_ok += 1;
            continue;
        }

        // Read provenance from the root bucket itself.
        let root_fields = bucket_fields(store, &root_path);
        let title = root_fields
            .get("title")
            .or_else(|| root_fields.get("display_name"))
            .cloned()
            .unwrap_or_else(|| name.clone());
        let created_by = root_fields
            .get("created_by")
            .or_else(|| root_fields.get("owner"))
            .cloned()
            .unwrap_or_else(|| actor_id.to_string());

        // Create the 00-node bucket.
        create_smartlist_bucket(store, &meta_path, false, actor_id, now)?;

        // Write provenance fields so execution_roots() can discover this plan.
        let fields = BTreeMap::from([
            ("path".to_string(), meta_path.clone()),
            ("display_name".to_string(), name.clone()),
            ("title".to_string(), title),
            ("state".to_string(), "parked".to_string()),
            ("active_node_path".to_string(), String::new()),
            ("parent_node_path".to_string(), String::new()),
            ("created_by".to_string(), created_by),
            ("node_path".to_string(), root_path.clone()),
        ]);
        set_smartlist_bucket_fields(store, &meta_path, &fields, actor_id, now)?;

        repaired += 1;
    }

    Ok(RepairRootsResult { repaired, already_ok })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::now_fixed;

    fn setup_store_with_root(name: &str) -> (AmsStore, String, DateTime<FixedOffset>) {
        let mut store = AmsStore::new();
        let now = now_fixed();
        let node_path = create_runtime_node(
            &mut store,
            &CreateNodeParams {
                name,
                owner: "test",
                kind: "work",
                state: "active",
                next_command: "callstack push",
                parent_node_path: None,
                root_path: None,
                description: Some("test root"),
                resume_policy: "next-sibling",
                extra_fields: Some(&BTreeMap::from([(
                    "active_node_path".to_string(),
                    String::new(),
                )])),
            },
            now,
        )
        .unwrap();
        // Set root active pointer to itself
        set_root_active_path(&mut store, &node_path, &node_path, "test", now).unwrap();
        (store, node_path, now)
    }

    #[test]
    fn test_slugify() {
        assert_eq!(slugify("Hello World!"), "hello-world");
        assert_eq!(slugify("  "), "node");
        assert_eq!(slugify("01-foo_bar"), "01-foo-bar");
    }

    #[test]
    fn test_create_and_find_active_node() {
        let (store, root_path, _) = setup_store_with_root("test-project");
        let active = find_active_node(&store, None).unwrap();
        assert_eq!(active.node_path, root_path);
    }

    #[test]
    fn test_active_path_frames() {
        let (mut store, root_path, now) = setup_store_with_root("test-project");
        let child_path = create_runtime_node(
            &mut store,
            &CreateNodeParams {
                name: "child-task",
                owner: "test",
                kind: "work",
                state: "active",
                next_command: "callstack push",
                parent_node_path: Some(&root_path),
                root_path: Some(&root_path),
                description: None,
                resume_policy: "next-sibling",
                extra_fields: None,
            },
            now,
        )
        .unwrap();
        set_root_active_path(&mut store, &root_path, &child_path, "test", now).unwrap();

        let frames = active_path_frames(&store, &child_path);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].node_path, root_path);
        assert_eq!(frames[1].node_path, child_path);
    }

    #[test]
    fn test_list_projects() {
        let (store, _, _) = setup_store_with_root("my-project");
        let projects = list_projects(&store);
        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0].name, "my-project");
    }

    #[test]
    fn test_render_context() {
        let (store, _, _) = setup_store_with_root("ctx-project");
        let ctx = render_context(&store, None).unwrap();
        assert_eq!(ctx.frames.len(), 1);
        assert_eq!(ctx.node_kind, "root");
        assert!(ctx.observations.len() >= 1); // description note
    }

    #[test]
    fn test_park_and_switch() {
        let (mut store, root_path, now) = setup_store_with_root("proj-a");

        // Park
        let parked = park_project(&mut store, None, "test", now).unwrap();
        assert_eq!(parked, Some("proj-a".to_string()));
        assert!(find_active_node(&store, None).is_none());

        // Switch back
        let active = switch_project(&mut store, "proj-a", "test", now).unwrap();
        assert_eq!(active, root_path);
        assert!(find_active_node(&store, None).is_some());
    }

    #[test]
    fn test_render_context_text_format() {
        let (store, _, _) = setup_store_with_root("fmt-project");
        let text = render_context_text(&store, None, 10000).unwrap();
        assert!(text.starts_with("[AMS Callstack Context]"));
        assert!(text.ends_with("[End callstack context]"));
        assert!(text.contains("node_kind=root"));
    }

    #[test]
    fn test_load_plan_creates_nodes() {
        let mut store = AmsStore::new();
        let now = chrono::Utc::now().fixed_offset();

        let node_defs = vec![
            PlanNodeDef { title: "step-a".to_string(), description: Some("First step".to_string()), depends_on: None, role: None },
            PlanNodeDef { title: "step-b".to_string(), description: None, depends_on: Some("step-a".to_string()), role: None },
        ];

        let result = callstack_load_plan(
            &mut store,
            "test-plan",
            Some("A test plan"),
            &node_defs,
            "test-actor",
            false,
            now,
        ).unwrap();

        assert_eq!(result.project, "test-plan");
        assert_eq!(result.nodes.len(), 2);
        assert_eq!(result.nodes[0].0, "step-a");
        assert_eq!(result.nodes[1].0, "step-b");

        // Root should exist and be active
        let active = find_active_node(&store, None);
        assert!(active.is_some());

        // step-b should have depends_on field
        let step_b_path = &result.nodes[1].1;
        let fields = bucket_fields(&store, &node_meta_path(step_b_path));
        assert_eq!(fields.get("depends_on").map(|s| s.as_str()), Some("step-a"));
        assert_eq!(fields.get("state").map(|s| s.as_str()), Some("ready"));
    }

    #[test]
    fn test_load_plan_rejects_circular_depends_on() {
        let mut store = AmsStore::new();
        let now = chrono::Utc::now().fixed_offset();

        // A depends_on B and B depends_on A — a direct cycle.
        let node_defs = vec![
            PlanNodeDef {
                title: "node-a".to_string(),
                description: None,
                depends_on: Some("node-b".to_string()),
                role: None,
            },
            PlanNodeDef {
                title: "node-b".to_string(),
                description: None,
                depends_on: Some("node-a".to_string()),
                role: None,
            },
        ];

        let result = callstack_load_plan(
            &mut store,
            "cycle-test",
            None,
            &node_defs,
            "test-actor",
            false,
            now,
        );

        assert!(result.is_err(), "expected Err for circular depends_on, got Ok");
        let msg = result.unwrap_err().to_string().to_lowercase();
        assert!(
            msg.contains("cycle") || msg.contains("dag"),
            "error message should mention 'cycle' or 'dag', got: {}",
            msg
        );
    }

    #[test]
    fn test_load_plan_visible_in_list_projects() {
        // Acceptance test for P8R-R1: callstack_load_plan must create a 00-node
        // sub-bucket for the new plan root so that execution_roots() / list_projects()
        // can discover the plan.
        let mut store = AmsStore::new();
        let now = chrono::Utc::now().fixed_offset();

        let node_defs = vec![
            PlanNodeDef { title: "task-one".to_string(), description: None, depends_on: None, role: None },
            PlanNodeDef { title: "task-two".to_string(), description: None, depends_on: Some("task-one".to_string()), role: None },
        ];

        let result = callstack_load_plan(
            &mut store,
            "my-new-plan",
            Some("Test plan for list_projects visibility"),
            &node_defs,
            "test-actor",
            false,
            now,
        ).unwrap();

        // The plan root should have a 00-node sub-bucket with empty parent_node_path,
        // so execution_roots() can find it and list_projects() returns the plan.
        let projects = list_projects(&store);
        assert_eq!(projects.len(), 1, "list_projects should return exactly one project");
        assert_eq!(projects[0].name, "my-new-plan");
        assert_eq!(projects[0].root_path, result.root);

        // The 00-node bucket must exist at the expected path and have the fields
        // that execution_roots() requires to discover this plan root.
        let meta_path = node_meta_path(&result.root);
        let fields = bucket_fields(&store, &meta_path);
        assert!(!fields.is_empty(), "00-node must have provenance fields");
        // execution_roots() skips any 00-node whose parent_node_path is non-empty.
        assert_eq!(fields.get("parent_node_path").map(|s| s.as_str()), Some(""),
            "root 00-node must have empty parent_node_path for execution_roots() to find it");
        // execution_roots() derives root_path by stripping "/00-node" from the path field.
        let expected_meta_path = format!("{}/00-node", result.root);
        assert_eq!(fields.get("path").map(|s| s.as_str()), Some(expected_meta_path.as_str()),
            "00-node provenance must have path set to the full 00-node bucket path");
        // The state and active_node_path fields must be present for find_active_node / list_projects.
        assert_eq!(fields.get("state").map(|s| s.as_str()), Some("active"),
            "root 00-node must have state=active");
        assert!(fields.contains_key("active_node_path"),
            "root 00-node must carry the active_node_path pointer field");
    }

    #[test]
    fn test_load_plan_dag_policy_set_on_children_container() {
        use crate::model::GraphShape;
        let mut store = AmsStore::new();
        let now = chrono::Utc::now().fixed_offset();

        let node_defs = vec![
            PlanNodeDef { title: "step-a".to_string(), description: None, depends_on: None, role: None },
            PlanNodeDef { title: "step-b".to_string(), description: None, depends_on: Some("step-a".to_string()), role: None },
        ];

        let result = callstack_load_plan(
            &mut store,
            "dag-policy-test",
            None,
            &node_defs,
            "test-actor",
            false,
            now,
        ).unwrap();

        let children_members_id = format!(
            "smartlist-members:{}/10-children",
            result.root
        );
        let policies = store.containers().get(&children_members_id)
            .map(|c| c.policies.graph_shape.clone());
        assert_eq!(
            policies,
            Some(GraphShape::Dag),
            "10-children members container should have graph_shape=dag"
        );
    }

    #[test]
    fn dag_policy_rejects_cycle_in_depends_on() {
        // P1d: DAG acyclicity enforced by policy engine
        // Create a plan with nodes A → B → C (depends_on chain) and verify
        // that attempting to set C → A (which would form a cycle) is rejected.
        let now = now_fixed();
        let mut store = AmsStore::new();

        let node_defs = vec![
            PlanNodeDef { title: "node-a".to_string(), description: None, depends_on: None, role: None },
            PlanNodeDef { title: "node-b".to_string(), description: None, depends_on: Some("node-a".to_string()), role: None },
            PlanNodeDef { title: "node-c".to_string(), description: None, depends_on: Some("node-b".to_string()), role: None },
        ];

        let result = callstack_load_plan(
            &mut store,
            "dag-cycle-test",
            None,
            &node_defs,
            "test-actor",
            false,
            now,
        ).unwrap();

        // Build the path for node-a
        let node_a_path = format!("{}/10-children/node-a", result.root);

        // Attempt to set node-a depends_on node-c — this would create a cycle: a→c→b→a
        let cycle_result = callstack_set_depends_on(
            &mut store,
            &node_a_path,
            "node-c",
            "test-actor",
            now,
        );

        assert!(
            cycle_result.is_err(),
            "Setting depends_on that creates a cycle must be rejected"
        );
        let err_msg = cycle_result.unwrap_err().to_string();
        assert!(
            err_msg.contains("cycle"),
            "Error message should mention 'cycle', got: {err_msg}"
        );
    }

    #[test]
    fn test_repair_roots_creates_missing_00_node() {
        let now = now_fixed();
        let mut store = AmsStore::new();

        // Simulate a plan root created without a 00-node (old load-plan path):
        // just create the root bucket at smartlist/execution-plan/legacy-plan
        // without any sub-buckets.
        let root_path = format!("{EXECUTION_PLAN_ROOT}/legacy-plan");
        create_smartlist_bucket(&mut store, &root_path, false, "old-actor", now).unwrap();
        let root_fields = BTreeMap::from([
            ("title".to_string(), "legacy-plan".to_string()),
            ("owner".to_string(), "old-actor".to_string()),
        ]);
        set_smartlist_bucket_fields(&mut store, &root_path, &root_fields, "old-actor", now).unwrap();

        // Before repair: list_projects finds nothing (no 00-node)
        assert_eq!(list_projects(&store).len(), 0);

        // Run repair
        let result = repair_roots(&mut store, "recovery", now).unwrap();
        assert_eq!(result.repaired, 1);
        assert_eq!(result.already_ok, 0);

        // After repair: list_projects finds the plan
        let projects = list_projects(&store);
        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0].name, "legacy-plan");

        // Running repair again is idempotent
        let result2 = repair_roots(&mut store, "recovery", now).unwrap();
        assert_eq!(result2.repaired, 0);
        assert_eq!(result2.already_ok, 1);
        assert_eq!(list_projects(&store).len(), 1);
    }

    #[test]
    fn test_repair_roots_skips_existing_00_node() {
        let (store, _, now) = setup_store_with_root("already-good");
        let mut store = store;
        // Already has 00-node from create_runtime_node
        let result = repair_roots(&mut store, "recovery", now).unwrap();
        assert_eq!(result.repaired, 0);
        assert_eq!(result.already_ok, 1);
    }
}
