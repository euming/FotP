//! BugReport and BugFix module — ported from tools/memoryctl/src/smartlist/BugReportService.cs.
//!
//! Stores bug reports and fix recipes as smartlist objects in the AMS store,
//! with cross-referencing between reports and fixes for FEP learning.

use std::collections::BTreeMap;

use anyhow::{bail, Result};
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::model::{JsonMap, ObjectRecord, SemanticPayload};
use crate::smartlist_write::{
    attach_member as smartlist_attach, create_bucket as create_smartlist_bucket,
    RETRIEVAL_VISIBILITY_DEFAULT, RETRIEVAL_VISIBILITY_KEY,
};
use crate::store::AmsStore;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const BUGREPORT_OBJECT_KIND: &str = "smartlist_bugreport";
pub const BUGFIX_OBJECT_KIND: &str = "smartlist_bugfix";
pub const DEFAULT_BUCKET_PATH: &str = "smartlist/bug-reports";
pub const BUGFIX_BUCKET_PATH: &str = "smartlist/bug-fixes";

pub const STATUS_OPEN: &str = "open";
pub const STATUS_IN_REPAIR: &str = "in-repair";
pub const STATUS_RESOLVED: &str = "resolved";

pub const SEVERITY_CRITICAL: &str = "critical";
pub const SEVERITY_HIGH: &str = "high";
pub const SEVERITY_MEDIUM: &str = "medium";
pub const SEVERITY_LOW: &str = "low";

const VALID_STATUSES: &[&str] = &[STATUS_OPEN, STATUS_IN_REPAIR, STATUS_RESOLVED];
const VALID_SEVERITIES: &[&str] = &[SEVERITY_CRITICAL, SEVERITY_HIGH, SEVERITY_MEDIUM, SEVERITY_LOW];

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BugReportInfo {
    pub bug_id: String,
    pub source_agent: String,
    pub parent_agent: String,
    pub error_output: String,
    pub stack_context: String,
    pub attempted_fixes: Vec<String>,
    pub reproduction_steps: Vec<String>,
    pub recommended_fix_plan: String,
    pub severity: String,
    pub status: String,
    pub durability: String,
    pub retrieval_visibility: String,
    pub bucket_paths: Vec<String>,
    pub created_at: DateTime<FixedOffset>,
    pub resolved_at: Option<DateTime<FixedOffset>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BugFixInfo {
    pub fix_id: String,
    pub title: String,
    pub description: String,
    pub fix_recipe: String,
    pub linked_bugreport_ids: Vec<String>,
    pub status: String,
    pub durability: String,
    pub bucket_paths: Vec<String>,
    pub created_at: DateTime<FixedOffset>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn normalize_status(status: &str) -> Result<String> {
    let lower = status.trim().to_ascii_lowercase();
    if VALID_STATUSES.contains(&lower.as_str()) {
        Ok(lower)
    } else {
        bail!(
            "Invalid status '{}'. Expected: {}",
            status,
            VALID_STATUSES.join(", ")
        )
    }
}

fn normalize_severity(severity: &str) -> Result<String> {
    let lower = severity.trim().to_ascii_lowercase();
    if VALID_SEVERITIES.contains(&lower.as_str()) {
        Ok(lower)
    } else {
        bail!(
            "Invalid severity '{}'. Expected: {}",
            severity,
            VALID_SEVERITIES.join(", ")
        )
    }
}

fn truncate_for_summary(text: &str) -> &str {
    let text = text.trim();
    if text.is_empty() {
        return "(empty)";
    }
    let first_line = text.split('\n').next().unwrap_or(text).trim();
    if first_line.len() > 120 {
        &first_line[..117]
    } else {
        first_line
    }
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
        Value::String(s) => Some(s.clone()),
        v => Some(v.to_string()),
    }
}

fn read_string_list(map: Option<&JsonMap>, key: &str) -> Vec<String> {
    let Some(Value::Array(arr)) = map.and_then(|m| m.get(key)) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect()
}

fn read_date(map: Option<&JsonMap>, key: &str) -> Option<DateTime<FixedOffset>> {
    let Value::String(raw) = map?.get(key)? else {
        return None;
    };
    DateTime::parse_from_rfc3339(raw).ok()
}

fn bucket_paths_for_object(store: &AmsStore, object_id: &str) -> Vec<String> {
    let prefix = "smartlist-members:";
    let mut paths: Vec<String> = store
        .containers_for_member_object(object_id)
        .into_iter()
        .filter(|id| id.starts_with(prefix))
        .map(|id| id[prefix.len()..].to_string())
        .collect();
    paths.sort();
    paths.dedup();
    paths
}

fn to_json_string_list(items: &[String]) -> Value {
    Value::Array(items.iter().map(|s| Value::String(s.clone())).collect())
}

// ---------------------------------------------------------------------------
// BugReport CRUD
// ---------------------------------------------------------------------------

pub struct CreateBugReportParams<'a> {
    pub source_agent: &'a str,
    pub parent_agent: &'a str,
    pub error_output: &'a str,
    pub stack_context: &'a str,
    pub attempted_fixes: Vec<String>,
    pub reproduction_steps: Vec<String>,
    pub recommended_fix_plan: &'a str,
    pub severity: &'a str,
    pub durable: bool,
    pub created_by: &'a str,
}

pub fn create_bugreport(
    store: &mut AmsStore,
    params: &CreateBugReportParams,
    now: DateTime<FixedOffset>,
) -> Result<BugReportInfo> {
    let severity = normalize_severity(params.severity)?;
    let durability = if params.durable { "durable" } else { "short_term" };

    // Ensure global registry bucket
    create_smartlist_bucket(store, DEFAULT_BUCKET_PATH, params.durable, params.created_by, now)?;

    let bug_id = format!("smartlist-bugreport:{}", Uuid::new_v4().as_simple());

    store.upsert_object(
        bug_id.clone(),
        BUGREPORT_OBJECT_KIND,
        None,
        Some(SemanticPayload {
            summary: Some(format!("Bug: {}", truncate_for_summary(params.error_output))),
            tags: Some(vec![
                BUGREPORT_OBJECT_KIND.to_string(),
                durability.to_string(),
                severity.clone(),
                STATUS_OPEN.to_string(),
            ]),
            ..Default::default()
        }),
        Some(now),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    {
        let obj = store
            .objects_mut()
            .get_mut(&bug_id)
            .expect("just upserted");
        let prov = ensure_prov(obj);
        prov.insert("bug_id".into(), Value::String(bug_id.clone()));
        prov.insert("source_agent".into(), Value::String(params.source_agent.trim().into()));
        prov.insert("parent_agent".into(), Value::String(params.parent_agent.trim().into()));
        prov.insert("error_output".into(), Value::String(params.error_output.trim().into()));
        prov.insert("stack_context".into(), Value::String(params.stack_context.trim().into()));
        prov.insert("attempted_fixes".into(), to_json_string_list(&params.attempted_fixes));
        prov.insert("reproduction_steps".into(), to_json_string_list(&params.reproduction_steps));
        prov.insert("recommended_fix_plan".into(), Value::String(params.recommended_fix_plan.trim().into()));
        prov.insert("severity".into(), Value::String(severity));
        prov.insert("status".into(), Value::String(STATUS_OPEN.into()));
        prov.insert("durability".into(), Value::String(durability.into()));
        prov.insert(RETRIEVAL_VISIBILITY_KEY.into(), Value::String(RETRIEVAL_VISIBILITY_DEFAULT.into()));
        prov.insert("created_by".into(), Value::String(params.created_by.into()));
        prov.insert("created_at".into(), Value::String(now.to_rfc3339()));
    }

    // Attach to global registry
    smartlist_attach(store, DEFAULT_BUCKET_PATH, &bug_id, params.created_by, now)?;

    get_bugreport(store, &bug_id)
        .ok_or_else(|| anyhow::anyhow!("failed to read created bug report '{}'", bug_id))
}

pub fn update_bugreport_status(
    store: &mut AmsStore,
    bug_id: &str,
    new_status: &str,
    now: DateTime<FixedOffset>,
) -> Result<BugReportInfo> {
    let status = normalize_status(new_status)?;

    let obj = store
        .objects_mut()
        .get_mut(bug_id)
        .filter(|o| o.object_kind == BUGREPORT_OBJECT_KIND)
        .ok_or_else(|| anyhow::anyhow!("Unknown bug report '{}'", bug_id))?;

    let prov = ensure_prov(obj);
    prov.insert("status".into(), Value::String(status.clone()));
    if status == STATUS_RESOLVED {
        prov.insert("resolved_at".into(), Value::String(now.to_rfc3339()));
    }

    // Update tags: remove old statuses, add new one
    if let Some(ref mut sp) = obj.semantic_payload {
        if let Some(ref mut tags) = sp.tags {
            tags.retain(|t| !VALID_STATUSES.contains(&t.as_str()));
            tags.push(status);
        }
    }
    obj.updated_at = now;

    get_bugreport(store, bug_id)
        .ok_or_else(|| anyhow::anyhow!("failed to read updated bug report '{}'", bug_id))
}

pub fn get_bugreport(store: &AmsStore, bug_id: &str) -> Option<BugReportInfo> {
    let obj = store.objects().get(bug_id)?;
    if obj.object_kind != BUGREPORT_OBJECT_KIND {
        return None;
    }
    let prov = obj.semantic_payload.as_ref().and_then(|sp| sp.provenance.as_ref());
    Some(BugReportInfo {
        bug_id: bug_id.to_string(),
        source_agent: read_string(prov, "source_agent").unwrap_or_default(),
        parent_agent: read_string(prov, "parent_agent").unwrap_or_default(),
        error_output: read_string(prov, "error_output").unwrap_or_default(),
        stack_context: read_string(prov, "stack_context").unwrap_or_default(),
        attempted_fixes: read_string_list(prov, "attempted_fixes"),
        reproduction_steps: read_string_list(prov, "reproduction_steps"),
        recommended_fix_plan: read_string(prov, "recommended_fix_plan").unwrap_or_default(),
        severity: read_string(prov, "severity").unwrap_or_else(|| SEVERITY_MEDIUM.into()),
        status: read_string(prov, "status").unwrap_or_else(|| STATUS_OPEN.into()),
        durability: read_string(prov, "durability").unwrap_or_else(|| "short_term".into()),
        retrieval_visibility: read_string(prov, RETRIEVAL_VISIBILITY_KEY)
            .unwrap_or_else(|| RETRIEVAL_VISIBILITY_DEFAULT.into()),
        bucket_paths: bucket_paths_for_object(store, bug_id),
        created_at: read_date(prov, "created_at").unwrap_or(obj.created_at),
        resolved_at: read_date(prov, "resolved_at"),
    })
}

pub fn list_bugreports(store: &AmsStore, status_filter: Option<&str>) -> Vec<BugReportInfo> {
    let container_id = format!("smartlist-members:{DEFAULT_BUCKET_PATH}");
    let member_ids: Vec<String> = store
        .iterate_forward(&container_id)
        .into_iter()
        .map(|ln| ln.object_id.clone())
        .collect();

    let mut results = Vec::new();
    for mid in member_ids {
        let Some(obj) = store.objects().get(&mid) else { continue };
        if obj.object_kind != BUGREPORT_OBJECT_KIND {
            continue;
        }
        let Some(report) = get_bugreport(store, &mid) else { continue };
        if let Some(filter) = status_filter {
            if !report.status.eq_ignore_ascii_case(filter) {
                continue;
            }
        }
        results.push(report);
    }
    results
}

pub fn search_bugreports(
    store: &AmsStore,
    query: &str,
    status_filter: Option<&str>,
) -> Vec<BugReportInfo> {
    let tokens: Vec<String> = query
        .split_whitespace()
        .map(|t| t.to_ascii_lowercase())
        .filter(|t| t.len() > 1)
        .collect();

    if tokens.is_empty() {
        return list_bugreports(store, status_filter);
    }

    let all = list_bugreports(store, status_filter);
    let mut scored: Vec<(BugReportInfo, usize)> = Vec::new();

    for report in all {
        let attempted = report.attempted_fixes.join(" ");
        let repro = report.reproduction_steps.join(" ");
        let searchable = [
            report.error_output.as_str(),
            report.stack_context.as_str(),
            report.recommended_fix_plan.as_str(),
            attempted.as_str(),
            repro.as_str(),
            report.source_agent.as_str(),
            report.parent_agent.as_str(),
        ]
        .join(" ")
        .to_ascii_lowercase();

        let score = tokens.iter().filter(|t| searchable.contains(t.as_str())).count();
        if score > 0 {
            scored.push((report, score));
        }
    }
    scored.sort_by(|a, b| b.1.cmp(&a.1));
    scored.into_iter().map(|(r, _)| r).collect()
}

// ---------------------------------------------------------------------------
// BugFix CRUD
// ---------------------------------------------------------------------------

pub struct CreateBugFixParams<'a> {
    pub title: &'a str,
    pub description: &'a str,
    pub fix_recipe: &'a str,
    pub linked_bugreport_id: Option<&'a str>,
    pub durable: bool,
    pub created_by: &'a str,
}

pub fn create_bugfix(
    store: &mut AmsStore,
    params: &CreateBugFixParams,
    now: DateTime<FixedOffset>,
) -> Result<BugFixInfo> {
    let durability = if params.durable { "durable" } else { "short_term" };

    create_smartlist_bucket(store, BUGFIX_BUCKET_PATH, params.durable, params.created_by, now)?;

    let fix_id = format!("smartlist-bugfix:{}", Uuid::new_v4().as_simple());

    store.upsert_object(
        fix_id.clone(),
        BUGFIX_OBJECT_KIND,
        None,
        Some(SemanticPayload {
            summary: Some(format!("Fix: {}", truncate_for_summary(params.title))),
            tags: Some(vec![BUGFIX_OBJECT_KIND.to_string(), durability.to_string()]),
            ..Default::default()
        }),
        Some(now),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut linked_ids: Vec<String> = Vec::new();
    {
        let obj = store.objects_mut().get_mut(&fix_id).expect("just upserted");
        let prov = ensure_prov(obj);
        prov.insert("fix_id".into(), Value::String(fix_id.clone()));
        prov.insert("title".into(), Value::String(params.title.trim().into()));
        prov.insert("description".into(), Value::String(params.description.trim().into()));
        prov.insert("fix_recipe".into(), Value::String(params.fix_recipe.trim().into()));
        prov.insert("status".into(), Value::String("available".into()));
        prov.insert("durability".into(), Value::String(durability.into()));
        prov.insert("created_by".into(), Value::String(params.created_by.into()));
        prov.insert("created_at".into(), Value::String(now.to_rfc3339()));

        if let Some(br_id) = params.linked_bugreport_id {
            if !br_id.trim().is_empty() {
                linked_ids.push(br_id.trim().to_string());
            }
        }
        prov.insert("linked_bugreport_ids".into(), to_json_string_list(&linked_ids));
    }

    // Cross-link
    if let Some(br_id) = linked_ids.first() {
        link_bugreport_to_fix(store, br_id, &fix_id, now);
    }

    smartlist_attach(store, BUGFIX_BUCKET_PATH, &fix_id, params.created_by, now)?;

    get_bugfix(store, &fix_id)
        .ok_or_else(|| anyhow::anyhow!("failed to read created bug fix '{}'", fix_id))
}

pub fn link_bugreport_to_fix(
    store: &mut AmsStore,
    bugreport_id: &str,
    bugfix_id: &str,
    now: DateTime<FixedOffset>,
) {
    // Link on the fix side
    if let Some(fix_obj) = store.objects_mut().get_mut(bugfix_id) {
        if fix_obj.object_kind == BUGFIX_OBJECT_KIND {
            let prov = ensure_prov(fix_obj);
            let mut existing = read_string_list(Some(prov), "linked_bugreport_ids");
            if !existing.contains(&bugreport_id.to_string()) {
                existing.push(bugreport_id.to_string());
                prov.insert("linked_bugreport_ids".into(), to_json_string_list(&existing));
                fix_obj.updated_at = now;
            }
        }
    }

    // Link on the report side
    if let Some(report_obj) = store.objects_mut().get_mut(bugreport_id) {
        if report_obj.object_kind == BUGREPORT_OBJECT_KIND {
            let prov = ensure_prov(report_obj);
            let mut existing = read_string_list(Some(prov), "linked_bugfix_ids");
            if !existing.contains(&bugfix_id.to_string()) {
                existing.push(bugfix_id.to_string());
                prov.insert("linked_bugfix_ids".into(), to_json_string_list(&existing));
                report_obj.updated_at = now;
            }
        }
    }
}

pub fn get_bugfix(store: &AmsStore, fix_id: &str) -> Option<BugFixInfo> {
    let obj = store.objects().get(fix_id)?;
    if obj.object_kind != BUGFIX_OBJECT_KIND {
        return None;
    }
    let prov = obj.semantic_payload.as_ref().and_then(|sp| sp.provenance.as_ref());
    Some(BugFixInfo {
        fix_id: fix_id.to_string(),
        title: read_string(prov, "title").unwrap_or_default(),
        description: read_string(prov, "description").unwrap_or_default(),
        fix_recipe: read_string(prov, "fix_recipe").unwrap_or_default(),
        linked_bugreport_ids: read_string_list(prov, "linked_bugreport_ids"),
        status: read_string(prov, "status").unwrap_or_else(|| "available".into()),
        durability: read_string(prov, "durability").unwrap_or_else(|| "short_term".into()),
        bucket_paths: bucket_paths_for_object(store, fix_id),
        created_at: read_date(prov, "created_at").unwrap_or(obj.created_at),
    })
}

pub fn list_bugfixes(store: &AmsStore) -> Vec<BugFixInfo> {
    let container_id = format!("smartlist-members:{BUGFIX_BUCKET_PATH}");
    let member_ids: Vec<String> = store
        .iterate_forward(&container_id)
        .into_iter()
        .map(|ln| ln.object_id.clone())
        .collect();

    let mut results = Vec::new();
    for mid in member_ids {
        if let Some(fix) = get_bugfix(store, &mid) {
            results.push(fix);
        }
    }
    results
}

pub fn get_linked_fixes_for_bugreport(store: &AmsStore, bugreport_id: &str) -> Vec<String> {
    let Some(obj) = store.objects().get(bugreport_id) else {
        return Vec::new();
    };
    if obj.object_kind != BUGREPORT_OBJECT_KIND {
        return Vec::new();
    }
    read_string_list(
        obj.semantic_payload.as_ref().and_then(|sp| sp.provenance.as_ref()),
        "linked_bugfix_ids",
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::now_fixed;

    fn make_now() -> DateTime<FixedOffset> {
        now_fixed()
    }

    #[test]
    fn test_create_and_get_bugreport() {
        let mut store = AmsStore::new();
        let now = make_now();
        let report = create_bugreport(
            &mut store,
            &CreateBugReportParams {
                source_agent: "worker-1",
                parent_agent: "orchestrator",
                error_output: "panicked at index out of bounds",
                stack_context: "callstack push > subtask-3",
                attempted_fixes: vec!["added bounds check".into()],
                reproduction_steps: vec!["run with empty input".into()],
                recommended_fix_plan: "validate input length before indexing",
                severity: "high",
                durable: false,
                created_by: "test",
            },
            now,
        )
        .unwrap();

        assert_eq!(report.source_agent, "worker-1");
        assert_eq!(report.severity, "high");
        assert_eq!(report.status, "open");
        assert!(report.bug_id.starts_with("smartlist-bugreport:"));

        let fetched = get_bugreport(&store, &report.bug_id).unwrap();
        assert_eq!(fetched.error_output, "panicked at index out of bounds");
    }

    #[test]
    fn test_update_status() {
        let mut store = AmsStore::new();
        let now = make_now();
        let report = create_bugreport(
            &mut store,
            &CreateBugReportParams {
                source_agent: "a",
                parent_agent: "b",
                error_output: "err",
                stack_context: "ctx",
                attempted_fixes: vec![],
                reproduction_steps: vec![],
                recommended_fix_plan: "",
                severity: "medium",
                durable: false,
                created_by: "test",
            },
            now,
        )
        .unwrap();

        let updated = update_bugreport_status(&mut store, &report.bug_id, "resolved", now).unwrap();
        assert_eq!(updated.status, "resolved");
        assert!(updated.resolved_at.is_some());
    }

    #[test]
    fn test_list_bugreports_with_filter() {
        let mut store = AmsStore::new();
        let now = make_now();
        let params = CreateBugReportParams {
            source_agent: "a",
            parent_agent: "b",
            error_output: "err1",
            stack_context: "",
            attempted_fixes: vec![],
            reproduction_steps: vec![],
            recommended_fix_plan: "",
            severity: "low",
            durable: false,
            created_by: "test",
        };
        let r1 = create_bugreport(&mut store, &params, now).unwrap();
        create_bugreport(
            &mut store,
            &CreateBugReportParams {
                error_output: "err2",
                ..params
            },
            now,
        )
        .unwrap();

        assert_eq!(list_bugreports(&store, None).len(), 2);
        assert_eq!(list_bugreports(&store, Some("open")).len(), 2);

        update_bugreport_status(&mut store, &r1.bug_id, "resolved", now).unwrap();
        assert_eq!(list_bugreports(&store, Some("open")).len(), 1);
        assert_eq!(list_bugreports(&store, Some("resolved")).len(), 1);
    }

    #[test]
    fn test_search_bugreports() {
        let mut store = AmsStore::new();
        let now = make_now();
        create_bugreport(
            &mut store,
            &CreateBugReportParams {
                source_agent: "a",
                parent_agent: "b",
                error_output: "null pointer dereference",
                stack_context: "",
                attempted_fixes: vec![],
                reproduction_steps: vec![],
                recommended_fix_plan: "",
                severity: "high",
                durable: false,
                created_by: "test",
            },
            now,
        )
        .unwrap();
        create_bugreport(
            &mut store,
            &CreateBugReportParams {
                source_agent: "a",
                parent_agent: "b",
                error_output: "timeout waiting for lock",
                stack_context: "",
                attempted_fixes: vec![],
                reproduction_steps: vec![],
                recommended_fix_plan: "",
                severity: "medium",
                durable: false,
                created_by: "test",
            },
            now,
        )
        .unwrap();

        let results = search_bugreports(&store, "pointer", None);
        assert_eq!(results.len(), 1);
        assert!(results[0].error_output.contains("pointer"));

        let results = search_bugreports(&store, "timeout lock", None);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_create_bugfix_and_link() {
        let mut store = AmsStore::new();
        let now = make_now();
        let report = create_bugreport(
            &mut store,
            &CreateBugReportParams {
                source_agent: "a",
                parent_agent: "b",
                error_output: "crash",
                stack_context: "",
                attempted_fixes: vec![],
                reproduction_steps: vec![],
                recommended_fix_plan: "",
                severity: "high",
                durable: false,
                created_by: "test",
            },
            now,
        )
        .unwrap();

        let fix = create_bugfix(
            &mut store,
            &CreateBugFixParams {
                title: "Add null check",
                description: "Check for null before deref",
                fix_recipe: "if ptr.is_null() { return Err(...) }",
                linked_bugreport_id: Some(&report.bug_id),
                durable: false,
                created_by: "test",
            },
            now,
        )
        .unwrap();

        assert!(fix.fix_id.starts_with("smartlist-bugfix:"));
        assert_eq!(fix.linked_bugreport_ids, vec![report.bug_id.clone()]);

        let linked = get_linked_fixes_for_bugreport(&store, &report.bug_id);
        assert_eq!(linked, vec![fix.fix_id]);
    }

    #[test]
    fn test_list_bugfixes() {
        let mut store = AmsStore::new();
        let now = make_now();
        create_bugfix(
            &mut store,
            &CreateBugFixParams {
                title: "fix-1",
                description: "desc",
                fix_recipe: "recipe",
                linked_bugreport_id: None,
                durable: false,
                created_by: "test",
            },
            now,
        )
        .unwrap();

        let fixes = list_bugfixes(&store);
        assert_eq!(fixes.len(), 1);
        assert_eq!(fixes[0].title, "fix-1");
    }

    #[test]
    fn test_invalid_severity() {
        let mut store = AmsStore::new();
        let now = make_now();
        let result = create_bugreport(
            &mut store,
            &CreateBugReportParams {
                source_agent: "a",
                parent_agent: "b",
                error_output: "err",
                stack_context: "",
                attempted_fixes: vec![],
                reproduction_steps: vec![],
                recommended_fix_plan: "",
                severity: "ultra",
                durable: false,
                created_by: "test",
            },
            now,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_status_update() {
        let mut store = AmsStore::new();
        let now = make_now();
        let report = create_bugreport(
            &mut store,
            &CreateBugReportParams {
                source_agent: "a",
                parent_agent: "b",
                error_output: "err",
                stack_context: "",
                attempted_fixes: vec![],
                reproduction_steps: vec![],
                recommended_fix_plan: "",
                severity: "low",
                durable: false,
                created_by: "test",
            },
            now,
        )
        .unwrap();

        let result = update_bugreport_status(&mut store, &report.bug_id, "invalid", now);
        assert!(result.is_err());
    }
}
