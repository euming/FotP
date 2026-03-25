use std::collections::BTreeSet;

use anyhow::{anyhow, Result};

use crate::corpus::MaterializedCorpus;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LineageScope {
    pub level: String,
    pub object_id: String,
    pub node_id: String,
    pub title: String,
    pub current_step: String,
    pub next_command: String,
    pub branch_off_anchor: Option<String>,
    pub artifact_refs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryContext {
    pub lineage: Vec<LineageScope>,
    pub agent_role: String,
    pub mode: String,
    pub failure_bucket: Option<String>,
    pub active_artifacts: Vec<String>,
    pub traversal_budget: usize,
    pub source: String,
}

impl QueryContext {
    pub fn has_lineage(&self) -> bool {
        !self.lineage.is_empty()
    }

    pub fn scope_lens(&self) -> &'static str {
        if self.has_lineage() {
            "local-first-lineage"
        } else {
            "global"
        }
    }

    pub fn context_terms(&self) -> Vec<String> {
        let mut terms = Vec::new();
        for scope in &self.lineage {
            terms.extend(crate::retrieval::tokenize(&scope.title));
            terms.extend(crate::retrieval::tokenize(&scope.current_step));
            terms.extend(crate::retrieval::tokenize(&scope.next_command));
            if let Some(anchor) = scope.branch_off_anchor.as_deref() {
                terms.extend(crate::retrieval::tokenize(anchor));
            }
            for artifact in &scope.artifact_refs {
                terms.extend(crate::retrieval::tokenize(artifact));
            }
        }
        if let Some(failure_bucket) = self.failure_bucket.as_deref() {
            terms.extend(crate::retrieval::tokenize(failure_bucket));
        }
        for artifact in &self.active_artifacts {
            terms.extend(crate::retrieval::tokenize(artifact));
        }
        terms.sort();
        terms.dedup();
        terms
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryContextOptions {
    pub current_node_id: Option<String>,
    pub parent_node_id: Option<String>,
    pub grandparent_node_id: Option<String>,
    pub agent_role: Option<String>,
    pub mode: Option<String>,
    pub failure_bucket: Option<String>,
    pub active_artifacts: Vec<String>,
    pub traversal_budget: usize,
    pub no_active_thread_context: bool,
}

pub fn build_query_context(corpus: &MaterializedCorpus, options: &QueryContextOptions) -> Result<Option<QueryContext>> {
    let explicit_ids = [
        options.current_node_id.as_deref(),
        options.parent_node_id.as_deref(),
        options.grandparent_node_id.as_deref(),
    ];
    let mut lineage = Vec::new();

    if explicit_ids.iter().any(|value| value.is_some()) {
        try_add_explicit_scope(corpus, &mut lineage, "self", options.current_node_id.as_deref())?;
        try_add_explicit_scope(corpus, &mut lineage, "parent", options.parent_node_id.as_deref())?;
        try_add_explicit_scope(corpus, &mut lineage, "grandparent", options.grandparent_node_id.as_deref())?;
    } else if !options.no_active_thread_context {
        lineage = build_active_thread_lineage(corpus);
    }

    let active_artifacts = dedupe_strings(
        lineage
            .iter()
            .flat_map(|scope| scope.artifact_refs.iter().cloned())
            .chain(options.active_artifacts.iter().cloned())
            .collect(),
    );
    let failure_bucket = options.failure_bucket.clone().filter(|value| !value.trim().is_empty());
    let agent_role = options
        .agent_role
        .clone()
        .unwrap_or_else(|| "implementer".to_string());
    let mode = options.mode.clone().unwrap_or_else(|| "build".to_string());
    let traversal_budget = options.traversal_budget.max(1);

    if lineage.is_empty()
        && failure_bucket.is_none()
        && active_artifacts.is_empty()
        && options.agent_role.is_none()
        && options.mode.is_none()
    {
        return Ok(None);
    }

    let source = if explicit_ids.iter().any(|value| value.is_some()) {
        "explicit".to_string()
    } else if lineage.is_empty() {
        "explicit".to_string()
    } else {
        "active-task-graph".to_string()
    };

    Ok(Some(QueryContext {
        lineage,
        agent_role,
        mode,
        failure_bucket,
        active_artifacts,
        traversal_budget,
        source,
    }))
}

pub fn render_context(context: Option<&QueryContext>) -> String {
    let Some(context) = context else {
        return String::new();
    };
    if !context.has_lineage() && context.failure_bucket.is_none() {
        return String::new();
    }

    let mut out = String::new();
    out.push_str("## Context\n");
    out.push_str(&format!("- scope_lens: {}\n", context.scope_lens()));
    out.push_str(&format!("- source: {}\n", context.source));
    out.push_str(&format!("- agent: role={} mode={}\n", context.agent_role, context.mode));
    if let Some(failure_bucket) = context.failure_bucket.as_deref() {
        out.push_str(&format!("- failure_bucket: {}\n", failure_bucket));
    }
    for scope in &context.lineage {
        out.push_str(&format!("- {}: {} | {}\n", scope.level, scope.node_id, scope.title));
        if !scope.current_step.is_empty() {
            out.push_str(&format!("  step: {}\n", scope.current_step));
        }
        if !scope.next_command.is_empty() {
            out.push_str(&format!("  next: {}\n", scope.next_command));
        }
    }
    out
}

fn build_active_thread_lineage(corpus: &MaterializedCorpus) -> Vec<LineageScope> {
    let Some(snapshot) = corpus.snapshot.as_ref() else {
        return Vec::new();
    };
    if !snapshot.containers().contains_key("task-graph:active") {
        return Vec::new();
    }

    let mut lineage = Vec::new();
    let mut current_object_id = snapshot
        .iterate_forward("task-graph:active")
        .first()
        .map(|link| link.object_id.clone());
    let levels = ["self", "parent", "grandparent"];
    for level in levels {
        let Some(object_id) = current_object_id.clone() else {
            break;
        };
        let Some(scope) = try_build_lineage_scope(corpus, &object_id, level) else {
            break;
        };
        current_object_id = resolve_parent_thread_object_id(corpus, &object_id);
        lineage.push(scope);
    }
    lineage
}

fn try_add_explicit_scope(
    corpus: &MaterializedCorpus,
    lineage: &mut Vec<LineageScope>,
    level: &str,
    raw_node_id: Option<&str>,
) -> Result<()> {
    let Some(raw_node_id) = raw_node_id.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };

    let scope = if let Some(object_id) = resolve_thread_object_id(corpus, raw_node_id) {
        try_build_lineage_scope(corpus, &object_id, level).ok_or_else(|| {
            anyhow!(
                "task thread '{}' was not materialized as a task_thread object",
                raw_node_id
            )
        })?
    } else {
        LineageScope {
            level: level.to_string(),
            object_id: raw_node_id.to_string(),
            node_id: raw_node_id.to_string(),
            title: raw_node_id.to_string(),
            current_step: String::new(),
            next_command: String::new(),
            branch_off_anchor: None,
            artifact_refs: Vec::new(),
        }
    };
    lineage.push(scope);
    Ok(())
}

fn try_build_lineage_scope(corpus: &MaterializedCorpus, object_id: &str, level: &str) -> Option<LineageScope> {
    let snapshot = corpus.snapshot.as_ref()?;
    let object = snapshot.objects().get(object_id)?;
    if object.object_kind != "task_thread" {
        return None;
    }

    let provenance = object.semantic_payload.as_ref()?.provenance.as_ref()?;
    let node_id = read_string(provenance, "thread_id").unwrap_or_else(|| suffix(object_id));
    Some(LineageScope {
        level: level.to_string(),
        object_id: object_id.to_string(),
        node_id: node_id.clone(),
        title: object
            .semantic_payload
            .as_ref()
            .and_then(|payload| payload.summary.clone())
            .unwrap_or(node_id.clone()),
        current_step: read_string(provenance, "current_step").unwrap_or_default(),
        next_command: read_string(provenance, "next_command").unwrap_or_default(),
        branch_off_anchor: read_string(provenance, "branch_off_anchor").filter(|value| !value.trim().is_empty()),
        artifact_refs: read_artifact_refs(corpus, &node_id),
    })
}

fn resolve_parent_thread_object_id(corpus: &MaterializedCorpus, thread_object_id: &str) -> Option<String> {
    let snapshot = corpus.snapshot.as_ref()?;
    let object = snapshot.objects().get(thread_object_id)?;
    let provenance = object.semantic_payload.as_ref()?.provenance.as_ref()?;
    let parent_thread_id = read_string(provenance, "parent_thread_id")?;
    let parent_thread_id = parent_thread_id.trim();
    if parent_thread_id.is_empty() {
        return None;
    }
    Some(format!("task-thread:{parent_thread_id}"))
}

fn read_artifact_refs(corpus: &MaterializedCorpus, thread_id: &str) -> Vec<String> {
    let Some(snapshot) = corpus.snapshot.as_ref() else {
        return Vec::new();
    };
    let container_id = format!("task-thread:{thread_id}:artifacts");
    if !snapshot.containers().contains_key(&container_id) {
        return Vec::new();
    }

    dedupe_strings(
        snapshot
            .iterate_forward(&container_id)
            .into_iter()
            .filter_map(|link| snapshot.objects().get(&link.object_id))
            .filter_map(|object| object.semantic_payload.as_ref())
            .filter_map(|payload| payload.provenance.as_ref())
            .filter_map(|provenance| read_string(provenance, "artifact_ref"))
            .collect(),
    )
}

fn resolve_thread_object_id(corpus: &MaterializedCorpus, raw_node_id: &str) -> Option<String> {
    let snapshot = corpus.snapshot.as_ref()?;
    if snapshot.objects().contains_key(raw_node_id) {
        return Some(raw_node_id.to_string());
    }

    let candidate = format!("task-thread:{raw_node_id}");
    if snapshot.objects().contains_key(&candidate) {
        return Some(candidate);
    }

    snapshot
        .objects()
        .values()
        .find(|object| {
            object.object_kind == "task_thread"
                && object
                    .semantic_payload
                    .as_ref()
                    .and_then(|payload| payload.provenance.as_ref())
                    .and_then(|provenance| read_string(provenance, "thread_id"))
                    .is_some_and(|thread_id| thread_id == raw_node_id)
        })
        .map(|object| object.object_id.clone())
}

fn read_string(map: &crate::model::JsonMap, key: &str) -> Option<String> {
    map.get(key).map(|value| match value {
        serde_json::Value::String(string) => string.clone(),
        other => other.to_string(),
    })
}

fn suffix(id: &str) -> String {
    id.split(':').next_back().unwrap_or(id).to_string()
}

fn dedupe_strings(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use chrono::DateTime;
    use serde_json::Value;

    use crate::corpus::MaterializedCorpus;
    use crate::store::AmsStore;

    use super::*;

    fn empty_corpus_with_snapshot(store: AmsStore) -> MaterializedCorpus {
        MaterializedCorpus {
            db_path: PathBuf::from("fixture.memory.jsonl"),
            snapshot_path: Some(PathBuf::from("fixture.memory.ams.json")),
            snapshot: Some(store),
            cards: BTreeMap::new(),
            binders: BTreeMap::new(),
            tag_links: BTreeMap::new(),
            payloads: BTreeMap::new(),
            unknown_record_types: BTreeMap::new(),
        }
    }

    fn make_task_snapshot() -> MaterializedCorpus {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z").unwrap();
        store.create_container("task-graph:active", "container", "task_graph_bucket").unwrap();
        store.upsert_object("task-thread:parent-thread", "task_thread", None, None, Some(now)).unwrap();
        store.upsert_object("task-thread:child-thread", "task_thread", None, None, Some(now)).unwrap();
        store
            .objects_mut()
            .get_mut("task-thread:parent-thread")
            .unwrap()
            .semantic_payload = Some(crate::model::SemanticPayload {
            embedding: None,
            tags: Some(vec!["task_thread".to_string()]),
            summary: Some("Parent architecture thread".to_string()),
            provenance: Some(BTreeMap::from([
                ("thread_id".to_string(), Value::String("parent-thread".to_string())),
                ("status".to_string(), Value::String("parked".to_string())),
                ("current_step".to_string(), Value::String("Review architecture".to_string())),
                ("next_command".to_string(), Value::String("review docs".to_string())),
                ("parent_thread_id".to_string(), Value::String(String::new())),
                ("branch_off_anchor".to_string(), Value::String("smartlist/execution-plan".to_string())),
            ])),
        });
        store
            .objects_mut()
            .get_mut("task-thread:child-thread")
            .unwrap()
            .semantic_payload = Some(crate::model::SemanticPayload {
            embedding: None,
            tags: Some(vec!["task_thread".to_string()]),
            summary: Some("Child implementation thread".to_string()),
            provenance: Some(BTreeMap::from([
                ("thread_id".to_string(), Value::String("child-thread".to_string())),
                ("status".to_string(), Value::String("active".to_string())),
                ("current_step".to_string(), Value::String("Implement retrieval".to_string())),
                ("next_command".to_string(), Value::String("cargo test".to_string())),
                ("parent_thread_id".to_string(), Value::String("parent-thread".to_string())),
                ("branch_off_anchor".to_string(), Value::String("smartlist/execution-plan/swarm-bootstrap".to_string())),
            ])),
        });
        store.add_object("task-graph:active", "task-thread:child-thread", None, None).unwrap();
        store.create_container("task-thread:child-thread:artifacts", "container", "task_thread_artifacts").unwrap();
        store.upsert_object("task-artifact:child-thread:1", "task_artifact", None, None, Some(now)).unwrap();
        store
            .objects_mut()
            .get_mut("task-artifact:child-thread:1")
            .unwrap()
            .semantic_payload = Some(crate::model::SemanticPayload {
            embedding: None,
            tags: None,
            summary: Some("RetrievalService.cs".to_string()),
            provenance: Some(BTreeMap::from([(
                "artifact_ref".to_string(),
                Value::String("src/MemoryGraph.Application/RetrievalService.cs".to_string()),
            )])),
        });
        store
            .add_object("task-thread:child-thread:artifacts", "task-artifact:child-thread:1", None, None)
            .unwrap();

        empty_corpus_with_snapshot(store)
    }

    #[test]
    fn builds_default_context_from_active_task_graph() {
        let corpus = make_task_snapshot();
        let context = build_query_context(&corpus, &QueryContextOptions::default())
            .unwrap()
            .unwrap();
        assert_eq!(context.scope_lens(), "local-first-lineage");
        assert_eq!(context.source, "active-task-graph");
        assert_eq!(context.lineage.len(), 2);
        assert_eq!(context.lineage[0].node_id, "child-thread");
        assert_eq!(context.lineage[1].node_id, "parent-thread");
        assert!(context.active_artifacts.iter().any(|artifact| artifact.contains("RetrievalService.cs")));
    }

    #[test]
    fn builds_explicit_context_and_disables_default_active_thread_lookup() {
        let corpus = make_task_snapshot();
        let context = build_query_context(
            &corpus,
            &QueryContextOptions {
                current_node_id: Some("child-thread".to_string()),
                parent_node_id: Some("parent-thread".to_string()),
                no_active_thread_context: true,
                agent_role: Some("architect".to_string()),
                mode: Some("design".to_string()),
                failure_bucket: Some("smartlist/execution-plan/swarm-bootstrap".to_string()),
                traversal_budget: 4,
                ..QueryContextOptions::default()
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(context.source, "explicit");
        assert_eq!(context.agent_role, "architect");
        assert_eq!(context.mode, "design");
        assert_eq!(context.failure_bucket.as_deref(), Some("smartlist/execution-plan/swarm-bootstrap"));
    }

    #[test]
    fn returns_none_when_no_context_inputs_exist() {
        let corpus = empty_corpus_with_snapshot(AmsStore::new());
        let context = build_query_context(
            &corpus,
            &QueryContextOptions {
                no_active_thread_context: true,
                ..QueryContextOptions::default()
            },
        )
        .unwrap();
        assert!(context.is_none());
    }
}
