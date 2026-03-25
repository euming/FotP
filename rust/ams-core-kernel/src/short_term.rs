use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, FixedOffset, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::context::QueryContext;
use crate::corpus::MaterializedCorpus;
use crate::freshness::{
    freshness_lane_boost, is_freshness_internal_object, is_freshness_internal_path, FreshnessObjectPosition,
};
use crate::model::ObjectRecord;
use crate::retrieval::tokenize;
use crate::store::AmsStore;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AgentShortTermHit {
    pub source_kind: String,
    pub source_ref: String,
    pub session_ref: String,
    pub session_title: String,
    pub snippet: String,
    pub score: f64,
    pub recency: f64,
    pub matched_tokens: Vec<String>,
    pub timestamp: Option<DateTime<FixedOffset>>,
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct ManualSmartListDoc {
    pub source_kind: String,
    pub source_ref: String,
    pub object_ref: String,
    pub title: String,
    pub snippet: String,
    pub timestamp: Option<DateTime<FixedOffset>>,
}

pub fn select_short_term_hits(
    corpus: &MaterializedCorpus,
    query: &str,
    top: usize,
    context: Option<&QueryContext>,
    include_suppressed: bool,
    freshness_positions: &HashMap<String, FreshnessObjectPosition>,
    excluded_object_ids: &HashSet<String>,
) -> Vec<AgentShortTermHit> {
    let Some(snapshot) = corpus.snapshot.as_ref() else {
        return Vec::new();
    };

    let tokens = tokenize(query);
    if tokens.is_empty() {
        return Vec::new();
    }

    let now_utc = Utc::now().fixed_offset();
    let session_index = build_session_message_index(snapshot);
    let mut hits = Vec::new();
    hits.extend(select_turn_hits(snapshot, &tokens, now_utc, &session_index));
    hits.extend(select_task_graph_hits(snapshot, &tokens, now_utc, context));
    hits.extend(select_manual_smartlist_hits(
        snapshot,
        &tokens,
        now_utc,
        context,
        include_suppressed,
    ));
    hits.extend(select_session_hits(snapshot, &tokens, now_utc));

    let mut deduped = Vec::new();
    let mut seen = HashSet::new();
    for mut hit in hits {
        if excluded_object_ids.contains(&hit.source_ref) || !seen.insert(hit.source_ref.clone()) {
            continue;
        }
        if let Some(position) = freshness_positions.get(&hit.source_ref) {
            hit.score += freshness_lane_boost(Some(position));
            hit.path = format!("{} -> freshness:{}", hit.path, position.temperature_label);
        }
        deduped.push(hit);
    }
    deduped.sort_by(|left, right| {
        let left_position = freshness_positions.get(&left.source_ref);
        let right_position = freshness_positions.get(&right.source_ref);
        freshness_ordering(left_position, right_position)
            .then_with(|| {
                right
                    .score
                    .partial_cmp(&left.score)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| {
                        right
                            .timestamp
                            .unwrap_or(DateTime::<FixedOffset>::MIN_UTC.fixed_offset())
                            .cmp(&left.timestamp.unwrap_or(DateTime::<FixedOffset>::MIN_UTC.fixed_offset()))
                    })
                    .then_with(|| left.source_ref.cmp(&right.source_ref))
            })
    });
    deduped.truncate(top);
    deduped
}

fn select_turn_hits(
    snapshot: &AmsStore,
    tokens: &[String],
    now_utc: DateTime<FixedOffset>,
    session_index: &HashMap<String, (String, String, Option<DateTime<FixedOffset>>)>,
) -> Vec<AgentShortTermHit> {
    snapshot
        .objects()
        .values()
        .filter(|obj| obj.object_kind == "chat_message")
        .filter_map(|obj| {
            let provenance = obj.semantic_payload.as_ref()?.provenance.as_ref();
            let text = read_string(provenance, "text").or_else(|| obj.semantic_payload.as_ref()?.summary.clone())?;
            let matched_tokens = match_tokens(&text, tokens);
            if matched_tokens.is_empty() {
                return None;
            }

            let (session_ref, session_title, session_ts) = session_index
                .get(&obj.object_id)
                .cloned()
                .unwrap_or_else(|| {
                    (
                        String::new(),
                        "Current session".to_string(),
                        read_date(provenance, "ts"),
                    )
                });
            let ts = read_date(provenance, "ts").or(session_ts);
            let recency = short_term_recency_score(now_utc, ts);
            let semantic = matched_tokens.len() as f64 / tokens.len().max(1) as f64;
            Some(AgentShortTermHit {
                source_kind: "turn".to_string(),
                source_ref: obj.object_id.clone(),
                session_ref,
                session_title,
                snippet: truncate_snippet(&text, 220),
                score: (semantic * 0.85) + (recency * 0.15),
                recency,
                matched_tokens,
                timestamp: ts,
                path: format!("short-term:turn -> {} -> global", obj.object_id),
            })
        })
        .collect()
}

fn select_session_hits(
    snapshot: &AmsStore,
    tokens: &[String],
    now_utc: DateTime<FixedOffset>,
) -> Vec<AgentShortTermHit> {
    snapshot
        .containers()
        .values()
        .filter(|container| container.container_kind == "chat_session")
        .filter_map(|session| {
            let title = read_string(session.metadata.as_ref(), "title").unwrap_or_else(|| session.container_id.clone());
            let matched_tokens = match_tokens(&title, tokens);
            if matched_tokens.is_empty() {
                return None;
            }

            let snippet = resolve_session_snippet(snapshot, &session.container_id);
            let ts = read_date(session.metadata.as_ref(), "ended_at")
                .or_else(|| read_date(session.metadata.as_ref(), "started_at"));
            let recency = short_term_recency_score(now_utc, ts);
            let semantic = matched_tokens.len() as f64 / tokens.len().max(1) as f64;
            Some(AgentShortTermHit {
                source_kind: "session".to_string(),
                source_ref: session.container_id.clone(),
                session_ref: session.container_id.clone(),
                session_title: title,
                snippet: truncate_snippet(&snippet, 220),
                score: (semantic * 0.80) + (recency * 0.20),
                recency,
                matched_tokens,
                timestamp: ts,
                path: format!("short-term:session -> {} -> global", session.container_id),
            })
        })
        .collect()
}

fn select_manual_smartlist_hits(
    snapshot: &AmsStore,
    tokens: &[String],
    now_utc: DateTime<FixedOffset>,
    context: Option<&QueryContext>,
    include_suppressed: bool,
) -> Vec<AgentShortTermHit> {
    select_manual_smartlist_docs(snapshot, context, include_suppressed)
        .into_iter()
        .filter_map(|doc| {
            let haystack = format!("{}\n{}", doc.title, doc.snippet);
            let matched_tokens = match_tokens(&haystack, tokens);
            if matched_tokens.is_empty() {
                return None;
            }

            let recency = short_term_recency_score(now_utc, doc.timestamp);
            let semantic = matched_tokens.len() as f64 / tokens.len().max(1) as f64;
            Some(AgentShortTermHit {
                source_kind: doc.source_kind.clone(),
                source_ref: doc.source_ref.clone(),
                session_ref: doc.object_ref.clone(),
                session_title: doc.title.clone(),
                snippet: truncate_snippet(&doc.snippet, 220),
                score: (semantic * 0.80) + (recency * 0.20),
                recency,
                matched_tokens,
                timestamp: doc.timestamp,
                path: format!("short-term:{} -> {} -> global", doc.source_kind, doc.object_ref),
            })
        })
        .collect()
}

pub(crate) fn select_manual_smartlist_docs(
    snapshot: &AmsStore,
    context: Option<&QueryContext>,
    include_suppressed: bool,
) -> Vec<ManualSmartListDoc> {
    snapshot
        .objects()
        .values()
        .filter(|obj| matches!(obj.object_kind.as_str(), "smartlist_bucket" | "smartlist_note"))
        .filter(|obj| !is_freshness_internal_object(snapshot, &obj.object_id))
        .filter(|obj| is_smartlist_visible_for_query(snapshot, obj, context, include_suppressed))
        .map(|obj| {
            let provenance = obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
            let title = match obj.object_kind.as_str() {
                "smartlist_bucket" => read_string(provenance, "path")
                    .or_else(|| obj.semantic_payload.as_ref().and_then(|payload| payload.summary.clone()))
                    .unwrap_or_else(|| obj.object_id.clone()),
                "smartlist_rollup" => read_string(provenance, "bucket_path")
                    .or_else(|| obj.semantic_payload.as_ref().and_then(|payload| payload.summary.clone()))
                    .unwrap_or_else(|| obj.object_id.clone()),
                _ => read_string(provenance, "title")
                    .or_else(|| obj.semantic_payload.as_ref().and_then(|payload| payload.summary.clone()))
                    .unwrap_or_else(|| obj.object_id.clone()),
            };
            let source_ref = read_string(provenance, "path")
                .or_else(|| read_string(provenance, "bucket_path"))
                .unwrap_or_else(|| obj.object_id.clone());
            ManualSmartListDoc {
                source_kind: obj.object_kind.clone(),
                source_ref: obj.object_id.clone(),
                object_ref: source_ref,
                title,
                snippet: build_manual_smartlist_snippet(snapshot, obj),
                timestamp: read_date(provenance, "updated_at")
                    .or_else(|| read_date(provenance, "created_at"))
                    .or(Some(obj.updated_at)),
            }
        })
        .collect()
}

fn select_task_graph_hits(
    snapshot: &AmsStore,
    tokens: &[String],
    now_utc: DateTime<FixedOffset>,
    context: Option<&QueryContext>,
) -> Vec<AgentShortTermHit> {
    let task_objects = if let Some(context) = context.filter(|context| context.has_lineage()) {
        context
            .lineage
            .iter()
            .flat_map(|scope| enumerate_task_graph_objects(snapshot, &scope.object_id, &scope.node_id))
            .collect::<Vec<_>>()
    } else {
        snapshot
            .objects()
            .values()
            .filter(|obj| matches!(obj.object_kind.as_str(), "task_thread" | "task_checkpoint" | "task_artifact"))
            .map(|obj| obj.object_id.clone())
            .collect::<Vec<_>>()
    };

    task_objects
        .into_iter()
        .filter_map(|object_id| snapshot.objects().get(&object_id))
        .filter_map(|obj| {
            let provenance = obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
            let title = obj
                .semantic_payload
                .as_ref()
                .and_then(|payload| payload.summary.clone())
                .unwrap_or_else(|| obj.object_id.clone());
            let thread_id = read_string(provenance, "thread_id").unwrap_or_else(|| suffix(&obj.object_id));
            let snippet = match obj.object_kind.as_str() {
                "task_thread" => {
                    let current = read_string(provenance, "current_step").unwrap_or_default();
                    let next = read_string(provenance, "next_command").unwrap_or_default();
                    [current, next]
                        .into_iter()
                        .filter(|value| !value.trim().is_empty())
                        .collect::<Vec<_>>()
                        .join(" | ")
                }
                "task_checkpoint" => format!(
                    "{} | next: {}",
                    read_string(provenance, "current_step").unwrap_or_else(|| title.clone()),
                    read_string(provenance, "next_command").unwrap_or_default()
                ),
                "task_artifact" => read_string(provenance, "artifact_ref").unwrap_or_else(|| title.clone()),
                _ => title.clone(),
            };
            let haystack = format!("{title}\n{snippet}\n{thread_id}\n{}", obj.object_id);
            let matched_tokens = match_tokens(&haystack, tokens);
            if matched_tokens.is_empty() {
                return None;
            }

            let ts = read_date(provenance, "updated_at")
                .or_else(|| read_date(provenance, "created_at"))
                .unwrap_or(obj.updated_at);
            let recency = short_term_recency_score(now_utc, Some(ts));
            let semantic = matched_tokens.len() as f64 / tokens.len().max(1) as f64;
            Some(AgentShortTermHit {
                source_kind: obj.object_kind.clone(),
                source_ref: obj.object_id.clone(),
                session_ref: thread_id.clone(),
                session_title: title,
                snippet: truncate_snippet(&snippet, 220),
                score: (semantic * 0.70) + (recency * 0.10) + 0.10,
                recency,
                matched_tokens,
                timestamp: Some(ts),
                path: format!("short-term:{} -> {} -> global", obj.object_kind, obj.object_id),
            })
        })
        .collect()
}

fn build_session_message_index(
    snapshot: &AmsStore,
) -> HashMap<String, (String, String, Option<DateTime<FixedOffset>>)> {
    let mut map = HashMap::new();
    for session in snapshot
        .containers()
        .values()
        .filter(|container| container.container_kind == "chat_session")
    {
        let title = read_string(session.metadata.as_ref(), "title").unwrap_or_else(|| session.container_id.clone());
        let ts = read_date(session.metadata.as_ref(), "ended_at")
            .or_else(|| read_date(session.metadata.as_ref(), "started_at"));
        for member in snapshot.iterate_forward(&session.container_id) {
            if snapshot
                .objects()
                .get(&member.object_id)
                .is_some_and(|obj| obj.object_kind == "chat_message")
            {
                map.insert(member.object_id.clone(), (session.container_id.clone(), title.clone(), ts));
            }
        }
    }
    map
}

fn resolve_session_snippet(snapshot: &AmsStore, session_container_id: &str) -> String {
    snapshot
        .iterate_forward(session_container_id)
        .into_iter()
        .filter_map(|link| snapshot.objects().get(&link.object_id))
        .filter(|obj| obj.object_kind == "chat_message")
        .filter_map(|obj| {
            obj.semantic_payload
                .as_ref()
                .and_then(|payload| payload.provenance.as_ref())
                .and_then(|provenance| read_string(Some(provenance), "text"))
                .or_else(|| obj.semantic_payload.as_ref().and_then(|payload| payload.summary.clone()))
        })
        .take(3)
        .collect::<Vec<_>>()
        .join(" ")
}

fn build_manual_smartlist_snippet(snapshot: &AmsStore, obj: &ObjectRecord) -> String {
    let provenance = obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
    match obj.object_kind.as_str() {
        "smartlist_bucket" => {
            let path = read_string(provenance, "path").unwrap_or_else(|| obj.object_id.clone());
            let rollup_id = format!("smartlist-rollup:{path}");
            if let Some(rollup) = snapshot.objects().get(&rollup_id) {
                let rollup_provenance = rollup.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
                let summary = read_string(rollup_provenance, "summary").unwrap_or_default();
                let scope = read_string(rollup_provenance, "scope").unwrap_or_default();
                let stop_hint = read_string(rollup_provenance, "stop_hint").unwrap_or_default();
                let child_highlights = rollup_child_highlights(rollup_provenance);
                return format!(
                    "bucket {path}; summary: {summary}{}{}{}",
                    if stop_hint.is_empty() {
                        String::new()
                    } else {
                        format!("; stop_hint: {stop_hint}")
                    },
                    if scope.is_empty() {
                        String::new()
                    } else {
                        format!("; scope: {scope}")
                    },
                    if child_highlights.is_empty() {
                        String::new()
                    } else {
                        format!(" children: {child_highlights}")
                    }
                );
            }

            let members_container_id = read_string(provenance, "members_container_id")
                .unwrap_or_else(|| format!("smartlist-members:{path}"));
            let members = if !snapshot.containers().contains_key(&members_container_id) {
                Vec::new()
            } else {
                snapshot
                    .iterate_forward(&members_container_id)
                    .into_iter()
                    .map(|link| link.object_id.clone())
                    .map(|member_id| {
                        snapshot
                            .objects()
                            .get(&member_id)
                            .map(|member| {
                                let member_provenance =
                                    member.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
                                match member.object_kind.as_str() {
                                    "smartlist_bucket" => {
                                        read_string(member_provenance, "path").unwrap_or(member_id.clone())
                                    }
                                    "smartlist_note" => read_string(member_provenance, "title")
                                        .or_else(|| member.semantic_payload.as_ref().and_then(|payload| payload.summary.clone()))
                                        .unwrap_or(member_id.clone()),
                                    _ => member
                                        .semantic_payload
                                        .as_ref()
                                        .and_then(|payload| payload.summary.clone())
                                        .unwrap_or(member_id.clone()),
                                }
                            })
                            .unwrap_or(member_id)
                    })
                    .take(6)
                    .collect::<Vec<_>>()
            };
            if members.is_empty() {
                format!("bucket {path}")
            } else {
                format!("bucket {path}; members: {}", members.join(", "))
            }
        }
        "smartlist_note" => {
            let text = read_string(provenance, "text").unwrap_or_default();
            let summary = obj
                .semantic_payload
                .as_ref()
                .and_then(|payload| payload.summary.clone())
                .unwrap_or_default();
            let bucket_paths = snapshot
                .containers_for_member_object(&obj.object_id)
                .into_iter()
                .filter(|container_id| container_id.starts_with("smartlist-members:"))
                .map(|container_id| container_id["smartlist-members:".len()..].to_string())
                .collect::<Vec<_>>();
            if bucket_paths.is_empty() {
                if text.trim().is_empty() {
                    summary
                } else {
                    text
                }
            } else {
                format!("{text} buckets: {}", bucket_paths.join(", "))
            }
        }
        _ => obj
            .semantic_payload
            .as_ref()
            .and_then(|payload| payload.summary.clone())
            .unwrap_or_default(),
    }
}

fn rollup_child_highlights(provenance: Option<&BTreeMap<String, Value>>) -> String {
    let Some(Value::Array(items)) = provenance.and_then(|map| map.get("child_highlights")) else {
        return String::new();
    };
    items
        .iter()
        .filter_map(|item| {
            let Value::Object(map) = item else {
                return None;
            };
            let child_path = map.get("path").and_then(Value::as_str).unwrap_or_default();
            let child_summary = map.get("summary").and_then(Value::as_str).unwrap_or_default();
            let value = if child_summary.trim().is_empty() {
                child_path.trim().to_string()
            } else {
                format!("{}: {}", child_path.trim(), child_summary.trim())
            };
            if value.trim().is_empty() {
                None
            } else {
                Some(value)
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn enumerate_task_graph_objects(snapshot: &AmsStore, thread_object_id: &str, node_id: &str) -> Vec<String> {
    let mut ids = vec![thread_object_id.to_string()];
    let checkpoints_container = format!("task-thread:{node_id}:checkpoints");
    if snapshot.containers().contains_key(&checkpoints_container) {
        ids.extend(
            snapshot
                .iterate_forward(&checkpoints_container)
                .into_iter()
                .map(|link| link.object_id.clone()),
        );
    }
    let artifacts_container = format!("task-thread:{node_id}:artifacts");
    if snapshot.containers().contains_key(&artifacts_container) {
        ids.extend(
            snapshot
                .iterate_forward(&artifacts_container)
                .into_iter()
                .map(|link| link.object_id.clone()),
        );
    }
    ids.sort();
    ids.dedup();
    ids
}

fn is_smartlist_visible_for_query(
    snapshot: &AmsStore,
    obj: &ObjectRecord,
    context: Option<&QueryContext>,
    include_suppressed: bool,
) -> bool {
    match read_retrieval_visibility(obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref())) {
        "default" => true,
        "scoped" => !has_active_smartlist_scope(context) || is_smartlist_in_active_scope(snapshot, obj, context),
        "suppressed" => {
            let paths = get_smartlist_scope_paths(snapshot, obj);
            include_suppressed && !paths.iter().any(|path| is_freshness_internal_path(path))
        }
        _ => true,
    }
}

fn has_active_smartlist_scope(context: Option<&QueryContext>) -> bool {
    context.is_some_and(|context| {
        context
            .lineage
            .iter()
            .filter_map(|scope| scope.branch_off_anchor.as_deref())
            .chain(context.failure_bucket.as_deref())
            .any(is_smartlist_path)
    })
}

fn is_smartlist_in_active_scope(snapshot: &AmsStore, obj: &ObjectRecord, context: Option<&QueryContext>) -> bool {
    let Some(context) = context else {
        return false;
    };

    let object_paths = get_smartlist_scope_paths(snapshot, obj);
    if object_paths.is_empty() {
        return false;
    }
    let scope_paths = context
        .lineage
        .iter()
        .filter_map(|scope| scope.branch_off_anchor.as_deref())
        .chain(context.failure_bucket.as_deref())
        .filter(|path| is_smartlist_path(path))
        .collect::<Vec<_>>();

    object_paths
        .iter()
        .any(|path| scope_paths.iter().any(|scope_path| smartlist_paths_overlap(path, scope_path)))
}

fn get_smartlist_scope_paths(snapshot: &AmsStore, obj: &ObjectRecord) -> Vec<String> {
    let provenance = obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
    match obj.object_kind.as_str() {
        "smartlist_bucket" => read_string(provenance, "path")
            .into_iter()
            .filter(|path| !is_freshness_internal_path(path))
            .collect(),
        "smartlist_rollup" => read_string(provenance, "bucket_path")
            .into_iter()
            .filter(|path| !is_freshness_internal_path(path))
            .collect(),
        "smartlist_note" => snapshot
            .containers_for_member_object(&obj.object_id)
            .into_iter()
            .filter(|container_id| container_id.starts_with("smartlist-members:"))
            .map(|container_id| container_id["smartlist-members:".len()..].to_string())
            .filter(|path| !is_freshness_internal_path(path))
            .collect(),
        _ => Vec::new(),
    }
}

fn freshness_ordering(
    left: Option<&FreshnessObjectPosition>,
    right: Option<&FreshnessObjectPosition>,
) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left
            .lane_index
            .cmp(&right.lane_index)
            .then_with(|| left.lane_path.cmp(&right.lane_path)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn smartlist_paths_overlap(left: &str, right: &str) -> bool {
    left == right || left.starts_with(&format!("{right}/")) || right.starts_with(&format!("{left}/"))
}

fn read_retrieval_visibility(provenance: Option<&BTreeMap<String, Value>>) -> &'static str {
    match read_string(provenance, "retrieval_visibility").as_deref() {
        Some("scoped") => "scoped",
        Some("suppressed") => "suppressed",
        _ => "default",
    }
}

fn is_smartlist_path(value: &str) -> bool {
    value.starts_with("smartlist/")
}

fn match_tokens(haystack: &str, tokens: &[String]) -> Vec<String> {
    let haystack = haystack.to_ascii_lowercase();
    tokens
        .iter()
        .filter(|token| haystack.contains(token.as_str()))
        .cloned()
        .collect()
}

fn short_term_recency_score(now_utc: DateTime<FixedOffset>, timestamp: Option<DateTime<FixedOffset>>) -> f64 {
    let Some(timestamp) = timestamp else {
        return 0.50;
    };
    let age_days = ((now_utc - timestamp).num_seconds().max(0) as f64) / 86_400.0;
    1.0 / (1.0 + (age_days / 7.0))
}

fn read_string(map: Option<&BTreeMap<String, Value>>, key: &str) -> Option<String> {
    match map?.get(key)? {
        Value::String(value) => Some(value.clone()),
        value => Some(value.to_string()),
    }
}

fn read_date(map: Option<&BTreeMap<String, Value>>, key: &str) -> Option<DateTime<FixedOffset>> {
    let value = map?.get(key)?;
    match value {
        Value::String(raw) => DateTime::parse_from_rfc3339(raw).ok(),
        _ => None,
    }
}

fn truncate_snippet(text: &str, max_chars: usize) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= max_chars {
        return trimmed.to_string();
    }
    let mut out = trimmed.chars().take(max_chars).collect::<String>();
    out = out.trim_end().to_string();
    out.push_str("...");
    out
}

fn suffix(id: &str) -> String {
    id.split_once(':')
        .map(|(_, value)| value.to_string())
        .unwrap_or_else(|| id.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use crate::model::SemanticPayload;
    use crate::store::AmsStore;

    use super::*;

    fn make_snapshot() -> AmsStore {
        let mut store = AmsStore::new();
        store.create_container("chat-session:s1", "chat_session", "chat_session").unwrap();
        store
            .upsert_object(
                "chat-msg:s1-0",
                "chat_message",
                None,
                Some(SemanticPayload {
                    embedding: None,
                    tags: None,
                    summary: Some("Use double buffering for swarm agents".to_string()),
                    provenance: Some(BTreeMap::from([
                        ("text".to_string(), json!("Use double buffering for swarm agents with shared memory.")),
                        ("ts".to_string(), json!("2026-03-13T19:00:00Z")),
                    ])),
                }),
                None,
            )
            .unwrap();
        store.containers_mut().get_mut("chat-session:s1").unwrap().metadata = Some(BTreeMap::from([
            ("title".to_string(), json!("Session s1")),
            ("started_at".to_string(), json!("2026-03-13T18:00:00Z")),
        ]));
        store.add_object("chat-session:s1", "chat-msg:s1-0", None, None).unwrap();

        store
            .upsert_object(
                "smartlist-note:1",
                "smartlist_note",
                None,
                Some(SemanticPayload {
                    embedding: None,
                    tags: None,
                    summary: Some("Codex Claude hook parity".to_string()),
                    provenance: Some(BTreeMap::from([
                        ("title".to_string(), json!("Codex Claude hook parity")),
                        ("text".to_string(), json!("Codex needs the same short-term sync capability as Claude hooks.")),
                        ("updated_at".to_string(), json!("2026-03-13T18:30:00Z")),
                    ])),
                }),
                None,
            )
            .unwrap();
        store.create_container("smartlist-members:smartlist/agents/claude", "container", "smartlist_members").unwrap();
        store.add_object("smartlist-members:smartlist/agents/claude", "smartlist-note:1", None, None).unwrap();

        store
    }

    #[test]
    fn selects_turn_and_smartlist_hits_from_snapshot() {
        let corpus = MaterializedCorpus {
            db_path: "fixture.memory.jsonl".into(),
            snapshot_path: Some("fixture.memory.ams.json".into()),
            snapshot: Some(make_snapshot()),
            cards: Default::default(),
            binders: Default::default(),
            tag_links: Default::default(),
            payloads: Default::default(),
            unknown_record_types: Default::default(),
        };

        let hits = select_short_term_hits(
            &corpus,
            "double buffering claude parity",
            10,
            None,
            false,
            &HashMap::new(),
            &HashSet::new(),
        );

        assert!(!hits.is_empty());
        assert!(hits.iter().any(|hit| hit.source_kind == "turn"));
        assert!(hits.iter().any(|hit| hit.source_kind == "smartlist_note"));
    }

    #[test]
    fn truncate_snippet_handles_unicode_boundaries() {
        let text = "Alpha “quoted” tail";

        let snippet = truncate_snippet(text, 8);

        assert_eq!(snippet, "Alpha “q...");
    }
}
