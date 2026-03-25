use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, FixedOffset};
use serde_json::Value;

use crate::context::LineageScope;
use crate::model::ObjectRecord;
use crate::store::AmsStore;

pub fn list_sessions(snapshot: &AmsStore, since: Option<&str>, n: usize) -> Result<String> {
    let since_date = if let Some(raw) = since.filter(|value| !value.trim().is_empty()) {
        DateTime::parse_from_rfc3339(raw)
            .or_else(|_| DateTime::parse_from_str(raw, "%Y-%m-%d%#z"))
            .or_else(|_| DateTime::parse_from_str(&format!("{raw}T00:00:00+00:00"), "%Y-%m-%dT%H:%M:%S%#z"))
            .map_err(|_| anyhow!("Invalid --since date: '{raw}'"))?
    } else {
        DateTime::parse_from_rfc3339("0001-01-01T00:00:00+00:00").unwrap()
    };

    let mut sessions = snapshot
        .containers()
        .values()
        .filter(|container| container.container_kind == "chat_session")
        .map(|container| {
            let started_at = read_date(container.metadata.as_ref(), "started_at");
            let title = read_string(container.metadata.as_ref(), "title").unwrap_or_default();
            let guid = suffix(&container.container_id);
            let msg_count = snapshot.iterate_forward(&container.container_id).len();
            (started_at, guid, title, msg_count)
        })
        .filter(|(started_at, _, _, _)| started_at.unwrap_or(since_date) >= since_date)
        .collect::<Vec<_>>();

    sessions.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));

    let mut out = String::new();
    for (started_at, guid, title, msg_count) in sessions.into_iter().take(n) {
        let date = started_at
            .map(|value| value.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "????-??-??".to_string());
        let guid8 = guid.chars().take(8).collect::<String>();
        let title = truncate_display(if title.trim().is_empty() { "(untitled)" } else { title.trim() }, 50);
        out.push_str(&format!("{date}  {guid8}  {title:<52}  ({msg_count} msgs)\n"));
    }
    Ok(out)
}

pub fn show_session(snapshot: &AmsStore, id_prefix: &str) -> Result<String> {
    let needle = id_prefix.trim();
    if needle.is_empty() {
        bail!("session id prefix is required");
    }

    let matches = snapshot
        .containers()
        .values()
        .filter(|container| container.container_kind == "chat_session")
        .filter(|container| suffix(&container.container_id).starts_with(needle))
        .collect::<Vec<_>>();

    if matches.is_empty() {
        bail!("no chat session found with id prefix '{needle}'.");
    }
    if matches.len() > 1 {
        let candidates = matches
            .iter()
            .map(|container| container.container_id.as_str())
            .collect::<Vec<_>>()
            .join("\n  ");
        bail!("ambiguous prefix '{needle}' matches {} sessions. Use a longer prefix.\n  {candidates}", matches.len());
    }

    let container = matches[0];
    let mut out = String::new();
    for link in snapshot.iterate_forward(&container.container_id) {
        let Some(obj) = snapshot.objects().get(&link.object_id) else {
            continue;
        };
        if obj.object_kind != "chat_message" {
            continue;
        }
        let provenance = obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
        let direction = read_string(provenance, "direction").unwrap_or_default();
        let mut text = read_string(provenance, "text")
            .or_else(|| obj.semantic_payload.as_ref().and_then(|payload| payload.summary.clone()))
            .unwrap_or_default();
        if text.chars().count() > 2000 {
            text = truncate_display(&text, 2000);
        }
        let time_str = read_date(provenance, "ts")
            .map(|ts| format!("[{}] ", ts.format("%H:%M")))
            .unwrap_or_default();
        let role = if direction == "in" { "USER" } else { "CLAUDE" };
        out.push_str(&format!("{time_str}{role}: {text}\n\n"));
    }
    Ok(out)
}

pub fn thread_status(snapshot: &AmsStore) -> String {
    let active_thread_object_id = snapshot
        .iterate_forward("task-graph:active")
        .first()
        .map(|link| link.object_id.clone());
    let parked = snapshot.iterate_forward("task-graph:parked").len();

    let mut out = String::from("# TASK GRAPH\n");
    let Some(active_thread_object_id) = active_thread_object_id else {
        out.push_str("active_thread=(none)\n");
        out.push_str(&format!("parked={parked}\n"));
        return out;
    };

    let Some(active_obj) = snapshot.objects().get(&active_thread_object_id) else {
        out.push_str("active_thread=(none)\n");
        out.push_str(&format!("parked={parked}\n"));
        return out;
    };
    let provenance = active_obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
    let thread_id = read_string(provenance, "thread_id").unwrap_or_else(|| suffix(&active_thread_object_id));
    let title = active_obj
        .semantic_payload
        .as_ref()
        .and_then(|payload| payload.summary.clone())
        .unwrap_or_else(|| thread_id.clone());
    let current_step = read_string(provenance, "current_step").unwrap_or_default();
    let next_command = read_string(provenance, "next_command").unwrap_or_default();
    let active_path = build_active_path(snapshot, &active_thread_object_id)
        .into_iter()
        .map(|scope| scope.node_id)
        .collect::<Vec<_>>()
        .join(" -> ");
    let checkpoints = snapshot.iterate_forward(&format!("task-thread:{thread_id}:checkpoints")).len();
    let artifacts = snapshot.iterate_forward(&format!("task-thread:{thread_id}:artifacts")).len();

    out.push_str(&format!("active_thread={thread_id}\n"));
    out.push_str(&format!("title={title}\n"));
    out.push_str(&format!("active_path={active_path}\n"));
    out.push_str(&format!("current_step={current_step}\n"));
    out.push_str(&format!("next_command={next_command}\n"));
    out.push_str(&format!("parked={parked}\n"));
    out.push_str(&format!("checkpoints={checkpoints}\n"));
    out.push_str(&format!("artifacts={artifacts}\n"));
    if let Some(claim_agent_id) = read_string(provenance, "claim_agent_id") {
        out.push_str(&format!("claim_agent={claim_agent_id}\n"));
        if let Some(claim_token) = read_string(provenance, "claim_token") {
            out.push_str(&format!("claim_token={claim_token}\n"));
        }
        if let Some(claim_lease_until) = read_string(provenance, "claim_lease_until") {
            out.push_str(&format!("claim_lease_until={claim_lease_until}\n"));
        }
    } else {
        out.push_str("claim_agent=(none)\n");
    }
    out
}

pub fn smartlist_inspect(snapshot: &AmsStore, path: &str, depth: usize) -> Result<String> {
    let canonical = normalize_smartlist_path(path);
    let bucket_id = format!("smartlist-bucket:{canonical}");
    let bucket = snapshot
        .objects()
        .get(&bucket_id)
        .ok_or_else(|| anyhow!("Unknown SmartList bucket '{canonical}'."))?;
    if bucket.object_kind != "smartlist_bucket" {
        bail!("Object '{bucket_id}' is not a SmartList bucket.");
    }

    let provenance = bucket.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
    let title = read_string(provenance, "title").unwrap_or_else(|| leaf_name(&canonical));
    let durability = read_string(provenance, "durability").unwrap_or_else(|| "durable".to_string());
    let visibility = read_retrieval_visibility(provenance);

    let mut out = String::new();
    out.push_str(&format!("path={canonical}\n"));
    out.push_str(&format!("title={title}\n"));
    out.push_str(&format!("durability={durability}\n"));
    out.push_str(&format!("retrieval_visibility={visibility}\n"));
    if depth > 0 {
        render_smartlist_entries(snapshot, &canonical, depth, 1, &mut out);
    }
    Ok(out)
}

fn render_smartlist_entries(snapshot: &AmsStore, path: &str, depth: usize, indent: usize, out: &mut String) {
    let Some(bucket) = snapshot.objects().get(&format!("smartlist-bucket:{path}")) else {
        return;
    };
    let provenance = bucket.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
    let members_container = read_string(provenance, "members_container_id")
        .unwrap_or_else(|| format!("smartlist-members:{path}"));
    let mut saw_rollup = false;

    for link in snapshot.iterate_forward(&members_container) {
        let Some(member) = snapshot.objects().get(&link.object_id) else {
            continue;
        };
        if member.object_kind == "smartlist_rollup" {
            saw_rollup = true;
        }
        render_smartlist_member(snapshot, member, depth, indent, out);
    }

    if !saw_rollup {
        if let Some(rollup) = snapshot.objects().get(&format!("smartlist-rollup:{path}")) {
        render_smartlist_member(snapshot, rollup, depth, indent, out);
        }
    }
}

fn render_smartlist_member(snapshot: &AmsStore, obj: &ObjectRecord, depth: usize, indent: usize, out: &mut String) {
    let pad = "  ".repeat(indent);
    let provenance = obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
    let durability = read_string(provenance, "durability").unwrap_or_else(|| "durable".to_string());
    let visibility = read_retrieval_visibility(provenance);

    match obj.object_kind.as_str() {
        "smartlist_bucket" => {
            let path = read_string(provenance, "path").unwrap_or_else(|| obj.object_id.clone());
            let title = read_string(provenance, "title").unwrap_or_else(|| leaf_name(&path));
            out.push_str(&format!(
                "{pad}- [bucket] {path} title={title} durability={durability} retrieval_visibility={visibility}\n"
            ));
            if depth > 1 {
                render_smartlist_entries(snapshot, &path, depth - 1, indent + 1, out);
            }
        }
        "smartlist_note" => {
            let title = read_string(provenance, "title")
                .or_else(|| obj.semantic_payload.as_ref().and_then(|payload| payload.summary.clone()))
                .unwrap_or_else(|| obj.object_id.clone());
            out.push_str(&format!(
                "{pad}- [note] {} title={} durability={} retrieval_visibility={}\n",
                obj.object_id, title, durability, visibility
            ));
        }
        "smartlist_rollup" => {
            let bucket_path = read_string(provenance, "bucket_path").unwrap_or_else(|| suffix(&obj.object_id));
            let title = read_string(provenance, "title").unwrap_or_else(|| leaf_name(&bucket_path));
            out.push_str(&format!(
                "{pad}- [rollup] {bucket_path} title={title} durability={durability} retrieval_visibility={visibility}\n"
            ));
        }
        _ => {}
    }
}

fn build_active_path(snapshot: &AmsStore, active_thread_object_id: &str) -> Vec<LineageScope> {
    let mut lineage = Vec::new();
    let mut cursor = Some(active_thread_object_id.to_string());
    let levels = ["self", "parent", "grandparent", "ancestor"];
    let mut index = 0usize;

    while let Some(object_id) = cursor {
        let Some(object) = snapshot.objects().get(&object_id) else {
            break;
        };
        let provenance = object.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
        let node_id = read_string(provenance, "thread_id").unwrap_or_else(|| suffix(&object_id));
        let title = object
            .semantic_payload
            .as_ref()
            .and_then(|payload| payload.summary.clone())
            .unwrap_or_else(|| node_id.clone());
        lineage.push(LineageScope {
            level: levels.get(index).unwrap_or(&"ancestor").to_string(),
            object_id: object_id.clone(),
            node_id,
            title,
            current_step: read_string(provenance, "current_step").unwrap_or_default(),
            next_command: read_string(provenance, "next_command").unwrap_or_default(),
            branch_off_anchor: read_string(provenance, "branch_off_anchor"),
            artifact_refs: Vec::new(),
        });

        cursor = read_string(provenance, "parent_thread_id")
            .filter(|value| !value.trim().is_empty())
            .map(|value| format!("task-thread:{value}"));
        index += 1;
    }

    lineage.reverse();
    lineage
}

fn normalize_smartlist_path(path: &str) -> String {
    let trimmed = path.trim().trim_matches('/');
    if trimmed.eq_ignore_ascii_case("smartlist") {
        "smartlist".to_string()
    } else if trimmed.starts_with("smartlist/") {
        trimmed.to_string()
    } else {
        format!("smartlist/{trimmed}")
    }
}

fn leaf_name(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

fn suffix(value: &str) -> String {
    value.rsplit(':').next().unwrap_or(value).to_string()
}

fn truncate_display(text: &str, max_chars: usize) -> String {
    let mut chars = text.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}…")
    } else {
        truncated
    }
}

fn read_string(map: Option<&BTreeMap<String, Value>>, key: &str) -> Option<String> {
    match map?.get(key)? {
        Value::String(value) => Some(value.clone()),
        value => Some(value.to_string()),
    }
}

fn read_date(map: Option<&BTreeMap<String, Value>>, key: &str) -> Option<DateTime<FixedOffset>> {
    let Value::String(raw) = map?.get(key)? else {
        return None;
    };
    DateTime::parse_from_rfc3339(raw).ok()
}

fn read_retrieval_visibility(provenance: Option<&BTreeMap<String, Value>>) -> String {
    read_string(provenance, "retrieval_visibility").unwrap_or_else(|| "default".to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::Value;

    use crate::{AmsStore, SemanticPayload};

    use super::{list_sessions, show_session, smartlist_inspect, thread_status};

    #[test]
    fn lists_sessions_from_snapshot() {
        let store = make_session_store();
        let rendered = list_sessions(&store, None, 10).unwrap();
        assert!(rendered.contains("12345678"));
        assert!(rendered.contains("Sample session"));
    }

    #[test]
    fn shows_session_transcript() {
        let store = make_session_store();
        let rendered = show_session(&store, "12345678").unwrap();
        assert!(rendered.contains("USER: hello"));
        assert!(rendered.contains("CLAUDE: world"));
    }

    #[test]
    fn renders_thread_status() {
        let mut store = AmsStore::new();
        store.create_container("task-graph:active", "container", "task_graph_bucket").unwrap();
        store.create_container("task-graph:parked", "container", "task_graph_bucket").unwrap();
        store.upsert_object("task-thread:parent", "task_thread", None, Some(payload("Parent", [
            ("thread_id", "parent"),
            ("current_step", "Review"),
            ("next_command", "read docs"),
        ])), None).unwrap();
        store.upsert_object("task-thread:child", "task_thread", None, Some(payload("Child", [
            ("thread_id", "child"),
            ("parent_thread_id", "parent"),
            ("current_step", "Implement"),
            ("next_command", "cargo test"),
        ])), None).unwrap();
        store.add_object("task-graph:active", "task-thread:child", None, None).unwrap();
        store.create_container("task-thread:child:checkpoints", "container", "task_thread_checkpoints").unwrap();
        store.create_container("task-thread:child:artifacts", "container", "task_thread_artifacts").unwrap();
        let rendered = thread_status(&store);
        assert!(rendered.contains("active_thread=child"));
        assert!(rendered.contains("active_path=parent -> child"));
    }

    #[test]
    fn inspects_smartlist_tree() {
        let mut store = AmsStore::new();
        store.upsert_object("smartlist-bucket:smartlist/root", "smartlist_bucket", None, Some(payload("root", [
            ("path", "smartlist/root"),
            ("durability", "durable"),
            ("retrieval_visibility", "suppressed"),
        ])), None).unwrap();
        store.create_container("smartlist-members:smartlist/root", "container", "smartlist_members").unwrap();
        store.upsert_object("smartlist-note:1", "smartlist_note", None, Some(payload("note", [
            ("title", "Example note"),
            ("text", "body"),
            ("durability", "durable"),
            ("retrieval_visibility", "suppressed"),
        ])), None).unwrap();
        store.add_object("smartlist-members:smartlist/root", "smartlist-note:1", None, None).unwrap();
        let rendered = smartlist_inspect(&store, "smartlist/root", 2).unwrap();
        assert!(rendered.contains("path=smartlist/root"));
        assert!(rendered.contains("[note] smartlist-note:1"));
    }

    fn make_session_store() -> AmsStore {
        let mut store = AmsStore::new();
        store.create_container("chat-session:12345678-1111-2222-3333-444444444444", "container", "chat_session").unwrap();
        store.containers_mut().get_mut("chat-session:12345678-1111-2222-3333-444444444444").unwrap().metadata =
            Some(BTreeMap::from([
                ("title".to_string(), Value::String("Sample session".to_string())),
                ("started_at".to_string(), Value::String("2026-03-13T12:00:00+00:00".to_string())),
            ]));
        store.upsert_object("chat-msg:1", "chat_message", None, Some(payload("hello", [
            ("direction", "in"),
            ("text", "hello"),
            ("ts", "2026-03-13T12:01:00+00:00"),
        ])), None).unwrap();
        store.upsert_object("chat-msg:2", "chat_message", None, Some(payload("world", [
            ("direction", "out"),
            ("text", "world"),
            ("ts", "2026-03-13T12:02:00+00:00"),
        ])), None).unwrap();
        store.add_object("chat-session:12345678-1111-2222-3333-444444444444", "chat-msg:1", None, None).unwrap();
        store.add_object("chat-session:12345678-1111-2222-3333-444444444444", "chat-msg:2", None, None).unwrap();
        store
    }

    fn payload<const N: usize>(summary: &str, entries: [(&str, &str); N]) -> SemanticPayload {
        SemanticPayload {
            embedding: None,
            tags: None,
            summary: Some(summary.to_string()),
            provenance: Some(entries.into_iter().map(|(key, value)| (key.to_string(), Value::String(value.to_string()))).collect()),
        }
    }
}
