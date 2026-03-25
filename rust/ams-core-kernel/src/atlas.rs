//! Atlas CLI commands — multi-scale Object page, search, and expand.
//!
//! These mirror the C# `AtlasPage`, `AtlasSearch`, and `AtlasExpand` implementations
//! in `GraphCommandModule.cs`, ported to Rust as part of Phase 3a.

use anyhow::{bail, Result};

use crate::model::{ContainerRecord, ObjectRecord};
use crate::store::AmsStore;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Resolve an exact object ID or unambiguous prefix.  Returns the resolved ID
/// or an error describing the ambiguity / miss.
fn resolve_object_id(store: &AmsStore, id: &str) -> Result<String> {
    if store.objects().contains_key(id) {
        return Ok(id.to_string());
    }
    let id_lower = id.to_lowercase();
    let matches: Vec<&str> = store
        .objects()
        .keys()
        .filter(|k| k.to_lowercase().starts_with(&id_lower))
        .map(String::as_str)
        .collect();
    match matches.len() {
        1 => Ok(matches[0].to_string()),
        0 => bail!("error: no object found for id '{id}'."),
        n => {
            let mut msg = format!("error: ambiguous id prefix '{id}' matches {n} objects.");
            for m in matches.iter().take(10) {
                msg.push_str(&format!("\n  {m}"));
            }
            bail!("{msg}")
        }
    }
}

// ── atlas-page ───────────────────────────────────────────────────────────────

/// Render an Object page: header metadata, SemanticPayload, and container
/// member list if the object is also a container.
pub fn atlas_page(store: &AmsStore, page_id: &str) -> Result<String> {
    if page_id.eq_ignore_ascii_case("atlas:0") {
        bail!("error: atlas:0 multi-resolution summary is not yet implemented (planned for a future phase).");
    }

    let resolved = resolve_object_id(store, page_id)?;
    let obj = &store.objects()[&resolved];

    let mut out = String::new();

    // Header
    out.push_str(&format!("=== {} ===\n", obj.object_id));
    out.push_str(&format!("kind:       {}\n", obj.object_kind));
    out.push_str(&format!(
        "created:    {}\n",
        obj.created_at.format("%Y-%m-%d %H:%M:%S")
    ));
    out.push_str(&format!(
        "updated:    {}\n",
        obj.updated_at.format("%Y-%m-%d %H:%M:%S")
    ));

    if let Some(ref r) = obj.in_situ_ref {
        out.push_str(&format!("in-situ:    {r}\n"));
    }

    if let Some(ref sp) = obj.semantic_payload {
        if let Some(ref summary) = sp.summary {
            if !summary.trim().is_empty() {
                out.push_str(&format!("summary:    {summary}\n"));
            }
        }
        if let Some(ref tags) = sp.tags {
            if !tags.is_empty() {
                out.push_str(&format!("tags:       {}\n", tags.join(", ")));
            }
        }
        if let Some(ref prov) = sp.provenance {
            if !prov.is_empty() {
                out.push_str("provenance:\n");
                for (k, v) in prov {
                    let val = if let Some(s) = v.as_str() {
                        s.to_string()
                    } else {
                        v.to_string()
                    };
                    let val = if val.len() > 200 {
                        format!("{}…", &val[..200])
                    } else {
                        val
                    };
                    out.push_str(&format!("  {k}: {val}\n"));
                }
            }
        }
    }

    // Container view
    if let Some(container) = store.containers().get(&resolved) {
        out.push_str(&format!("container:  {}\n", container.container_kind));

        if let Some(ref meta) = container.metadata {
            if !meta.is_empty() {
                out.push_str("metadata:\n");
                for (k, v) in meta {
                    let val = if let Some(s) = v.as_str() {
                        s.to_string()
                    } else {
                        v.to_string()
                    };
                    out.push_str(&format!("  {k}: {val}\n"));
                }
            }
        }

        // Walk the link chain
        let members = walk_members(store, container, 5000, 20);
        let member_count = members.len();
        out.push_str(&format!("members ({member_count}):\n"));
        for (id, kind, summary) in members.iter().take(20) {
            let summary_str = match summary {
                Some(s) if !s.trim().is_empty() => {
                    let truncated = if s.len() > 80 { &s[..80] } else { s.as_str() };
                    format!("  — {truncated}")
                }
                _ => String::new(),
            };
            out.push_str(&format!("  [{kind}] {id}{summary_str}\n"));
        }
        if member_count > 20 {
            out.push_str(&format!("  … and {} more\n", member_count - 20));
        }
    }

    Ok(out)
}

// ── atlas-search ─────────────────────────────────────────────────────────────

/// Keyword search across object summaries, tags, and IDs.
pub fn atlas_search(store: &AmsStore, query: &str, top: usize) -> String {
    let tokens: Vec<String> = query
        .split_whitespace()
        .map(|t| t.to_lowercase())
        .collect();

    if tokens.is_empty() {
        return "(no results)\n".to_string();
    }

    let mut scored: Vec<(&ObjectRecord, usize)> = store
        .objects()
        .values()
        .filter_map(|obj| {
            let score = score_object(obj, &tokens);
            if score > 0 { Some((obj, score)) } else { None }
        })
        .collect();

    // Descending by score, then stable by object_id
    scored.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.object_id.cmp(&b.0.object_id)));

    if scored.is_empty() {
        return "(no results)\n".to_string();
    }

    let mut out = String::new();
    for (obj, score) in scored.iter().take(top) {
        out.push_str(&format!("[score={score}] {}  ({})\n", obj.object_id, obj.object_kind));
        if let Some(ref sp) = obj.semantic_payload {
            if let Some(ref summary) = sp.summary {
                if !summary.trim().is_empty() {
                    let truncated = if summary.len() > 100 {
                        format!("{}…", &summary[..100])
                    } else {
                        summary.clone()
                    };
                    out.push_str(&format!("  {truncated}\n"));
                }
            }
        }
    }
    out
}

fn score_object(obj: &ObjectRecord, tokens: &[String]) -> usize {
    let mut score = 0usize;
    let id_lower = obj.object_id.to_lowercase();

    if let Some(ref sp) = obj.semantic_payload {
        if let Some(ref summary) = sp.summary {
            let lc = summary.to_lowercase();
            for t in tokens {
                if lc.contains(t.as_str()) {
                    score += 1;
                }
            }
        }
        if let Some(ref tags) = sp.tags {
            for tag in tags {
                let tag_lc = tag.to_lowercase();
                for t in tokens {
                    if tag_lc.contains(t.as_str()) {
                        score += 1;
                    }
                }
            }
        }
    }

    // ID match on first token (mirrors C# behaviour)
    if !tokens.is_empty() && id_lower.contains(tokens[0].as_str()) {
        score += 1;
    }

    score
}

// ── atlas-expand ─────────────────────────────────────────────────────────────

/// Show containment relationships for an object: which containers it belongs to
/// and, if it is itself a container, its direct children.
pub fn atlas_expand(store: &AmsStore, ref_id: &str) -> Result<String> {
    let resolved = resolve_object_id(store, ref_id)?;
    let obj = &store.objects()[&resolved];

    let mut out = String::new();
    out.push_str(&format!("=== expand: {} ({}) ===\n", resolved, obj.object_kind));

    // Containers this object belongs to
    let mut parent_containers: Vec<&ContainerRecord> = store
        .containers()
        .values()
        .filter(|c| container_has_member(store, c, &resolved))
        .collect();
    parent_containers.sort_by_key(|c| c.container_id.as_str());

    out.push_str(&format!("member-of ({}):\n", parent_containers.len()));
    for c in parent_containers.iter().take(20) {
        let c_summary = store
            .objects()
            .get(&c.container_id)
            .and_then(|co| co.semantic_payload.as_ref())
            .and_then(|sp| sp.summary.as_ref())
            .map(|s| {
                let truncated = if s.len() > 80 { &s[..80] } else { s.as_str() };
                format!("  — {truncated}")
            })
            .unwrap_or_default();
        out.push_str(&format!("  [{}] {}{c_summary}\n", c.container_kind, c.container_id));
    }

    // If this object is itself a container, list children
    if let Some(self_container) = store.containers().get(&resolved) {
        let children = walk_member_ids(store, self_container, 200);
        out.push_str(&format!("children ({}):\n", children.len()));
        for child_id in children.iter().take(20) {
            let child_kind = store
                .objects()
                .get(child_id)
                .map(|co| co.object_kind.as_str())
                .unwrap_or("?");
            out.push_str(&format!("  [{child_kind}] {child_id}\n"));
        }
        if children.len() > 20 {
            out.push_str(&format!("  … and {} more\n", children.len() - 20));
        }
    }

    Ok(out)
}

// ── internal helpers ─────────────────────────────────────────────────────────

/// Walk a container's link chain up to `guard` steps; return (id, kind, summary) tuples.
fn walk_members(
    store: &AmsStore,
    container: &ContainerRecord,
    guard_limit: usize,
    _display_limit: usize,
) -> Vec<(String, String, Option<String>)> {
    let mut members = Vec::new();
    let mut cur = container.head_linknode_id.clone();
    let mut guard = 0usize;
    while let Some(ref ln_id) = cur.clone() {
        if guard >= guard_limit {
            break;
        }
        guard += 1;
        let ln = match store.link_nodes().get(ln_id) {
            Some(ln) => ln,
            None => break,
        };
        if let Some(member_obj) = store.objects().get(&ln.object_id) {
            let summary = member_obj
                .semantic_payload
                .as_ref()
                .and_then(|sp| sp.summary.clone());
            members.push((member_obj.object_id.clone(), member_obj.object_kind.clone(), summary));
        }
        cur = ln.next_linknode_id.clone();
    }
    members
}

/// Walk a container's link chain; return the raw object IDs.
fn walk_member_ids(store: &AmsStore, container: &ContainerRecord, guard_limit: usize) -> Vec<String> {
    let mut ids = Vec::new();
    let mut cur = container.head_linknode_id.clone();
    let mut guard = 0usize;
    while let Some(ref ln_id) = cur.clone() {
        if guard >= guard_limit {
            break;
        }
        guard += 1;
        let ln = match store.link_nodes().get(ln_id) {
            Some(ln) => ln,
            None => break,
        };
        ids.push(ln.object_id.clone());
        cur = ln.next_linknode_id.clone();
    }
    ids
}

fn container_has_member(store: &AmsStore, container: &ContainerRecord, object_id: &str) -> bool {
    let mut cur = container.head_linknode_id.clone();
    let mut guard = 0usize;
    while let Some(ref ln_id) = cur.clone() {
        if guard >= 5000 {
            break;
        }
        guard += 1;
        let ln = match store.link_nodes().get(ln_id) {
            Some(ln) => ln,
            None => break,
        };
        if ln.object_id == object_id {
            return true;
        }
        cur = ln.next_linknode_id.clone();
    }
    false
}
