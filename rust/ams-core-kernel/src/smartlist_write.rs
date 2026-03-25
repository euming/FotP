use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, FixedOffset};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::model::{now_fixed, JsonMap, ObjectRecord, OverflowPolicy, SemanticPayload};
use crate::policy::{enforce_add_member_policies, AddMemberDecision};
use crate::store::{AmsStore, StoreError};

pub const SHORT_TERM_ROOT_CONTAINER: &str = "agent-memory:short-term:smartlists";
pub const DURABLE_ROOT_CONTAINER: &str = "agent-memory:smartlists";
pub const BUCKET_OBJECT_KIND: &str = "smartlist_bucket";
pub const NOTE_OBJECT_KIND: &str = "smartlist_note";
pub const ROLLUP_OBJECT_KIND: &str = "smartlist_rollup";
pub const RETRIEVAL_VISIBILITY_KEY: &str = "retrieval_visibility";
pub const RETRIEVAL_VISIBILITY_DEFAULT: &str = "default";
pub const RETRIEVAL_VISIBILITY_SCOPED: &str = "scoped";
pub const RETRIEVAL_VISIBILITY_SUPPRESSED: &str = "suppressed";
pub const SHORT_TERM_DURABILITY: &str = "short_term";
pub const DURABLE_DURABILITY: &str = "durable";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListBucketInfo {
    pub path: String,
    pub object_id: String,
    pub display_name: String,
    pub parent_path: Option<String>,
    pub durability: String,
    pub retrieval_visibility: String,
    pub created_at: DateTime<FixedOffset>,
    pub updated_at: DateTime<FixedOffset>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListNoteInfo {
    pub note_id: String,
    pub title: String,
    pub text: String,
    pub durability: String,
    pub retrieval_visibility: String,
    pub bucket_paths: Vec<String>,
    pub created_at: DateTime<FixedOffset>,
    pub updated_at: DateTime<FixedOffset>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListRollupChild {
    pub path: String,
    pub summary: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListRollupInfo {
    pub rollup_id: String,
    pub bucket_path: String,
    pub title: String,
    pub summary: String,
    pub scope: String,
    pub stop_hint: Option<String>,
    pub durability: String,
    pub retrieval_visibility: String,
    pub source_mode: String,
    pub child_highlights: Vec<SmartListRollupChild>,
    pub created_at: DateTime<FixedOffset>,
    pub updated_at: DateTime<FixedOffset>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListVisibilityResult {
    pub path: String,
    pub retrieval_visibility: String,
    pub buckets_updated: usize,
    pub notes_updated: usize,
    pub rollups_updated: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListAttachResult {
    pub path: String,
    pub member_object_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListDetachResult {
    pub path: String,
    pub member_object_id: String,
    pub removed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListMoveResult {
    pub source_path: String,
    pub target_path: String,
    pub member_object_id: String,
}

pub fn create_bucket(
    store: &mut AmsStore,
    path: &str,
    durable: bool,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<SmartListBucketInfo> {
    ensure_scaffold(store)?;
    let canonical = normalize_path(path)?;
    if canonical == "smartlist" {
        bail!("smartlist root is reserved; create a bucket under smartlist/<name>");
    }
    ensure_bucket_path(
        store,
        &canonical,
        if durable { DURABLE_DURABILITY } else { SHORT_TERM_DURABILITY },
        created_by,
        now_utc,
    )
}

pub fn create_note(
    store: &mut AmsStore,
    title: &str,
    text: &str,
    bucket_paths: &[String],
    durable: bool,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
    note_id: Option<&str>,
) -> Result<SmartListNoteInfo> {
    ensure_scaffold(store)?;
    let title = title.trim();
    let text = text.trim();
    if title.is_empty() {
        bail!("title is required");
    }
    if text.is_empty() {
        bail!("text is required");
    }

    let durability = if durable { DURABLE_DURABILITY } else { SHORT_TERM_DURABILITY };
    let note_id = note_id
        .map(|value| value.to_string())
        .unwrap_or_else(|| format!("smartlist-note:{}", Uuid::new_v4().simple()));

    store
        .upsert_object(note_id.clone(), NOTE_OBJECT_KIND, None, None, Some(now_utc))
        .map_err(to_anyhow)?;
    {
        let note = store
            .objects_mut()
            .get_mut(&note_id)
            .ok_or_else(|| anyhow!("failed to materialize note '{}'", note_id))?;
        note.semantic_payload.get_or_insert_with(SemanticPayload::default).summary = Some(title.to_string());
        note.semantic_payload.as_mut().expect("semantic payload initialized").tags =
            Some(vec![NOTE_OBJECT_KIND.to_string(), durability.to_string()]);
        let created_at =
            read_date(note.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref()), "created_at")
                .unwrap_or(now_utc);
        note.created_at = created_at;
        note.updated_at = now_utc;
        let prov = ensure_prov(note);
        prov.insert("title".to_string(), Value::String(title.to_string()));
        prov.insert("text".to_string(), Value::String(text.to_string()));
        prov.insert("durability".to_string(), Value::String(durability.to_string()));
        let visibility =
            read_string(Some(prov), RETRIEVAL_VISIBILITY_KEY).unwrap_or_else(|| RETRIEVAL_VISIBILITY_DEFAULT.to_string());
        prov.insert(RETRIEVAL_VISIBILITY_KEY.to_string(), Value::String(visibility));
        prov.insert("created_by".to_string(), Value::String(created_by.to_string()));
        prov.insert("created_at".to_string(), Value::String(created_at.to_rfc3339()));
        prov.insert("updated_at".to_string(), Value::String(now_utc.to_rfc3339()));
        prov.insert("source".to_string(), Value::String("manual".to_string()));
    }

    let normalized_paths = bucket_paths
        .iter()
        .map(|path| normalize_path(path))
        .collect::<Result<BTreeSet<_>>>()?;

    if normalized_paths.is_empty() {
        ensure_attached(store, root_container_id(durable), &note_id)?;
    } else {
        for bucket_path in normalized_paths {
            let bucket = ensure_bucket_path(store, &bucket_path, durability, created_by, now_utc)?;
            ensure_attached(store, &members_container_id(&bucket.path), &note_id)?;
        }
    }

    get_note(store, &note_id).ok_or_else(|| anyhow!("failed to read created note '{}'", note_id))
}

pub fn attach_member(
    store: &mut AmsStore,
    path: &str,
    member_ref: &str,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<SmartListAttachResult> {
    ensure_scaffold(store)?;
    let bucket = ensure_bucket_path(store, &normalize_path(path)?, SHORT_TERM_DURABILITY, created_by, now_utc)?;
    let member_object_id = resolve_member_object(store, member_ref, created_by, now_utc)?;
    ensure_attached(store, &members_container_id(&bucket.path), &member_object_id)?;
    Ok(SmartListAttachResult { path: bucket.path, member_object_id })
}

pub fn attach_member_before(
    store: &mut AmsStore,
    path: &str,
    member_ref: &str,
    before_member_ref: &str,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<SmartListAttachResult> {
    ensure_scaffold(store)?;
    let bucket = ensure_bucket_path(store, &normalize_path(path)?, SHORT_TERM_DURABILITY, created_by, now_utc)?;
    let container_id = members_container_id(&bucket.path);
    let member_object_id = resolve_member_object(store, member_ref, created_by, now_utc)?;
    remove_membership(store, &container_id, &member_object_id)?;
    let before_member_object_id = resolve_member_object(store, before_member_ref, created_by, now_utc)?;
    let before_link = membership_link_id(store, &container_id, &before_member_object_id)
        .ok_or_else(|| anyhow!("member '{}' is not attached to '{}'", before_member_ref, bucket.path))?;
    store
        .insert_before(&container_id, &before_link, &member_object_id, None, None)
        .map_err(to_anyhow)?;
    Ok(SmartListAttachResult { path: bucket.path, member_object_id })
}

pub fn detach_member(
    store: &mut AmsStore,
    path: &str,
    member_ref: &str,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<SmartListDetachResult> {
    ensure_scaffold(store)?;
    let bucket = ensure_bucket_path(store, &normalize_path(path)?, SHORT_TERM_DURABILITY, created_by, now_utc)?;
    let member_object_id = resolve_member_object(store, member_ref, created_by, now_utc)?;
    let removed = remove_membership(store, &members_container_id(&bucket.path), &member_object_id)?;
    Ok(SmartListDetachResult {
        path: bucket.path,
        member_object_id,
        removed,
    })
}

pub fn move_member(
    store: &mut AmsStore,
    source_path: &str,
    target_path: &str,
    member_ref: &str,
    before_member_ref: Option<&str>,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<SmartListMoveResult> {
    ensure_scaffold(store)?;
    let source_bucket = ensure_bucket_path(store, &normalize_path(source_path)?, SHORT_TERM_DURABILITY, created_by, now_utc)?;
    let target_bucket = ensure_bucket_path(store, &normalize_path(target_path)?, SHORT_TERM_DURABILITY, created_by, now_utc)?;
    let member_object_id = resolve_member_object(store, member_ref, created_by, now_utc)?;
    remove_membership(store, &members_container_id(&source_bucket.path), &member_object_id)?;
    if source_bucket.path != target_bucket.path {
        remove_membership(store, &members_container_id(&target_bucket.path), &member_object_id)?;
    }
    let target_container_id = members_container_id(&target_bucket.path);
    if let Some(before_member_ref) = before_member_ref {
        let before_member_object_id = resolve_member_object(store, before_member_ref, created_by, now_utc)?;
        let before_link = membership_link_id(store, &target_container_id, &before_member_object_id)
            .ok_or_else(|| anyhow!("member '{}' is not attached to '{}'", before_member_ref, target_bucket.path))?;
        store
            .insert_before(&target_container_id, &before_link, &member_object_id, None, None)
            .map_err(to_anyhow)?;
    } else {
        ensure_attached(store, &target_container_id, &member_object_id)?;
    }
    Ok(SmartListMoveResult {
        source_path: source_bucket.path,
        target_path: target_bucket.path,
        member_object_id,
    })
}

pub fn set_bucket_fields(
    store: &mut AmsStore,
    path: &str,
    fields: &BTreeMap<String, String>,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<SmartListBucketInfo> {
    ensure_scaffold(store)?;
    let bucket = ensure_bucket_path(store, &normalize_path(path)?, SHORT_TERM_DURABILITY, created_by, now_utc)?;
    {
        let object = store
            .objects_mut()
            .get_mut(&bucket.object_id)
            .ok_or_else(|| anyhow!("failed to materialize bucket '{}'", bucket.object_id))?;
        let prov = ensure_prov(object);
        for (key, value) in fields {
            prov.insert(key.clone(), Value::String(value.clone()));
        }
        prov.insert("updated_at".to_string(), Value::String(now_utc.to_rfc3339()));
        object.updated_at = now_utc;
    }
    get_bucket(store, &bucket.path).ok_or_else(|| anyhow!("failed to read bucket '{}'", bucket.path))
}

pub fn set_rollup(
    store: &mut AmsStore,
    path: &str,
    summary: &str,
    scope: &str,
    stop_hint: Option<&str>,
    child_highlights: &[SmartListRollupChild],
    durable: bool,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<SmartListRollupInfo> {
    ensure_scaffold(store)?;
    let summary = summary.trim();
    let scope = scope.trim();
    if summary.is_empty() {
        bail!("summary is required");
    }
    if scope.is_empty() {
        bail!("scope is required");
    }

    let bucket = ensure_bucket_path(
        store,
        &normalize_path(path)?,
        if durable { DURABLE_DURABILITY } else { SHORT_TERM_DURABILITY },
        created_by,
        now_utc,
    )?;
    let rollup_id = rollup_object_id(&bucket.path);
    store
        .upsert_object(rollup_id.clone(), ROLLUP_OBJECT_KIND, None, None, Some(now_utc))
        .map_err(to_anyhow)?;
    {
        let rollup = store
            .objects_mut()
            .get_mut(&rollup_id)
            .ok_or_else(|| anyhow!("failed to materialize rollup '{}'", rollup_id))?;
        rollup.semantic_payload.get_or_insert_with(SemanticPayload::default).summary =
            Some(bucket.display_name.clone());
        rollup.semantic_payload.as_mut().expect("semantic payload initialized").tags =
            Some(vec![ROLLUP_OBJECT_KIND.to_string(), bucket.durability.clone()]);
        let created_at =
            read_date(rollup.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref()), "created_at")
                .unwrap_or(now_utc);
        rollup.created_at = created_at;
        rollup.updated_at = now_utc;
        let prov = ensure_prov(rollup);
        prov.insert("bucket_path".to_string(), Value::String(bucket.path.clone()));
        prov.insert("title".to_string(), Value::String(bucket.display_name.clone()));
        prov.insert("summary".to_string(), Value::String(summary.to_string()));
        prov.insert("scope".to_string(), Value::String(scope.to_string()));
        prov.insert("stop_hint".to_string(), Value::String(stop_hint.unwrap_or_default().trim().to_string()));
        prov.insert("durability".to_string(), Value::String(bucket.durability.clone()));
        let visibility =
            read_string(Some(prov), RETRIEVAL_VISIBILITY_KEY).unwrap_or_else(|| bucket.retrieval_visibility.clone());
        prov.insert(RETRIEVAL_VISIBILITY_KEY.to_string(), Value::String(visibility));
        prov.insert("source_mode".to_string(), Value::String("manual".to_string()));
        let created_by = read_string(Some(prov), "created_by").unwrap_or_else(|| created_by.to_string());
        prov.insert("created_by".to_string(), Value::String(created_by));
        prov.insert("created_at".to_string(), Value::String(created_at.to_rfc3339()));
        prov.insert("updated_at".to_string(), Value::String(now_utc.to_rfc3339()));
        prov.insert(
            "child_highlights".to_string(),
            Value::Array(
                child_highlights
                    .iter()
                    .map(|child| {
                        let mut map = serde_json::Map::new();
                        map.insert(
                            "path".to_string(),
                            Value::String(normalize_path(&child.path).unwrap_or_else(|_| child.path.clone())),
                        );
                        map.insert("summary".to_string(), Value::String(child.summary.trim().to_string()));
                        Value::Object(map)
                    })
                    .collect(),
            ),
        );
    }
    ensure_attached(store, &members_container_id(&bucket.path), &rollup_id)?;
    get_rollup(store, &bucket.path).ok_or_else(|| anyhow!("failed to read rollup for '{}'", bucket.path))
}

pub fn set_retrieval_visibility(
    store: &mut AmsStore,
    path: &str,
    visibility: &str,
    recursive: bool,
    include_notes: bool,
    include_rollups: bool,
    now_utc: DateTime<FixedOffset>,
) -> Result<SmartListVisibilityResult> {
    ensure_scaffold(store)?;
    let canonical = normalize_path(path)?;
    let bucket = get_bucket(store, &canonical).ok_or_else(|| anyhow!("unknown SmartList bucket '{}'", canonical))?;
    let normalized_visibility = normalize_retrieval_visibility(visibility)?;

    let mut buckets_updated = 0usize;
    let mut notes_updated = 0usize;
    let mut rollups_updated = 0usize;
    apply_bucket_visibility(
        store,
        &bucket.path,
        &normalized_visibility,
        recursive,
        include_notes,
        include_rollups,
        now_utc,
        &mut buckets_updated,
        &mut notes_updated,
        &mut rollups_updated,
    )?;

    Ok(SmartListVisibilityResult {
        path: bucket.path,
        retrieval_visibility: normalized_visibility,
        buckets_updated,
        notes_updated,
        rollups_updated,
    })
}

pub fn get_bucket(store: &AmsStore, path: &str) -> Option<SmartListBucketInfo> {
    let canonical = normalize_path(path).ok()?;
    if canonical == "smartlist" {
        return None;
    }
    let object_id = bucket_object_id(&canonical);
    let obj = store.objects().get(&object_id)?;
    if obj.object_kind != BUCKET_OBJECT_KIND {
        return None;
    }

    let prov = obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
    Some(SmartListBucketInfo {
        path: read_string(prov, "path").unwrap_or(canonical.clone()),
        object_id,
        display_name: read_string(prov, "display_name")
            .or_else(|| read_string(prov, "title"))
            .unwrap_or_else(|| last_segment(&canonical)),
        parent_path: empty_to_none(read_string(prov, "parent_path")),
        durability: read_string(prov, "durability").unwrap_or_else(|| SHORT_TERM_DURABILITY.to_string()),
        retrieval_visibility: read_retrieval_visibility(prov),
        created_at: read_date(prov, "created_at").unwrap_or(obj.created_at),
        updated_at: read_date(prov, "updated_at").unwrap_or(obj.updated_at),
    })
}

pub fn get_note(store: &AmsStore, note_id: &str) -> Option<SmartListNoteInfo> {
    let obj = store.objects().get(note_id)?;
    if obj.object_kind != NOTE_OBJECT_KIND {
        return None;
    }
    let prov = obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
    Some(SmartListNoteInfo {
        note_id: note_id.to_string(),
        title: read_string(prov, "title")
            .or_else(|| obj.semantic_payload.as_ref().and_then(|payload| payload.summary.clone()))
            .unwrap_or_else(|| note_id.to_string()),
        text: read_string(prov, "text").unwrap_or_default(),
        durability: read_string(prov, "durability").unwrap_or_else(|| SHORT_TERM_DURABILITY.to_string()),
        retrieval_visibility: read_retrieval_visibility(prov),
        bucket_paths: bucket_paths_for_note(store, note_id),
        created_at: read_date(prov, "created_at").unwrap_or(obj.created_at),
        updated_at: read_date(prov, "updated_at").unwrap_or(obj.updated_at),
    })
}

pub fn get_rollup(store: &AmsStore, path: &str) -> Option<SmartListRollupInfo> {
    let canonical = normalize_path(path).ok()?;
    let rollup_id = rollup_object_id(&canonical);
    let obj = store.objects().get(&rollup_id)?;
    if obj.object_kind != ROLLUP_OBJECT_KIND {
        return None;
    }
    let prov = obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
    let child_highlights = match prov.and_then(|map| map.get("child_highlights")) {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| {
                let Value::Object(map) = item else {
                    return None;
                };
                let path = map.get("path").and_then(|value| value.as_str())?.to_string();
                let summary = map
                    .get("summary")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string();
                Some(SmartListRollupChild { path, summary })
            })
            .collect(),
        _ => Vec::new(),
    };

    Some(SmartListRollupInfo {
        rollup_id,
        bucket_path: read_string(prov, "bucket_path").unwrap_or(canonical.clone()),
        title: read_string(prov, "title")
            .or_else(|| obj.semantic_payload.as_ref().and_then(|payload| payload.summary.clone()))
            .unwrap_or_else(|| last_segment(&canonical)),
        summary: read_string(prov, "summary").unwrap_or_default(),
        scope: read_string(prov, "scope").unwrap_or_default(),
        stop_hint: empty_to_none(read_string(prov, "stop_hint")),
        durability: read_string(prov, "durability").unwrap_or_else(|| SHORT_TERM_DURABILITY.to_string()),
        retrieval_visibility: read_retrieval_visibility(prov),
        source_mode: read_string(prov, "source_mode").unwrap_or_else(|| "manual".to_string()),
        child_highlights,
        created_at: read_date(prov, "created_at").unwrap_or(obj.created_at),
        updated_at: read_date(prov, "updated_at").unwrap_or(obj.updated_at),
    })
}

pub fn normalize_path(path: &str) -> Result<String> {
    if path.trim().is_empty() {
        bail!("path is required");
    }

    let normalized = path.replace('\\', "/");
    let mut parts = normalized
        .split('/')
        .filter_map(|segment| {
            let segment = segment.trim();
            if segment.is_empty() {
                None
            } else {
                Some(normalize_path_part(segment))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if parts.is_empty() {
        bail!("path must contain at least one segment");
    }
    if parts[0] != "smartlist" {
        parts.insert(0, "smartlist".to_string());
    }
    Ok(parts.join("/"))
}

pub fn normalize_retrieval_visibility(value: &str) -> Result<String> {
    match value.trim().to_ascii_lowercase().as_str() {
        RETRIEVAL_VISIBILITY_DEFAULT => Ok(RETRIEVAL_VISIBILITY_DEFAULT.to_string()),
        RETRIEVAL_VISIBILITY_SCOPED => Ok(RETRIEVAL_VISIBILITY_SCOPED.to_string()),
        RETRIEVAL_VISIBILITY_SUPPRESSED => Ok(RETRIEVAL_VISIBILITY_SUPPRESSED.to_string()),
        other => bail!("invalid retrieval visibility '{}'", other),
    }
}

pub fn default_note_id_for_mutation(mutation_id: &str) -> String {
    let compact = mutation_id
        .chars()
        .filter(|value| value.is_ascii_alphanumeric())
        .take(32)
        .collect::<String>();
    let suffix = if compact.is_empty() {
        now_fixed().timestamp_nanos_opt().unwrap_or_default().to_string()
    } else {
        compact
    };
    format!("smartlist-note:{suffix}")
}

fn ensure_scaffold(store: &mut AmsStore) -> Result<()> {
    ensure_container(store, SHORT_TERM_ROOT_CONTAINER, "smartlist_root")?;
    ensure_container(store, DURABLE_ROOT_CONTAINER, "smartlist_root")?;
    // Declare unique_members policy on all chat_session containers so that
    // the policy engine enforces uniqueness at mutation time (P1b).
    // This covers both new containers (scaffold at creation time) and
    // existing containers loaded from snapshots that predate this policy.
    for container in store.containers_mut().values_mut() {
        if container.container_kind == "chat_session" {
            container.policies.unique_members = true;
        }
    }
    Ok(())
}

fn ensure_bucket_path(
    store: &mut AmsStore,
    canonical_path: &str,
    durability: &str,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<SmartListBucketInfo> {
    if !canonical_path.starts_with("smartlist/") {
        bail!("SmartList paths must live under the smartlist namespace. Got '{}'.", canonical_path);
    }
    let segments = canonical_path.split('/').collect::<Vec<_>>();
    if segments.len() < 2 {
        bail!("SmartList paths must include at least one bucket name. Got '{}'.", canonical_path);
    }

    let mut parent_path = None;
    let mut current = None;
    for depth in 1..segments.len() {
        let path = segments[..=depth].join("/");
        current = Some(ensure_bucket(store, &path, parent_path.as_deref(), durability, created_by, now_utc)?);
        parent_path = Some(path);
    }
    current.ok_or_else(|| anyhow!("failed to ensure SmartList path '{}'", canonical_path))
}

fn ensure_bucket(
    store: &mut AmsStore,
    path: &str,
    parent_path: Option<&str>,
    durability: &str,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<SmartListBucketInfo> {
    let object_id = bucket_object_id(path);
    let created = !store.objects().contains_key(&object_id);
    store
        .upsert_object(object_id.clone(), BUCKET_OBJECT_KIND, None, None, Some(now_utc))
        .map_err(to_anyhow)?;
    {
        let bucket = store
            .objects_mut()
            .get_mut(&object_id)
            .ok_or_else(|| anyhow!("failed to materialize bucket '{}'", object_id))?;
        bucket.semantic_payload.get_or_insert_with(SemanticPayload::default).summary = Some(last_segment(path));
        bucket.semantic_payload.as_mut().expect("semantic payload initialized").tags =
            Some(vec![BUCKET_OBJECT_KIND.to_string(), durability.to_string()]);
        let created_at =
            read_date(bucket.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref()), "created_at")
                .unwrap_or(now_utc);
        bucket.created_at = created_at;
        bucket.updated_at = now_utc;
        let prov = ensure_prov(bucket);
        prov.insert("path".to_string(), Value::String(path.to_string()));
        prov.insert("display_name".to_string(), Value::String(last_segment(path)));
        prov.insert("title".to_string(), Value::String(last_segment(path)));
        prov.insert("parent_path".to_string(), Value::String(parent_path.unwrap_or_default().to_string()));
        let effective_durability = read_string(Some(prov), "durability").unwrap_or_else(|| durability.to_string());
        prov.insert("durability".to_string(), Value::String(effective_durability));
        let visibility =
            read_string(Some(prov), RETRIEVAL_VISIBILITY_KEY).unwrap_or_else(|| RETRIEVAL_VISIBILITY_DEFAULT.to_string());
        prov.insert(RETRIEVAL_VISIBILITY_KEY.to_string(), Value::String(visibility));
        let created_by = read_string(Some(prov), "created_by").unwrap_or_else(|| created_by.to_string());
        prov.insert("created_by".to_string(), Value::String(created_by));
        prov.insert("created_at".to_string(), Value::String(created_at.to_rfc3339()));
        prov.insert("updated_at".to_string(), Value::String(now_utc.to_rfc3339()));
        prov.insert("source".to_string(), Value::String("manual".to_string()));
        prov.insert("members_container_id".to_string(), Value::String(members_container_id(path)));
    }

    ensure_container(store, &members_container_id(path), "smartlist_members")?;
    if let Some(parent_path) = parent_path {
        let parent =
            ensure_bucket(store, parent_path, parent_path_of(parent_path).as_deref(), durability, created_by, now_utc)?;
        ensure_attached(store, &members_container_id(&parent.path), &object_id)?;
    } else {
        ensure_attached(store, root_container_id(durability == DURABLE_DURABILITY), &object_id)?;
    }

    if !created {
        let effective_durability = get_bucket(store, path)
            .map(|bucket| bucket.durability)
            .unwrap_or_else(|| durability.to_string());
        ensure_root_membership(store, path, &effective_durability)?;
    }
    get_bucket(store, path).ok_or_else(|| anyhow!("failed to read bucket '{}'", path))
}

fn apply_bucket_visibility(
    store: &mut AmsStore,
    bucket_path: &str,
    visibility: &str,
    recursive: bool,
    include_notes: bool,
    include_rollups: bool,
    now_utc: DateTime<FixedOffset>,
    buckets_updated: &mut usize,
    notes_updated: &mut usize,
    rollups_updated: &mut usize,
) -> Result<()> {
    update_object_visibility(store, &bucket_object_id(bucket_path), visibility, now_utc)?;
    *buckets_updated += 1;

    let rollup_id = rollup_object_id(bucket_path);
    if include_rollups {
        if let Some(obj) = store.objects().get(&rollup_id) {
            if obj.object_kind == ROLLUP_OBJECT_KIND {
                update_object_visibility(store, &rollup_id, visibility, now_utc)?;
                *rollups_updated += 1;
            }
        }
    }

    let member_ids = store
        .iterate_forward(&members_container_id(bucket_path))
        .iter()
        .map(|node| node.object_id.clone())
        .collect::<Vec<_>>();
    for member_id in member_ids {
        let Some(obj) = store.objects().get(&member_id) else {
            continue;
        };
        if obj.object_kind == NOTE_OBJECT_KIND {
            if include_notes {
                update_object_visibility(store, &member_id, visibility, now_utc)?;
                *notes_updated += 1;
            }
            continue;
        }

        if obj.object_kind == BUCKET_OBJECT_KIND && recursive {
            let child_path = read_string(obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref()), "path")
                .unwrap_or(member_id.clone());
            apply_bucket_visibility(
                store,
                &child_path,
                visibility,
                true,
                include_notes,
                include_rollups,
                now_utc,
                buckets_updated,
                notes_updated,
                rollups_updated,
            )?;
        }
    }
    Ok(())
}

fn update_object_visibility(
    store: &mut AmsStore,
    object_id: &str,
    visibility: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<()> {
    let obj = store
        .objects_mut()
        .get_mut(object_id)
        .ok_or_else(|| anyhow!("unknown object '{}'", object_id))?;
    let prov = ensure_prov(obj);
    prov.insert(RETRIEVAL_VISIBILITY_KEY.to_string(), Value::String(visibility.to_string()));
    prov.insert("updated_at".to_string(), Value::String(now_utc.to_rfc3339()));
    obj.updated_at = now_utc;
    Ok(())
}

fn resolve_member_object(
    store: &mut AmsStore,
    member_ref: &str,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<String> {
    if store.objects().contains_key(member_ref) {
        return Ok(member_ref.to_string());
    }
    let normalized = normalize_path(member_ref)?;
    if normalized.starts_with("smartlist/") {
        return Ok(ensure_bucket_path(store, &normalized, SHORT_TERM_DURABILITY, created_by, now_utc)?.object_id);
    }
    bail!("unknown SmartList member '{}'", member_ref)
}

fn bucket_paths_for_note(store: &AmsStore, note_id: &str) -> Vec<String> {
    let mut paths = store
        .containers_for_member_object(note_id)
        .into_iter()
        .filter_map(|container_id| container_id_to_bucket_path(&container_id))
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    paths
}

fn ensure_attached(store: &mut AmsStore, container_id: &str, object_id: &str) -> Result<()> {
    match enforce_add_member_policies(store, container_id, object_id)? {
        AddMemberDecision::Skip(_) => {}
        AddMemberDecision::Add => {
            if !store.has_membership(container_id, object_id) {
                store.add_object(container_id, object_id, None, None).map_err(to_anyhow)?;
            }
        }
        AddMemberDecision::EvictOldest(evict_link_node_id) => {
            // Remove the oldest (head) member to make room, then add the new one.
            store.remove_linknode(container_id, &evict_link_node_id).map_err(to_anyhow)?;
            store.add_object(container_id, object_id, None, None).map_err(to_anyhow)?;
        }
    }
    Ok(())
}

fn remove_membership(store: &mut AmsStore, container_id: &str, object_id: &str) -> Result<bool> {
    let link_node_id = membership_link_id(store, container_id, object_id);
    if let Some(link_node_id) = link_node_id {
        return store.remove_linknode(container_id, &link_node_id).map_err(to_anyhow);
    }
    Ok(false)
}

fn membership_link_id(store: &AmsStore, container_id: &str, object_id: &str) -> Option<String> {
    store
        .links_for_member_object(object_id)
        .into_iter()
        .find(|node| node.container_id == container_id)
        .map(|node| node.link_node_id.clone())
}

fn ensure_container(store: &mut AmsStore, container_id: &str, container_kind: &str) -> Result<()> {
    if !store.containers().contains_key(container_id) {
        store
            .create_container(container_id.to_string(), "container", container_kind.to_string())
            .map_err(to_anyhow)?;
    }
    if let Some(container) = store.containers_mut().get_mut(container_id) {
        container.policies.unique_members = true;
    }
    Ok(())
}

fn ensure_root_membership(store: &mut AmsStore, path: &str, durability: &str) -> Result<()> {
    if parent_path_of(path).is_some() {
        return Ok(());
    }
    let object_id = bucket_object_id(path);
    let desired_root = root_container_id(durability == DURABLE_DURABILITY);
    let other_root = if desired_root == DURABLE_ROOT_CONTAINER {
        SHORT_TERM_ROOT_CONTAINER
    } else {
        DURABLE_ROOT_CONTAINER
    };
    remove_membership(store, other_root, &object_id)?;
    ensure_attached(store, desired_root, &object_id)
}

fn root_container_id(durable: bool) -> &'static str {
    if durable { DURABLE_ROOT_CONTAINER } else { SHORT_TERM_ROOT_CONTAINER }
}

fn bucket_object_id(path: &str) -> String {
    format!("smartlist-bucket:{path}")
}

fn rollup_object_id(path: &str) -> String {
    format!("smartlist-rollup:{path}")
}

fn members_container_id(path: &str) -> String {
    format!("smartlist-members:{path}")
}

fn parent_path_of(path: &str) -> Option<String> {
    let slash = path.rfind('/')?;
    if slash <= "smartlist".len() {
        None
    } else {
        Some(path[..slash].to_string())
    }
}

fn last_segment(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

fn container_id_to_bucket_path(container_id: &str) -> Option<String> {
    container_id.strip_prefix("smartlist-members:").map(|value| value.to_string())
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

fn read_retrieval_visibility(map: Option<&JsonMap>) -> String {
    read_string(map, RETRIEVAL_VISIBILITY_KEY).unwrap_or_else(|| RETRIEVAL_VISIBILITY_DEFAULT.to_string())
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

fn normalize_path_part(segment: &str) -> Result<String> {
    static RX: OnceLock<Regex> = OnceLock::new();
    let rx = RX.get_or_init(|| Regex::new("[^a-z0-9-]+").expect("valid SmartList normalization regex"));
    let normalized = rx
        .replace_all(&segment.trim().to_ascii_lowercase(), "-")
        .trim_matches('-')
        .to_string();
    if normalized.is_empty() {
        bail!("invalid SmartList path segment '{}'", segment);
    }
    Ok(normalized)
}

// ── Ordering policies ────────────────────────────────────────────────

pub const ORDERING_ATTACHED_AT: &str = "attached_at";
pub const ORDERING_CREATED_AT: &str = "created_at";
pub const ORDERING_UPDATED_AT: &str = "updated_at";
pub const ORDERING_VOTE_SCORE: &str = "vote_score";
pub const ORDERING_NAME: &str = "name";
pub const ORDERING_MANUAL: &str = "manual";

pub const VALID_ORDERING_POLICIES: &[&str] = &[
    ORDERING_ATTACHED_AT, ORDERING_CREATED_AT, ORDERING_UPDATED_AT,
    ORDERING_VOTE_SCORE, ORDERING_NAME, ORDERING_MANUAL,
];

// ── Recency tiers ────────────────────────────────────────────────────

pub const RECENCY_SHORT_TERM: &str = "smartlist/recency/short-term";
pub const RECENCY_MEDIUM_TERM: &str = "smartlist/recency/medium-term";
pub const RECENCY_LONG_TERM: &str = "smartlist/recency/long-term";
pub const RECENCY_FROZEN: &str = "smartlist/recency/frozen";
pub const RECENCY_TIERS: &[&str] = &[
    RECENCY_SHORT_TERM, RECENCY_MEDIUM_TERM, RECENCY_LONG_TERM, RECENCY_FROZEN,
];
pub const INBOX_PATH: &str = "smartlist/inbox";

// Default ladder policy: max_members per tier, rotation_threshold_hours
pub const DEFAULT_LADDER_POLICY: &[(usize, u64)] = &[
    (50, 24),       // short-term: 50 items, rotate after 24h
    (200, 168),     // medium-term: 200 items, rotate after 7 days
    (1000, 720),    // long-term: 1000 items, rotate after 30 days
    (usize::MAX, 0), // frozen: unlimited, never auto-rotate
];

// ── Result types for new operations ──────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListMembershipsResult {
    pub object_id: String,
    pub bucket_paths: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListCategoryInfo {
    pub name: String,
    pub bucket_path: String,
    pub member_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SmartListBrowseItem {
    pub object_id: String,
    pub object_kind: String,
    pub display_name: String,
    pub sort_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListRecencyTierInfo {
    pub tier: String,
    pub bucket_path: String,
    pub member_count: usize,
    pub max_members: usize,
    pub rotation_threshold_hours: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListRotationResult {
    pub promotions: Vec<SmartListRotationPromotion>,
    pub dry_run: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListRotationPromotion {
    pub object_id: String,
    pub from_tier: String,
    pub to_tier: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListCategorizationResult {
    pub processed: usize,
    pub categorized: usize,
    pub already_categorized: usize,
    pub dry_run: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListGcResult {
    pub removed: Vec<SmartListGcRemoval>,
    pub restored_to_inbox: usize,
    pub dry_run: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartListGcRemoval {
    pub object_id: String,
    pub bucket_path: String,
    pub reason: String,
}

// ── T1: Multi-membership substrate ──────────────────────────────────

/// List all SmartList bucket paths an object is a member of.
pub fn list_memberships(store: &AmsStore, object_id: &str) -> SmartListMembershipsResult {
    let mut paths = store
        .containers_for_member_object(object_id)
        .into_iter()
        .filter_map(|container_id| container_id_to_bucket_path(&container_id))
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    SmartListMembershipsResult {
        object_id: object_id.to_string(),
        bucket_paths: paths,
    }
}

// ── T2: Category namespace ──────────────────────────────────────────

pub const CATEGORY_PREFIX: &str = "smartlist/category/";

/// Create a category bucket at `smartlist/category/<kebab-name>`.
pub fn create_category(
    store: &mut AmsStore,
    name: &str,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<SmartListCategoryInfo> {
    ensure_scaffold(store)?;
    let canonical = normalize_path(&format!("category/{}", name))?;
    if !canonical.starts_with(CATEGORY_PREFIX) {
        bail!("category path must start with '{}'", CATEGORY_PREFIX);
    }
    let bucket = ensure_bucket_path(store, &canonical, SHORT_TERM_DURABILITY, created_by, now_utc)?;
    // Tag with category_kind metadata
    {
        let object = store
            .objects_mut()
            .get_mut(&bucket.object_id)
            .ok_or_else(|| anyhow!("failed to materialize category bucket '{}'", bucket.object_id))?;
        let prov = ensure_prov(object);
        prov.insert("category_kind".to_string(), Value::String("category".to_string()));
    }
    let member_count = store.iterate_forward(&members_container_id(&bucket.path)).len();
    Ok(SmartListCategoryInfo {
        name: last_segment(&bucket.path),
        bucket_path: bucket.path,
        member_count,
    })
}

/// List all categories under `smartlist/category/`.
pub fn list_categories(store: &AmsStore) -> Vec<SmartListCategoryInfo> {
    let category_container = members_container_id("smartlist/category");
    store
        .iterate_forward(&category_container)
        .iter()
        .filter_map(|node| {
            let obj = store.objects().get(&node.object_id)?;
            if obj.object_kind != BUCKET_OBJECT_KIND {
                return None;
            }
            let prov = obj.semantic_payload.as_ref().and_then(|p| p.provenance.as_ref());
            let path = read_string(prov, "path")?;
            if !path.starts_with(CATEGORY_PREFIX) {
                return None;
            }
            let member_count = store.iterate_forward(&members_container_id(&path)).len();
            Some(SmartListCategoryInfo {
                name: last_segment(&path),
                bucket_path: path,
                member_count,
            })
        })
        .collect()
}

/// Attach an object to a category by name.
pub fn attach_to_category(
    store: &mut AmsStore,
    object_id: &str,
    category_name: &str,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<SmartListAttachResult> {
    let category_path = normalize_path(&format!("category/{}", category_name))?;
    attach_member(store, &category_path, object_id, created_by, now_utc)
}

// ── T3: Ordering policy ─────────────────────────────────────────────

/// Set the ordering policy on a bucket's provenance.
pub fn set_ordering_policy(
    store: &mut AmsStore,
    path: &str,
    policy: &str,
    direction: &str,
    tie_breaker: Option<&str>,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<SmartListBucketInfo> {
    let normalized_policy = policy.trim().to_ascii_lowercase();
    if !VALID_ORDERING_POLICIES.contains(&normalized_policy.as_str()) {
        bail!("invalid ordering policy '{}'. Valid: {:?}", policy, VALID_ORDERING_POLICIES);
    }
    let normalized_dir = match direction.trim().to_ascii_lowercase().as_str() {
        "asc" | "ascending" => "asc",
        "desc" | "descending" => "desc",
        other => bail!("invalid direction '{}'. Use 'asc' or 'desc'.", other),
    };
    ensure_scaffold(store)?;
    let bucket = ensure_bucket_path(store, &normalize_path(path)?, SHORT_TERM_DURABILITY, created_by, now_utc)?;
    {
        let object = store
            .objects_mut()
            .get_mut(&bucket.object_id)
            .ok_or_else(|| anyhow!("failed to materialize bucket '{}'", bucket.object_id))?;
        let prov = ensure_prov(object);
        prov.insert("ordering_policy".to_string(), Value::String(normalized_policy));
        prov.insert("ordering_direction".to_string(), Value::String(normalized_dir.to_string()));
        if let Some(tb) = tie_breaker {
            prov.insert("ordering_tie_breaker".to_string(), Value::String(tb.to_string()));
        }
        prov.insert("updated_at".to_string(), Value::String(now_utc.to_rfc3339()));
        object.updated_at = now_utc;
    }
    get_bucket(store, &bucket.path).ok_or_else(|| anyhow!("failed to read bucket '{}'", bucket.path))
}

/// Browse bucket members with ordering applied at read time.
pub fn browse_bucket(store: &AmsStore, path: &str) -> Result<Vec<SmartListBrowseItem>> {
    let canonical = normalize_path(path)?;
    let bucket = get_bucket(store, &canonical)
        .ok_or_else(|| anyhow!("unknown SmartList bucket '{}'", canonical))?;

    let bucket_prov = store.objects().get(&bucket.object_id)
        .and_then(|obj| obj.semantic_payload.as_ref())
        .and_then(|sp| sp.provenance.as_ref());
    let policy = read_string(bucket_prov, "ordering_policy").unwrap_or_else(|| ORDERING_MANUAL.to_string());
    let direction = read_string(bucket_prov, "ordering_direction").unwrap_or_else(|| "asc".to_string());

    let container_id = members_container_id(&canonical);
    let members = store.iterate_forward(&container_id);

    let mut items: Vec<SmartListBrowseItem> = members
        .iter()
        .map(|node| {
            let obj = store.objects().get(&node.object_id);
            let prov = obj.and_then(|o| o.semantic_payload.as_ref()).and_then(|sp| sp.provenance.as_ref());
            let display_name = read_string(prov, "display_name")
                .or_else(|| read_string(prov, "title"))
                .or_else(|| obj.and_then(|o| o.semantic_payload.as_ref()).and_then(|sp| sp.summary.clone()))
                .unwrap_or_else(|| node.object_id.clone());
            let sort_key = match policy.as_str() {
                ORDERING_CREATED_AT => obj.map(|o| o.created_at.to_rfc3339()).unwrap_or_default(),
                ORDERING_UPDATED_AT => obj.map(|o| o.updated_at.to_rfc3339()).unwrap_or_default(),
                ORDERING_VOTE_SCORE => {
                    let score = prov.and_then(|p| p.get("vote_score"))
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    format!("{:020.6}", score)
                }
                ORDERING_NAME => display_name.to_ascii_lowercase(),
                ORDERING_ATTACHED_AT => read_string(prov, "attached_at")
                    .or_else(|| obj.map(|o| o.created_at.to_rfc3339()))
                    .unwrap_or_default(),
                _ => String::new(), // manual: preserve insertion order
            };
            SmartListBrowseItem {
                object_id: node.object_id.clone(),
                object_kind: obj.map(|o| o.object_kind.clone()).unwrap_or_default(),
                display_name,
                sort_key,
            }
        })
        .collect();

    if policy != ORDERING_MANUAL {
        if direction == "desc" {
            items.sort_by(|a, b| b.sort_key.cmp(&a.sort_key));
        } else {
            items.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
        }
    }

    Ok(items)
}

// ── T4: Recency ladder ──────────────────────────────────────────────

/// Bootstrap the 4 recency tier buckets (idempotent).
pub fn bootstrap_recency_ladder(
    store: &mut AmsStore,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<Vec<SmartListRecencyTierInfo>> {
    ensure_scaffold(store)?;
    let mut tiers = Vec::new();
    for (i, &tier_path) in RECENCY_TIERS.iter().enumerate() {
        let bucket = ensure_bucket_path(store, tier_path, SHORT_TERM_DURABILITY, created_by, now_utc)?;
        let (max_members, rotation_threshold_hours) = DEFAULT_LADDER_POLICY[i];
        {
            let object = store
                .objects_mut()
                .get_mut(&bucket.object_id)
                .ok_or_else(|| anyhow!("failed to materialize recency tier '{}'", tier_path))?;
            let prov = ensure_prov(object);
            prov.insert("ladder_policy".to_string(), Value::String("recency_tier".to_string()));
            prov.insert("max_members".to_string(), Value::Number(serde_json::Number::from(max_members as u64)));
            prov.insert("rotation_threshold_hours".to_string(), Value::Number(serde_json::Number::from(rotation_threshold_hours)));
            prov.insert("tier_index".to_string(), Value::Number(serde_json::Number::from(i as u64)));
        }
        // Declare policies on the member container so the enforcement engine
        // rejects/evicts at mutation time rather than relying on ad-hoc checks.
        {
            let mcid = members_container_id(tier_path);
            if let Some(container) = store.containers_mut().get_mut(&mcid) {
                container.policies.ordered_by_recency = true;
                if max_members < usize::MAX {
                    container.policies.max_members = Some(max_members);
                    container.policies.overflow_policy = OverflowPolicy::EvictOldest;
                }
            }
        }
        let member_count = store.iterate_forward(&members_container_id(tier_path)).len();
        tiers.push(SmartListRecencyTierInfo {
            tier: last_segment(tier_path),
            bucket_path: tier_path.to_string(),
            member_count,
            max_members,
            rotation_threshold_hours,
        });
    }
    // Also ensure inbox exists
    ensure_bucket_path(store, INBOX_PATH, SHORT_TERM_DURABILITY, created_by, now_utc)?;
    Ok(tiers)
}

/// List recency tier info.
pub fn list_recency_tiers(store: &AmsStore) -> Vec<SmartListRecencyTierInfo> {
    RECENCY_TIERS
        .iter()
        .enumerate()
        .filter_map(|(i, &tier_path)| {
            let bucket = get_bucket(store, tier_path)?;
            let prov = store.objects().get(&bucket.object_id)
                .and_then(|o| o.semantic_payload.as_ref())
                .and_then(|sp| sp.provenance.as_ref());
            let (default_max, default_hours) = DEFAULT_LADDER_POLICY[i];
            let max_members = prov.and_then(|p| p.get("max_members"))
                .and_then(|v| v.as_u64())
                .map(|v| v as usize)
                .unwrap_or(default_max);
            let rotation_threshold_hours = prov.and_then(|p| p.get("rotation_threshold_hours"))
                .and_then(|v| v.as_u64())
                .unwrap_or(default_hours);
            let member_count = store.iterate_forward(&members_container_id(tier_path)).len();
            Some(SmartListRecencyTierInfo {
                tier: last_segment(tier_path),
                bucket_path: tier_path.to_string(),
                member_count,
                max_members,
                rotation_threshold_hours,
            })
        })
        .collect()
}

// ── T5: Write-time attachment ───────────────────────────────────────

/// Auto-attach a newly created object to inbox + short-term recency tier.
/// Call this after any object creation to apply the built-in attachment rules.
pub fn write_time_attach(
    store: &mut AmsStore,
    object_id: &str,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<Vec<String>> {
    let mut attached_to = Vec::new();

    // Ensure recency ladder exists
    bootstrap_recency_ladder(store, created_by, now_utc)?;

    // Rule 1: Always attach to inbox
    ensure_attached(store, &members_container_id(INBOX_PATH), object_id)?;
    attached_to.push(INBOX_PATH.to_string());

    // Rule 2: Always attach to short-term recency
    ensure_attached(store, &members_container_id(RECENCY_SHORT_TERM), object_id)?;
    attached_to.push(RECENCY_SHORT_TERM.to_string());

    // Rule 3-14: Kind-based attachment rules
    if let Some(obj) = store.objects().get(object_id) {
        let kind = obj.object_kind.clone();
        let auto_categories: Vec<&str> = match kind.as_str() {
            NOTE_OBJECT_KIND => vec!["notes"],
            BUCKET_OBJECT_KIND => vec![], // buckets don't auto-categorize
            ROLLUP_OBJECT_KIND => vec!["rollups"],
            "session" => vec!["sessions"],
            "dream_topic" | "dream_thread" => vec!["dreams"],
            "dream_decision" | "dream_invariant" => vec!["decisions"],
            "task_thread" => vec!["tasks"],
            "agent_tool_prior" => vec!["tool-priors"],
            "route_episode" => vec!["routes"],
            "binder" | "card" => vec!["cards"],
            _ => vec![],
        };
        for cat_name in auto_categories {
            let cat_path = normalize_path(&format!("category/{}", cat_name))?;
            ensure_bucket_path(store, &cat_path, SHORT_TERM_DURABILITY, created_by, now_utc)?;
            ensure_attached(store, &members_container_id(&cat_path), object_id)?;
            attached_to.push(cat_path);
        }
    }

    Ok(attached_to)
}

// ── T6: Rotation maintenance ────────────────────────────────────────

/// Run a rotation sweep across recency tiers: promote objects by age, then by capacity.
pub fn rotate_recency_tiers(
    store: &mut AmsStore,
    now_utc: DateTime<FixedOffset>,
    dry_run: bool,
    created_by: &str,
) -> Result<SmartListRotationResult> {
    bootstrap_recency_ladder(store, created_by, now_utc)?;
    let mut promotions = Vec::new();

    // Process tiers bottom-up (short-term first → promote to medium-term, etc.)
    for i in 0..(RECENCY_TIERS.len() - 1) {
        let source_tier = RECENCY_TIERS[i];
        let target_tier = RECENCY_TIERS[i + 1];
        let (max_members, rotation_threshold_hours) = DEFAULT_LADDER_POLICY[i];

        let members = store.iterate_forward(&members_container_id(source_tier));
        let mut to_promote = Vec::new();

        // Age-based promotion
        for node in &members {
            if let Some(obj) = store.objects().get(&node.object_id) {
                let age_hours = (now_utc - obj.updated_at).num_hours();
                if age_hours >= rotation_threshold_hours as i64 {
                    to_promote.push((node.object_id.clone(), format!("age: {}h >= {}h threshold", age_hours, rotation_threshold_hours)));
                }
            }
        }

        // Capacity-based promotion (oldest first when over max)
        let current_count = members.len();
        if current_count > max_members {
            let overflow = current_count - max_members;
            let mut age_sorted: Vec<_> = members.iter()
                .filter_map(|node| {
                    let obj = store.objects().get(&node.object_id)?;
                    Some((node.object_id.clone(), obj.updated_at))
                })
                .collect();
            age_sorted.sort_by_key(|(_, updated)| *updated);
            for (oid, _) in age_sorted.into_iter().take(overflow) {
                if !to_promote.iter().any(|(id, _)| id == &oid) {
                    to_promote.push((oid, format!("capacity: {} > {} max", current_count, max_members)));
                }
            }
        }

        for (object_id, reason) in to_promote {
            promotions.push(SmartListRotationPromotion {
                object_id: object_id.clone(),
                from_tier: source_tier.to_string(),
                to_tier: target_tier.to_string(),
                reason,
            });
            if !dry_run {
                remove_membership(store, &members_container_id(source_tier), &object_id)?;
                ensure_attached(store, &members_container_id(target_tier), &object_id)?;
            }
        }
    }

    Ok(SmartListRotationResult { promotions, dry_run })
}

// ── T7: Background categorization ───────────────────────────────────

/// Scan inbox and categorize uncategorized objects based on their kind.
pub fn categorize_inbox(
    store: &mut AmsStore,
    now_utc: DateTime<FixedOffset>,
    dry_run: bool,
    created_by: &str,
) -> Result<SmartListCategorizationResult> {
    ensure_scaffold(store)?;
    let inbox_container = members_container_id(INBOX_PATH);
    let members = store.iterate_forward(&inbox_container);
    let mut processed = 0usize;
    let mut categorized = 0usize;
    let mut already_categorized = 0usize;

    let object_ids: Vec<String> = members.iter().map(|n| n.object_id.clone()).collect();

    for object_id in object_ids {
        processed += 1;

        // Check if already in any category
        let current_memberships = list_memberships(store, &object_id);
        let has_category = current_memberships.bucket_paths.iter().any(|p| p.starts_with(CATEGORY_PREFIX));
        if has_category {
            already_categorized += 1;
            continue;
        }

        if dry_run {
            categorized += 1;
            continue;
        }

        // Categorize by object kind
        let kind = store.objects().get(&object_id)
            .map(|o| o.object_kind.clone())
            .unwrap_or_default();
        let cat_names: Vec<&str> = match kind.as_str() {
            NOTE_OBJECT_KIND => vec!["notes"],
            ROLLUP_OBJECT_KIND => vec!["rollups"],
            "session" => vec!["sessions"],
            "dream_topic" | "dream_thread" => vec!["dreams"],
            "dream_decision" | "dream_invariant" => vec!["decisions"],
            "task_thread" => vec!["tasks"],
            "agent_tool_prior" => vec!["tool-priors"],
            "route_episode" => vec!["routes"],
            "binder" | "card" => vec!["cards"],
            _ => vec!["uncategorized"],
        };

        for cat_name in cat_names {
            let cat_path = normalize_path(&format!("category/{}", cat_name))?;
            ensure_bucket_path(store, &cat_path, SHORT_TERM_DURABILITY, created_by, now_utc)?;
            ensure_attached(store, &members_container_id(&cat_path), &object_id)?;
        }
        categorized += 1;

        // Remove from inbox after categorization
        remove_membership(store, &inbox_container, &object_id)?;
    }

    Ok(SmartListCategorizationResult { processed, categorized, already_categorized, dry_run })
}

// ── T8: Query surface ───────────────────────────────────────────────

/// Browse a category with ordering applied.
pub fn browse_category(store: &AmsStore, category_name: &str) -> Result<Vec<SmartListBrowseItem>> {
    let cat_path = normalize_path(&format!("category/{}", category_name))?;
    browse_bucket(store, &cat_path)
}

/// Browse a recency tier with ordering applied.
pub fn browse_tier(store: &AmsStore, tier: &str) -> Result<Vec<SmartListBrowseItem>> {
    let tier_path = match tier.to_ascii_lowercase().as_str() {
        "short-term" => RECENCY_SHORT_TERM,
        "medium-term" => RECENCY_MEDIUM_TERM,
        "long-term" => RECENCY_LONG_TERM,
        "frozen" => RECENCY_FROZEN,
        other => bail!("unknown tier '{}'. Valid: short-term, medium-term, long-term, frozen", other),
    };
    browse_bucket(store, tier_path)
}

/// Intersection query: objects in both a category and a recency tier.
pub fn browse_category_by_tier(
    store: &AmsStore,
    category_name: &str,
    tier: &str,
) -> Result<Vec<SmartListBrowseItem>> {
    let cat_items = browse_category(store, category_name)?;
    let tier_items = browse_tier(store, tier)?;
    let tier_ids: BTreeSet<String> = tier_items.iter().map(|i| i.object_id.clone()).collect();
    Ok(cat_items.into_iter().filter(|i| tier_ids.contains(&i.object_id)).collect())
}

// ── T9: Shadow GC ───────────────────────────────────────────────────

/// GC sweep: remove shadow memberships that have expired TTL, restore orphans to inbox.
pub fn gc_sweep(
    store: &mut AmsStore,
    now_utc: DateTime<FixedOffset>,
    default_ttl_hours: u64,
    dry_run: bool,
    created_by: &str,
) -> Result<SmartListGcResult> {
    ensure_scaffold(store)?;
    let mut removed = Vec::new();
    let mut restored_to_inbox = 0usize;

    // Collect all smartlist member containers
    let all_buckets: Vec<String> = store
        .objects()
        .iter()
        .filter(|(_, obj)| obj.object_kind == BUCKET_OBJECT_KIND)
        .filter_map(|(_, obj)| {
            let prov = obj.semantic_payload.as_ref().and_then(|sp| sp.provenance.as_ref());
            read_string(prov, "path")
        })
        .collect();

    for bucket_path in &all_buckets {
        // Check bucket-level TTL override
        let bucket_obj_id = bucket_object_id(bucket_path);
        let bucket_ttl = store.objects().get(&bucket_obj_id)
            .and_then(|o| o.semantic_payload.as_ref())
            .and_then(|sp| sp.provenance.as_ref())
            .and_then(|p| p.get("ttl_hours"))
            .and_then(|v| v.as_u64())
            .unwrap_or(default_ttl_hours);

        if bucket_ttl == 0 {
            continue; // TTL=0 means no expiry
        }

        let container_id = members_container_id(bucket_path);
        let members: Vec<(String, DateTime<FixedOffset>)> = store
            .iterate_forward(&container_id)
            .iter()
            .filter_map(|node| {
                let obj = store.objects().get(&node.object_id)?;
                Some((node.object_id.clone(), obj.updated_at))
            })
            .collect();

        for (object_id, updated_at) in members {
            let age_hours = (now_utc - updated_at).num_hours();
            if age_hours >= bucket_ttl as i64 {
                removed.push(SmartListGcRemoval {
                    object_id: object_id.clone(),
                    bucket_path: bucket_path.clone(),
                    reason: format!("ttl: {}h >= {}h", age_hours, bucket_ttl),
                });
                if !dry_run {
                    remove_membership(store, &container_id, &object_id)?;

                    // Safety net: if object has no remaining memberships, restore to inbox
                    let remaining = list_memberships(store, &object_id);
                    if remaining.bucket_paths.is_empty() {
                        let inbox_container = members_container_id(INBOX_PATH);
                        ensure_bucket_path(store, INBOX_PATH, SHORT_TERM_DURABILITY, created_by, now_utc)?;
                        ensure_attached(store, &inbox_container, &object_id)?;
                        restored_to_inbox += 1;
                    }
                }
            }
        }
    }

    Ok(SmartListGcResult { removed, restored_to_inbox, dry_run })
}

fn to_anyhow(error: StoreError) -> anyhow::Error {
    anyhow!(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_bucket_tree_and_note_membership() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-15T10:00:00+00:00").unwrap();

        let bucket = create_bucket(&mut store, "architecture/incubation/rust-replatform", true, "tester", now).unwrap();
        assert_eq!(bucket.path, "smartlist/architecture/incubation/rust-replatform");
        assert!(store.has_membership(
            "smartlist-members:smartlist/architecture/incubation",
            "smartlist-bucket:smartlist/architecture/incubation/rust-replatform"
        ));

        let note = create_note(
            &mut store,
            "Title",
            "Body",
            &[bucket.path.clone()],
            true,
            "tester",
            now,
            Some("smartlist-note:test"),
        )
        .unwrap();
        assert_eq!(note.bucket_paths, vec![bucket.path.clone()]);
    }

    #[test]
    fn sets_visibility_recursively() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-15T10:00:00+00:00").unwrap();
        create_bucket(&mut store, "smartlist/root/child", true, "tester", now).unwrap();
        create_note(
            &mut store,
            "Title",
            "Body",
            &[String::from("smartlist/root/child")],
            true,
            "tester",
            now,
            Some("smartlist-note:test"),
        )
        .unwrap();

        let result =
            set_retrieval_visibility(&mut store, "smartlist/root", "scoped", true, true, true, now).unwrap();
        assert_eq!(result.buckets_updated, 2);
        assert_eq!(result.notes_updated, 1);
        assert_eq!(get_note(&store, "smartlist-note:test").unwrap().retrieval_visibility, "scoped");
    }

    #[test]
    fn attach_before_detach_and_move_preserve_order() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-15T10:00:00+00:00").unwrap();
        create_bucket(&mut store, "smartlist/root/10-children/alpha", true, "tester", now).unwrap();
        create_bucket(&mut store, "smartlist/root/10-children/gamma", true, "tester", now).unwrap();
        create_bucket(&mut store, "smartlist/root/10-children/beta", true, "tester", now).unwrap();
        detach_member(&mut store, "smartlist/root/10-children", "smartlist/root/10-children/beta", "tester", now).unwrap();
        attach_member_before(
            &mut store,
            "smartlist/root/10-children",
            "smartlist/root/10-children/beta",
            "smartlist/root/10-children/gamma",
            "tester",
            now,
        )
        .unwrap();

        let children = store
            .iterate_forward("smartlist-members:smartlist/root/10-children")
            .iter()
            .map(|node| node.object_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            children,
            vec![
                "smartlist-bucket:smartlist/root/10-children/alpha",
                "smartlist-bucket:smartlist/root/10-children/beta",
                "smartlist-bucket:smartlist/root/10-children/gamma",
            ]
        );

        move_member(
            &mut store,
            "smartlist/root/10-children",
            "smartlist/root/90-archive",
            "smartlist/root/10-children/beta",
            None,
            "tester",
            now,
        )
        .unwrap();

        let active_children = store
            .iterate_forward("smartlist-members:smartlist/root/10-children")
            .iter()
            .map(|node| node.object_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            active_children,
            vec![
                "smartlist-bucket:smartlist/root/10-children/alpha",
                "smartlist-bucket:smartlist/root/10-children/gamma",
            ]
        );
        let archived = store
            .iterate_forward("smartlist-members:smartlist/root/90-archive")
            .iter()
            .map(|node| node.object_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(archived, vec!["smartlist-bucket:smartlist/root/10-children/beta"]);
    }

    #[test]
    fn set_bucket_fields_updates_runtime_metadata() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-15T10:00:00+00:00").unwrap();
        create_bucket(&mut store, "smartlist/execution-plan/root/00-node", true, "tester", now).unwrap();
        let fields = BTreeMap::from([
            ("kind".to_string(), "work".to_string()),
            ("state".to_string(), "active".to_string()),
            ("active_node_path".to_string(), "smartlist/execution-plan/root".to_string()),
        ]);
        let updated =
            set_bucket_fields(&mut store, "smartlist/execution-plan/root/00-node", &fields, "tester", now).unwrap();
        assert_eq!(updated.path, "smartlist/execution-plan/root/00-node");
        let bucket_obj = store
            .objects()
            .get("smartlist-bucket:smartlist/execution-plan/root/00-node")
            .unwrap();
        let prov = bucket_obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref()).unwrap();
        assert_eq!(read_string(Some(prov), "state").as_deref(), Some("active"));
    }

    #[test]
    fn list_memberships_returns_all_buckets() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-15T10:00:00+00:00").unwrap();
        create_bucket(&mut store, "smartlist/a", false, "tester", now).unwrap();
        create_bucket(&mut store, "smartlist/b", false, "tester", now).unwrap();
        let note = create_note(&mut store, "T", "B", &["smartlist/a".into(), "smartlist/b".into()], false, "tester", now, Some("smartlist-note:multi")).unwrap();
        let result = list_memberships(&store, &note.note_id);
        assert!(result.bucket_paths.contains(&"smartlist/a".to_string()));
        assert!(result.bucket_paths.contains(&"smartlist/b".to_string()));
    }

    #[test]
    fn detach_from_one_bucket_leaves_other() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-15T10:00:00+00:00").unwrap();
        create_bucket(&mut store, "smartlist/x", false, "tester", now).unwrap();
        create_bucket(&mut store, "smartlist/y", false, "tester", now).unwrap();
        let note = create_note(&mut store, "T", "B", &["smartlist/x".into(), "smartlist/y".into()], false, "tester", now, Some("smartlist-note:detach-test")).unwrap();
        detach_member(&mut store, "smartlist/x", &note.note_id, "tester", now).unwrap();
        let result = list_memberships(&store, &note.note_id);
        assert!(!result.bucket_paths.contains(&"smartlist/x".to_string()));
        assert!(result.bucket_paths.contains(&"smartlist/y".to_string()));
    }

    #[test]
    fn category_create_list_attach() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-15T10:00:00+00:00").unwrap();
        let cat = create_category(&mut store, "test-cat", "tester", now).unwrap();
        assert_eq!(cat.bucket_path, "smartlist/category/test-cat");
        let cats = list_categories(&store);
        assert_eq!(cats.len(), 1);
        assert_eq!(cats[0].name, "test-cat");

        let note = create_note(&mut store, "T", "B", &[], false, "tester", now, Some("smartlist-note:cat-test")).unwrap();
        attach_to_category(&mut store, &note.note_id, "test-cat", "tester", now).unwrap();
        let memberships = list_memberships(&store, &note.note_id);
        assert!(memberships.bucket_paths.contains(&"smartlist/category/test-cat".to_string()));
    }

    #[test]
    fn ordering_policy_and_browse() {
        let mut store = AmsStore::new();
        let t1 = DateTime::parse_from_rfc3339("2026-03-15T10:00:00+00:00").unwrap();
        let t2 = DateTime::parse_from_rfc3339("2026-03-15T11:00:00+00:00").unwrap();
        create_bucket(&mut store, "smartlist/ordered", false, "tester", t1).unwrap();
        create_note(&mut store, "Beta", "b", &["smartlist/ordered".into()], false, "tester", t1, Some("smartlist-note:beta")).unwrap();
        create_note(&mut store, "Alpha", "a", &["smartlist/ordered".into()], false, "tester", t2, Some("smartlist-note:alpha")).unwrap();

        set_ordering_policy(&mut store, "smartlist/ordered", "name", "asc", None, "tester", t2).unwrap();
        let items = browse_bucket(&store, "smartlist/ordered").unwrap();
        let names: Vec<&str> = items.iter().map(|i| i.display_name.as_str()).collect();
        assert!(names.windows(2).all(|w| w[0] <= w[1]), "expected sorted by name asc: {:?}", names);
    }

    #[test]
    fn recency_ladder_bootstrap_and_list() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-15T10:00:00+00:00").unwrap();
        let tiers = bootstrap_recency_ladder(&mut store, "tester", now).unwrap();
        assert_eq!(tiers.len(), 4);
        assert_eq!(tiers[0].tier, "short-term");
        assert_eq!(tiers[3].tier, "frozen");

        let listed = list_recency_tiers(&store);
        assert_eq!(listed.len(), 4);
    }

    #[test]
    fn write_time_attach_adds_to_inbox_and_short_term() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-15T10:00:00+00:00").unwrap();
        let note = create_note(&mut store, "T", "B", &[], false, "tester", now, Some("smartlist-note:wt-test")).unwrap();
        let attached = write_time_attach(&mut store, &note.note_id, "tester", now).unwrap();
        assert!(attached.contains(&INBOX_PATH.to_string()));
        assert!(attached.contains(&RECENCY_SHORT_TERM.to_string()));
    }

    #[test]
    fn categorize_inbox_assigns_categories() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-15T10:00:00+00:00").unwrap();
        let note = create_note(&mut store, "T", "B", &[], false, "tester", now, Some("smartlist-note:inbox-cat")).unwrap();
        write_time_attach(&mut store, &note.note_id, "tester", now).unwrap();
        let result = categorize_inbox(&mut store, now, false, "tester").unwrap();
        assert!(result.categorized > 0 || result.already_categorized > 0);
    }
}
