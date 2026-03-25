use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::Result;
use chrono::{DateTime, FixedOffset};
use serde::Serialize;
use serde_json::Value;

use crate::model::ObjectRecord;
use crate::retrieval::tokenize;
use crate::smartlist_write::{
    attach_member, create_bucket, get_bucket, move_member, set_bucket_fields, BUCKET_OBJECT_KIND,
    NOTE_OBJECT_KIND, RETRIEVAL_VISIBILITY_KEY, RETRIEVAL_VISIBILITY_SUPPRESSED, ROLLUP_OBJECT_KIND,
};
use crate::store::AmsStore;

pub const AGENT_MEMORY_SMARTLIST_ROOT_PATH: &str = "smartlist/agent-memory";
pub const FRESHNESS_SMARTLIST_ROOT_PATH: &str = "smartlist/agent-memory/freshness";
pub const FRESHNESS_LANE_ROOT_PATH: &str = "smartlist/agent-memory/freshness/lanes";
pub const FRESHNESS_STATUS_ACTIVE: &str = "active";
pub const FRESHNESS_STATUS_HISTORICAL: &str = "historical";
pub const FRESHNESS_STATUS_FROZEN: &str = "frozen";
pub const FRESHNESS_HEAD_LANE_MAX: usize = 11;

const DEEP_MEMORY_TOKENS: &[&str] = &["deep", "frozen", "archive", "old"];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FreshnessLaneInfo {
    pub path: String,
    pub object_id: String,
    pub status: String,
    pub topic_key: String,
    pub member_count: usize,
    pub created_at: DateTime<FixedOffset>,
    pub updated_at: DateTime<FixedOffset>,
    pub index: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FreshnessObjectPosition {
    pub lane_path: String,
    pub lane_status: String,
    pub topic_key: String,
    pub lane_index: usize,
    pub temperature_label: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum FreshnessWriteAction {
    CreateBucket { path: String, durable: bool },
    SetBucketFields { path: String, fields: BTreeMap<String, String> },
    AttachMember { path: String, member_ref: String },
    MoveMember {
        source_path: String,
        target_path: String,
        member_ref: String,
        before_member_ref: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FreshnessPrepareResult {
    pub positions: HashMap<String, FreshnessObjectPosition>,
    pub frozen_exclusions: HashSet<String>,
    pub admissions: usize,
    pub actions: Vec<FreshnessWriteAction>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FreshnessTouchResult {
    pub admissions: usize,
    pub actions: Vec<FreshnessWriteAction>,
}

pub fn prepare_snapshot_freshness(
    snapshot: &mut AmsStore,
    query: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<FreshnessPrepareResult> {
    let mut actions = Vec::new();
    ensure_freshness_scaffold(snapshot, now_utc, &mut actions)?;

    let unseen = enumerate_freshness_candidate_object_ids(snapshot)
        .into_iter()
        .filter(|object_id| !has_freshness_membership(snapshot, object_id))
        .collect::<Vec<_>>();
    let topic_key = build_topic_key(&build_free_text_tokens(query));
    let admissions = admit_objects_to_freshness(snapshot, &unseen, now_utc, "ingest", &topic_key, &mut actions)?;

    let include_frozen = is_deep_memory_query(query);
    Ok(FreshnessPrepareResult {
        positions: build_freshness_positions(snapshot, include_frozen),
        frozen_exclusions: if include_frozen {
            HashSet::new()
        } else {
            build_frozen_only_object_ids(snapshot)
        },
        admissions,
        actions,
    })
}

pub fn touch_snapshot_freshness(
    snapshot: &mut AmsStore,
    object_ids: &[String],
    query: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<FreshnessTouchResult> {
    let mut actions = Vec::new();
    ensure_freshness_scaffold(snapshot, now_utc, &mut actions)?;
    let topic_key = build_topic_key(&build_free_text_tokens(query));
    let admissions = admit_objects_to_freshness(snapshot, object_ids, now_utc, "touch", &topic_key, &mut actions)?;
    Ok(FreshnessTouchResult { admissions, actions })
}

pub fn is_deep_memory_query(query: &str) -> bool {
    build_free_text_tokens(query)
        .iter()
        .any(|token| DEEP_MEMORY_TOKENS.contains(&token.as_str()))
}

pub fn build_freshness_positions(
    snapshot: &AmsStore,
    include_frozen: bool,
) -> HashMap<String, FreshnessObjectPosition> {
    let mut positions = HashMap::new();
    for lane in get_freshness_lanes(snapshot, include_frozen) {
        let label = freshness_temperature_label(&lane.status, lane.index);
        for member_id in lane_member_object_ids(snapshot, &lane.path) {
            if is_freshness_internal_object(snapshot, &member_id) || positions.contains_key(&member_id) {
                continue;
            }
            positions.insert(
                member_id,
                FreshnessObjectPosition {
                    lane_path: lane.path.clone(),
                    lane_status: lane.status.clone(),
                    topic_key: lane.topic_key.clone(),
                    lane_index: lane.index,
                    temperature_label: label.clone(),
                },
            );
        }
    }
    positions
}

pub fn build_frozen_only_object_ids(snapshot: &AmsStore) -> HashSet<String> {
    let accessible = build_freshness_positions(snapshot, false)
        .into_keys()
        .collect::<HashSet<_>>();
    let mut frozen_only = HashSet::new();
    for lane in get_freshness_lanes(snapshot, true)
        .into_iter()
        .filter(|lane| lane.status == FRESHNESS_STATUS_FROZEN)
    {
        for member_id in lane_member_object_ids(snapshot, &lane.path) {
            if !accessible.contains(&member_id) {
                frozen_only.insert(member_id);
            }
        }
    }
    frozen_only
}

pub fn freshness_lane_boost(position: Option<&FreshnessObjectPosition>) -> f64 {
    match position.map(|value| value.temperature_label.as_str()) {
        Some("hot") => 0.22,
        Some("warm") => 0.12,
        Some("cold") => 0.06,
        Some("frozen") => 0.03,
        _ => 0.0,
    }
}

pub fn is_freshness_internal_path(path: &str) -> bool {
    path == AGENT_MEMORY_SMARTLIST_ROOT_PATH
        || path.starts_with(&format!("{AGENT_MEMORY_SMARTLIST_ROOT_PATH}/"))
}

pub fn is_freshness_internal_object(snapshot: &AmsStore, object_id: &str) -> bool {
    let Some(obj) = snapshot.objects().get(object_id) else {
        return object_id.starts_with(&format!("smartlist-members:{FRESHNESS_SMARTLIST_ROOT_PATH}"));
    };

    let provenance = obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
    match obj.object_kind.as_str() {
        BUCKET_OBJECT_KIND => read_string(provenance, "path")
            .as_deref()
            .is_some_and(is_freshness_internal_path),
        ROLLUP_OBJECT_KIND => read_string(provenance, "bucket_path")
            .as_deref()
            .is_some_and(is_freshness_internal_path),
        _ => object_id.starts_with(&format!("smartlist-members:{FRESHNESS_SMARTLIST_ROOT_PATH}")),
    }
}

fn ensure_freshness_scaffold(
    snapshot: &mut AmsStore,
    now_utc: DateTime<FixedOffset>,
    actions: &mut Vec<FreshnessWriteAction>,
) -> Result<()> {
    create_bucket(snapshot, FRESHNESS_SMARTLIST_ROOT_PATH, true, "agent-memory", now_utc)?;
    create_bucket(snapshot, FRESHNESS_LANE_ROOT_PATH, true, "agent-memory", now_utc)?;
    actions.push(FreshnessWriteAction::CreateBucket {
        path: AGENT_MEMORY_SMARTLIST_ROOT_PATH.to_string(),
        durable: true,
    });
    actions.push(FreshnessWriteAction::CreateBucket {
        path: FRESHNESS_SMARTLIST_ROOT_PATH.to_string(),
        durable: true,
    });
    actions.push(FreshnessWriteAction::CreateBucket {
        path: FRESHNESS_LANE_ROOT_PATH.to_string(),
        durable: true,
    });

    let suppression_fields = BTreeMap::from([(
        RETRIEVAL_VISIBILITY_KEY.to_string(),
        RETRIEVAL_VISIBILITY_SUPPRESSED.to_string(),
    )]);
    set_bucket_fields(
        snapshot,
        AGENT_MEMORY_SMARTLIST_ROOT_PATH,
        &suppression_fields,
        "agent-memory",
        now_utc,
    )?;
    set_bucket_fields(snapshot, FRESHNESS_SMARTLIST_ROOT_PATH, &suppression_fields, "agent-memory", now_utc)?;
    set_bucket_fields(snapshot, FRESHNESS_LANE_ROOT_PATH, &suppression_fields, "agent-memory", now_utc)?;
    actions.push(FreshnessWriteAction::SetBucketFields {
        path: AGENT_MEMORY_SMARTLIST_ROOT_PATH.to_string(),
        fields: suppression_fields.clone(),
    });
    actions.push(FreshnessWriteAction::SetBucketFields {
        path: FRESHNESS_SMARTLIST_ROOT_PATH.to_string(),
        fields: suppression_fields.clone(),
    });
    actions.push(FreshnessWriteAction::SetBucketFields {
        path: FRESHNESS_LANE_ROOT_PATH.to_string(),
        fields: suppression_fields,
    });

    if get_freshness_lanes(snapshot, true).is_empty() {
        create_freshness_head_lane(snapshot, now_utc, "", "bootstrap", actions)?;
    }
    Ok(())
}

fn admit_objects_to_freshness(
    snapshot: &mut AmsStore,
    object_ids: &[String],
    now_utc: DateTime<FixedOffset>,
    admission_reason: &str,
    topic_key: &str,
    actions: &mut Vec<FreshnessWriteAction>,
) -> Result<usize> {
    let mut admitted = 0;
    let mut seen = HashSet::new();
    for object_id in object_ids {
        if object_id.trim().is_empty() || !seen.insert(object_id.clone()) {
            continue;
        }
        admitted += admit_object_to_freshness(snapshot, object_id, now_utc, admission_reason, topic_key, actions)?;
    }
    Ok(admitted)
}

fn admit_object_to_freshness(
    snapshot: &mut AmsStore,
    object_id: &str,
    now_utc: DateTime<FixedOffset>,
    admission_reason: &str,
    topic_key: &str,
    actions: &mut Vec<FreshnessWriteAction>,
) -> Result<usize> {
    if (!snapshot.objects().contains_key(object_id) && !snapshot.containers().contains_key(object_id))
        || is_freshness_internal_object(snapshot, object_id)
    {
        return Ok(0);
    }

    let head = ensure_freshness_head_lane(snapshot, now_utc, topic_key, admission_reason, Some(object_id), actions)?;
    let head_member_ids = lane_member_object_ids(snapshot, &head.path);
    if head_member_ids.iter().any(|member_id| member_id == object_id) {
        return Ok(0);
    }

    attach_member(snapshot, &head.path, object_id, "agent-memory", now_utc)?;
    actions.push(FreshnessWriteAction::AttachMember {
        path: head.path.clone(),
        member_ref: object_id.to_string(),
    });
    update_freshness_lane_fields(
        snapshot,
        &head.path,
        FRESHNESS_STATUS_ACTIVE,
        if topic_key.is_empty() { &head.topic_key } else { topic_key },
        admission_reason,
        now_utc,
        actions,
    )?;
    Ok(1)
}

fn ensure_freshness_head_lane(
    snapshot: &mut AmsStore,
    now_utc: DateTime<FixedOffset>,
    topic_key: &str,
    reason: &str,
    incoming_object_id: Option<&str>,
    actions: &mut Vec<FreshnessWriteAction>,
) -> Result<FreshnessLaneInfo> {
    let lanes = get_freshness_lanes(snapshot, true);
    if lanes.is_empty() {
        return create_freshness_head_lane(snapshot, now_utc, topic_key, reason, actions);
    }

    let head = lanes[0].clone();
    let head_member_ids = lane_member_object_ids(snapshot, &head.path);
    let incoming_distinct = incoming_object_id
        .is_some_and(|object_id| !head_member_ids.iter().any(|member_id| member_id == object_id));

    if head.status == FRESHNESS_STATUS_FROZEN
        || reason == "topic-shift"
        || reason == "user-shelve"
        || (incoming_distinct && head.member_count >= FRESHNESS_HEAD_LANE_MAX)
        || should_rotate_for_topic_shift(&head.topic_key, topic_key, head.member_count)
    {
        let rotate_reason = if reason == "topic-shift" || reason == "user-shelve" {
            reason
        } else if incoming_distinct && head.member_count >= FRESHNESS_HEAD_LANE_MAX {
            "overflow"
        } else {
            "topic-shift"
        };
        return create_freshness_head_lane(snapshot, now_utc, topic_key, rotate_reason, actions);
    }

    if head.status != FRESHNESS_STATUS_ACTIVE {
        update_freshness_lane_fields(
            snapshot,
            &head.path,
            FRESHNESS_STATUS_ACTIVE,
            if topic_key.is_empty() { &head.topic_key } else { topic_key },
            reason,
            now_utc,
            actions,
        )?;
    }

    Ok(get_freshness_lanes(snapshot, true)
        .into_iter()
        .find(|lane| lane.path == head.path)
        .unwrap_or(head))
}

fn create_freshness_head_lane(
    snapshot: &mut AmsStore,
    now_utc: DateTime<FixedOffset>,
    topic_key: &str,
    reason: &str,
    actions: &mut Vec<FreshnessWriteAction>,
) -> Result<FreshnessLaneInfo> {
    let lanes = get_freshness_lanes(snapshot, true);
    if let Some(head) = lanes.first().filter(|lane| lane.status == FRESHNESS_STATUS_ACTIVE) {
        update_freshness_lane_fields(
            snapshot,
            &head.path,
            FRESHNESS_STATUS_HISTORICAL,
            &head.topic_key,
            reason,
            now_utc,
            actions,
        )?;
    }

    let lane_suffix = format!("{}", uuid::Uuid::new_v4().simple());
    let lane_path = format!("{FRESHNESS_LANE_ROOT_PATH}/lane-{}-{}", now_utc.format("%Y%m%d%H%M%S"), &lane_suffix[..8]);
    create_bucket(snapshot, &lane_path, true, "agent-memory", now_utc)?;
    actions.push(FreshnessWriteAction::CreateBucket {
        path: lane_path.clone(),
        durable: true,
    });
    update_freshness_lane_fields(
        snapshot,
        &lane_path,
        FRESHNESS_STATUS_ACTIVE,
        topic_key,
        reason,
        now_utc,
        actions,
    )?;

    let lane_root_members = lane_member_bucket_paths(snapshot, FRESHNESS_LANE_ROOT_PATH);
    if let Some(first_path) = lane_root_members
        .into_iter()
        .find(|path| path != &lane_path)
    {
        move_member(
            snapshot,
            FRESHNESS_LANE_ROOT_PATH,
            FRESHNESS_LANE_ROOT_PATH,
            &lane_path,
            Some(&first_path),
            "agent-memory",
            now_utc,
        )?;
        actions.push(FreshnessWriteAction::MoveMember {
            source_path: FRESHNESS_LANE_ROOT_PATH.to_string(),
            target_path: FRESHNESS_LANE_ROOT_PATH.to_string(),
            member_ref: lane_path.clone(),
            before_member_ref: Some(first_path),
        });
    }

    Ok(get_freshness_lanes(snapshot, true)
        .into_iter()
        .find(|lane| lane.path == lane_path)
        .expect("new freshness lane must be visible"))
}

fn update_freshness_lane_fields(
    snapshot: &mut AmsStore,
    lane_path: &str,
    status: &str,
    topic_key: &str,
    admission_reason: &str,
    now_utc: DateTime<FixedOffset>,
    actions: &mut Vec<FreshnessWriteAction>,
) -> Result<()> {
    let lane_object_id = format!("smartlist-bucket:{lane_path}");
    let created_at = snapshot
        .objects()
        .get(&lane_object_id)
        .and_then(|obj| obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref()))
        .and_then(|provenance| read_string(Some(provenance), "created_at"))
        .unwrap_or_else(|| now_utc.to_rfc3339());
    let mut fields = BTreeMap::new();
    fields.insert("lane_id".to_string(), lane_path.rsplit('/').next().unwrap_or(lane_path).to_string());
    fields.insert("status".to_string(), status.to_string());
    fields.insert("topic_key".to_string(), topic_key.to_string());
    fields.insert("admission_reason".to_string(), admission_reason.to_string());
    fields.insert("member_count".to_string(), count_freshness_lane_members(snapshot, lane_path).to_string());
    fields.insert(RETRIEVAL_VISIBILITY_KEY.to_string(), RETRIEVAL_VISIBILITY_SUPPRESSED.to_string());
    fields.insert("updated_at".to_string(), now_utc.to_rfc3339());
    fields.insert("created_at".to_string(), created_at);
    set_bucket_fields(snapshot, lane_path, &fields, "agent-memory", now_utc)?;
    actions.push(FreshnessWriteAction::SetBucketFields {
        path: lane_path.to_string(),
        fields,
    });
    Ok(())
}

fn get_freshness_lanes(snapshot: &AmsStore, include_frozen: bool) -> Vec<FreshnessLaneInfo> {
    lane_member_bucket_paths(snapshot, FRESHNESS_LANE_ROOT_PATH)
        .into_iter()
        .filter_map(|path| get_bucket(snapshot, &path))
        .filter_map(|bucket| {
            let prov = snapshot
                .objects()
                .get(&bucket.object_id)
                .and_then(|obj| obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref()))?;
            let status = read_string(Some(prov), "status").unwrap_or_else(|| FRESHNESS_STATUS_HISTORICAL.to_string());
            if !include_frozen && status == FRESHNESS_STATUS_FROZEN {
                return None;
            }
            Some((bucket, prov.clone(), status))
        })
        .enumerate()
        .map(|(index, (bucket, prov, status))| FreshnessLaneInfo {
            path: bucket.path.clone(),
            object_id: bucket.object_id.clone(),
            status,
            topic_key: read_string(Some(&prov), "topic_key").unwrap_or_default(),
            member_count: count_freshness_lane_members(snapshot, &bucket.path),
            created_at: read_date(Some(&prov), "created_at").unwrap_or(bucket.created_at),
            updated_at: read_date(Some(&prov), "updated_at").unwrap_or(bucket.updated_at),
            index,
        })
        .collect()
}

fn lane_member_bucket_paths(snapshot: &AmsStore, path: &str) -> Vec<String> {
    let container_id = format!("smartlist-members:{path}");
    if !snapshot.containers().contains_key(&container_id) {
        return Vec::new();
    }
    snapshot
        .iterate_forward(&container_id)
        .into_iter()
        .filter_map(|link| snapshot.objects().get(&link.object_id))
        .filter(|obj| obj.object_kind == BUCKET_OBJECT_KIND)
        .filter_map(|obj| {
            let path = obj
                .semantic_payload
                .as_ref()
                .and_then(|payload| payload.provenance.as_ref())
                .and_then(|provenance| read_string(Some(provenance), "path"))?;
            path.starts_with(&format!("{FRESHNESS_LANE_ROOT_PATH}/")).then_some(path)
        })
        .collect()
}

fn lane_member_object_ids(snapshot: &AmsStore, lane_path: &str) -> Vec<String> {
    let container_id = format!("smartlist-members:{lane_path}");
    if !snapshot.containers().contains_key(&container_id) {
        return Vec::new();
    }
    snapshot
        .iterate_forward(&container_id)
        .into_iter()
        .map(|link| link.object_id.clone())
        .collect()
}

fn count_freshness_lane_members(snapshot: &AmsStore, lane_path: &str) -> usize {
    lane_member_object_ids(snapshot, lane_path)
        .into_iter()
        .filter(|member_id| !is_freshness_internal_object(snapshot, member_id))
        .count()
}

fn enumerate_freshness_candidate_object_ids(snapshot: &AmsStore) -> Vec<String> {
    let mut candidates = snapshot
        .objects()
        .values()
        .filter(|obj| is_freshness_candidate_object(snapshot, obj))
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| right.created_at.cmp(&left.created_at))
            .then_with(|| left.object_id.cmp(&right.object_id))
    });
    candidates
        .into_iter()
        .map(|obj| obj.object_id.clone())
        .collect()
}

fn is_freshness_candidate_object(snapshot: &AmsStore, obj: &ObjectRecord) -> bool {
    if is_freshness_internal_object(snapshot, &obj.object_id) {
        return false;
    }

    match obj.object_kind.as_str() {
        "lesson" | "chat_message" | "task_thread" | "task_checkpoint" | "task_artifact" | NOTE_OBJECT_KIND
        | ROLLUP_OBJECT_KIND => true,
        BUCKET_OBJECT_KIND => obj
            .semantic_payload
            .as_ref()
            .and_then(|payload| payload.provenance.as_ref())
            .and_then(|provenance| read_string(Some(provenance), "path"))
            .as_deref()
            .is_some_and(|path| !is_freshness_internal_path(path)),
        "container" => snapshot
            .containers()
            .get(&obj.object_id)
            .is_some_and(|container| container.container_kind == "chat_session"),
        _ => false,
    }
}

fn has_freshness_membership(snapshot: &AmsStore, object_id: &str) -> bool {
    snapshot
        .containers_for_member_object(object_id)
        .into_iter()
        .any(|container_id| container_id.starts_with(&format!("smartlist-members:{FRESHNESS_LANE_ROOT_PATH}/")))
}

fn should_rotate_for_topic_shift(current_topic_key: &str, incoming_topic_key: &str, member_count: usize) -> bool {
    if member_count < 4 || current_topic_key.trim().is_empty() || incoming_topic_key.trim().is_empty() {
        return false;
    }
    let current = current_topic_key
        .split_whitespace()
        .map(|token| token.to_string())
        .collect::<HashSet<_>>();
    let incoming = incoming_topic_key
        .split_whitespace()
        .map(|token| token.to_string())
        .collect::<HashSet<_>>();
    current.is_disjoint(&incoming)
}

fn freshness_temperature_label(status: &str, lane_index: usize) -> String {
    if status == FRESHNESS_STATUS_FROZEN {
        "frozen".to_string()
    } else if lane_index == 0 {
        "hot".to_string()
    } else if lane_index <= 2 {
        "warm".to_string()
    } else {
        "cold".to_string()
    }
}

fn build_topic_key(tokens: &[String]) -> String {
    tokens.iter().take(6).cloned().collect::<Vec<_>>().join(" ")
}

fn build_free_text_tokens(text: &str) -> Vec<String> {
    tokenize(text)
        .into_iter()
        .filter(|token| token.len() >= 3)
        .collect()
}

pub fn build_object_topic_tokens(snapshot: &AmsStore, object_id: &str) -> Vec<String> {
    let Some(obj) = snapshot.objects().get(object_id) else {
        return Vec::new();
    };
    let provenance = obj.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
    let mut inputs = vec![
        obj.semantic_payload
            .as_ref()
            .and_then(|payload| payload.summary.clone())
            .unwrap_or_default(),
        read_string(provenance, "title").unwrap_or_default(),
        read_string(provenance, "text").unwrap_or_default(),
        read_string(provenance, "path").unwrap_or_default(),
        obj.object_id.clone(),
    ];
    if obj.object_kind == "container" {
        if let Some(container) = snapshot.containers().get(object_id) {
            if container.container_kind == "chat_session" {
                inputs.push(read_string(container.metadata.as_ref(), "title").unwrap_or_default());
            }
        }
    }
    build_free_text_tokens(&inputs.join(" "))
}

fn read_string(map: Option<&BTreeMap<String, Value>>, key: &str) -> Option<String> {
    match map?.get(key)? {
        Value::String(value) => Some(value.clone()),
        value => Some(value.to_string()),
    }
}

fn read_date(map: Option<&BTreeMap<String, Value>>, key: &str) -> Option<DateTime<FixedOffset>> {
    match map?.get(key)? {
        Value::String(raw) => DateTime::parse_from_rfc3339(raw).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use crate::model::SemanticPayload;

    use super::*;

    fn make_snapshot() -> AmsStore {
        let mut store = AmsStore::new();
        store.create_container("chat-session:s1", "chat_session", "chat_session").unwrap();
        for i in 0..12 {
            let suffix = format!("{i:02}");
            store
                .upsert_object(
                    format!("chat-msg:s1-{suffix}"),
                    "chat_message",
                    None,
                    Some(SemanticPayload {
                        embedding: None,
                        tags: None,
                        summary: Some(format!("Freshness token{suffix} shared memory")),
                        provenance: Some(BTreeMap::from([
                            ("text".to_string(), json!(format!("Freshness token{suffix} shared memory"))),
                            ("ts".to_string(), json!("2026-03-13T19:00:00Z")),
                        ])),
                    }),
                    None,
                )
                .unwrap();
            store
                .add_object("chat-session:s1", &format!("chat-msg:s1-{suffix}"), None, None)
                .unwrap();
        }
        store
    }

    #[test]
    fn prepare_snapshot_freshness_bootstraps_and_rotates() {
        let mut snapshot = make_snapshot();
        let result = prepare_snapshot_freshness(&mut snapshot, "token00 shared memory", Utc::now().fixed_offset()).unwrap();
        let lanes = get_freshness_lanes(&snapshot, true);
        assert!(result.admissions >= 12);
        assert!(lanes.len() >= 2);
        assert_eq!(lanes[0].status, FRESHNESS_STATUS_ACTIVE);
        assert_eq!(lanes[1].status, FRESHNESS_STATUS_HISTORICAL);
    }

    #[test]
    fn frozen_only_objects_are_excluded_from_default_positions() {
        let mut snapshot = make_snapshot();
        prepare_snapshot_freshness(&mut snapshot, "token00 shared memory", Utc::now().fixed_offset()).unwrap();
        let head = get_freshness_lanes(&snapshot, true).into_iter().next().unwrap();
        update_freshness_lane_fields(
            &mut snapshot,
            &head.path,
            FRESHNESS_STATUS_FROZEN,
            "",
            "freeze",
            Utc::now().fixed_offset(),
            &mut Vec::new(),
        )
        .unwrap();
        let positions = build_freshness_positions(&snapshot, false);
        let frozen = build_frozen_only_object_ids(&snapshot);
        assert!(positions.is_empty() || positions.values().all(|value| value.lane_status != FRESHNESS_STATUS_FROZEN));
        assert!(!frozen.is_empty());
    }
}
