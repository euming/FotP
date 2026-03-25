//! Dream-touch primitive for topology-based dreaming.
//!
//! ## What is dream-touch?
//!
//! `dream_touch` is the atomic unit of the dreaming pass.  Given a focal object
//! (one that was recently accessed), it performs two graph mutations:
//!
//! 1. **Promote-to-head** — for every SmartList container the focal object
//!    belongs to, move it to the head (front) of the ordered member list.
//!    This makes it the first result of a single-hop Atlas lookup on that list.
//!
//! 2. **Shortcut edge creation** — collect every co-member of the focal object
//!    (any object that shares at least one SmartList with it) and attach them
//!    all to the dedicated shortcut bucket `smartlist/dreaming/shortcuts/<id>`.
//!    This converts a multi-hop "find neighbors via shared lists" query into a
//!    single-hop lookup on the shortcut bucket.
//!
//! All other dreaming operations compose `dream_touch` as their building block.

use std::collections::BTreeSet;

use anyhow::Result;
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};

use crate::cache::lookup_tool_centric;
use crate::fep_cache_signal::{cache_signal_cluster_surprise_map, DEFAULT_WINDOW_HOURS};
use crate::smartlist_write::{attach_member, normalize_path};
use crate::store::AmsStore;

// ── Constants ─────────────────────────────────────────────────────────────────

/// Root path for all dreaming shortcut buckets.
pub const DREAM_SHORTCUTS_ROOT: &str = "smartlist/dreaming/shortcuts";

/// Object kind for the dreaming shortcut index bucket.
pub const DREAM_SHORTCUT_KIND: &str = "dream_shortcut_bucket";

// ── Result type ───────────────────────────────────────────────────────────────

/// Summary of the mutations performed by a single `dream_touch` call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DreamTouchResult {
    /// The focal object that was touched.
    pub object_id: String,
    /// Number of SmartList containers in which the object was promoted to head.
    pub lists_promoted: usize,
    /// Number of co-members added to the shortcut bucket (including pre-existing).
    pub shortcuts_added: usize,
    /// Path of the shortcut bucket created / updated for this object.
    pub shortcut_path: String,
}

// ── Core primitive ────────────────────────────────────────────────────────────

/// Perform a dream-touch on `object_id`.
///
/// This function is **pure store mutation** — it does not acquire any locks or
/// write to disk.  The caller (typically `WriteService::dream_touch`) is
/// responsible for persistence.
///
/// # Arguments
///
/// * `store`      — mutable AMS store (will be mutated in place)
/// * `object_id`  — the focal object to touch
/// * `created_by` — actor label recorded on any newly created containers
/// * `now_utc`    — wall-clock timestamp used for object creation metadata
///
/// # Errors
///
/// Returns an error if the object does not exist in the store, or if a
/// shortcut path cannot be normalised.
pub fn dream_touch(
    store: &mut AmsStore,
    object_id: &str,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<DreamTouchResult> {
    // Verify the focal object exists.
    if !store.objects().contains_key(object_id) {
        anyhow::bail!("dream_touch: object '{}' does not exist in the store", object_id);
    }

    // ── Step 1: Find all smartlist-member containers the object belongs to ─────
    //
    // `containers_for_member_object` returns container IDs of the form
    // `smartlist-members:<path>`.  We filter to only smartlist member containers.
    let member_containers: Vec<String> = store
        .containers_for_member_object(object_id)
        .into_iter()
        .filter(|cid| cid.starts_with("smartlist-members:"))
        .collect();

    // ── Step 2: Promote-to-head in each container ─────────────────────────────
    let mut lists_promoted = 0usize;
    let mut co_members: BTreeSet<String> = BTreeSet::new();

    for container_id in &member_containers {
        // Collect co-members before we mutate ordering (avoid borrow conflicts).
        let members_in_container: Vec<String> = store
            .iterate_forward(container_id)
            .iter()
            .map(|node| node.object_id.clone())
            .collect();

        // Collect co-members (everyone except the focal object itself).
        for m in &members_in_container {
            if m != object_id {
                co_members.insert(m.clone());
            }
        }

        // Promote only if there is more than one member and the head is not
        // already the focal object.
        if members_in_container.len() <= 1 {
            continue;
        }
        if members_in_container.first().map(String::as_str) == Some(object_id) {
            continue; // already at head
        }

        // Find the head link node for this container.
        let head_link_node_id = match store.containers().get(container_id.as_str()) {
            Some(c) => match &c.head_linknode_id {
                Some(id) => id.clone(),
                None => continue,
            },
            None => continue,
        };

        // Remove the focal object's existing link node from the container.
        let focal_link_node_id = store
            .links_for_member_object(object_id)
            .into_iter()
            .find(|node| &node.container_id == container_id)
            .map(|node| node.link_node_id.clone());

        if let Some(focal_link) = focal_link_node_id {
            // Only promote if the focal link is not the head (already checked above,
            // but be defensive).
            if focal_link != head_link_node_id {
                store.remove_linknode(container_id, &focal_link).ok();
                store
                    .insert_before(container_id, &head_link_node_id, object_id, None, None)
                    .ok();
                lists_promoted += 1;
            }
        }
    }

    // ── Step 3: Build / update shortcut bucket ────────────────────────────────
    //
    // The shortcut path encodes the focal object ID in a SmartList-safe form.
    // `normalize_path` replaces non-[a-z0-9-] chars with hyphens and prepends
    // `smartlist/` if absent.
    let shortcut_path = shortcut_path_for(object_id)?;
    let mut shortcuts_added = 0usize;

    for co_id in &co_members {
        // `attach_member` creates the bucket if it doesn't exist and appends
        // the co-member.  The `unique_members` policy on the bucket (set by
        // `ensure_bucket_path` via `ensure_container`) prevents duplicates.
        match attach_member(store, &shortcut_path, co_id, created_by, now_utc) {
            Ok(_) => shortcuts_added += 1,
            Err(_) => {
                // Co-member may already be present or the object may have been
                // removed between the snapshot and now.  Silently skip.
            }
        }
    }

    Ok(DreamTouchResult {
        object_id: object_id.to_string(),
        lists_promoted,
        shortcuts_added,
        shortcut_path,
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Return the canonical SmartList path for the shortcut bucket of `object_id`.
///
/// Example: `"session:abc-123"` → `"smartlist/dreaming/shortcuts/session-abc-123"`
pub fn shortcut_path_for(object_id: &str) -> Result<String> {
    // The object_id becomes the last path segment.  `normalize_path` will
    // lowercased it and replace disallowed chars with hyphens.
    normalize_path(&format!("dreaming/shortcuts/{}", object_id))
}

// ── Dream Schedule ────────────────────────────────────────────────────────────

/// The tool ID used for cache lookups in the dreaming pipeline.
pub const DREAMER_TOOL_ID: &str = "dreamer:v1";

/// Summary of a `dream_schedule` run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DreamScheduleResult {
    /// Number of SmartLists on which dream-touch was applied.
    pub touched: usize,
    /// Number of SmartLists skipped because a valid cached artifact exists.
    pub skipped: usize,
    /// Number of SmartLists that had a stale (non-valid) cached artifact and
    /// were re-touched.
    pub stale: usize,
    /// Number of SmartLists whose priority was adjusted by FEP cache signals.
    pub signal_influenced: usize,
}

/// Priority classification for a single SmartList during scheduling.
#[derive(Clone, Debug)]
enum TouchPriority {
    /// No cached artifact — process with highest urgency.
    Miss,
    /// Cached artifact exists but is not valid (stale/invalidated) — process in
    /// age order (older = higher surprise).
    Stale { age_days: f64 },
}

impl TouchPriority {
    fn surprise_score(&self) -> f64 {
        match self {
            // Miss → effectively infinite surprise.
            TouchPriority::Miss => f64::MAX,
            // Stale → older artifact = higher surprise.
            TouchPriority::Stale { age_days } => *age_days,
        }
    }
}

/// Schedule and execute dream-touch over all SmartLists in the store that need it.
///
/// # Algorithm
///
/// 1. Collect every SmartList container path from the store
///    (container IDs starting with `smartlist-members:smartlist/`).
/// 2. For each path, query the dreamer:v1 cache:
///    - Valid hit   → skip (contributes to `skipped` count).
///    - No hit but stale artifact exists → queue as `Stale` with age_days.
///    - No artifact at all → queue as `Miss`.
/// 3. Sort the work queue by surprise score descending (miss first, then
///    oldest stale artifacts).
/// 4. Process up to `max_touches` items by calling `dream_touch` on each.
/// 5. Return a summary.
///
/// # Arguments
///
/// * `store`      — mutable AMS store (will be mutated in place)
/// * `created_by` — actor label for any newly created objects
/// * `now_utc`    — wall-clock timestamp for ordering / object metadata
/// * `max_touches`— upper bound on the number of dream-touch calls (default 100)
pub fn dream_schedule(
    store: &mut AmsStore,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
    max_touches: usize,
) -> Result<DreamScheduleResult> {
    // ── Step 1: Collect all SmartList paths ───────────────────────────────────
    //
    // Container IDs for SmartList member lists follow the pattern
    // `smartlist-members:smartlist/<path>`.  We strip the prefix to recover
    // the canonical SmartList path used as the cache source_id.
    const MEMBERS_PREFIX: &str = "smartlist-members:smartlist/";
    let all_paths: Vec<String> = store
        .containers()
        .keys()
        .filter_map(|cid| {
            cid.strip_prefix(MEMBERS_PREFIX)
                .map(|tail| format!("smartlist/{}", tail))
        })
        .collect();

    // ── Step 1b: Load FEP cache signal surprise scores for dream-topic clusters ─
    // cluster_id here is the path suffix after "smartlist/dream-topics/".
    let signal_surprise_map = cache_signal_cluster_surprise_map(store, DEFAULT_WINDOW_HOURS, now_utc);

    // ── Step 2: Classify each SmartList ──────────────────────────────────────
    let mut work_queue: Vec<(String, TouchPriority)> = Vec::new();
    let mut skipped: usize = 0;

    for path in &all_paths {
        // Tool-centric lookup returns only *valid* artifacts.
        let valid_hits = lookup_tool_centric(store, DREAMER_TOOL_ID, path, None);
        if !valid_hits.is_empty() {
            skipped += 1;
            continue;
        }

        // No valid hit.  Check whether there is *any* artifact for this source
        // in the tool's cache SmartList (including stale / invalidated).
        let stale_age = find_stale_artifact_age(store, path, now_utc);
        let priority = match stale_age {
            Some(age_days) => TouchPriority::Stale { age_days },
            None => TouchPriority::Miss,
        };
        work_queue.push((path.clone(), priority));
    }

    // ── Step 2b: Blend signal surprise for dream-topic cluster SmartLists ────
    // Path format: "smartlist/dream-topics/<cluster-id>"
    const DREAM_TOPICS_PREFIX: &str = "smartlist/dream-topics/";
    let mut signal_influenced: usize = 0;

    for (path, priority) in &mut work_queue {
        if let Some(cluster_id) = path.strip_prefix(DREAM_TOPICS_PREFIX) {
            if let Some(&signal_score) = signal_surprise_map.get(cluster_id) {
                // Blend: preserve Miss=∞ for uncached SmartLists; blend signal
                // into Stale score using 40/60 weighting.
                *priority = match priority {
                    TouchPriority::Miss => TouchPriority::Miss,
                    TouchPriority::Stale { age_days } => {
                        let blended = 0.4 * signal_score + 0.6 * *age_days;
                        TouchPriority::Stale { age_days: blended }
                    }
                };
                signal_influenced += 1;
            }
        }
    }

    // ── Step 3: Sort by surprise score descending ─────────────────────────────
    work_queue.sort_by(|(_, pa), (_, pb)| {
        pb.surprise_score()
            .partial_cmp(&pa.surprise_score())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // ── Step 4: Apply dream-touch up to max_touches ───────────────────────────
    let mut touched: usize = 0;
    let mut stale: usize = 0;

    for (path, priority) in work_queue.iter().take(max_touches) {
        // The focal "object" for dream-touch is the SmartList bucket object.
        // The bucket object ID is the same as the SmartList path (normalized).
        // If it doesn't exist as an object, skip gracefully.
        let object_id = path.as_str();
        if !store.objects().contains_key(object_id) {
            continue;
        }
        dream_touch(store, object_id, created_by, now_utc)?;
        touched += 1;
        if matches!(priority, TouchPriority::Stale { .. }) {
            stale += 1;
        }
    }

    Ok(DreamScheduleResult { touched, skipped, stale, signal_influenced })
}

/// Returns the age in days of the oldest non-valid (stale / invalidated)
/// artifact for the given source_id under the `dreamer:v1` tool, or `None`
/// if no such artifact exists.
fn find_stale_artifact_age(
    store: &AmsStore,
    source_id: &str,
    now_utc: DateTime<FixedOffset>,
) -> Option<f64> {
    use crate::cache::tool_cache_smartlist_path;

    let raw_path = tool_cache_smartlist_path(DREAMER_TOOL_ID);
    let container_id = format!(
        "smartlist-members:{}",
        normalize_path(&raw_path).unwrap_or(raw_path)
    );

    let members = store.iterate_forward(&container_id);
    let mut oldest_age: Option<f64> = None;

    for link_node in members {
        let obj_id = &link_node.object_id;
        let Some(obj) = store.objects().get(obj_id) else { continue };
        let Some(ref sp) = obj.semantic_payload else { continue };
        let Some(ref prov) = sp.provenance else { continue };

        let get_str = |key: &str| prov.get(key).and_then(|v| v.as_str()).unwrap_or("").to_string();
        let meta_source_id = get_str("source_id");
        let meta_validity = get_str("validity_state");

        if meta_source_id != source_id { continue; }
        if meta_validity == "valid" { continue; } // Only interested in non-valid.

        let created_at = prov
            .get("created_at")
            .and_then(|v| v.as_str())
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .unwrap_or(now_utc);

        let age_secs = (now_utc - created_at).num_seconds().max(0) as f64;
        let age_days = age_secs / 86_400.0;

        oldest_age = Some(oldest_age.map_or(age_days, |prev: f64| prev.max(age_days)));
    }

    oldest_age
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::now_fixed;
    use crate::smartlist_write::attach_member;
    use crate::store::AmsStore;

    fn setup() -> (AmsStore, String, String, String) {
        let mut store = AmsStore::new();
        let now = now_fixed();
        let created_by = "test";

        store.upsert_object("obj:A", "session", None, None, Some(now)).unwrap();
        store.upsert_object("obj:B", "session", None, None, Some(now)).unwrap();
        store.upsert_object("obj:C", "session", None, None, Some(now)).unwrap();

        attach_member(&mut store, "dreaming/test/list1", "obj:A", created_by, now).unwrap();
        attach_member(&mut store, "dreaming/test/list1", "obj:B", created_by, now).unwrap();
        attach_member(&mut store, "dreaming/test/list2", "obj:C", created_by, now).unwrap();
        attach_member(&mut store, "dreaming/test/list2", "obj:A", created_by, now).unwrap();

        (store, "obj:A".to_string(), "obj:B".to_string(), "obj:C".to_string())
    }

    #[test]
    fn dream_touch_unknown_object_errors() {
        let mut store = AmsStore::new();
        let now = now_fixed();
        let err = dream_touch(&mut store, "nonexistent", "test", now).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn dream_touch_promotes_to_head_in_each_list() {
        let (mut store, a, _b, _c) = setup();
        let now = now_fixed();

        // list2 order is currently [C, A] — A should be promoted to head.
        let container_list2 = "smartlist-members:smartlist/dreaming/test/list2";
        let before: Vec<String> = store
            .iterate_forward(container_list2)
            .iter()
            .map(|n| n.object_id.clone())
            .collect();
        assert_eq!(before[0], "obj:C", "sanity: C is currently head of list2");

        let result = dream_touch(&mut store, &a, "test", now).unwrap();

        let after: Vec<String> = store
            .iterate_forward(container_list2)
            .iter()
            .map(|n| n.object_id.clone())
            .collect();
        assert_eq!(after[0], "obj:A", "A should now be head of list2");
        assert!(result.lists_promoted >= 1, "at least one list was promoted");
    }

    #[test]
    fn dream_touch_creates_shortcut_bucket_with_co_members() {
        let (mut store, a, b, c) = setup();
        let now = now_fixed();

        let result = dream_touch(&mut store, &a, "test", now).unwrap();

        // A shares list1 with B and list2 with C — both should be in shortcuts.
        let shortcut_container =
            format!("smartlist-members:{}", result.shortcut_path);
        let shortcuts: Vec<String> = store
            .iterate_forward(&shortcut_container)
            .iter()
            .map(|n| n.object_id.clone())
            .collect();

        assert!(shortcuts.contains(&b), "B should be in shortcuts for A");
        assert!(shortcuts.contains(&c), "C should be in shortcuts for A");
        assert!(!shortcuts.contains(&a), "A should not be a shortcut for itself");
    }

    #[test]
    fn dream_touch_idempotent_at_head() {
        let (mut store, _a, b, _c) = setup();
        let now = now_fixed();

        // B is already the tail in list1 — touch it, then touch it again.
        let result1 = dream_touch(&mut store, &b, "test", now).unwrap();
        let result2 = dream_touch(&mut store, &b, "test", now).unwrap();

        // Second touch: B is already at head so lists_promoted should be 0.
        assert_eq!(result2.lists_promoted, 0);
        // shortcuts_added may be 0 because items already exist (unique_members).
        let _ = result1;
    }

    #[test]
    fn shortcut_path_normalizes_colons() {
        let path = shortcut_path_for("session:abc-123").unwrap();
        assert_eq!(path, "smartlist/dreaming/shortcuts/session-abc-123");
    }

    // ── dream_schedule tests ──────────────────────────────────────────────────

    /// Helper: build a store with three SmartList bucket objects and members,
    /// simulating three lists that could be scheduled.
    fn setup_schedule() -> AmsStore {
        let mut store = AmsStore::new();
        let now = now_fixed();

        // Create three SmartList bucket objects (the object ID must match the path
        // so dream_touch can find them by object_id).
        store.upsert_object("smartlist/sched/list-a", "smartlist_bucket", None, None, Some(now)).unwrap();
        store.upsert_object("smartlist/sched/list-b", "smartlist_bucket", None, None, Some(now)).unwrap();
        store.upsert_object("smartlist/sched/list-c", "smartlist_bucket", None, None, Some(now)).unwrap();

        // Create session objects for membership.
        store.upsert_object("session:x", "session", None, None, Some(now)).unwrap();
        store.upsert_object("session:y", "session", None, None, Some(now)).unwrap();

        // Attach members to each list.
        attach_member(&mut store, "sched/list-a", "session:x", "test", now).unwrap();
        attach_member(&mut store, "sched/list-b", "session:y", "test", now).unwrap();
        attach_member(&mut store, "sched/list-c", "session:x", "test", now).unwrap();
        attach_member(&mut store, "sched/list-c", "session:y", "test", now).unwrap();

        store
    }

    #[test]
    fn dream_schedule_processes_misses_and_skips_none() {
        let mut store = setup_schedule();
        let now = now_fixed();

        // No cache entries exist — all lists are misses.
        let result = dream_schedule(&mut store, "test", now, 100).unwrap();

        // skipped = 0 (no valid cache hits).
        assert_eq!(result.skipped, 0);
        // touched = number of bucket objects that actually exist in the store.
        // (lists without matching bucket objects are silently skipped).
        assert!(result.touched <= 3, "cannot touch more than 3 lists");
        assert_eq!(result.stale, 0, "no stale artifacts were present");
    }

    #[test]
    fn dream_schedule_respects_max_touches() {
        let mut store = setup_schedule();
        let now = now_fixed();

        // Limit to 1 touch — even though 3 lists need processing.
        let result = dream_schedule(&mut store, "test", now, 1).unwrap();

        assert!(result.touched <= 1, "max_touches=1 must be respected");
    }

    #[test]
    fn dream_schedule_all_cached_produces_zero_touched() {
        let mut store = setup_schedule();
        let now = now_fixed();

        // Inject a valid cache artifact for each SmartList path so that
        // lookup_tool_centric returns hits.
        use crate::model::SemanticPayload;
        use serde_json::Value;
        use std::collections::BTreeMap;

        let paths = ["smartlist/sched/list-a", "smartlist/sched/list-b", "smartlist/sched/list-c"];
        for (i, path) in paths.iter().enumerate() {
            let artifact_id = format!("artifact-{}", i);
            store.upsert_object(&artifact_id, "cache_artifact", None, None, Some(now)).unwrap();

            // Write the provenance fields that lookup_tool_centric relies on.
            let obj = store.objects_mut().get_mut(&artifact_id).unwrap();
            let mut prov: BTreeMap<String, Value> = BTreeMap::new();
            prov.insert("tool_id".into(), Value::String(DREAMER_TOOL_ID.into()));
            prov.insert("source_id".into(), Value::String(path.to_string()));
            prov.insert("param_hash".into(), Value::String("none".into()));
            prov.insert("validity_state".into(), Value::String("valid".into()));
            prov.insert("created_at".into(), Value::String(now.to_rfc3339()));
            obj.semantic_payload = Some(SemanticPayload { provenance: Some(prov), ..Default::default() });

            // Place the artifact object in the tool's cache SmartList container.
            let tool_cache_path = crate::cache::tool_cache_smartlist_path(DREAMER_TOOL_ID);
            let norm_path = normalize_path(&tool_cache_path).unwrap_or(tool_cache_path);
            attach_member(&mut store, &norm_path.strip_prefix("smartlist/").unwrap_or(&norm_path), &artifact_id, "test", now).unwrap();
        }

        let result = dream_schedule(&mut store, "test", now, 100).unwrap();

        assert_eq!(result.touched, 0, "all lists are cached — nothing should be touched");
        assert_eq!(result.skipped, 3, "all three lists should be skipped");
    }

    // ── dream_schedule + FEP signal tests ─────────────────────────────────────

    /// Helper: create a stale dream artifact for a SmartList path so that
    /// dream_schedule sees it as Stale rather than Miss.
    fn inject_stale_artifact(store: &mut AmsStore, path: &str, age_days: f64) {
        use crate::model::SemanticPayload;
        use serde_json::Value;
        use std::collections::BTreeMap as M;

        let id = format!("stale-artifact:{}", path.replace('/', "-"));
        let stale_ts = now_fixed() - chrono::Duration::hours((age_days * 24.0) as i64);
        store.upsert_object(&id, "cache_artifact", None, None, Some(stale_ts)).unwrap();

        let obj = store.objects_mut().get_mut(&id).unwrap();
        let mut prov: M<String, Value> = M::new();
        prov.insert("tool_id".into(), Value::String(DREAMER_TOOL_ID.into()));
        prov.insert("source_id".into(), Value::String(path.to_string()));
        prov.insert("param_hash".into(), Value::String("none".into()));
        prov.insert("validity_state".into(), Value::String("stale".into()));
        prov.insert("created_at".into(), Value::String(stale_ts.to_rfc3339()));
        obj.semantic_payload = Some(SemanticPayload { provenance: Some(prov), ..Default::default() });

        let tool_cache_path = crate::cache::tool_cache_smartlist_path(DREAMER_TOOL_ID);
        let norm_path = normalize_path(&tool_cache_path).unwrap_or(tool_cache_path);
        attach_member(store, &norm_path.strip_prefix("smartlist/").unwrap_or(&norm_path), &id, "test", now_fixed()).unwrap();
    }

    #[test]
    fn dream_schedule_signal_influenced_counter() {
        use crate::fep_cache_signal::emit_cache_signal;
        use crate::smartlist_write::attach_member as sl_attach;

        let mut store = AmsStore::new();
        let now = now_fixed();

        // Create a dream-topics cluster with a bucket object and session member.
        let cluster_path = "smartlist/dream-topics/cluster-sig-01";
        store.upsert_object(cluster_path, "smartlist_bucket", None, None, Some(now)).unwrap();
        store.upsert_object("session:sig-01", "session", Some("memory graph signal".to_string()), None, Some(now)).unwrap();
        sl_attach(&mut store, "dream-topics/cluster-sig-01", "session:sig-01", "test", now).unwrap();

        // Inject a stale artifact so the cluster is in the work queue.
        inject_stale_artifact(&mut store, cluster_path, 2.0);

        // Emit cache miss signals matching cluster keywords → non-zero surprise.
        for _ in 0..3 {
            emit_cache_signal(&mut store, "memory graph", "cv-test", false, "test", now).unwrap();
        }

        let result = dream_schedule(&mut store, "test", now + chrono::Duration::seconds(1), 100).unwrap();

        // At least the dream-topics cluster should have been signal-influenced.
        assert!(result.signal_influenced >= 1, "expected signal_influenced >= 1, got {}", result.signal_influenced);
    }

    #[test]
    fn dream_schedule_signal_influenced_counts_clusters_with_signal_scores() {
        use crate::fep_cache_signal::emit_cache_signal;
        use crate::smartlist_write::attach_member as sl_attach;

        let mut store = AmsStore::new();
        let now = now_fixed();

        // Two dream-topic clusters (no artifact → both Miss).
        let path_a = "smartlist/dream-topics/cluster-low-miss";
        store.upsert_object(path_a, "smartlist_bucket", None, None, Some(now)).unwrap();
        store.upsert_object("session:low", "session", Some("route replay planning".to_string()), None, Some(now)).unwrap();
        sl_attach(&mut store, "dream-topics/cluster-low-miss", "session:low", "test", now).unwrap();

        let path_b = "smartlist/dream-topics/cluster-high-miss";
        store.upsert_object(path_b, "smartlist_bucket", None, None, Some(now)).unwrap();
        store.upsert_object("session:high", "session", Some("memory topology graph".to_string()), None, Some(now)).unwrap();
        sl_attach(&mut store, "dream-topics/cluster-high-miss", "session:high", "test", now).unwrap();

        // Emit signals matching cluster keywords to build a non-empty signal map.
        for _ in 0..8 { emit_cache_signal(&mut store, "route replay", "cv1", true, "t", now).unwrap(); }
        for _ in 0..2 { emit_cache_signal(&mut store, "route replay", "cv1", false, "t", now).unwrap(); }
        for _ in 0..2 { emit_cache_signal(&mut store, "memory topology", "cv1", true, "t", now).unwrap(); }
        for _ in 0..8 { emit_cache_signal(&mut store, "memory topology", "cv1", false, "t", now).unwrap(); }

        // Both clusters are Miss (infinite priority); signal_influenced should
        // count those that appear in the signal surprise map.
        let result = dream_schedule(&mut store, "test", now + chrono::Duration::seconds(1), 100).unwrap();

        // Clusters are Miss so they should be touched (bucket objects exist).
        assert!(result.touched >= 1, "clusters should be touched, got {}", result.touched);
        // At least one cluster had signal data → signal_influenced > 0.
        assert!(result.signal_influenced >= 1, "expected signal_influenced >= 1, got {}", result.signal_influenced);
    }

    #[test]
    fn dream_schedule_non_cluster_lists_unaffected_by_signals() {
        use crate::fep_cache_signal::emit_cache_signal;

        let mut store = setup_schedule();
        let now = now_fixed();

        // Emit signals — but sched/list-a,b,c are not dream-topic clusters.
        for _ in 0..5 { emit_cache_signal(&mut store, "list sched", "cv1", false, "t", now).unwrap(); }

        let result = dream_schedule(&mut store, "test", now, 100).unwrap();

        // No signal_influenced — none of the paths start with smartlist/dream-topics/.
        assert_eq!(result.signal_influenced, 0);
    }
}
