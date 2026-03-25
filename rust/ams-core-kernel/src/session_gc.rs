//! Session garbage-collection helpers: tombstone creation and prune safety check.
//!
//! ## What is a session tombstone?
//!
//! When a session is pruned (ghosted) from the store, its cluster membership
//! information and embedding signature would be lost, causing dream-cluster
//! topology to drift.  A **session tombstone** is a lightweight Object of kind
//! `session_tombstone` that preserves:
//!
//! - The original session ID
//! - The set of SmartList containers the session belonged to (cluster membership)
//! - The session's embedding vector (if present)
//! - A timestamp recording when the ghost was created
//!
//! The `dream_cluster` pipeline counts tombstones as cluster members (same
//! object-kind scan extended to `session_tombstone`) so clusters remain stable
//! even after live sessions are removed.
//!
//! ## Session prune safety check (P6-A2)
//!
//! Before ghosting a session, call [`session_prune_check`] to verify that no
//! dream-topic cluster would drop below 2 live members.  A *live* member is an
//! Object with `object_kind='session_ref'` or `object_kind='session'`.
//! Tombstones do **not** count as live members for this purpose.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use ams_core_kernel::session_gc::{
//!     create_session_tombstone, session_prune_check, SessionTombstoneResult,
//! };
//!
//! let check = session_prune_check(&store, "session:abc123")?;
//! if check.safe {
//!     let result = create_session_tombstone(&mut store, "session:abc123", "gc-agent", now)?;
//!     println!("tombstone id: {}", result.tombstone_object_id);
//! } else {
//!     println!("unsafe: {}", check.reason.unwrap());
//! }
//! ```

use std::collections::BTreeSet;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, FixedOffset};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::model::SemanticPayload;
use crate::store::AmsStore;

// ── Constants ─────────────────────────────────────────────────────────────────

/// Object kind for session tombstone objects.
pub const SESSION_TOMBSTONE_KIND: &str = "session_tombstone";

/// Provenance key: original session object ID stored in the tombstone's payload.
pub const PROV_ORIGINAL_SESSION_ID: &str = "original_session_id";

/// Provenance key: ISO-8601 timestamp when the tombstone was created.
pub const PROV_GHOSTED_AT: &str = "ghosted_at";

/// Provenance key: actor that created the tombstone.
pub const PROV_CREATED_BY: &str = "created_by";

/// Provenance key: list of container IDs the session belonged to.
pub const PROV_CLUSTER_MEMBERSHIPS: &str = "cluster_memberships";

// ── Result type ───────────────────────────────────────────────────────────────

/// Summary of a tombstone creation operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionTombstoneResult {
    /// Object ID of the original session that was ghosted.
    pub original_session_id: String,
    /// Object ID of the new tombstone object (e.g. `"tombstone:abc123"`).
    pub tombstone_object_id: String,
    /// Container membership set snapshotted from the original session.
    pub cluster_memberships: BTreeSet<String>,
    /// Whether the original session had an embedding that was carried over.
    pub embedding_preserved: bool,
}

// ── Core function ─────────────────────────────────────────────────────────────

/// Create a session tombstone for the given session object.
///
/// The tombstone preserves the session's cluster membership set and embedding
/// so that `dream_cluster` topology remains stable after the live session is
/// removed.
///
/// # Arguments
///
/// * `store`      — mutable AMS store
/// * `session_id` — object ID of the session to ghost (must exist in the store)
/// * `created_by` — actor label recorded in the tombstone's provenance
/// * `now`        — wall-clock timestamp for the new tombstone object
///
/// # Returns
///
/// A [`SessionTombstoneResult`] describing what was written.
///
/// # Errors
///
/// Returns an error if `session_id` does not exist in the store or is not of
/// kind `session` / `session_ref`.
pub fn create_session_tombstone(
    store: &mut AmsStore,
    session_id: &str,
    created_by: &str,
    now: DateTime<FixedOffset>,
) -> Result<SessionTombstoneResult> {
    // ── Validate the session object ───────────────────────────────────────────
    let (embedding, tags, summary) = {
        let obj = store
            .objects()
            .get(session_id)
            .ok_or_else(|| anyhow!("session not found: {session_id}"))?;

        if obj.object_kind != "session" && obj.object_kind != "session_ref" {
            return Err(anyhow!(
                "object '{session_id}' has kind '{}', expected 'session' or 'session_ref'",
                obj.object_kind
            ));
        }

        let (emb, tgs, summ) = obj
            .semantic_payload
            .as_ref()
            .map(|p| (p.embedding.clone(), p.tags.clone(), p.summary.clone()))
            .unwrap_or((None, None, None));
        (emb, tgs, summ)
    };

    // ── Snapshot cluster memberships ──────────────────────────────────────────
    let cluster_memberships: BTreeSet<String> = store
        .containers_for_member_object(session_id)
        .into_iter()
        .filter(|c| c.starts_with("smartlist-members:"))
        .collect();

    // ── Build tombstone object ID ─────────────────────────────────────────────
    // Strip any "session:" prefix so tombstone IDs are concise.
    let bare_id = session_id
        .strip_prefix("session:")
        .unwrap_or(session_id);
    let tombstone_object_id = format!("tombstone:{bare_id}");

    // ── Build provenance map ──────────────────────────────────────────────────
    let memberships_json: Vec<serde_json::Value> = cluster_memberships
        .iter()
        .map(|s| json!(s))
        .collect();

    let mut provenance = crate::model::JsonMap::new();
    provenance.insert(
        PROV_ORIGINAL_SESSION_ID.to_string(),
        json!(session_id),
    );
    provenance.insert(
        PROV_GHOSTED_AT.to_string(),
        json!(now.to_rfc3339()),
    );
    provenance.insert(PROV_CREATED_BY.to_string(), json!(created_by));
    provenance.insert(
        PROV_CLUSTER_MEMBERSHIPS.to_string(),
        json!(memberships_json),
    );

    let embedding_preserved = embedding.is_some();

    let semantic_payload = SemanticPayload {
        embedding,
        tags,
        summary,
        provenance: Some(provenance),
    };

    // ── Write tombstone to store ──────────────────────────────────────────────
    store.upsert_object(
        tombstone_object_id.clone(),
        SESSION_TOMBSTONE_KIND,
        None,
        Some(semantic_payload),
        Some(now),
    )?;

    Ok(SessionTombstoneResult {
        original_session_id: session_id.to_string(),
        tombstone_object_id,
        cluster_memberships,
        embedding_preserved,
    })
}

// ── Session prune safety check (P6-A2) ───────────────────────────────────────

/// Prefix used by the smartlist_write layer for membership containers.
const SMARTLIST_MEMBERS_PREFIX: &str = "smartlist-members:";

/// Path prefix for dream-topic cluster SmartLists.
const DREAM_TOPICS_PATH_PREFIX: &str = "smartlist/dream-topics/";

/// Returns `true` when the given `object_kind` counts as a *live* session
/// for the purposes of the cluster stability check.
fn is_live_session_kind(kind: &str) -> bool {
    kind == "session_ref" || kind == "session"
}

/// Per-cluster detail produced by [`session_prune_check`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterPruneInfo {
    /// Cluster identifier derived from the SmartList path, e.g. `"cluster-0001"`.
    pub cluster_id: String,
    /// Normalised SmartList path, e.g. `"smartlist/dream-topics/cluster-0001"`.
    pub smartlist_path: String,
    /// Count of *other* live session members that would remain after removal.
    pub remaining_live: usize,
    /// True when removing the candidate would violate the 2-member minimum.
    pub would_isolate: bool,
}

/// Output of [`session_prune_check`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionPruneCheckResult {
    /// The session object ID that was evaluated.
    pub session_id: String,
    /// `true` when it is safe to remove this session without isolating any cluster.
    pub safe: bool,
    /// Human-readable explanation when `safe=false`.
    pub reason: Option<String>,
    /// Number of dream-topic clusters the session participates in.
    pub cluster_count: usize,
    /// Per-cluster details (one entry per dream-topic cluster).
    pub clusters: Vec<ClusterPruneInfo>,
}

/// Check whether pruning (ghosting) a session is safe for dream graph topology.
///
/// A session is **safe to prune** when every dream-topic cluster it belongs to
/// would still have at least 2 *other* live members (`object_kind='session_ref'`
/// or `object_kind='session'`) after removal.  Session tombstones do not count
/// as live members.
///
/// # Arguments
///
/// * `store`      — read-only reference to the AMS store (not mutated)
/// * `session_id` — object ID of the session candidate
///
/// # Output key=value fields (via [`format_prune_check_result`])
///
/// ```text
/// session_id=<id>
/// safe=yes|no
/// reason=<...>           # only present when safe=no
/// cluster_count=<N>
/// cluster=<id> remaining_live=<N> would_isolate=yes|no
/// ```
pub fn session_prune_check(
    store: &AmsStore,
    session_id: &str,
) -> Result<SessionPruneCheckResult> {
    // ── Step 1: locate the session object and confirm it is a live session ─────
    let session_obj = store
        .objects()
        .get(session_id)
        .ok_or_else(|| anyhow!("session '{}' not found in snapshot", session_id))?;

    if !is_live_session_kind(&session_obj.object_kind) {
        return Err(anyhow!(
            "object '{}' has kind '{}', expected 'session_ref' or 'session'",
            session_id,
            session_obj.object_kind
        ));
    }

    // ── Step 2: find all dream-topic cluster containers the session belongs to ─
    //
    // The smartlist_write layer uses container IDs of the form
    // `smartlist-members:<normalized-path>`.  We want those whose path part
    // starts with `smartlist/dream-topics/`.
    let cluster_container_ids: Vec<String> = store
        .containers_for_member_object(session_id)
        .into_iter()
        .filter(|cid| {
            cid.strip_prefix(SMARTLIST_MEMBERS_PREFIX)
                .map(|path| path.starts_with(DREAM_TOPICS_PATH_PREFIX))
                .unwrap_or(false)
        })
        .collect();

    let cluster_count = cluster_container_ids.len();
    let members_index = store.container_members_index();
    let objects = store.objects();

    // ── Step 3: for each cluster count remaining live members ─────────────────
    let mut cluster_infos: Vec<ClusterPruneInfo> = Vec::new();
    let mut unsafe_reason: Option<String> = None;

    for container_id in &cluster_container_ids {
        let smartlist_path = container_id
            .strip_prefix(SMARTLIST_MEMBERS_PREFIX)
            .unwrap_or(container_id.as_str())
            .to_string();
        let cluster_id = smartlist_path
            .strip_prefix(DREAM_TOPICS_PATH_PREFIX)
            .unwrap_or(&smartlist_path)
            .to_string();

        // Count live session objects in the cluster excluding the candidate.
        let remaining_live: usize = members_index
            .get(container_id)
            .map(|member_set| {
                member_set
                    .iter()
                    .filter(|&mid| {
                        mid != session_id
                            && objects
                                .get(mid)
                                .map(|o| is_live_session_kind(&o.object_kind))
                                .unwrap_or(false)
                    })
                    .count()
            })
            .unwrap_or(0);

        let would_isolate = remaining_live < 2;

        if would_isolate && unsafe_reason.is_none() {
            unsafe_reason = Some(format!(
                "cluster {} would drop to {} live member{}",
                cluster_id,
                remaining_live,
                if remaining_live == 1 { "" } else { "s" }
            ));
        }

        cluster_infos.push(ClusterPruneInfo {
            cluster_id,
            smartlist_path,
            remaining_live,
            would_isolate,
        });
    }

    Ok(SessionPruneCheckResult {
        session_id: session_id.to_string(),
        safe: unsafe_reason.is_none(),
        reason: unsafe_reason,
        cluster_count,
        clusters: cluster_infos,
    })
}

/// Render a [`SessionPruneCheckResult`] as machine-readable key=value lines.
pub fn format_prune_check_result(result: &SessionPruneCheckResult) -> String {
    let mut lines = Vec::new();
    lines.push(format!("session_id={}", result.session_id));
    lines.push(format!("safe={}", if result.safe { "yes" } else { "no" }));
    if let Some(ref reason) = result.reason {
        lines.push(format!("reason={}", reason));
    }
    lines.push(format!("cluster_count={}", result.cluster_count));
    for info in &result.clusters {
        lines.push(format!(
            "cluster={} remaining_live={} would_isolate={}",
            info.cluster_id,
            info.remaining_live,
            if info.would_isolate { "yes" } else { "no" }
        ));
    }
    lines.join("\n") + "\n"
}

// ── Session prune safe (P6-B1) ────────────────────────────────────────────────

/// Result of a [`session_prune_safe`] call.
#[derive(Clone, Debug, PartialEq)]
pub struct SessionPruneSafeResult {
    /// The session object ID that was evaluated.
    pub session_id: String,
    /// `true` when the session was pruned (tombstone created and original ghosted).
    /// `false` when pruning was skipped because it would isolate a cluster.
    pub pruned: bool,
    /// Tombstone object ID (only set when `pruned=true`).
    pub tombstone_object_id: Option<String>,
    /// Number of clusters preserved by the tombstone (only meaningful when `pruned=true`).
    pub clusters_preserved: usize,
    /// Reason for skipping (only set when `pruned=false`).
    pub skip_reason: Option<String>,
}

/// Composite safe-prune operation: check safety, create tombstone, ghost original.
///
/// If the session would isolate any dream-topic cluster, the operation is
/// skipped (returns `pruned=false`) without modifying the store.
///
/// If safe, the operation:
/// 1. Creates a session tombstone (preserving cluster membership + embedding).
/// 2. Marks the original session as ghosted via `ghost_artifact`.
///
/// # Returns
///
/// A [`SessionPruneSafeResult`] describing the outcome.
pub fn session_prune_safe(
    store: &mut AmsStore,
    session_id: &str,
    created_by: &str,
    now: DateTime<FixedOffset>,
) -> Result<SessionPruneSafeResult> {
    // Safety check (read-only).
    let check = session_prune_check(store, session_id)?;

    if !check.safe {
        return Ok(SessionPruneSafeResult {
            session_id: session_id.to_string(),
            pruned: false,
            tombstone_object_id: None,
            clusters_preserved: 0,
            skip_reason: check.reason,
        });
    }

    // Create tombstone.
    let tombstone = create_session_tombstone(store, session_id, created_by, now)?;
    let clusters_preserved = tombstone.cluster_memberships.len();
    let tombstone_object_id = tombstone.tombstone_object_id.clone();

    // Ghost the original session object.
    crate::cache::ghost_artifact(store, session_id, "pruned:session-prune-safe")?;

    Ok(SessionPruneSafeResult {
        session_id: session_id.to_string(),
        pruned: true,
        tombstone_object_id: Some(tombstone_object_id),
        clusters_preserved,
        skip_reason: None,
    })
}

/// Result of a [`session_prune_batch`] call.
#[derive(Clone, Debug, PartialEq)]
pub struct SessionPruneBatchResult {
    pub pruned: usize,
    pub skipped: usize,
    pub total: usize,
}

/// Batch safe-prune: call [`session_prune_safe`] for each session ID in the list.
///
/// Continues processing remaining IDs even if one returns an error (errors are
/// counted as skipped).  The updated store is mutated in-place.
pub fn session_prune_batch(
    store: &mut AmsStore,
    session_ids: &[String],
    created_by: &str,
    now: DateTime<FixedOffset>,
) -> SessionPruneBatchResult {
    let mut pruned = 0usize;
    let mut skipped = 0usize;

    for id in session_ids {
        match session_prune_safe(store, id, created_by, now) {
            Ok(r) if r.pruned => pruned += 1,
            _ => skipped += 1,
        }
    }

    SessionPruneBatchResult {
        pruned,
        skipped,
        total: session_ids.len(),
    }
}

// ── Tombstone expiry (P6-C2) ──────────────────────────────────────────────────

/// Result of a [`session_tombstone_expire`] call.
#[derive(Clone, Debug, PartialEq)]
pub struct SessionTombstoneExpireResult {
    /// Number of tombstone objects ghosted in this pass.
    pub expired: usize,
    /// Number of tombstone objects that were within the age threshold (kept).
    pub kept: usize,
}

/// Ghost tombstones older than `max_age_days` days.
///
/// Scans all objects with `object_kind='session_tombstone'` and ghosts any
/// whose `ghosted_at` provenance timestamp is older than the given threshold.
/// Already-ghosted tombstones (validity_state="ghosted") are ignored.
///
/// # Arguments
///
/// * `store`        — mutable AMS store
/// * `max_age_days` — tombstones older than this many days are expired
/// * `now`          — current wall-clock time used to compute the cutoff
///
/// # Output key=value fields (via CLI)
///
/// ```text
/// expired=N
/// kept=M
/// ```
pub fn session_tombstone_expire(
    store: &mut AmsStore,
    max_age_days: u32,
    now: DateTime<FixedOffset>,
) -> Result<SessionTombstoneExpireResult> {
    let cutoff = now - Duration::days(max_age_days as i64);

    // ── Collect IDs of non-ghosted tombstones ────────────────────────────────
    // Categorise each non-ghosted tombstone as "to expire" or "to keep".
    let mut to_expire: Vec<String> = Vec::new();
    let mut kept: usize = 0;

    {
        let objects = store.objects();
        for (id, obj) in objects.iter() {
            if obj.object_kind != SESSION_TOMBSTONE_KIND {
                continue;
            }
            // Skip already-ghosted tombstones — they were expired in a prior pass.
            let already_ghosted = obj
                .semantic_payload
                .as_ref()
                .and_then(|p| p.provenance.as_ref())
                .and_then(|pv| pv.get("validity_state"))
                .and_then(|v| v.as_str())
                == Some("ghosted");
            if already_ghosted {
                continue;
            }

            // Parse the ghosted_at timestamp from provenance.
            let ghosted_at_opt: Option<DateTime<FixedOffset>> = obj
                .semantic_payload
                .as_ref()
                .and_then(|p| p.provenance.as_ref())
                .and_then(|pv| pv.get(PROV_GHOSTED_AT))
                .and_then(|v| v.as_str())
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok());

            match ghosted_at_opt {
                Some(ts) if ts < cutoff => to_expire.push(id.clone()),
                _ => kept += 1,
            }
        }
    }

    let expired = to_expire.len();
    for id in &to_expire {
        crate::cache::ghost_artifact(store, id, "tombstone-expired")?;
    }

    Ok(SessionTombstoneExpireResult { expired, kept })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::model::SemanticPayload;
    use crate::store::AmsStore;

    fn now() -> DateTime<FixedOffset> {
        Utc::now().fixed_offset()
    }

    fn make_store_with_session(session_id: &str, with_embedding: bool) -> AmsStore {
        let mut store = AmsStore::new();
        let payload = if with_embedding {
            Some(SemanticPayload {
                embedding: Some(vec![0.1, 0.2, 0.3]),
                tags: Some(vec!["alpha".into(), "beta".into()]),
                summary: Some("test session".into()),
                provenance: None,
            })
        } else {
            None
        };
        store
            .upsert_object(session_id, "session", None, payload, Some(now()))
            .unwrap();
        store
    }

    #[test]
    fn creates_tombstone_with_embedding() {
        let mut store = make_store_with_session("session:abc", true);
        let result = create_session_tombstone(&mut store, "session:abc", "test-agent", now())
            .expect("should succeed");

        assert_eq!(result.original_session_id, "session:abc");
        assert_eq!(result.tombstone_object_id, "tombstone:abc");
        assert!(result.embedding_preserved);

        let tombstone = store
            .objects()
            .get("tombstone:abc")
            .expect("tombstone must exist");
        assert_eq!(tombstone.object_kind, SESSION_TOMBSTONE_KIND);

        let payload = tombstone.semantic_payload.as_ref().unwrap();
        assert_eq!(payload.embedding.as_ref().unwrap(), &[0.1f32, 0.2, 0.3]);

        let prov = payload.provenance.as_ref().unwrap();
        assert_eq!(prov[PROV_ORIGINAL_SESSION_ID], json!("session:abc"));
        assert_eq!(prov[PROV_CREATED_BY], json!("test-agent"));
    }

    #[test]
    fn creates_tombstone_without_embedding() {
        let mut store = make_store_with_session("session:xyz", false);
        let result = create_session_tombstone(&mut store, "session:xyz", "gc", now())
            .expect("should succeed");

        assert!(!result.embedding_preserved);
        assert_eq!(result.tombstone_object_id, "tombstone:xyz");
    }

    #[test]
    fn captures_cluster_memberships() {
        let session_id = "session:s1";
        let mut store = make_store_with_session(session_id, false);

        // Add session to two containers.
        store
            .create_container("smartlist-members:cluster-0001", "smartlist", "smartlist")
            .unwrap();
        store
            .create_container("smartlist-members:cluster-0002", "smartlist", "smartlist")
            .unwrap();
        store
            .add_object("smartlist-members:cluster-0001", session_id, None, None)
            .unwrap();
        store
            .add_object("smartlist-members:cluster-0002", session_id, None, None)
            .unwrap();

        let result = create_session_tombstone(&mut store, session_id, "gc", now()).unwrap();

        assert_eq!(result.cluster_memberships.len(), 2);
        assert!(result
            .cluster_memberships
            .contains("smartlist-members:cluster-0001"));
        assert!(result
            .cluster_memberships
            .contains("smartlist-members:cluster-0002"));

        // Memberships also baked into tombstone provenance.
        let tombstone = store.objects().get("tombstone:s1").unwrap();
        let prov = tombstone
            .semantic_payload
            .as_ref()
            .unwrap()
            .provenance
            .as_ref()
            .unwrap();
        let memberships = prov[PROV_CLUSTER_MEMBERSHIPS].as_array().unwrap();
        assert_eq!(memberships.len(), 2);
    }

    #[test]
    fn rejects_missing_session() {
        let mut store = AmsStore::new();
        let err =
            create_session_tombstone(&mut store, "session:missing", "gc", now()).unwrap_err();
        assert!(err.to_string().contains("session not found"));
    }

    #[test]
    fn rejects_wrong_object_kind() {
        let mut store = AmsStore::new();
        store
            .upsert_object("obj:foo", "dream_topic", None, None, None)
            .unwrap();
        let err =
            create_session_tombstone(&mut store, "obj:foo", "gc", now()).unwrap_err();
        assert!(err.to_string().contains("expected 'session'"));
    }

    // ── session_prune_check tests ─────────────────────────────────────────────

    /// Helper: create a store with N session_ref objects all in the same
    /// dream-topics cluster SmartList.
    fn make_cluster_store(session_ids: &[&str], cluster_name: &str) -> AmsStore {
        let path = format!("smartlist/dream-topics/{}", cluster_name);
        let container_id = format!("smartlist-members:{}", path);

        let mut store = AmsStore::new();
        store
            .create_container(container_id.clone(), "smartlist_members", "smartlist_members")
            .unwrap();

        for &sid in session_ids {
            store
                .upsert_object(sid, "session_ref", None, None, None)
                .unwrap();
            store
                .add_object(&container_id, sid, None, None)
                .unwrap();
        }
        store
    }

    // Test 1: cluster with 3 members → safe=yes
    #[test]
    fn prune_check_safe_yes_three_members() {
        let store = make_cluster_store(
            &["s-a", "s-b", "s-c"],
            "cluster-0001",
        );
        let result = session_prune_check(&store, "s-a").unwrap();
        assert!(result.safe, "expected safe=yes");
        assert_eq!(result.cluster_count, 1);
        assert_eq!(result.clusters[0].remaining_live, 2);
        assert!(!result.clusters[0].would_isolate);
        assert!(result.reason.is_none());
    }

    // Test 2: cluster with 2 members → safe=no
    #[test]
    fn prune_check_safe_no_two_members() {
        let store = make_cluster_store(&["s-a", "s-b"], "cluster-0001");
        let result = session_prune_check(&store, "s-a").unwrap();
        assert!(!result.safe, "expected safe=no");
        assert_eq!(result.clusters[0].remaining_live, 1);
        assert!(result.clusters[0].would_isolate);
        let reason = result.reason.as_deref().unwrap_or("");
        assert!(reason.contains("cluster-0001"), "reason={}", reason);
        assert!(reason.contains("1"), "reason={}", reason);
    }

    // Test 3: session in no clusters → safe=yes, cluster_count=0
    #[test]
    fn prune_check_no_clusters() {
        let mut store = AmsStore::new();
        store
            .upsert_object("lone-session", "session_ref", None, None, None)
            .unwrap();
        let result = session_prune_check(&store, "lone-session").unwrap();
        assert!(result.safe);
        assert_eq!(result.cluster_count, 0);
        assert!(result.clusters.is_empty());
    }

    // Test 4: tombstones do NOT count as live members
    #[test]
    fn prune_check_tombstones_not_live() {
        // cluster has: s-candidate, s-live, tombstone-x
        // removing s-candidate leaves 1 live → safe=no
        let path = "smartlist/dream-topics/cluster-tomb";
        let cid = format!("smartlist-members:{}", path);

        let mut store = AmsStore::new();
        store.create_container(cid.clone(), "smartlist_members", "smartlist_members").unwrap();

        store.upsert_object("s-candidate", "session_ref", None, None, None).unwrap();
        store.upsert_object("s-live", "session_ref", None, None, None).unwrap();
        store.upsert_object("tombstone-x", SESSION_TOMBSTONE_KIND, None, None, None).unwrap();

        for id in &["s-candidate", "s-live", "tombstone-x"] {
            store.add_object(&cid, *id, None, None).unwrap();
        }

        let result = session_prune_check(&store, "s-candidate").unwrap();
        assert!(!result.safe, "tombstone should not count as live");
        assert_eq!(result.clusters[0].remaining_live, 1);
    }

    // Test 5: session not found → error
    #[test]
    fn prune_check_session_not_found() {
        let store = AmsStore::new();
        let err = session_prune_check(&store, "nonexistent").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    // Test 6: format_prune_check_result key=value output
    #[test]
    fn prune_check_format_safe_yes() {
        let result = SessionPruneCheckResult {
            session_id: "s-1".to_string(),
            safe: true,
            reason: None,
            cluster_count: 1,
            clusters: vec![ClusterPruneInfo {
                cluster_id: "cluster-0001".to_string(),
                smartlist_path: "smartlist/dream-topics/cluster-0001".to_string(),
                remaining_live: 3,
                would_isolate: false,
            }],
        };
        let out = format_prune_check_result(&result);
        assert!(out.contains("safe=yes"), "out={}", out);
        assert!(out.contains("cluster_count=1"), "out={}", out);
        assert!(!out.contains("reason="), "should not have reason when safe");
    }

    #[test]
    fn prune_check_format_safe_no() {
        let result = SessionPruneCheckResult {
            session_id: "s-2".to_string(),
            safe: false,
            reason: Some("cluster cluster-0001 would drop to 1 live member".to_string()),
            cluster_count: 1,
            clusters: vec![ClusterPruneInfo {
                cluster_id: "cluster-0001".to_string(),
                smartlist_path: "smartlist/dream-topics/cluster-0001".to_string(),
                remaining_live: 1,
                would_isolate: true,
            }],
        };
        let out = format_prune_check_result(&result);
        assert!(out.contains("safe=no"), "out={}", out);
        assert!(out.contains("reason=cluster cluster-0001"), "out={}", out);
        assert!(out.contains("would_isolate=yes"), "out={}", out);
    }

    // ── session_prune_safe tests ──────────────────────────────────────────────

    // Test: safe session → pruned, tombstone exists, original ghosted.
    #[test]
    fn prune_safe_safe_session_is_pruned() {
        // 3-member cluster — removing one is safe.
        let mut store = make_cluster_store(
            &["s-candidate", "s-b", "s-c"],
            "cluster-0001",
        );
        let result = session_prune_safe(&mut store, "s-candidate", "test-gc", now()).unwrap();
        assert!(result.pruned, "expected pruned=true");
        assert_eq!(result.tombstone_object_id.as_deref(), Some("tombstone:s-candidate"));
        assert_eq!(result.clusters_preserved, 1);
        assert!(result.skip_reason.is_none());

        // Tombstone must exist.
        assert!(store.objects().contains_key("tombstone:s-candidate"), "tombstone not found");

        // Original session must be ghosted.
        let orig = store.objects().get("s-candidate").unwrap();
        let prov = orig.semantic_payload.as_ref().unwrap().provenance.as_ref().unwrap();
        assert_eq!(prov.get("validity_state").and_then(|v| v.as_str()), Some("ghosted"));
    }

    // Test: unsafe session → skipped, no tombstone, original untouched.
    #[test]
    fn prune_safe_unsafe_session_is_skipped() {
        // 2-member cluster — removing one would isolate it.
        let mut store = make_cluster_store(&["s-a", "s-b"], "cluster-0001");
        let result = session_prune_safe(&mut store, "s-a", "test-gc", now()).unwrap();
        assert!(!result.pruned, "expected pruned=false");
        assert!(result.tombstone_object_id.is_none());
        assert!(result.skip_reason.is_some());

        // Tombstone must NOT exist.
        assert!(!store.objects().contains_key("tombstone:s-a"), "tombstone should not be created");

        // Original session must NOT be ghosted.
        let orig = store.objects().get("s-a").unwrap();
        let ghosted = orig
            .semantic_payload
            .as_ref()
            .and_then(|p| p.provenance.as_ref())
            .and_then(|pv| pv.get("validity_state"))
            .and_then(|v| v.as_str())
            == Some("ghosted");
        assert!(!ghosted, "original should not be ghosted when skipped");
    }

    // ── session_prune_batch tests ─────────────────────────────────────────────

    // Test: batch with 3 sessions, 2 safe → pruned=2 skipped=1.
    #[test]
    fn prune_batch_two_safe_one_unsafe() {
        // cluster-A: 3 members (safe to remove s-a1)
        // cluster-B: 3 members (safe to remove s-b1)
        // cluster-C: 2 members (unsafe to remove s-c1)
        let mut store = AmsStore::new();

        fn add_cluster(store: &mut AmsStore, name: &str, members: &[&str]) {
            let path = format!("smartlist/dream-topics/{}", name);
            let cid = format!("smartlist-members:{}", path);
            store.create_container(cid.clone(), "smartlist_members", "smartlist_members").unwrap();
            for &m in members {
                if !store.objects().contains_key(m) {
                    store.upsert_object(m, "session_ref", None, None, None).unwrap();
                }
                store.add_object(&cid, m, None, None).unwrap();
            }
        }

        add_cluster(&mut store, "cluster-A", &["s-a1", "s-a2", "s-a3"]);
        add_cluster(&mut store, "cluster-B", &["s-b1", "s-b2", "s-b3"]);
        add_cluster(&mut store, "cluster-C", &["s-c1", "s-c2"]);

        let ids: Vec<String> = vec!["s-a1".into(), "s-b1".into(), "s-c1".into()];
        let result = session_prune_batch(&mut store, &ids, "test-gc", now());

        assert_eq!(result.pruned, 2);
        assert_eq!(result.skipped, 1);
        assert_eq!(result.total, 3);
    }

    // ── session_tombstone_expire tests ────────────────────────────────────────

    /// Helper: create a tombstone with a specified ghosted_at timestamp.
    fn make_tombstone_at(store: &mut AmsStore, tombstone_id: &str, ghosted_at: DateTime<FixedOffset>) {
        use crate::model::JsonMap;
        let mut prov = JsonMap::new();
        prov.insert(PROV_GHOSTED_AT.to_string(), json!(ghosted_at.to_rfc3339()));
        let payload = SemanticPayload {
            embedding: None,
            tags: None,
            summary: None,
            provenance: Some(prov),
        };
        store
            .upsert_object(tombstone_id, SESSION_TOMBSTONE_KIND, None, Some(payload), Some(ghosted_at))
            .unwrap();
    }

    // Test 1: 3 tombstones, 2 older than threshold → expired=2 kept=1
    #[test]
    fn tombstone_expire_two_old_one_recent() {
        let base = DateTime::parse_from_rfc3339("2026-01-01T00:00:00+00:00").unwrap();
        let now_ts = DateTime::parse_from_rfc3339("2026-03-20T00:00:00+00:00").unwrap();
        // 78 days before now_ts
        let old1 = base; // 78 days ago — older than 30d threshold
        let old2 = DateTime::parse_from_rfc3339("2026-02-01T00:00:00+00:00").unwrap(); // 47 days ago
        let recent = DateTime::parse_from_rfc3339("2026-03-15T00:00:00+00:00").unwrap(); // 5 days ago

        let mut store = AmsStore::new();
        make_tombstone_at(&mut store, "tombstone:old1", old1);
        make_tombstone_at(&mut store, "tombstone:old2", old2);
        make_tombstone_at(&mut store, "tombstone:recent", recent);

        let result = session_tombstone_expire(&mut store, 30, now_ts).unwrap();
        assert_eq!(result.expired, 2);
        assert_eq!(result.kept, 1);

        // old tombstones must be ghosted
        for id in &["tombstone:old1", "tombstone:old2"] {
            let obj = store.objects().get(*id).unwrap();
            let prov = obj.semantic_payload.as_ref().unwrap().provenance.as_ref().unwrap();
            assert_eq!(
                prov.get("validity_state").and_then(|v| v.as_str()),
                Some("ghosted"),
                "{id} should be ghosted"
            );
        }

        // recent tombstone must NOT be ghosted
        let recent_obj = store.objects().get("tombstone:recent").unwrap();
        let recent_ghosted = recent_obj
            .semantic_payload
            .as_ref()
            .and_then(|p| p.provenance.as_ref())
            .and_then(|pv| pv.get("validity_state"))
            .and_then(|v| v.as_str())
            == Some("ghosted");
        assert!(!recent_ghosted, "recent tombstone should not be ghosted");
    }

    // Test 2: max_age_days=0 expires everything
    #[test]
    fn tombstone_expire_max_age_zero_expires_all() {
        let ts = DateTime::parse_from_rfc3339("2026-01-01T00:00:00+00:00").unwrap();
        let now_ts = DateTime::parse_from_rfc3339("2026-03-20T00:00:00+00:00").unwrap();

        let mut store = AmsStore::new();
        make_tombstone_at(&mut store, "tombstone:a", ts);
        make_tombstone_at(&mut store, "tombstone:b", ts);

        let result = session_tombstone_expire(&mut store, 0, now_ts).unwrap();
        assert_eq!(result.expired, 2);
        assert_eq!(result.kept, 0);
    }

    // Test 3: no tombstones → expired=0 kept=0
    #[test]
    fn tombstone_expire_no_tombstones() {
        let mut store = AmsStore::new();
        store.upsert_object("session:x", "session_ref", None, None, None).unwrap();
        let now_ts = DateTime::parse_from_rfc3339("2026-03-20T00:00:00+00:00").unwrap();

        let result = session_tombstone_expire(&mut store, 30, now_ts).unwrap();
        assert_eq!(result.expired, 0);
        assert_eq!(result.kept, 0);
    }

    // Test 4: already-ghosted tombstones are ignored
    #[test]
    fn tombstone_expire_ignores_already_ghosted() {
        let old_ts = DateTime::parse_from_rfc3339("2026-01-01T00:00:00+00:00").unwrap();
        let now_ts = DateTime::parse_from_rfc3339("2026-03-20T00:00:00+00:00").unwrap();

        let mut store = AmsStore::new();
        make_tombstone_at(&mut store, "tombstone:already-ghosted", old_ts);
        // Pre-ghost it (simulate a previous expiry run).
        crate::cache::ghost_artifact(&mut store, "tombstone:already-ghosted", "tombstone-expired").unwrap();

        let result = session_tombstone_expire(&mut store, 30, now_ts).unwrap();
        // Should not re-expire the already-ghosted tombstone.
        assert_eq!(result.expired, 0);
        assert_eq!(result.kept, 0);
    }
}
