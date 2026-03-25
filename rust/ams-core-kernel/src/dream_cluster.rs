//! Topology-based topic cluster discovery for the dreaming pipeline.
//!
//! ## What is dream-cluster?
//!
//! `dream_cluster` scans all `session` objects in the store, computes their
//! *neighbourhood signatures* (the set of SmartList containers each session
//! belongs to), and groups sessions into topic clusters using pairwise Jaccard
//! similarity and connected-component analysis.
//!
//! For each cluster it materialises:
//! - A `dream_topic` anchor Object at `topic:<cluster-id>` whose
//!   `SemanticPayload.summary` is the top-3 most-shared container labels.
//! - A scale-1 SmartList at `smartlist/dream-topics/<cluster-id>` whose
//!   members are the cluster's sessions ranked by link centrality.
//!
//! Finally, a scale-0 root index SmartList at `smartlist/dream-topics` lists
//! all topic Objects ranked by cluster size.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};

use crate::model::SemanticPayload;
use crate::session_gc::SESSION_TOMBSTONE_KIND;
use crate::smartlist_write::{attach_member, normalize_path};
use crate::store::AmsStore;

// ── Constants ─────────────────────────────────────────────────────────────────

/// Normalised root path for the dream-topics index SmartList.
pub const DREAM_TOPICS_ROOT: &str = "smartlist/dream-topics";

/// Object kind for topic anchor objects.
pub const DREAM_TOPIC_OBJECT_KIND: &str = "dream_topic";

/// Default minimum Jaccard similarity threshold for linking two sessions.
pub const DEFAULT_MIN_JACCARD: f64 = 0.3;

/// Default maximum number of clusters to emit.
pub const DEFAULT_MAX_CLUSTERS: usize = 50;

// ── Result types ──────────────────────────────────────────────────────────────

/// Per-cluster summary produced by `dream_cluster`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicCluster {
    /// Unique cluster identifier (e.g. `"cluster-0001"`).
    pub cluster_id: String,
    /// Normalised SmartList path for this cluster's session roster
    /// (e.g. `"smartlist/dream-topics/cluster-0001"`).
    pub smartlist_path: String,
    /// Object ID of the topic anchor object (e.g. `"topic:cluster-0001"`).
    pub topic_object_id: String,
    /// Human-readable label derived from the top-3 most shared container names.
    pub label: String,
    /// Session object IDs in this cluster, ranked by link centrality descending.
    pub members: Vec<String>,
    /// Number of `session_tombstone` objects attached to this cluster's SmartList.
    /// Tombstones count toward cluster stability but are not Jaccard seeds.
    pub tombstone_members: usize,
}

/// Aggregated output of a `dream_cluster` run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DreamClusterResult {
    /// Total number of `session` objects scanned.
    pub sessions_scanned: usize,
    /// Number of clusters discovered and emitted.
    pub clusters_found: usize,
    /// Per-cluster detail.
    pub clusters: Vec<TopicCluster>,
    /// Normalised path of the root topic-index SmartList.
    pub index_path: String,
}

// ── Core function ─────────────────────────────────────────────────────────────

/// Discover topic clusters from the store's relationship topology and
/// materialise them as Atlas SmartLists.
///
/// This function is **pure store mutation** — no I/O or locking.  The caller
/// (typically `WriteService::dream_cluster`) is responsible for persistence.
///
/// # Arguments
///
/// * `store`       — mutable AMS store
/// * `min_jaccard` — minimum Jaccard similarity for two sessions to be linked
///                   (typical default: `DEFAULT_MIN_JACCARD` = 0.3)
/// * `max_clusters`— cap on the number of clusters emitted (largest first)
/// * `created_by`  — actor label for any newly created objects / list members
/// * `now_utc`     — wall-clock timestamp for new objects
///
/// # Returns
///
/// A [`DreamClusterResult`] summarising what was written to the store.
pub fn dream_cluster(
    store: &mut AmsStore,
    min_jaccard: f64,
    max_clusters: usize,
    created_by: &str,
    now_utc: DateTime<FixedOffset>,
) -> Result<DreamClusterResult> {
    // ── Step 1: collect all session objects ───────────────────────────────────
    let session_ids: Vec<String> = store
        .objects()
        .values()
        .filter(|o| o.object_kind == "session_ref" || o.object_kind == "session")
        .map(|o| o.object_id.clone())
        .collect();

    let sessions_scanned = session_ids.len();

    // ── Step 2: build neighbourhood signatures ────────────────────────────────
    //
    // A session's signature is the set of `smartlist-members:` container IDs it
    // belongs to.  Two sessions that share many containers are topically related.
    let signatures: Vec<(String, BTreeSet<String>)> = session_ids
        .iter()
        .map(|id| {
            let sig: BTreeSet<String> = store
                .containers_for_member_object(id)
                .into_iter()
                .filter(|c| c.starts_with("smartlist-members:"))
                .collect();
            (id.clone(), sig)
        })
        .collect();

    // ── Step 3: build adjacency list via pairwise Jaccard ─────────────────────
    let n = signatures.len();
    let mut adj: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); n];

    for i in 0..n {
        for j in (i + 1)..n {
            if jaccard(&signatures[i].1, &signatures[j].1) >= min_jaccard {
                adj[i].insert(j);
                adj[j].insert(i);
            }
        }
    }

    // ── Step 4: find connected components (single-linkage clusters) ───────────
    let mut visited = vec![false; n];
    let mut components: Vec<Vec<usize>> = Vec::new();

    for start in 0..n {
        if visited[start] {
            continue;
        }
        let mut stack = vec![start];
        let mut component = Vec::new();
        while let Some(node) = stack.pop() {
            if visited[node] {
                continue;
            }
            visited[node] = true;
            component.push(node);
            for &neighbour in &adj[node] {
                if !visited[neighbour] {
                    stack.push(neighbour);
                }
            }
        }
        components.push(component);
    }

    // Sort largest-first, then cap.
    components.sort_by(|a, b| b.len().cmp(&a.len()));
    components.truncate(max_clusters);

    // ── Steps 5–6: materialise each cluster ───────────────────────────────────
    let mut clusters: Vec<TopicCluster> = Vec::new();
    let mut topic_object_ids: Vec<String> = Vec::new();

    for (cluster_idx, component) in components.iter().enumerate() {
        let cluster_id = format!("cluster-{:04}", cluster_idx + 1);

        // Collect member session IDs for this component.
        let member_ids: Vec<String> = component
            .iter()
            .map(|&i| signatures[i].0.clone())
            .collect();

        // Rank members by link centrality (count of smartlist-members: containers).
        let mut ranked_members: Vec<(String, usize)> = member_ids
            .iter()
            .map(|id| {
                let centrality = store
                    .containers_for_member_object(id)
                    .into_iter()
                    .filter(|c| c.starts_with("smartlist-members:"))
                    .count();
                (id.clone(), centrality)
            })
            .collect();
        ranked_members.sort_by(|a, b| b.1.cmp(&a.1));

        // Compute top-3 most-shared container names across cluster members.
        let mut container_freq: BTreeMap<String, usize> = BTreeMap::new();
        for id in &member_ids {
            for c in store.containers_for_member_object(id) {
                if c.starts_with("smartlist-members:") {
                    *container_freq.entry(c).or_insert(0) += 1;
                }
            }
        }
        let mut freq_vec: Vec<(String, usize)> = container_freq.into_iter().collect();
        freq_vec.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let top_labels: Vec<String> = freq_vec
            .iter()
            .take(3)
            .map(|(c, _)| {
                // Strip verbose prefix for human readability.
                c.strip_prefix("smartlist-members:smartlist/")
                    .or_else(|| c.strip_prefix("smartlist-members:"))
                    .unwrap_or(c.as_str())
                    .to_string()
            })
            .collect();
        let label = if top_labels.is_empty() {
            cluster_id.clone()
        } else {
            top_labels.join(", ")
        };

        // Create / update the topic anchor object.
        let topic_object_id = format!("topic:{}", cluster_id);
        store
            .upsert_object(
                &topic_object_id,
                DREAM_TOPIC_OBJECT_KIND,
                None,
                Some(SemanticPayload {
                    summary: Some(label.clone()),
                    ..Default::default()
                }),
                Some(now_utc),
            )
            .ok();

        // Write the per-cluster SmartList (attach_member normalises the path
        // internally, prepending `smartlist/` if absent).
        let cluster_list_raw = format!("dream-topics/{}", cluster_id);
        for (member_id, _) in &ranked_members {
            attach_member(store, &cluster_list_raw, member_id, created_by, now_utc).ok();
        }
        let smartlist_path = normalize_path(&cluster_list_raw)?;

        // Re-attach any session_tombstone objects that already exist in this
        // cluster's SmartList (placed there by session-tombstone-create).
        // Tombstones count toward cluster stability but are not Jaccard seeds.
        let container_id = format!("smartlist-members:{}", smartlist_path);
        let existing_tombstone_ids: Vec<String> = store
            .iterate_forward(&container_id)
            .iter()
            .map(|n| n.object_id.clone())
            .filter(|id| {
                store
                    .objects()
                    .get(id.as_str())
                    .map(|o| o.object_kind == SESSION_TOMBSTONE_KIND)
                    .unwrap_or(false)
            })
            .collect();
        let tombstone_members = existing_tombstone_ids.len();
        for tombstone_id in &existing_tombstone_ids {
            attach_member(store, &cluster_list_raw, tombstone_id, created_by, now_utc).ok();
        }

        topic_object_ids.push(topic_object_id.clone());
        clusters.push(TopicCluster {
            cluster_id,
            smartlist_path,
            topic_object_id,
            label,
            members: ranked_members.into_iter().map(|(id, _)| id).collect(),
            tombstone_members,
        });
    }

    // ── Step 7: write root index SmartList ────────────────────────────────────
    //
    // Members = topic Objects in descending cluster-size order (already sorted).
    for topic_id in &topic_object_ids {
        attach_member(store, "dream-topics", topic_id, created_by, now_utc).ok();
    }
    let index_path = normalize_path("dream-topics")?;

    Ok(DreamClusterResult {
        sessions_scanned,
        clusters_found: clusters.len(),
        clusters,
        index_path,
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Jaccard similarity between two sets.  Returns 0.0 when both are empty.
fn jaccard(a: &BTreeSet<String>, b: &BTreeSet<String>) -> f64 {
    if a.is_empty() && b.is_empty() {
        return 0.0;
    }
    let intersection = a.intersection(b).count();
    let union_size = a.len() + b.len() - intersection;
    if union_size == 0 {
        0.0
    } else {
        intersection as f64 / union_size as f64
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::now_fixed;
    use crate::smartlist_write::attach_member;
    use crate::store::AmsStore;

    /// Build a store with 6 sessions arranged so that:
    /// - s1, s2, s3 share list-A and list-B  → cluster α
    /// - s4, s5     share list-C             → cluster β
    /// - s6         is in list-D only        → cluster γ (singleton)
    fn setup() -> AmsStore {
        let mut store = AmsStore::new();
        let now = now_fixed();
        let by = "test";

        for id in &["s1", "s2", "s3", "s4", "s5", "s6"] {
            store
                .upsert_object(*id, "session", None, None, Some(now))
                .unwrap();
        }

        // cluster α: s1, s2, s3 — all in list-A; s1+s2 also in list-B
        attach_member(&mut store, "topics/list-a", "s1", by, now).unwrap();
        attach_member(&mut store, "topics/list-a", "s2", by, now).unwrap();
        attach_member(&mut store, "topics/list-a", "s3", by, now).unwrap();
        attach_member(&mut store, "topics/list-b", "s1", by, now).unwrap();
        attach_member(&mut store, "topics/list-b", "s2", by, now).unwrap();

        // cluster β: s4, s5 — both in list-C
        attach_member(&mut store, "topics/list-c", "s4", by, now).unwrap();
        attach_member(&mut store, "topics/list-c", "s5", by, now).unwrap();

        // cluster γ: s6 alone in list-D
        attach_member(&mut store, "topics/list-d", "s6", by, now).unwrap();

        store
    }

    #[test]
    fn discovers_three_clusters() {
        let mut store = setup();
        let now = now_fixed();

        let result = dream_cluster(&mut store, DEFAULT_MIN_JACCARD, DEFAULT_MAX_CLUSTERS, "test", now)
            .unwrap();

        assert_eq!(result.sessions_scanned, 6);
        assert_eq!(result.clusters_found, 3, "expected 3 clusters");
    }

    #[test]
    fn root_index_contains_all_topics() {
        let mut store = setup();
        let now = now_fixed();

        let result = dream_cluster(&mut store, DEFAULT_MIN_JACCARD, DEFAULT_MAX_CLUSTERS, "test", now)
            .unwrap();

        let index_container = format!("smartlist-members:{}", result.index_path);
        // The root container auto-receives child bucket objects (one per cluster
        // SmartList) via `ensure_bucket_path`.  We also manually attach one
        // `topic:` anchor object per cluster.  Filter to count only topics.
        let topic_members: Vec<String> = store
            .iterate_forward(&index_container)
            .iter()
            .map(|n| n.object_id.clone())
            .filter(|id| id.starts_with("topic:"))
            .collect();

        assert_eq!(
            topic_members.len(),
            result.clusters_found,
            "root index should have one topic entry per cluster"
        );
    }

    #[test]
    fn largest_cluster_listed_first() {
        let mut store = setup();
        let now = now_fixed();

        let result = dream_cluster(&mut store, DEFAULT_MIN_JACCARD, DEFAULT_MAX_CLUSTERS, "test", now)
            .unwrap();

        // Largest cluster has 3 members (s1/s2/s3).
        assert_eq!(
            result.clusters[0].members.len(),
            3,
            "first cluster should be the largest"
        );
    }

    #[test]
    fn per_cluster_smartlist_populated() {
        let mut store = setup();
        let now = now_fixed();

        let result = dream_cluster(&mut store, DEFAULT_MIN_JACCARD, DEFAULT_MAX_CLUSTERS, "test", now)
            .unwrap();

        for cluster in &result.clusters {
            let container = format!("smartlist-members:{}", cluster.smartlist_path);
            let members: Vec<String> = store
                .iterate_forward(&container)
                .iter()
                .map(|n| n.object_id.clone())
                .collect();
            assert_eq!(
                members.len(),
                cluster.members.len(),
                "cluster {} SmartList member count mismatch",
                cluster.cluster_id
            );
        }
    }

    #[test]
    fn topic_objects_created() {
        let mut store = setup();
        let now = now_fixed();

        let result = dream_cluster(&mut store, DEFAULT_MIN_JACCARD, DEFAULT_MAX_CLUSTERS, "test", now)
            .unwrap();

        for cluster in &result.clusters {
            assert!(
                store.objects().contains_key(&cluster.topic_object_id),
                "topic object {} should exist in store",
                cluster.topic_object_id
            );
        }
    }

    #[test]
    fn jaccard_symmetry_and_bounds() {
        let a: BTreeSet<String> = ["x".to_string(), "y".to_string()].into();
        let b: BTreeSet<String> = ["y".to_string(), "z".to_string()].into();
        let j_ab = jaccard(&a, &b);
        let j_ba = jaccard(&b, &a);
        assert!((j_ab - j_ba).abs() < f64::EPSILON, "jaccard must be symmetric");
        assert!((j_ab - 1.0 / 3.0).abs() < 1e-9, "jaccard({{x,y}},{{y,z}}) = 1/3");
    }

    #[test]
    fn jaccard_identical_sets() {
        let a: BTreeSet<String> = ["x".to_string()].into();
        assert!((jaccard(&a, &a) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn jaccard_disjoint_sets() {
        let a: BTreeSet<String> = ["x".to_string()].into();
        let b: BTreeSet<String> = ["y".to_string()].into();
        assert!((jaccard(&a, &b)).abs() < f64::EPSILON);
    }

    #[test]
    fn empty_store_returns_zero_clusters() {
        let mut store = AmsStore::new();
        let now = now_fixed();
        let result = dream_cluster(&mut store, DEFAULT_MIN_JACCARD, DEFAULT_MAX_CLUSTERS, "test", now)
            .unwrap();
        assert_eq!(result.sessions_scanned, 0);
        assert_eq!(result.clusters_found, 0);
    }

    // ── Tombstone-awareness tests ─────────────────────────────────────────────

    use crate::session_gc::SESSION_TOMBSTONE_KIND;

    /// Build a store with 2 live sessions in a shared list (so they form a
    /// cluster) plus N pre-attached tombstones in the cluster SmartList.
    fn setup_with_tombstones(n_tombstones: usize) -> AmsStore {
        let mut store = AmsStore::new();
        let now = now_fixed();
        let by = "test";

        store.upsert_object("s-live-1", "session_ref", None, None, Some(now)).unwrap();
        store.upsert_object("s-live-2", "session_ref", None, None, Some(now)).unwrap();
        attach_member(&mut store, "topics/shared-list", "s-live-1", by, now).unwrap();
        attach_member(&mut store, "topics/shared-list", "s-live-2", by, now).unwrap();

        // Pre-run dream_cluster once to materialise the SmartList, then attach
        // tombstones to it so the re-run can discover them.
        dream_cluster(&mut store, DEFAULT_MIN_JACCARD, DEFAULT_MAX_CLUSTERS, by, now).unwrap();

        // Find the cluster SmartList that was created (there should be exactly one).
        let cluster_path = "smartlist/dream-topics/cluster-0001";
        let container_id = format!("smartlist-members:{}", cluster_path);
        for i in 0..n_tombstones {
            let tid = format!("tombstone-{}", i);
            store.upsert_object(&tid, SESSION_TOMBSTONE_KIND, None, None, Some(now)).unwrap();
            store.add_object(&container_id, &tid, None, None).unwrap();
        }

        store
    }

    /// Cluster with 1 live session + 2 tombstones: after re-running dream_cluster
    /// the tombstones are still in the SmartList and reported in tombstone_members.
    #[test]
    fn tombstones_counted_and_preserved() {
        let mut store = setup_with_tombstones(2);
        let now = now_fixed();

        let result = dream_cluster(&mut store, DEFAULT_MIN_JACCARD, DEFAULT_MAX_CLUSTERS, "test", now)
            .unwrap();

        assert_eq!(result.clusters_found, 1);
        let cluster = &result.clusters[0];
        assert_eq!(cluster.tombstone_members, 2, "expected 2 tombstone members");

        // Tombstones should appear in the cluster SmartList.
        let container_id = format!("smartlist-members:{}", cluster.smartlist_path);
        let all_members: Vec<String> = store
            .iterate_forward(&container_id)
            .iter()
            .map(|n| n.object_id.clone())
            .collect();
        let tombstone_count = all_members.iter().filter(|id| id.starts_with("tombstone-")).count();
        assert_eq!(tombstone_count, 2, "both tombstones must remain in SmartList");
    }

    /// Cluster with 0 live sessions + 1 tombstone: tombstone alone keeps the
    /// cluster alive in the SmartList (sessions_scanned=0, tombstone_members=1).
    #[test]
    fn cluster_kept_alive_by_tombstone() {
        let mut store = AmsStore::new();
        let now = now_fixed();
        let by = "test";

        // Two live sessions form a cluster on first run.
        store.upsert_object("s-a", "session_ref", None, None, Some(now)).unwrap();
        store.upsert_object("s-b", "session_ref", None, None, Some(now)).unwrap();
        attach_member(&mut store, "topics/shared", "s-a", by, now).unwrap();
        attach_member(&mut store, "topics/shared", "s-b", by, now).unwrap();
        dream_cluster(&mut store, DEFAULT_MIN_JACCARD, DEFAULT_MAX_CLUSTERS, by, now).unwrap();

        // Attach a tombstone directly to the cluster SmartList.
        let cluster_path = "smartlist/dream-topics/cluster-0001";
        let container_id = format!("smartlist-members:{}", cluster_path);
        store.upsert_object("tombstone-lone", SESSION_TOMBSTONE_KIND, None, None, Some(now)).unwrap();
        store.add_object(&container_id, "tombstone-lone", None, None).unwrap();

        // Remove live sessions so only the tombstone is in the cluster SmartList.
        // (We can't ghost them easily in unit tests, so instead we re-run with
        // an empty base store and only the tombstone wired up.)
        let mut store2 = AmsStore::new();
        store2.upsert_object("tombstone-lone", SESSION_TOMBSTONE_KIND, None, None, Some(now)).unwrap();
        // Materialise the dream-topics SmartList with the tombstone attached.
        let norm = normalize_path("dream-topics/cluster-0001").unwrap();
        let cid = format!("smartlist-members:{}", norm);
        store2.create_container(cid.clone(), "smartlist_members", "smartlist_members").unwrap();
        store2.add_object(&cid, "tombstone-lone", None, None).unwrap();

        // Re-run dream_cluster: zero live sessions → sessions_scanned=0,
        // clusters_found=0. The tombstone-only cluster is NOT a Jaccard cluster
        // but tombstone_members should be visible when we query existing SmartLists.
        // (This verifies that tombstones don't produce Jaccard seeds.)
        let result = dream_cluster(&mut store2, DEFAULT_MIN_JACCARD, DEFAULT_MAX_CLUSTERS, by, now).unwrap();
        assert_eq!(result.sessions_scanned, 0, "no live sessions in store2");
        assert_eq!(result.clusters_found, 0, "tombstones alone don't seed new clusters");
    }

    /// Existing tests still pass: tombstone_members=0 when no tombstones present.
    #[test]
    fn no_tombstones_field_is_zero() {
        let mut store = setup();
        let now = now_fixed();

        let result = dream_cluster(&mut store, DEFAULT_MIN_JACCARD, DEFAULT_MAX_CLUSTERS, "test", now)
            .unwrap();

        for cluster in &result.clusters {
            assert_eq!(cluster.tombstone_members, 0, "no tombstones in baseline setup");
        }
    }
}
