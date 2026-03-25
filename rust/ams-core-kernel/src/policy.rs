//! Layer 3c — Policy Layers
//!
//! Graph constraint policies enforced as optional layers over the substrate at mutation time.
//!
//! Policies are stored on `ContainerRecord.policies` and checked before any membership mutation:
//!
//! - `unique_members`      — already-present members are silently skipped (idempotent attach)
//! - `max_members`         — cardinality ceiling; behaviour on overflow controlled by `overflow_policy`
//! - `overflow_policy`     — `reject` (default) errors on overflow; `evict_oldest` removes head first
//! - `ordered_by_recency`  — new member's `created_at` must be >= current tail's `created_at`
//! - `graph_shape = Tree`  — each object may belong to at most one container
//! - `graph_shape = Dag`   — membership chains must not form directed cycles
//!
//! Entry point: `enforce_add_member_policies` — call before committing a new membership edge.
//! Entry point: `set_container_policy`        — write one policy field onto a container.

use anyhow::{bail, Result};

use crate::model::{ContainerPolicies, GraphShape, OverflowPolicy};
use crate::store::AmsStore;

// ── Public enforcement API ────────────────────────────────────────────────────

/// Check all active policies on `container_id` before adding `object_id` as a new member.
///
/// Returns `Ok(AddMemberDecision)` describing whether the add should proceed or be skipped.
pub fn enforce_add_member_policies(
    store: &AmsStore,
    container_id: &str,
    object_id: &str,
) -> Result<AddMemberDecision> {
    let policies = match store.containers().get(container_id) {
        Some(c) => c.policies.clone(),
        None => ContainerPolicies::default(),
    };

    // ── unique_members ───────────────────────────────────────────────────────
    if policies.unique_members && store.has_membership(container_id, object_id) {
        return Ok(AddMemberDecision::Skip("already a member (unique_members)"));
    }

    // ── max_members / overflow_policy ────────────────────────────────────────
    if let Some(max) = policies.max_members {
        let members = store.iterate_forward(container_id);
        let current = members.len();
        if current >= max {
            match &policies.overflow_policy {
                OverflowPolicy::Reject => {
                    bail!(
                        "policy violation: container '{}' has reached max_members limit of {}",
                        container_id,
                        max
                    );
                }
                OverflowPolicy::EvictOldest => {
                    // Return the head link-node ID so the caller can remove it before adding.
                    if let Some(head) = members.first() {
                        return Ok(AddMemberDecision::EvictOldest(head.link_node_id.clone()));
                    }
                    // Degenerate: count >= max but no head — fall through to Add.
                }
            }
        }
    }

    // ── ordered_by_recency ───────────────────────────────────────────────────
    if policies.ordered_by_recency {
        // The new object's `created_at` must be >= the current tail's `created_at`.
        if let Some(tail_node) = store.iterate_forward(container_id).last() {
            let tail_obj_id = tail_node.object_id.clone();
            if let (Some(tail_obj), Some(new_obj)) = (
                store.objects().get(&tail_obj_id),
                store.objects().get(object_id),
            ) {
                if new_obj.created_at < tail_obj.created_at {
                    bail!(
                        "policy violation: ordered_by_recency — object '{}' created_at {} is \
                         earlier than current tail '{}' created_at {}; items must be attached \
                         in non-decreasing date order",
                        object_id,
                        new_obj.created_at,
                        tail_obj_id,
                        tail_obj.created_at,
                    );
                }
            }
        }
    }

    // ── graph_shape ──────────────────────────────────────────────────────────
    match &policies.graph_shape {
        GraphShape::Any => {}

        GraphShape::Tree => {
            if !policies.allow_multi_parent {
                let existing_containers = store.containers_for_member_object(object_id);
                let foreign: Vec<_> = existing_containers
                    .iter()
                    .filter(|cid| cid.as_str() != container_id)
                    .collect();
                if !foreign.is_empty() {
                    bail!(
                        "policy violation: graph_shape=tree — object '{}' already belongs to \
                         container(s) {:?}. Remove from those containers first or set \
                         allow_multi_parent=true.",
                        object_id,
                        foreign
                    );
                }
            }
        }

        GraphShape::Dag => {
            // Reject if `container_id` is transitively reachable from `object_id`.
            // We walk the membership graph from `object_id` upward via container membership
            // looking for `container_id`.
            if has_path_to_container(store, object_id, container_id) {
                bail!(
                    "policy violation: graph_shape=dag — adding object '{}' to container '{}' \
                     would create a cycle.",
                    object_id,
                    container_id
                );
            }
        }
    }

    Ok(AddMemberDecision::Add)
}

/// Decision returned by `enforce_add_member_policies`.
#[derive(Debug, PartialEq, Eq)]
pub enum AddMemberDecision {
    /// Proceed with the add.
    Add,
    /// Skip silently (idempotent — member already present).
    Skip(&'static str),
    /// Evict the head member (identified by this link-node ID) first, then add.
    /// Returned when `overflow_policy = evict_oldest` and the container is at capacity.
    EvictOldest(String),
}

// ── Policy mutation API ───────────────────────────────────────────────────────

/// Result of a `set_container_policy` call.
#[derive(Debug)]
pub struct SetPolicyResult {
    pub container_id: String,
    pub field: String,
}

/// Set a single policy field on an existing container.
///
/// `field` is one of:
/// - `"unique_members"` — value `"true"` / `"false"`
/// - `"max_members"` — value `"<n>"` or `"none"` to remove the limit
/// - `"overflow_policy"` — value `"reject"` / `"evict_oldest"`
/// - `"ordered_by_recency"` — value `"true"` / `"false"`
/// - `"graph_shape"` — value `"any"` / `"tree"` / `"dag"`
/// - `"allow_multi_parent"` — value `"true"` / `"false"`
pub fn set_container_policy(
    store: &mut AmsStore,
    container_id: &str,
    field: &str,
    value: &str,
) -> Result<SetPolicyResult> {
    let container = store
        .containers_mut()
        .get_mut(container_id)
        .ok_or_else(|| anyhow::anyhow!("container '{}' not found", container_id))?;

    match field {
        "unique_members" => {
            container.policies.unique_members = parse_bool(value, field)?;
        }
        "allow_multi_parent" => {
            container.policies.allow_multi_parent = parse_bool(value, field)?;
        }
        "ordered_by_recency" => {
            container.policies.ordered_by_recency = parse_bool(value, field)?;
        }
        "max_members" => {
            container.policies.max_members = if value.eq_ignore_ascii_case("none") {
                None
            } else {
                let n: usize = value
                    .parse()
                    .map_err(|_| anyhow::anyhow!("max_members must be a non-negative integer or 'none', got '{}'", value))?;
                Some(n)
            };
        }
        "overflow_policy" => {
            container.policies.overflow_policy = match value.to_ascii_lowercase().as_str() {
                "reject" => OverflowPolicy::Reject,
                "evict_oldest" => OverflowPolicy::EvictOldest,
                other => bail!(
                    "unknown overflow_policy '{}'. Valid values: reject, evict_oldest",
                    other
                ),
            };
        }
        "graph_shape" => {
            container.policies.graph_shape = match value.to_ascii_lowercase().as_str() {
                "any" => GraphShape::Any,
                "tree" => GraphShape::Tree,
                "dag" => GraphShape::Dag,
                other => bail!(
                    "unknown graph_shape '{}'. Valid values: any, tree, dag",
                    other
                ),
            };
        }
        other => bail!(
            "unknown policy field '{}'. Valid: unique_members, max_members, overflow_policy, ordered_by_recency, graph_shape, allow_multi_parent",
            other
        ),
    }

    Ok(SetPolicyResult {
        container_id: container_id.to_string(),
        field: field.to_string(),
    })
}

/// Read and render the current policies for a container as a JSON-like string.
pub fn show_container_policy(store: &AmsStore, container_id: &str) -> Result<ContainerPolicies> {
    let container = store
        .containers()
        .get(container_id)
        .ok_or_else(|| anyhow::anyhow!("container '{}' not found", container_id))?;
    Ok(container.policies.clone())
}

// ── DAG cycle detection ───────────────────────────────────────────────────────

/// Returns `true` if `target_container_id` is reachable from `start_object_id` by following
/// object → container membership edges upward through the store.
///
/// This is a BFS over the membership graph.  We look at every container that `start_object_id`
/// belongs to, then at every object that is a member of *those* containers, and so on.
fn has_path_to_container(
    store: &AmsStore,
    start_object_id: &str,
    target_container_id: &str,
) -> bool {
    use std::collections::VecDeque;

    let mut visited_objects: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    queue.push_back(start_object_id.to_string());

    while let Some(obj_id) = queue.pop_front() {
        if visited_objects.contains(&obj_id) {
            continue;
        }
        visited_objects.insert(obj_id.clone());

        let containers = store.containers_for_member_object(&obj_id);
        for cid in containers {
            if cid == target_container_id {
                return true;
            }
            // Expand: every object in `cid` could be a path to target
            for link in store.iterate_forward(&cid) {
                if !visited_objects.contains(&link.object_id) {
                    queue.push_back(link.object_id.clone());
                }
            }
        }
    }
    false
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn parse_bool(value: &str, field: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        other => bail!("'{}' expects true/false, got '{}'", field, other),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::AmsStore;

    fn make_store() -> AmsStore {
        AmsStore::new()
    }

    fn add_container(store: &mut AmsStore, id: &str) {
        store.create_container(id.to_string(), "container", "test").unwrap();
    }

    fn add_object(store: &mut AmsStore, id: &str) {
        store.upsert_object(id.to_string(), "test", None, None, None).unwrap();
    }

    fn add_membership(store: &mut AmsStore, container_id: &str, object_id: &str) {
        store.add_object(container_id, object_id, None, None).unwrap();
    }

    #[test]
    fn unique_members_skip_on_duplicate() {
        let mut store = make_store();
        add_container(&mut store, "c1");
        store.containers_mut().get_mut("c1").unwrap().policies.unique_members = true;
        add_object(&mut store, "o1");
        add_membership(&mut store, "c1", "o1");

        let decision = enforce_add_member_policies(&store, "c1", "o1").unwrap();
        assert_eq!(decision, AddMemberDecision::Skip("already a member (unique_members)"));
    }

    #[test]
    fn unique_members_add_on_new() {
        let mut store = make_store();
        add_container(&mut store, "c1");
        store.containers_mut().get_mut("c1").unwrap().policies.unique_members = true;
        add_object(&mut store, "o1");

        let decision = enforce_add_member_policies(&store, "c1", "o1").unwrap();
        assert_eq!(decision, AddMemberDecision::Add);
    }

    #[test]
    fn max_members_rejects_when_full() {
        let mut store = make_store();
        add_container(&mut store, "c1");
        store.containers_mut().get_mut("c1").unwrap().policies.max_members = Some(2);
        add_object(&mut store, "o1");
        add_object(&mut store, "o2");
        add_object(&mut store, "o3");
        add_membership(&mut store, "c1", "o1");
        add_membership(&mut store, "c1", "o2");

        let result = enforce_add_member_policies(&store, "c1", "o3");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max_members"));
    }

    #[test]
    fn max_members_allows_when_not_full() {
        let mut store = make_store();
        add_container(&mut store, "c1");
        store.containers_mut().get_mut("c1").unwrap().policies.max_members = Some(3);
        add_object(&mut store, "o1");
        add_membership(&mut store, "c1", "o1");

        let decision = enforce_add_member_policies(&store, "c1", "o2").unwrap();
        assert_eq!(decision, AddMemberDecision::Add);
    }

    #[test]
    fn tree_shape_rejects_multi_parent() {
        let mut store = make_store();
        add_container(&mut store, "c1");
        add_container(&mut store, "c2");
        let c1 = store.containers_mut().get_mut("c1").unwrap();
        c1.policies.graph_shape = GraphShape::Tree;
        let c2 = store.containers_mut().get_mut("c2").unwrap();
        c2.policies.graph_shape = GraphShape::Tree;
        add_object(&mut store, "o1");
        add_membership(&mut store, "c1", "o1");

        let result = enforce_add_member_policies(&store, "c2", "o1");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("tree"));
    }

    #[test]
    fn tree_shape_allows_with_multi_parent_flag() {
        let mut store = make_store();
        add_container(&mut store, "c1");
        add_container(&mut store, "c2");
        let c1 = store.containers_mut().get_mut("c1").unwrap();
        c1.policies.graph_shape = GraphShape::Tree;
        let c2 = store.containers_mut().get_mut("c2").unwrap();
        c2.policies.graph_shape = GraphShape::Tree;
        c2.policies.allow_multi_parent = true;
        add_object(&mut store, "o1");
        add_membership(&mut store, "c1", "o1");

        let decision = enforce_add_member_policies(&store, "c2", "o1").unwrap();
        assert_eq!(decision, AddMemberDecision::Add);
    }

    #[test]
    fn dag_shape_rejects_cycle() {
        // c1 contains o1; o1 is same as the bucket object for c1 (self-reference scenario)
        // More concretely: c2 contains o_c1 (the object representing c1's bucket);
        // adding c2 to c1 would create a cycle.
        let mut store = make_store();
        add_container(&mut store, "c1");
        add_container(&mut store, "c2");
        store.containers_mut().get_mut("c1").unwrap().policies.graph_shape = GraphShape::Dag;
        // o_c1 represents c1's bucket object
        add_object(&mut store, "o_c1");
        add_object(&mut store, "o_c2");
        // c2 contains o_c1
        add_membership(&mut store, "c2", "o_c1");
        // c1 contains o_c2
        add_membership(&mut store, "c1", "o_c2");
        // Now adding o_c1 to c1 would make c1 reachable from o_c1 (o_c1 → c2 → o_c2... wait)
        // Let's construct a direct cycle: c1 contains o1, o1 belongs to c1 already
        // Actually testing DAG: o_cycle → c1 (c1 contains o_cycle → cycle)
        add_object(&mut store, "o_cycle");
        add_membership(&mut store, "c1", "o_cycle");
        // Now try to add o_cycle to c1 (self-cycle through c1)
        // has_path_to_container from o_cycle to c1:
        // o_cycle is member of c1; c1 == target_container_id → true
        let result = enforce_add_member_policies(&store, "c1", "o_cycle");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cycle"));
    }

    #[test]
    fn set_policy_max_members() {
        let mut store = make_store();
        add_container(&mut store, "c1");
        set_container_policy(&mut store, "c1", "max_members", "5").unwrap();
        assert_eq!(store.containers()["c1"].policies.max_members, Some(5));
        set_container_policy(&mut store, "c1", "max_members", "none").unwrap();
        assert_eq!(store.containers()["c1"].policies.max_members, None);
    }

    #[test]
    fn set_policy_graph_shape() {
        let mut store = make_store();
        add_container(&mut store, "c1");
        set_container_policy(&mut store, "c1", "graph_shape", "tree").unwrap();
        assert_eq!(store.containers()["c1"].policies.graph_shape, GraphShape::Tree);
        set_container_policy(&mut store, "c1", "graph_shape", "dag").unwrap();
        assert_eq!(store.containers()["c1"].policies.graph_shape, GraphShape::Dag);
        set_container_policy(&mut store, "c1", "graph_shape", "any").unwrap();
        assert_eq!(store.containers()["c1"].policies.graph_shape, GraphShape::Any);
    }

    #[test]
    fn set_policy_unknown_field_errors() {
        let mut store = make_store();
        add_container(&mut store, "c1");
        let result = set_container_policy(&mut store, "c1", "nonexistent", "foo");
        assert!(result.is_err());
    }

    #[test]
    fn overflow_policy_reject_is_default_behavior() {
        // OverflowPolicy::Reject is the default; hitting max_members should still error.
        let mut store = make_store();
        add_container(&mut store, "c1");
        store.containers_mut().get_mut("c1").unwrap().policies.max_members = Some(1);
        // overflow_policy is Reject by default
        add_object(&mut store, "o1");
        add_object(&mut store, "o2");
        add_membership(&mut store, "c1", "o1");

        let result = enforce_add_member_policies(&store, "c1", "o2");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max_members"));
    }

    #[test]
    fn overflow_policy_evict_oldest_returns_head_linknode() {
        let mut store = make_store();
        add_container(&mut store, "c1");
        {
            let c = store.containers_mut().get_mut("c1").unwrap();
            c.policies.max_members = Some(2);
            c.policies.overflow_policy = OverflowPolicy::EvictOldest;
        }
        add_object(&mut store, "o1");
        add_object(&mut store, "o2");
        add_object(&mut store, "o3");
        add_membership(&mut store, "c1", "o1"); // head
        add_membership(&mut store, "c1", "o2"); // tail

        // Container is full; adding o3 should return EvictOldest with o1's link-node id.
        let head_link_id = store.iterate_forward("c1")[0].link_node_id.clone();
        let decision = enforce_add_member_policies(&store, "c1", "o3").unwrap();
        assert_eq!(decision, AddMemberDecision::EvictOldest(head_link_id));
    }

    #[test]
    fn ordered_by_recency_rejects_out_of_order() {
        use chrono::DateTime;
        let mut store = make_store();
        add_container(&mut store, "c1");
        store.containers_mut().get_mut("c1").unwrap().policies.ordered_by_recency = true;

        // Create o_new with a later timestamp then o_old with an earlier timestamp.
        let t_early = DateTime::parse_from_rfc3339("2026-01-01T00:00:00+00:00").unwrap();
        let t_late  = DateTime::parse_from_rfc3339("2026-06-01T00:00:00+00:00").unwrap();

        store.upsert_object("o_new", "test", None, None, Some(t_late)).unwrap();
        store.upsert_object("o_old", "test", None, None, Some(t_early)).unwrap();

        // Attach the later object first (valid).
        add_membership(&mut store, "c1", "o_new");

        // Attaching the older object after the newer one violates ordered_by_recency.
        let result = enforce_add_member_policies(&store, "c1", "o_old");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ordered_by_recency"));
    }

    #[test]
    fn ordered_by_recency_allows_same_or_later_timestamp() {
        use chrono::DateTime;
        let mut store = make_store();
        add_container(&mut store, "c1");
        store.containers_mut().get_mut("c1").unwrap().policies.ordered_by_recency = true;

        let t1 = DateTime::parse_from_rfc3339("2026-01-01T00:00:00+00:00").unwrap();
        let t2 = DateTime::parse_from_rfc3339("2026-06-01T00:00:00+00:00").unwrap();

        store.upsert_object("o_a", "test", None, None, Some(t1)).unwrap();
        store.upsert_object("o_b", "test", None, None, Some(t2)).unwrap();

        add_membership(&mut store, "c1", "o_a");

        // Attaching a later object is fine.
        let decision = enforce_add_member_policies(&store, "c1", "o_b").unwrap();
        assert_eq!(decision, AddMemberDecision::Add);
    }

    #[test]
    fn set_policy_overflow_policy() {
        let mut store = make_store();
        add_container(&mut store, "c1");
        set_container_policy(&mut store, "c1", "overflow_policy", "evict_oldest").unwrap();
        assert_eq!(store.containers()["c1"].policies.overflow_policy, OverflowPolicy::EvictOldest);
        set_container_policy(&mut store, "c1", "overflow_policy", "reject").unwrap();
        assert_eq!(store.containers()["c1"].policies.overflow_policy, OverflowPolicy::Reject);
    }

    #[test]
    fn set_policy_ordered_by_recency() {
        let mut store = make_store();
        add_container(&mut store, "c1");
        set_container_policy(&mut store, "c1", "ordered_by_recency", "true").unwrap();
        assert!(store.containers()["c1"].policies.ordered_by_recency);
        set_container_policy(&mut store, "c1", "ordered_by_recency", "false").unwrap();
        assert!(!store.containers()["c1"].policies.ordered_by_recency);
    }
}
