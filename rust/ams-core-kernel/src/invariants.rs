use std::collections::HashSet;

use crate::store::AmsStore;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvariantViolation {
    pub code: &'static str,
    pub message: String,
}

pub fn validate_invariants(store: &AmsStore) -> Vec<InvariantViolation> {
    let mut violations = Vec::new();

    for container in store.containers().values() {
        if !store.objects().contains_key(&container.container_id) {
            violations.push(InvariantViolation {
                code: "container_object_missing",
                message: format!("container '{}' is missing its object record", container.container_id),
            });
        }

        if let Some(head_id) = container.head_linknode_id.as_ref() {
            match store.link_nodes().get(head_id) {
                Some(head) if head.prev_linknode_id.is_none() => {}
                Some(_) => violations.push(InvariantViolation {
                    code: "head_prev_non_null",
                    message: format!("container '{}' head '{}' has a predecessor", container.container_id, head_id),
                }),
                None => violations.push(InvariantViolation {
                    code: "head_missing",
                    message: format!("container '{}' head '{}' does not exist", container.container_id, head_id),
                }),
            }
        }

        if let Some(tail_id) = container.tail_linknode_id.as_ref() {
            match store.link_nodes().get(tail_id) {
                Some(tail) if tail.next_linknode_id.is_none() => {}
                Some(_) => violations.push(InvariantViolation {
                    code: "tail_next_non_null",
                    message: format!("container '{}' tail '{}' has a successor", container.container_id, tail_id),
                }),
                None => violations.push(InvariantViolation {
                    code: "tail_missing",
                    message: format!("container '{}' tail '{}' does not exist", container.container_id, tail_id),
                }),
            }
        }

        let forward = store.iterate_forward(&container.container_id);
        let backward = store.iterate_backward(&container.container_id);
        let forward_ids = forward.iter().map(|node| node.link_node_id.clone()).collect::<Vec<_>>();
        let backward_ids = backward.iter().rev().map(|node| node.link_node_id.clone()).collect::<Vec<_>>();
        if forward_ids != backward_ids {
            violations.push(InvariantViolation {
                code: "forward_backward_mismatch",
                message: format!("container '{}' forward and backward traversals disagree", container.container_id),
            });
        }

        if let Some(indexed_members) = store.container_members_index().get(&container.container_id) {
            let traversed_members = forward.iter().map(|node| node.object_id.clone()).collect::<HashSet<_>>();
            if traversed_members != *indexed_members {
                violations.push(InvariantViolation {
                    code: "member_index_mismatch",
                    message: format!("container '{}' membership index disagrees with traversal", container.container_id),
                });
            }
        }
    }

    for node in store.link_nodes().values() {
        if !store.containers().contains_key(&node.container_id) {
            violations.push(InvariantViolation {
                code: "link_container_missing",
                message: format!("link node '{}' references unknown container '{}'", node.link_node_id, node.container_id),
            });
        }
        if !store.objects().contains_key(&node.object_id) {
            violations.push(InvariantViolation {
                code: "link_object_missing",
                message: format!("link node '{}' references unknown object '{}'", node.link_node_id, node.object_id),
            });
        }

        if let Some(prev) = node.prev_linknode_id.as_ref() {
            match store.link_nodes().get(prev) {
                Some(prev_node) if prev_node.next_linknode_id.as_deref() == Some(node.link_node_id.as_str()) => {}
                Some(_) => violations.push(InvariantViolation {
                    code: "prev_next_mismatch",
                    message: format!("link node '{}' prev '{}' does not point back", node.link_node_id, prev),
                }),
                None => violations.push(InvariantViolation {
                    code: "prev_missing",
                    message: format!("link node '{}' prev '{}' does not exist", node.link_node_id, prev),
                }),
            }
        }

        if let Some(next) = node.next_linknode_id.as_ref() {
            match store.link_nodes().get(next) {
                Some(next_node) if next_node.prev_linknode_id.as_deref() == Some(node.link_node_id.as_str()) => {}
                Some(_) => violations.push(InvariantViolation {
                    code: "next_prev_mismatch",
                    message: format!("link node '{}' next '{}' does not point back", node.link_node_id, next),
                }),
                None => violations.push(InvariantViolation {
                    code: "next_missing",
                    message: format!("link node '{}' next '{}' does not exist", node.link_node_id, next),
                }),
            }
        }
    }

    for (object_id, containers) in store.member_to_containers_index() {
        for container_id in containers {
            if !store.has_membership(container_id, object_id) {
                violations.push(InvariantViolation {
                    code: "member_container_index_mismatch",
                    message: format!(
                        "member-to-container index says object '{}' is in '{}', but container index disagrees",
                        object_id, container_id
                    ),
                });
            }
        }
    }

    for (object_id, link_ids) in store.member_to_link_nodes_index() {
        for link_id in link_ids {
            match store.link_nodes().get(link_id) {
                Some(node) if node.object_id.as_str() == object_id.as_str() => {}
                Some(node) => violations.push(InvariantViolation {
                    code: "member_link_index_mismatch",
                    message: format!(
                        "member-to-link index maps object '{}' to link '{}', but link points to '{}'",
                        object_id, link_id, node.object_id
                    ),
                }),
                None => violations.push(InvariantViolation {
                    code: "member_link_missing",
                    message: format!(
                        "member-to-link index maps object '{}' to missing link '{}'",
                        object_id, link_id
                    ),
                }),
            }
        }
    }

    violations
}

#[cfg(test)]
mod tests {
    use crate::model::{ContainerRecord, LinkNodeRecord, ObjectRecord};

    use super::*;

    fn baseline_store() -> AmsStore {
        let mut store = AmsStore::new();
        store
            .insert_object_record(ObjectRecord::new(
                "ctr:ordered".to_string(),
                "container".to_string(),
                None,
                None,
                None,
            ));
        store.insert_object_record(ObjectRecord::new(
            "obj:a".to_string(),
            "thing".to_string(),
            None,
            None,
            None,
        ));
        store.insert_object_record(ObjectRecord::new(
            "obj:b".to_string(),
            "thing".to_string(),
            None,
            None,
            None,
        ));
        let mut container = ContainerRecord::new("ctr:ordered".to_string(), "smartlist".to_string());
        container.head_linknode_id = Some("ln-1".to_string());
        container.tail_linknode_id = Some("ln-2".to_string());
        store.containers_mut().insert("ctr:ordered".to_string(), container);
        store.link_nodes_mut().insert(
            "ln-1".to_string(),
            LinkNodeRecord::new(
                "ln-1".to_string(),
                "ctr:ordered".to_string(),
                "obj:a".to_string(),
                None,
                Some("ln-2".to_string()),
                None,
            ),
        );
        store.link_nodes_mut().insert(
            "ln-2".to_string(),
            LinkNodeRecord::new(
                "ln-2".to_string(),
                "ctr:ordered".to_string(),
                "obj:b".to_string(),
                Some("ln-1".to_string()),
                None,
                None,
            ),
        );
        store.rebuild_membership_indexes_from_snapshot();
        store
    }

    #[test]
    fn detects_missing_head_and_prev_next_corruption() {
        let mut store = baseline_store();
        store.containers_mut().get_mut("ctr:ordered").unwrap().head_linknode_id = Some("missing".to_string());
        store.link_nodes_mut().get_mut("ln-1").unwrap().next_linknode_id = None;

        let violations = validate_invariants(&store);
        let codes = violations.iter().map(|v| v.code).collect::<HashSet<_>>();
        assert!(codes.contains("head_missing"));
        assert!(codes.contains("forward_backward_mismatch"));
    }

    #[test]
    fn detects_member_index_and_member_link_index_mismatch() {
        let mut store = baseline_store();
        store
            .member_to_containers_index_mut()
            .entry("obj:a".to_string())
            .or_default()
            .insert("ctr:ghost".to_string());
        store
            .member_to_link_nodes_index_mut()
            .entry("obj:b".to_string())
            .or_default()
            .push("ln-missing".to_string());

        let violations = validate_invariants(&store);
        let codes = violations.iter().map(|v| v.code).collect::<HashSet<_>>();
        assert!(codes.contains("member_container_index_mismatch"));
        assert!(codes.contains("member_link_missing"));
    }

}
