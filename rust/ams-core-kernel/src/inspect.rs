use std::collections::BTreeSet;

use anyhow::{anyhow, Result};

use crate::persistence::serialize_snapshot;
use crate::store::AmsStore;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraversalDirection {
    Forward,
    Backward,
    Both,
}

pub fn list_objects(store: &AmsStore, kind: Option<&str>) -> String {
    let mut objects = store.objects().values().collect::<Vec<_>>();
    objects.sort_by(|left, right| left.object_id.cmp(&right.object_id));

    let filtered = objects
        .into_iter()
        .filter(|object| kind.is_none_or(|expected| object.object_kind == expected))
        .collect::<Vec<_>>();

    let mut out = String::new();
    out.push_str(&format!("objects={}\n", filtered.len()));
    for object in filtered {
        out.push_str(&format!("object={} kind={}\n", object.object_id, object.object_kind));
    }
    out
}

pub fn show_object(store: &AmsStore, object_id: &str) -> Result<String> {
    let object = store
        .objects()
        .get(object_id)
        .ok_or_else(|| anyhow!("object '{}' not found", object_id))?;

    let containers = store.containers_for_member_object(object_id);
    let links = store
        .links_for_member_object(object_id)
        .into_iter()
        .map(|node| node.link_node_id.clone())
        .collect::<Vec<_>>();

    let mut out = String::new();
    out.push_str(&format!("object={}\n", object.object_id));
    out.push_str(&format!("kind={}\n", object.object_kind));
    out.push_str(&format!("created_at={}\n", object.created_at.to_rfc3339()));
    out.push_str(&format!("updated_at={}\n", object.updated_at.to_rfc3339()));
    if let Some(in_situ_ref) = object.in_situ_ref.as_ref() {
        out.push_str(&format!("in_situ_ref={}\n", in_situ_ref));
    }
    if let Some(payload) = object.semantic_payload.as_ref() {
        out.push_str(&format!(
            "semantic_payload={}\n",
            serde_json::to_string_pretty(payload)?
        ));
    }
    out.push_str(&format!(
        "containers={}\n",
        if containers.is_empty() {
            "<none>".to_string()
        } else {
            containers.join(", ")
        }
    ));
    out.push_str(&format!(
        "links={}\n",
        if links.is_empty() {
            "<none>".to_string()
        } else {
            links.join(", ")
        }
    ));
    Ok(out)
}

pub fn list_containers(store: &AmsStore, kind: Option<&str>) -> String {
    let mut containers = store.containers().values().collect::<Vec<_>>();
    containers.sort_by(|left, right| left.container_id.cmp(&right.container_id));

    let filtered = containers
        .into_iter()
        .filter(|container| kind.is_none_or(|expected| container.container_kind == expected))
        .collect::<Vec<_>>();

    let mut out = String::new();
    out.push_str(&format!("containers={}\n", filtered.len()));
    for container in filtered {
        out.push_str(&format!(
            "container={} kind={} head={} tail={}\n",
            container.container_id,
            container.container_kind,
            container.head_linknode_id.as_deref().unwrap_or("<none>"),
            container.tail_linknode_id.as_deref().unwrap_or("<none>")
        ));
    }
    out
}

pub fn list_link_nodes(store: &AmsStore, container_id: Option<&str>, object_id: Option<&str>) -> String {
    let mut nodes = store.link_nodes().values().collect::<Vec<_>>();
    nodes.sort_by(|left, right| left.link_node_id.cmp(&right.link_node_id));

    let filtered = nodes
        .into_iter()
        .filter(|node| container_id.is_none_or(|expected| node.container_id == expected))
        .filter(|node| object_id.is_none_or(|expected| node.object_id == expected))
        .collect::<Vec<_>>();

    let mut out = String::new();
    out.push_str(&format!("link_nodes={}\n", filtered.len()));
    for node in filtered {
        out.push_str(&format!(
            "link={} container={} object={} prev={} next={}\n",
            node.link_node_id,
            node.container_id,
            node.object_id,
            node.prev_linknode_id.as_deref().unwrap_or("<none>"),
            node.next_linknode_id.as_deref().unwrap_or("<none>")
        ));
    }
    out
}

pub fn show_container(store: &AmsStore, container_id: &str, direction: TraversalDirection) -> Result<String> {
    let container = store
        .containers()
        .get(container_id)
        .ok_or_else(|| anyhow!("container '{}' not found", container_id))?;

    let mut out = String::new();
    out.push_str(&format!("container={}\n", container.container_id));
    out.push_str(&format!("kind={}\n", container.container_kind));
    out.push_str(&format!(
        "head={}\n",
        container.head_linknode_id.as_deref().unwrap_or("<none>")
    ));
    out.push_str(&format!(
        "tail={}\n",
        container.tail_linknode_id.as_deref().unwrap_or("<none>")
    ));
    out.push_str(&format!(
        "unique_members={}\n",
        container.policies.unique_members
    ));
    if let Some(anchors) = container.anchors.as_ref() {
        out.push_str(&format!("anchors={}\n", anchors.join(", ")));
    }
    if let Some(metadata) = container.metadata.as_ref() {
        out.push_str(&format!("metadata={}\n", serde_json::to_string_pretty(metadata)?));
    }

    if matches!(direction, TraversalDirection::Forward | TraversalDirection::Both) {
        out.push_str("forward:\n");
        for node in store.iterate_forward(container_id) {
            out.push_str(&format!(
                "  {} obj={} prev={} next={}\n",
                node.link_node_id,
                node.object_id,
                node.prev_linknode_id.as_deref().unwrap_or("<none>"),
                node.next_linknode_id.as_deref().unwrap_or("<none>")
            ));
        }
    }

    if matches!(direction, TraversalDirection::Backward | TraversalDirection::Both) {
        out.push_str("backward:\n");
        for node in store.iterate_backward(container_id) {
            out.push_str(&format!(
                "  {} obj={} prev={} next={}\n",
                node.link_node_id,
                node.object_id,
                node.prev_linknode_id.as_deref().unwrap_or("<none>"),
                node.next_linknode_id.as_deref().unwrap_or("<none>")
            ));
        }
    }

    Ok(out)
}

pub fn show_link_node(store: &AmsStore, link_node_id: &str) -> Result<String> {
    let node = store
        .link_nodes()
        .get(link_node_id)
        .ok_or_else(|| anyhow!("link node '{}' not found", link_node_id))?;

    let mut out = String::new();
    out.push_str(&format!("link_node={}\n", node.link_node_id));
    out.push_str(&format!("container={}\n", node.container_id));
    out.push_str(&format!("object={}\n", node.object_id));
    out.push_str(&format!(
        "prev={}\n",
        node.prev_linknode_id.as_deref().unwrap_or("<none>")
    ));
    out.push_str(&format!(
        "next={}\n",
        node.next_linknode_id.as_deref().unwrap_or("<none>")
    ));
    if let Some(rel_delta) = node.rel_delta.as_ref() {
        out.push_str(&format!("rel_delta={}\n", serde_json::to_string_pretty(rel_delta)?));
    }
    if let Some(metadata) = node.metadata.as_ref() {
        out.push_str(&format!("metadata={}\n", serde_json::to_string_pretty(metadata)?));
    }
    Ok(out)
}

pub fn show_memberships(store: &AmsStore, object_id: &str) -> Result<String> {
    let object = store
        .objects()
        .get(object_id)
        .ok_or_else(|| anyhow!("object '{}' not found", object_id))?;

    let containers = store.containers_for_member_object(object_id);
    let links = store.links_for_member_object(object_id);

    let mut out = String::new();
    out.push_str(&format!("object={}\n", object.object_id));
    out.push_str(&format!("kind={}\n", object.object_kind));
    out.push_str(&format!("container_count={}\n", containers.len()));
    for container_id in containers {
        out.push_str(&format!("container={}\n", container_id));
    }
    out.push_str(&format!("link_count={}\n", links.len()));
    for node in links {
        out.push_str(&format!(
            "link={} container={} prev={} next={}\n",
            node.link_node_id,
            node.container_id,
            node.prev_linknode_id.as_deref().unwrap_or("<none>"),
            node.next_linknode_id.as_deref().unwrap_or("<none>")
        ));
    }
    Ok(out)
}

pub fn diff_snapshots(left: &AmsStore, right: &AmsStore) -> Result<String> {
    let mut out = String::new();
    let left_json = serialize_snapshot(left)?;
    let right_json = serialize_snapshot(right)?;

    out.push_str(&format!("left_objects={}\n", left.objects().len()));
    out.push_str(&format!("right_objects={}\n", right.objects().len()));
    out.push_str(&format!("left_containers={}\n", left.containers().len()));
    out.push_str(&format!("right_containers={}\n", right.containers().len()));
    out.push_str(&format!("left_link_nodes={}\n", left.link_nodes().len()));
    out.push_str(&format!("right_link_nodes={}\n", right.link_nodes().len()));

    append_set_diff(
        &mut out,
        "object_only_left",
        left.objects().keys().cloned().collect(),
        right.objects().keys().cloned().collect(),
    );
    append_set_diff(
        &mut out,
        "container_only_left",
        left.containers().keys().cloned().collect(),
        right.containers().keys().cloned().collect(),
    );
    append_set_diff(
        &mut out,
        "link_only_left",
        left.link_nodes().keys().cloned().collect(),
        right.link_nodes().keys().cloned().collect(),
    );

    let mut changed_containers = BTreeSet::new();
    for container_id in left.containers().keys() {
        if let (Some(left_container), Some(right_container)) = (
            left.containers().get(container_id),
            right.containers().get(container_id),
        ) {
            let left_forward = left
                .iterate_forward(container_id)
                .iter()
                .map(|node| node.object_id.clone())
                .collect::<Vec<_>>();
            let right_forward = right
                .iterate_forward(container_id)
                .iter()
                .map(|node| node.object_id.clone())
                .collect::<Vec<_>>();
            if left_container != right_container || left_forward != right_forward {
                changed_containers.insert(container_id.clone());
            }
        }
    }

    out.push_str(&format!(
        "changed_containers={}\n",
        if changed_containers.is_empty() {
            "<none>".to_string()
        } else {
            changed_containers.into_iter().collect::<Vec<_>>().join(", ")
        }
    ));

    out.push_str(&format!("canonical_equal={}\n", left_json == right_json));
    Ok(out)
}

fn append_set_diff(
    out: &mut String,
    label: &str,
    left: BTreeSet<String>,
    right: BTreeSet<String>,
) {
    let left_only = left.difference(&right).cloned().collect::<Vec<_>>();
    let right_only = right.difference(&left).cloned().collect::<Vec<_>>();
    out.push_str(&format!(
        "{label}={}\n",
        if left_only.is_empty() {
            "<none>".to_string()
        } else {
            left_only.join(", ")
        }
    ));
    out.push_str(&format!(
        "{}={}\n",
        label.replacen("_left", "_right", 1),
        if right_only.is_empty() {
            "<none>".to_string()
        } else {
            right_only.join(", ")
        }
    ));
}

#[cfg(test)]
mod tests {
    use crate::store::AmsStore;

    use super::*;
    use serde_json::Value;

    fn make_store() -> AmsStore {
        let mut store = AmsStore::new();
        store
            .upsert_object("obj:a", "thing", Some("fixture://a".to_string()), None, None)
            .unwrap();
        store.upsert_object("obj:b", "thing", None, None, None).unwrap();
        store.create_container("ctr:ordered", "container", "smartlist").unwrap();
        store
            .add_object(
                "ctr:ordered",
                "obj:a",
                Some(Value::from(1)),
                Some("ln-1".to_string()),
            )
            .unwrap();
        store
            .add_object(
                "ctr:ordered",
                "obj:b",
                Some(Value::from(2)),
                Some("ln-2".to_string()),
            )
            .unwrap();
        store
    }

    #[test]
    fn list_objects_is_sorted() {
        let output = list_objects(&make_store(), None);
        assert!(output.contains("objects=3"));
        let lines = output.lines().collect::<Vec<_>>();
        assert_eq!(lines[1], "object=ctr:ordered kind=container");
        assert_eq!(lines[2], "object=obj:a kind=thing");
        assert_eq!(lines[3], "object=obj:b kind=thing");
    }

    #[test]
    fn show_container_includes_forward_and_backward_members() {
        let output = show_container(&make_store(), "ctr:ordered", TraversalDirection::Both).unwrap();
        assert!(output.contains("forward:"));
        assert!(output.contains("backward:"));
        assert!(output.contains("ln-1 obj=obj:a"));
        assert!(output.contains("ln-2 obj=obj:b"));
    }

    #[test]
    fn list_link_nodes_can_filter_by_container() {
        let output = list_link_nodes(&make_store(), Some("ctr:ordered"), None);
        assert!(output.contains("link_nodes=2"));
        assert!(output.contains("link=ln-1 container=ctr:ordered object=obj:a"));
        assert!(output.contains("link=ln-2 container=ctr:ordered object=obj:b"));
    }

    #[test]
    fn snapshot_diff_reports_structural_changes() {
        let left = make_store();
        let mut right = make_store();
        right.upsert_object("obj:c", "thing", None, None, None).unwrap();

        let output = diff_snapshots(&left, &right).unwrap();
        assert!(output.contains("object_only_right=obj:c"));
        assert!(output.contains("canonical_equal=false"));
    }
}
