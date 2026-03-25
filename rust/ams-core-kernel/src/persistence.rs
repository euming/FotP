use anyhow::{anyhow, Result};

use crate::model::AmsSnapshot;
use crate::store::AmsStore;

pub fn serialize_snapshot(store: &AmsStore) -> Result<String> {
    let mut snapshot = AmsSnapshot {
        objects: store.objects().values().cloned().collect(),
        containers: store.containers().values().cloned().collect(),
        link_nodes: store.link_nodes().values().cloned().collect(),
    };

    snapshot.objects.sort_by(|left, right| left.object_id.cmp(&right.object_id));
    snapshot
        .containers
        .sort_by(|left, right| left.container_id.cmp(&right.container_id));
    snapshot
        .link_nodes
        .sort_by(|left, right| left.link_node_id.cmp(&right.link_node_id));

    Ok(serde_json::to_string_pretty(&snapshot)?)
}

pub fn deserialize_snapshot(json: &str) -> Result<AmsStore> {
    let json = json.strip_prefix('\u{feff}').unwrap_or(json);
    let snapshot: AmsSnapshot = serde_json::from_str(json)?;
    let mut store = AmsStore::new();

    for object in snapshot.objects {
        store.insert_object_record(object);
    }

    for container in snapshot.containers {
        if !store.objects().contains_key(&container.container_id) {
            return Err(anyhow!(
                "container '{}' is missing its corresponding object record",
                container.container_id
            ));
        }
        store.insert_container_record(container);
    }

    store.bulk_import_linknodes(snapshot.link_nodes);

    // Validate that container head/tail pointers reference existing linknodes
    for container in store.containers().values() {
        if let Some(ref head) = container.head_linknode_id {
            if !store.link_nodes().contains_key(head) {
                return Err(anyhow!(
                    "container '{}' head_linknode_id '{}' not found in link_nodes",
                    container.container_id, head
                ));
            }
        }
        if let Some(ref tail) = container.tail_linknode_id {
            if !store.link_nodes().contains_key(tail) {
                return Err(anyhow!(
                    "container '{}' tail_linknode_id '{}' not found in link_nodes",
                    container.container_id, tail
                ));
            }
        }
    }

    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::AmsStore;

    #[test]
    fn snapshot_roundtrip_restores_true_chain_order() {
        let mut store = AmsStore::new();
        store.upsert_object("obj:a", "thing", None, None, None).unwrap();
        store.upsert_object("obj:b", "thing", None, None, None).unwrap();
        store.create_container("ctr:ordered", "container", "smartlist").unwrap();
        store.add_object("ctr:ordered", "obj:a", None, Some("ln-b".to_string())).unwrap();
        store.add_object("ctr:ordered", "obj:b", None, Some("ln-a".to_string())).unwrap();

        let json = serialize_snapshot(&store).unwrap();
        let restored = deserialize_snapshot(&json).unwrap();
        let order = restored
            .iterate_forward("ctr:ordered")
            .iter()
            .map(|node| node.object_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(order, vec!["obj:a", "obj:b"]);
    }

    #[test]
    fn deserialize_snapshot_accepts_utf8_bom_prefix() {
        let json = concat!(
            "\u{feff}{",
            "\"objects\":[{\"objectId\":\"obj:a\",\"objectKind\":\"thing\",\"createdAt\":\"2026-03-13T00:00:00+00:00\",\"updatedAt\":\"2026-03-13T00:00:00+00:00\"}],",
            "\"containers\":[],",
            "\"linkNodes\":[]",
            "}"
        );

        let store = deserialize_snapshot(json).unwrap();
        assert!(store.objects().contains_key("obj:a"));
    }
}
