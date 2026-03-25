use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::{
    AbsoluteSemantics, AmsSnapshot, ContainerPolicies, ExpectationMetadata, HypothesisAnnotation, JsonMap,
    LinkNodeRecord, SemanticPayload,
};
use crate::store::AmsStore;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum MutationLogEntry {
    UpsertObject {
        object_id: String,
        object_kind: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        in_situ_ref: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        semantic_payload: Option<SemanticPayload>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_time: Option<DateTime<FixedOffset>>,
    },
    CreateContainer {
        container_id: String,
        object_kind: String,
        container_kind: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        object_created_at: Option<DateTime<FixedOffset>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        object_updated_at: Option<DateTime<FixedOffset>>,
        #[serde(default)]
        expectation_metadata: ExpectationMetadata,
        #[serde(default)]
        policies: ContainerPolicies,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        anchors: Option<Vec<String>>,
        #[serde(default)]
        absolute_semantics: AbsoluteSemantics,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<JsonMap>,
        #[serde(default)]
        hypothesis_state: std::collections::BTreeMap<String, HypothesisAnnotation>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        semantic_payload: Option<SemanticPayload>,
    },
    AddObject {
        container_id: String,
        object_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        rel_delta: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        link_node_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<JsonMap>,
    },
    InsertAfter {
        container_id: String,
        existing_link_node_id: String,
        object_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        rel_delta: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        new_link_node_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<JsonMap>,
    },
    RemoveLinkNode {
        container_id: String,
        link_node_id: String,
    },
    BeliefUpdate {
        container_id: String,
        confidence_delta: f64,
        prediction_error: f64,
        updated_attributes: std::collections::BTreeMap<String, f64>,
    },
}

impl MutationLogEntry {
    pub fn apply(&self, store: &mut AmsStore) -> Result<()> {
        match self {
            Self::UpsertObject {
                object_id,
                object_kind,
                in_situ_ref,
                semantic_payload,
                event_time,
            } => {
                store.upsert_object(
                    object_id.clone(),
                    object_kind.clone(),
                    in_situ_ref.clone(),
                    semantic_payload.clone(),
                    event_time.clone(),
                )?;
            }
            Self::CreateContainer {
                container_id,
                object_kind,
                container_kind,
                object_created_at,
                object_updated_at,
                expectation_metadata,
                policies,
                anchors,
                absolute_semantics,
                metadata,
                hypothesis_state,
                semantic_payload,
            } => {
                store.create_container(container_id.clone(), object_kind.clone(), container_kind.clone())?;
                if let Some(object) = store.objects_mut().get_mut(container_id) {
                    if let Some(created_at) = object_created_at {
                        object.created_at = *created_at;
                    }
                    if let Some(updated_at) = object_updated_at {
                        object.updated_at = *updated_at;
                    }
                }
                if let Some(container) = store.containers_mut().get_mut(container_id) {
                    container.expectation_metadata = expectation_metadata.clone();
                    container.policies = policies.clone();
                    container.anchors = anchors.clone();
                    container.absolute_semantics = absolute_semantics.clone();
                    container.metadata = metadata.clone();
                    container.hypothesis_state = hypothesis_state.clone();
                    container.semantic_payload = semantic_payload.clone();
                }
            }
            Self::AddObject {
                container_id,
                object_id,
                rel_delta,
                link_node_id,
                metadata,
            } => {
                let id = store.add_object(container_id, object_id, rel_delta.clone(), link_node_id.clone())?;
                if let Some(metadata) = metadata {
                    if let Some(node) = store.link_nodes_mut().get_mut(&id) {
                        node.metadata = Some(metadata.clone());
                    }
                }
            }
            Self::InsertAfter {
                container_id,
                existing_link_node_id,
                object_id,
                rel_delta,
                new_link_node_id,
                metadata,
            } => {
                let id = store.insert_after(
                    container_id,
                    existing_link_node_id,
                    object_id,
                    rel_delta.clone(),
                    new_link_node_id.clone(),
                )?;
                if let Some(metadata) = metadata {
                    if let Some(node) = store.link_nodes_mut().get_mut(&id) {
                        node.metadata = Some(metadata.clone());
                    }
                }
            }
            Self::RemoveLinkNode {
                container_id,
                link_node_id,
            } => {
                store.remove_linknode(container_id, link_node_id)?;
            }
            Self::BeliefUpdate {
                container_id,
                confidence_delta,
                prediction_error,
                updated_attributes,
            } => {
                if let Some(container) = store.containers_mut().get_mut(container_id) {
                    let delta = crate::active_inference::BeliefDelta {
                        container_id: container_id.clone(),
                        confidence_delta: *confidence_delta,
                        prediction_error: *prediction_error,
                        updated_attributes: updated_attributes.clone(),
                    };
                    crate::active_inference::apply_belief_delta(container, &delta);
                }
            }
        }
        Ok(())
    }
}

pub fn append_log_entry(path: impl AsRef<Path>, entry: &MutationLogEntry) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    serde_json::to_writer(&mut file, entry)?;
    file.write_all(b"\n")?;
    Ok(())
}

pub fn replay_log(path: impl AsRef<Path>) -> Result<AmsStore> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut store = AmsStore::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let entry: MutationLogEntry = serde_json::from_str(&line)?;
        entry.apply(&mut store)?;
    }

    Ok(store)
}

pub fn snapshot_to_log_entries(snapshot: &AmsSnapshot) -> Result<Vec<MutationLogEntry>> {
    let mut entries = Vec::new();

    for object in &snapshot.objects {
        entries.push(MutationLogEntry::UpsertObject {
            object_id: object.object_id.clone(),
            object_kind: object.object_kind.clone(),
            in_situ_ref: object.in_situ_ref.clone(),
            semantic_payload: object.semantic_payload.clone(),
            event_time: Some(object.created_at),
        });
    }

    for container in &snapshot.containers {
        let object_kind = snapshot
            .objects
            .iter()
            .find(|object| object.object_id == container.container_id)
            .map(|object| object.object_kind.clone())
            .unwrap_or_else(|| "container".to_string());

        entries.push(MutationLogEntry::CreateContainer {
            container_id: container.container_id.clone(),
            object_kind,
            container_kind: container.container_kind.clone(),
            object_created_at: snapshot
                .objects
                .iter()
                .find(|object| object.object_id == container.container_id)
                .map(|object| object.created_at),
            object_updated_at: snapshot
                .objects
                .iter()
                .find(|object| object.object_id == container.container_id)
                .map(|object| object.updated_at),
            expectation_metadata: container.expectation_metadata.clone(),
            policies: container.policies.clone(),
            anchors: container.anchors.clone(),
            absolute_semantics: container.absolute_semantics.clone(),
            metadata: container.metadata.clone(),
            hypothesis_state: container.hypothesis_state.clone(),
            semantic_payload: container.semantic_payload.clone(),
        });
    }

    let by_container = snapshot
        .link_nodes
        .iter()
        .cloned()
        .fold(std::collections::HashMap::<String, Vec<LinkNodeRecord>>::new(), |mut acc, node| {
            acc.entry(node.container_id.clone()).or_default().push(node);
            acc
        });

    for (_container_id, nodes) in by_container {
        let local_map = nodes
            .iter()
            .cloned()
            .map(|node| (node.link_node_id.clone(), node))
            .collect::<std::collections::HashMap<_, _>>();

        let Some(head) = nodes.iter().find(|node| node.prev_linknode_id.is_none()).cloned() else {
            continue;
        };

        let mut current = Some(head);
        let mut guard = local_map.len() + 1;
        while let Some(node) = current {
            if guard == 0 {
                anyhow::bail!(
                    "detected cyclic or corrupt snapshot while converting container '{}' into log entries",
                    node.container_id
                );
            }
            guard -= 1;

            entries.push(MutationLogEntry::AddObject {
                container_id: node.container_id.clone(),
                object_id: node.object_id.clone(),
                rel_delta: node.rel_delta.clone(),
                link_node_id: Some(node.link_node_id.clone()),
                metadata: node.metadata.clone(),
            });

            current = node
                .next_linknode_id
                .as_ref()
                .and_then(|next_id| local_map.get(next_id).cloned());
        }
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{deserialize_snapshot, serialize_snapshot};
    use tempfile::NamedTempFile;

    const CSHARP_SNAPSHOT_FIXTURE: &str = r#"{
  "objects": [
    {
      "objectId": "ctr:ordered",
      "objectKind": "container",
      "createdAt": "2026-03-13T00:00:00+00:00",
      "updatedAt": "2026-03-13T00:00:00+00:00"
    },
    {
      "objectId": "obj:a",
      "objectKind": "thing",
      "inSituRef": "fixture://a",
      "semanticPayload": {
        "tags": [
          "fixture",
          "alpha"
        ],
        "summary": "Fixture object A"
      },
      "createdAt": "2026-03-13T00:00:00+00:00",
      "updatedAt": "2026-03-13T00:00:00+00:00"
    },
    {
      "objectId": "obj:b",
      "objectKind": "thing",
      "createdAt": "2026-03-13T00:00:00+00:00",
      "updatedAt": "2026-03-13T00:00:00+00:00"
    }
  ],
  "containers": [
    {
      "containerId": "ctr:ordered",
      "containerKind": "smartlist",
      "expectationMetadata": {
        "interpretation": "ordered_frame"
      },
      "policies": {
        "uniqueMembers": true
      },
      "anchors": [
        "fixture-root"
      ],
      "absoluteSemantics": {
        "absoluteKind": "other"
      },
      "headLinknodeId": "ln-b",
      "tailLinknodeId": "ln-a",
      "metadata": {
        "owner": "fixture"
      }
    }
  ],
  "linkNodes": [
    {
      "linkNodeId": "ln-a",
      "containerId": "ctr:ordered",
      "objectId": "obj:b",
      "prevLinknodeId": "ln-b",
      "relDelta": 2,
      "metadata": {
        "position": "tail"
      }
    },
    {
      "linkNodeId": "ln-b",
      "containerId": "ctr:ordered",
      "objectId": "obj:a",
      "nextLinknodeId": "ln-a",
      "relDelta": 1
    }
  ]
}"#;

    #[test]
    fn replay_log_reaches_same_state() {
        let file = NamedTempFile::new().unwrap();
        append_log_entry(
            file.path(),
            &MutationLogEntry::UpsertObject {
                object_id: "obj:a".to_string(),
                object_kind: "thing".to_string(),
                in_situ_ref: None,
                semantic_payload: None,
                event_time: None,
            },
        )
        .unwrap();
        append_log_entry(
            file.path(),
            &MutationLogEntry::CreateContainer {
                container_id: "ctr:ordered".to_string(),
                object_kind: "container".to_string(),
                container_kind: "smartlist".to_string(),
                object_created_at: None,
                object_updated_at: None,
                expectation_metadata: ExpectationMetadata::default(),
                policies: ContainerPolicies::default(),
                anchors: None,
                absolute_semantics: AbsoluteSemantics::default(),
                metadata: None,
                hypothesis_state: Default::default(),
                semantic_payload: None,
            },
        )
        .unwrap();
        append_log_entry(
            file.path(),
            &MutationLogEntry::AddObject {
                container_id: "ctr:ordered".to_string(),
                object_id: "obj:a".to_string(),
                rel_delta: None,
                link_node_id: Some("ln-1".to_string()),
                metadata: None,
            },
        )
        .unwrap();

        let store = replay_log(file.path()).unwrap();
        assert!(store.objects().contains_key("obj:a"));
        assert!(store.containers().contains_key("ctr:ordered"));
        assert_eq!(store.iterate_forward("ctr:ordered").len(), 1);
    }

    #[test]
    fn snapshot_log_replay_matches_direct_import_for_fixture() {
        let snapshot: AmsSnapshot = serde_json::from_str(CSHARP_SNAPSHOT_FIXTURE).unwrap();
        let direct_store = deserialize_snapshot(CSHARP_SNAPSHOT_FIXTURE).unwrap();

        let file = NamedTempFile::new().unwrap();
        for entry in snapshot_to_log_entries(&snapshot).unwrap() {
            append_log_entry(file.path(), &entry).unwrap();
        }

        let replayed_store = replay_log(file.path()).unwrap();
        let direct_json = serialize_snapshot(&direct_store).unwrap();
        let replay_json = serialize_snapshot(&replayed_store).unwrap();
        assert_eq!(replay_json, direct_json);
    }
}
