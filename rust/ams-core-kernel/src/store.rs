use std::collections::{HashMap, HashSet};

use chrono::{DateTime, FixedOffset};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::model::{now_fixed, ContainerRecord, LinkNodeRecord, ObjectRecord, SemanticPayload};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("container '{0}' already exists")]
    ContainerExists(String),
    #[error("unknown container '{0}'")]
    UnknownContainer(String),
    #[error("unknown object '{0}'")]
    UnknownObject(String),
    #[error("link node '{0}' already exists")]
    LinkNodeExists(String),
    #[error("existing link node '{0}' not found in container '{1}'")]
    InvalidExistingLinkNode(String, String),
    #[error("container '{container_id}' enforces unique members; object '{object_id}' is already present")]
    UniqueMembershipViolation { container_id: String, object_id: String },
}

#[derive(Clone, Debug, Default)]
pub struct AmsStore {
    objects: HashMap<String, ObjectRecord>,
    containers: HashMap<String, ContainerRecord>,
    link_nodes: HashMap<String, LinkNodeRecord>,
    container_members: HashMap<String, HashSet<String>>,
    member_to_containers: HashMap<String, HashSet<String>>,
    member_to_link_nodes: HashMap<String, Vec<String>>,
}

impl AmsStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn objects(&self) -> &HashMap<String, ObjectRecord> {
        &self.objects
    }

    pub fn containers(&self) -> &HashMap<String, ContainerRecord> {
        &self.containers
    }

    pub fn link_nodes(&self) -> &HashMap<String, LinkNodeRecord> {
        &self.link_nodes
    }

    pub fn objects_mut(&mut self) -> &mut HashMap<String, ObjectRecord> {
        &mut self.objects
    }

    pub fn containers_mut(&mut self) -> &mut HashMap<String, ContainerRecord> {
        &mut self.containers
    }

    pub fn link_nodes_mut(&mut self) -> &mut HashMap<String, LinkNodeRecord> {
        &mut self.link_nodes
    }

    pub fn container_members_index(&self) -> &HashMap<String, HashSet<String>> {
        &self.container_members
    }

    pub fn member_to_containers_index(&self) -> &HashMap<String, HashSet<String>> {
        &self.member_to_containers
    }

    pub fn member_to_link_nodes_index(&self) -> &HashMap<String, Vec<String>> {
        &self.member_to_link_nodes
    }

    #[cfg(test)]
    pub(crate) fn member_to_containers_index_mut(&mut self) -> &mut HashMap<String, HashSet<String>> {
        &mut self.member_to_containers
    }

    #[cfg(test)]
    pub(crate) fn member_to_link_nodes_index_mut(&mut self) -> &mut HashMap<String, Vec<String>> {
        &mut self.member_to_link_nodes
    }

    pub fn upsert_object(
        &mut self,
        object_id: impl Into<String>,
        object_kind: impl Into<String>,
        in_situ_ref: Option<String>,
        semantic_payload: Option<SemanticPayload>,
        event_time: Option<DateTime<FixedOffset>>,
    ) -> Result<(), StoreError> {
        let object_id = object_id.into();
        let object_kind = object_kind.into();

        if let Some(existing) = self.objects.get_mut(&object_id) {
            existing.object_kind = object_kind;
            if in_situ_ref.is_some() {
                existing.in_situ_ref = in_situ_ref;
            }
            if semantic_payload.is_some() {
                existing.semantic_payload = semantic_payload;
            }
            existing.updated_at = now_fixed();
            return Ok(());
        }

        let record = ObjectRecord::new(object_id.clone(), object_kind, in_situ_ref, semantic_payload, event_time);
        self.objects.insert(object_id, record);
        Ok(())
    }

    pub fn create_container(
        &mut self,
        container_id: impl Into<String>,
        object_kind: impl Into<String>,
        container_kind: impl Into<String>,
    ) -> Result<(), StoreError> {
        let container_id = container_id.into();
        if self.containers.contains_key(&container_id) {
            return Err(StoreError::ContainerExists(container_id));
        }

        self.upsert_object(container_id.clone(), object_kind.into(), None, None, None)?;
        self.containers
            .insert(container_id.clone(), ContainerRecord::new(container_id.clone(), container_kind.into()));
        self.container_members.entry(container_id).or_default();
        Ok(())
    }

    pub fn add_object(
        &mut self,
        container_id: impl AsRef<str>,
        object_id: impl AsRef<str>,
        rel_delta: Option<Value>,
        link_node_id: Option<String>,
    ) -> Result<String, StoreError> {
        let container_id = container_id.as_ref();
        let object_id = object_id.as_ref();
        self.ensure_container_and_object(container_id, object_id)?;

        let unique_members = self
            .containers
            .get(container_id)
            .map(|container| container.policies.unique_members)
            .unwrap_or(false);
        if unique_members && self.has_membership(container_id, object_id) {
            return Err(StoreError::UniqueMembershipViolation {
                container_id: container_id.to_string(),
                object_id: object_id.to_string(),
            });
        }

        let tail_id = self
            .containers
            .get(container_id)
            .and_then(|container| container.tail_linknode_id.clone());
        let head_was_none = self
            .containers
            .get(container_id)
            .map(|container| container.head_linknode_id.is_none())
            .unwrap_or(false);

        let new_id = link_node_id.unwrap_or_else(generate_link_node_id);
        if self.link_nodes.contains_key(&new_id) {
            return Err(StoreError::LinkNodeExists(new_id));
        }

        let link = LinkNodeRecord::new(
            new_id.clone(),
            container_id.to_string(),
            object_id.to_string(),
            tail_id.clone(),
            None,
            rel_delta,
        );

        if let Some(tail_id) = tail_id {
            if let Some(tail) = self.link_nodes.get_mut(&tail_id) {
                tail.next_linknode_id = Some(new_id.clone());
            }
        }

        if let Some(container) = self.containers.get_mut(container_id) {
            if head_was_none {
                container.head_linknode_id = Some(new_id.clone());
            }
            container.tail_linknode_id = Some(new_id.clone());
        }

        self.link_nodes.insert(new_id.clone(), link.clone());
        self.index_link_node(&link);
        Ok(new_id)
    }

    pub fn insert_after(
        &mut self,
        container_id: impl AsRef<str>,
        existing_link_node_id: impl AsRef<str>,
        object_id: impl AsRef<str>,
        rel_delta: Option<Value>,
        new_link_node_id: Option<String>,
    ) -> Result<String, StoreError> {
        let container_id = container_id.as_ref();
        let existing_link_node_id = existing_link_node_id.as_ref();
        let object_id = object_id.as_ref();
        self.ensure_container_and_object(container_id, object_id)?;

        let existing = self
            .link_nodes
            .get(existing_link_node_id)
            .cloned()
            .filter(|node| node.container_id == container_id)
            .ok_or_else(|| {
                StoreError::InvalidExistingLinkNode(existing_link_node_id.to_string(), container_id.to_string())
            })?;

        let unique_members = self
            .containers
            .get(container_id)
            .map(|container| container.policies.unique_members)
            .unwrap_or(false);
        if unique_members && self.has_membership(container_id, object_id) {
            return Err(StoreError::UniqueMembershipViolation {
                container_id: container_id.to_string(),
                object_id: object_id.to_string(),
            });
        }

        let new_id = new_link_node_id.unwrap_or_else(generate_link_node_id);
        if self.link_nodes.contains_key(&new_id) {
            return Err(StoreError::LinkNodeExists(new_id));
        }

        let created = LinkNodeRecord::new(
            new_id.clone(),
            container_id.to_string(),
            object_id.to_string(),
            Some(existing_link_node_id.to_string()),
            existing.next_linknode_id.clone(),
            rel_delta,
        );

        if let Some(old_next) = existing.next_linknode_id.as_ref() {
            if let Some(next_node) = self.link_nodes.get_mut(old_next) {
                next_node.prev_linknode_id = Some(new_id.clone());
            }
        } else if let Some(container) = self.containers.get_mut(container_id) {
            container.tail_linknode_id = Some(new_id.clone());
        }

        if let Some(existing_node) = self.link_nodes.get_mut(existing_link_node_id) {
            existing_node.next_linknode_id = Some(new_id.clone());
        }

        self.link_nodes.insert(new_id.clone(), created.clone());
        self.index_link_node(&created);
        Ok(new_id)
    }

    pub fn insert_before(
        &mut self,
        container_id: impl AsRef<str>,
        existing_link_node_id: impl AsRef<str>,
        object_id: impl AsRef<str>,
        rel_delta: Option<Value>,
        new_link_node_id: Option<String>,
    ) -> Result<String, StoreError> {
        let container_id = container_id.as_ref();
        let existing_link_node_id = existing_link_node_id.as_ref();
        let object_id = object_id.as_ref();
        self.ensure_container_and_object(container_id, object_id)?;

        let existing = self
            .link_nodes
            .get(existing_link_node_id)
            .cloned()
            .filter(|node| node.container_id == container_id)
            .ok_or_else(|| {
                StoreError::InvalidExistingLinkNode(existing_link_node_id.to_string(), container_id.to_string())
            })?;

        let unique_members = self
            .containers
            .get(container_id)
            .map(|container| container.policies.unique_members)
            .unwrap_or(false);
        if unique_members && self.has_membership(container_id, object_id) {
            return Err(StoreError::UniqueMembershipViolation {
                container_id: container_id.to_string(),
                object_id: object_id.to_string(),
            });
        }

        let new_id = new_link_node_id.unwrap_or_else(generate_link_node_id);
        if self.link_nodes.contains_key(&new_id) {
            return Err(StoreError::LinkNodeExists(new_id));
        }

        let created = LinkNodeRecord::new(
            new_id.clone(),
            container_id.to_string(),
            object_id.to_string(),
            existing.prev_linknode_id.clone(),
            Some(existing_link_node_id.to_string()),
            rel_delta,
        );

        if let Some(old_prev) = existing.prev_linknode_id.as_ref() {
            if let Some(prev_node) = self.link_nodes.get_mut(old_prev) {
                prev_node.next_linknode_id = Some(new_id.clone());
            }
        } else if let Some(container) = self.containers.get_mut(container_id) {
            container.head_linknode_id = Some(new_id.clone());
        }

        if let Some(existing_node) = self.link_nodes.get_mut(existing_link_node_id) {
            existing_node.prev_linknode_id = Some(new_id.clone());
        }

        self.link_nodes.insert(new_id.clone(), created.clone());
        self.index_link_node(&created);
        Ok(new_id)
    }

    pub fn remove_linknode(
        &mut self,
        container_id: impl AsRef<str>,
        link_node_id: impl AsRef<str>,
    ) -> Result<bool, StoreError> {
        let container_id = container_id.as_ref();
        let link_node_id = link_node_id.as_ref();

        let Some(node) = self.link_nodes.get(link_node_id).cloned() else {
            return Ok(false);
        };
        if node.container_id != container_id {
            return Ok(false);
        }

        if let Some(prev) = node.prev_linknode_id.as_ref() {
            if let Some(prev_node) = self.link_nodes.get_mut(prev) {
                prev_node.next_linknode_id = node.next_linknode_id.clone();
            }
        } else if let Some(container) = self.containers.get_mut(container_id) {
            container.head_linknode_id = node.next_linknode_id.clone();
        }

        if let Some(next) = node.next_linknode_id.as_ref() {
            if let Some(next_node) = self.link_nodes.get_mut(next) {
                next_node.prev_linknode_id = node.prev_linknode_id.clone();
            }
        } else if let Some(container) = self.containers.get_mut(container_id) {
            container.tail_linknode_id = node.prev_linknode_id.clone();
        }

        self.link_nodes.remove(link_node_id);
        self.unindex_link_node(&node);
        Ok(true)
    }

    pub fn has_membership(&self, container_id: impl AsRef<str>, object_id: impl AsRef<str>) -> bool {
        self.container_members
            .get(container_id.as_ref())
            .map(|members| members.contains(object_id.as_ref()))
            .unwrap_or(false)
    }

    pub fn iterate_forward(&self, container_id: impl AsRef<str>) -> Vec<&LinkNodeRecord> {
        let container_id = container_id.as_ref();
        let Some(container) = self.containers.get(container_id) else {
            return Vec::new();
        };

        let mut out = Vec::new();
        let mut cursor = container.head_linknode_id.clone();
        let mut seen = HashSet::new();
        while let Some(id) = cursor {
            if !seen.insert(id.clone()) {
                break;
            }
            let Some(current) = self.link_nodes.get(&id) else {
                break;
            };
            out.push(current);
            cursor = current.next_linknode_id.clone();
        }
        out
    }

    pub fn iterate_backward(&self, container_id: impl AsRef<str>) -> Vec<&LinkNodeRecord> {
        let container_id = container_id.as_ref();
        let Some(container) = self.containers.get(container_id) else {
            return Vec::new();
        };

        let mut out = Vec::new();
        let mut cursor = container.tail_linknode_id.clone();
        let mut seen = HashSet::new();
        while let Some(id) = cursor {
            if !seen.insert(id.clone()) {
                break;
            }
            let Some(current) = self.link_nodes.get(&id) else {
                break;
            };
            out.push(current);
            cursor = current.prev_linknode_id.clone();
        }
        out
    }

    pub fn containers_for_member_object(&self, object_id: impl AsRef<str>) -> Vec<String> {
        let object_id = object_id.as_ref();
        let Some(container_ids) = self.member_to_containers.get(object_id) else {
            return Vec::new();
        };

        let mut ordered = self
            .link_nodes
            .values()
            .filter(|node| node.object_id.as_str() == object_id && container_ids.contains(&node.container_id))
            .map(|node| node.container_id.clone())
            .collect::<Vec<_>>();
        ordered.sort();
        ordered.dedup();
        ordered
    }

    pub fn links_for_member_object(&self, object_id: impl AsRef<str>) -> Vec<&LinkNodeRecord> {
        let object_id = object_id.as_ref();
        let Some(link_ids) = self.member_to_link_nodes.get(object_id) else {
            return Vec::new();
        };

        link_ids
            .iter()
            .filter_map(|id| self.link_nodes.get(id))
            .collect()
    }

    pub fn rebuild_membership_indexes_from_snapshot(&mut self) {
        self.container_members.clear();
        self.member_to_containers.clear();
        self.member_to_link_nodes.clear();

        for container_id in self.containers.keys() {
            self.container_members.insert(container_id.clone(), HashSet::new());
        }

        let nodes = self.link_nodes.values().cloned().collect::<Vec<_>>();
        for node in &nodes {
            self.index_link_node(node);
        }
    }

    pub(crate) fn insert_object_record(&mut self, record: ObjectRecord) {
        self.objects.insert(record.object_id.clone(), record);
    }

    pub(crate) fn insert_container_record(&mut self, record: ContainerRecord) {
        self.container_members.entry(record.container_id.clone()).or_default();
        self.containers.insert(record.container_id.clone(), record);
    }

    pub(crate) fn bulk_import_linknodes(&mut self, nodes: Vec<LinkNodeRecord>) {
        self.link_nodes.reserve(nodes.len());
        for node in nodes {
            self.container_members
                .entry(node.container_id.clone())
                .or_default()
                .insert(node.object_id.clone());
            self.member_to_containers
                .entry(node.object_id.clone())
                .or_default()
                .insert(node.container_id.clone());
            self.member_to_link_nodes
                .entry(node.object_id.clone())
                .or_default()
                .push(node.link_node_id.clone());
            self.link_nodes.insert(node.link_node_id.clone(), node);
        }
    }

    fn ensure_container_and_object(&self, container_id: &str, object_id: &str) -> Result<(), StoreError> {
        if !self.containers.contains_key(container_id) {
            return Err(StoreError::UnknownContainer(container_id.to_string()));
        }
        if !self.objects.contains_key(object_id) {
            return Err(StoreError::UnknownObject(object_id.to_string()));
        }
        Ok(())
    }

    fn index_link_node(&mut self, link_node: &LinkNodeRecord) {
        self.container_members
            .entry(link_node.container_id.clone())
            .or_default()
            .insert(link_node.object_id.clone());

        self.member_to_containers
            .entry(link_node.object_id.clone())
            .or_default()
            .insert(link_node.container_id.clone());

        self.member_to_link_nodes
            .entry(link_node.object_id.clone())
            .or_default()
            .push(link_node.link_node_id.clone());
    }

    fn unindex_link_node(&mut self, link_node: &LinkNodeRecord) {
        if let Some(links) = self.member_to_link_nodes.get_mut(&link_node.object_id) {
            links.retain(|id| id != &link_node.link_node_id);
            if links.is_empty() {
                self.member_to_link_nodes.remove(&link_node.object_id);
            }
        }

        let still_in_container = self
            .iterate_forward(&link_node.container_id)
            .iter()
            .any(|node| node.object_id.as_str() == link_node.object_id.as_str());

        if !still_in_container {
            if let Some(members) = self.container_members.get_mut(&link_node.container_id) {
                members.remove(&link_node.object_id);
            }
            if let Some(containers) = self.member_to_containers.get_mut(&link_node.object_id) {
                containers.remove(&link_node.container_id);
                if containers.is_empty() {
                    self.member_to_containers.remove(&link_node.object_id);
                }
            }
        }
    }
}

fn generate_link_node_id() -> String {
    format!("ln_{}", Uuid::new_v4().simple())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store() -> AmsStore {
        let mut store = AmsStore::new();
        store.upsert_object("obj:a", "thing", None, None, None).unwrap();
        store.upsert_object("obj:b", "thing", None, None, None).unwrap();
        store.upsert_object("obj:c", "thing", None, None, None).unwrap();
        store.create_container("ctr:ordered", "container", "smartlist").unwrap();
        store
    }

    #[test]
    fn add_insert_remove_preserves_link_order() {
        let mut store = make_store();
        store.add_object("ctr:ordered", "obj:a", None, Some("ln-1".to_string())).unwrap();
        store.add_object("ctr:ordered", "obj:c", None, Some("ln-3".to_string())).unwrap();
        store
            .insert_after("ctr:ordered", "ln-1", "obj:b", None, Some("ln-2".to_string()))
            .unwrap();

        let forward = store
            .iterate_forward("ctr:ordered")
            .iter()
            .map(|node| node.object_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(forward, vec!["obj:a", "obj:b", "obj:c"]);

        let removed = store.remove_linknode("ctr:ordered", "ln-2").unwrap();
        assert!(removed);

        let forward = store
            .iterate_forward("ctr:ordered")
            .iter()
            .map(|node| node.object_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(forward, vec!["obj:a", "obj:c"]);
    }

    #[test]
    fn insert_before_updates_head_and_order() {
        let mut store = make_store();
        store.add_object("ctr:ordered", "obj:b", None, Some("ln-2".to_string())).unwrap();
        store.add_object("ctr:ordered", "obj:c", None, Some("ln-3".to_string())).unwrap();
        store
            .insert_before("ctr:ordered", "ln-2", "obj:a", None, Some("ln-1".to_string()))
            .unwrap();

        let forward = store
            .iterate_forward("ctr:ordered")
            .iter()
            .map(|node| node.object_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(forward, vec!["obj:a", "obj:b", "obj:c"]);
        assert_eq!(
            store
                .containers()
                .get("ctr:ordered")
                .and_then(|container| container.head_linknode_id.as_deref()),
            Some("ln-1")
        );
    }

    #[test]
    fn unique_membership_is_enforced() {
        let mut store = make_store();
        store.containers_mut().get_mut("ctr:ordered").unwrap().policies.unique_members = true;
        store.add_object("ctr:ordered", "obj:a", None, Some("ln-1".to_string())).unwrap();
        let err = store.add_object("ctr:ordered", "obj:a", None, Some("ln-2".to_string())).unwrap_err();
        assert!(matches!(err, StoreError::UniqueMembershipViolation { .. }));
    }
}
