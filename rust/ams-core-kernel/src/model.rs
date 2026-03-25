use std::collections::BTreeMap;

use chrono::{DateTime, FixedOffset, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type JsonMap = BTreeMap<String, Value>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct SemanticPayload {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<Vec<f32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<JsonMap>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectRecord {
    pub object_id: String,
    pub object_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_situ_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_payload: Option<SemanticPayload>,
    pub created_at: DateTime<FixedOffset>,
    pub updated_at: DateTime<FixedOffset>,
}

impl ObjectRecord {
    pub fn new(
        object_id: String,
        object_kind: String,
        in_situ_ref: Option<String>,
        semantic_payload: Option<SemanticPayload>,
        event_time: Option<DateTime<FixedOffset>>,
    ) -> Self {
        let now = event_time.unwrap_or_else(now_fixed);
        Self {
            object_id,
            object_kind,
            in_situ_ref,
            semantic_payload,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExpectationMetadata {
    #[serde(default = "default_interpretation")]
    pub interpretation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stereotype: Option<JsonMap>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stereotype_linknode_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comparison_rules: Option<JsonMap>,
}

impl Default for ExpectationMetadata {
    fn default() -> Self {
        Self {
            interpretation: default_interpretation(),
            stereotype: None,
            stereotype_linknode_id: None,
            comparison_rules: None,
        }
    }
}

/// Allowed graph topology for a container.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum GraphShape {
    /// No topology constraint — members may appear in any number of containers (default).
    #[default]
    Any,
    /// Tree: each member object may belong to at most one container at a time.
    /// Adding a member that already belongs to another container is rejected.
    Tree,
    /// DAG: directed acyclic graph — membership chains must not form cycles.
    /// Adding member M to container C is rejected if C is transitively reachable
    /// from M through container membership.
    Dag,
}

/// What to do when a container with `max_members` is full and a new member is added.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OverflowPolicy {
    /// Reject the add with an error (default — preserves existing behavior).
    #[default]
    Reject,
    /// Silently evict the oldest (head) member to make room, then add the new member.
    /// "Oldest" is defined as the head link node (first-inserted position).
    EvictOldest,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ContainerPolicies {
    /// Reject duplicate membership: adding an already-present member is a no-op (not an error).
    #[serde(default)]
    pub unique_members: bool,
    /// Maximum number of members allowed in this container.  `None` = unlimited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_members: Option<usize>,
    /// What to do when `max_members` is reached and a new member is added.
    /// `Reject` (default) returns an error; `EvictOldest` removes the head member first.
    #[serde(default, skip_serializing_if = "is_overflow_policy_reject")]
    pub overflow_policy: OverflowPolicy,
    /// When true, members must be attached in non-decreasing `created_at` order.
    /// Attaching an object whose `created_at` is earlier than the current tail's
    /// `created_at` is rejected.  Use this to enforce recency-tier ordering at
    /// the substrate level rather than in ad-hoc caller logic.
    #[serde(default)]
    pub ordered_by_recency: bool,
    /// Topology constraint applied at mutation time.
    #[serde(default, skip_serializing_if = "is_graph_shape_any")]
    pub graph_shape: GraphShape,
    /// When true, adding an object that is already a member of another container is silently
    /// allowed (overrides `Tree` cardinality for cross-container moves that are managed
    /// externally).  False by default.
    #[serde(default)]
    pub allow_multi_parent: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additional_policies: Option<JsonMap>,
}

fn is_graph_shape_any(s: &GraphShape) -> bool {
    *s == GraphShape::Any
}

fn is_overflow_policy_reject(p: &OverflowPolicy) -> bool {
    *p == OverflowPolicy::Reject
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AbsoluteSemantics {
    #[serde(default = "default_absolute_kind")]
    pub absolute_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_metadata: Option<JsonMap>,
}

impl Default for AbsoluteSemantics {
    fn default() -> Self {
        Self {
            absolute_kind: default_absolute_kind(),
            origin_metadata: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HypothesisAnnotation {
    pub key: String,
    pub value: String,
    pub updated_at: DateTime<FixedOffset>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContainerRecord {
    pub container_id: String,
    pub container_kind: String,
    #[serde(default)]
    pub expectation_metadata: ExpectationMetadata,
    #[serde(default)]
    pub policies: ContainerPolicies,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchors: Option<Vec<String>>,
    #[serde(default)]
    pub absolute_semantics: AbsoluteSemantics,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_linknode_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_linknode_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonMap>,
    #[serde(default)]
    pub hypothesis_state: BTreeMap<String, HypothesisAnnotation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_payload: Option<SemanticPayload>,
}

impl ContainerRecord {
    pub fn new(container_id: String, container_kind: String) -> Self {
        Self {
            container_id,
            container_kind,
            expectation_metadata: ExpectationMetadata::default(),
            policies: ContainerPolicies::default(),
            anchors: None,
            absolute_semantics: AbsoluteSemantics::default(),
            head_linknode_id: None,
            tail_linknode_id: None,
            metadata: None,
            hypothesis_state: BTreeMap::new(),
            semantic_payload: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LinkNodeRecord {
    pub link_node_id: String,
    pub container_id: String,
    pub object_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_linknode_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_linknode_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rel_delta: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonMap>,
}

impl LinkNodeRecord {
    pub fn new(
        link_node_id: String,
        container_id: String,
        object_id: String,
        prev_linknode_id: Option<String>,
        next_linknode_id: Option<String>,
        rel_delta: Option<Value>,
    ) -> Self {
        Self {
            link_node_id,
            container_id,
            object_id,
            prev_linknode_id,
            next_linknode_id,
            rel_delta,
            metadata: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct AmsSnapshot {
    pub objects: Vec<ObjectRecord>,
    pub containers: Vec<ContainerRecord>,
    pub link_nodes: Vec<LinkNodeRecord>,
}

pub fn now_fixed() -> DateTime<FixedOffset> {
    Utc::now().fixed_offset()
}

fn default_interpretation() -> String {
    "ordered_frame".to_string()
}

fn default_absolute_kind() -> String {
    "other".to_string()
}
