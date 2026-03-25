use std::collections::BTreeMap;

use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::{now_fixed, JsonMap};

// ── Agent types ──────────────────────────────────────────────────────

/// The taxonomy of specialized swarm agent types.
/// Each maps to a different LLM configuration (model size, tools, fine-tuning).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentKind {
    CodeWriter,
    Validator,
    Summarizer,
    Router,
    MemoryReader,
    Critic,
    /// A temporary orchestrator spawned for recursive decomposition (DQ1).
    TempOrchestrator,
    /// A repair agent inserted as a blocker node (DQ3).
    Repairer,
}

impl std::fmt::Display for AgentKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::CodeWriter => "code_writer",
            Self::Validator => "validator",
            Self::Summarizer => "summarizer",
            Self::Router => "router",
            Self::MemoryReader => "memory_reader",
            Self::Critic => "critic",
            Self::TempOrchestrator => "temp_orchestrator",
            Self::Repairer => "repairer",
        };
        f.write_str(s)
    }
}

// ── DAG node ─────────────────────────────────────────────────────────

/// Status of a DAG node through its lifecycle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    #[default]
    Pending,
    /// Blocked by a repair agent (DQ3 blocker node).
    Blocked,
    Running,
    Completed,
    Failed,
}

/// A single node in the execution DAG.
///
/// Each node represents a subtask assigned to a swarm agent.
/// Nodes link to their parent, dependencies, and children in the DAG.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DagNode {
    pub node_id: String,
    pub parent_id: Option<String>,
    pub task_description: String,
    pub agent_kind: AgentKind,
    pub status: NodeStatus,

    /// IDs of nodes that must complete before this node can run.
    pub depends_on: Vec<String>,
    /// IDs of child nodes (subtasks from decomposition).
    pub children: Vec<String>,

    /// Input data from parent or upstream dependencies.
    pub input: Option<Value>,
    /// Output schema expected from the agent.
    pub output_schema: Option<Value>,
    /// Actual output produced by the agent.
    pub output: Option<Value>,

    /// Home-node context: summaries of neighbor/parent goals (DQ4).
    pub neighbor_context: Option<JsonMap>,

    /// Performance tracking for FEP agent selection (DQ7).
    pub attempt_count: u32,
    pub repair_count: u32,

    pub created_at: DateTime<FixedOffset>,
    pub updated_at: DateTime<FixedOffset>,
}

impl DagNode {
    pub fn new(
        node_id: String,
        parent_id: Option<String>,
        task_description: String,
        agent_kind: AgentKind,
    ) -> Self {
        let now = now_fixed();
        Self {
            node_id,
            parent_id,
            task_description,
            agent_kind,
            status: NodeStatus::Pending,
            depends_on: Vec::new(),
            children: Vec::new(),
            input: None,
            output_schema: None,
            output: None,
            neighbor_context: None,
            attempt_count: 0,
            repair_count: 0,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    pub fn is_ready(&self, completed: &[String]) -> bool {
        self.status == NodeStatus::Pending
            && self.depends_on.iter().all(|dep| completed.contains(dep))
    }
}

// ── Execution DAG ────────────────────────────────────────────────────

/// The full execution DAG for a swarm computation.
///
/// Nodes are stored in a BTreeMap for O(log n) lookup.
/// The root_id identifies the top-level task node.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionDag {
    pub dag_id: String,
    pub root_id: String,
    pub nodes: BTreeMap<String, DagNode>,
    pub created_at: DateTime<FixedOffset>,
}

impl ExecutionDag {
    pub fn new(dag_id: String, root_node: DagNode) -> Self {
        let root_id = root_node.node_id.clone();
        let mut nodes = BTreeMap::new();
        nodes.insert(root_id.clone(), root_node);
        Self {
            dag_id,
            root_id,
            nodes,
            created_at: now_fixed(),
        }
    }

    pub fn add_node(&mut self, node: DagNode) {
        let node_id = node.node_id.clone();
        if let Some(parent_id) = &node.parent_id {
            if let Some(parent) = self.nodes.get_mut(parent_id) {
                parent.children.push(node_id.clone());
            }
        }
        self.nodes.insert(node_id, node);
    }

    pub fn get_node(&self, node_id: &str) -> Option<&DagNode> {
        self.nodes.get(node_id)
    }

    pub fn get_node_mut(&mut self, node_id: &str) -> Option<&mut DagNode> {
        self.nodes.get_mut(node_id)
    }

    /// Returns IDs of all nodes ready to execute (dependencies satisfied, status pending).
    pub fn ready_nodes(&self) -> Vec<String> {
        let completed: Vec<String> = self
            .nodes
            .values()
            .filter(|n| n.status == NodeStatus::Completed)
            .map(|n| n.node_id.clone())
            .collect();
        self.nodes
            .values()
            .filter(|n| n.is_ready(&completed))
            .map(|n| n.node_id.clone())
            .collect()
    }

    /// Insert a repair agent as a blocker node in front of a failed node (DQ3).
    pub fn insert_repair_node(
        &mut self,
        failed_node_id: &str,
        repair_node_id: String,
        repair_description: String,
    ) -> Option<String> {
        let failed = self.nodes.get(failed_node_id)?;
        let parent_id = failed.parent_id.clone();
        let error_context = failed.output.clone();

        let mut repair = DagNode::new(
            repair_node_id.clone(),
            parent_id,
            repair_description,
            AgentKind::Repairer,
        );
        repair.input = error_context;

        // The failed node now depends on the repair node
        if let Some(failed) = self.nodes.get_mut(failed_node_id) {
            failed.depends_on.push(repair_node_id.clone());
            failed.status = NodeStatus::Blocked;
            failed.repair_count += 1;
        }

        self.nodes.insert(repair_node_id.clone(), repair);
        Some(repair_node_id)
    }

    /// Check if repair depth exceeds the limit for a given node (DQ3/DQ5 escalation).
    pub fn repair_depth_exceeded(&self, node_id: &str, max_depth: u32) -> bool {
        self.nodes
            .get(node_id)
            .map(|n| n.repair_count > max_depth)
            .unwrap_or(false)
    }

    /// Collect all nodes affected by a failure for issue-node creation (DQ5).
    pub fn affected_subtree(&self, node_id: &str) -> Vec<String> {
        let mut result = vec![node_id.to_string()];
        if let Some(node) = self.nodes.get(node_id) {
            for child_id in &node.children {
                result.extend(self.affected_subtree(child_id));
            }
        }
        result
    }

    /// Returns true when all nodes are completed.
    pub fn is_complete(&self) -> bool {
        self.nodes.values().all(|n| n.status == NodeStatus::Completed)
    }
}

// ── Agent dispatch ───────────────────────────────────────────────────

/// A dispatch record sent to a swarm agent. This is the agent's "context window".
/// Designed to fit within ~1K tokens.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentDispatch {
    pub node_id: String,
    pub task_description: String,
    pub agent_kind: AgentKind,
    pub input: Option<Value>,
    pub output_schema: Option<Value>,
    /// Summary of parent's goal (DQ4 home-node context).
    pub parent_goal: Option<String>,
    /// Summaries of neighbor nodes' goals (DQ4 locality).
    pub neighbor_goals: Vec<NeighborSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NeighborSummary {
    pub node_id: String,
    pub task_summary: String,
    pub status: NodeStatus,
}

/// Result returned by a swarm agent after execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentResult {
    pub node_id: String,
    pub success: bool,
    pub output: Option<Value>,
    pub error: Option<String>,
    /// If the agent determines the task needs further decomposition (DQ1),
    /// it returns subtask descriptions instead of a direct result.
    pub decomposition: Option<Vec<SubtaskSpec>>,
}

/// A subtask specification returned when recursive decomposition is needed (DQ1).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubtaskSpec {
    pub description: String,
    pub agent_kind: AgentKind,
    pub input: Option<Value>,
    pub output_schema: Option<Value>,
    pub depends_on_indices: Vec<usize>,
}

// ── Performance graph for FEP agent selection (DQ7) ──────────────────

/// Tracks success/failure rates per AgentKind × task-type for FEP optimization.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AgentPerformanceEntry {
    pub agent_kind: AgentKind,
    pub task_type: String,
    pub successes: u64,
    pub failures: u64,
    pub total_attempts: u64,
}

impl Default for AgentKind {
    fn default() -> Self {
        Self::CodeWriter
    }
}

impl AgentPerformanceEntry {
    pub fn success_rate(&self) -> f64 {
        if self.total_attempts == 0 {
            return 0.5; // uninformative prior
        }
        self.successes as f64 / self.total_attempts as f64
    }

    pub fn record_outcome(&mut self, success: bool) {
        self.total_attempts += 1;
        if success {
            self.successes += 1;
        } else {
            self.failures += 1;
        }
    }
}

// ── Issue node for human escalation (DQ5) ────────────────────────────

/// An issue node created when repair depth is exceeded.
/// Contains all affected nodes and a summary for the human operator.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IssueNode {
    pub issue_id: String,
    pub summary: String,
    pub affected_node_ids: Vec<String>,
    pub escalation_reason: String,
    pub created_at: DateTime<FixedOffset>,
}

impl IssueNode {
    pub fn new(
        issue_id: String,
        summary: String,
        affected_node_ids: Vec<String>,
        escalation_reason: String,
    ) -> Self {
        Self {
            issue_id,
            summary,
            affected_node_ids,
            escalation_reason,
            created_at: now_fixed(),
        }
    }
}
