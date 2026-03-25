use super::model::*;
use uuid::Uuid;

/// Action returned by orchestrator after processing results or advancing the DAG.
#[derive(Clone, Debug)]
pub enum OrchestratorAction {
    /// Dispatches ready for agent execution.
    Dispatch(Vec<AgentDispatch>),
    /// Task was decomposed into subtasks.
    Decomposed {
        parent_id: String,
        child_count: usize,
    },
    /// A repair node was inserted to fix a failed task.
    RepairInserted {
        repair_node_id: String,
        failed_node_id: String,
    },
    /// Escalated to human review due to repair depth exceeded.
    Escalated(IssueNode),
    /// DAG is fully complete.
    Complete,
}

/// Orchestrator manages DAG execution, agent selection, and repair/decomposition logic.
pub struct Orchestrator {
    max_repair_depth: u32,
}

impl Orchestrator {
    /// Create a new orchestrator with configurable repair depth limit.
    pub fn new(max_repair_depth: u32) -> Self {
        Self { max_repair_depth }
    }

    /// Decompose a parent task into subtasks, wiring up dependencies.
    ///
    /// Takes a list of SubtaskSpec and creates DagNode children under parent,
    /// linking them via `depends_on` according to the dependency indices.
    pub fn decompose_task(
        &self,
        dag: &mut ExecutionDag,
        parent_id: &str,
        subtasks: Vec<SubtaskSpec>,
    ) {
        let mut node_id_map = vec![];

        // Create all child nodes first
        for spec in &subtasks {
            let node_id = Uuid::new_v4().to_string();
            node_id_map.push(node_id.clone());

            let node = DagNode::new(
                node_id,
                Some(parent_id.to_string()),
                spec.description.clone(),
                spec.agent_kind.clone(),
            );
            let mut node = node;
            node.input = spec.input.clone();
            node.output_schema = spec.output_schema.clone();

            dag.add_node(node);
        }

        // Wire up dependencies
        for (idx, spec) in subtasks.iter().enumerate() {
            if let Some(node) = dag.get_node_mut(&node_id_map[idx]) {
                for dep_idx in &spec.depends_on_indices {
                    if let Some(dep_id) = node_id_map.get(*dep_idx) {
                        node.depends_on.push(dep_id.clone());
                    }
                }
            }
        }
    }

    /// Build a dispatch context for a node, including parent and neighbor summaries (DQ4).
    pub fn build_dispatch(&self, dag: &ExecutionDag, node_id: &str) -> Option<AgentDispatch> {
        let node = dag.get_node(node_id)?;

        // Get parent goal summary
        let parent_goal = node
            .parent_id
            .as_ref()
            .and_then(|pid| dag.get_node(pid))
            .map(|p| format!("Parent: {}", p.task_description));

        // Get neighbor summaries (siblings at the same level)
        let neighbor_goals = node
            .parent_id
            .as_ref()
            .and_then(|pid| dag.get_node(pid))
            .map(|parent| {
                parent
                    .children
                    .iter()
                    .filter_map(|cid| {
                        dag.get_node(cid).map(|c| NeighborSummary {
                            node_id: c.node_id.clone(),
                            task_summary: c.task_description.clone(),
                            status: c.status.clone(),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        Some(AgentDispatch {
            node_id: node.node_id.clone(),
            task_description: node.task_description.clone(),
            agent_kind: node.agent_kind.clone(),
            input: node.input.clone(),
            output_schema: node.output_schema.clone(),
            parent_goal,
            neighbor_goals,
        })
    }

    /// Process an agent result: success, failure with repair, or decomposition.
    pub fn apply_result(
        &mut self,
        dag: &mut ExecutionDag,
        result: AgentResult,
    ) -> OrchestratorAction {
        if result.success {
            // Mark as completed
            if let Some(node) = dag.get_node_mut(&result.node_id) {
                node.status = NodeStatus::Completed;
                node.output = result.output;
            }
            return OrchestratorAction::Dispatch(vec![]);
        }

        // Check for decomposition (DQ1)
        if let Some(decomposition) = result.decomposition {
            let child_count = decomposition.len();
            self.decompose_task(dag, &result.node_id, decomposition);
            return OrchestratorAction::Decomposed {
                parent_id: result.node_id,
                child_count,
            };
        }

        // Failure case: check repair depth (DQ3/DQ5)
        if dag.repair_depth_exceeded(&result.node_id, self.max_repair_depth) {
            // Escalate to issue node
            let affected = dag.affected_subtree(&result.node_id);
            let issue_id = Uuid::new_v4().to_string();
            let issue = IssueNode::new(
                issue_id,
                format!("Task failed after max repairs: {}", result.node_id),
                affected,
                format!("Repair depth exceeded for node {}", result.node_id),
            );
            return OrchestratorAction::Escalated(issue);
        }

        // Insert repair node
        let repair_id = Uuid::new_v4().to_string();
        let repair_desc = format!(
            "Repair failed task {}: {}",
            &result.node_id,
            result.error.as_deref().unwrap_or("unknown error")
        );

        dag.insert_repair_node(&result.node_id, repair_id.clone(), repair_desc);
        OrchestratorAction::RepairInserted {
            repair_node_id: repair_id,
            failed_node_id: result.node_id,
        }
    }

    /// Select best agent kind based on performance data, with fallback to default mapping.
    pub fn select_agent_kind(perf: &[AgentPerformanceEntry], task_type: &str) -> AgentKind {
        // Filter for task type and find best success rate
        let best = perf
            .iter()
            .filter(|e| e.task_type == task_type)
            .max_by(|a, b| {
                a.success_rate()
                    .partial_cmp(&b.success_rate())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

        if let Some(entry) = best {
            return entry.agent_kind.clone();
        }

        // Fallback: default mapping based on task type keywords
        match task_type.to_lowercase().as_str() {
            t if t.contains("code") || t.contains("implement") => AgentKind::CodeWriter,
            t if t.contains("validate") || t.contains("check") => AgentKind::Validator,
            t if t.contains("summarize") || t.contains("summary") => AgentKind::Summarizer,
            t if t.contains("route") || t.contains("decide") => AgentKind::Router,
            t if t.contains("memory") || t.contains("retrieve") => AgentKind::MemoryReader,
            t if t.contains("critique") || t.contains("review") => AgentKind::Critic,
            _ => AgentKind::CodeWriter, // default
        }
    }

    /// Advance the DAG one step: find ready nodes and build dispatches for parallel execution.
    pub fn step(&self, dag: &mut ExecutionDag) -> Vec<AgentDispatch> {
        if dag.is_complete() {
            return vec![];
        }

        let ready = dag.ready_nodes();
        let dispatches = ready
            .iter()
            .filter_map(|node_id| {
                let dispatch = self.build_dispatch(dag, node_id)?;
                // Mark as running
                if let Some(node) = dag.get_node_mut(node_id) {
                    node.status = NodeStatus::Running;
                }
                Some(dispatch)
            })
            .collect();

        dispatches
    }
}
