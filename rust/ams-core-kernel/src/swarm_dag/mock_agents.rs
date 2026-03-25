use serde_json::json;

use super::model::*;

/// Trait for swarm agents that execute tasks in the DAG.
pub trait SwarmAgent {
    fn execute(&self, dispatch: &AgentDispatch) -> AgentResult;
    fn agent_kind(&self) -> AgentKind;
}

// ── Mock implementations ──────────────────────────────────────────────

/// Mock code writer agent. Simulates generating code output.
pub struct MockCodeWriter;

impl SwarmAgent for MockCodeWriter {
    fn execute(&self, dispatch: &AgentDispatch) -> AgentResult {
        let code = format!(
            "// Generated code for task: {}\nfn task_implementation() {{\n    println!(\"Task output\");\n}}",
            dispatch.task_description
        );

        AgentResult {
            node_id: dispatch.node_id.clone(),
            success: true,
            output: Some(json!({ "code": code })),
            error: None,
            decomposition: None,
        }
    }

    fn agent_kind(&self) -> AgentKind {
        AgentKind::CodeWriter
    }
}

/// Mock validator agent. Checks if input contains required fields.
pub struct MockValidator;

impl SwarmAgent for MockValidator {
    fn execute(&self, dispatch: &AgentDispatch) -> AgentResult {
        let success = dispatch.input.is_some();
        let message = if success {
            "Validation passed".to_string()
        } else {
            "Validation failed: missing required input".to_string()
        };

        AgentResult {
            node_id: dispatch.node_id.clone(),
            success,
            output: Some(json!({ "valid": success, "message": message })),
            error: if success { None } else { Some(message) },
            decomposition: None,
        }
    }

    fn agent_kind(&self) -> AgentKind {
        AgentKind::Validator
    }
}

/// Mock summarizer agent. Returns shortened version of input.
pub struct MockSummarizer;

impl SwarmAgent for MockSummarizer {
    fn execute(&self, dispatch: &AgentDispatch) -> AgentResult {
        let summary = if dispatch.task_description.len() > 100 {
            format!("{}...", &dispatch.task_description[..100])
        } else {
            dispatch.task_description.clone()
        };

        AgentResult {
            node_id: dispatch.node_id.clone(),
            success: true,
            output: Some(json!({ "summary": summary })),
            error: None,
            decomposition: None,
        }
    }

    fn agent_kind(&self) -> AgentKind {
        AgentKind::Summarizer
    }
}

/// Mock router agent. Decomposes tasks into subtasks based on keywords.
pub struct MockRouter;

impl SwarmAgent for MockRouter {
    fn execute(&self, dispatch: &AgentDispatch) -> AgentResult {
        let desc = dispatch.task_description.to_lowercase();

        // Simple keyword-based decomposition
        let decomposition = if desc.contains("split") || desc.contains("decompose") {
            Some(vec![
                SubtaskSpec {
                    description: "Subtask 1: Analyze requirements".to_string(),
                    agent_kind: AgentKind::CodeWriter,
                    input: dispatch.input.clone(),
                    output_schema: None,
                    depends_on_indices: vec![],
                },
                SubtaskSpec {
                    description: "Subtask 2: Implement solution".to_string(),
                    agent_kind: AgentKind::CodeWriter,
                    input: None,
                    output_schema: None,
                    depends_on_indices: vec![0],
                },
                SubtaskSpec {
                    description: "Subtask 3: Validate output".to_string(),
                    agent_kind: AgentKind::Validator,
                    input: None,
                    output_schema: None,
                    depends_on_indices: vec![1],
                },
            ])
        } else {
            Some(vec![
                SubtaskSpec {
                    description: "Single subtask for: ".to_string() + &dispatch.task_description,
                    agent_kind: AgentKind::CodeWriter,
                    input: dispatch.input.clone(),
                    output_schema: None,
                    depends_on_indices: vec![],
                },
            ])
        };

        AgentResult {
            node_id: dispatch.node_id.clone(),
            success: true,
            output: Some(json!({ "decomposed": decomposition.is_some() })),
            error: None,
            decomposition,
        }
    }

    fn agent_kind(&self) -> AgentKind {
        AgentKind::Router
    }
}

/// Mock memory reader agent. Returns mock memory retrieval results.
pub struct MockMemoryReader;

impl SwarmAgent for MockMemoryReader {
    fn execute(&self, dispatch: &AgentDispatch) -> AgentResult {
        let query = dispatch.task_description.clone();
        let results = vec![
            json!({
                "session_id": "abc123",
                "title": "Previous work on similar task",
                "relevance": 0.95
            }),
            json!({
                "session_id": "def456",
                "title": "Related architecture discussion",
                "relevance": 0.72
            }),
        ];

        AgentResult {
            node_id: dispatch.node_id.clone(),
            success: true,
            output: Some(json!({
                "query": query,
                "results": results
            })),
            error: None,
            decomposition: None,
        }
    }

    fn agent_kind(&self) -> AgentKind {
        AgentKind::MemoryReader
    }
}

/// Mock critic agent. Approves or requests revision deterministically based on task.
pub struct MockCritic;

impl SwarmAgent for MockCritic {
    fn execute(&self, dispatch: &AgentDispatch) -> AgentResult {
        // Deterministic: approve if task description is longer than 50 chars
        let approve = dispatch.task_description.len() > 50;

        let decision = if approve {
            "approved"
        } else {
            "revision_requested"
        };

        let feedback = if approve {
            "Solution meets quality standards".to_string()
        } else {
            "Needs more detail and error handling".to_string()
        };

        AgentResult {
            node_id: dispatch.node_id.clone(),
            success: true,
            output: Some(json!({
                "decision": decision,
                "feedback": feedback,
                "approved": approve
            })),
            error: None,
            decomposition: None,
        }
    }

    fn agent_kind(&self) -> AgentKind {
        AgentKind::Critic
    }
}

/// Mock repairer agent. Always succeeds with repaired output.
pub struct MockRepairer;

impl SwarmAgent for MockRepairer {
    fn execute(&self, dispatch: &AgentDispatch) -> AgentResult {
        let repaired = format!("[REPAIRED] {}", dispatch.task_description);

        AgentResult {
            node_id: dispatch.node_id.clone(),
            success: true,
            output: Some(json!({
                "repaired_description": repaired,
                "repair_applied": true
            })),
            error: None,
            decomposition: None,
        }
    }

    fn agent_kind(&self) -> AgentKind {
        AgentKind::Repairer
    }
}

// ── Agent pool ────────────────────────────────────────────────────────

/// Pool of mock agents that dispatches to the appropriate mock implementation.
pub struct MockAgentPool;

impl MockAgentPool {
    /// Dispatch a task to the appropriate mock agent based on agent_kind.
    pub fn dispatch(dispatch: &AgentDispatch) -> AgentResult {
        match dispatch.agent_kind {
            AgentKind::CodeWriter => MockCodeWriter.execute(dispatch),
            AgentKind::Validator => MockValidator.execute(dispatch),
            AgentKind::Summarizer => MockSummarizer.execute(dispatch),
            AgentKind::Router => MockRouter.execute(dispatch),
            AgentKind::MemoryReader => MockMemoryReader.execute(dispatch),
            AgentKind::Critic => MockCritic.execute(dispatch),
            AgentKind::Repairer => MockRepairer.execute(dispatch),
            AgentKind::TempOrchestrator => {
                // TempOrchestrator is not a standalone agent, but handle gracefully
                AgentResult {
                    node_id: dispatch.node_id.clone(),
                    success: false,
                    output: None,
                    error: Some("TempOrchestrator should not be dispatched directly".to_string()),
                    decomposition: None,
                }
            }
        }
    }
}
