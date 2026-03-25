use ams_core_kernel::swarm_dag::mock_agents::MockAgentPool;
use ams_core_kernel::swarm_dag::model::*;
use ams_core_kernel::swarm_dag::orchestrator::Orchestrator;

#[test]
fn test_dag_lifecycle_with_mock_agents() {
    let orch = Orchestrator::new(3);

    // Create a root task
    let root = DagNode::new(
        "root".into(),
        None,
        "Build a web server".into(),
        AgentKind::TempOrchestrator,
    );
    let mut dag = ExecutionDag::new("test-dag".into(), root);

    // Decompose root into subtasks
    let subtasks = vec![
        SubtaskSpec {
            description: "Write the HTTP handler code with proper error handling and routing".into(),
            agent_kind: AgentKind::CodeWriter,
            input: Some(serde_json::json!({"framework": "actix"})),
            output_schema: None,
            depends_on_indices: vec![],
        },
        SubtaskSpec {
            description: "Validate the handler output".into(),
            agent_kind: AgentKind::Validator,
            input: None,
            output_schema: None,
            depends_on_indices: vec![0],
        },
        SubtaskSpec {
            description: "Summarize the implementation for documentation purposes with full context".into(),
            agent_kind: AgentKind::Summarizer,
            input: None,
            output_schema: None,
            depends_on_indices: vec![1],
        },
    ];

    orch.decompose_task(&mut dag, "root", subtasks);

    // Mark root as completed (orchestrator has done its decomposition job)
    dag.get_node_mut("root").unwrap().status = NodeStatus::Completed;

    // Root should now have 3 children
    assert_eq!(dag.get_node("root").unwrap().children.len(), 3);

    // Step 1: only the first subtask (code writer) should be ready
    let dispatches = orch.step(&mut dag);
    assert_eq!(dispatches.len(), 1);
    assert_eq!(dispatches[0].agent_kind, AgentKind::CodeWriter);
    assert!(dispatches[0].parent_goal.is_some());

    // Execute with mock agent
    let result = MockAgentPool::dispatch(&dispatches[0]);
    assert!(result.success);

    // Apply result
    let mut orch = Orchestrator::new(3);
    let action = orch.apply_result(&mut dag, result);
    assert!(matches!(action, ams_core_kernel::swarm_dag::orchestrator::OrchestratorAction::Dispatch(_)));

    // Step 2: validator should now be ready
    let dispatches = orch.step(&mut dag);
    assert_eq!(dispatches.len(), 1);
    assert_eq!(dispatches[0].agent_kind, AgentKind::Validator);

    // Execute validator — it will fail because input is None (upstream output not wired)
    let result = MockAgentPool::dispatch(&dispatches[0]);
    // Validator with no input fails
    assert!(!result.success);

    // Apply failure — should insert repair node
    let action = orch.apply_result(&mut dag, result);
    assert!(matches!(
        action,
        ams_core_kernel::swarm_dag::orchestrator::OrchestratorAction::RepairInserted { .. }
    ));
}

#[test]
fn test_repair_escalation() {
    let mut orch = Orchestrator::new(1); // very low repair depth

    let mut root = DagNode::new("root".into(), None, "task".into(), AgentKind::CodeWriter);
    root.repair_count = 2; // already exceeded
    let mut dag = ExecutionDag::new("test-dag".into(), root);

    let failure = AgentResult {
        node_id: "root".into(),
        success: false,
        output: None,
        error: Some("persistent failure".into()),
        decomposition: None,
    };

    let action = orch.apply_result(&mut dag, failure);
    assert!(matches!(
        action,
        ams_core_kernel::swarm_dag::orchestrator::OrchestratorAction::Escalated(_)
    ));
}

#[test]
fn test_agent_selection_with_performance_data() {
    let perf = vec![
        AgentPerformanceEntry {
            agent_kind: AgentKind::CodeWriter,
            task_type: "code".into(),
            successes: 8,
            failures: 2,
            total_attempts: 10,
        },
        AgentPerformanceEntry {
            agent_kind: AgentKind::Critic,
            task_type: "code".into(),
            successes: 9,
            failures: 1,
            total_attempts: 10,
        },
    ];

    // Should pick Critic (90% vs 80%)
    let best = Orchestrator::select_agent_kind(&perf, "code");
    assert_eq!(best, AgentKind::Critic);

    // Fallback for unknown task type
    let fallback = Orchestrator::select_agent_kind(&[], "validate something");
    assert_eq!(fallback, AgentKind::Validator);
}

#[test]
fn test_full_dag_completion() {
    let orch = Orchestrator::new(3);

    let root = DagNode::new("root".into(), None, "simple task".into(), AgentKind::CodeWriter);
    let mut dag = ExecutionDag::new("test-dag".into(), root);

    // Step and execute
    let dispatches = orch.step(&mut dag);
    assert_eq!(dispatches.len(), 1);

    let result = MockAgentPool::dispatch(&dispatches[0]);
    let mut orch = Orchestrator::new(3);
    orch.apply_result(&mut dag, result);

    assert!(dag.is_complete());
}
