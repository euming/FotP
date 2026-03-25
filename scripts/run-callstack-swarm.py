#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from ams_common import build_rust_ams_cmd, repo_root, rust_backend_env

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(repo_root())
AMS_PY = REPO_ROOT / "scripts" / "ams.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog=r"scripts\run-callstack-swarm.py",
        description="Run the SmartList-first interrupt and callstack smoke flow.",
    )
    parser.add_argument("--workspace", type=Path, help="Optional workspace directory. Defaults to a temp directory.")
    parser.add_argument("--keep-workspace", action="store_true", help="Keep the temp workspace on success.")
    parser.add_argument("--actor-id", default="smoke-harness")
    parser.add_argument("--root-name", default="root-task")
    parser.add_argument("--work-name", default="broken-work")
    parser.add_argument("--repair-hint", default="Apply the SmartList-first repair policy.")
    return parser.parse_args()


def parse_kv(stdout: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def run_wrapper(backend_root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    if not args:
        raise RuntimeError("run_wrapper requires command arguments")
    if args[0] == "callstack":
        cmd = [
            sys.executable,
            str(AMS_PY),
            "callstack",
            "--corpus",
            "all",
            "--backend-root",
            str(backend_root),
            *args[1:],
        ]
    else:
        cmd = [
            sys.executable,
            str(AMS_PY),
            *args,
            "--corpus",
            "all",
            "--backend-root",
            str(backend_root),
        ]
    return subprocess.run(cmd, cwd=REPO_ROOT, text=True, capture_output=True, check=False)


def ensure_ok(result: subprocess.CompletedProcess[str], label: str) -> None:
    if result.returncode == 0:
        return
    raise RuntimeError(
        f"{label} failed with exit={result.returncode}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )


def resolve_snapshot_path(corpus_path: Path, backend_root: Path) -> Path:
    kernel_cmd = build_rust_ams_cmd()
    if kernel_cmd is None:
        raise RuntimeError("unable to locate the Rust AMS kernel binary or Cargo project")
    result = subprocess.run(
        [*kernel_cmd, "backend-status", "--input", str(corpus_path)],
        cwd=REPO_ROOT,
        env=rust_backend_env(str(backend_root)),
        text=True,
        capture_output=True,
        check=False,
    )
    ensure_ok(result, "backend-status")
    data = parse_kv(result.stdout)
    snapshot_path = data.get("snapshot_path")
    if not snapshot_path:
        raise RuntimeError("backend-status did not report snapshot_path")
    return Path(snapshot_path)


def snapshot_indexes(snapshot: dict) -> tuple[dict[str, dict], dict[str, dict], dict[str, dict]]:
    objects = {obj["objectId"]: obj for obj in snapshot.get("objects", [])}
    containers = {container["containerId"]: container for container in snapshot.get("containers", [])}
    links = {link["linkNodeId"]: link for link in snapshot.get("linkNodes", [])}
    return objects, containers, links


def container_members(snapshot: dict, container_id: str) -> list[str]:
    _, containers, links = snapshot_indexes(snapshot)
    container = containers.get(container_id)
    if container is None:
        return []
    members: list[str] = []
    current = container.get("headLinknodeId")
    visited: set[str] = set()
    while current:
        if current in visited:
            break
        visited.add(current)
        link = links.get(current)
        if link is None:
            break
        members.append(link["objectId"])
        current = link.get("nextLinknodeId")
    return members


def bucket_object_id(path: str) -> str:
    return f"smartlist-bucket:{path}"


def bucket_fields(snapshot: dict, path: str) -> dict[str, str]:
    objects, _, _ = snapshot_indexes(snapshot)
    obj = objects.get(bucket_object_id(path))
    if obj is None:
        return {}
    provenance = ((obj.get("semanticPayload") or {}).get("provenance") or {})
    return {key: str(value) for key, value in provenance.items()}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main() -> int:
    args = parse_args()

    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    if args.workspace is None:
        temp_dir = tempfile.TemporaryDirectory(prefix="ams-callstack-smartlist-")
        workspace = Path(temp_dir.name)
    else:
        workspace = args.workspace.resolve()
        workspace.mkdir(parents=True, exist_ok=True)

    backend_root = workspace / "backend"
    corpus_dir = workspace / "all-agents-sessions"
    backend_root.mkdir(parents=True, exist_ok=True)
    corpus_dir.mkdir(parents=True, exist_ok=True)
    corpus_path = corpus_dir / "all-agents-sessions.memory.jsonl"
    corpus_path.write_text("", encoding="utf-8")

    report: dict[str, object] = {
        "workspace": str(workspace),
        "backend_root": str(backend_root),
        "corpus_path": str(corpus_path),
        "actor_id": args.actor_id,
    }

    try:
        steps: list[dict[str, object]] = []

        root_push = run_wrapper(backend_root, "callstack", "push", args.root_name, "--actor-id", args.actor_id)
        ensure_ok(root_push, "callstack push root")
        root_push_data = parse_kv(root_push.stdout)
        root_path = root_push_data["node_path"]
        steps.append({"label": "push_root", "stdout": root_push.stdout})

        work_push = run_wrapper(
            backend_root,
            "callstack",
            "push",
            args.work_name,
            "--description",
            "Primary work item that will be interrupted.",
            "--actor-id",
            args.actor_id,
        )
        ensure_ok(work_push, "callstack push work")
        work_push_data = parse_kv(work_push.stdout)
        interrupted_node_path = work_push_data["node_path"]
        steps.append({"label": "push_work", "stdout": work_push.stdout})

        interrupt = run_wrapper(
            backend_root,
            "callstack",
            "interrupt",
            "--policy",
            "repair",
            "--reason",
            "smoke-test",
            "--error-output",
            "simulated failure",
            "--context",
            "smoke harness",
            "--attempted-fix",
            "none",
            "--repair-hint",
            args.repair_hint,
            "--actor-id",
            args.actor_id,
        )
        ensure_ok(interrupt, "callstack interrupt")
        interrupt_data = parse_kv(interrupt.stdout)
        interrupt_path = interrupt_data["interrupt_path"]
        policy_path = interrupt_data["policy_path"]
        require(policy_path, "interrupt command did not auto-push a repair policy child")
        steps.append({"label": "interrupt", "stdout": interrupt.stdout})

        # --- Repairer agent simulation ---

        # Repairer reads context — policy node should be active
        repairer_ctx = run_wrapper(backend_root, "callstack", "context")
        ensure_ok(repairer_ctx, "repairer: callstack context")
        require(
            "repair" in repairer_ctx.stdout.lower(),
            "repairer context did not reflect repair policy node",
        )
        steps.append({"label": "repairer_context", "stdout": repairer_ctx.stdout})

        # Repairer records diagnosis observation
        repairer_obs_1 = run_wrapper(
            backend_root, "callstack", "observe",
            "--title", "diagnosis",
            "--text", "Root cause: simulated failure in smoke harness.",
            "--actor-id", "claude-team-repairer",
        )
        ensure_ok(repairer_obs_1, "repairer: observe diagnosis")
        steps.append({"label": "repairer_observe_diagnosis", "stdout": repairer_obs_1.stdout})

        # Repairer records fix observation
        repairer_obs_2 = run_wrapper(
            backend_root, "callstack", "observe",
            "--title", "fix",
            "--text", "Applied SmartList-first repair policy to resolve the simulated failure.",
            "--actor-id", "claude-team-repairer",
        )
        ensure_ok(repairer_obs_2, "repairer: observe fix")
        steps.append({"label": "repairer_observe_fix", "stdout": repairer_obs_2.stdout})

        # Repairer reads context again — observations should appear
        repairer_ctx_2 = run_wrapper(backend_root, "callstack", "context")
        ensure_ok(repairer_ctx_2, "repairer: callstack context after observations")
        require(
            "Root cause: simulated failure" in repairer_ctx_2.stdout,
            "repairer context did not include diagnosis observation",
        )
        require(
            "SmartList-first repair policy" in repairer_ctx_2.stdout,
            "repairer context did not include fix observation",
        )
        steps.append({"label": "repairer_context_with_obs", "stdout": repairer_ctx_2.stdout})

        # Repairer pops with REPAIRED: prefix
        policy_pop = run_wrapper(
            backend_root,
            "callstack",
            "pop",
            "--return-text",
            "REPAIRED: Applied SmartList-first repair for simulated failure.",
            "--actor-id",
            "claude-team-repairer",
        )
        ensure_ok(policy_pop, "callstack pop policy (repairer)")
        steps.append({"label": "pop_policy", "stdout": policy_pop.stdout})

        resume = run_wrapper(backend_root, "callstack", "resume", "--actor-id", args.actor_id)
        ensure_ok(resume, "callstack resume")
        steps.append({"label": "resume", "stdout": resume.stdout})

        work_pop = run_wrapper(
            backend_root,
            "callstack",
            "pop",
            "--return-text",
            "work complete",
            "--actor-id",
            args.actor_id,
        )
        ensure_ok(work_pop, "callstack pop work")
        steps.append({"label": "pop_work", "stdout": work_pop.stdout})

        root_pop = run_wrapper(
            backend_root,
            "callstack",
            "pop",
            "--return-text",
            "root complete",
            "--actor-id",
            args.actor_id,
        )
        ensure_ok(root_pop, "callstack pop root")
        steps.append({"label": "pop_root", "stdout": root_pop.stdout})

        show = run_wrapper(backend_root, "callstack", "show")
        ensure_ok(show, "callstack show")
        steps.append({"label": "show_final", "stdout": show.stdout})

        # --- Extended smoke tests for callstack context and observe ---

        # Push a fresh root to test context/observe commands
        ctx_root_push = run_wrapper(
            backend_root, "callstack", "push", "context-smoke-root", "--actor-id", args.actor_id,
        )
        ensure_ok(ctx_root_push, "callstack push context-smoke-root")
        ctx_root_data = parse_kv(ctx_root_push.stdout)
        ctx_root_path = ctx_root_data["node_path"]
        steps.append({"label": "ctx_push_root", "stdout": ctx_root_push.stdout})

        # Verify context returns frame with empty observations
        ctx_1 = run_wrapper(backend_root, "callstack", "context")
        ensure_ok(ctx_1, "callstack context (root only)")
        require(
            "[AMS Callstack Context]" in ctx_1.stdout,
            "callstack context did not emit header",
        )
        require(
            "context-smoke-root" in ctx_1.stdout,
            "callstack context did not include root frame title",
        )
        require(
            "Active observations:" not in ctx_1.stdout,
            "callstack context should have no observations yet",
        )
        steps.append({"label": "ctx_root_only", "stdout": ctx_1.stdout})

        # Observe on root
        obs_1 = run_wrapper(
            backend_root, "callstack", "observe",
            "--title", "smoke-obs-1",
            "--text", "first observation on root",
            "--actor-id", args.actor_id,
        )
        ensure_ok(obs_1, "callstack observe on root")
        steps.append({"label": "observe_root", "stdout": obs_1.stdout})

        # Verify context now includes the observation
        ctx_2 = run_wrapper(backend_root, "callstack", "context")
        ensure_ok(ctx_2, "callstack context (root with obs)")
        require(
            "first observation on root" in ctx_2.stdout,
            "callstack context did not include observation text",
        )
        steps.append({"label": "ctx_root_with_obs", "stdout": ctx_2.stdout})

        # Push child, observe, verify 2 frames in context
        ctx_child_push = run_wrapper(
            backend_root, "callstack", "push", "context-smoke-child", "--actor-id", args.actor_id,
        )
        ensure_ok(ctx_child_push, "callstack push context-smoke-child")
        steps.append({"label": "ctx_push_child", "stdout": ctx_child_push.stdout})

        obs_2 = run_wrapper(
            backend_root, "callstack", "observe",
            "--title", "smoke-obs-2",
            "--text", "child observation",
            "--actor-id", args.actor_id,
        )
        ensure_ok(obs_2, "callstack observe on child")
        steps.append({"label": "observe_child", "stdout": obs_2.stdout})

        ctx_3 = run_wrapper(backend_root, "callstack", "context")
        ensure_ok(ctx_3, "callstack context (child)")
        require(
            "context-smoke-root" in ctx_3.stdout and "context-smoke-child" in ctx_3.stdout,
            "callstack context did not show both frames",
        )
        require(
            "child observation" in ctx_3.stdout,
            "callstack context did not include child observation",
        )
        steps.append({"label": "ctx_child", "stdout": ctx_3.stdout})

        # Pop child, verify parent receipt appears
        ctx_child_pop = run_wrapper(
            backend_root, "callstack", "pop",
            "--return-text", "child completed successfully",
            "--actor-id", args.actor_id,
        )
        ensure_ok(ctx_child_pop, "callstack pop context-smoke-child")
        steps.append({"label": "ctx_pop_child", "stdout": ctx_child_pop.stdout})

        # Now active is root again; context should not have parent receipt (root has no parent)
        ctx_4 = run_wrapper(backend_root, "callstack", "context")
        ensure_ok(ctx_4, "callstack context (back to root after pop)")
        require(
            "context-smoke-root" in ctx_4.stdout,
            "callstack context did not return to root",
        )
        steps.append({"label": "ctx_after_child_pop", "stdout": ctx_4.stdout})

        # Interrupt from root (need child work node first)
        ctx_work_push = run_wrapper(
            backend_root, "callstack", "push", "context-interruptable", "--actor-id", args.actor_id,
        )
        ensure_ok(ctx_work_push, "callstack push context-interruptable")
        steps.append({"label": "ctx_push_interruptable", "stdout": ctx_work_push.stdout})

        ctx_interrupt = run_wrapper(
            backend_root, "callstack", "interrupt",
            "--policy", "repair",
            "--reason", "context-smoke-interrupt",
            "--error-output", "simulated",
            "--context", "context smoke",
            "--attempted-fix", "none",
            "--repair-hint", "test hint",
            "--actor-id", args.actor_id,
        )
        ensure_ok(ctx_interrupt, "callstack interrupt for context smoke")
        steps.append({"label": "ctx_interrupt", "stdout": ctx_interrupt.stdout})

        ctx_5 = run_wrapper(backend_root, "callstack", "context")
        ensure_ok(ctx_5, "callstack context (interrupt active)")
        require(
            "interrupt" in ctx_5.stdout.lower(),
            "callstack context did not reflect interrupt state",
        )
        steps.append({"label": "ctx_during_interrupt", "stdout": ctx_5.stdout})

        # Pop repair policy, resume, pop interruptable, pop root
        ctx_policy_pop = run_wrapper(
            backend_root, "callstack", "pop", "--return-text", "repair done", "--actor-id", args.actor_id,
        )
        ensure_ok(ctx_policy_pop, "callstack pop repair policy")

        ctx_resume = run_wrapper(backend_root, "callstack", "resume", "--actor-id", args.actor_id)
        ensure_ok(ctx_resume, "callstack resume after interrupt")

        ctx_6 = run_wrapper(backend_root, "callstack", "context")
        ensure_ok(ctx_6, "callstack context (after resume)")
        require(
            "context-interruptable" in ctx_6.stdout,
            "callstack context did not return to interrupted work after resume",
        )
        steps.append({"label": "ctx_after_resume", "stdout": ctx_6.stdout})

        # Clean up: pop remaining frames
        for label in ("pop_interruptable", "pop_ctx_root"):
            cleanup_pop = run_wrapper(
                backend_root, "callstack", "pop", "--return-text", f"{label} done", "--actor-id", args.actor_id,
            )
            ensure_ok(cleanup_pop, f"callstack {label}")
            steps.append({"label": label, "stdout": cleanup_pop.stdout})

        ctx_final = run_wrapper(backend_root, "callstack", "context")
        ensure_ok(ctx_final, "callstack context (final empty)")
        require(
            "[AMS Callstack Context]" not in ctx_final.stdout,
            "callstack context should be empty after popping all frames",
        )
        steps.append({"label": "ctx_final_empty", "stdout": ctx_final.stdout})

        # --- Dependency-aware dispatch smoke test (A6) ---
        # Push a parent with 3 children: A (no deps), B (depends on A), C (depends on B)
        dep_root_push = run_wrapper(
            backend_root, "callstack", "push", "dep-smoke-root", "--actor-id", args.actor_id,
        )
        ensure_ok(dep_root_push, "callstack push dep-smoke-root")
        dep_root_data = parse_kv(dep_root_push.stdout)
        dep_root_path = dep_root_data["node_path"]
        steps.append({"label": "dep_push_root", "stdout": dep_root_push.stdout})

        # Push child A (no dependencies)
        dep_a_push = run_wrapper(
            backend_root, "callstack", "push", "dep-node-A",
            "--description", "Independent node A",
            "--actor-id", args.actor_id,
        )
        ensure_ok(dep_a_push, "callstack push dep-node-A")
        dep_a_path = parse_kv(dep_a_push.stdout)["node_path"]
        steps.append({"label": "dep_push_A", "stdout": dep_a_push.stdout})

        # Pop A so we can push B as sibling
        dep_a_pop = run_wrapper(
            backend_root, "callstack", "pop", "--return-text", "A done", "--actor-id", args.actor_id,
        )
        ensure_ok(dep_a_pop, "callstack pop dep-node-A")

        # Advance back to root to push B
        dep_adv_1 = run_wrapper(backend_root, "callstack", "advance", "--actor-id", args.actor_id)
        ensure_ok(dep_adv_1, "callstack advance after A")

        # Push child B (depends on A)
        dep_b_push = run_wrapper(
            backend_root, "callstack", "push", "dep-node-B",
            "--description", "Depends on A",
            "--depends-on", "dep-node-A",
            "--actor-id", args.actor_id,
        )
        ensure_ok(dep_b_push, "callstack push dep-node-B")
        dep_b_path = parse_kv(dep_b_push.stdout)["node_path"]
        steps.append({"label": "dep_push_B", "stdout": dep_b_push.stdout})

        # Pop B
        dep_b_pop = run_wrapper(
            backend_root, "callstack", "pop", "--return-text", "B done", "--actor-id", args.actor_id,
        )
        ensure_ok(dep_b_pop, "callstack pop dep-node-B")

        # Advance to push C
        dep_adv_2 = run_wrapper(backend_root, "callstack", "advance", "--actor-id", args.actor_id)
        ensure_ok(dep_adv_2, "callstack advance after B")

        # Push child C (depends on B)
        dep_c_push = run_wrapper(
            backend_root, "callstack", "push", "dep-node-C",
            "--description", "Depends on B",
            "--depends-on", "dep-node-B",
            "--actor-id", args.actor_id,
        )
        ensure_ok(dep_c_push, "callstack push dep-node-C")
        dep_c_path = parse_kv(dep_c_push.stdout)["node_path"]
        steps.append({"label": "dep_push_C", "stdout": dep_c_push.stdout})

        # Verify depends_on was stored by reading the snapshot
        dep_snapshot_path = resolve_snapshot_path(corpus_path, backend_root)
        dep_snapshot = json.loads(dep_snapshot_path.read_text(encoding="utf-8"))
        dep_b_fields = bucket_fields(dep_snapshot, f"{dep_b_path}/00-node")
        dep_c_fields = bucket_fields(dep_snapshot, f"{dep_c_path}/00-node")
        require(
            dep_b_fields.get("depends_on") == "dep-node-A",
            f"dep-node-B depends_on should be 'dep-node-A', got '{dep_b_fields.get('depends_on')}'",
        )
        require(
            dep_c_fields.get("depends_on") == "dep-node-B",
            f"dep-node-C depends_on should be 'dep-node-B', got '{dep_c_fields.get('depends_on')}'",
        )
        steps.append({"label": "dep_verify_depends_on", "stdout": "depends_on fields verified"})

        # Clean up: pop C, pop root
        for label in ("pop_dep_C", "pop_dep_root"):
            cleanup = run_wrapper(
                backend_root, "callstack", "pop", "--return-text", f"{label} done", "--actor-id", args.actor_id,
            )
            ensure_ok(cleanup, f"callstack {label}")
            steps.append({"label": label, "stdout": cleanup.stdout})

        print("dep_test=ok")

        snapshot_path = resolve_snapshot_path(corpus_path, backend_root)
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        report["snapshot_path"] = str(snapshot_path)

        root_meta = bucket_fields(snapshot, f"{root_path}/00-node")
        interrupted_meta = bucket_fields(snapshot, f"{interrupted_node_path}/00-node")
        interrupt_meta = bucket_fields(snapshot, f"{interrupt_path}/00-node")
        policy_meta = bucket_fields(snapshot, f"{policy_path}/00-node")

        require(root_meta.get("active_node_path", "") == "", "root active_node_path was not cleared")
        require(interrupted_meta.get("state") == "completed", "interrupted node did not complete")
        require(interrupt_meta.get("state") == "archived", "interrupt did not archive")
        require(interrupt_meta.get("kind") == "interrupt", "interrupt node kind mismatch")
        require(interrupt_meta.get("policy_kind") == "repair", "interrupt policy_kind mismatch")
        require(policy_meta.get("state") == "completed", "policy node did not complete")
        require(policy_meta.get("kind") == "policy", "policy node kind mismatch")
        require(policy_meta.get("policy_kind") == "repair", "policy node policy_kind mismatch")
        require(
            show.stdout.strip() == "(empty call stack - no active SmartList runtime)",
            "final callstack show was not empty",
        )

        # Assert policy node has >= 2 observations from the repairer
        policy_obs_members = container_members(snapshot, f"smartlist-members:{policy_path}/20-observations")
        require(
            len(policy_obs_members) >= 2,
            f"policy node should have >= 2 observations, got {len(policy_obs_members)}",
        )

        archive_members = container_members(snapshot, f"smartlist-members:{root_path}/90-archive")
        child_members = container_members(snapshot, f"smartlist-members:{root_path}/10-children")
        require(bucket_object_id(interrupt_path) in archive_members, "archived interrupt missing from archive bucket")
        require(bucket_object_id(interrupt_path) not in child_members, "archived interrupt still linked in active children")

        report["steps"] = steps
        report["root_path"] = root_path
        report["interrupted_node_path"] = interrupted_node_path
        report["interrupt_path"] = interrupt_path
        report["policy_path"] = policy_path
        report["final_show"] = show.stdout

        report_path = workspace / "callstack-smartlist-report.json"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        print(f"workspace={workspace}")
        print(f"backend_root={backend_root}")
        print(f"snapshot_path={snapshot_path}")
        print(f"root_path={root_path}")
        print(f"interrupted_node_path={interrupted_node_path}")
        print(f"interrupt_path={interrupt_path}")
        print(f"policy_path={policy_path}")
        print(f"report={report_path}")
        print("result=ok")
        return 0
    finally:
        if temp_dir is not None and not args.keep_workspace:
            temp_dir.cleanup()


if __name__ == "__main__":
    sys.exit(main())
