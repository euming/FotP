#!/usr/bin/env python3
"""
test-fep-repair-loop.py

End-to-end smoke test for the FEP tool anomaly -> repair feedback loop.

Validates:
  1. Synthetic tool-call objects with known priors -> expected anomalies detected
  2. Normal calls (matching priors) -> no anomalies
  3. SmartList notes created with correct title format, provenance fields
  4. Repair interrupt triggered from anomaly
  5. Callstack has interrupt + policy node
  6. Repairer pop with "REPAIRED: ..." restores work

Usage:
    python scripts/test-fep-repair-loop.py [--workspace <dir>] [--keep-workspace]
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

sys.path.insert(0, str(SCRIPT_DIR))
from ams_common import build_rust_ams_cmd, repo_root, rust_backend_env

AMS_PY = REPO_ROOT / "scripts" / "ams.py"


# ---------------------------------------------------------------------------
# Helpers (same pattern as run-callstack-swarm.py)
# ---------------------------------------------------------------------------

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
            sys.executable, str(AMS_PY),
            "callstack", "--corpus", "all", "--backend-root", str(backend_root),
            *args[1:],
        ]
    else:
        cmd = [
            sys.executable, str(AMS_PY),
            *args, "--corpus", "all", "--backend-root", str(backend_root),
        ]
    return subprocess.run(cmd, cwd=REPO_ROOT, text=True, capture_output=True, check=False)


def run_rust_kernel(backend_root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    kernel_cmd = build_rust_ams_cmd()
    if kernel_cmd is None:
        raise RuntimeError("unable to locate the Rust AMS kernel binary or Cargo project")
    cmd = [*kernel_cmd, *args]
    return subprocess.run(
        cmd, cwd=REPO_ROOT, env=rust_backend_env(str(backend_root)),
        text=True, capture_output=True, check=False,
    )


def ensure_ok(result: subprocess.CompletedProcess[str], label: str) -> None:
    if result.returncode == 0:
        return
    raise RuntimeError(
        f"{label} failed with exit={result.returncode}\n"
        f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(f"ASSERTION FAILED: {message}")


def resolve_snapshot_path(corpus_path: Path, backend_root: Path) -> Path:
    kernel_cmd = build_rust_ams_cmd()
    if kernel_cmd is None:
        raise RuntimeError("unable to locate the Rust AMS kernel binary or Cargo project")
    result = subprocess.run(
        [*kernel_cmd, "backend-status", "--input", str(corpus_path)],
        cwd=REPO_ROOT, env=rust_backend_env(str(backend_root)),
        text=True, capture_output=True, check=False,
    )
    ensure_ok(result, "backend-status")
    data = parse_kv(result.stdout)
    snapshot_path = data.get("snapshot_path")
    if not snapshot_path:
        raise RuntimeError("backend-status did not report snapshot_path")
    return Path(snapshot_path)


def snapshot_indexes(snapshot: dict) -> tuple[dict, dict, dict]:
    objects = {obj["objectId"]: obj for obj in snapshot.get("objects", [])}
    containers = {c["containerId"]: c for c in snapshot.get("containers", [])}
    links = {ln["linkNodeId"]: ln for ln in snapshot.get("linkNodes", [])}
    return objects, containers, links


# ---------------------------------------------------------------------------
# Synthetic snapshot with tool-call objects
# ---------------------------------------------------------------------------

def make_tool_call_object(
    object_id: str,
    tool_name: str,
    is_error: bool,
    result_preview: str,
    created_at: str,
) -> dict:
    return {
        "objectId": object_id,
        "objectKind": "tool-call",
        "createdAt": created_at,
        "updatedAt": created_at,
        "semanticPayload": {
            "provenance": {
                "tool_name": tool_name,
                "is_error": is_error,
                "result_preview": result_preview,
            }
        },
    }


def build_synthetic_snapshot(past_iso: str, now_iso: str) -> dict:
    """Build a minimal AMS snapshot with synthetic tool-call objects.

    - 30 historical Bash successes (past) -> strong success prior
    - 10 historical Grep successes (past) -> moderate success prior
    - 2 recent Bash errors (now) -> should be anomalous
    - 1 recent Bash success (now) -> should NOT be anomalous
    - 1 recent Grep null (now) -> should be anomalous
    """
    objects = []

    # Historical successes for Bash
    for i in range(30):
        objects.append(make_tool_call_object(
            f"hist-bash-{i}", "Bash", False, "Compiled OK", past_iso,
        ))

    # Historical successes for Grep
    for i in range(10):
        objects.append(make_tool_call_object(
            f"hist-grep-{i}", "Grep", False, "src/main.rs:42:fn main()", past_iso,
        ))

    # Recent anomalous calls
    objects.append(make_tool_call_object(
        "recent-bash-err-1", "Bash", True, "exit code 1: compilation failed", now_iso,
    ))
    objects.append(make_tool_call_object(
        "recent-bash-err-2", "Bash", True, "segfault", now_iso,
    ))
    objects.append(make_tool_call_object(
        "recent-grep-null", "Grep", False, "No matches", now_iso,
    ))

    # Recent normal call
    objects.append(make_tool_call_object(
        "recent-bash-ok", "Bash", False, "Build succeeded", now_iso,
    ))

    return {
        "formatVersion": 2,
        "snapshotId": "fep-repair-loop-test",
        "createdAt": now_iso,
        "objects": objects,
        "containers": [],
        "linkNodes": [],
    }


# ---------------------------------------------------------------------------
# Main test flow
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="End-to-end smoke test for FEP tool anomaly repair loop."
    )
    parser.add_argument("--workspace", type=Path, help="Optional workspace dir.")
    parser.add_argument("--keep-workspace", action="store_true")
    args = parser.parse_args()

    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    if args.workspace is None:
        temp_dir = tempfile.TemporaryDirectory(prefix="ams-fep-repair-loop-")
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

    print(f"workspace={workspace}")
    print(f"backend_root={backend_root}")
    step = 0

    def log_step(label: str) -> None:
        nonlocal step
        step += 1
        print(f"\n--- Step {step}: {label} ---")

    try:
        # ---------------------------------------------------------------
        # Step 1: Create synthetic snapshot with tool-call objects
        # ---------------------------------------------------------------
        log_step("Create synthetic snapshot with tool-call objects")

        now = datetime.now(timezone.utc)
        past = now - timedelta(hours=48)
        since = now - timedelta(hours=1)
        now_iso = now.isoformat()
        past_iso = past.isoformat()
        since_iso = since.isoformat()

        snapshot_data = build_synthetic_snapshot(past_iso, now_iso)
        snapshot_file = workspace / "test-snapshot.ams.json"
        snapshot_file.write_text(json.dumps(snapshot_data, indent=2), encoding="utf-8")
        print(f"  Created snapshot with {len(snapshot_data['objects'])} objects")
        print(f"  Snapshot: {snapshot_file}")

        # ---------------------------------------------------------------
        # Step 2: Bootstrap agent-tool priors
        # ---------------------------------------------------------------
        log_step("Run fep-bootstrap-agent-tools")

        bootstrapped_file = workspace / "test-snapshot.bootstrapped.ams.json"
        result = run_rust_kernel(
            backend_root,
            "fep-bootstrap-agent-tools",
            "--input", str(snapshot_file),
            "--output", str(bootstrapped_file),
        )
        ensure_ok(result, "fep-bootstrap-agent-tools")
        print(result.stdout)

        # Verify priors were created
        require(bootstrapped_file.exists(), "bootstrapped snapshot file should exist")
        bs = json.loads(bootstrapped_file.read_text(encoding="utf-8"))
        bs_objects, bs_containers, _ = snapshot_indexes(bs)

        # The priors container should exist
        require(
            "fep:agent-tool-outcome-priors" in bs_containers,
            "agent-tool-outcome-priors container should exist after bootstrap"
        )
        priors_container = bs_containers["fep:agent-tool-outcome-priors"]
        hypothesis_state = priors_container.get("hypothesisState", {})
        require(
            any(k.startswith("fep:agent-tool:") for k in hypothesis_state),
            "bootstrap should have written fep:agent-tool:* keys"
        )
        print(f"  Priors container has {len(hypothesis_state)} keys")

        # Verify Bash and Grep have priors
        bash_key = next((k for k in hypothesis_state if "Bash" in k), None)
        grep_key = next((k for k in hypothesis_state if "Grep" in k), None)
        require(bash_key is not None, "Bash should have a prior")
        require(grep_key is not None, "Grep should have a prior")

        # ---------------------------------------------------------------
        # Step 3: Detect anomalies
        # ---------------------------------------------------------------
        log_step("Run fep-detect-tool-anomalies")

        detected_file = workspace / "test-snapshot.detected.ams.json"
        result = run_rust_kernel(
            backend_root,
            "fep-detect-tool-anomalies",
            "--input", str(bootstrapped_file),
            "--since", since_iso,
            "--output", str(detected_file),
        )
        ensure_ok(result, "fep-detect-tool-anomalies")
        print(result.stdout)

        # Verify anomalies were detected
        require("anomalies detected" in result.stdout, "should report anomalies detected")
        require(
            "recent-bash-ok" not in result.stdout,
            "normal Bash success should not appear in anomalies"
        )
        require(
            "recent-bash-err" in result.stdout,
            "Bash errors should appear in anomalies"
        )

        # ---------------------------------------------------------------
        # Step 4: Verify SmartList notes in output snapshot
        # ---------------------------------------------------------------
        log_step("Verify SmartList anomaly notes")

        require(detected_file.exists(), "detected output file should exist")
        ds = json.loads(detected_file.read_text(encoding="utf-8"))
        ds_objects, ds_containers, _ = snapshot_indexes(ds)

        # Find anomaly notes
        anomaly_notes = [
            obj for obj in ds_objects.values()
            if obj.get("objectKind") == "smartlist_note"
            and ((obj.get("semanticPayload") or {}).get("provenance") or {}).get("source") == "fep-anomaly-detector"
        ]
        require(len(anomaly_notes) > 0, "should have at least 1 anomaly SmartList note")
        print(f"  Found {len(anomaly_notes)} anomaly SmartList notes")

        # Verify note title format: "FEP anomaly: <tool> <outcome> (FE=<n>)"
        for note in anomaly_notes:
            prov = (note.get("semanticPayload") or {}).get("provenance") or {}
            title = prov.get("title", "")
            require(
                title.startswith("FEP anomaly:"),
                f"note title should start with 'FEP anomaly:', got: {title!r}"
            )
            require("(FE=" in title, f"note title should contain '(FE=', got: {title!r}")

            # Verify machine-parseable provenance fields
            require(prov.get("tool_name") is not None, "provenance.tool_name must exist")
            require(prov.get("tool_use_id") is not None, "provenance.tool_use_id must exist")
            require(prov.get("outcome") is not None, "provenance.outcome must exist")
            require(prov.get("free_energy") is not None, "provenance.free_energy must exist")
            require(prov.get("threshold") is not None, "provenance.threshold must exist")
            require(
                prov.get("source") == "fep-anomaly-detector",
                "provenance.source should be 'fep-anomaly-detector'"
            )
            print(f"  Note OK: {title}")

        # Verify the anomaly bucket container exists
        require(
            "smartlist-members:smartlist/fep-tool-anomalies" in ds_containers,
            "smartlist/fep-tool-anomalies bucket members container should exist"
        )

        # ---------------------------------------------------------------
        # Step 5: Set up callstack with root + work node
        # ---------------------------------------------------------------
        log_step("Set up callstack (root + work node)")

        root_push = run_wrapper(
            backend_root, "callstack", "push", "fep-repair-test-root",
            "--actor-id", "test-harness",
        )
        ensure_ok(root_push, "callstack push root")
        root_data = parse_kv(root_push.stdout)
        root_path = root_data["node_path"]
        print(f"  root_path={root_path}")

        work_push = run_wrapper(
            backend_root, "callstack", "push", "work-in-progress",
            "--description", "Work that will be interrupted by FEP repair.",
            "--actor-id", "test-harness",
        )
        ensure_ok(work_push, "callstack push work")
        work_data = parse_kv(work_push.stdout)
        work_path = work_data["node_path"]
        print(f"  work_path={work_path}")

        # ---------------------------------------------------------------
        # Step 6: Trigger repair interrupt (simulate fep-repair-trigger.py)
        # ---------------------------------------------------------------
        log_step("Trigger repair interrupt")

        # Pick the first anomaly note's provenance for the repair hint
        first_note_prov = (anomaly_notes[0].get("semanticPayload") or {}).get("provenance") or {}
        tool_name = first_note_prov.get("tool_name", "Bash")
        outcome = first_note_prov.get("outcome", "Error")
        fe_val = first_note_prov.get("free_energy", 0.0)

        repair_reason = (
            f"FEP anomaly: {tool_name} {outcome} (FE={fe_val:.2f})"
        )
        repair_hint = (
            f"Tool '{tool_name}' produced anomalous '{outcome}' outcomes. "
            f"Search memory for prior fixes: scripts\\ams.bat search \"{tool_name} error\""
        )

        interrupt = run_wrapper(
            backend_root,
            "callstack", "interrupt",
            "--policy", "repair",
            "--reason", repair_reason,
            "--error-output", f"FEP detected {len(anomaly_notes)} anomalous tool calls",
            "--context", f"tool={tool_name}, outcome={outcome}",
            "--attempted-fix", "none",
            "--repair-hint", repair_hint,
            "--actor-id", "fep-repair-trigger",
        )
        ensure_ok(interrupt, "callstack interrupt")
        interrupt_data = parse_kv(interrupt.stdout)
        interrupt_path = interrupt_data["interrupt_path"]
        policy_path = interrupt_data["policy_path"]
        print(f"  interrupt_path={interrupt_path}")
        print(f"  policy_path={policy_path}")

        # ---------------------------------------------------------------
        # Step 7: Verify callstack has interrupt + policy node
        # ---------------------------------------------------------------
        log_step("Verify callstack state after interrupt")

        ctx = run_wrapper(backend_root, "callstack", "context")
        ensure_ok(ctx, "callstack context")
        require(
            "repair" in ctx.stdout.lower(),
            "callstack context should reflect repair policy node"
        )
        require(
            "interrupt" in ctx.stdout.lower(),
            "callstack context should mention interrupt"
        )
        print("  Callstack context confirms interrupt + repair policy active")

        # ---------------------------------------------------------------
        # Step 8: Simulate repairer agent
        # ---------------------------------------------------------------
        log_step("Simulate repairer (observe + pop)")

        # Repairer records diagnosis
        obs_diag = run_wrapper(
            backend_root, "callstack", "observe",
            "--title", "diagnosis",
            "--text", f"Root cause: {tool_name} failures due to stale configuration.",
            "--actor-id", "claude-team-repairer",
        )
        ensure_ok(obs_diag, "repairer: observe diagnosis")

        # Repairer records fix
        obs_fix = run_wrapper(
            backend_root, "callstack", "observe",
            "--title", "fix",
            "--text", f"Updated {tool_name} configuration paths in CLAUDE.md.",
            "--actor-id", "claude-team-repairer",
        )
        ensure_ok(obs_fix, "repairer: observe fix")

        # Repairer pops with REPAIRED prefix
        policy_pop = run_wrapper(
            backend_root, "callstack", "pop",
            "--return-text", f"REPAIRED: Fixed stale {tool_name} configuration paths.",
            "--actor-id", "claude-team-repairer",
        )
        ensure_ok(policy_pop, "callstack pop policy (repairer)")
        print(f"  Repairer popped with: REPAIRED: Fixed stale {tool_name} configuration paths.")

        # ---------------------------------------------------------------
        # Step 9: Resume and verify work restored
        # ---------------------------------------------------------------
        log_step("Resume interrupted work")

        resume = run_wrapper(
            backend_root, "callstack", "resume", "--actor-id", "test-harness",
        )
        ensure_ok(resume, "callstack resume")

        ctx_after = run_wrapper(backend_root, "callstack", "context")
        ensure_ok(ctx_after, "callstack context after resume")
        require(
            "work-in-progress" in ctx_after.stdout,
            "callstack should return to 'work-in-progress' after resume"
        )
        print("  Work node restored after resume")

        # ---------------------------------------------------------------
        # Step 10: Clean up callstack
        # ---------------------------------------------------------------
        log_step("Clean up callstack")

        work_pop = run_wrapper(
            backend_root, "callstack", "pop",
            "--return-text", "work complete", "--actor-id", "test-harness",
        )
        ensure_ok(work_pop, "callstack pop work")

        root_pop = run_wrapper(
            backend_root, "callstack", "pop",
            "--return-text", "root complete", "--actor-id", "test-harness",
        )
        ensure_ok(root_pop, "callstack pop root")

        final_show = run_wrapper(backend_root, "callstack", "show")
        ensure_ok(final_show, "callstack show (final)")
        print("  Callstack fully unwound")

        # ---------------------------------------------------------------
        # Step 11: Verify final snapshot state
        # ---------------------------------------------------------------
        log_step("Verify final snapshot state")

        final_snapshot_path = resolve_snapshot_path(corpus_path, backend_root)
        final_snap = json.loads(final_snapshot_path.read_text(encoding="utf-8"))
        _, final_containers, _ = snapshot_indexes(final_snap)

        # The interrupt node should be archived
        def bucket_fields(snap: dict, path: str) -> dict:
            objects, _, _ = snapshot_indexes(snap)
            obj = objects.get(f"smartlist-bucket:{path}")
            if obj is None:
                return {}
            return (obj.get("semanticPayload") or {}).get("provenance") or {}

        interrupt_meta = bucket_fields(final_snap, f"{interrupt_path}/00-node")
        policy_meta = bucket_fields(final_snap, f"{policy_path}/00-node")

        require(
            interrupt_meta.get("state") == "archived",
            f"interrupt node should be archived, got: {interrupt_meta.get('state')}"
        )
        require(
            interrupt_meta.get("kind") == "interrupt",
            f"interrupt kind mismatch: {interrupt_meta.get('kind')}"
        )
        require(
            policy_meta.get("state") == "completed",
            f"policy node should be completed, got: {policy_meta.get('state')}"
        )
        require(
            policy_meta.get("kind") == "policy",
            f"policy kind mismatch: {policy_meta.get('kind')}"
        )
        print("  Interrupt archived, policy completed")

        # ---------------------------------------------------------------
        # Summary
        # ---------------------------------------------------------------
        print("\n" + "=" * 60)
        print("ALL CHECKS PASSED")
        print("=" * 60)
        print(f"  workspace:       {workspace}")
        print(f"  tool-call objs:  {len(snapshot_data['objects'])}")
        print(f"  anomaly notes:   {len(anomaly_notes)}")
        print(f"  interrupt_path:  {interrupt_path}")
        print(f"  policy_path:     {policy_path}")
        print("result=ok")
        return 0

    except RuntimeError as e:
        print(f"\nFAILED: {e}", file=sys.stderr)
        print(f"workspace={workspace}", file=sys.stderr)
        return 1
    finally:
        if temp_dir is not None and not args.keep_workspace:
            temp_dir.cleanup()


if __name__ == "__main__":
    sys.exit(main())
