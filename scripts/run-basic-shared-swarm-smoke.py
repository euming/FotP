#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ams_common import build_rust_ams_cmd, repo_root, rust_backend_env


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(repo_root())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="scripts\\run-basic-shared-swarm-smoke.py",
        description=(
            "Run the simplest shared-backend swarm smoke: 1 orchestrator seeds a root plus 2 worker tangents, "
            "then 2 workers claim/heartbeat/release those tangents concurrently against one backend root."
        ),
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        help="Optional workspace directory for the temporary corpus and backend. Defaults to a temp directory.",
    )
    parser.add_argument(
        "--keep-workspace",
        action="store_true",
        help="Keep the generated workspace directory instead of deleting it on success.",
    )
    parser.add_argument(
        "--heartbeat-delay-seconds",
        type=float,
        default=0.25,
        help="Delay between claim and heartbeat for each worker.",
    )
    return parser.parse_args()


def resolve_kernel_cmd() -> list[str]:
    cmd = build_rust_ams_cmd()
    if cmd is None:
        raise RuntimeError("unable to locate the Rust AMS kernel binary or Cargo project")
    return cmd


def run_kernel(
    kernel_cmd: list[str],
    backend_root: Path,
    *args: str,
    cwd: Path,
) -> subprocess.CompletedProcess[str]:
    cmd = [*kernel_cmd, *args]
    return subprocess.run(
        cmd,
        cwd=cwd,
        env=rust_backend_env(str(backend_root)),
        text=True,
        capture_output=True,
        check=False,
    )


def ensure_ok(result: subprocess.CompletedProcess[str], label: str) -> None:
    if result.returncode == 0:
        return
    raise RuntimeError(
        f"{label} failed with exit={result.returncode}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )


def parse_kv(stdout: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def worker_flow(
    kernel_cmd: list[str],
    backend_root: Path,
    input_path: Path,
    thread_id: str,
    agent_id: str,
    heartbeat_delay_seconds: float,
) -> dict[str, object]:
    claim_token = f"{agent_id}-claim"
    claim = run_kernel(
        kernel_cmd,
        backend_root,
        "thread-claim",
        "--input",
        str(input_path),
        "--id",
        thread_id,
        "--agent",
        agent_id,
        "--lease-seconds",
        "300",
        "--claim-token",
        claim_token,
        cwd=REPO_ROOT,
    )
    ensure_ok(claim, f"{agent_id} claim")
    time.sleep(heartbeat_delay_seconds)
    heartbeat = run_kernel(
        kernel_cmd,
        backend_root,
        "thread-heartbeat",
        "--input",
        str(input_path),
        "--id",
        thread_id,
        "--agent",
        agent_id,
        "--claim-token",
        claim_token,
        "--lease-seconds",
        "300",
        cwd=REPO_ROOT,
    )
    ensure_ok(heartbeat, f"{agent_id} heartbeat")
    release = run_kernel(
        kernel_cmd,
        backend_root,
        "thread-release",
        "--input",
        str(input_path),
        "--id",
        thread_id,
        "--agent",
        agent_id,
        "--claim-token",
        claim_token,
        "--reason",
        "smoke-complete",
        cwd=REPO_ROOT,
    )
    ensure_ok(release, f"{agent_id} release")
    return {
        "agent_id": agent_id,
        "thread_id": thread_id,
        "claim": parse_kv(claim.stdout),
        "heartbeat": parse_kv(heartbeat.stdout),
        "release": parse_kv(release.stdout),
    }


def main() -> int:
    args = parse_args()
    kernel_cmd = resolve_kernel_cmd()

    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    if args.workspace is None:
        temp_dir = tempfile.TemporaryDirectory(prefix="ams-basic-shared-swarm-")
        workspace = Path(temp_dir.name)
    else:
        workspace = args.workspace.resolve()
        workspace.mkdir(parents=True, exist_ok=True)

    backend_root = workspace / "backend"
    corpus_dir = workspace / "all-agents-sessions"
    backend_root.mkdir(parents=True, exist_ok=True)
    corpus_dir.mkdir(parents=True, exist_ok=True)
    input_path = corpus_dir / "all-agents-sessions.memory.jsonl"
    input_path.write_text("", encoding="utf-8")

    report: dict[str, object] = {
        "workspace": str(workspace),
        "backend_root": str(backend_root),
        "input_path": str(input_path),
        "topology": {
            "orchestrator": "orchestrator-agent",
            "workers": ["worker-a", "worker-b"],
            "note": (
                "This harness uses concurrent claim/heartbeat/release on separate tangent threads. "
                "It does not attempt simultaneous multi-active-thread execution because the current task graph "
                "still models a single active_thread lane."
            ),
        },
    }

    try:
        initial_backend = run_kernel(kernel_cmd, backend_root, "backend-status", "--input", str(input_path), cwd=REPO_ROOT)
        ensure_ok(initial_backend, "initial backend-status")
        initial_recovery = run_kernel(
            kernel_cmd,
            backend_root,
            "backend-recover-validate",
            "--input",
            str(input_path),
            "--assert-clean",
            cwd=REPO_ROOT,
        )
        ensure_ok(initial_recovery, "initial backend-recover-validate")

        orchestrator_steps: list[dict[str, object]] = []
        for step_args, label in [
            (
                [
                    "thread-start",
                    "--input",
                    str(input_path),
                    "--title",
                    "Orchestrator root",
                    "--current-step",
                    "Seed worker tangents",
                    "--next-command",
                    "thread-push-tangent",
                    "--id",
                    "orchestrator-root",
                    "--actor-id",
                    "orchestrator-agent",
                ],
                "start_root",
            ),
            (
                [
                    "thread-push-tangent",
                    "--input",
                    str(input_path),
                    "--title",
                    "Worker A tangent",
                    "--current-step",
                    "Wait for claim",
                    "--next-command",
                    "thread-claim --agent worker-a",
                    "--id",
                    "worker-a-thread",
                    "--actor-id",
                    "orchestrator-agent",
                ],
                "push_worker_a",
            ),
            (
                [
                    "thread-pop",
                    "--input",
                    str(input_path),
                    "--actor-id",
                    "orchestrator-agent",
                ],
                "pop_after_worker_a",
            ),
            (
                [
                    "thread-push-tangent",
                    "--input",
                    str(input_path),
                    "--title",
                    "Worker B tangent",
                    "--current-step",
                    "Wait for claim",
                    "--next-command",
                    "thread-claim --agent worker-b",
                    "--id",
                    "worker-b-thread",
                    "--actor-id",
                    "orchestrator-agent",
                ],
                "push_worker_b",
            ),
            (
                [
                    "thread-pop",
                    "--input",
                    str(input_path),
                    "--actor-id",
                    "orchestrator-agent",
                ],
                "pop_after_worker_b",
            ),
        ]:
            result = run_kernel(kernel_cmd, backend_root, *step_args, cwd=REPO_ROOT)
            ensure_ok(result, label)
            orchestrator_steps.append({"label": label, "result": parse_kv(result.stdout)})

        report["orchestrator_steps"] = orchestrator_steps

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    worker_flow,
                    kernel_cmd,
                    backend_root,
                    input_path,
                    "worker-a-thread",
                    "worker-a",
                    args.heartbeat_delay_seconds,
                ),
                executor.submit(
                    worker_flow,
                    kernel_cmd,
                    backend_root,
                    input_path,
                    "worker-b-thread",
                    "worker-b",
                    args.heartbeat_delay_seconds,
                ),
            ]
            worker_reports = [future.result() for future in futures]

        report["workers"] = worker_reports

        thread_status = run_kernel(kernel_cmd, backend_root, "thread-status", "--input", str(input_path), cwd=REPO_ROOT)
        ensure_ok(thread_status, "thread-status")
        thread_list = run_kernel(kernel_cmd, backend_root, "thread-list", "--input", str(input_path), cwd=REPO_ROOT)
        ensure_ok(thread_list, "thread-list")
        backend_status = run_kernel(kernel_cmd, backend_root, "backend-status", "--input", str(input_path), cwd=REPO_ROOT)
        ensure_ok(backend_status, "final backend-status")
        recovery = run_kernel(
            kernel_cmd,
            backend_root,
            "backend-recover-validate",
            "--input",
            str(input_path),
            "--assert-clean",
            cwd=REPO_ROOT,
        )
        ensure_ok(recovery, "final backend-recover-validate")

        report["final_thread_status"] = thread_status.stdout
        report["final_thread_list"] = thread_list.stdout
        report["final_backend_status"] = parse_kv(backend_status.stdout)
        report["final_recovery"] = parse_kv(recovery.stdout)

        report_path = workspace / "basic-shared-swarm-report.json"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        print(f"workspace={workspace}")
        print(f"backend_root={backend_root}")
        print("topology=1 orchestrator + 2 workers")
        print("result=ok")
        print(f"report={report_path}")
        print("final_active_thread=orchestrator-root")
        print("worker_threads=worker-a-thread,worker-b-thread")
        print("note=workers operated concurrently via shared claim/lease flow on separate tangent threads")
        return 0
    finally:
        if temp_dir is not None and not args.keep_workspace:
            temp_dir.cleanup()


if __name__ == "__main__":
    sys.exit(main())
