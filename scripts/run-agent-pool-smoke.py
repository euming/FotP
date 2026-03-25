#!/usr/bin/env python3
"""Smoke test for the agent pool (Phase 2 of LLM Swarm Computer)."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

from ams_common import build_rust_ams_cmd, repo_root, rust_backend_env

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(repo_root())
AMS_PY = REPO_ROOT / "scripts" / "ams.py"

sys.path.insert(0, str(SCRIPT_DIR))
from swarm.registry import bootstrap_agent_pool


def parse_kv(stdout: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def run_kernel(backend_root: str, *args: str) -> subprocess.CompletedProcess[str]:
    cmd = build_rust_ams_cmd(*args)
    if cmd is None:
        raise RuntimeError("unable to locate the Rust AMS kernel binary or Cargo project")
    return subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        env=rust_backend_env(backend_root),
        text=True,
        capture_output=True,
        check=False,
    )


def run_wrapper(backend_root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    cmd = [
        sys.executable,
        str(AMS_PY),
        "callstack",
        "--corpus", "all",
        "--backend-root", str(backend_root),
        *args,
    ]
    return subprocess.run(cmd, cwd=REPO_ROOT, text=True, capture_output=True, check=False)


def ensure_ok(result: subprocess.CompletedProcess[str], label: str) -> None:
    if result.returncode == 0:
        return
    raise RuntimeError(
        f"{label} failed with exit={result.returncode}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main() -> int:
    temp_dir = tempfile.TemporaryDirectory(prefix="ams-agent-pool-smoke-")
    workspace = Path(temp_dir.name)

    backend_root = workspace / "backend"
    corpus_dir = workspace / "all-agents-sessions"
    backend_root.mkdir(parents=True, exist_ok=True)
    corpus_dir.mkdir(parents=True, exist_ok=True)
    corpus_path = corpus_dir / "all-agents-sessions.memory.jsonl"
    corpus_path.write_text("", encoding="utf-8")

    input_path = str(corpus_path)
    br = str(backend_root)

    print("=== Step 1: Bootstrap pool (8 slots) ===")
    agent_ids = bootstrap_agent_pool(input_path, pool_size=8, backend_root=br)
    require(len(agent_ids) == 8, f"expected 8 agent IDs, got {len(agent_ids)}")
    print(f"  created {len(agent_ids)} agent slots: {agent_ids}")

    print("\n=== Step 2: Check status — expect 8 free, 0 allocated ===")
    result = run_kernel(br, "agent-pool-status", "--input", input_path)
    ensure_ok(result, "agent-pool-status")
    data = parse_kv(result.stdout)
    print(f"  {result.stdout.strip()}")
    require(data.get("free_count") == "8", f"expected free_count=8, got {data.get('free_count')}")
    require(data.get("allocated_count") == "0", f"expected allocated_count=0, got {data.get('allocated_count')}")

    slot0 = agent_ids[0]
    slot1 = agent_ids[1]

    print(f"\n=== Step 3: Allocate {slot0} to smartlist/test/task-a ===")
    result = run_kernel(br, "agent-pool-allocate", "--input", input_path, "--agent-ref", slot0, "--task-path", "smartlist/test/task-a")
    ensure_ok(result, "agent-pool-allocate slot0")
    print(f"  {result.stdout.strip()}")

    result = run_kernel(br, "agent-pool-status", "--input", input_path)
    ensure_ok(result, "agent-pool-status after allocate 0")
    data = parse_kv(result.stdout)
    require(data.get("free_count") == "7", f"expected free_count=7, got {data.get('free_count')}")
    require(data.get("allocated_count") == "1", f"expected allocated_count=1, got {data.get('allocated_count')}")

    print(f"\n=== Step 4: Allocate {slot1} to smartlist/test/task-b ===")
    result = run_kernel(br, "agent-pool-allocate", "--input", input_path, "--agent-ref", slot1, "--task-path", "smartlist/test/task-b")
    ensure_ok(result, "agent-pool-allocate slot1")
    print(f"  {result.stdout.strip()}")

    result = run_kernel(br, "agent-pool-status", "--input", input_path)
    ensure_ok(result, "agent-pool-status after allocate 1")
    data = parse_kv(result.stdout)
    require(data.get("free_count") == "6", f"expected free_count=6, got {data.get('free_count')}")
    require(data.get("allocated_count") == "2", f"expected allocated_count=2, got {data.get('allocated_count')}")

    print(f"\n=== Step 5: Release {slot0} from task-a ===")
    result = run_kernel(br, "agent-pool-release", "--input", input_path, "--agent-ref", slot0, "--task-path", "smartlist/test/task-a")
    ensure_ok(result, "agent-pool-release slot0")
    print(f"  {result.stdout.strip()}")

    result = run_kernel(br, "agent-pool-status", "--input", input_path)
    ensure_ok(result, "agent-pool-status after release 0")
    data = parse_kv(result.stdout)
    require(data.get("free_count") == "7", f"expected free_count=7, got {data.get('free_count')}")
    require(data.get("allocated_count") == "1", f"expected allocated_count=1, got {data.get('allocated_count')}")

    print(f"\n=== Step 6: Re-allocate {slot0} (should succeed, it's free) ===")
    result = run_kernel(br, "agent-pool-allocate", "--input", input_path, "--agent-ref", slot0, "--task-path", "smartlist/test/task-c")
    ensure_ok(result, "agent-pool-allocate slot0 again")
    print(f"  {result.stdout.strip()}")

    print(f"\n=== Step 7: Try allocating {slot1} (already allocated — expect error) ===")
    result = run_kernel(br, "agent-pool-allocate", "--input", input_path, "--agent-ref", slot1, "--task-path", "smartlist/test/task-d")
    require(result.returncode != 0, f"expected error allocating already-allocated agent, but got rc=0")
    print(f"  correctly failed: {result.stderr.strip()[:120]}")

    print(f"\n=== Step 8: Release all and verify 8 free ===")
    result = run_kernel(br, "agent-pool-release", "--input", input_path, "--agent-ref", slot0, "--task-path", "smartlist/test/task-c")
    ensure_ok(result, "agent-pool-release slot0 from task-c")
    result = run_kernel(br, "agent-pool-release", "--input", input_path, "--agent-ref", slot1, "--task-path", "smartlist/test/task-b")
    ensure_ok(result, "agent-pool-release slot1 from task-b")

    result = run_kernel(br, "agent-pool-status", "--input", input_path)
    ensure_ok(result, "agent-pool-status final")
    data = parse_kv(result.stdout)
    require(data.get("free_count") == "8", f"expected free_count=8, got {data.get('free_count')}")
    require(data.get("allocated_count") == "0", f"expected allocated_count=0, got {data.get('allocated_count')}")

    print("\nresult=ok")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\nFAILED: {e}", file=sys.stderr)
        sys.exit(1)
