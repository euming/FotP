#!/usr/bin/env python3
"""End-to-end smoke test for the full Phase 3 LLM Swarm Computer runtime stack.

Bootstraps agent pool + artifact store + message queue, assigns home nodes,
produces LOD summaries, sends messages between agents, and stores/retrieves
artifacts.  Validates the integrated runtime by exercising every subsystem
in a single workspace.
"""
from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from ams_common import build_rust_ams_cmd, rust_backend_env

from swarm.registry import bootstrap_agent_pool
from swarm.locality import bootstrap_locality, assign_home_node, get_home_node, read_neighborhood
from swarm.lod import summarize_subtree, inject_context
from swarm.artifacts import bootstrap_artifact_store, store_artifact, list_artifacts, read_artifact
from swarm.messaging import (
    bootstrap_message_queue, send_message, receive_messages,
    send_to_inbox, read_inbox, broadcast,
)


def run_kernel(backend_root: str, *args: str) -> subprocess.CompletedProcess[str]:
    cmd = build_rust_ams_cmd(*args)
    if cmd is None:
        raise RuntimeError("unable to locate the Rust AMS kernel binary or Cargo project")
    return subprocess.run(
        cmd, env=rust_backend_env(backend_root), text=True, capture_output=True, check=False,
    )


def parse_kv(stdout: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def ensure_ok(result: subprocess.CompletedProcess[str], label: str) -> None:
    if result.returncode != 0:
        raise RuntimeError(f"{label} failed: rc={result.returncode}\n{result.stderr}")


def main() -> int:
    temp_dir = tempfile.TemporaryDirectory(prefix="ams-phase3-smoke-")
    workspace = Path(temp_dir.name)

    backend_root = workspace / "backend"
    corpus_dir = workspace / "all-agents-sessions"
    backend_root.mkdir(parents=True, exist_ok=True)
    corpus_dir.mkdir(parents=True, exist_ok=True)
    corpus_path = corpus_dir / "all-agents-sessions.memory.jsonl"
    corpus_path.write_text("", encoding="utf-8")

    ip = str(corpus_path)
    br = str(backend_root)

    # =========================================================================
    # Phase 2 prerequisite: Agent Pool
    # =========================================================================
    print("=== Phase 2: Bootstrap agent pool (4 slots) ===")
    agent_ids = bootstrap_agent_pool(ip, pool_size=4, backend_root=br)
    require(len(agent_ids) == 4, f"expected 4 agent IDs, got {len(agent_ids)}")
    print(f"  pool created: {agent_ids}")

    # Allocate two agents to tasks
    a0, a1 = agent_ids[0], agent_ids[1]
    result = run_kernel(br, "agent-pool-allocate", "--input", ip, "--agent-ref", a0, "--task-path", "smartlist/project/alpha")
    ensure_ok(result, "allocate agent-0")
    result = run_kernel(br, "agent-pool-allocate", "--input", ip, "--agent-ref", a1, "--task-path", "smartlist/project/beta")
    ensure_ok(result, "allocate agent-1")

    result = run_kernel(br, "agent-pool-status", "--input", ip)
    ensure_ok(result, "agent-pool-status")
    data = parse_kv(result.stdout)
    require(data.get("allocated_count") == "2", f"expected 2 allocated, got {data.get('allocated_count')}")
    print(f"  allocated 2 agents: {a0}, {a1}")

    # =========================================================================
    # Setup: Create SmartList bucket tree for locality/LOD tests
    # =========================================================================
    print("\n=== Setup: Create SmartList bucket tree ===")
    for path in [
        "smartlist/project/alpha",
        "smartlist/project/alpha/sub1",
        "smartlist/project/alpha/sub2",
        "smartlist/project/beta",
        "smartlist/project/beta/sub1",
    ]:
        result = run_kernel(br, "smartlist-create", "--input", ip, "--path", path)
        ensure_ok(result, f"create {path}")
    print("  bucket tree created")

    # =========================================================================
    # 3a: Locality — home nodes + neighborhoods
    # =========================================================================
    print("\n=== 3a: Bootstrap locality + assign home nodes ===")
    bootstrap_locality(ip, backend_root=br)
    nid0 = assign_home_node(ip, a0, "smartlist/project/alpha", backend_root=br)
    nid1 = assign_home_node(ip, a1, "smartlist/project/beta", backend_root=br)
    require(len(nid0) > 0, "expected non-empty home node for agent-0")
    require(len(nid1) > 0, "expected non-empty home node for agent-1")
    print(f"  {a0} -> smartlist/project/alpha ({nid0})")
    print(f"  {a1} -> smartlist/project/beta  ({nid1})")

    # Verify get_home_node round-trip
    home0 = get_home_node(ip, a0, backend_root=br)
    require(home0 == "smartlist/project/alpha", f"expected alpha home, got {home0}")

    # Read neighborhood
    hood = read_neighborhood(ip, "smartlist/project/alpha", backend_root=br)
    require(hood["node"] == "smartlist/project/alpha", "wrong neighborhood node")
    require(hood["parent"] == "smartlist/project", "wrong parent")
    print(f"  neighborhood: parent={hood['parent']}, children={hood['children']}")

    # =========================================================================
    # 3b: LOD — subtree summaries + context injection
    # =========================================================================
    print("\n=== 3b: LOD summaries + context injection ===")
    summary = summarize_subtree(ip, "smartlist/project", max_depth=2, backend_root=br)
    require(len(summary) > 0, "expected non-empty LOD summary")
    print(f"  summary ({len(summary)} chars): {summary[:120]}...")

    ctx = inject_context(
        ip, a0, "smartlist/project/alpha",
        sibling_paths=["smartlist/project/beta"],
        backend_root=br,
    )
    require("Local Neighborhood" in ctx, "missing Local Neighborhood section")
    require("LOD summary" in ctx, "missing LOD summary section")
    print(f"  context block ({len(ctx)} chars)")

    # =========================================================================
    # 3c: Artifact Store — store + list + read
    # =========================================================================
    print("\n=== 3c: Artifact store ===")
    bootstrap_artifact_store(ip, "results", backend_root=br)

    art1 = store_artifact(ip, "results", "test-output-1", "All tests pass.", a0,
                          "smartlist/project/alpha", backend_root=br)
    art2 = store_artifact(ip, "results", "test-output-2", "Coverage 95%.", a1,
                          "smartlist/project/beta", backend_root=br)
    require(len(art1) > 0, "artifact 1 empty")
    require(len(art2) > 0, "artifact 2 empty")

    refs = list_artifacts(ip, "results", backend_root=br)
    require(len(refs) >= 2, f"expected >=2 artifacts, got {len(refs)}")
    print(f"  stored {len(refs)} artifacts")

    body = read_artifact(ip, art1, backend_root=br)
    require("All tests pass" in str(body), "artifact content mismatch")
    print(f"  read artifact: {body}")

    # =========================================================================
    # 3d: Messaging — channels, inboxes, broadcast
    # =========================================================================
    print("\n=== 3d: Message queue + inboxes + broadcast ===")
    bootstrap_message_queue(ip, "swarm-bus", backend_root=br)

    # Agent-to-agent on channel
    m1 = send_message(ip, "swarm-bus", a0, a1, "hello from agent-0",
                      backend_root=br, timestamp="2026-03-18T13:00:00Z")
    require(len(m1) > 0, "channel message empty")
    print(f"  channel msg: {m1}")

    # Direct inbox
    m2 = send_to_inbox(ip, a0, a1, "task-update", "subtask done",
                       backend_root=br, timestamp="2026-03-18T13:01:00Z")
    require(len(m2) > 0, "inbox message empty")

    inbox = read_inbox(ip, a1, backend_root=br)
    require(len(inbox) >= 1, f"expected >=1 inbox messages, got {len(inbox)}")
    print(f"  {a1} inbox: {len(inbox)} messages")

    # Broadcast
    bids = broadcast(ip, "orchestrator", [a0, a1], "sync", "checkpoint reached",
                     backend_root=br)
    require(len(bids) == 2, f"expected 2 broadcast ids, got {len(bids)}")
    print(f"  broadcast to 2 agents")

    # System channel (orchestrator)
    bootstrap_message_queue(ip, "system", backend_root=br)
    sm = send_message(ip, "system", "orchestrator", "*", "phase3-validated",
                      backend_root=br, timestamp="2026-03-18T14:00:00Z")
    require(len(sm) > 0, "system channel message empty")
    print(f"  system channel broadcast: {sm}")

    # Worker status back to orchestrator
    su = send_to_inbox(ip, a0, "orchestrator", "status", "all-green", backend_root=br)
    require(len(su) > 0, "worker status update empty")
    orch_inbox = read_inbox(ip, "orchestrator", backend_root=br)
    require(len(orch_inbox) >= 1, "expected orchestrator inbox messages")
    print(f"  orchestrator inbox: {len(orch_inbox)} msgs")

    # =========================================================================
    # Cleanup: release agents back to pool
    # =========================================================================
    print("\n=== Cleanup: Release agents ===")
    result = run_kernel(br, "agent-pool-release", "--input", ip, "--agent-ref", a0, "--task-path", "smartlist/project/alpha")
    ensure_ok(result, "release agent-0")
    result = run_kernel(br, "agent-pool-release", "--input", ip, "--agent-ref", a1, "--task-path", "smartlist/project/beta")
    ensure_ok(result, "release agent-1")

    result = run_kernel(br, "agent-pool-status", "--input", ip)
    ensure_ok(result, "agent-pool-status final")
    data = parse_kv(result.stdout)
    require(data.get("allocated_count") == "0", f"expected 0 allocated after release, got {data.get('allocated_count')}")
    print(f"  all agents released back to pool")

    print("\n=== Phase 3 full stack validated ===")
    print("result=ok")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\nFAILED: {e}", file=sys.stderr)
        import traceback; traceback.print_exc()
        sys.exit(1)
