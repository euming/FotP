#!/usr/bin/env python3
"""Smoke test for Phase 3 swarm runtime: locality, LOD, artifacts, messaging."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from ams_common import build_rust_ams_cmd, rust_backend_env

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


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def ensure_ok(result: subprocess.CompletedProcess[str], label: str) -> None:
    if result.returncode != 0:
        raise RuntimeError(f"{label} failed: rc={result.returncode}\n{result.stderr}")


def main() -> int:
    temp_dir = tempfile.TemporaryDirectory(prefix="ams-swarm-runtime-smoke-")
    workspace = Path(temp_dir.name)

    backend_root = workspace / "backend"
    corpus_dir = workspace / "all-agents-sessions"
    backend_root.mkdir(parents=True, exist_ok=True)
    corpus_dir.mkdir(parents=True, exist_ok=True)
    corpus_path = corpus_dir / "all-agents-sessions.memory.jsonl"
    corpus_path.write_text("", encoding="utf-8")

    ip = str(corpus_path)
    br = str(backend_root)

    # --- Create some SmartList structure for LOD/neighborhood tests ---
    print("=== Setup: Create SmartList bucket tree ===")
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

    # --- 3a: Locality ---
    print("\n=== 3a1: Bootstrap locality + assign home nodes ===")
    bootstrap_locality(ip, backend_root=br)
    nid1 = assign_home_node(ip, "agent-0", "smartlist/project/alpha", backend_root=br)
    nid2 = assign_home_node(ip, "agent-1", "smartlist/project/beta", backend_root=br)
    require(len(nid1) > 0, "expected non-empty note_id for agent-0 home")
    require(len(nid2) > 0, "expected non-empty note_id for agent-1 home")
    print(f"  agent-0 -> smartlist/project/alpha ({nid1})")
    print(f"  agent-1 -> smartlist/project/beta ({nid2})")

    print("\n=== 3a2: Read neighborhood ===")
    hood = read_neighborhood(ip, "smartlist/project/alpha", depth=1, backend_root=br)
    require(hood["node"] == "smartlist/project/alpha", "wrong node")
    require(hood["parent"] == "smartlist/project", "wrong parent")
    print(f"  neighborhood: parent={hood['parent']}, children={hood['children']}")

    # --- 3b: LOD ---
    print("\n=== 3b1: Summarize subtree ===")
    summary = summarize_subtree(ip, "smartlist/project", max_depth=2, backend_root=br)
    require(len(summary) > 0, "expected non-empty summary")
    print(f"  summary:\n{summary}")

    print("\n=== 3b2: Inject context ===")
    ctx = inject_context(
        ip, "agent-0", "smartlist/project/alpha",
        sibling_paths=["smartlist/project/beta"],
        backend_root=br,
    )
    require("Local Neighborhood" in ctx, "missing local section")
    require("LOD summary" in ctx, "missing LOD section")
    print(f"  context block ({len(ctx)} chars):\n{ctx[:300]}...")

    # --- 3c: Artifacts ---
    print("\n=== 3c1+3c2: Artifact store ===")
    bootstrap_artifact_store(ip, "results", backend_root=br)
    a1 = store_artifact(ip, "results", "test-output-1", "All tests pass.", "agent-0",
                         "smartlist/project/alpha", backend_root=br)
    a2 = store_artifact(ip, "results", "test-output-2", "Coverage 95%.", "agent-1",
                         "smartlist/project/beta", backend_root=br)
    require(len(a1) > 0, "artifact 1 empty")
    require(len(a2) > 0, "artifact 2 empty")

    refs = list_artifacts(ip, "results", backend_root=br)
    require(len(refs) >= 2, f"expected >=2 artifacts, got {len(refs)}")
    print(f"  stored {len(refs)} artifacts")

    art = read_artifact(ip, a1, backend_root=br)
    print(f"  read artifact: {art}")

    # --- 3d: Messaging ---
    print("\n=== 3d1: Bootstrap message queue ===")
    bootstrap_message_queue(ip, "swarm-bus", backend_root=br)
    print("  channel 'swarm-bus' created")

    print("\n=== 3d2: Send and receive messages ===")
    m1 = send_message(ip, "swarm-bus", "agent-0", "agent-1", "hello from 0",
                       backend_root=br, timestamp="2026-03-18T13:00:00Z")
    require(len(m1) > 0, "message 1 empty")
    print(f"  sent msg on channel: {m1}")

    m2 = send_to_inbox(ip, "agent-0", "agent-1", "task-update", "subtask done",
                        backend_root=br, timestamp="2026-03-18T13:01:00Z")
    require(len(m2) > 0, "inbox message empty")
    print(f"  sent inbox msg: {m2}")

    inbox = read_inbox(ip, "agent-1", backend_root=br)
    require(len(inbox) >= 1, f"expected >=1 inbox messages, got {len(inbox)}")
    print(f"  agent-1 inbox: {len(inbox)} messages")

    print("\n=== 3d2: Broadcast ===")
    bids = broadcast(ip, "orchestrator", ["agent-0", "agent-1"], "sync", "checkpoint reached",
                      backend_root=br)
    require(len(bids) == 2, f"expected 2 broadcast note_ids, got {len(bids)}")
    print(f"  broadcast to 2 agents: {bids}")

    # --- 3d3: Orchestrator messaging (system channel) ---
    print("\n=== 3d3: System channel (orchestrator broadcasts) ===")
    bootstrap_message_queue(ip, "system", backend_root=br)
    sm1 = send_message(ip, "system", "orchestrator", "*", "orchestration-start",
                        backend_root=br, timestamp="2026-03-18T14:00:00Z")
    require(len(sm1) > 0, "system channel message empty")
    sm2 = send_message(ip, "system", "orchestrator", "*", "orchestration-complete",
                        backend_root=br, timestamp="2026-03-18T14:30:00Z")
    require(len(sm2) > 0, "system channel completion msg empty")
    print(f"  system channel: sent 2 orchestrator broadcasts ({sm1}, {sm2})")

    # Workers can send status updates back to orchestrator inbox
    su1 = send_to_inbox(ip, "agent-0", "orchestrator", "status:3a1", "home-node assigned",
                         backend_root=br)
    require(len(su1) > 0, "worker status update empty")
    orch_inbox = read_inbox(ip, "orchestrator", backend_root=br)
    require(len(orch_inbox) >= 1, f"expected >=1 orchestrator inbox msgs, got {len(orch_inbox)}")
    print(f"  orchestrator inbox: {len(orch_inbox)} status updates from workers")

    print("\n=== 3e: Full Phase 3 runtime stack validated ===")
    print("result=ok")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\nFAILED: {e}", file=sys.stderr)
        import traceback; traceback.print_exc()
        sys.exit(1)
