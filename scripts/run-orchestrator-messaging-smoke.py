#!/usr/bin/env python3
"""Smoke test for orchestrator messaging (3d3): inbox-driven completion,
system broadcast channel, and cross-SmartList triggers."""
from __future__ import annotations

import subprocess
import sys
import tempfile
import time
from pathlib import Path

from ams_common import build_rust_ams_cmd, repo_root, rust_backend_env

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(repo_root())

sys.path.insert(0, str(SCRIPT_DIR))
from swarm.messaging import (
    bootstrap_message_queue,
    send_to_inbox, read_inbox, ensure_inbox,
    bootstrap_system_channel, system_broadcast, read_system_broadcasts,
    register_trigger, fire_triggers, list_triggers,
)


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


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main() -> int:
    temp_dir = tempfile.TemporaryDirectory(prefix="ams-orch-messaging-smoke-")
    workspace = Path(temp_dir.name)

    backend_root = workspace / "backend"
    corpus_dir = workspace / "all-agents-sessions"
    backend_root.mkdir(parents=True, exist_ok=True)
    corpus_dir.mkdir(parents=True, exist_ok=True)
    corpus_path = corpus_dir / "all-agents-sessions.memory.jsonl"
    corpus_path.write_text("", encoding="utf-8")

    input_path = str(corpus_path)
    br = str(backend_root)

    # -----------------------------------------------------------------------
    # 1. Inbox-driven worker -> orchestrator notification
    # -----------------------------------------------------------------------
    print("=== Step 1: Worker sends task-complete to orchestrator inbox ===")
    ensure_inbox(input_path, "orchestrator", backend_root=br)
    note1 = send_to_inbox(
        input_path, "worker-0", "orchestrator",
        subject="task-complete:build-parser",
        body="Parser implementation done",
        backend_root=br,
    )
    require(len(note1) > 0, "expected non-empty note_id")
    print(f"  sent task-complete, note_id={note1}")

    time.sleep(0.05)

    print("\n=== Step 2: Worker sends task-failed to orchestrator inbox ===")
    note2 = send_to_inbox(
        input_path, "worker-1", "orchestrator",
        subject="task-failed:lint-check",
        body="Lint errors in module X",
        backend_root=br,
    )
    require(len(note2) > 0, "expected non-empty note_id")
    print(f"  sent task-failed, note_id={note2}")

    print("\n=== Step 3: Orchestrator reads inbox ===")
    messages = read_inbox(input_path, "orchestrator", backend_root=br)
    print(f"  orchestrator inbox has {len(messages)} message(s)")
    require(len(messages) >= 2, f"expected >=2 messages, got {len(messages)}")

    # Verify message titles contain the expected subjects
    names = [m.get("name", "") for m in messages]
    has_complete = any("task-complete:build-parser" in n for n in names)
    has_failed = any("task-failed:lint-check" in n for n in names)
    require(has_complete, f"task-complete message not found in inbox: {names}")
    require(has_failed, f"task-failed message not found in inbox: {names}")
    print("  found task-complete and task-failed messages")

    # -----------------------------------------------------------------------
    # 2. System broadcast channel
    # -----------------------------------------------------------------------
    print("\n=== Step 4: Bootstrap system broadcast channel ===")
    sys_bucket = bootstrap_system_channel(input_path, backend_root=br)
    require("system" in sys_bucket, f"unexpected system bucket: {sys_bucket}")
    print(f"  system channel: {sys_bucket}")

    print("\n=== Step 5: Send system broadcasts ===")
    sb1 = system_broadcast(
        input_path, "orchestrator", "orchestration-start",
        "Plan tree execution beginning.",
        backend_root=br,
    )
    require(len(sb1) > 0, "expected non-empty note_id for system broadcast")
    print(f"  broadcast 1: {sb1}")

    time.sleep(0.05)

    sb2 = system_broadcast(
        input_path, "orchestrator", "orchestration-complete",
        "Plan tree done. Steps: 5",
        backend_root=br,
    )
    require(len(sb2) > 0, "expected non-empty note_id for system broadcast")
    print(f"  broadcast 2: {sb2}")

    print("\n=== Step 6: Read system broadcasts ===")
    broadcasts = read_system_broadcasts(input_path, backend_root=br)
    require(len(broadcasts) >= 2, f"expected >=2 broadcasts, got {len(broadcasts)}")
    print(f"  {len(broadcasts)} system broadcast(s) found")

    # -----------------------------------------------------------------------
    # 3. Cross-SmartList triggers
    # -----------------------------------------------------------------------
    print("\n=== Step 7: Register cross-project trigger ===")
    tid = register_trigger(
        input_path,
        source_project="project-alpha",
        target_project="project-beta",
        event="task-complete",
        action_subject="dependency-ready",
        action_body="project-alpha finished; project-beta can proceed",
        backend_root=br,
    )
    require(len(tid) > 0, "expected non-empty trigger note_id")
    print(f"  registered trigger, note_id={tid}")

    print("\n=== Step 8: List triggers ===")
    triggers = list_triggers(input_path, "project-alpha", backend_root=br)
    require(len(triggers) >= 1, f"expected >=1 trigger, got {len(triggers)}")
    print(f"  {len(triggers)} trigger(s) for project-alpha")

    print("\n=== Step 9: Fire triggers ===")
    # Ensure the target orchestrator inbox exists
    ensure_inbox(input_path, "orchestrator:project-beta", backend_root=br)
    delivered = fire_triggers(input_path, "project-alpha", "task-complete", backend_root=br)
    require(len(delivered) >= 1, f"expected >=1 delivered message, got {len(delivered)}")
    print(f"  fired {len(delivered)} trigger(s)")

    # Verify the target inbox received the trigger notification
    target_msgs = read_inbox(input_path, "orchestrator:project-beta", backend_root=br)
    require(len(target_msgs) >= 1, f"expected >=1 message in target inbox, got {len(target_msgs)}")
    print(f"  target inbox has {len(target_msgs)} message(s)")

    print("\n=== Step 10: Fire triggers for non-matching event (should be 0) ===")
    delivered2 = fire_triggers(input_path, "project-alpha", "shutdown", backend_root=br)
    require(len(delivered2) == 0, f"expected 0 deliveries for non-matching event, got {len(delivered2)}")
    print("  correctly delivered 0 for non-matching event")

    print("\nresult=ok")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\nFAILED: {e}", file=sys.stderr)
        sys.exit(1)
