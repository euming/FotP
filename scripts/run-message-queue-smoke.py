#!/usr/bin/env python3
"""Smoke test for the inter-agent message queue (Phase 3 of LLM Swarm Computer)."""
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
    bootstrap_message_queue, send_message, receive_messages,
    list_channels, peek_messages, acknowledge_message,
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
    temp_dir = tempfile.TemporaryDirectory(prefix="ams-message-queue-smoke-")
    workspace = Path(temp_dir.name)

    backend_root = workspace / "backend"
    corpus_dir = workspace / "all-agents-sessions"
    backend_root.mkdir(parents=True, exist_ok=True)
    corpus_dir.mkdir(parents=True, exist_ok=True)
    corpus_path = corpus_dir / "all-agents-sessions.memory.jsonl"
    corpus_path.write_text("", encoding="utf-8")

    input_path = str(corpus_path)
    br = str(backend_root)

    print("=== Step 1: Bootstrap message queue channel 'dispatch' ===")
    bucket = bootstrap_message_queue(input_path, "dispatch", backend_root=br)
    require(bucket == "smartlist/message-queue/dispatch", f"unexpected bucket path: {bucket}")
    print(f"  created bucket: {bucket}")

    print("\n=== Step 2: Send message from orchestrator to worker-0 ===")
    note1 = send_message(
        input_path,
        channel="dispatch",
        sender="orchestrator",
        recipient="worker-0",
        payload="run task 3d1",
        backend_root=br,
        timestamp="2026-03-18T14:00:00Z",
    )
    require(len(note1) > 0, "expected non-empty note_id for message 1")
    print(f"  sent message 1, note_id={note1}")

    time.sleep(0.05)  # avoid kernel note-id collision on fast successive writes

    print("\n=== Step 3: Send message from orchestrator to worker-1 ===")
    note2 = send_message(
        input_path,
        channel="dispatch",
        sender="orchestrator",
        recipient="worker-1",
        payload="run task 3d2",
        backend_root=br,
        timestamp="2026-03-18T14:01:00Z",
    )
    require(len(note2) > 0, "expected non-empty note_id for message 2")
    print(f"  sent message 2, note_id={note2}")

    print("\n=== Step 4: Send message from worker-0 to orchestrator ===")
    note3 = send_message(
        input_path,
        channel="dispatch",
        sender="worker-0",
        recipient="orchestrator",
        payload="task 3d1 complete",
        backend_root=br,
        timestamp="2026-03-18T14:02:00Z",
    )
    require(len(note3) > 0, "expected non-empty note_id for message 3")
    print(f"  sent message 3, note_id={note3}")

    print("\n=== Step 5: Receive messages for worker-0 ===")
    msgs_w0 = receive_messages(input_path, "dispatch", "worker-0", backend_root=br)
    print(f"  worker-0 has {len(msgs_w0)} message(s)")
    require(len(msgs_w0) == 1, f"expected 1 message for worker-0, got {len(msgs_w0)}")
    require(msgs_w0[0]["sender"] == "orchestrator", f"unexpected sender: {msgs_w0[0]['sender']}")
    print(f"  message: {msgs_w0[0]}")

    print("\n=== Step 6: Receive messages for worker-1 ===")
    msgs_w1 = receive_messages(input_path, "dispatch", "worker-1", backend_root=br)
    print(f"  worker-1 has {len(msgs_w1)} message(s)")
    require(len(msgs_w1) == 1, f"expected 1 message for worker-1, got {len(msgs_w1)}")

    print("\n=== Step 7: Receive messages for orchestrator ===")
    msgs_orch = receive_messages(input_path, "dispatch", "orchestrator", backend_root=br)
    print(f"  orchestrator has {len(msgs_orch)} message(s)")
    require(len(msgs_orch) == 1, f"expected 1 message for orchestrator, got {len(msgs_orch)}")
    require(msgs_orch[0]["sender"] == "worker-0", f"unexpected sender: {msgs_orch[0]['sender']}")

    print("\n=== Step 8: Bootstrap second channel 'alerts' ===")
    bucket2 = bootstrap_message_queue(input_path, "alerts", backend_root=br)
    require(bucket2 == "smartlist/message-queue/alerts", f"unexpected bucket path: {bucket2}")
    print(f"  created bucket: {bucket2}")

    note4 = send_message(
        input_path,
        channel="alerts",
        sender="monitor",
        recipient="orchestrator",
        payload="worker-2 unresponsive",
        backend_root=br,
    )
    require(len(note4) > 0, "expected non-empty note_id for alert message")
    print(f"  sent alert, note_id={note4}")

    msgs_alert = receive_messages(input_path, "alerts", "orchestrator", backend_root=br)
    require(len(msgs_alert) == 1, f"expected 1 alert for orchestrator, got {len(msgs_alert)}")
    print(f"  orchestrator alert: {msgs_alert[0]}")

    print("\n=== Step 9: Priority levels ===")
    note_hi = send_message(
        input_path,
        channel="dispatch",
        sender="orchestrator",
        recipient="worker-0",
        payload="urgent task",
        backend_root=br,
        priority="critical",
        timestamp="2026-03-18T15:00:00Z",
    )
    require(len(note_hi) > 0, "expected non-empty note_id for priority message")
    print(f"  sent critical-priority message, note_id={note_hi}")

    # Verify bad priority is rejected
    try:
        send_message(input_path, channel="dispatch", sender="x", recipient="y",
                     payload="bad", backend_root=br, priority="mega")
        require(False, "should have raised ValueError for invalid priority")
    except ValueError:
        print("  invalid priority correctly rejected")

    print("\n=== Step 10: list_channels ===")
    channels = list_channels(input_path, backend_root=br)
    require("dispatch" in channels, f"'dispatch' not in channels: {channels}")
    require("alerts" in channels, f"'alerts' not in channels: {channels}")
    print(f"  channels: {channels}")

    print("\n=== Step 11: peek_messages ===")
    peeked = peek_messages(input_path, "dispatch", backend_root=br)
    require(len(peeked) >= 3, f"expected >=3 messages on dispatch, got {len(peeked)}")
    print(f"  peeked {len(peeked)} message(s) on dispatch")

    print("\n=== Step 12: acknowledge_message ===")
    count_before = len(peek_messages(input_path, "dispatch", backend_root=br))
    acknowledge_message(input_path, "dispatch", note1, backend_root=br)
    count_after = len(peek_messages(input_path, "dispatch", backend_root=br))
    require(count_after == count_before - 1,
            f"expected {count_before - 1} after ack, got {count_after}")
    print(f"  acknowledged note {note1}, messages {count_before} -> {count_after}")

    print("\nresult=ok")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\nFAILED: {e}", file=sys.stderr)
        sys.exit(1)
