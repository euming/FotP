#!/usr/bin/env python3
"""
SessionEnd hook: keep AMS memory current after each session.

Pipeline:
1) sync-all-agent-memory.bat for the unified Claude+Codex corpus
2) FEP online belief update from this session's tool calls
"""
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

HOOK_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HOOK_DIR))
SCRIPTS_DIR = os.path.join(REPO_ROOT, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from ams_common import corpus_db


def _sync_command() -> list[str]:
    script = os.path.join(
        REPO_ROOT,
        "scripts",
        "sync-all-agent-memory.bat" if os.name == "nt" else "sync-all-agent-memory.sh",
    )
    if os.name == "nt":
        return [script, "--no-browser"]
    return ["bash", script, "--no-browser"]


def _resolve_db_path():
    """Resolve the unified corpus path through the shared wrapper contract."""
    return corpus_db("all")


def _find_rust_kernel():
    """Find the Rust AMS kernel binary."""
    exe_name = "ams-core-kernel.exe" if os.name == "nt" else "ams-core-kernel"
    for profile in ("release", "debug"):
        exe = os.path.join(
            REPO_ROOT, "rust", "ams-core-kernel", "target", profile, exe_name
        )
        if os.path.exists(exe):
            return exe
    return None


def _run_fep_belief_update(session_start_iso: str):
    """Run FEP online belief update for tool calls from this session."""
    kernel = _find_rust_kernel()
    if kernel is None:
        return  # silently skip if binary not built

    db = _resolve_db_path()
    if not os.path.exists(db):
        return

    try:
        result = subprocess.run(
            [
                kernel,
                "fep-update-agent-tool-beliefs",
                "--input", db,
                "--since", session_start_iso,
                "--precision", "1.0",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=60,
        )
        if result.returncode != 0:
            print(
                f"[session-end-ingest] FEP belief update failed: {result.stderr[-400:]}",
                file=sys.stderr,
            )
    except Exception as e:
        print(f"[session-end-ingest] FEP belief update error: {e}", file=sys.stderr)


def _run_fep_precision_decay():
    """Apply precision decay to agent tool priors between sessions.

    Moves stale priors back toward default uncertainty so that old high-confidence
    beliefs don't suppress detection of new anomalies.
    """
    kernel = _find_rust_kernel()
    if kernel is None:
        return

    db = _resolve_db_path()
    if not os.path.exists(db):
        return

    try:
        result = subprocess.run(
            [
                kernel,
                "fep-decay-tool-priors",
                "--input", db,
                "--decay-rate", "0.1",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=60,
        )
        if result.returncode != 0:
            print(
                f"[session-end-ingest] FEP precision decay failed: {result.stderr[-400:]}",
                file=sys.stderr,
            )
    except Exception as e:
        print(f"[session-end-ingest] FEP precision decay error: {e}", file=sys.stderr)


def main():
    # Record session start time (approximate — the hook fires at session end,
    # but we use the session's start time if available from stdin metadata).
    session_start_iso = None

    try:
        raw = sys.stdin.read()
        data = json.loads(raw) if raw.strip() else {}
    except Exception:
        data = {}

    reason = data.get("reason", "unknown")

    # Try to get session start from hook data; fall back to 2 hours ago
    session_start_iso = data.get("session_start")
    if not session_start_iso:
        # Conservative fallback: process tool calls from the last 2 hours
        from datetime import timedelta
        fallback = datetime.now(timezone.utc) - timedelta(hours=2)
        session_start_iso = fallback.isoformat()

    pipeline = subprocess.run(
        _sync_command(),
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=300,
    )

    if pipeline.returncode != 0:
        print(f"[session-end-ingest] sync failed (reason={reason}):", file=sys.stderr)
        tail = pipeline.stderr[-800:] if pipeline.stderr else pipeline.stdout[-800:]
        print(tail, file=sys.stderr)
        sys.exit(0)

    # After successful sync, run FEP online belief update
    _run_fep_belief_update(session_start_iso)

    # Apply precision decay so stale priors don't suppress new anomalies
    _run_fep_precision_decay()

    sys.exit(0)


if __name__ == "__main__":
    main()
