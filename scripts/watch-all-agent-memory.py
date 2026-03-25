#!/usr/bin/env python3
"""
watch-all-agent-memory.py

Poll Claude + Codex session roots and run sync-all-agent-memory.bat after
changes settle. Intended for long-running background use.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


TASK_NAME = "NetworkGraphMemory-AgentMemoryWatch"


def iter_session_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*.jsonl") if path.is_file())


def compute_signature(roots: list[Path]) -> tuple[int, int]:
    count = 0
    latest_ns = 0
    for root in roots:
        for path in iter_session_files(root):
            count += 1
            try:
                stat = path.stat()
            except OSError:
                continue
            latest_ns = max(latest_ns, int(stat.st_mtime_ns))
    return count, latest_ns


def output_missing(output_dir: Path) -> bool:
    required = [
        output_dir / "all-agents-sessions.chat.raw.jsonl",
        output_dir / "all-agents-sessions.memory.jsonl",
        output_dir / "all-agents-sessions.memory.ams.json",
        output_dir / "all-agents-sessions.memory.embeddings.json",
        output_dir / "all-agents-sessions.ams-debug.html",
    ]
    return any(not path.exists() for path in required)


def try_acquire_lock(lock_path: Path) -> None:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if lock_path.exists():
        try:
            existing_pid = int(lock_path.read_text(encoding="utf-8").strip() or "0")
        except (OSError, ValueError):
            existing_pid = 0
        if existing_pid > 0:
            try:
                os.kill(existing_pid, 0)
            except OSError:
                pass
            else:
                raise RuntimeError(f"watcher already running with pid {existing_pid}")
        lock_path.unlink(missing_ok=True)
    lock_path.write_text(str(os.getpid()), encoding="utf-8")


def release_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink(missing_ok=True)
    except OSError:
        pass


def run_sync(sync_script: Path, output_dir: Path) -> int:
    env = os.environ.copy()
    env["AMS_NO_BROWSER"] = "1"
    if sys.platform == "win32":
        cmd = ["cmd", "/c", str(sync_script), str(output_dir), "--no-browser"]
    else:
        cmd = ["bash", str(sync_script), str(output_dir), "--no-browser"]
    return subprocess.run(cmd, env=env).returncode


def default_output_dir(script_dir: Path) -> Path:
    override = os.environ.get("AMS_OUTPUT_ROOT")
    if override:
        return Path(override).expanduser() / "all-agents-sessions"

    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "NetworkGraphMemory" / "agent-memory" / "all-agents-sessions"

    return script_dir / "output" / "all-agents-sessions"


def main() -> int:
    default_root = Path(os.environ.get("USERPROFILE", os.environ.get("HOME", "~")))
    script_dir = Path(__file__).resolve().parent
    default_output = default_output_dir(script_dir)

    parser = argparse.ArgumentParser(description="Watch Claude + Codex sessions and auto-sync AMS memory.")
    parser.add_argument("--root-dir", type=Path, default=default_root,
                        help="User root that contains .claude and .codex. Default: %%USERPROFILE%%")
    parser.add_argument("--output-dir", type=Path, default=default_output,
                        help="Output dir for all-agents-sessions artifacts.")
    parser.add_argument("--poll-seconds", type=int, default=15,
                        help="Polling interval in seconds. Default: 15")
    parser.add_argument("--settle-seconds", type=int, default=45,
                        help="Wait this long after the last change before syncing. Default: 45")
    parser.add_argument("--initial-sync", action="store_true",
                        help="Run one sync immediately on startup.")
    args = parser.parse_args()

    claude_root = args.root_dir / ".claude" / "projects"
    codex_root = args.root_dir / ".codex" / "sessions"
    roots = [claude_root, codex_root]
    sync_script = (
        script_dir / "sync-all-agent-memory.bat"
        if sys.platform == "win32"
        else script_dir / "sync-all-agent-memory.sh"
    )
    lock_path = args.output_dir / ".watch.lock"

    if not sync_script.exists():
        print(f"ERROR: sync script not found: {sync_script}", file=sys.stderr)
        return 1

    try:
        try_acquire_lock(lock_path)
    except RuntimeError as ex:
        print(f"ERROR: {ex}", file=sys.stderr)
        return 1

    should_stop = False

    def handle_stop(signum: int, _frame) -> None:
        nonlocal should_stop
        should_stop = True
        print(f"[watch] received signal {signum}, stopping...")

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    print("[watch] starting all-agent memory watcher")
    print(f"[watch] claude root : {claude_root}")
    print(f"[watch] codex root  : {codex_root}")
    print(f"[watch] output dir  : {args.output_dir}")
    print(f"[watch] poll        : {args.poll_seconds}s")
    print(f"[watch] settle      : {args.settle_seconds}s")

    last_seen = compute_signature(roots)
    pending_since: float | None = None

    try:
        if args.initial_sync or output_missing(args.output_dir):
            print("[watch] initial sync triggered")
            exit_code = run_sync(sync_script, args.output_dir)
            print(f"[watch] initial sync exit={exit_code}")
            last_seen = compute_signature(roots)

        while not should_stop:
            time.sleep(max(1, args.poll_seconds))
            current = compute_signature(roots)
            if current != last_seen:
                last_seen = current
                pending_since = time.time()
                print("[watch] change detected; waiting for session files to settle...")
                continue

            if pending_since is None:
                continue

            if (time.time() - pending_since) < max(1, args.settle_seconds):
                continue

            print("[watch] running sync after settled changes")
            exit_code = run_sync(sync_script, args.output_dir)
            print(f"[watch] sync exit={exit_code}")
            pending_since = None

    finally:
        release_lock(lock_path)

    print("[watch] stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
