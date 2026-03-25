#!/usr/bin/env python3
"""
Convert ALL Codex session JSONL logs into a single combined chat_event file.

Usage:
  python ingest-all-codex.py \
      --sessions-dir "%USERPROFILE%\\.codex\\sessions" \
      --out output\\all-codex-sessions.chat.raw.jsonl

Options:
  --sessions-dir DIR   Root directory containing Codex session JSONL files.
                       Default: %USERPROFILE%\\.codex\\sessions
  --out FILE           Output .raw.jsonl path.
                       Default: output/all-codex-sessions.chat.raw.jsonl
  --project NAME       Optional substring filter on derived project name.

Channel encoding:
  Each event gets channel = "codex/<ProjectName>" so AMS can group by project.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional


def extract_user_request(msg: str) -> str:
    """Return the user's direct request without IDE header boilerplate."""
    m = re.search(r"## My request for Codex:\n(.*?)(?:\Z)", msg, re.DOTALL)
    return m.group(1).strip() if m else msg.strip()


def derive_project_name_from_cwd(cwd: str) -> str:
    """Derive a project label from a workspace path."""
    if not cwd:
        return "unknown"

    normalized = cwd.replace("/", "\\").rstrip("\\")
    marker = "\\git\\"
    idx = normalized.lower().rfind(marker)
    if idx >= 0:
        candidate = normalized[idx + len(marker):].split("\\", 1)[0].strip()
        if candidate:
            return candidate

    parts = [p for p in normalized.split("\\") if p]
    return parts[-1] if parts else "unknown"


def parse_session(path: Path) -> tuple[str, str, list[dict]]:
    """
    Parse one Codex session file.
    Returns: (session_id, project_name, events)
    """
    session_id = path.stem
    project_name = "unknown"
    events: list[dict] = []
    msg_index = 0

    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            ts = obj.get("timestamp")
            rec_type = obj.get("type", "")
            payload = obj.get("payload", {})

            if rec_type == "session_meta":
                session_id = payload.get("id", session_id)
                project_name = derive_project_name_from_cwd(payload.get("cwd", ""))
                continue

            if rec_type != "event_msg":
                continue

            payload_type = payload.get("type", "")
            if payload_type == "user_message":
                text = extract_user_request(payload.get("message", ""))
                if text:
                    events.append(
                        {
                            "ts": ts,
                            "session_id": session_id,
                            "message_id": f"{session_id}-u-{msg_index:04d}",
                            "direction": "in",
                            "author": "User",
                            "text": text,
                            "project": project_name,
                        }
                    )
                    msg_index += 1
                continue

            if payload_type == "agent_message":
                text = payload.get("message", "").strip()
                if text:
                    events.append(
                        {
                            "ts": ts,
                            "session_id": session_id,
                            "message_id": f"{session_id}-a-{msg_index:04d}",
                            "direction": "out",
                            "author": "Codex",
                            "text": text,
                            "project": project_name,
                        }
                    )
                    msg_index += 1

    return session_id, project_name, events


# ---------------------------------------------------------------------------
# Session fingerprint cache
# ---------------------------------------------------------------------------

def load_cache(cache_path: Path) -> dict:
    """Load the fingerprint cache from disk, or return an empty dict."""
    if cache_path.is_file():
        try:
            with open(cache_path, encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def save_cache(cache_path: Path, cache: dict) -> None:
    """Persist the fingerprint cache to disk."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2)


def file_fingerprint(path: Path) -> dict:
    """Return a fingerprint dict for a file based on mtime and size."""
    st = path.stat()
    return {'mtime': st.st_mtime, 'size': st.st_size}


def is_cached(path: Path, cache: dict) -> bool:
    """Return True if the file's fingerprint matches the cache entry."""
    key = str(path)
    if key not in cache:
        return False
    cached = cache[key]
    current = file_fingerprint(path)
    return cached.get('mtime') == current['mtime'] and cached.get('size') == current['size']


def discover_sessions(sessions_root: Path):
    """Yield session JSONL files under sessions root."""
    yield from sorted(sessions_root.rglob("*.jsonl"))


def main() -> None:
    default_root = Path(os.environ.get("USERPROFILE", os.environ.get("HOME", "~"))) / ".codex" / "sessions"

    parser = argparse.ArgumentParser(
        description="Convert all Codex session JSONL files to combined chat_event raw file."
    )
    parser.add_argument(
        "--sessions-dir",
        type=Path,
        default=default_root,
        help="Root directory containing Codex session JSONL files.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("output/all-codex-sessions.chat.raw.jsonl"),
        help="Output .raw.jsonl path.",
    )
    parser.add_argument(
        "--project",
        default=None,
        help="Optional substring filter on derived project name.",
    )
    parser.add_argument(
        '--cache', type=Path, default=None,
        help='Fingerprint cache path. Default: <out>.cache.json. '
             'Pass --no-cache to disable incremental mode.')
    parser.add_argument(
        '--no-cache', action='store_true',
        help='Disable incremental caching; always do a full rebuild.')
    args = parser.parse_args()

    if not args.sessions_dir.is_dir():
        print(f"ERROR: sessions-dir not found: {args.sessions_dir}", file=sys.stderr)
        sys.exit(1)

    # Resolve cache path
    cache_path: Optional[Path] = None
    cache: dict = {}
    if not args.no_cache:
        cache_path = args.cache if args.cache else Path(str(args.out) + '.cache.json')
        cache = load_cache(cache_path)

    incremental = (
        not args.no_cache
        and cache_path is not None
        and bool(cache)
        and args.out.is_file()
    )
    write_mode = 'a' if incremental else 'w'

    print(f"Sessions root : {args.sessions_dir}")
    print(f"Project filter: {args.project or '(all)'}")
    print(f"Mode          : {'incremental (append)' if incremental else 'full rebuild'}")
    print()

    all_events: list[dict] = []
    sessions_used = 0
    skipped_count = 0
    new_cache_entries: dict = {}

    for path in discover_sessions(args.sessions_dir):
        # Skip unchanged sessions in incremental mode
        if not args.no_cache and is_cached(path, cache):
            skipped_count += 1
            new_cache_entries[str(path)] = cache[str(path)]
            continue

        session_id, project_name, events = parse_session(path)
        if args.project and args.project.lower() not in project_name.lower():
            continue

        if not events:
            if not args.no_cache:
                new_cache_entries[str(path)] = file_fingerprint(path)
            continue

        user_count = sum(1 for e in events if e["direction"] == "in")
        codex_count = sum(1 for e in events if e["direction"] == "out")
        print(f"  [{project_name}] {path.name}  user={user_count}  codex={codex_count}")

        all_events.extend(events)
        sessions_used += 1
        if not args.no_cache:
            new_cache_entries[str(path)] = file_fingerprint(path)

    if not all_events:
        if skipped_count > 0:
            print(f"No new sessions (skipped {skipped_count} unchanged). Output unchanged.")
            return
        print("No usable Codex messages found.", file=sys.stderr)
        sys.exit(1)

    all_events.sort(key=lambda e: (f"codex/{e['project']}", e["session_id"], e["ts"]))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, write_mode, encoding="utf-8") as out:
        for e in all_events:
            out.write(
                json.dumps(
                    {
                        "type": "chat_event",
                        "channel": f"codex/{e['project']}",
                        "chat_id": e["session_id"],
                        "message_id": e["message_id"],
                        "ts": e["ts"],
                        "author": e["author"],
                        "direction": e["direction"],
                        "text": e["text"],
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

    # Persist updated cache
    if cache_path is not None:
        merged_cache = {**cache, **new_cache_entries}
        save_cache(cache_path, merged_cache)

    user_total = sum(1 for e in all_events if e["direction"] == "in")
    codex_total = sum(1 for e in all_events if e["direction"] == "out")
    print()
    print(f"Total   : {len(all_events)} events  (User: {user_total}, Codex: {codex_total})")
    print(f"Sessions: {sessions_used} new/changed  ({skipped_count} unchanged, skipped)")
    print(f"Output  : {args.out}")
    if cache_path:
        print(f"Cache   : {cache_path}")


if __name__ == "__main__":
    main()
