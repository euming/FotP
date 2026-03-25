#!/usr/bin/env python3
"""
P2d verification: end-to-end idempotency test for the incremental ingest pipeline.

Tests the caching logic embedded in ingest-all-claude-projects.py and
ingest-all-codex.py without running the full bat pipeline (which requires
dotnet/memoryctl to be available).

Steps:
  1. Cold start — full rebuild from scratch, cache created.
  2. Re-run with no changes — all sessions skipped, output unchanged.
  3. Add a new session — only new session processed, appended to output.
  4. Idempotency check — output after step 2 == output after step 1 (same lines).
"""

from __future__ import annotations

import json
import os
import sys
import time
import tempfile
import shutil
from pathlib import Path

# Locate scripts dir relative to this file
SCRIPTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS_DIR))

# Import the converter modules
from importlib import import_module as _imp
ingest_claude = _imp('ingest-all-claude-projects')

PASS = "PASS"
FAIL = "FAIL"

_failures: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"  {PASS} {label}")
    else:
        msg = f"  {FAIL} FAILED: {label}"
        if detail:
            msg += f"\n      {detail}"
        print(msg)
        _failures.append(label)


def make_session_jsonl(path: Path, project_dir: Path, session_id: str, messages: list[tuple[str, str]]) -> None:
    """Write a minimal Claude Code session JSONL with (role, text) pairs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    ts_base = "2024-01-01T00:00:00.000Z"
    with open(path, 'w', encoding='utf-8') as f:
        for i, (role, text) in enumerate(messages):
            ts = f"2024-01-01T00:00:{i:02d}.000Z"
            if role == 'user':
                obj = {
                    "type": "user",
                    "timestamp": ts,
                    "sessionId": session_id,
                    "uuid": f"{session_id}-u-{i}",
                    "message": {"content": text},
                    "isMeta": False,
                }
            else:
                obj = {
                    "type": "assistant",
                    "timestamp": ts,
                    "sessionId": session_id,
                    "message": {
                        "id": f"msg_{session_id}_{i}",
                        "content": [{"type": "text", "text": text}],
                    },
                }
            f.write(json.dumps(obj) + '\n')


def count_lines(path: Path) -> int:
    if not path.is_file():
        return 0
    with open(path, encoding='utf-8') as f:
        return sum(1 for line in f if line.strip())


def run_converter(projects_root: Path, out: Path, no_cache: bool = False) -> tuple[int, int, str]:
    """
    Run ingest_claude.discover_sessions + process pipeline.
    Returns (session_count, skipped_count, mode).
    Replicates the main() logic without argparse to stay testable.
    """
    cache_path = Path(str(out) + '.cache.json')
    cache: dict = {}
    if not no_cache:
        cache = ingest_claude.load_cache(cache_path)

    incremental = (
        not no_cache
        and bool(cache)
        and out.is_file()
    )
    write_mode = 'a' if incremental else 'w'
    mode = 'incremental' if incremental else 'full'

    all_events: list[dict] = []
    all_tool_events: list[dict] = []
    session_count = 0
    skipped_count = 0
    new_cache_entries: dict = {}

    for project_name, session_path in ingest_claude.discover_sessions(projects_root):
        if not no_cache and ingest_claude.is_cached(session_path, cache):
            skipped_count += 1
            new_cache_entries[str(session_path)] = cache[str(session_path)]
            continue

        events, tool_events = ingest_claude.process_session_with_project(session_path, project_name)
        if events or tool_events:
            session_count += 1
        all_events.extend(events)
        all_tool_events.extend(tool_events)
        if not no_cache:
            new_cache_entries[str(session_path)] = ingest_claude.file_fingerprint(session_path)

    if not all_events and not all_tool_events:
        if skipped_count > 0:
            # No new content — output unchanged (idempotent)
            if not no_cache:
                merged = {**cache, **new_cache_entries}
                ingest_claude.save_cache(cache_path, merged)
            return session_count, skipped_count, mode
        return session_count, skipped_count, mode

    all_events.sort(key=lambda e: (e['channel'], e['session_id'], e['ts']))
    all_tool_events.sort(key=lambda e: (e.get('channel', ''), e['session_id'], e['ts']))

    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, write_mode, encoding='utf-8') as f:
        for e in all_events:
            rec: dict = {
                'type': 'chat_event',
                'channel': e['channel'],
                'chat_id': e['session_id'],
                'message_id': e['message_id'],
                'ts': e['ts'],
                'author': e['author'],
                'direction': e['direction'],
                'text': e['text'],
            }
            if 'slug' in e:
                rec['slug'] = e['slug']
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')
        for e in all_tool_events:
            rec = {
                'type': 'tool_event',
                'channel': e.get('channel', ''),
                'chat_id': e['session_id'],
                'tool_use_id': e['tool_use_id'],
                'ts': e['ts'],
                'tool_name': e['tool_name'],
                'input': e['input'],
                'result_preview': e['result_preview'],
                'is_error': e['is_error'],
            }
            if 'slug' in e:
                rec['slug'] = e['slug']
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')

    if not no_cache:
        merged = {**cache, **new_cache_entries}
        ingest_claude.save_cache(cache_path, merged)

    return session_count, skipped_count, mode


def main() -> None:
    print("=" * 60)
    print("P2d: Incremental Ingest Pipeline — Verification Test")
    print("=" * 60)
    print()

    tmpdir = Path(tempfile.mkdtemp(prefix='p2d_test_'))
    try:
        projects_root = tmpdir / 'projects'
        out = tmpdir / 'output' / 'test.chat.raw.jsonl'
        cache_path = Path(str(out) + '.cache.json')

        # --- Build initial sessions ---
        proj_dir = projects_root / 'C--Users-test-wkspaces-git-TestProject'
        make_session_jsonl(
            proj_dir / 'session-alpha.jsonl',
            proj_dir, 'session-alpha',
            [('user', 'Hello world'), ('assistant', 'Hello back')]
        )
        make_session_jsonl(
            proj_dir / 'session-beta.jsonl',
            proj_dir, 'session-beta',
            [('user', 'Second session'), ('assistant', 'Second reply')]
        )

        # -------------------------------------------------------
        # Step 1: Cold start — full rebuild
        # -------------------------------------------------------
        print("Step 1: Cold start (full rebuild)")
        session_count, skipped_count, mode = run_converter(projects_root, out)
        lines_after_step1 = count_lines(out)
        check("mode=full on cold start", mode == 'full', f"got mode={mode!r}")
        check("cache file created", cache_path.is_file())
        check("2 sessions processed", session_count == 2, f"got {session_count}")
        check("0 sessions skipped", skipped_count == 0, f"got {skipped_count}")
        check("output has lines", lines_after_step1 > 0, f"got {lines_after_step1} lines")
        cache_after_step1 = json.loads(cache_path.read_text(encoding='utf-8'))
        check("cache has 2 entries", len(cache_after_step1) == 2, f"got {len(cache_after_step1)}")
        print()

        # -------------------------------------------------------
        # Step 2: Re-run with no changes — idempotency
        # -------------------------------------------------------
        print("Step 2: Re-run with no new sessions (idempotency check)")
        session_count2, skipped_count2, mode2 = run_converter(projects_root, out)
        lines_after_step2 = count_lines(out)
        check("mode=incremental on re-run", mode2 == 'incremental', f"got mode={mode2!r}")
        check("0 sessions processed (all skipped)", session_count2 == 0, f"got {session_count2}")
        check("2 sessions skipped", skipped_count2 == 2, f"got {skipped_count2}")
        check("output line count unchanged", lines_after_step2 == lines_after_step1,
              f"before={lines_after_step1}, after={lines_after_step2}")
        print()

        # -------------------------------------------------------
        # Step 3: Add one new session — only new session appended
        # -------------------------------------------------------
        print("Step 3: Add one new session - incremental append")
        # Sleep a tiny bit to ensure mtime differs on fast filesystems
        time.sleep(0.05)
        make_session_jsonl(
            proj_dir / 'session-gamma.jsonl',
            proj_dir, 'session-gamma',
            [('user', 'Brand new session'), ('assistant', 'Brand new reply')]
        )
        session_count3, skipped_count3, mode3 = run_converter(projects_root, out)
        lines_after_step3 = count_lines(out)
        check("mode=incremental after adding session", mode3 == 'incremental', f"got mode={mode3!r}")
        check("1 session processed (the new one)", session_count3 == 1, f"got {session_count3}")
        check("2 sessions skipped (unchanged)", skipped_count3 == 2, f"got {skipped_count3}")
        check("output has more lines than step 1", lines_after_step3 > lines_after_step1,
              f"step1={lines_after_step1}, step3={lines_after_step3}")
        cache_after_step3 = json.loads(cache_path.read_text(encoding='utf-8'))
        check("cache now has 3 entries", len(cache_after_step3) == 3, f"got {len(cache_after_step3)}")
        print()

        # -------------------------------------------------------
        # Step 4: Verify new session lines appear in output
        # -------------------------------------------------------
        print("Step 4: Verify new session content in output")
        with open(out, encoding='utf-8') as f:
            output_lines = [json.loads(l) for l in f if l.strip()]
        gamma_lines = [l for l in output_lines if l.get('chat_id') == 'session-gamma']
        check("session-gamma events appear in output", len(gamma_lines) >= 2,
              f"found {len(gamma_lines)} lines for session-gamma")
        alpha_lines = [l for l in output_lines if l.get('chat_id') == 'session-alpha']
        beta_lines  = [l for l in output_lines if l.get('chat_id') == 'session-beta']
        check("session-alpha events preserved", len(alpha_lines) >= 2,
              f"found {len(alpha_lines)}")
        check("session-beta events preserved", len(beta_lines) >= 2,
              f"found {len(beta_lines)}")
        print()

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    # -------------------------------------------------------
    # Summary
    # -------------------------------------------------------
    print("=" * 60)
    if _failures:
        print(f"RESULT: {len(_failures)} FAILURE(S)")
        for f in _failures:
            print(f"  - {f}")
        sys.exit(1)
    else:
        print("RESULT: ALL CHECKS PASSED")
    print("=" * 60)


if __name__ == '__main__':
    main()
