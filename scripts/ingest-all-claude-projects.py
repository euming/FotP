#!/usr/bin/env python3
"""
Convert ALL Claude Code projects under a shared projects root to a single
combined chat_event rawUser file, ready for ingest-chatlog.

Usage:
  python ingest-all-claude-projects.py \\
      --projects-dir "%USERPROFILE%\\.claude\\projects" \\
      --out output\\all-claude-projects.chat.raw.jsonl

Options:
  --projects-dir DIR   Root directory containing per-project subdirectories.
                       Default: %USERPROFILE%\\.claude\\projects
  --out FILE           Output .raw.jsonl path. Default: output/all-claude-projects.chat.raw.jsonl
  --project NAME       Process only the named project (partial match on derived name).

Derived project name:
  Directory names follow the Claude Code encoding:
    "C--Users-eumin-wkspaces-git-AMS"  ->  "AMS"
  The part after the last "-git-" token is used as the project name.
  If "-git-" is absent, the last hyphen-delimited token is used.

Channel encoding:
  Each event gets  channel = "claude-code/<ProjectName>"
  so the ingestor builds a 3-level MemAnchor hierarchy:
    Conversations → Project: <Name> → Day: yyyy-MM-dd [<Name>] → Session: <slug>

Slug:
  The first "slug" field found in the session JSONL is attached to every event
  so the ingestor can use it as a human-readable session label.

Sort order:
  Events are sorted by (channel, session_id, ts) so each chat stays contiguous
  while preserving chronological order within that chat.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional

# Import tool event extraction from convert-claude.py
sys.path.insert(0, str(Path(__file__).resolve().parent))
from importlib import import_module as _imp
_convert = _imp('convert-claude')
extract_tool_events = _convert.extract_tool_events
collect_tool_results = _convert.collect_tool_results


# ---------------------------------------------------------------------------
# Helpers shared with convert-claude.py
# ---------------------------------------------------------------------------

def is_real_user_message(obj: dict) -> bool:
    """True only for genuine human-typed messages."""
    if obj.get('type') != 'user':
        return False
    if obj.get('isMeta'):
        return False
    content = obj.get('message', {}).get('content', '')
    if not isinstance(content, str):
        return False
    if not content.strip():
        return False
    if content.lstrip().startswith('<'):
        return False
    return True


def extract_text_blocks(obj: dict) -> str:
    """Return only the text blocks from an assistant message."""
    content = obj.get('message', {}).get('content', [])
    if not isinstance(content, list):
        return ''
    texts = [
        b['text'] for b in content
        if b.get('type') == 'text' and b.get('text', '').strip()
    ]
    return '\n'.join(texts)


def process_session(path: Path) -> tuple[list[dict], list[dict]]:
    """Process one .jsonl file and return (chat_events, tool_events)."""
    user_events: list[dict] = []
    asst_by_id: dict[str, dict] = {}
    all_tool_results: dict[str, dict] = {}

    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            t = obj.get('type', '')

            if t == 'user':
                if is_real_user_message(obj):
                    user_events.append(obj)
                all_tool_results.update(collect_tool_results(obj))
            elif t == 'assistant':
                mid = obj.get('message', {}).get('id', '')
                if mid:
                    asst_by_id[mid] = obj

    session_id = path.stem
    events: list[dict] = []

    for obj in user_events:
        events.append({
            'ts':         obj['timestamp'],
            'session_id': obj.get('sessionId', session_id),
            'message_id': obj.get('uuid', ''),
            'direction':  'in',
            'author':     'User',
            'text':       obj['message']['content'].strip(),
        })

    for mid, obj in asst_by_id.items():
        text = extract_text_blocks(obj)
        if text:
            usage = obj.get('message', {}).get('usage', {})
            events.append({
                'ts':                  obj['timestamp'],
                'session_id':          obj.get('sessionId', session_id),
                'message_id':          mid,
                'direction':           'out',
                'author':              'Claude',
                'text':                text,
                'tokens_in':           usage.get('input_tokens', 0),
                'tokens_out':          usage.get('output_tokens', 0),
                'tokens_cache_read':   usage.get('cache_read_input_tokens', 0),
                'tokens_cache_create': usage.get('cache_creation_input_tokens', 0),
            })

    effective_session_id = next(
        (obj.get('sessionId', session_id) for obj in user_events),
        session_id,
    )
    tool_events = extract_tool_events(asst_by_id, all_tool_results, effective_session_id)

    return events, tool_events


# ---------------------------------------------------------------------------
# Multi-project discovery
# ---------------------------------------------------------------------------

def derive_project_name(dir_name: str) -> str:
    """
    'C--Users-eumin-wkspaces-git-AMS'  ->  'AMS'
    'C--Users-eumin-wkspaces-git-NetworkGraphMemory'  ->  'NetworkGraphMemory'
    Falls back to last hyphen-delimited token if '-git-' is absent.
    """
    marker = '-git-'
    idx = dir_name.find(marker)
    if idx >= 0:
        return dir_name[idx + len(marker):]
    return dir_name.split('-')[-1]


def extract_slug(path: Path) -> str | None:
    """Return the first slug found in a session JSONL file, or None."""
    try:
        with open(path, encoding='utf-8') as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    slug = obj.get('slug')
                    if slug:
                        return str(slug)
                except json.JSONDecodeError:
                    continue
    except OSError:
        pass
    return None


def discover_sessions(projects_root: Path, project_filter: str | None = None):
    """
    Yield (project_name, session_jsonl_path) for all non-empty *.jsonl files
    under projects_root.  Applies project_filter (case-insensitive substring)
    if provided.
    """
    for project_dir in sorted(projects_root.iterdir()):
        if not project_dir.is_dir():
            continue
        project_name = derive_project_name(project_dir.name)
        if project_filter and project_filter.lower() not in project_name.lower():
            continue
        for f in sorted(project_dir.glob('*.jsonl')):
            if f.stat().st_size > 0:
                yield project_name, f


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


def process_session_with_project(path: Path, project_name: str) -> tuple[list[dict], list[dict]]:
    """Like process_session() but embeds project in channel and adds slug."""
    slug = extract_slug(path)
    events, tool_events = process_session(path)
    channel = f'claude-code/{project_name}'
    for e in events:
        e['channel'] = channel
        if slug:
            e['slug'] = slug
    for e in tool_events:
        e['channel'] = channel
        if slug:
            e['slug'] = slug
    return events, tool_events


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    default_projects_dir = Path(os.environ.get('USERPROFILE', os.environ.get('HOME', '~'))) / '.claude' / 'projects'

    parser = argparse.ArgumentParser(
        description='Convert all Claude Code projects to a combined chat_event file.')
    parser.add_argument('--projects-dir', type=Path, default=default_projects_dir,
                        help='Root directory containing per-project subdirectories.')
    parser.add_argument('--out', type=Path, default=Path('output/all-claude-projects.chat.raw.jsonl'),
                        help='Output .raw.jsonl path.')
    parser.add_argument('--project', default=None,
                        help='Process only the named project (substring match).')
    parser.add_argument('--cache', type=Path, default=None,
                        help='Fingerprint cache path. Default: <out>.cache.json. '
                             'Pass --no-cache to disable incremental mode.')
    parser.add_argument('--no-cache', action='store_true',
                        help='Disable incremental caching; always do a full rebuild.')
    args = parser.parse_args()

    projects_root: Path = args.projects_dir
    if not projects_root.is_dir():
        print(f"ERROR: projects-dir not found: {projects_root}", file=sys.stderr)
        sys.exit(1)

    # Resolve cache path
    cache_path: Optional[Path] = None
    cache: dict = {}
    if not args.no_cache:
        cache_path = args.cache if args.cache else Path(str(args.out) + '.cache.json')
        cache = load_cache(cache_path)

    # Incremental mode: if the output exists and we have a non-empty cache, append.
    # Otherwise do a full rebuild (write mode).
    incremental = (
        not args.no_cache
        and cache_path is not None
        and bool(cache)
        and args.out.is_file()
    )
    write_mode = 'a' if incremental else 'w'

    print(f"Projects root : {projects_root}")
    print(f"Project filter: {args.project or '(all)'}")
    print(f"Mode          : {'incremental (append)' if incremental else 'full rebuild'}")
    print()

    all_events: list[dict] = []
    all_tool_events: list[dict] = []
    session_count = 0
    skipped_count = 0
    new_cache_entries: dict = {}

    for project_name, session_path in discover_sessions(projects_root, args.project):
        # Skip unchanged sessions in incremental mode
        if not args.no_cache and is_cached(session_path, cache):
            skipped_count += 1
            # Carry forward unchanged entry in the updated cache
            new_cache_entries[str(session_path)] = cache[str(session_path)]
            continue

        events, tool_events = process_session_with_project(session_path, project_name)
        n_u = sum(1 for e in events if e['direction'] == 'in')
        n_a = sum(1 for e in events if e['direction'] == 'out')
        n_t = len(tool_events)
        if n_u + n_a + n_t > 0:
            slug = events[0].get('slug', '') if events else ''
            slug_label = f'  [{slug}]' if slug else ''
            print(f"  [{project_name}] {session_path.name[:36]}{slug_label}  user={n_u}  claude={n_a}  tools={n_t}")
            session_count += 1
        all_events.extend(events)
        all_tool_events.extend(tool_events)

        # Record fingerprint for sessions we processed
        if not args.no_cache:
            new_cache_entries[str(session_path)] = file_fingerprint(session_path)

    if not all_events and not all_tool_events:
        if skipped_count > 0:
            print(f"No new sessions (skipped {skipped_count} unchanged). Output unchanged.")
            return
        print("No usable messages found.", file=sys.stderr)
        sys.exit(1)

    # In full-rebuild mode, sort across all sessions for contiguous chat layout.
    # In incremental/append mode, still sort the new batch by (channel, session, ts).
    all_events.sort(key=lambda e: (e['channel'], e['session_id'], e['ts']))
    all_tool_events.sort(key=lambda e: (e.get('channel', ''), e['session_id'], e['ts']))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, write_mode, encoding='utf-8') as out:
        for e in all_events:
            rec: dict = {
                'type':       'chat_event',
                'channel':    e['channel'],
                'chat_id':    e['session_id'],
                'message_id': e['message_id'],
                'ts':         e['ts'],
                'author':     e['author'],
                'direction':  e['direction'],
                'text':       e['text'],
            }
            if 'slug' in e:
                rec['slug'] = e['slug']
            for tok_key in ('tokens_in', 'tokens_out', 'tokens_cache_read', 'tokens_cache_create'):
                if e.get(tok_key, 0):
                    rec[tok_key] = e[tok_key]
            out.write(json.dumps(rec, ensure_ascii=False) + '\n')
        for e in all_tool_events:
            rec = {
                'type':           'tool_event',
                'channel':        e.get('channel', ''),
                'chat_id':        e['session_id'],
                'tool_use_id':    e['tool_use_id'],
                'ts':             e['ts'],
                'tool_name':      e['tool_name'],
                'input':          e['input'],
                'result_preview': e['result_preview'],
                'is_error':       e['is_error'],
            }
            if 'slug' in e:
                rec['slug'] = e['slug']
            out.write(json.dumps(rec, ensure_ascii=False) + '\n')

    # Persist updated cache (merge old unchanged entries + new entries)
    if cache_path is not None:
        merged_cache = {**cache, **new_cache_entries}
        save_cache(cache_path, merged_cache)

    n_u = sum(1 for e in all_events if e['direction'] == 'in')
    n_a = sum(1 for e in all_events if e['direction'] == 'out')
    n_t = len(all_tool_events)
    print()
    print(f"Total   : {len(all_events)} chat events  (User: {n_u}, Claude: {n_a})")
    print(f"Tools   : {n_t} tool events")
    print(f"Sessions: {session_count} new/changed  ({skipped_count} unchanged, skipped)")
    print(f"Output  : {args.out}")
    if cache_path:
        print(f"Cache   : {cache_path}")


if __name__ == '__main__':
    main()
