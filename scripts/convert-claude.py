#!/usr/bin/env python3
"""
Convert Claude Code session JSONL(s) to chat_event rawUser format.

Usage:
  python convert-claude.py <session.jsonl>  <output.raw.jsonl>
  python convert-claude.py <sessions-dir/>  <output.raw.jsonl>

When given a directory, all *.jsonl files are processed; each session
becomes its own chat_id. Events are sorted by (session_id, timestamp)
so session groups stay contiguous for the time-gap chunker in
ingest-chatlog (use --gap-min 120 to guarantee clean session boundaries).

What is kept:
  User side    : type=="user", not isMeta, plain string content that does
                 not start with "<" (strips tool results, slash commands,
                 local command output, system reminders, etc.)
  Claude side  : type=="assistant", deduplicated by message ID (last
                 streaming chunk = complete content), only "text" blocks
                 (strips thinking, tool_use, progress entries).
  Tool calls   : type=="tool_event" sidecar records pairing tool_use blocks
                 from assistant messages with tool_result blocks from user
                 messages. Parameters are truncated per-tool; results are
                 previewed at 200 characters.
"""

import json
import sys
from pathlib import Path
from typing import Optional


RESULT_PREVIEW_LEN = 200
COMMAND_PREVIEW_LEN = 200
FILE_PATH_PREVIEW_LEN = 200
EDIT_PREVIEW_LEN = 50


def truncate(value: str, max_len: int) -> str:
    if len(value) <= max_len:
        return value
    return value[:max_len] + '...'


def truncate_tool_input(tool_name: str, raw_input: dict) -> dict:
    """Return a truncated copy of tool input parameters, per-tool."""
    if tool_name == 'Bash':
        return {'command': truncate(raw_input.get('command', ''), COMMAND_PREVIEW_LEN)}
    if tool_name == 'Read':
        return {'file_path': raw_input.get('file_path', '')}
    if tool_name in ('Glob', 'Grep'):
        out: dict = {'pattern': raw_input.get('pattern', '')}
        if 'path' in raw_input:
            out['path'] = raw_input['path']
        return out
    if tool_name in ('Edit', 'Write'):
        out = {'file_path': raw_input.get('file_path', '')}
        if 'old_string' in raw_input:
            out['old_string'] = truncate(raw_input['old_string'], EDIT_PREVIEW_LEN)
        return out
    if tool_name == 'Agent':
        out = {}
        if 'description' in raw_input:
            out['description'] = raw_input['description']
        if 'subagent_type' in raw_input:
            out['subagent_type'] = raw_input['subagent_type']
        return out
    # Fallback: keep keys but truncate all string values
    return {
        k: truncate(str(v), COMMAND_PREVIEW_LEN) if isinstance(v, str) else v
        for k, v in raw_input.items()
    }


def truncate_result(content: str | list, is_error: bool) -> str:
    """Return a truncated preview of tool result content."""
    if isinstance(content, list):
        # Content blocks — extract text
        parts = []
        for block in content:
            if isinstance(block, dict) and 'text' in block:
                parts.append(block['text'])
            elif isinstance(block, str):
                parts.append(block)
        text = '\n'.join(parts)
    elif isinstance(content, str):
        text = content
    else:
        text = str(content) if content else ''
    if is_error:
        return f'ERROR: {truncate(text, RESULT_PREVIEW_LEN)}'
    return truncate(text, RESULT_PREVIEW_LEN)


def extract_tool_events(
    asst_by_id: dict[str, dict],
    tool_results: dict[str, dict],
    session_id: str,
) -> list[dict]:
    """Pair tool_use blocks from assistant messages with tool_result blocks.

    Returns a list of tool_event dicts ready for JSONL output.
    """
    events: list[dict] = []
    for _mid, obj in asst_by_id.items():
        content = obj.get('message', {}).get('content', [])
        if not isinstance(content, list):
            continue
        ts = obj.get('timestamp', '')
        for block in content:
            if block.get('type') != 'tool_use':
                continue
            tool_use_id = block.get('id', '')
            tool_name = block.get('name', '')
            raw_input = block.get('input', {})

            result_info = tool_results.get(tool_use_id, {})
            result_content = result_info.get('content', '')
            is_error = bool(result_info.get('is_error', False))

            events.append({
                'ts': ts,
                'session_id': session_id,
                'tool_use_id': tool_use_id,
                'tool_name': tool_name,
                'input': truncate_tool_input(tool_name, raw_input),
                'result_preview': truncate_result(result_content, is_error),
                'is_error': is_error,
            })
    return events


def is_real_user_message(obj: dict) -> bool:
    """True only for genuine human-typed messages."""
    if obj.get('type') != 'user':
        return False
    if obj.get('isMeta'):
        return False
    content = obj.get('message', {}).get('content', '')
    # Tool results arrive as arrays, not strings.
    if not isinstance(content, str):
        return False
    if not content.strip():
        return False
    # XML-wrapped entries: system reminders, local command output,
    # slash command plumbing, etc.
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


def collect_tool_results(obj: dict) -> dict[str, dict]:
    """Extract tool_result blocks from a user message with array content.

    Returns {tool_use_id: {content, is_error}} for each result block.
    """
    content = obj.get('message', {}).get('content', '')
    if not isinstance(content, list):
        return {}
    results: dict[str, dict] = {}
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get('type') != 'tool_result':
            continue
        tuid = block.get('tool_use_id', '')
        if tuid:
            results[tuid] = {
                'content': block.get('content', ''),
                'is_error': block.get('is_error', False),
            }
    return results


def process_session(path: Path) -> tuple[list[dict], list[dict]]:
    """Process one .jsonl file and return (chat_events, tool_events)."""
    user_events: list[dict] = []
    asst_by_id: dict[str, dict] = {}   # message id -> last streaming chunk
    all_tool_results: dict[str, dict] = {}  # tool_use_id -> result info

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
                # Also collect tool results from array-content user messages
                all_tool_results.update(collect_tool_results(obj))

            elif t == 'assistant':
                mid = obj.get('message', {}).get('id', '')
                if mid:
                    asst_by_id[mid] = obj   # last chunk = complete content

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
            events.append({
                'ts':         obj['timestamp'],
                'session_id': obj.get('sessionId', session_id),
                'message_id': mid,
                'direction':  'out',
                'author':     'Claude',
                'text':       text,
            })

    # Extract tool events by pairing tool_use with tool_result
    effective_session_id = next(
        (obj.get('sessionId', session_id) for obj in user_events),
        session_id,
    )
    tool_events = extract_tool_events(asst_by_id, all_tool_results, effective_session_id)

    return events, tool_events


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


def convert(input_path: str, output_path: str, no_cache: bool = False) -> None:
    p = Path(input_path)
    out_path = Path(output_path)
    is_dir_mode = p.is_dir()

    if is_dir_mode:
        files = sorted(p.glob('*.jsonl'))
    elif p.is_file():
        files = [p]
    else:
        print(f"ERROR: '{input_path}' is not a file or directory.", file=sys.stderr)
        sys.exit(1)

    # Skip zero-byte files (Claude Code creates these for empty sessions).
    files = [f for f in files if f.stat().st_size > 0]

    if not files:
        print(f"No .jsonl files found in '{input_path}'.", file=sys.stderr)
        sys.exit(1)

    # Incremental cache (directory mode only; single-file mode always rebuilds)
    cache_path: Optional[Path] = None
    cache: dict = {}
    if is_dir_mode and not no_cache:
        cache_path = Path(str(out_path) + '.cache.json')
        cache = load_cache(cache_path)

    incremental = bool(cache) and out_path.is_file() and cache_path is not None
    write_mode = 'a' if incremental else 'w'
    if incremental:
        print(f"Mode: incremental (append to existing output)")

    all_events: list[dict] = []
    all_tool_events: list[dict] = []
    new_cache_entries: dict = {}
    skipped = 0

    for f in files:
        if cache_path is not None and is_cached(f, cache):
            skipped += 1
            new_cache_entries[str(f)] = cache[str(f)]
            continue
        session_events, tool_events = process_session(f)
        n_u = sum(1 for e in session_events if e['direction'] == 'in')
        n_a = sum(1 for e in session_events if e['direction'] == 'out')
        n_t = len(tool_events)
        if n_u + n_a + n_t > 0:
            print(f"  {f.name[:36]}  user={n_u}  claude={n_a}  tools={n_t}")
        all_events.extend(session_events)
        all_tool_events.extend(tool_events)
        if cache_path is not None:
            new_cache_entries[str(f)] = file_fingerprint(f)

    if not all_events and not all_tool_events:
        if skipped > 0:
            print(f"No new sessions (skipped {skipped} unchanged). Output unchanged.")
            return
        print("No usable messages found.", file=sys.stderr)
        sys.exit(1)

    # Sort by (session_id, ts) so each session's messages are contiguous.
    # This lets the time-gap chunker in ingest-chatlog see a clear boundary
    # between sessions rather than interleaving them by wall-clock time.
    all_events.sort(key=lambda e: (e['session_id'], e['ts']))
    all_tool_events.sort(key=lambda e: (e['session_id'], e['ts']))

    channel = 'claude-code'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, write_mode, encoding='utf-8') as out:
        for e in all_events:
            rec = {
                'type':       'chat_event',
                'channel':    channel,
                'chat_id':    e['session_id'],
                'message_id': e['message_id'],
                'ts':         e['ts'],
                'author':     e['author'],
                'direction':  e['direction'],
                'text':       e['text'],
            }
            out.write(json.dumps(rec, ensure_ascii=False) + '\n')
        for e in all_tool_events:
            rec = {
                'type':        'tool_event',
                'channel':     channel,
                'chat_id':     e['session_id'],
                'tool_use_id': e['tool_use_id'],
                'ts':          e['ts'],
                'tool_name':   e['tool_name'],
                'input':       e['input'],
                'result_preview': e['result_preview'],
                'is_error':    e['is_error'],
            }
            out.write(json.dumps(rec, ensure_ascii=False) + '\n')

    if cache_path is not None:
        merged = {**cache, **new_cache_entries}
        save_cache(cache_path, merged)

    n_u = sum(1 for e in all_events if e['direction'] == 'in')
    n_a = sum(1 for e in all_events if e['direction'] == 'out')
    n_t = len(all_tool_events)
    print(f"\nTotal   : {len(all_events)} chat events  (User: {n_u}, Claude: {n_a})")
    print(f"Tools   : {n_t} tool events")
    print(f"Sessions: {len(files) - skipped} new/changed  ({skipped} unchanged, skipped)")
    print(f"Output  : {output_path}")
    if cache_path:
        print(f"Cache   : {cache_path}")


if __name__ == '__main__':
    import argparse as _ap
    _parser = _ap.ArgumentParser(description='Convert Claude session JSONL to chat_event rawUser format.')
    _parser.add_argument('input', help='Session .jsonl file or directory of .jsonl files')
    _parser.add_argument('output', help='Output .raw.jsonl path')
    _parser.add_argument('--no-cache', action='store_true',
                         help='Disable incremental caching; always do a full rebuild.')
    _args = _parser.parse_args()
    convert(_args.input, _args.output, no_cache=_args.no_cache)
