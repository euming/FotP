#!/usr/bin/env python3
"""
generate-claude-md.py

Reads an AMS .memory.ams.json file that has been processed by the Dreaming
pipeline and writes a CLAUDE.local.md into a target directory.

Claude Code automatically loads CLAUDE.local.md alongside CLAUDE.md, giving
the agent context synthesized from past session history without touching the
committed, shared CLAUDE.md.

Usage:
  python generate-claude-md.py \\
      --ams-json output/AMS/AMS.memory.ams.json \\
      --project-name AMS \\
      --out-dir C:/Users/eumin/wkspaces/git/AMS

  python generate-claude-md.py \\
      --ams-json output/AMS/AMS.memory.ams.json \\
      --project-name AMS \\
      --out-dir C:/Users/eumin/wkspaces/git/AMS \\
      --dry-run
"""

import argparse
import hashlib
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from ams_common import (
    claude_local_drilldown_lines,
    memory_command_label,
    validate_claude_local_contract,
)


# ---------------------------------------------------------------------------
# AMS reader — dream winner objects
# ---------------------------------------------------------------------------

def load_ams_dream_objects(ams_json_path: Path) -> dict[str, list[dict]]:
    """
    Parse an AMS .ams.json snapshot and return dream winner objects by kind.

    Only objects from the MOST RECENT dream run are included, identified by
    matching provenance.run_id against the latest dreamrun object's createdAt.
    This prevents stale objects from prior runs (different corpus hashes)
    from polluting the output.

    The snapshot uses camelCase (JsonNamingPolicy.CamelCase).
    Canonical nodes have objectKind in {topic, thread, decision, invariant}
    and store label/vote_score/run_id in semanticPayload.provenance.

    Returns:
        {
            'topic':     [{'label': str, 'vote_score': float}, ...],
            'thread':    [...],
            'decision':  [...],
            'invariant': [...],
        }
    Sorted by vote_score desc, label asc (matches reconciler output order).
    """
    empty: dict[str, list[dict]] = {'topic': [], 'thread': [], 'decision': [], 'invariant': []}

    if not ams_json_path.exists():
        return empty

    # C# File.WriteAllText with System.Text.Encoding.UTF8 emits a BOM;
    # 'utf-8-sig' handles that transparently.
    with open(ams_json_path, encoding='utf-8-sig') as f:
        snapshot = json.load(f)

    objects = snapshot.get('objects', [])

    # Find the latest dream run by comparing createdAt timestamps on dreamrun objects.
    # AmsWriterAdapter.UpsertObject(objectId=runId.Value, objectKind="dreamrun", ...)
    # — one per distinct corpus hash + K configuration.
    dreamrun_objects = [o for o in objects if o.get('objectKind') == 'dreamrun']
    if not dreamrun_objects:
        return empty

    latest_run = max(dreamrun_objects, key=lambda o: o.get('createdAt', ''))
    latest_run_id: str = latest_run.get('objectId', '')

    # AmsWriterAdapter.CreateSemanticNode writes run_id into provenance.
    # UpsertObject means a node that won in multiple consecutive runs always carries
    # the run_id of the most recent run that touched it.
    result: dict[str, list[dict]] = {k: [] for k in empty}
    kinds = set(result)

    container_ids: set[str] = {
        c.get('containerId', '') for c in snapshot.get('containers', [])
    }

    for obj in objects:
        obj_id   = obj.get('objectId', '')
        if obj_id in container_ids:
            continue  # skip membership-container backing objects

        obj_kind = obj.get('objectKind', '')
        if obj_kind not in kinds:
            continue

        payload  = obj.get('semanticPayload') or {}
        prov     = payload.get('provenance') or {}

        # Only include nodes from the latest run.
        if prov.get('run_id') != latest_run_id:
            continue

        label      = prov.get('label')
        vote_score = prov.get('vote_score')

        if label is None:
            label = payload.get('summary') or obj_id

        result[obj_kind].append({
            'label':      str(label),
            'vote_score': float(vote_score) if vote_score is not None else 0.0,
        })

    for kind in result:
        result[kind].sort(key=lambda x: (-x['vote_score'], x['label']))

    return result


def load_working_memory(ams_json_path: Path) -> list[dict]:
    """
    Read the working-memory container from AMS JSON and return ordered dream objects.

    Returns list of {id, kind, label} dicts in ranked order (best first),
    or empty list if the working-memory SmartList is absent.
    """
    if not ams_json_path.exists():
        return []

    with open(ams_json_path, encoding='utf-8-sig') as f:
        snap = json.load(f)

    obj_map  = {o['objectId']: o for o in snap.get('objects', [])}
    link_map = {ln['linkNodeId']: ln for ln in snap.get('linkNodes', [])}

    # Find working-memory container
    wm_container = next(
        (c for c in snap.get('containers', []) if c.get('containerId') == 'working-memory'),
        None,
    )
    if not wm_container:
        return []

    # Walk head -> tail chain
    head = wm_container.get('headLinknodeId')
    if not head:
        return []

    result: list[dict] = []
    curr = head
    visited: set[str] = set()
    while curr and curr not in visited:
        visited.add(curr)
        ln = link_map.get(curr)
        if not ln:
            break
        oid = ln.get('objectId', '')
        obj = obj_map.get(oid, {})
        kind = obj.get('objectKind', '')
        payload = obj.get('semanticPayload') or {}
        prov = payload.get('provenance') or {}
        label = payload.get('summary') or prov.get('label') or oid
        result.append({'id': oid, 'kind': kind, 'label': str(label)})
        curr = ln.get('nextLinknodeId')

    return result


def count_chat_sessions(ams_json_path: Path) -> int:
    """Count chat_session containers in the AMS snapshot."""
    if not ams_json_path.exists():
        return 0
    with open(ams_json_path, encoding='utf-8-sig') as f:
        snapshot = json.load(f)
    return sum(
        1 for c in snapshot.get('containers', [])
        if c.get('containerKind') == 'chat_session'
    )


# ---------------------------------------------------------------------------
# AMS reader — recent session content
# ---------------------------------------------------------------------------

def load_sessions_with_content(ams_json_path: Path, max_sessions: int = 8) -> list[dict]:
    """
    Return the most recent chat sessions, each annotated with the first
    substantive user message found by walking the session's link chain.

    A 'substantive' message has >= 25 characters.  Falls back to the first
    user message of any length if none qualify.

    Returns a list of dicts sorted by started_at descending (newest first):
        [{'started_at': str (ISO 8601), 'snippet': str}, ...]
    """
    if not ams_json_path.exists():
        return []

    with open(ams_json_path, encoding='utf-8-sig') as f:
        snap = json.load(f)

    obj_by_id  = {o['objectId']: o for o in snap.get('objects', [])}
    link_by_id = {l['linkNodeId']: l for l in snap.get('linkNodes', [])}

    sessions: list[dict] = []
    for c in snap.get('containers', []):
        if c.get('containerKind') != 'chat_session':
            continue
        meta = c.get('metadata') or {}

        # Walk the doubly-linked message chain, collecting user messages.
        # Stop after 120 nodes to bound cost on very large sessions.
        user_msgs: list[str] = []
        cur_id  = c.get('headLinknodeId')
        visited = 0
        while cur_id and visited < 120:
            ln = link_by_id.get(cur_id)
            if not ln:
                break
            obj = obj_by_id.get(ln.get('objectId', ''), {})
            pay = obj.get('semanticPayload') or {}
            prov = pay.get('provenance') or {}
            if prov.get('direction') == 'in':
                text = prov.get('text', '').strip()
                if text:
                    user_msgs.append(text)
            cur_id  = ln.get('nextLinknodeId')
            visited += 1

        # Prefer the first message >= 25 chars; fall back to first message.
        snippet = next((m for m in user_msgs if len(m) >= 25), user_msgs[0] if user_msgs else '')

        sessions.append({
            'started_at': meta.get('started_at', ''),
            'snippet':    snippet,
            'title':      meta.get('title', ''),
        })

    sessions.sort(key=lambda s: s['started_at'], reverse=True)

    # Deduplicate: if two consecutive sessions share the same snippet prefix
    # (first 60 chars), only keep the first occurrence.  This happens when the
    # same initial message was reused across short reconnect sessions.
    seen_prefixes: set[str] = set()
    deduped: list[dict] = []
    for sess in sessions:
        prefix = sess['snippet'][:60]
        if prefix not in seen_prefixes:
            seen_prefixes.add(prefix)
            deduped.append(sess)

    return deduped[:max_sessions]


def load_chat_message_objects(ams_json_path: Path) -> list[dict]:
    """
    Return all chat_message objects from the AMS snapshot.
    Used by find_keyword_snippet to locate representative excerpts.
    """
    if not ams_json_path.exists():
        return []
    with open(ams_json_path, encoding='utf-8-sig') as f:
        snap = json.load(f)
    return [o for o in snap.get('objects', []) if o.get('objectKind') == 'chat_message']


# ---------------------------------------------------------------------------
# Snippet helper
# ---------------------------------------------------------------------------

def find_top_n_snippets(keyword: str, chat_objects: list[dict], n: int = 3, max_len: int = 110) -> list[str]:
    """
    Return up to n distinct non-overlapping excerpts containing keyword.
    De-duplicates on 40-char prefix so similar passages don't repeat.
    Prefers user messages (direction='in') then falls back to assistant messages.
    """
    words = keyword.split()
    if len(words) == 1:
        pattern = re.compile(r'\b' + re.escape(keyword) + r'\b', re.IGNORECASE)
    else:
        pattern = re.compile(re.escape(keyword), re.IGNORECASE)

    def _extract(text: str) -> str:
        m = pattern.search(text)
        if not m:
            return ''
        idx   = m.start()
        start = max(0, idx - 30)
        end   = min(len(text), idx + len(keyword) + 70)
        raw   = text[start:end].replace('\n', ' ').strip()
        prefix = '…' if start > 0 else ''
        suffix = '…' if end < len(text) else ''
        return (prefix + raw + suffix)[:max_len]

    results: list[str] = []
    seen_prefixes: set[str] = set()

    def _collect(objs):
        for obj in objs:
            if len(results) >= n:
                return
            pay  = obj.get('semanticPayload') or {}
            prov = pay.get('provenance') or {}
            excerpt = _extract(prov.get('text', ''))
            if excerpt:
                prefix = excerpt[:40]
                if prefix not in seen_prefixes:
                    seen_prefixes.add(prefix)
                    results.append(excerpt)

    user_msgs = [o for o in chat_objects
                 if (o.get('semanticPayload') or {}).get('provenance', {}).get('direction') == 'in']
    _collect(user_msgs)
    if len(results) < n:
        other_msgs = [o for o in chat_objects
                      if (o.get('semanticPayload') or {}).get('provenance', {}).get('direction') != 'in']
        _collect(other_msgs)

    return results


_DECISION_PHRASES = re.compile(
    r'\b(we decided|decision:|the approach is|going with|architecture:|we\'re using|we will use|chose to|settled on)\b',
    re.IGNORECASE,
)

def find_decision_snippets(chat_objects: list[dict], n: int = 5, max_len: int = 110) -> list[str]:
    """
    Scan all chat messages for sentences containing explicit decision language.
    Returns up to n distinct excerpts (de-duplicated on 40-char prefix).
    """
    results: list[str] = []
    seen: set[str] = set()
    for obj in chat_objects:
        if len(results) >= n:
            break
        pay  = obj.get('semanticPayload') or {}
        prov = pay.get('provenance') or {}
        text = prov.get('text', '')
        m = _DECISION_PHRASES.search(text)
        if not m:
            continue
        idx   = m.start()
        start = max(0, idx - 20)
        end   = min(len(text), idx + 90)
        raw   = text[start:end].replace('\n', ' ').strip()
        prefix = '…' if start > 0 else ''
        suffix = '…' if end < len(text) else ''
        excerpt = (prefix + raw + suffix)[:max_len]
        key = excerpt[:40]
        if key not in seen:
            seen.add(key)
            results.append(excerpt)
    return results


def find_keyword_snippet(keyword: str, chat_objects: list[dict], max_len: int = 110) -> str:
    """
    Search chat_message objects for a short excerpt that contains `keyword`
    (case-insensitive, whole-word for single-word labels).

    Returns a truncated excerpt (with leading/trailing ellipsis where text was
    cut), or empty string if the keyword is not found.

    Prefers user messages (direction='in') so the snippet reflects what the
    user asked, not what Claude answered.

    Uses word-boundary matching for single-word keywords to avoid false matches
    (e.g. 'main' matching 'maintain').  Multi-word phrases use substring search.
    """
    # Build a pattern: \b-boundary for single-word labels, plain substring otherwise.
    words = keyword.split()
    if len(words) == 1:
        pattern = re.compile(r'\b' + re.escape(keyword) + r'\b', re.IGNORECASE)
    else:
        pattern = re.compile(re.escape(keyword), re.IGNORECASE)

    def _extract(text: str) -> str:
        m = pattern.search(text)
        if not m:
            return ''
        idx    = m.start()
        start  = max(0, idx - 30)
        end    = min(len(text), idx + len(keyword) + 70)
        raw    = text[start:end].replace('\n', ' ').strip()
        prefix = '…' if start > 0 else ''
        suffix = '…' if end < len(text) else ''
        return (prefix + raw + suffix)[:max_len]

    # First pass: prefer user messages
    for obj in chat_objects:
        pay  = obj.get('semanticPayload') or {}
        prov = pay.get('provenance') or {}
        if prov.get('direction') != 'in':
            continue
        excerpt = _extract(prov.get('text', ''))
        if excerpt:
            return excerpt

    # Second pass: accept assistant messages
    for obj in chat_objects:
        pay  = obj.get('semanticPayload') or {}
        prov = pay.get('provenance') or {}
        excerpt = _extract(prov.get('text', ''))
        if excerpt:
            return excerpt

    return ''


# ---------------------------------------------------------------------------
# Atlas summary loader
# ---------------------------------------------------------------------------

def load_atlas_summary(
    ams_db_path: 'Path | None',
    topic_keywords: list[str] | None = None,
    max_chars: int = 500,
) -> str:
    """
    Call `memoryctl atlas-page --db <path> --page-id atlas:0` and return
    the stdout text (truncated to max_chars), or empty string if unavailable.

    atlas:0 is a synthetic multi-resolution summary page planned for Phase 3.
    Falls back to `atlas-search` with top dream topic keywords when atlas:0
    is not yet implemented, providing a coarser but useful summary.

    This function is wired in now so the section appears in CLAUDE.local.md
    once the command is implemented (and is already useful via fallback).
    """
    if ams_db_path is None or not ams_db_path.exists():
        return ''

    # Locate memoryctl binary relative to this script (tools/memoryctl/src)
    script_dir  = Path(__file__).parent
    repo_root   = script_dir.parent
    memoryctl_proj = repo_root / 'tools' / 'memoryctl' / 'src' / 'MemoryCtl.csproj'

    def _run_memoryctl(*extra_args: str) -> str:
        if memoryctl_proj.exists():
            cmd = ['dotnet', 'run', '--project', str(memoryctl_proj), '--', *extra_args]
        else:
            cmd = ['memoryctl', *extra_args]
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if r.returncode == 0 and r.stdout.strip():
                return r.stdout.strip()
        except Exception:
            pass
        return ''

    # Primary: atlas:0 multi-resolution page
    text = _run_memoryctl('atlas-page', '--db', str(ams_db_path), '--page-id', 'atlas:0')

    # Fallback: atlas-search with top dream topic keywords
    if not text and topic_keywords:
        q = ' '.join(topic_keywords[:5])
        text = _run_memoryctl('atlas-search', '--db', str(ams_db_path), '--q', q, '--top', '5')

    if not text:
        return ''

    # Truncate to keep CLAUDE.local.md compact
    if len(text) > max_chars:
        text = text[:max_chars].rstrip() + '\n…'
    return text


# ---------------------------------------------------------------------------
# CLAUDE.local.md generator
# ---------------------------------------------------------------------------

def generate_claude_local_md(
    dream_objects:   dict[str, list[dict]],
    project_name:    str,
    session_count:   int,
    recent_sessions: list[dict] | None = None,
    chat_objects:    list[dict] | None = None,
    ams_json_path:   'Path | None' = None,
    working_memory:  list[dict] | None = None,
    atlas_summary:   str = '',
) -> str:
    """
    Build the CLAUDE.local.md content from dream winners and raw session data.

    Sections:
      1. Recent Sessions  — last N sessions with first substantive user message
      2. Key Topics       — dream topic winners with representative excerpts
      3. Active Threads   — dream thread winners with excerpts
      4. Decisions Made   — dream decision winners with excerpts
      5. Stable Invariants — dream invariant winners with excerpts

    The four dream sections map directly to AMS Dreaming candidate kinds:
      topic → Key Topics, thread → Active Threads,
      decision → Decisions Made, invariant → Stable Invariants
    """
    memory_cmd = memory_command_label()
    recent_sessions = recent_sessions or []
    chat_objects    = chat_objects    or []
    working_memory  = working_memory  or []

    now     = datetime.now().strftime('%Y-%m-%d %H:%M')
    has_any = any(len(v) > 0 for v in dream_objects.values())

    lines: list[str] = [
        '<!--',
        '  AUTO-GENERATED -- do not edit by hand.',
        '  Tool   : maintain-claude-memory / AMS Dreaming v0.1',
        f'  Project: {project_name}',
        f'  Updated: {now}',
        f'  Source : {session_count} ingested chat sessions',
        '  Add CLAUDE.local.md to .gitignore to keep this out of version control.',
        '-->',
        '',
        f'# AI Memory: {project_name}',
        '',
        ('This file is auto-generated from Claude Code session history using '
         'AMS Dreaming. It gives Claude immediate, token-efficient context at '
         'the start of each session without re-reading source files.'),
        '',
    ]

    # ── Section 0: Working Memory ─────────────────────────────────────────────
    if working_memory:
        lines += [
            '## Working Memory',
            'Most relevant to current work (recency × relevance):',
        ]
        for item in working_memory:
            lines.append(f'- [{item["kind"]}] {item["label"]}')
        lines.append('')

    # ── Section 1: Recent Sessions ────────────────────────────────────────────
    if recent_sessions:
        lines += [
            '## Recent Sessions',
            'Most recent development sessions (newest first):',
        ]
        def _format_title_badge(title: str) -> str:
            import re
            m = re.match(r'^(.+?)\s*\|\s*.+\([\d-]+,\s*(\d+)\s*msgs\)', title)
            if not m:
                m = re.match(r'^(.+?)\s+\d{4}-\d{2}-\d{2}.*?\((\d+)\s*msgs\)', title)
            if m:
                ident = m.group(1).strip()
                count = m.group(2)
                if re.match(r'^[0-9a-f]{8}-', ident):
                    ident = ident[:8]
                return f'*({ident} | {count} msgs)*'
            return f'*(title: "{title.replace(chr(10), " ").replace(chr(13), "")[:80]}")*'

        for sess in recent_sessions:
            date    = sess['started_at'][:10]  # YYYY-MM-DD
            snippet = sess['snippet'].replace('\n', ' ')
            title   = sess.get('title', '').strip()
            if len(snippet) > 300:
                snippet = snippet[:297] + '…'
            if snippet:
                lines.append(f'- **{date}**: {snippet}')
                if title:
                    lines.append(f'  {_format_title_badge(title)}')
            else:
                lines.append(f'- **{date}**: *(no user messages)*')
        lines.append('')

    if not has_any:
        lines += [
            '> No dream candidates found yet.',
            '> Run more sessions then re-run `maintain-claude-memory.bat`.',
            '',
            '> You can still query AMS memory directly.',
            '',
            '## Drill-Down',
            *claude_local_drilldown_lines(),
        ]
        content = '\n'.join(lines)
        violations = validate_claude_local_contract(content)
        if violations:
            raise ValueError(" ; ".join(violations))
        return content

    # ── Dream sections ────────────────────────────────────────────────────────
    # Labels only — semantic search (via memory-search skill) injects relevant
    # cards on demand, so we keep this section compact.

    def label_section(title: str, intro: str, items: list[dict]) -> list[str]:
        if not items:
            return []
        labels = ', '.join(it['label'] for it in items)
        return [f'## {title}', intro, f'- {labels}', '']

    lines += label_section(
        'Key Topics',
        'Recurring themes (use `/memory-search <topic>` for details):',
        dream_objects['topic'],
    )

    # ── Atlas Summary (atlas:0 multi-resolution page) ─────────────────────────
    if atlas_summary:
        lines += [
            '## Atlas Summary',
            f'*(Multi-resolution memory index — `{memory_cmd} atlas page atlas:0`)*',
            '',
            '```',
            atlas_summary,
            '```',
            '',
        ]

    lines += label_section(
        'Active Threads',
        'Current work patterns (use `/memory-search <thread>` for details):',
        dream_objects['thread'],
    )
    lines += label_section(
        'Decisions Made',
        'Architectural decisions (use `/memory-search <decision>` for details):',
        dream_objects['decision'],
    )
    lines += label_section(
        'Stable Invariants',
        'Constraints that appear consistently (use `/memory-search <term>` for details):',
        dream_objects['invariant'],
    )

    # ── Drill-Down footer ─────────────────────────────────────────────────────
    lines += [
        '## Drill-Down',
        *claude_local_drilldown_lines(),
    ]

    content = '\n'.join(lines)
    violations = validate_claude_local_contract(content)
    if violations:
        raise ValueError(" ; ".join(violations))
    return content


def ensure_archive_copy(out_dir: Path, content: str, source_ams: Path | None = None) -> Path:
    """
    Persist a timestamped snapshot of generated markdown under memory.archive.

    This keeps markdown as an emergency recovery surface during SmartList-first
    cutover without letting it remain the primary runtime memory path.
    """
    archive_dir = out_dir / 'memory.archive' / 'claude-local'
    archive_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now().strftime('%Y%m%d-%H%M%S')
    digest = hashlib.sha1(content.encode('utf-8')).hexdigest()[:10]
    archive_path = archive_dir / f'CLAUDE.local.{now}.{digest}.md'
    archive_path.write_text(content, encoding='utf-8')

    latest_path = archive_dir / 'LATEST.md'
    latest_path.write_text(content, encoding='utf-8')

    manifest = {
        'written_at': datetime.now().isoformat(timespec='seconds'),
        'archive_file': archive_path.name,
        'latest_file': latest_path.name,
        'source_ams_json': str(source_ams) if source_ams else '',
        'sha1': digest,
    }
    (archive_dir / 'LATEST.json').write_text(
        json.dumps(manifest, indent=2),
        encoding='utf-8',
    )
    return archive_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Generate CLAUDE.local.md from AMS Dreaming output.')
    parser.add_argument('--ams-json', type=Path, required=True,
                        help='Path to the .memory.ams.json file produced by the dream step.')
    parser.add_argument('--project-name', required=True,
                        help='Human-readable project name used in the header.')
    parser.add_argument('--out-dir', type=Path, required=True,
                        help='Directory to write CLAUDE.local.md into (typically the repo root).')
    parser.add_argument('--recent-sessions', type=int, default=8,
                        help='Number of recent sessions to include in the output (default: 8).')
    parser.add_argument('--ams-db', type=Path, default=None,
                        help='Path to the AMS .db file (used for atlas-page atlas:0 summary).')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print the generated content; do not write any files.')
    args = parser.parse_args()

    if not args.ams_json.exists():
        print(f'ERROR: AMS json not found: {args.ams_json}', file=sys.stderr)
        sys.exit(1)

    dream_objects   = load_ams_dream_objects(args.ams_json)
    session_count   = count_chat_sessions(args.ams_json)
    recent_sessions = load_sessions_with_content(args.ams_json, max_sessions=args.recent_sessions)
    chat_objects    = load_chat_message_objects(args.ams_json)
    working_memory  = load_working_memory(args.ams_json)
    topic_keywords  = [t['label'] for t in dream_objects.get('topic', [])]
    atlas_summary   = load_atlas_summary(args.ams_db, topic_keywords=topic_keywords)

    content = generate_claude_local_md(
        dream_objects   = dream_objects,
        project_name    = args.project_name,
        session_count   = session_count,
        recent_sessions = recent_sessions,
        chat_objects    = chat_objects,
        ams_json_path   = args.ams_json,
        working_memory  = working_memory,
        atlas_summary   = atlas_summary,
    )

    if args.dry_run:
        # sys.stdout may use a narrow encoding on Windows; reconfigure to utf-8.
        sys.stdout.reconfigure(encoding='utf-8')  # type: ignore[union-attr]
        print(content)
        return

    if not args.out_dir.is_dir():
        print(f'WARNING: out-dir does not exist: {args.out_dir} — skipping write.', file=sys.stderr)
        sys.exit(1)

    out_path = args.out_dir / 'CLAUDE.local.md'
    out_path.write_text(content, encoding='utf-8')
    archive_path = ensure_archive_copy(args.out_dir, content, args.ams_json)
    print(f'  Written : {out_path}')
    print(f'  Archived: {archive_path}')

    gitignore = args.out_dir / '.gitignore'
    if gitignore.exists():
        if 'CLAUDE.local.md' not in gitignore.read_text(encoding='utf-8'):
            print(f'  TIP     : Add "CLAUDE.local.md" to {gitignore}')


if __name__ == '__main__':
    main()
