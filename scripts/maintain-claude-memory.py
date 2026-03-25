#!/usr/bin/env python3
"""
maintain-claude-memory.py

Full per-project pipeline orchestrator:
  1. Discover projects in ~/.claude/projects/
  2. For each project (or the one named by --project):
       a. Convert Claude Code sessions to chat_event raw JSONL
       b. Ingest into a per-project AMS database  (via memoryctl ingest-chatlog)
       c. Run Dreaming pipeline                   (via memoryctl dream)
       d. Write CLAUDE.local.md into the repo     (via generate-claude-md.py)

CLAUDE.local.md is automatically loaded by Claude Code alongside the
committed CLAUDE.md, giving the agent synthesized context from session
history without touching version-controlled files.

Usage:
  python maintain-claude-memory.py
  python maintain-claude-memory.py --project AMS
  python maintain-claude-memory.py --output-root D:\\memory\\output
  python maintain-claude-memory.py --dry-run
  python maintain-claude-memory.py --project AMS --topic-k 8 --thread-k 5

Called by maintain-claude-memory.bat — prefer the .bat wrapper on Windows.
"""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Session discovery & conversion  (inlined from ingest-all-claude-projects.py)
# ---------------------------------------------------------------------------

def derive_project_name(dir_name: str) -> str:
    """
    'C--Users-eumin-wkspaces-git-AMS'           -> 'AMS'
    'C--Users-eumin-wkspaces-git-NetworkGraphMemory' -> 'NetworkGraphMemory'
    Falls back to the last hyphen-delimited token when '-git-' is absent.
    """
    marker = '-git-'
    idx = dir_name.find(marker)
    if idx >= 0:
        return dir_name[idx + len(marker):]
    return dir_name.split('-')[-1]


def decode_project_dir_to_path(dir_name: str) -> Path | None:
    """
    Decode a Claude Code project-directory name back to the original repo path.

    Claude Code encodes paths by replacing ':' and path separators with '-':
      'C:\\Users\\eumin\\wkspaces\\git\\AMS'
        -> 'C--Users-eumin-wkspaces-git-AMS'

    Decoding:
      drive letter + '--' prefix  -> 'X:\\'
      subsequent '-'              -> '\\'

    Returns None if the name does not look like an encoded Windows path.
    Note: path components that contain literal hyphens will be mis-decoded.
    For typical repo paths this is not a problem.
    """
    m = re.match(r'^([A-Za-z])--(.*)', dir_name)
    if not m:
        return None
    drive = m.group(1).upper()
    rest  = m.group(2).replace('-', '\\')
    return Path(f'{drive}:\\{rest}')


def is_real_user_message(obj: dict) -> bool:
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
    content = obj.get('message', {}).get('content', [])
    if not isinstance(content, list):
        return ''
    return '\n'.join(
        b['text'] for b in content
        if b.get('type') == 'text' and b.get('text', '').strip()
    )


def extract_slug(path: Path) -> str | None:
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


def process_session(path: Path) -> list[dict]:
    user_events: list[dict] = []
    asst_by_id: dict[str, dict] = {}

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
            if t == 'user' and is_real_user_message(obj):
                user_events.append(obj)
            elif t == 'assistant':
                mid = obj.get('message', {}).get('id', '')
                if mid:
                    asst_by_id[mid] = obj

    events: list[dict] = []
    for obj in user_events:
        events.append({
            'ts':         obj['timestamp'],
            'session_id': obj.get('sessionId', path.stem),
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
                'session_id': obj.get('sessionId', path.stem),
                'message_id': mid,
                'direction':  'out',
                'author':     'Claude',
                'text':       text,
            })
    return events


def discover_sessions(projects_root: Path, project_filter: str | None = None):
    """Yield (project_name, session_jsonl_path) for all non-empty sessions."""
    for project_dir in sorted(projects_root.iterdir()):
        if not project_dir.is_dir():
            continue
        project_name = derive_project_name(project_dir.name)
        if project_filter and project_filter.lower() not in project_name.lower():
            continue
        for f in sorted(project_dir.glob('*.jsonl')):
            if f.stat().st_size > 0:
                yield project_name, f


def sessions_to_raw_jsonl(
    projects_root: Path,
    project_filter: str,
    out_path: Path,
) -> int:
    """
    Convert sessions for one project to a raw JSONL chat_event file.
    Returns the number of sessions written (0 = no usable content).
    """
    all_events: list[dict] = []
    session_count = 0

    for project_name, session_path in discover_sessions(projects_root, project_filter):
        slug   = extract_slug(session_path)
        events = process_session(session_path)
        channel = f'claude-code/{project_name}'
        for e in events:
            e['channel'] = channel
            if slug:
                e['slug'] = slug

        n_u = sum(1 for e in events if e['direction'] == 'in')
        n_a = sum(1 for e in events if e['direction'] == 'out')
        if n_u + n_a > 0:
            slug_label = f'  [{slug}]' if slug else ''
            print(f'    [{project_name}] {session_path.name[:40]}{slug_label}  u={n_u} a={n_a}')
            session_count += 1
        all_events.extend(events)

    if not all_events:
        return 0

    # Sort by timestamp so ingest-chatlog's gap-min detection works correctly.
    # Sorting by session_id would interleave sessions whose UUIDs don't sort
    # chronologically, causing backwards timestamps that fool gap detection and
    # merge distinct sessions together with wrong started_at metadata.
    all_events.sort(key=lambda e: e['ts'])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
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
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')

    return session_count


# ---------------------------------------------------------------------------
# Session pruning
# ---------------------------------------------------------------------------

# Sessions older than this many days are candidates for tombstone pruning.
PRUNE_SESSION_AGE_DAYS = 90


def prune_old_sessions(kernel, db_path: Path) -> None:
    """
    Run the session pruning pass:
      1. Find session_ref objects older than PRUNE_SESSION_AGE_DAYS.
      2. Write their IDs to a temp file.
      3. Call ams-core-kernel session-prune-batch.
      4. Log the result.

    If the kernel binary is unavailable, logs a warning and returns.
    The pruning pass is always non-fatal — failures are logged, not raised.
    """
    import tempfile
    from datetime import datetime, timezone, timedelta

    cmd_check = kernel('session-prune-batch', '--help')
    if cmd_check is None:
        print('  WARNING: ams-core-kernel not found — skipping prune-old-sessions.')
        return

    # Ask the kernel for a list of objects with object_kind=session_ref
    cmd = kernel('corpus-inspect', '--input', str(db_path), '--kind', 'session_ref', '--fields', 'object_id,created_at')
    if cmd is None:
        print('  WARNING: ams-core-kernel not found — skipping prune-old-sessions.')
        return

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f'  WARNING: corpus-inspect failed (rc={result.returncode}) — skipping prune-old-sessions.')
        return

    cutoff = datetime.now(timezone.utc) - timedelta(days=PRUNE_SESSION_AGE_DAYS)
    candidate_ids: list[str] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        created_at_str = obj.get('created_at') or obj.get('createdAt') or ''
        object_id = obj.get('object_id') or obj.get('objectId') or ''
        if not object_id or not created_at_str:
            continue
        try:
            created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            if created_at < cutoff:
                candidate_ids.append(object_id)
        except (ValueError, TypeError):
            continue

    if not candidate_ids:
        print(f'  prune-old-sessions: no sessions older than {PRUNE_SESSION_AGE_DAYS} days')
        return

    print(f'  prune-old-sessions: {len(candidate_ids)} candidate(s) older than {PRUNE_SESSION_AGE_DAYS} days')

    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
        tmp_path = f.name
        f.write('\n'.join(candidate_ids))

    try:
        prune_cmd = kernel('session-prune-batch', '--input', str(db_path), '--ids-file', tmp_path)
        if prune_cmd is None:
            print('  WARNING: ams-core-kernel not found — skipping session-prune-batch.')
            return
        prune_result = subprocess.run(prune_cmd, capture_output=True, text=True)
        output = prune_result.stdout + prune_result.stderr
        # Parse summary line: pruned=N skipped=M total=K
        summary = {k: v for k, v in (pair.split('=', 1) for pair in output.split() if '=' in pair)}
        pruned  = summary.get('pruned',  '?')
        skipped = summary.get('skipped', '?')
        print(f'  prune-old-sessions: pruned={pruned} skipped={skipped}')
        if prune_result.returncode != 0:
            print(f'  WARNING: session-prune-batch exited with code {prune_result.returncode}')
    finally:
        try:
            import os as _os
            _os.unlink(tmp_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Subprocess helpers
# ---------------------------------------------------------------------------

def run_cmd(cmd: list[str], step: str) -> bool:
    """Run a command, streaming output. Returns True on success."""
    print(f'    $ {" ".join(str(c) for c in cmd)}')
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f'  ERROR: {step} exited with code {result.returncode}')
        return False
    return True


def _get_corpus_version(kernel, db_path: Path) -> str | None:
    """Return the current corpus version hash, or None if the binary is unavailable."""
    cmd = kernel('search-corpus-version', '--input', str(db_path))
    if cmd is None:
        return None
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        if line.startswith('corpus_version='):
            return line.split('=', 1)[1].strip()
    return None


def _invalidate_search_cache_on_corpus_change(kernel, db_path: Path, old_version: str | None) -> None:
    """Invalidate search cache entries if the corpus version changed after ingest."""
    if old_version is None:
        return
    new_version = _get_corpus_version(kernel, db_path)
    if new_version is None or new_version == old_version:
        return
    cmd = kernel('search-cache-invalidate', '--input', str(db_path), '--corpus-version', old_version)
    if cmd is None:
        return
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f'  search cache invalidated (corpus version changed: {old_version} -> {new_version})')
    else:
        print(f'  WARNING: search-cache-invalidate failed (rc={result.returncode})')


# ---------------------------------------------------------------------------
# Per-project pipeline
# ---------------------------------------------------------------------------

def process_project(
    project_name: str,
    project_dir: Path,
    projects_root: Path,
    output_root: Path,
    memoryctl_proj: Path,
    generator_script: Path,
    topic_k: int,
    thread_k: int,
    decision_k: int,
    invariant_k: int,
    dry_run: bool,
    ingest_only: bool = False,
) -> bool:
    """Run the full 4-step pipeline for one project. Returns True on success."""
    print(f'\n{"=" * 60}')
    print(f'  Project : {project_name}')
    print(f'{"=" * 60}')

    out_dir = output_root / project_name
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_path    = out_dir / f'{project_name}.chat.raw.jsonl'
    db_path     = out_dir / f'{project_name}.memory.jsonl'
    cursor_path = out_dir / f'{project_name}.cursor.json'
    ams_json    = out_dir / f'{project_name}.memory.ams.json'

    # ── Step 1: Convert sessions ──────────────────────────────────────────
    print(f'\n  [1/4] Converting sessions...')
    n_sessions = sessions_to_raw_jsonl(projects_root, project_name, raw_path)
    if n_sessions == 0:
        print(f'  No usable sessions found for "{project_name}" — skipping.')
        return True
    print(f'  {n_sessions} sessions -> {raw_path.name}')

    # ── Step 2: Ingest into AMS ───────────────────────────────────────────
    print(f'\n  [2/4] Ingesting into AMS...')
    from ams_common import build_rust_ams_cmd as _kernel_for_cache
    _pre_ingest_corpus_version = _get_corpus_version(_kernel_for_cache, db_path)
    ok = run_cmd([
        'dotnet', 'run', '--project', str(memoryctl_proj), '--',
        'ingest-chatlog',
        '--db',       str(db_path),
        '--chatlog',  str(raw_path),
        '--cursor',   str(cursor_path),
        '--max',      '5000',
        '--gap-min',  '120',
    ], 'ingest-chatlog')
    if not ok:
        return False
    _invalidate_search_cache_on_corpus_change(_kernel_for_cache, db_path, _pre_ingest_corpus_version)

    if ingest_only:
        print('  --ingest-only: skipping dream/embed/generate steps.')
        return True

    # ── Step 3: Run Dreaming (Rust topology-based, GNUISNGNU v0.2) ───────────
    # Old: dotnet run ... dream  (C# stub — returns error, do not use)
    # New: ams-core-kernel dream-schedule + dream-cluster + dream-generate-md
    print(f'\n  [3/4] Running Dreaming pipeline (Rust)...')
    from ams_common import build_rust_ams_cmd
    kernel = build_rust_ams_cmd
    claude_md_out = project_dir / 'CLAUDE.local.md'

    if not dry_run:
        for sub_cmd, label in [
            (['dream-schedule', '--input', str(db_path)], 'dream-schedule'),
            (['dream-cluster',  '--input', str(db_path)], 'dream-cluster'),
            (['dream-generate-md', '--input', str(db_path), '--out', str(claude_md_out)], 'dream-generate-md'),
        ]:
            cmd = kernel(*sub_cmd)
            if cmd is None:
                print(f'  WARNING: ams-core-kernel not found — skipping {label}.')
                break
            ok = run_cmd(cmd, label)
            if not ok:
                print(f'  WARNING: {label} failed — continuing pipeline.')
    else:
        print('  Dry-run: skipping Rust dream steps.')

    if dry_run:
        print('  Dry-run: skipping CLAUDE.local.md generation.')
        return True

    # ── Step 4: Generate semantic embeddings ──────────────────────────────
    print(f'\n  [4/5] Generating semantic embeddings...')
    embeddings_path = ams_json.with_suffix('.embeddings.json')
    embed_script = generator_script.parent / 'embed-dream-cards.py'
    if embed_script.exists() and ams_json.exists():
        run_cmd([
            'python', str(embed_script),
            '--ams-json', str(ams_json),
            '--out',      str(embeddings_path),
        ], 'embed-dream-cards')
    else:
        print('  Skipping embed step (embed-dream-cards.py not found or no ams.json).')

    # ── Step 4b: Watts-Strogatz dream shortcut linker ─────────────────────
    if ams_json.exists() and embeddings_path.exists():
        cmd = kernel('dream-shortcut', '--input', str(ams_json), '--embeddings', str(embeddings_path))
        if cmd is not None:
            run_cmd(cmd, 'dream-shortcut')
        else:
            print('  WARNING: ams-core-kernel not found — skipping dream-shortcut.')
    else:
        print('  Skipping dream-shortcut (ams.json or embeddings not found).')

    # ── Step 4c: Prune old sessions ───────────────────────────────────────
    print(f'\n  [4c] Pruning old sessions (>= {PRUNE_SESSION_AGE_DAYS} days)...')
    prune_old_sessions(kernel, db_path)

    # ── Step 5: Generate CLAUDE.local.md ─────────────────────────────────
    print(f'\n  [5/5] Generating CLAUDE.local.md...')

    if not ams_json.exists():
        print(f'  WARNING: {ams_json.name} not found — dream may have produced no output.')
        return True

    # Resolve target repo directory
    repo_path = decode_project_dir_to_path(project_dir.name)
    if repo_path and repo_path.is_dir():
        target_dir = repo_path
    else:
        if repo_path:
            print(f'  WARNING: decoded path {repo_path} not on disk — writing to output dir.')
        target_dir = out_dir

    ok = run_cmd([
        'python', str(generator_script),
        '--ams-json',      str(ams_json),
        '--project-name',  project_name,
        '--out-dir',       str(target_dir),
        '--ams-db',        str(db_path),
    ], 'generate-claude-md')
    if not ok:
        return False

    # Suggest adding to .gitignore if missing
    gitignore = target_dir / '.gitignore'
    if gitignore.exists():
        if 'CLAUDE.local.md' not in gitignore.read_text(encoding='utf-8'):
            print(f'  TIP: Add "CLAUDE.local.md" to {gitignore}')

    return True


# ---------------------------------------------------------------------------
# Cross-domain pipeline  (all projects combined)
# ---------------------------------------------------------------------------

def process_cross_domain(
    projects_root: Path,
    output_root: Path,
    memoryctl_proj: Path,
    topic_k: int,
    thread_k: int,
    decision_k: int,
    invariant_k: int,
    dry_run: bool,
    ingest_only: bool = False,
) -> bool:
    """
    Combine sessions from ALL projects into one AMS database and run Dreaming.
    Output goes to <output_root>/../all-claude-projects/.
    Returns True on success.
    """
    print(f'\n{"=" * 60}')
    print('  Cross-domain: all projects combined')
    print(f'{"=" * 60}')

    out_dir = output_root.parent / 'all-claude-projects'
    out_dir.mkdir(parents=True, exist_ok=True)

    stem        = 'all-claude-projects'
    raw_path    = out_dir / f'{stem}.chat.raw.jsonl'
    db_path     = out_dir / f'{stem}.memory.jsonl'
    cursor_path = out_dir / f'{stem}.cursor.json'
    html_path   = out_dir / f'{stem}.ams-debug.html'

    # ── Step 1: Convert ALL sessions (no project filter) ─────────────────
    print(f'\n  [1/4] Converting all sessions...')
    n_sessions = sessions_to_raw_jsonl(projects_root, None, raw_path)
    if n_sessions == 0:
        print('  No usable sessions found — skipping cross-domain pipeline.')
        return True
    print(f'  {n_sessions} sessions -> {raw_path.name}')

    # ── Step 2: Ingest — always reset cursor to pick up latest raw data ───
    if cursor_path.exists():
        cursor_path.unlink()
    if db_path.exists():
        db_path.unlink()
    print(f'\n  [2/4] Ingesting into AMS...')
    from ams_common import build_rust_ams_cmd as _kernel_for_cache_cd
    _pre_ingest_corpus_version_cd = _get_corpus_version(_kernel_for_cache_cd, db_path)
    ok = run_cmd([
        'dotnet', 'run', '--project', str(memoryctl_proj), '--',
        'ingest-chatlog',
        '--db',      str(db_path),
        '--chatlog', str(raw_path),
        '--cursor',  str(cursor_path),
        '--max',     '10000',
        '--gap-min', '120',
    ], 'ingest-chatlog (cross-domain)')
    if not ok:
        return False
    _invalidate_search_cache_on_corpus_change(_kernel_for_cache_cd, db_path, _pre_ingest_corpus_version_cd)

    if ingest_only:
        print('  --ingest-only: skipping dream/embed/generate steps.')
        return True

    # ── Step 3: Dream (Rust topology-based, GNUISNGNU v0.2) ──────────────────
    # Old: dotnet run ... dream  (C# stub — returns error, do not use)
    print(f'\n  [3/4] Running Dreaming pipeline (Rust)...')
    if dry_run:
        print('  Dry-run: skipping Rust dream steps.')
    else:
        from ams_common import build_rust_ams_cmd
        kernel = build_rust_ams_cmd
        for sub_cmd, label in [
            (['dream-schedule', '--input', str(db_path)], 'dream-schedule (cross-domain)'),
            (['dream-cluster',  '--input', str(db_path)], 'dream-cluster (cross-domain)'),
        ]:
            cmd = kernel(*sub_cmd)
            if cmd is None:
                print(f'  WARNING: ams-core-kernel not found — skipping {label}.')
                break
            run_cmd(cmd, label)  # non-fatal — dreaming failure doesn't block HTML gen

    # ── Step 4: Generate semantic embeddings ──────────────────────────────
    ams_json = out_dir / f'{stem}.memory.ams.json'
    print(f'\n  [4/5] Generating semantic embeddings (cross-domain)...')
    embed_script = Path(__file__).parent / 'embed-dream-cards.py'
    if embed_script.exists() and ams_json.exists():
        embeddings_path = ams_json.with_suffix('.embeddings.json')
        run_cmd([
            'python', str(embed_script),
            '--ams-json', str(ams_json),
            '--out',      str(embeddings_path),
        ], 'embed-dream-cards (cross-domain)')
        # ── Step 4b: Watts-Strogatz dream shortcut linker ──────────────────
        if embeddings_path.exists():
            from ams_common import build_rust_ams_cmd as _kernel
            cmd = _kernel('dream-shortcut', '--input', str(ams_json), '--embeddings', str(embeddings_path))
            if cmd is not None:
                run_cmd(cmd, 'dream-shortcut (cross-domain)')
            else:
                print('  WARNING: ams-core-kernel not found — skipping dream-shortcut.')
    else:
        print('  Skipping embed step.')

    # ── Step 5: Regenerate HTML browser ───────────────────────────────────
    print(f'\n  [5/5] Generating HTML memory browser...')
    ok = run_cmd([
        'dotnet', 'run', '--project', str(memoryctl_proj), '--',
        'debug-ams',
        '--db',  str(db_path),
        '--out', str(html_path),
    ], 'debug-ams (cross-domain)')
    if not ok:
        return False

    print(f'  HTML browser: {html_path}')
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    default_projects_dir = (
        Path(os.environ.get('USERPROFILE', os.environ.get('HOME', '~')))
        / '.claude' / 'projects'
    )
    script_dir = Path(__file__).parent

    parser = argparse.ArgumentParser(
        description='Ingest -> Dream -> CLAUDE.local.md pipeline for Claude Code projects.')
    parser.add_argument(
        '--output-root', type=Path,
        default=script_dir / 'output' / 'per-project',
        help='Root directory for per-project intermediate files. '
             'Default: scripts/output/per-project/')
    parser.add_argument(
        '--projects-dir', type=Path, default=default_projects_dir,
        help='Root of Claude Code project directories. '
             'Default: %%USERPROFILE%%\\.claude\\projects')
    parser.add_argument(
        '--project', default=None,
        help='Limit to one project (case-insensitive substring match on project name).')
    parser.add_argument(
        '--memoryctl', type=Path,
        default=script_dir.parent / 'tools' / 'memoryctl' / 'MemoryCtl.csproj',
        help='Path to MemoryCtl.csproj.')
    parser.add_argument('--topic-k',     type=int, default=5,
                        help='Max topic winners (default: 5)')
    parser.add_argument('--thread-k',    type=int, default=3,
                        help='Max thread winners (default: 3)')
    parser.add_argument('--decision-k',  type=int, default=3,
                        help='Max decision winners (default: 3)')
    parser.add_argument('--invariant-k', type=int, default=3,
                        help='Max invariant winners (default: 3)')
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Run dreaming analysis but do not write CLAUDE.local.md files.')
    parser.add_argument(
        '--ingest-only', action='store_true',
        help='Only convert sessions and ingest into AMS (skip dream/embed/generate).')
    args = parser.parse_args()

    generator_script = script_dir / 'generate-claude-md.py'

    # Validate inputs
    if not args.projects_dir.is_dir():
        print(f'ERROR: projects-dir not found: {args.projects_dir}', file=sys.stderr)
        sys.exit(1)
    if not args.memoryctl.exists():
        print(f'ERROR: MemoryCtl.csproj not found: {args.memoryctl}', file=sys.stderr)
        sys.exit(1)
    if not generator_script.exists():
        print(f'ERROR: generate-claude-md.py not found: {generator_script}', file=sys.stderr)
        sys.exit(1)

    print('=== maintain-claude-memory ===')
    print(f'Projects dir : {args.projects_dir}')
    print(f'Output root  : {args.output_root}')
    print(f'Filter       : {args.project or "(all projects)"}')
    print(f'Mode         : {"DRY RUN" if args.dry_run else "live"}')
    print(f'Top-K        : topic={args.topic_k} thread={args.thread_k} '
          f'decision={args.decision_k} invariant={args.invariant_k}')

    # Discover projects that have at least one non-empty session file
    projects: dict[str, Path] = {}
    for project_dir in sorted(args.projects_dir.iterdir()):
        if not project_dir.is_dir():
            continue
        project_name = derive_project_name(project_dir.name)
        if args.project and args.project.lower() not in project_name.lower():
            continue
        has_sessions = any(
            f.stat().st_size > 0
            for f in project_dir.glob('*.jsonl')
        )
        if has_sessions:
            projects[project_name] = project_dir

    if not projects:
        filter_msg = f' matching "{args.project}"' if args.project else ''
        print(f'\nNo projects found{filter_msg} in {args.projects_dir}')
        sys.exit(1)

    print(f'\nProjects     : {", ".join(projects)}')

    results: list[tuple[str, bool]] = []
    for project_name, project_dir in projects.items():
        ok = process_project(
            project_name     = project_name,
            project_dir      = project_dir,
            projects_root    = args.projects_dir,
            output_root      = args.output_root,
            memoryctl_proj   = args.memoryctl,
            generator_script = generator_script,
            topic_k          = args.topic_k,
            thread_k         = args.thread_k,
            decision_k       = args.decision_k,
            invariant_k      = args.invariant_k,
            dry_run          = args.dry_run,
            ingest_only      = args.ingest_only,
        )
        results.append((project_name, ok))

    # ── Cross-domain pipeline (all projects combined) ─────────────────────────
    # Only run when processing all projects (not filtered to one project).
    if not args.project:
        ok = process_cross_domain(
            projects_root  = args.projects_dir,
            output_root    = args.output_root,
            memoryctl_proj = args.memoryctl,
            topic_k        = args.topic_k,
            thread_k       = args.thread_k,
            decision_k     = args.decision_k,
            invariant_k    = args.invariant_k,
            dry_run        = args.dry_run,
            ingest_only    = args.ingest_only,
        )
        results.append(('(cross-domain)', ok))

    # Summary
    print(f'\n{"=" * 60}')
    print('Summary:')
    for name, ok in results:
        print(f'  {"OK    " if ok else "FAILED"} {name}')

    any_failed = any(not ok for _, ok in results)
    if any_failed:
        sys.exit(1)


if __name__ == '__main__':
    main()
