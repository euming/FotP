#!/usr/bin/env python3
"""
semantic-query.py

Semantic search against a .memory.embeddings.json sidecar produced by
embed-dream-cards.py. Uses cosine similarity to find the dream objects most
relevant to a query string, then resolves linked sessions via the AMS snapshot.

Usage:
  python semantic-query.py \\
      --embeddings scripts/output/all-claude-projects/all-claude-projects.memory.embeddings.json \\
      --q "cross project" \\
      --top 10

Optional: --ams-json to also resolve which sessions belong to top dream topics.

Output format (one line per result):
  <score> | <kind> | <text>
  ...
  --- Sessions linked to top topics ---
  <date> | <session_id[:8]}> | <title>
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Cache helpers (P5-C1)
# ---------------------------------------------------------------------------

def _find_ams_exe() -> list[str] | None:
    """Return the command prefix for ams-core-kernel, or None if not found."""
    # Try to import ams_common from the same directory.
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "ams_common",
        Path(__file__).parent / "ams_common.py",
    )
    if spec and spec.loader:
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[arg-type]
        return mod.build_rust_ams_cmd()
    return None


def _cache_lookup(snapshot: str, query: str, corpus_version: str) -> tuple[bool, str]:
    """
    Call search-cache-lookup.  Returns (is_hit, text).
    text is the cached payload on a hit, empty string on a miss.
    """
    prefix = _find_ams_exe()
    if not prefix:
        return False, ""
    cmd = prefix + [
        "search-cache-lookup",
        "--input", snapshot,
        "--query", query,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    except Exception:
        return False, ""
    lines = proc.stdout.splitlines()
    status = next((ln.removeprefix("status=") for ln in lines if ln.startswith("status=")), "miss")
    if status == "hit":
        text = next((ln[len("text="):] for ln in lines if ln.startswith("text=")), "")
        return True, text
    return False, ""


def _emit_cache_signal(snapshot: str, query: str, is_hit: bool) -> None:
    """Emit a FEP cache-signal tool-call Object recording the hit/miss outcome (P7-B2).

    Fire-and-forget via Popen so it does not block the caller.
    If the binary is missing or the Popen fails, the error is silently swallowed.
    """
    prefix = _find_ams_exe()
    if not prefix:
        return
    cmd = prefix + [
        "fep-cache-signal-emit",
        "--input", snapshot,
        "--query", query,
        "--is-hit", "true" if is_hit else "false",
    ]
    try:
        subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


def _cache_promote(snapshot: str, query: str, corpus_version: str, result_text: str) -> None:
    """Call search-cache-promote to store result_text for this query."""
    prefix = _find_ams_exe()
    if not prefix:
        return
    cmd = prefix + [
        "search-cache-promote",
        "--input", snapshot,
        "--query", query,
        "--text", result_text,
    ]
    try:
        subprocess.run(cmd, capture_output=True, timeout=10)
    except Exception:
        pass


def cosine_similarity(a: list, b: list) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def load_embeddings(path: Path) -> tuple[str, list[dict]]:
    """Load embeddings sidecar. Returns (model_name, entries)."""
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    return data.get('model', 'all-MiniLM-L6-v2'), data.get('entries', [])


def load_session_map(ams_json_path: Path) -> dict:
    """
    Load AMS snapshot and return a mapping from dream object ID -> list of session metadata
    via container membership.

    Also returns session metadata keyed by container ID.
    """
    if not ams_json_path or not ams_json_path.exists():
        return {}, {}

    with open(ams_json_path, encoding='utf-8-sig') as f:
        snapshot = json.load(f)

    # session metadata by container ID
    sessions_by_id = {}
    for c in snapshot.get('containers', []):
        if c.get('containerKind') == 'chat_session':
            meta = c.get('metadata') or {}
            sessions_by_id[c.get('containerId', '')] = {
                'id':    c.get('containerId', ''),
                'date':  (meta.get('started_at') or '')[:10],
                'title': meta.get('title', ''),
            }

    # memberships: object -> containers
    obj_to_sessions: dict[str, list] = {}
    for ln in snapshot.get('linkNodes', []):
        # membership link nodes connect objects to containers
        cid = ln.get('containerId', '')
        oid = ln.get('objectId', '')
        if cid in sessions_by_id and oid:
            obj_to_sessions.setdefault(oid, []).append(sessions_by_id[cid])

    return obj_to_sessions, sessions_by_id


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Semantic search against AMS embeddings sidecar.')
    parser.add_argument('--embeddings', type=Path, required=True,
                        help='Path to .memory.embeddings.json')
    parser.add_argument('--q', required=True,
                        help='Query string to search for.')
    parser.add_argument('--top', type=int, default=10,
                        help='Number of top results to return (default: 10).')
    parser.add_argument('--ams-json', type=Path, default=None,
                        help='Optional: .memory.ams.json to resolve linked sessions.')
    parser.add_argument('--min-score', type=float, default=0.2,
                        help='Minimum cosine similarity to report (default: 0.2).')
    parser.add_argument('--corpus-version', default=None,
                        help='Corpus version hash for cache lookup/promote (P5-C1).')
    parser.add_argument('--snapshot', default=None,
                        help='Path to AMS snapshot for cache operations (P5-C1).')
    args = parser.parse_args()

    # ── P5-C1: cache lookup (before any expensive computation) ──────────────────
    use_cache = bool(args.corpus_version and args.snapshot)
    query_start = time.monotonic()
    if use_cache:
        is_hit, cached_text = _cache_lookup(args.snapshot, args.q, args.corpus_version)
        if is_hit:
            # P7-B2: emit hit signal fire-and-forget on the cache-hit path
            _emit_cache_signal(args.snapshot, args.q, True)
            print('CACHE_HIT')
            print(cached_text)
            return
    # ────────────────────────────────────────────────────────────────────────────

    if not args.embeddings.exists():
        print(f'ERROR: embeddings file not found: {args.embeddings}', file=sys.stderr)
        print('Run embed-dream-cards.py first.', file=sys.stderr)
        sys.exit(1)

    model_name, entries = load_embeddings(args.embeddings)
    if not entries:
        print('No entries in embeddings file.')
        return

    try:
        from sentence_transformers import SentenceTransformer  # type: ignore
    except ImportError:
        print('ERROR: sentence-transformers not installed.', file=sys.stderr)
        print('  Run: pip install sentence-transformers', file=sys.stderr)
        sys.exit(1)

    model = SentenceTransformer(model_name)
    query_vec = model.encode([args.q], convert_to_numpy=True)[0].tolist()

    # Score all entries
    scored = []
    for entry in entries:
        emb = entry.get('embedding', [])
        if not emb:
            continue
        score = cosine_similarity(query_vec, emb)
        scored.append((score, entry))

    scored.sort(key=lambda x: x[0], reverse=True)
    top_hits = [(s, e) for s, e in scored[:args.top] if s >= args.min_score]

    if not top_hits:
        print(f"No strong memory matches found for '{args.q}'.")
        return

    output_lines: list[str] = []
    output_lines.append(f"Top {len(top_hits)} results for: '{args.q}'\n")
    output_lines.append(f"{'Score':>6}  {'Kind':<12}  Text")
    output_lines.append('-' * 80)
    for score, entry in top_hits:
        text = entry.get('text', '')
        if len(text) > 100:
            text = text[:97] + '...'
        output_lines.append(f"{score:>6.3f}  {entry.get('kind', ''):<12}  {text}")

    # Optionally resolve linked sessions
    if args.ams_json:
        obj_to_sessions, _ = load_session_map(args.ams_json)
        seen_sessions: set = set()
        linked: list = []
        for _, entry in top_hits[:5]:  # resolve top 5 hits
            oid = entry.get('id', '')
            for sess in obj_to_sessions.get(oid, []):
                sid = sess['id']
                if sid not in seen_sessions:
                    seen_sessions.add(sid)
                    linked.append(sess)

        if linked:
            linked.sort(key=lambda s: s['date'], reverse=True)
            output_lines.append('\n--- Sessions linked to top topics ---')
            for sess in linked[:10]:
                short_id = sess['id'][:8]
                output_lines.append(f"  {sess['date']}  {short_id}  {sess['title']}")

    result_text = '\n'.join(output_lines)
    print(result_text)

    # ── P7-B2: emit miss signal after computation (fire-and-forget) ──────────
    if use_cache:
        _emit_cache_signal(args.snapshot, args.q, False)
    # ─────────────────────────────────────────────────────────────────────────

    # ── P5-C1: promote to cache after computing ──────────────────────────────
    if use_cache:
        _cache_promote(args.snapshot, args.q, args.corpus_version, result_text)
    # ─────────────────────────────────────────────────────────────────────────


if __name__ == '__main__':
    main()
