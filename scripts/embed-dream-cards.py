#!/usr/bin/env python3
"""
embed-dream-cards.py

Reads dream objects from a .memory.ams.json file and generates semantic
embeddings using sentence-transformers (all-MiniLM-L6-v2).

Writes a sidecar .memory.embeddings.json with one embedding per dream object
(topic / thread / decision / invariant) and per session title.

Usage:
  python embed-dream-cards.py \\
      --ams-json scripts/output/per-project/AMS/AMS.memory.ams.json \\
      --out      scripts/output/per-project/AMS/AMS.memory.embeddings.json

The sidecar is consumed by semantic-query.py for cosine-similarity search.
"""

import argparse
import json
import re
import sys
from pathlib import Path


TITLE_TOKEN_RX = re.compile(r"[a-z0-9]{3,}")
TITLE_STOPWORDS = {
    "a", "an", "the", "and", "or", "but", "if", "then", "else", "of", "to", "in", "on", "at", "for", "with", "by",
    "is", "are", "was", "were", "be", "been", "being", "do", "does", "did", "have", "has", "had", "it", "this", "that",
    "from", "into", "onto", "over", "under", "after", "before", "through", "without", "within",
}
GENERIC_TITLE_TOKENS = {
    "address", "adjustments", "always", "analysis", "apply", "build", "clarity", "communications", "compliance",
    "configuration", "consistency", "context", "contextual", "current", "decision", "enhance", "experience",
    "feature", "feedback", "general", "identify", "implement", "improvements", "improve", "integration", "interactions",
    "issues", "next", "open", "process", "proceed", "requests", "resolve", "resolved", "review", "session", "sessions",
    "settings", "steps", "system", "terms", "thread", "topic", "updates", "user", "users", "validation", "never",
}


def _looks_low_signal_title(text: str) -> bool:
    tokens = [token for token in TITLE_TOKEN_RX.findall((text or "").lower()) if token not in TITLE_STOPWORDS]
    if len(text.split()) <= 1 or len(tokens) < 2:
        return True
    nongeneric = [token for token in tokens if token not in GENERIC_TITLE_TOKENS]
    return len(nongeneric) < 2


def _trusted_thread_title(meta: dict) -> str:
    enriched = str(meta.get('enriched_title') or '').strip()
    validation = str(meta.get('title_validation') or '').strip().lower()
    if enriched and validation == 'accepted' and not _looks_low_signal_title(enriched):
        return enriched

    bootstrap = str(meta.get('bootstrap_title') or '').strip()
    if bootstrap and not _looks_low_signal_title(bootstrap):
        return bootstrap

    raw_title = str(meta.get('title') or '').strip()
    if raw_title and not _looks_low_signal_title(raw_title):
        return raw_title

    return ''


def load_ams_dream_objects_full(ams_json_path: Path):
    """
    Parse AMS snapshot. Returns:
      - dream_entries: list of {id, kind, label, snippets, object}
      - session_entries: list of {id, kind, text}

    Only objects from the latest dream run are included.
    """
    with open(ams_json_path, encoding='utf-8-sig') as f:
        snapshot = json.load(f)

    objects = snapshot.get('objects', [])

    container_ids: set = {c.get('containerId', '') for c in snapshot.get('containers', [])}

    dream_kinds = {'topic', 'thread', 'decision', 'invariant', 'sprint'}
    dreamrun_created_at = {
        str(o.get('objectId', '')): str(o.get('createdAt', ''))
        for o in objects
        if o.get('objectKind') == 'dreamrun'
    }
    scoped_nodes_by_run: dict[str, list[dict]] = {}
    unscoped_nodes: list[dict] = []

    for obj in objects:
        obj_id = obj.get('objectId', '')
        obj_kind = obj.get('objectKind', '')
        if obj_id in container_ids:
            continue
        if obj_kind not in dream_kinds or obj_kind == 'sprint':
            continue
        if not obj_id.startswith(f'{obj_kind}:'):
            continue
        payload = obj.get('semanticPayload') or {}
        prov = payload.get('provenance') or {}
        run_id = str(prov.get('run_id') or '').strip()
        if run_id:
            scoped_nodes_by_run.setdefault(run_id, []).append(obj)
        else:
            unscoped_nodes.append(obj)

    latest_run_id = ''
    if scoped_nodes_by_run:
        latest_run_id = max(
            scoped_nodes_by_run,
            key=lambda run_id: (dreamrun_created_at.get(run_id, ''), run_id),
        )

    dream_entries = []

    for obj in objects:
        obj_id = obj.get('objectId', '')
        if obj_id in container_ids:
            continue
        obj_kind = obj.get('objectKind', '')
        if obj_kind not in dream_kinds:
            continue

        payload = obj.get('semanticPayload') or {}
        prov = payload.get('provenance') or {}

        # Sprint objects are pipeline-wide (not tied to a dream run); always include them
        if obj_kind != 'sprint':
            if latest_run_id:
                if prov.get('run_id') != latest_run_id:
                    continue
            elif scoped_nodes_by_run:
                continue

        # Prefer LLM-enriched summary over raw extracted keyword
        label = (payload.get('summary') if prov.get('enriched_by') == 'lm' else None) \
                or prov.get('label') or payload.get('summary') or obj_id
        # Extract snippet clusters if present (stored as array of strings in provenance)
        snippets = prov.get('snippets') or []

        dream_entries.append({
            'id':       obj_id,
            'kind':     obj_kind,
            'label':    str(label),
            'snippets': snippets[:3],
        })

    # Session titles - use only validated thread titles from conversation_thread
    obj_by_id = {o['objectId']: o for o in objects}
    link_by_id = {l['linkNodeId']: l for l in snapshot.get('linkNodes', [])}

    # Build map: chat_session containerId -> enriched_title from its conversation_thread
    thread_title_by_session: dict[str, str] = {}
    for c in snapshot.get('containers', []):
        if c.get('containerKind') != 'conversation_thread':
            continue
        meta = c.get('metadata') or {}
        can_id = meta.get('canonical_session_id')
        enriched = _trusted_thread_title(meta)
        if can_id and enriched:
            thread_title_by_session[can_id] = enriched

    session_entries = []
    for c in snapshot.get('containers', []):
        if c.get('containerKind') != 'chat_session':
            continue
        meta = c.get('metadata') or {}
        started_at = meta.get('started_at', '')
        container_id = c.get('containerId', '')

        # Prefer validated thread title from conversation_thread, fall back to raw session title
        raw_session_title = meta.get('title', '').strip()
        title = thread_title_by_session.get(container_id) or (
            raw_session_title if not _looks_low_signal_title(raw_session_title) else ''
        )

        # Walk link chain to get first user message as fallback when no title
        snippet = ''
        if not title:
            cur_id = c.get('headLinknodeId')
            visited = 0
            while cur_id and visited < 20:
                ln = link_by_id.get(cur_id)
                if not ln:
                    break
                obj = obj_by_id.get(ln.get('objectId', ''), {})
                pay = obj.get('semanticPayload') or {}
                prov = pay.get('provenance') or {}
                if prov.get('direction') == 'in':
                    text = prov.get('text', '').strip()
                    if len(text) >= 25:
                        snippet = text[:200]
                        break
                cur_id = ln.get('nextLinknodeId')
                visited += 1

        text = title or snippet
        if text:
            session_entries.append({
                'id':   container_id,
                'kind': 'session',
                'text': text,
                'date': started_at[:10] if started_at else '',
            })

    return dream_entries, session_entries


def build_prose(entry: dict) -> str:
    """Build a human-readable prose string to embed."""
    label = entry['label']
    snippets = entry.get('snippets', [])
    if snippets:
        joined = ' '.join(s.strip() for s in snippets if s.strip())
        return f"{label}: {joined}"
    return label


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Generate semantic embeddings sidecar from AMS dream output.')
    parser.add_argument('--ams-json', type=Path, required=True,
                        help='Path to .memory.ams.json produced by the dream step.')
    parser.add_argument('--out', type=Path, required=True,
                        help='Output path for the .memory.embeddings.json sidecar.')
    parser.add_argument('--model', default='all-MiniLM-L6-v2',
                        help='sentence-transformers model name (default: all-MiniLM-L6-v2).')
    parser.add_argument('--no-sessions', action='store_true',
                        help='Omit session titles from embeddings (included by default).')
    args = parser.parse_args()

    if not args.ams_json.exists():
        print(f'ERROR: AMS json not found: {args.ams_json}', file=sys.stderr)
        sys.exit(1)

    print(f'  Loading AMS dream objects from: {args.ams_json.name}')
    dream_entries, session_entries = load_ams_dream_objects_full(args.ams_json)

    if not dream_entries:
        print('  WARNING: No dream objects found. Run the dream step first.')
        # Write empty sidecar so downstream scripts don't fail
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps({'model': args.model, 'entries': []}, indent=2),
                            encoding='utf-8')
        return

    print(f'  Found {len(dream_entries)} dream objects, {len(session_entries)} sessions')

    # Build text corpus
    entries_to_embed = []
    for e in dream_entries:
        entries_to_embed.append({
            'id':   e['id'],
            'kind': e['kind'],
            'text': build_prose(e),
        })
    if not args.no_sessions:
        entries_to_embed.extend(session_entries)

    texts = [e['text'] for e in entries_to_embed]

    print(f'  Loading sentence-transformers model: {args.model}')
    print('  (First run downloads ~80 MB — subsequent runs use cache)')
    try:
        from sentence_transformers import SentenceTransformer  # type: ignore
    except ImportError:
        print('ERROR: sentence-transformers not installed.', file=sys.stderr)
        print('  Run: pip install sentence-transformers', file=sys.stderr)
        sys.exit(1)

    model = SentenceTransformer(args.model)
    print(f'  Encoding {len(texts)} texts...')
    embeddings = model.encode(texts, show_progress_bar=True, convert_to_numpy=True)

    output_entries = []
    for i, entry in enumerate(entries_to_embed):
        output_entries.append({
            'id':        entry['id'],
            'kind':      entry['kind'],
            'text':      entry['text'],
            'embedding': embeddings[i].tolist(),
        })

    sidecar = {
        'model':   args.model,
        'entries': output_entries,
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(sidecar, ensure_ascii=False), encoding='utf-8')
    print(f'  Written: {args.out}  ({len(output_entries)} entries)')


if __name__ == '__main__':
    main()
