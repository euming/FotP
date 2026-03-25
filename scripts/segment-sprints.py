"""
segment-sprints.py — Sprint-based topic segmentation for AMS memory.

Detects natural work sprints from session timestamps and LLM-labels each sprint.
Writes sprint:N objects, sprint-members:N containers, and a sprints SmartList into
the AMS JSON. Slots into the pipeline between enrich-titles and embed-dream-cards.

Usage:
    python segment-sprints.py
        --ams <path.memory.ams.json>
        [--gap-days N]          gap threshold in days (default: 7)
        [--provider openai|anthropic|claude-cli]
        [--api-key KEY]
        [--model ID]
        [--dry-run]
        [--force]               re-label even if sprint already exists
"""

import argparse
import json
import os
import sys
import tempfile
import uuid
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

SPRINT_SYSTEM_PROMPT = (
    "You label work sprints in a developer memory system. Each sprint is a continuous "
    "period of work sessions separated by idle gaps. Labels are used by an AI agent to "
    "find relevant history efficiently."
)

SPRINT_USER_PROMPT_TEMPLATE = """\
Each item below is a sprint — a continuous work period. It has a date range and a list \
of session titles from that period.

Write a 6-10 word label for each sprint naming:
1. The main project or codebase worked on
2. The primary technical outcome or feature delivered
3. The date range (abbreviated month + year)

Rules:
- Be specific: name files, subsystems, commands, or features — not generic themes
- Lead with the technical substance, not meta-words like "work on" or "development"
- Include the date range at the end in parentheses

Examples of GOOD labels:
  "1D-ACC Label.lean proofs and Lean formalization sprint (Oct 2025)"
  "AMS dream pipeline HTML viewer and ingest deduplication (Feb 2026)"
  "NetworkGraphMemory ingest, enrich-titles, and memory search fixes (Mar 2026)"

Examples of BAD labels:
  "Various development work (Oct 2025)"
  "Continued work on project (Feb 2026)"

Return ONLY a JSON array: [{{"id": "<id>", "label": "<label>"}}, ...]

Sprints:
{sprints_json}"""

DEFAULT_MODELS = {
    "openai": "gpt-4o-mini",
    "anthropic": "claude-haiku-4-5-20251001",
    "claude-cli": "claude-haiku-4-5-20251001",
}


# ---------------------------------------------------------------------------
# Provider helpers (mirrors enrich-titles.py)
# ---------------------------------------------------------------------------

def detect_provider(args) -> tuple[str, str]:
    explicit = getattr(args, "provider", None)
    if explicit == "claude-cli":
        return "claude-cli", ""
    if explicit == "anthropic" or (not explicit and not os.environ.get("OPENAI_API_KEY")):
        key = args.api_key or os.environ.get("ANTHROPIC_API_KEY")
        if key:
            return "anthropic", key
    if explicit == "openai" or not explicit:
        key = args.api_key or os.environ.get("OPENAI_API_KEY")
        if key:
            return "openai", key
    print("No API key found — trying claude-cli provider (OAuth via Claude Code).", file=sys.stderr)
    return "claude-cli", ""


def _call_claude_cli(model: str, system: str, user: str) -> str:
    import subprocess
    prompt = f"{system}\n\n{user}"
    cmd = ["claude", "-p", prompt]
    if model:
        cmd += ["--model", model]
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"claude-cli failed: {result.stderr.strip()}")
    return result.stdout.strip()


def _parse_label_response(raw: str) -> dict[str, str]:
    raw = raw.strip()
    if raw.startswith("```"):
        lines = raw.splitlines()
        raw = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    items = json.loads(raw)
    return {item["id"]: item["label"] for item in items}


def call_llm(provider, client, model: str, sprints_payload: list[dict]) -> dict[str, str]:
    sprints_json = json.dumps(sprints_payload, indent=2, ensure_ascii=False)
    user_msg = SPRINT_USER_PROMPT_TEMPLATE.format(sprints_json=sprints_json)

    if provider == "claude-cli":
        raw = _call_claude_cli(model, SPRINT_SYSTEM_PROMPT, user_msg)
        return _parse_label_response(raw)
    elif provider == "anthropic":
        response = client.messages.create(
            model=model,
            max_tokens=512,
            system=SPRINT_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        return _parse_label_response(response.content[0].text)
    else:  # openai
        response = client.chat.completions.create(
            model=model,
            max_tokens=512,
            messages=[
                {"role": "system", "content": SPRINT_SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
        )
        return _parse_label_response(response.choices[0].message.content)


# ---------------------------------------------------------------------------
# AMS helpers
# ---------------------------------------------------------------------------

def parse_date(s: str) -> datetime:
    """Parse ISO datetime string to UTC datetime."""
    if not s:
        return datetime.min.replace(tzinfo=timezone.utc)
    s = s.rstrip("Z")
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return datetime.min.replace(tzinfo=timezone.utc)


def collect_sessions(data: dict) -> list[dict]:
    """Return chat_session containers sorted by started_at."""
    sessions = [c for c in data.get("containers", [])
                if c.get("containerKind") == "chat_session"]
    sessions.sort(key=lambda c: parse_date(c.get("metadata", {}).get("started_at", "")))
    return sessions


def get_session_title(sess: dict, thread_title_map: dict) -> str:
    """Return best available title for a session."""
    cid = sess.get("containerId", "")
    if cid in thread_title_map:
        return thread_title_map[cid]
    meta = sess.get("metadata") or {}
    return meta.get("enriched_title") or meta.get("title") or cid


def build_thread_title_map(data: dict) -> dict[str, str]:
    """Map chat_session containerId -> enriched_title from conversation_thread containers."""
    mapping = {}
    for c in data.get("containers", []):
        if c.get("containerKind") != "conversation_thread":
            continue
        meta = c.get("metadata") or {}
        can_id = meta.get("canonical_session_id")
        title = meta.get("enriched_title", "").strip()
        if can_id and title:
            mapping[can_id] = title
    return mapping


def detect_sprints(sessions: list[dict], gap_days: int) -> list[list[dict]]:
    """Split sorted sessions into sprints by gap threshold."""
    if not sessions:
        return []
    sprints = []
    current = [sessions[0]]
    for sess in sessions[1:]:
        prev_dt = parse_date(current[-1].get("metadata", {}).get("started_at", ""))
        curr_dt = parse_date(sess.get("metadata", {}).get("started_at", ""))
        gap = (curr_dt - prev_dt).total_seconds() / 86400.0
        if gap > gap_days:
            sprints.append(current)
            current = [sess]
        else:
            current.append(sess)
    sprints.append(current)
    return sprints


# ---------------------------------------------------------------------------
# AMS write helpers (doubly-linked list)
# ---------------------------------------------------------------------------

def make_link_node(link_node_id: str, container_id: str, object_id: str,
                   prev_id: str | None, next_id: str | None) -> dict:
    return {
        "linkNodeId": link_node_id,
        "containerId": container_id,
        "objectId": object_id,
        "prevLinknodeId": prev_id,
        "nextLinknodeId": next_id,
    }


def build_linked_list(container_id: str, object_ids: list[str]) -> tuple[list[dict], str, str]:
    """Build doubly-linked linkNodes for a container. Returns (nodes, head_id, tail_id)."""
    if not object_ids:
        return [], "", ""
    node_ids = [str(uuid.uuid4()) for _ in object_ids]
    nodes = []
    for i, (nid, oid) in enumerate(zip(node_ids, object_ids)):
        prev_id = node_ids[i - 1] if i > 0 else None
        next_id = node_ids[i + 1] if i < len(node_ids) - 1 else None
        nodes.append(make_link_node(nid, container_id, oid, prev_id, next_id))
    return nodes, node_ids[0], node_ids[-1]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Segment AMS sessions into sprints")
    parser.add_argument("--ams", required=True, help="Path to .memory.ams.json")
    parser.add_argument("--gap-days", type=int, default=7,
                        help="Gap in days that starts a new sprint (default: 7)")
    parser.add_argument("--provider", choices=["openai", "anthropic", "claude-cli"], default=None)
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--model", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true",
                        help="Re-label even if sprint objects already exist")
    args = parser.parse_args()

    provider, api_key = detect_provider(args)
    model = args.model or DEFAULT_MODELS[provider]
    print(f"Provider: {provider}  Model: {model}")

    ams_path = args.ams
    if not os.path.exists(ams_path):
        print(f"ERROR: File not found: {ams_path}", file=sys.stderr)
        sys.exit(1)

    with open(ams_path, encoding="utf-8-sig") as f:
        data = json.load(f)

    # Check if sprints already exist and --force not set
    existing_sprints = [o for o in data.get("objects", [])
                        if o.get("objectKind") == "sprint"]
    if existing_sprints and not args.force:
        print(f"Sprint objects already exist ({len(existing_sprints)} sprints). "
              f"Use --force to re-label.")
        return

    thread_title_map = build_thread_title_map(data)
    sessions = collect_sessions(data)
    if not sessions:
        print("No chat_session containers found — nothing to segment.")
        return

    sprint_groups = detect_sprints(sessions, args.gap_days)
    print(f"\nDetected {len(sprint_groups)} sprints (gap threshold: {args.gap_days} days):")
    for i, group in enumerate(sprint_groups, 1):
        dates = [s.get("metadata", {}).get("started_at", "?") for s in group]
        d_start = dates[0][:10] if dates[0] != "?" else "?"
        d_end = dates[-1][:10] if dates[-1] != "?" else "?"
        print(f"  Sprint {i}: {len(group)} sessions  [{d_start} .. {d_end}]")
        for s in group:
            title = get_session_title(s, thread_title_map)
            print(f"    - {title}")

    # Build LLM payload
    llm_payload = []
    for i, group in enumerate(sprint_groups, 1):
        dates = [s.get("metadata", {}).get("started_at", "?") for s in group]
        d_start = dates[0][:10] if dates[0] != "?" else "?"
        d_end = dates[-1][:10] if dates[-1] != "?" else "?"
        titles = [get_session_title(s, thread_title_map) for s in group]
        llm_payload.append({
            "id": f"sprint:{i}",
            "date_start": d_start,
            "date_end": d_end,
            "session_count": len(group),
            "session_titles": titles,
        })

    # Call LLM
    if provider == "claude-cli":
        client = None
    elif provider == "anthropic":
        try:
            import anthropic as _anthropic
        except ImportError:
            print("ERROR: anthropic package not installed.", file=sys.stderr)
            sys.exit(1)
        client = _anthropic.Anthropic(api_key=api_key)
    else:
        try:
            import openai as _openai
        except ImportError:
            print("ERROR: openai package not installed.", file=sys.stderr)
            sys.exit(1)
        client = _openai.OpenAI(api_key=api_key)

    print("\nCalling LLM for sprint labels...")
    try:
        labels = call_llm(provider, client, model, llm_payload)
    except Exception as e:
        print(f"ERROR: LLM call failed: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nSprint labels:")
    for sp in llm_payload:
        label = labels.get(sp["id"], "(no label)")
        print(f"  [{sp['id']}] {label}")

    if args.dry_run:
        print("\n(dry-run — no changes written)")
        return

    # Remove existing sprint objects/containers/linknodes if --force
    if existing_sprints:
        sprint_obj_ids = {o["objectId"] for o in existing_sprints}
        sprint_cids = {f"sprint-members:{o['objectId'].split(':',1)[1]}"
                       for o in existing_sprints}
        sprint_cids.add("sprints")
        all_sprint_obj_ids = sprint_obj_ids | sprint_cids  # sprint:N + sprint-members:N + sprints
        data["objects"] = [o for o in data.get("objects", [])
                           if o.get("objectId") not in all_sprint_obj_ids]
        data["containers"] = [c for c in data.get("containers", [])
                               if c.get("containerId") not in sprint_cids]
        data["linkNodes"] = [ln for ln in data.get("linkNodes", [])
                              if ln.get("containerId") not in sprint_cids]

    # Build new sprint objects, containers, link nodes
    new_objects = []
    new_containers = []
    new_link_nodes = []
    sprint_obj_ids_ordered = []  # for sprints SmartList

    for i, (group, sp) in enumerate(zip(sprint_groups, llm_payload), 1):
        sprint_obj_id = f"sprint:{i}"
        sprint_members_cid = f"sprint-members:{i}"
        sprint_obj_ids_ordered.append(sprint_obj_id)

        dates = [s.get("metadata", {}).get("started_at", "?") for s in group]
        d_start = dates[0][:10] if dates[0] != "?" else "?"
        d_end = dates[-1][:10] if dates[-1] != "?" else "?"
        label = labels.get(sprint_obj_id, f"Sprint {i} ({d_start} - {d_end})")

        # Sprint object
        new_objects.append({
            "objectId": sprint_obj_id,
            "objectKind": "sprint",
            "semanticPayload": {
                "summary": label,
                "provenance": {
                    "sprint_num": i,
                    "date_start": d_start,
                    "date_end": d_end,
                    "session_count": len(group),
                    "gap_days_threshold": args.gap_days,
                    "enriched_by": "lm",
                },
            },
        })

        # Sprint-members container (doubly-linked list of session containerIds)
        # AMS requires every container to have a backing object with the same ID.
        new_objects.append({
            "objectId": sprint_members_cid,
            "objectKind": "sprint_members",
        })
        session_ids = [s["containerId"] for s in group]
        nodes, head_id, tail_id = build_linked_list(sprint_members_cid, session_ids)
        new_link_nodes.extend(nodes)
        new_containers.append({
            "containerId": sprint_members_cid,
            "containerKind": "sprint_members",
            "headLinknodeId": head_id,
            "tailLinknodeId": tail_id,
        })

    # sprints SmartList object
    new_objects.append({
        "objectId": "sprints",
        "objectKind": "SmartList",
        "semanticPayload": {
            "summary": f"Sprint index: {len(sprint_groups)} sprints",
        },
    })

    # sprints container (ordered list of sprint:N refs)
    sprints_nodes, sprints_head, sprints_tail = build_linked_list("sprints", sprint_obj_ids_ordered)
    new_link_nodes.extend(sprints_nodes)
    new_containers.append({
        "containerId": "sprints",
        "containerKind": "sprint_index",
        "headLinknodeId": sprints_head,
        "tailLinknodeId": sprints_tail,
    })

    # Append to data
    data.setdefault("objects", []).extend(new_objects)
    data.setdefault("containers", []).extend(new_containers)
    data.setdefault("linkNodes", []).extend(new_link_nodes)

    # Atomic write
    dir_name = os.path.dirname(os.path.abspath(ams_path))
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=dir_name,
                                     suffix=".tmp", delete=False) as tf:
        tmp_path = tf.name
        json.dump(data, tf, ensure_ascii=False, indent=2)

    os.replace(tmp_path, ams_path)
    print(f"\nWritten: {ams_path}")
    print(f"Added {len(sprint_groups)} sprint objects + {len(sprint_groups)} sprint-members containers + sprints index")


if __name__ == "__main__":
    main()
