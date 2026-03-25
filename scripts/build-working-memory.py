"""
build-working-memory.py — Recency-weighted working memory SmartList for AMS.

Scores dream objects by vote_score * exp(-days_since / half_life_days) and writes
a working-memory SmartList + container into the AMS JSON. This is a pure-computation
step with no LLM calls.

Usage:
    python build-working-memory.py
        --ams <path.memory.ams.json>
        [--top N]               max items in working memory (default: 10)
        [--half-life-days N]    recency decay half-life in days (default: 14)
        [--dry-run]
        [--force]               rebuild even if working-memory already exists
"""

import argparse
import json
import math
import os
import sys
import tempfile
import uuid
from datetime import datetime, timezone


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


def walk_chain(container: dict, link_node_map: dict) -> list[str]:
    """Return ordered list of objectIds from a container's head->tail chain."""
    head = container.get("headLinknodeId")
    if not head:
        return []
    chain = []
    curr = head
    visited = set()
    while curr and curr not in visited:
        visited.add(curr)
        ln = link_node_map.get(curr)
        if not ln:
            break
        chain.append(ln["objectId"])
        curr = ln.get("nextLinknodeId")
    return chain


def build_linked_list(container_id: str, object_ids: list[str]) -> tuple[list[dict], str, str]:
    """Build doubly-linked linkNodes for a container. Returns (nodes, head_id, tail_id)."""
    if not object_ids:
        return [], "", ""
    node_ids = [str(uuid.uuid4()) for _ in object_ids]
    nodes = []
    for i, (nid, oid) in enumerate(zip(node_ids, object_ids)):
        prev_id = node_ids[i - 1] if i > 0 else None
        next_id = node_ids[i + 1] if i < len(node_ids) - 1 else None
        nodes.append({
            "linkNodeId": nid,
            "containerId": container_id,
            "objectId": oid,
            "prevLinknodeId": prev_id,
            "nextLinknodeId": next_id,
        })
    return nodes, node_ids[0], node_ids[-1]


# ---------------------------------------------------------------------------
# Working memory computation
# ---------------------------------------------------------------------------

DREAM_KINDS = {"topic", "thread", "decision", "invariant", "sprint"}


def compute_working_memory(data: dict, top_n: int, half_life_days: float) -> list[dict]:
    """
    Score dream objects by recency * vote_score and return the top N.

    For each dream object:
    1. Find the most recent session date among sessions linked through its members container.
    2. Compute recency = exp(-days_since / half_life_days).
    3. Compute final_score = vote_score * recency.

    Returns list of {object_id, kind, label, vote_score, recency, final_score, max_date} dicts,
    sorted by final_score desc.
    """
    obj_map = {o["objectId"]: o for o in data.get("objects", [])}
    container_map = {c["containerId"]: c for c in data.get("containers", [])}
    link_node_map = {ln["linkNodeId"]: ln for ln in data.get("linkNodes", [])}

    # Find latest dream run (same logic as generate-claude-md.py)
    dreamrun_objects = [o for o in data.get("objects", []) if o.get("objectKind") == "dreamrun"]
    if not dreamrun_objects:
        return []
    latest_run = max(dreamrun_objects, key=lambda o: o.get("createdAt", ""))
    latest_run_id: str = latest_run.get("objectId", "")

    # Build session started_at map
    session_dates: dict[str, datetime] = {}
    for c in data.get("containers", []):
        if c.get("containerKind") == "chat_session":
            meta = c.get("metadata") or {}
            cid = c.get("containerId", "")
            dt = parse_date(meta.get("started_at", ""))
            session_dates[cid] = dt

    today = datetime.now(tz=timezone.utc)

    scored: list[dict] = []

    for obj in data.get("objects", []):
        obj_id = obj.get("objectId", "")
        kind = obj.get("objectKind", "")
        if kind not in DREAM_KINDS:
            continue

        # Skip objects that aren't from the latest run (except sprints which have no run_id)
        payload = obj.get("semanticPayload") or {}
        prov = payload.get("provenance") or {}

        if kind != "sprint" and prov.get("run_id") != latest_run_id:
            continue

        # Get vote_score (sprints use 1.0 since they have no vote_score)
        vote_score = float(prov.get("vote_score", 1.0))

        # Get label: prefer LLM summary, fall back to provenance label, then object_id
        label = payload.get("summary") or prov.get("label") or obj_id

        # Find members container: "<kind>-members:<sig>" where sig = id suffix after ":"
        # For sprint:1 -> sprint-members:1
        # For topic/thread/decision/invariant objects, members container uses object's sig
        if ":" in obj_id:
            sig = obj_id.split(":", 1)[1]
            members_cid = f"{kind}-members:{sig}"
        else:
            members_cid = f"{kind}-members:{obj_id}"

        members_container = container_map.get(members_cid)

        max_date = datetime.min.replace(tzinfo=timezone.utc)

        if members_container:
            # Walk members chain -> get session containerIds
            member_ids = walk_chain(members_container, link_node_map)
            for mid in member_ids:
                dt = session_dates.get(mid, datetime.min.replace(tzinfo=timezone.utc))
                if dt > max_date:
                    max_date = dt

        # If no sessions found via members, fall back to object's own createdAt
        if max_date == datetime.min.replace(tzinfo=timezone.utc):
            obj_created = parse_date(obj.get("createdAt", ""))
            if obj_created > max_date:
                max_date = obj_created

        days_since = max(0.0, (today - max_date).total_seconds() / 86400.0)
        recency = math.exp(-days_since / half_life_days)
        final_score = vote_score * recency

        scored.append({
            "object_id": obj_id,
            "kind": kind,
            "label": str(label),
            "vote_score": vote_score,
            "recency": recency,
            "days_since": days_since,
            "final_score": final_score,
            "max_date": max_date.isoformat() if max_date != datetime.min.replace(tzinfo=timezone.utc) else "",
        })

    scored.sort(key=lambda x: -x["final_score"])
    return scored[:top_n]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Build working memory SmartList in AMS JSON")
    parser.add_argument("--ams", required=True, help="Path to .memory.ams.json")
    parser.add_argument("--top", type=int, default=10, help="Max items in working memory (default: 10)")
    parser.add_argument("--half-life-days", type=float, default=14.0,
                        help="Recency decay half-life in days (default: 14)")
    parser.add_argument("--dry-run", action="store_true", help="Print ranking without writing")
    parser.add_argument("--force", action="store_true",
                        help="Rebuild even if working-memory already exists")
    args = parser.parse_args()

    ams_path = args.ams
    if not os.path.exists(ams_path):
        print(f"ERROR: File not found: {ams_path}", file=sys.stderr)
        sys.exit(1)

    with open(ams_path, encoding="utf-8-sig") as f:
        data = json.load(f)

    # Check if already exists
    existing = [o for o in data.get("objects", []) if o.get("objectId") == "working-memory"]
    if existing and not args.force:
        print("working-memory SmartList already exists. Use --force to rebuild.")
        return

    items = compute_working_memory(data, args.top, args.half_life_days)

    if not items:
        print("No dream objects found — nothing to write.")
        return

    print(f"\nWorking memory ranking (top {len(items)}, half-life={args.half_life_days}d):")
    for rank, item in enumerate(items, 1):
        days_str = f"{item['days_since']:.1f}d ago" if item['max_date'] else "unknown date"
        print(f"  {rank:2}. [{item['kind']:9}] score={item['final_score']:.4f} "
              f"(vote={item['vote_score']:.2f}, recency={item['recency']:.3f}, {days_str})")
        print(f"       {item['label'][:90]}")

    if args.dry_run:
        print("\n(dry-run — no changes written)")
        return

    # Remove existing working-memory objects/containers/linknodes if --force
    if existing:
        data["objects"] = [o for o in data.get("objects", [])
                           if o.get("objectId") != "working-memory"]
        data["containers"] = [c for c in data.get("containers", [])
                               if c.get("containerId") != "working-memory"]
        data["linkNodes"] = [ln for ln in data.get("linkNodes", [])
                             if ln.get("containerId") != "working-memory"]

    now_iso = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # working-memory SmartList object
    data.setdefault("objects", []).append({
        "objectId": "working-memory",
        "objectKind": "SmartList",
        "semanticPayload": {
            "summary": f"Working memory: top {len(items)} recency-weighted dream objects",
            "provenance": {
                "half_life_days": args.half_life_days,
                "item_count": len(items),
                "updated_at": now_iso,
            },
        },
    })

    # working-memory container (doubly-linked list of dream objectIds in score order)
    object_ids = [item["object_id"] for item in items]
    nodes, head_id, tail_id = build_linked_list("working-memory", object_ids)
    data.setdefault("linkNodes", []).extend(nodes)
    data.setdefault("containers", []).append({
        "containerId": "working-memory",
        "containerKind": "working_memory",
        "headLinknodeId": head_id,
        "tailLinknodeId": tail_id,
    })

    # Atomic write
    dir_name = os.path.dirname(os.path.abspath(ams_path))
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=dir_name,
                                     suffix=".tmp", delete=False) as tf:
        tmp_path = tf.name
        json.dump(data, tf, ensure_ascii=False, indent=2)

    os.replace(tmp_path, ams_path)
    print(f"\nWritten: {ams_path}")
    print(f"Added working-memory SmartList with {len(items)} items")


if __name__ == "__main__":
    main()
