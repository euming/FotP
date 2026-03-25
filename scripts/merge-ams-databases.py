#!/usr/bin/env python3
"""Merge two AMS databases into one, unifying SmartList containers.

For overlapping containers: union their member linked lists.
For overlapping objects: keep the one with more data (larger semanticPayload).
For non-overlapping entities: copy as-is.
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path


def load_db(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def merge_databases(primary: dict, secondary: dict) -> dict:
    """Merge secondary into primary. Primary wins on object conflicts."""
    # Index everything
    p_objects = {o["objectId"]: o for o in primary.get("objects", [])}
    s_objects = {o["objectId"]: o for o in secondary.get("objects", [])}

    p_containers = {c["containerId"]: c for c in primary.get("containers", [])}
    s_containers = {c["containerId"]: c for c in secondary.get("containers", [])}

    p_links = {l["linkNodeId"]: l for l in primary.get("linkNodes", [])}
    s_links = {l["linkNodeId"]: l for l in secondary.get("linkNodes", [])}

    # --- Merge objects: union, prefer primary on conflict ---
    merged_objects = dict(p_objects)
    new_objects = 0
    for oid, obj in s_objects.items():
        if oid not in merged_objects:
            merged_objects[oid] = obj
            new_objects += 1
        # else: keep primary version
    print(f"Objects: {len(p_objects)} primary + {new_objects} new from secondary = {len(merged_objects)} total")

    # --- Merge containers: union, merge member lists on conflict ---
    merged_containers = dict(p_containers)
    merged_links = dict(p_links)
    new_containers = 0
    merged_member_lists = 0

    for cid, s_container in s_containers.items():
        if cid not in merged_containers:
            # New container — copy it and all its link nodes
            merged_containers[cid] = s_container
            new_containers += 1
            # Copy all link nodes belonging to this container
            head = s_container.get("headLinknodeId")
            current = head
            visited = set()
            while current and current not in visited:
                visited.add(current)
                if current in s_links:
                    merged_links[current] = s_links[current]
                    current = s_links[current].get("nextLinknodeId")
                else:
                    break
        else:
            # Overlapping container — append secondary's members to primary's tail
            p_container = merged_containers[cid]

            # Find primary's tail
            p_tail_id = None
            current = p_container.get("headLinknodeId")
            visited = set()
            p_member_refs = set()
            while current and current not in visited:
                visited.add(current)
                if current in merged_links:
                    link = merged_links[current]
                    p_member_refs.add(link.get("memberObjectId", ""))
                    p_tail_id = current
                    current = link.get("nextLinknodeId")
                else:
                    break

            # Collect secondary's members (skip duplicates)
            s_members_to_add = []
            current = s_container.get("headLinknodeId")
            visited = set()
            while current and current not in visited:
                visited.add(current)
                if current in s_links:
                    link = s_links[current]
                    member_ref = link.get("memberObjectId", "")
                    if member_ref and member_ref not in p_member_refs:
                        s_members_to_add.append(link)
                    current = link.get("nextLinknodeId")
                else:
                    break

            if s_members_to_add:
                merged_member_lists += 1
                # Append to primary's tail
                prev_id = p_tail_id
                for link in s_members_to_add:
                    new_link_id = f"merged-link:{uuid.uuid4()}"
                    new_link = {
                        "linkNodeId": new_link_id,
                        "containerId": cid,
                        "memberObjectId": link["memberObjectId"],
                        "nextLinknodeId": None,
                        "prevLinknodeId": prev_id,
                    }
                    # Copy any extra fields from the original link
                    for key in link:
                        if key not in new_link and key not in ("linkNodeId", "nextLinknodeId", "prevLinknodeId"):
                            new_link[key] = link[key]

                    merged_links[new_link_id] = new_link
                    if prev_id and prev_id in merged_links:
                        merged_links[prev_id]["nextLinknodeId"] = new_link_id
                    elif not p_container.get("headLinknodeId"):
                        # Primary container was empty
                        merged_containers[cid]["headLinknodeId"] = new_link_id
                    prev_id = new_link_id

    print(f"Containers: {len(p_containers)} primary + {new_containers} new = {len(merged_containers)} total")
    print(f"  ({merged_member_lists} overlapping containers had members merged)")
    print(f"LinkNodes: {len(p_links)} primary + {len(merged_links) - len(p_links)} new = {len(merged_links)} total")

    # --- Build output ---
    result = dict(primary)
    result["objects"] = list(merged_objects.values())
    result["containers"] = list(merged_containers.values())
    result["linkNodes"] = list(merged_links.values())
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge two AMS databases")
    parser.add_argument("primary", type=Path, help="Primary database (wins conflicts)")
    parser.add_argument("secondary", type=Path, help="Secondary database to merge in")
    parser.add_argument("--output", "-o", type=Path, help="Output path (default: overwrite primary)")
    parser.add_argument("--dry-run", action="store_true", help="Print stats without writing")
    args = parser.parse_args()

    print(f"Primary:   {args.primary}")
    print(f"Secondary: {args.secondary}")

    primary = load_db(args.primary)
    secondary = load_db(args.secondary)

    merged = merge_databases(primary, secondary)

    if args.dry_run:
        print("(dry run — not writing output)")
        return 0

    output = args.output or args.primary
    print(f"Writing:   {output}")
    with open(output, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
