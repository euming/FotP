"""
repair-ams-backing-objects.py

One-shot repair: adds missing backing objects for containers that lack them.

The AMS invariant (enforced by AmsPersistence.Deserialize) requires every
container to have an object with the same ID. Containers written directly by
Python scripts (segment-sprints.py sprint-members:N, etc.) may be missing
their backing objects if an older version of the script didn't write them.

Usage:
    python repair-ams-backing-objects.py --ams <path.memory.ams.json> [--dry-run]
"""

import argparse
import json
import os
import sys
import tempfile


def main():
    parser = argparse.ArgumentParser(description="Add missing AMS backing objects")
    parser.add_argument("--ams", required=True, help="Path to .memory.ams.json")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not os.path.exists(args.ams):
        print(f"ERROR: File not found: {args.ams}", file=sys.stderr)
        sys.exit(1)

    with open(args.ams, encoding="utf-8-sig") as f:
        data = json.load(f)

    obj_ids = {o["objectId"] for o in data.get("objects", [])}
    missing = []

    for c in data.get("containers", []):
        cid = c.get("containerId", "")
        if cid and cid not in obj_ids:
            # Infer objectKind from containerKind
            ck = c.get("containerKind", "")
            kind = ck if ck else "container"
            missing.append({"objectId": cid, "objectKind": kind})
            print(f"  Missing backing object: {cid!r}  (kind={kind!r})")

    if not missing:
        print("No missing backing objects found — nothing to repair.")
        return

    print(f"\nFound {len(missing)} missing backing object(s).")

    if args.dry_run:
        print("(dry-run — no changes written)")
        return

    data.setdefault("objects", []).extend(missing)

    dir_name = os.path.dirname(os.path.abspath(args.ams))
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=dir_name,
                                     suffix=".tmp", delete=False) as tf:
        tmp_path = tf.name
        json.dump(data, tf, ensure_ascii=False, indent=2)

    os.replace(tmp_path, args.ams)
    print(f"Repaired: {args.ams}")


if __name__ == "__main__":
    main()
