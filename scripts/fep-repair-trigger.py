#!/usr/bin/env python3
"""
fep-repair-trigger.py

Reads unprocessed anomaly notes from the SmartList bucket
`smartlist/fep-tool-anomalies`. When FE > 3.0 or 3+ same-tool anomalies,
triggers a callstack repair interrupt with tool-specific repair hints.

Usage:
    python fep-repair-trigger.py --db <path.memory.jsonl> [--dry-run] [--fe-threshold 3.0] [--count-threshold 3]

The script:
  1. Loads the paired AMS snapshot for the given .memory.jsonl DB
  2. Walks SmartList notes under `smartlist/fep-tool-anomalies`
  3. Filters to unprocessed notes (provenance.processed != true)
  4. Groups by tool_name and applies threshold logic
  5. Maps tool+failure to repair hints
  6. Calls `callstack interrupt --policy repair` for each triggered group
  7. Marks processed notes (patches provenance.processed = true)
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

sys.path.insert(0, str(SCRIPT_DIR))
from ams_common import build_rust_ams_cmd, corpus_db, repo_root, rust_backend_env

ANOMALY_BUCKET_PATH = "smartlist/fep-tool-anomalies"
DEFAULT_FE_THRESHOLD = 3.0
DEFAULT_COUNT_THRESHOLD = 3

# -------------------------------------------------------------------------
# Repair hint mapping: tool_name + outcome -> (target description, hint)
# -------------------------------------------------------------------------
# Repair hints: positive-only framing. Tell the agent what TO DO,
# never what not to do. LLMs can latch onto negative examples.
REPAIR_HINTS: dict[tuple[str, str], tuple[str, str]] = {
    ("Bash", "Error"): (
        "shell command reliability",
        "Use absolute paths for executables. Verify commands exist with "
        "'which' or 'where' before running. Set working directory explicitly.",
    ),
    ("Bash", "Null"): (
        "shell command output",
        "Use absolute paths and explicit environment variables. "
        "Confirm the target file/directory exists before operating on it.",
    ),
    ("Read", "Error"): (
        "file reading",
        "Use forward slashes in paths. Verify the file exists with Glob "
        "before reading. Use the exact path from Glob results.",
    ),
    ("Read", "Null"): (
        "file reading",
        "Verify the file has content with Glob (check file size > 0). "
        "Use git status to confirm the file is tracked and current.",
    ),
    ("Grep", "Null"): (
        "code search",
        "Use broad patterns first, then narrow down. Try case-insensitive "
        "search (-i flag). Verify the search path contains the expected files.",
    ),
    ("Grep", "Error"): (
        "code search",
        "Use simple literal patterns first. Escape special regex characters. "
        "Verify the search directory exists with Glob first.",
    ),
    ("Glob", "Null"): (
        "file discovery",
        "Use broad glob patterns like **/*.ext first, then filter. "
        "Verify the base directory exists. Try alternative file extensions.",
    ),
    ("Edit", "Error"): (
        "file editing",
        "Always Read the file first to see current content. Copy the exact "
        "string to match, preserving whitespace and indentation.",
    ),
    ("Write", "Error"): (
        "file writing",
        "Verify the parent directory exists (use Bash mkdir -p if needed). "
        "Use forward slashes in paths.",
    ),
}

DEFAULT_REPAIR_HINT = (
    "tool usage",
    "Verify prerequisites before calling the tool. Use the simplest "
    "invocation first, then add complexity only if needed.",
)


def resolve_snapshot_path(db_path: str) -> Path:
    """Find the .ams.json snapshot paired with a .memory.jsonl file."""
    db = Path(db_path)
    # Convention: foo.memory.jsonl -> foo.memory.ams.json
    ams_path = db.with_suffix("").with_suffix(".ams.json")
    if ams_path.exists():
        return ams_path
    # Try sibling with just .ams.json suffix
    ams_path2 = db.parent / (db.stem.split(".")[0] + ".ams.json")
    if ams_path2.exists():
        return ams_path2
    return ams_path  # return expected path even if missing


def load_snapshot(ams_path: Path) -> dict:
    """Load and return the AMS snapshot JSON."""
    with open(ams_path, encoding="utf-8-sig") as f:
        return json.load(f)


def find_anomaly_notes(snapshot: dict) -> list[dict]:
    """Find all SmartList note objects under the anomaly bucket."""
    notes = []

    # Build a set of container IDs that are under the anomaly bucket path
    anomaly_container_ids: set[str] = set()
    for container in snapshot.get("containers", []):
        cid = container.get("containerId", "")
        if ANOMALY_BUCKET_PATH in cid:
            anomaly_container_ids.add(cid)

    # Also check link nodes for membership
    member_containers: dict[str, set[str]] = defaultdict(set)
    for link_node in snapshot.get("linkNodes", []):
        cid = link_node.get("containerId", "")
        oid = link_node.get("objectId", "")
        if cid and oid:
            member_containers[oid].add(cid)

    for obj in snapshot.get("objects", []):
        oid = obj.get("objectId", "")
        ok = obj.get("objectKind", "")

        # Check if it's a note-like object under the anomaly bucket
        is_anomaly_note = False
        if any(cid in anomaly_container_ids for cid in member_containers.get(oid, set())):
            is_anomaly_note = True
        # Also match by objectId pattern (notes created by emit_anomaly_notes)
        if ANOMALY_BUCKET_PATH in oid or ok == "smartlist_note":
            if any(cid in anomaly_container_ids for cid in member_containers.get(oid, set())):
                is_anomaly_note = True

        # Fallback: check provenance source
        prov = (obj.get("semanticPayload") or {}).get("provenance") or {}
        if prov.get("source") == "fep-anomaly-detector":
            is_anomaly_note = True

        if is_anomaly_note:
            notes.append(obj)

    return notes


def is_processed(obj: dict) -> bool:
    """Check if a note has already been processed."""
    prov = (obj.get("semanticPayload") or {}).get("provenance") or {}
    return prov.get("processed") is True


def get_provenance(obj: dict) -> dict:
    """Extract provenance fields from an object."""
    return (obj.get("semanticPayload") or {}).get("provenance") or {}


def mark_processed(snapshot: dict, object_ids: set[str]) -> int:
    """Patch provenance.processed = true for the given object IDs."""
    count = 0
    for obj in snapshot.get("objects", []):
        if obj.get("objectId") in object_ids:
            sp = obj.setdefault("semanticPayload", {})
            prov = sp.setdefault("provenance", {})
            prov["processed"] = True
            count += 1
    return count


def save_snapshot(snapshot: dict, ams_path: Path) -> None:
    """Write the snapshot back atomically."""
    tmp = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".ams.json",
        dir=str(ams_path.parent),
        delete=False,
        encoding="utf-8",
    )
    try:
        json.dump(snapshot, tmp, ensure_ascii=False, indent=2)
        tmp.close()
        os.replace(tmp.name, str(ams_path))
    except BaseException:
        tmp.close()
        os.unlink(tmp.name)
        raise


GUIDANCE_JSON = REPO_ROOT / ".claude" / "fep-tool-guidance.json"


def save_lesson(tool_name: str, outcome: str, hint: str, count: int, max_fe: float) -> bool:
    """Persist guidance to .claude/fep-tool-guidance.json for PreToolUse injection.

    Only positive guidance is stored — never examples of wrong behavior.
    Each tool gets a list of tips. Duplicates are skipped.
    """
    try:
        if GUIDANCE_JSON.exists():
            with open(GUIDANCE_JSON, encoding="utf-8") as f:
                guidance = json.load(f)
        else:
            guidance = {"tools": {}}

        tools = guidance.setdefault("tools", {})
        tips = tools.setdefault(tool_name, [])

        # Add hint if not already present (avoid duplicates)
        if hint not in tips:
            tips.append(hint)
            with open(GUIDANCE_JSON, "w", encoding="utf-8") as f:
                json.dump(guidance, f, indent=2, ensure_ascii=False)
            print(f"  Saved guidance for {tool_name}: {hint[:60]}...")
            return True
        else:
            print(f"  Guidance already exists for {tool_name}, skipping.")
            return True
    except Exception as e:
        print(f"  WARNING: Failed to save guidance: {e}", file=sys.stderr)
        return False


def trigger_repair(
    tool_name: str,
    outcome: str,
    count: int,
    max_fe: float,
    note_ids: list[str],
    dry_run: bool,
) -> bool:
    """Call callstack interrupt --policy repair for a tool failure group."""
    target, hint = REPAIR_HINTS.get((tool_name, outcome), DEFAULT_REPAIR_HINT)

    reason = (
        f"FEP anomaly: {count} anomalous {tool_name} {outcome} event(s), "
        f"max FE={max_fe:.2f} — {target}"
    )
    context = f"tool={tool_name}, outcome={outcome}, count={count}, note_ids={','.join(note_ids)}"
    repair_hint = hint

    if dry_run:
        print(f"  [DRY RUN] Would trigger repair: {reason}")
        print(f"            Hint: {repair_hint}")
        return True

    # Save positive-only lesson to permanent memory
    save_lesson(tool_name, outcome, hint, count, max_fe)

    cmd_args = [
        "callstack", "interrupt",
        "--policy", "repair",
        "--reason", reason,
        "--context", context,
        "--repair-hint", repair_hint,
        "--actor-id", "fep-repair-trigger",
    ]

    # Use ams.bat to invoke callstack interrupt
    ams_bat = REPO_ROOT / "scripts" / "ams.bat"
    if ams_bat.exists():
        cmd = [str(ams_bat)] + cmd_args
    else:
        # Fallback to Python ams.py
        cmd = [sys.executable, str(SCRIPT_DIR / "ams.py")] + cmd_args

    result = subprocess.run(cmd, cwd=repo_root(), capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  WARNING: callstack interrupt failed (rc={result.returncode})", file=sys.stderr)
        if result.stderr:
            print(f"    {result.stderr.strip()}", file=sys.stderr)
        return False

    print(f"  Triggered repair: {reason}")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Trigger repair interrupts from FEP tool anomaly notes"
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to .memory.jsonl database",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without actually triggering repairs",
    )
    parser.add_argument(
        "--fe-threshold",
        type=float,
        default=DEFAULT_FE_THRESHOLD,
        help=f"Free-energy threshold for single-event trigger (default: {DEFAULT_FE_THRESHOLD})",
    )
    parser.add_argument(
        "--count-threshold",
        type=int,
        default=DEFAULT_COUNT_THRESHOLD,
        help=f"Number of same-tool anomalies to trigger repair (default: {DEFAULT_COUNT_THRESHOLD})",
    )
    args = parser.parse_args()

    ams_path = resolve_snapshot_path(args.db)
    if not ams_path.exists():
        print(f"No AMS snapshot found at {ams_path} — skipping.", file=sys.stderr)
        sys.exit(0)

    snapshot = load_snapshot(ams_path)
    all_notes = find_anomaly_notes(snapshot)
    unprocessed = [n for n in all_notes if not is_processed(n)]

    if not unprocessed:
        print("No unprocessed anomaly notes found.")
        return

    print(f"Found {len(unprocessed)} unprocessed anomaly notes (of {len(all_notes)} total).")

    # Group by (tool_name, outcome)
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for note in unprocessed:
        prov = get_provenance(note)
        tool = prov.get("tool_name", "unknown")
        outcome = prov.get("outcome", "unknown")
        groups[(tool, outcome)].append(note)

    triggered_note_ids: set[str] = set()

    for (tool_name, outcome), notes in groups.items():
        fes = []
        note_ids = []
        for n in notes:
            prov = get_provenance(n)
            fe = prov.get("free_energy", 0.0)
            if isinstance(fe, (int, float)):
                fes.append(float(fe))
            note_ids.append(n.get("objectId", "?"))

        max_fe = max(fes) if fes else 0.0
        count = len(notes)

        # Trigger if: any single event has FE > fe_threshold, OR count >= count_threshold
        should_trigger = max_fe > args.fe_threshold or count >= args.count_threshold

        if should_trigger:
            success = trigger_repair(
                tool_name, outcome, count, max_fe, note_ids, args.dry_run
            )
            if success:
                triggered_note_ids.update(note_ids)
        else:
            print(
                f"  {tool_name}/{outcome}: {count} notes, max FE={max_fe:.2f} "
                f"— below thresholds (FE>{args.fe_threshold}, count>={args.count_threshold})"
            )

    # Mark processed notes
    if triggered_note_ids and not args.dry_run:
        marked = mark_processed(snapshot, triggered_note_ids)
        save_snapshot(snapshot, ams_path)
        print(f"\nMarked {marked} anomaly notes as processed.")
    elif triggered_note_ids and args.dry_run:
        print(f"\n[DRY RUN] Would mark {len(triggered_note_ids)} notes as processed.")

    if not triggered_note_ids:
        print("\nNo repairs triggered.")


if __name__ == "__main__":
    main()
