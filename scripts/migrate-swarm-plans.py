#!/usr/bin/env python3
"""One-time migration script: move swarm-plan execution state from factories into per-plan stores.

Each plan gets its own write-service store at:
  shared-memory/system-memory/swarm-plans/<plan-name>.memory.jsonl

Usage:
  python scripts/migrate-swarm-plans.py [--dry-run]

Options:
  --dry-run   Print what would be migrated without writing any files.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
_RUST_EXE_NAME = "ams-core-kernel.exe" if sys.platform == "win32" else "ams-core-kernel"
RUST_EXE = REPO_ROOT / "rust" / "ams-core-kernel" / "target" / "release" / _RUST_EXE_NAME
FACTORIES_JSONL = (
    REPO_ROOT / "shared-memory" / "system-memory" / "factories" / "factories.memory.jsonl"
)
SWARM_PLANS_DIR = REPO_ROOT / "shared-memory" / "system-memory" / "swarm-plans"


def run(args: list[str], capture: bool = True) -> "subprocess.CompletedProcess[str]":
    return subprocess.run(
        args,
        capture_output=capture,
        text=True,
    )


def list_plans_in_store(store_path: Path) -> list[str]:
    """Return plan names reported by swarm-plan-list for the given store."""
    result = run([str(RUST_EXE), "swarm-plan-list", "--input", str(store_path)])
    plans = []
    for line in result.stdout.splitlines():
        stripped = line.strip()
        if stripped.startswith("(") or stripped == "Projects:":
            continue
        # Lines look like: "  p7-fep-cache-signal [work/active]  cursor=..."
        parts = stripped.split()
        if parts:
            plans.append(parts[0])
    return plans


def plan_already_migrated(plan_name: str) -> bool:
    """Return True if the per-plan store exists and contains valid roots."""
    dest = SWARM_PLANS_DIR / f"{plan_name}.memory.jsonl"
    if not dest.exists():
        return False
    plans = list_plans_in_store(dest)
    return plan_name in plans


def migrate_plan(plan_name: str, dry_run: bool) -> dict:
    """Migrate a single plan. Returns a result dict."""
    dest = SWARM_PLANS_DIR / f"{plan_name}.memory.jsonl"

    result = {
        "plan": plan_name,
        "dest": str(dest),
        "status": "UNKNOWN",
        "objects": "?",
        "containers": "?",
        "note": "",
    }

    if plan_already_migrated(plan_name):
        result["status"] = "SKIP"
        result["note"] = "already migrated"
        return result

    if dry_run:
        result["status"] = "DRY-RUN"
        result["note"] = "would migrate"
        return result

    proc = run([
        str(RUST_EXE),
        "swarm-plan-migrate",
        "--from", str(FACTORIES_JSONL),
        "--to", str(dest),
        "--project", plan_name,
    ])

    if proc.returncode != 0:
        result["status"] = "FAIL"
        result["note"] = proc.stderr.strip() or "non-zero exit"
        return result

    # Parse output: "migrated_objects=N migrated_containers=M plan=name"
    for token in proc.stdout.split():
        if token.startswith("migrated_objects="):
            result["objects"] = token.split("=", 1)[1]
        elif token.startswith("migrated_containers="):
            result["containers"] = token.split("=", 1)[1]

    # Verify
    verified_plans = list_plans_in_store(dest)
    if plan_name in verified_plans:
        result["status"] = "OK"
    else:
        result["status"] = "FAIL"
        result["note"] = f"verification failed: plan not found in dest (got: {verified_plans})"

    return result


def print_summary(results: list[dict]) -> None:
    col_widths = {
        "plan": max(len(r["plan"]) for r in results),
        "status": 8,
        "objects": 7,
        "containers": 10,
    }
    header = (
        f"{'Plan':<{col_widths['plan']}}  "
        f"{'Status':<{col_widths['status']}}  "
        f"{'Objects':<{col_widths['objects']}}  "
        f"{'Containers':<{col_widths['containers']}}  Note"
    )
    print()
    print(header)
    print("-" * (len(header) + 20))
    for r in results:
        print(
            f"{r['plan']:<{col_widths['plan']}}  "
            f"{r['status']:<{col_widths['status']}}  "
            f"{str(r['objects']):<{col_widths['objects']}}  "
            f"{str(r['containers']):<{col_widths['containers']}}  "
            f"{r['note']}"
        )
    print()
    failed = [r for r in results if r["status"] == "FAIL"]
    if failed:
        print(f"WARNING: {len(failed)} plan(s) failed migration: {[r['plan'] for r in failed]}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Print plan without writing files")
    args = parser.parse_args()

    if not RUST_EXE.exists():
        print(f"ERROR: ams-core-kernel not found at {RUST_EXE}")
        print("Run: cargo build --release  (in rust/ams-core-kernel/)")
        return 1

    if not FACTORIES_JSONL.exists():
        print(f"ERROR: factories store not found at {FACTORIES_JSONL}")
        return 1

    # List all plans in factories store
    print(f"Reading plan list from: {FACTORIES_JSONL}")
    plan_names = list_plans_in_store(FACTORIES_JSONL)
    if not plan_names:
        print("No plans found in factories store.")
        return 0

    print(f"Found {len(plan_names)} plan(s): {', '.join(plan_names)}")
    if args.dry_run:
        print("[dry-run mode — no files will be written]")

    SWARM_PLANS_DIR.mkdir(parents=True, exist_ok=True)

    results = []
    for plan_name in plan_names:
        print(f"  {'[dry-run] ' if args.dry_run else ''}migrating {plan_name}...", end=" ", flush=True)
        r = migrate_plan(plan_name, dry_run=args.dry_run)
        print(r["status"])
        results.append(r)

    print_summary(results)

    failed = [r for r in results if r["status"] == "FAIL"]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
