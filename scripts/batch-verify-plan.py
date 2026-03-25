#!/usr/bin/env python3
"""
Batch verification command for swarm-plan nodes.

Usage:
    python scripts/batch-verify-plan.py <plan-name> [--workers N] [--output report.md]

Given a per-plan JSONL file, extracts all completed nodes, spawns a parallel
Claude subagent for each one, collects verdicts (VERIFIED / PARTIAL / FRAUDULENT),
and writes an aggregate markdown report.

Registered in scripts/ams.bat as:
    ams batch-verify <plan-name>
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SWARM_PLAN_DIR = REPO_ROOT / "shared-memory" / "system-memory" / "swarm-plans"


# ---------------------------------------------------------------------------
# Store helpers
# ---------------------------------------------------------------------------

def load_plan_store(plan_name: str) -> dict:
    """Load the per-plan JSONL (which is actually a JSON object with 'objects' key)."""
    path = SWARM_PLAN_DIR / f"{plan_name}.memory.jsonl"
    if not path.exists():
        # Also try .memory.ams.json
        path = SWARM_PLAN_DIR / f"{plan_name}.memory.ams.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Per-plan store not found for '{plan_name}'. "
            f"Looked at: {SWARM_PLAN_DIR / (plan_name + '.memory.jsonl')}"
        )
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def extract_completed_nodes(store: dict) -> list[dict]:
    """
    Walk all smartlist_bucket objects and collect those with state='completed'.
    Returns a list of dicts with keys: path, title, description, return_text.
    """
    objects = store.get("objects", [])
    completed = []

    # Build a lookup: objectId -> object
    by_id: dict[str, dict] = {o["objectId"]: o for o in objects}

    for obj in objects:
        if obj.get("objectKind") != "smartlist_bucket":
            continue
        prov = obj.get("semanticPayload", {}).get("provenance", {})
        state = prov.get("state") or prov.get("status") or prov.get("node_state")
        if state != "completed":
            continue

        path = prov.get("path", obj["objectId"])
        raw_title = prov.get("title") or prov.get("display_name") or path.split("/")[-1]
        # "00-node" is an internal execution marker — use the parent path segment instead
        if raw_title in ("00-node", "10-children", "20-observations"):
            parts = path.split("/")
            # Find the segment just before the "00-node" suffix
            for i, seg in enumerate(parts):
                if seg == raw_title and i > 0:
                    raw_title = parts[i - 1]
                    break
        title = raw_title
        description = prov.get("description", "")

        # Find the return text: it's stored in a smartlist_note whose title starts with "return:"
        # and whose path is under this node's observations bucket.
        obs_path = path + "/20-observations"
        return_text = ""
        for note in objects:
            if note.get("objectKind") != "smartlist_note":
                continue
            note_prov = note.get("semanticPayload", {}).get("provenance", {})
            note_title = note_prov.get("title", "")
            if note_title.startswith("return:") and title.lower() in note_title.lower():
                return_text = note_prov.get("text", "")
                break

        completed.append({
            "path": path,
            "title": title,
            "description": description,
            "return_text": return_text,
        })

    return completed


# ---------------------------------------------------------------------------
# Verifier agent
# ---------------------------------------------------------------------------

def format_verifier_prompt(node: dict) -> str:
    title = node["title"]
    description = node["description"] or "No spec available."
    return_text = node["return_text"] or "(No return text recorded)"
    return f"""You are verifying whether a worker agent genuinely completed its task.

## Task: {title}

## Task Spec (what was supposed to be done)
{description}

## Worker Return Text (what the worker claimed)
{return_text}

## Instructions

Check whether the deliverables described in the spec actually exist in the codebase.
Use Glob, Grep, and Read to inspect the actual files and logic. Do NOT modify any files.

Output your verdict as the FIRST line using EXACTLY one of these formats:
- VERIFIED: <summary>
- PARTIAL: <what is incomplete>
- FRAUDULENT: <what is missing or fabricated>
- UNVERIFIABLE: <why>

Then provide brief supporting evidence (2-5 lines).
"""


def spawn_verifier(node: dict, model: str | None = None, timeout: int = 180) -> dict:
    """
    Run a headless Claude verifier for one completed node.
    Returns a dict with keys: title, path, verdict, evidence, raw_output, error.
    """
    title = node["title"]
    prompt = format_verifier_prompt(node)
    cmd = [
        "claude", "-p", prompt,
        "--permission-mode", "bypassPermissions",
        "--output-format", "text",
    ]
    if model:
        cmd.extend(["--model", model])

    env = {**os.environ, "AMS_HOOK_SKIP": "1"}
    result = {
        "title": title,
        "path": node["path"],
        "verdict": "ERROR",
        "evidence": "",
        "raw_output": "",
        "error": None,
    }

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout,
        )
        output = proc.stdout.strip()
        result["raw_output"] = output

        # Parse verdict from first non-empty line
        for line in output.splitlines():
            line = line.strip()
            if not line:
                continue
            upper = line.upper()
            for verdict in ("VERIFIED", "PARTIAL", "FRAUDULENT", "UNVERIFIABLE"):
                if upper.startswith(verdict):
                    result["verdict"] = verdict
                    result["evidence"] = "\n".join(output.splitlines()[1:5]).strip()
                    break
            else:
                continue
            break

        if proc.returncode != 0 and result["verdict"] == "ERROR":
            result["error"] = proc.stderr[:500] if proc.stderr else "non-zero exit"

    except subprocess.TimeoutExpired:
        result["error"] = f"Timed out after {timeout}s"
    except FileNotFoundError:
        result["error"] = "claude binary not found on PATH"
    except Exception as exc:
        result["error"] = str(exc)

    return result


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

VERDICT_EMOJI = {
    "VERIFIED": "✅",
    "PARTIAL": "⚠️",
    "FRAUDULENT": "❌",
    "UNVERIFIABLE": "❓",
    "ERROR": "💥",
}


def build_report(plan_name: str, results: list[dict]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total = len(results)
    counts: dict[str, int] = {}
    for r in results:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1

    lines: list[str] = [
        f"# Batch Verification Report: `{plan_name}`",
        f"",
        f"Generated: {now}  |  Nodes checked: {total}",
        f"",
        "## Summary",
        "",
        "| Verdict | Count |",
        "|---------|-------|",
    ]
    for verdict in ("VERIFIED", "PARTIAL", "FRAUDULENT", "UNVERIFIABLE", "ERROR"):
        n = counts.get(verdict, 0)
        if n:
            emoji = VERDICT_EMOJI[verdict]
            lines.append(f"| {emoji} {verdict} | {n} |")

    lines += [
        "",
        "## Node Results",
        "",
        "| Node | Verdict | Evidence |",
        "|------|---------|----------|",
    ]
    for r in results:
        emoji = VERDICT_EMOJI.get(r["verdict"], "")
        title = r["title"]
        verdict = r["verdict"]
        evidence = (r["evidence"] or r["error"] or "").replace("\n", " ")[:120]
        lines.append(f"| `{title}` | {emoji} {verdict} | {evidence} |")

    lines += ["", "## Details", ""]
    for r in results:
        emoji = VERDICT_EMOJI.get(r["verdict"], "")
        lines.append(f"### {emoji} `{r['title']}`")
        lines.append(f"")
        lines.append(f"**Path:** `{r['path']}`")
        lines.append(f"**Verdict:** {r['verdict']}")
        if r["error"]:
            lines.append(f"**Error:** {r['error']}")
        if r["evidence"]:
            lines.append(f"")
            lines.append(r["evidence"])
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch-verify all completed nodes in a swarm-plan."
    )
    parser.add_argument("plan_name", help="Name of the swarm-plan (e.g. insights-action-items)")
    parser.add_argument(
        "--workers", "-w", type=int, default=3,
        help="Max parallel verifier agents (default: 3)",
    )
    parser.add_argument(
        "--output", "-o", type=str, default=None,
        help="Write markdown report to this file (default: stdout)",
    )
    parser.add_argument(
        "--model", type=str, default=None,
        help="Claude model to use for verifiers (default: inherited)",
    )
    parser.add_argument(
        "--timeout", type=int, default=180,
        help="Per-agent timeout in seconds (default: 180)",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List completed nodes without running verifiers",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # Load store
    try:
        store = load_plan_store(args.plan_name)
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    completed = extract_completed_nodes(store)
    if not completed:
        print(f"No completed nodes found in plan '{args.plan_name}'.", file=sys.stderr)
        return 0

    if args.list:
        print(f"Completed nodes in '{args.plan_name}':")
        for node in completed:
            print(f"  {node['title']}")
        return 0

    print(
        f"Found {len(completed)} completed node(s) in '{args.plan_name}'. "
        f"Spawning up to {args.workers} verifier(s)...",
        file=sys.stderr,
    )

    results: list[dict] = []
    lock = threading.Lock()

    def run_one(node: dict) -> dict:
        r = spawn_verifier(node, model=args.model, timeout=args.timeout)
        with lock:
            emoji = VERDICT_EMOJI.get(r["verdict"], "")
            print(f"  {emoji} {r['title']}: {r['verdict']}", file=sys.stderr)
        return r

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_one, node): node for node in completed}
        for future in as_completed(futures):
            results.append(future.result())

    # Sort by verdict severity: FRAUDULENT first, then PARTIAL, ERROR, UNVERIFIABLE, VERIFIED
    severity = {"FRAUDULENT": 0, "PARTIAL": 1, "ERROR": 2, "UNVERIFIABLE": 3, "VERIFIED": 4}
    results.sort(key=lambda r: severity.get(r["verdict"], 5))

    report = build_report(args.plan_name, results)

    if args.output:
        Path(args.output).write_text(report, encoding="utf-8")
        print(f"Report written to: {args.output}", file=sys.stderr)
    else:
        print(report)

    # Exit non-zero if any FRAUDULENT or ERROR results
    if any(r["verdict"] in ("FRAUDULENT", "ERROR") for r in results):
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
