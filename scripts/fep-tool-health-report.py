#!/usr/bin/env python3
"""
fep-tool-health-report.py

Generates a tool health report by comparing tool success rates across
two time windows. Writes the report to a CLAUDE.local.md-compatible
section and to stdout.

Usage:
    python fep-tool-health-report.py --db <path.memory.jsonl>
        [--recent-days 7] [--baseline-days 30] [--output <path>]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


def ams_path(db_path: str) -> str:
    p = Path(db_path).resolve()
    if p.suffix == ".json" and p.exists():
        return str(p)
    return str(p.parent / (p.stem + ".ams.json"))


def load_snapshot(db_path: str) -> dict:
    ap = ams_path(db_path)
    if not os.path.exists(ap):
        print(f"ERROR: AMS snapshot not found: {ap}", file=sys.stderr)
        sys.exit(1)
    with open(ap, encoding="utf-8-sig") as f:
        return json.load(f)


def extract_tool_calls(snapshot: dict) -> list[dict]:
    """Extract tool-call objects with provenance from AMS snapshot."""
    calls = []
    objects = snapshot.get("objects", {})
    if isinstance(objects, list):
        objects = {o.get("objectId", ""): o for o in objects}
    for obj_id, obj in objects.items():
        if obj.get("objectKind", obj.get("object_kind")) != "tool-call":
            continue
        sp = obj.get("semanticPayload") or obj.get("semantic_payload") or {}
        prov = sp.get("provenance") or {}
        tool_name = prov.get("tool_name", "")
        is_error = prov.get("is_error", False)
        result_preview = prov.get("result_preview", "")
        ts_str = prov.get("ts", obj.get("createdAt", obj.get("created_at", "")))
        if not tool_name or not ts_str:
            continue
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        # Classify outcome
        if is_error:
            outcome = "error"
        elif not result_preview or result_preview.strip() in ("", "No matches found", "Found 0 files"):
            outcome = "null"
        else:
            outcome = "success"
        duration_s = prov.get("duration_s")
        try:
            duration_s = float(duration_s) if duration_s is not None else None
        except (TypeError, ValueError):
            duration_s = None
        calls.append({"tool_name": tool_name, "ts": ts, "outcome": outcome, "duration_s": duration_s})
    return calls


def compute_rates(calls: list[dict]) -> dict[str, dict]:
    """Compute per-tool success/error/null rates and duration stats."""
    by_tool: dict[str, list[dict]] = defaultdict(list)
    for c in calls:
        by_tool[c["tool_name"]].append(c)
    rates = {}
    for tool, tool_calls in by_tool.items():
        n = len(tool_calls)
        outcomes = [c["outcome"] for c in tool_calls]
        durations = sorted(d for c in tool_calls if (d := c.get("duration_s")) is not None)
        avg_duration = sum(durations) / len(durations) if durations else None
        p95_duration = durations[int(len(durations) * 0.95)] if durations else None
        max_duration = durations[-1] if durations else None
        rates[tool] = {
            "n": n,
            "success": sum(1 for o in outcomes if o == "success") / n,
            "error": sum(1 for o in outcomes if o == "error") / n,
            "null": sum(1 for o in outcomes if o == "null") / n,
            "avg_duration_s": avg_duration,
            "p95_duration_s": p95_duration,
            "max_duration_s": max_duration,
            "duration_n": len(durations),
        }
    return rates


def format_report(
    baseline_rates: dict, recent_rates: dict,
    baseline_days: int, recent_days: int,
) -> str:
    all_tools = sorted(set(baseline_rates) | set(recent_rates))

    # Build rows first so we can compute column widths
    header = ("Tool", f"Base({baseline_days}d)", "OK%", "Err%", f"Recent({recent_days}d)", "OK%", "Err%", "Delta")
    rows = []
    alerts = []
    for tool in all_tools:
        b = baseline_rates.get(tool, {"n": 0, "success": 0, "error": 0, "null": 0})
        r = recent_rates.get(tool, {"n": 0, "success": 0, "error": 0, "null": 0})
        if b["n"] == 0 and r["n"] == 0:
            continue
        delta = r["success"] - b["success"] if b["n"] > 0 and r["n"] > 0 else 0
        delta_str = f"{delta:+.0%}" if b["n"] > 0 and r["n"] > 0 else "n/a"
        rows.append((
            tool,
            str(b["n"]),
            f"{b['success']:.0%}",
            f"{b['error']:.0%}",
            str(r["n"]),
            f"{r['success']:.0%}",
            f"{r['error']:.0%}",
            delta_str,
        ))
        # Flag regressions
        if b["n"] >= 5 and r["n"] >= 3 and delta < -0.1:
            alerts.append(f"  ! {tool}: success dropped {delta_str} ({b['success']:.0%} -> {r['success']:.0%})")
        # Flag chronic failures
        if r["n"] >= 5 and r["success"] < 0.5:
            alerts.append(f"  ! {tool}: low success rate {r['success']:.0%} over {r['n']} recent calls")

    # Compute column widths
    all_rows = [header] + rows
    widths = [max(len(row[i]) for row in all_rows) for i in range(len(header))]

    def fmt_row(row):
        return "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row))

    lines = [
        "FEP Tool Health Report",
        "=" * 60,
        "",
        fmt_row(header),
        "  ".join("-" * w for w in widths),
    ]
    for row in rows:
        lines.append(fmt_row(row))

    if alerts:
        lines.append("")
        lines.append("Alerts:")
        lines.extend(alerts)
    else:
        lines.append("")
        lines.append("No regressions or chronic failures detected.")

    # Slow tools section: flag tools with avg > 10s or p95 > 30s in recent window
    slow_tools = []
    for tool in all_tools:
        r = recent_rates.get(tool, {})
        avg = r.get("avg_duration_s")
        p95 = r.get("p95_duration_s")
        dn = r.get("duration_n", 0)
        if dn < 2:
            continue
        reasons = []
        if avg is not None and avg > 10:
            reasons.append(f"avg {avg:.0f}s")
        if p95 is not None and p95 > 30:
            reasons.append(f"p95 {p95:.0f}s")
        if reasons:
            slow_tools.append((tool, reasons, r))

    if slow_tools:
        lines.append("")
        lines.append("Slow Tools (avg>10s or p95>30s):")
        for tool, reasons, r in sorted(slow_tools, key=lambda x: x[2].get("avg_duration_s") or 0, reverse=True):
            max_s = r.get("max_duration_s")
            max_str = f", max {max_s:.0f}s" if max_s is not None else ""
            lines.append(f"  ! {tool}: {', '.join(reasons)}{max_str} (n={r['duration_n']})")

    return "\n".join(lines)


def write_duration_guidance(recent_rates: dict, guidance_path: str) -> int:
    """Write slow-tool duration warnings into fep-tool-guidance.json.

    For each tool with avg_duration > 10s (and at least 2 duration observations),
    injects a WARNING hint into the guidance file that the pretool hook reads.
    Returns the number of tools updated.
    """
    guidance_file = Path(guidance_path)
    try:
        existing = json.loads(guidance_file.read_text(encoding="utf-8")) if guidance_file.exists() else {}
    except (OSError, json.JSONDecodeError):
        existing = {}

    tools_guidance: dict = existing.get("tools", {})

    updated = 0
    for tool, stats in recent_rates.items():
        avg = stats.get("avg_duration_s")
        p95 = stats.get("p95_duration_s")
        dn = stats.get("duration_n", 0)
        if dn < 2 or avg is None:
            continue
        if avg <= 10 and (p95 is None or p95 <= 30):
            continue

        # Build warning message
        parts = []
        if avg > 10:
            parts.append(f"averages {avg:.0f}s")
        if p95 is not None and p95 > 30:
            parts.append(f"p95={p95:.0f}s")
        max_s = stats.get("max_duration_s")
        if max_s is not None and max_s > 60:
            parts.append(f"max={max_s:.0f}s")
        warning = f"WARNING: {tool} {', '.join(parts)} — consider a faster alternative"

        # Preserve existing tips that aren't duration warnings, then add/replace ours
        existing_tips = tools_guidance.get(tool, [])
        if isinstance(existing_tips, str):
            existing_tips = [existing_tips]
        non_duration = [t for t in existing_tips if not t.startswith("WARNING:")]
        tools_guidance[tool] = non_duration + [warning]
        updated += 1

    # Remove stale duration warnings for tools that are no longer slow
    for tool in list(tools_guidance.keys()):
        if tool in recent_rates:
            continue
        tips = tools_guidance[tool]
        if isinstance(tips, str):
            continue
        tools_guidance[tool] = [t for t in tips if not t.startswith("WARNING:")]
        if not tools_guidance[tool]:
            del tools_guidance[tool]

    existing["tools"] = tools_guidance
    guidance_file.parent.mkdir(parents=True, exist_ok=True)
    guidance_file.write_text(json.dumps(existing, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return updated


def main() -> None:
    parser = argparse.ArgumentParser(description="FEP Tool Health Report")
    parser.add_argument("--db", required=True, help="Path to .memory.jsonl")
    parser.add_argument("--recent-days", type=int, default=7)
    parser.add_argument("--baseline-days", type=int, default=30)
    parser.add_argument("--output", default=None, help="Write report to file")
    parser.add_argument(
        "--write-guidance",
        default=None,
        metavar="PATH",
        help="Update fep-tool-guidance.json at PATH with duration warnings for slow tools",
    )
    args = parser.parse_args()

    snapshot = load_snapshot(args.db)
    all_calls = extract_tool_calls(snapshot)
    if not all_calls:
        print("No tool-call objects found in snapshot.")
        return

    now = datetime.now(timezone.utc)
    baseline_cutoff = now - timedelta(days=args.baseline_days)
    recent_cutoff = now - timedelta(days=args.recent_days)

    baseline_calls = [c for c in all_calls if c["ts"] >= baseline_cutoff]
    recent_calls = [c for c in all_calls if c["ts"] >= recent_cutoff]

    baseline_rates = compute_rates(baseline_calls)
    recent_rates = compute_rates(recent_calls)

    report = format_report(baseline_rates, recent_rates, args.baseline_days, args.recent_days)
    print(report)

    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(report + "\n")
        print(f"\nReport written to: {args.output}")

    if args.write_guidance:
        n = write_duration_guidance(recent_rates, args.write_guidance)
        print(f"\nDuration guidance updated: {n} tools written to {args.write_guidance}")


if __name__ == "__main__":
    main()
