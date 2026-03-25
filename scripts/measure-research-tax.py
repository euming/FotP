#!/usr/bin/env python3
"""
measure-research-tax.py — Quantify cold-start exploratory tool usage in a session.

Usage:
  python scripts/measure-research-tax.py <session-id-prefix>
  python scripts/measure-research-tax.py <session-id-prefix> --window 5
  python scripts/measure-research-tax.py --compare <id-a> <id-b>
  python scripts/measure-research-tax.py --recent 5

"Research tax" = exploratory file-system calls (Read, Glob, Grep, Bash ls/find/cat/head)
fired in the first --window assistant turns.  Lower is better; 0 means the agent started
working from injected context alone.
"""
import argparse
import glob
import json
import os
import re
import sys

SESSIONS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "..", "..", "..", ".claude",
    "projects",
    "C--Users-eumin-wkspaces-git-NetworkGraphMemory",
)
SESSIONS_DIR = os.path.normpath(SESSIONS_DIR)

EXPLORATORY_TOOLS = {"Read", "Glob", "Grep"}
EXPLORATORY_BASH_PATTERNS = re.compile(
    r'\b(ls|find|cat|head|tail|grep|rg|dir)\b'
)


def find_session(prefix: str) -> str | None:
    pattern = os.path.join(SESSIONS_DIR, f"{prefix}*.jsonl")
    matches = glob.glob(pattern)
    if not matches:
        return None
    return sorted(matches)[-1]


def load_session(path: str) -> list[dict]:
    entries = []
    with open(path, encoding='utf-8', errors='replace') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return entries


def score_session(entries: list[dict], window: int = 5) -> dict:
    """Count exploratory tool calls in the first `window` assistant turns."""
    results = {
        "assistant_turns_scanned": 0,
        "total_exploratory_calls": 0,
        "by_tool": {},
        "calls": [],  # (turn_idx, tool, detail)
    }

    assistant_turn = 0
    for entry in entries:
        if entry.get("type") != "assistant":
            continue
        if assistant_turn >= window:
            break
        assistant_turn += 1

        msg = entry.get("message", {})
        for block in msg.get("content", []):
            if not isinstance(block, dict) or block.get("type") != "tool_use":
                continue
            tool = block.get("name", "")
            inp = block.get("input", {})

            is_exploratory = False
            detail = ""

            if tool in EXPLORATORY_TOOLS:
                is_exploratory = True
                if tool == "Read":
                    detail = inp.get("file_path", "")
                elif tool == "Glob":
                    detail = inp.get("pattern", "")
                elif tool == "Grep":
                    detail = inp.get("pattern", "")
            elif tool == "Bash":
                cmd = inp.get("command", "")
                if EXPLORATORY_BASH_PATTERNS.search(cmd):
                    # Exclude ams.bat calls and git commands (not cold-start exploration)
                    if not re.search(r'ams\.bat|ams\.py|git |cargo ', cmd):
                        is_exploratory = True
                        detail = cmd[:80]

            if is_exploratory:
                results["total_exploratory_calls"] += 1
                results["by_tool"][tool] = results["by_tool"].get(tool, 0) + 1
                results["calls"].append((assistant_turn, tool, detail))

    results["assistant_turns_scanned"] = assistant_turn
    return results


def format_score(session_id: str, path: str, score: dict, window: int) -> str:
    lines = [
        f"Session : {os.path.basename(path)}",
        f"Window  : first {window} assistant turns ({score['assistant_turns_scanned']} scanned)",
        f"Tax     : {score['total_exploratory_calls']} exploratory calls",
    ]
    if score["by_tool"]:
        breakdown = ", ".join(f"{k}×{v}" for k, v in sorted(score["by_tool"].items()))
        lines.append(f"Breakdown: {breakdown}")
    if score["calls"]:
        lines.append("Detail:")
        for turn, tool, detail in score["calls"]:
            lines.append(f"  [turn {turn}] {tool}: {detail[:70]}")
    return "\n".join(lines)


def recent_sessions(n: int) -> list[str]:
    pattern = os.path.join(SESSIONS_DIR, "*.jsonl")
    files = glob.glob(pattern)
    files = [f for f in files if not f.endswith("memory")]
    files.sort(key=os.path.getmtime, reverse=True)
    return files[:n]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("session", nargs="?", help="Session ID prefix (8+ chars)")
    parser.add_argument("--window", type=int, default=5, metavar="N",
                        help="Number of assistant turns to scan (default: 5)")
    parser.add_argument("--compare", nargs=2, metavar=("ID_A", "ID_B"),
                        help="Compare two sessions side-by-side")
    parser.add_argument("--recent", type=int, metavar="N",
                        help="Score the N most recent sessions")
    args = parser.parse_args()

    if args.compare:
        ids = args.compare
        paths = [find_session(i) for i in ids]
        for sid, path in zip(ids, paths):
            if not path:
                print(f"Session not found: {sid}", file=sys.stderr)
                sys.exit(1)
        scores = [score_session(load_session(p), args.window) for p in paths]
        labels = ["A (baseline?)", "B (with AKC?)"]
        for label, path, sc in zip(labels, paths, scores):
            print(f"=== {label} ===")
            print(format_score("", path, sc, args.window))
            print()
        delta = scores[1]["total_exploratory_calls"] - scores[0]["total_exploratory_calls"]
        pct = (delta / max(scores[0]["total_exploratory_calls"], 1)) * 100
        print(f"Delta: {delta:+d} calls ({pct:+.0f}%)")
        return

    if args.recent:
        files = recent_sessions(args.recent)
        if not files:
            print("No sessions found.", file=sys.stderr)
            sys.exit(1)
        rows = []
        for path in files:
            entries = load_session(path)
            sc = score_session(entries, args.window)
            name = os.path.basename(path)
            # Try to get first user message for label
            label = ""
            for e in entries:
                if e.get("type") == "user":
                    msg = e.get("message", {})
                    content = msg.get("content", "")
                    if isinstance(content, list):
                        for c in content:
                            if isinstance(c, dict) and c.get("type") == "text":
                                label = c["text"][:60].replace("\n", " ")
                                break
                    elif isinstance(content, str):
                        label = content[:60].replace("\n", " ")
                    if label:
                        break
            tax = sc["total_exploratory_calls"]
            rows.append((tax, name[:12], label))
        print(f"{'Tax':>4}  {'Session':12}  {'First message'}")
        print("-" * 80)
        for tax, name, label in sorted(rows):
            print(f"{tax:>4}  {name}  {label}")
        return

    if not args.session:
        parser.print_help()
        sys.exit(1)

    path = find_session(args.session)
    if not path:
        print(f"Session not found: {args.session}", file=sys.stderr)
        print(f"Looked in: {SESSIONS_DIR}", file=sys.stderr)
        sys.exit(1)

    entries = load_session(path)
    sc = score_session(entries, args.window)
    print(format_score(args.session, path, sc, args.window))


if __name__ == "__main__":
    main()
