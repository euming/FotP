#!/usr/bin/env python3
"""PreToolUse hook: inject FEP-learned guidance before tool execution.

Reads .claude/fep-tool-guidance.json and outputs guidance for the
tool about to be called. Only fires if guidance exists for that tool.
Costs ~20 tokens when it fires, zero when no guidance exists.

Guidance file lookup order:
  1. <CWD>/.claude/fep-tool-guidance.json  (project-local, written by fep-tool-health-report.py --write-guidance)
  2. <HOOK_DIR>/../fep-tool-guidance.json   (global fallback)
"""
import json
import os
import sys

HOOK_DIR = os.path.dirname(os.path.abspath(__file__))
GLOBAL_GUIDANCE_PATH = os.path.join(HOOK_DIR, "..", "fep-tool-guidance.json")
LOCAL_GUIDANCE_PATH = os.path.join(os.getcwd(), ".claude", "fep-tool-guidance.json")


def load_guidance():
    """Load guidance, merging project-local over global (local wins on conflict)."""
    result: dict = {}
    for path in (GLOBAL_GUIDANCE_PATH, LOCAL_GUIDANCE_PATH):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            tools = data.get("tools", {})
            for tool, tips in tools.items():
                if isinstance(tips, str):
                    tips = [tips]
                existing = result.get(tool, [])
                if isinstance(existing, str):
                    existing = [existing]
                # Merge: keep non-WARNING tips from global, WARNING tips from latest source
                non_warn = [t for t in existing if not t.startswith("WARNING:")]
                result[tool] = non_warn + tips
        except (OSError, json.JSONDecodeError):
            pass
    return result


def main():
    try:
        raw = sys.stdin.read()
        data = json.loads(raw)
        tool_name = data.get("tool_name", "")
    except Exception:
        sys.exit(0)

    if not tool_name:
        sys.exit(0)

    tools_guidance = load_guidance()
    tips = tools_guidance.get(tool_name)
    if not tips:
        sys.exit(0)

    # Output concise guidance — this becomes context for the LLM
    if isinstance(tips, list):
        print(f"[FEP] {tool_name}: " + " | ".join(tips))
    else:
        print(f"[FEP] {tool_name}: {tips}")


if __name__ == "__main__":
    main()
