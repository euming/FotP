#!/usr/bin/env python3
"""
Measure and compare context sizes between the old (markdown-file) and new
(callstack-first) context injection approaches.

Outputs a comparison table showing byte-level savings at session start and
per-prompt.
"""
from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
AMS_PY = REPO_ROOT / "scripts" / "ams.py"

# Paths for session-start files
CLAUDE_MD = REPO_ROOT / "CLAUDE.md"
CLAUDE_LOCAL_MD = REPO_ROOT / "CLAUDE.local.md"
GLOBAL_CLAUDE_MD = Path.home() / ".claude" / "CLAUDE.md"
MEMORY_MD = (
    Path.home()
    / ".claude"
    / "projects"
    / "C--Users-eumin-wkspaces-git-NetworkGraphMemory"
    / "memory"
    / "MEMORY.md"
)

sys.path.insert(0, str(SCRIPT_DIR))
from ams_common import build_rust_ams_cmd, rust_backend_env


def file_size(path: Path) -> int:
    if path.exists():
        return path.stat().st_size
    return 0


def git_show_size(rev_spec: str) -> int:
    result = subprocess.run(
        ["git", "show", rev_spec],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return 0
    return len(result.stdout.encode("utf-8"))


def run_ams(backend_root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    cmd = [
        sys.executable,
        str(AMS_PY),
        "callstack",
        "--corpus", "all",
        "--backend-root", str(backend_root),
        *args,
    ]
    return subprocess.run(cmd, cwd=REPO_ROOT, text=True, capture_output=True, check=False)


def measure_callstack_context(backend_root: Path) -> int:
    """Push root + child, observe on both, measure callstack context output."""
    run_ams(backend_root, "push", "measure-root", "--actor-id", "token-test")
    run_ams(
        backend_root, "observe",
        "--title", "root-obs",
        "--text", "Root-level observation for measurement.",
        "--actor-id", "token-test",
    )
    run_ams(backend_root, "push", "measure-child", "--actor-id", "token-test")
    run_ams(
        backend_root, "observe",
        "--title", "child-obs",
        "--text", "Child-level observation for measurement.",
        "--actor-id", "token-test",
    )
    ctx = run_ams(backend_root, "context")
    ctx_size = len(ctx.stdout.encode("utf-8"))

    # Cleanup
    run_ams(backend_root, "pop", "--return-text", "done", "--actor-id", "token-test")
    run_ams(backend_root, "pop", "--return-text", "done", "--actor-id", "token-test")
    return ctx_size


def measure_legacy_per_prompt() -> int:
    """Estimate legacy per-prompt cost from hook constants."""
    # Legacy hook injects: task_graph (up to 500 chars) + handoff (up to 700 chars)
    #   + search results (up to 800 chars) + wrapper text (~100 chars)
    # Use the configured max constants from the hook
    task_graph_max = 500 + 30   # TASK_GRAPH_OUTPUT_CHARS + header/footer
    handoff_max = 700 + 30      # HANDOFF_OUTPUT_CHARS + header/footer
    search_max = 800 + 50       # MAX_OUTPUT_CHARS + header/footer
    return task_graph_max + handoff_max + search_max


def fmt(n: int) -> str:
    return f"{n:,}"


def pct(old: int, new: int) -> str:
    if old == 0:
        return "n/a"
    reduction = (1 - new / old) * 100
    return f"{reduction:.0f}%"


def main() -> int:
    # --- Session-start context sizes ---
    old_claude_md = git_show_size("HEAD~1:CLAUDE.md")
    new_claude_md = file_size(CLAUDE_MD)

    # Global CLAUDE.md: not in repo git, use known pre-slim size
    old_global_claude_md = 1400  # estimated ~41 lines pre-slim
    new_global_claude_md = file_size(GLOBAL_CLAUDE_MD)

    claude_local_md = file_size(CLAUDE_LOCAL_MD)
    memory_md = file_size(MEMORY_MD)

    old_session_start = old_claude_md + old_global_claude_md + claude_local_md + memory_md
    new_session_start = new_claude_md + new_global_claude_md + claude_local_md + memory_md

    # --- Per-prompt context sizes ---
    legacy_per_prompt = measure_legacy_per_prompt()

    with tempfile.TemporaryDirectory(prefix="ams-token-measure-") as tmp:
        workspace = Path(tmp)
        backend_root = workspace / "backend"
        corpus_dir = workspace / "all-agents-sessions"
        backend_root.mkdir(parents=True, exist_ok=True)
        corpus_dir.mkdir(parents=True, exist_ok=True)
        (corpus_dir / "all-agents-sessions.memory.jsonl").write_text("", encoding="utf-8")
        callstack_per_prompt = measure_callstack_context(backend_root)

    # --- N-turn conversation totals ---
    turns = 5
    old_conversation = old_session_start + turns * legacy_per_prompt
    new_conversation = new_session_start + turns * callstack_per_prompt

    # --- Print table ---
    print()
    print("=" * 70)
    print("  Callstack-First Context: Token Savings Report")
    print("=" * 70)
    print()

    print("Session-start file sizes:")
    print(f"  {'File':<30} {'Old':>10} {'New':>10} {'Saved':>8}")
    print(f"  {'-'*30} {'-'*10} {'-'*10} {'-'*8}")
    print(f"  {'CLAUDE.md':<30} {fmt(old_claude_md):>10} {fmt(new_claude_md):>10} {pct(old_claude_md, new_claude_md):>8}")
    print(f"  {'~/.claude/CLAUDE.md':<30} {fmt(old_global_claude_md):>10} {fmt(new_global_claude_md):>10} {pct(old_global_claude_md, new_global_claude_md):>8}")
    print(f"  {'CLAUDE.local.md':<30} {fmt(claude_local_md):>10} {fmt(claude_local_md):>10} {'0%':>8}")
    print(f"  {'MEMORY.md':<30} {fmt(memory_md):>10} {fmt(memory_md):>10} {'0%':>8}")
    print()

    print("Aggregate comparison:")
    print(f"  {'Context Surface':<30} {'Old (bytes)':>12} {'New (bytes)':>12} {'Savings':>8}")
    print(f"  {'-'*30} {'-'*12} {'-'*12} {'-'*8}")
    print(f"  {'Session start (md files)':<30} {fmt(old_session_start):>12} {fmt(new_session_start):>12} {pct(old_session_start, new_session_start):>8}")
    print(f"  {'Per-prompt (hook)':<30} {fmt(legacy_per_prompt):>12} {fmt(callstack_per_prompt):>12} {pct(legacy_per_prompt, callstack_per_prompt):>8}")
    print(f"  {f'{turns}-turn conversation':<30} {fmt(old_conversation):>12} {fmt(new_conversation):>12} {pct(old_conversation, new_conversation):>8}")
    print()
    print("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(main())
