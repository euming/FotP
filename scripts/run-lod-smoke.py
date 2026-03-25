#!/usr/bin/env python3
"""Smoke test for the LOD (Level-of-Detail) summarizer."""
from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from ams_common import build_rust_ams_cmd, rust_backend_env

from swarm.lod import summarize_subtree, inject_context, _estimate_tokens


def run_kernel(backend_root: str, *args: str) -> subprocess.CompletedProcess[str]:
    cmd = build_rust_ams_cmd(*args)
    if cmd is None:
        raise RuntimeError("unable to locate the Rust AMS kernel binary or Cargo project")
    return subprocess.run(
        cmd, env=rust_backend_env(backend_root), text=True, capture_output=True, check=False,
    )


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def ensure_ok(result: subprocess.CompletedProcess[str], label: str) -> None:
    if result.returncode != 0:
        raise RuntimeError(f"{label} failed: rc={result.returncode}\n{result.stderr}")


def main() -> int:
    temp_dir = tempfile.TemporaryDirectory(prefix="ams-lod-smoke-")
    workspace = Path(temp_dir.name)

    backend_root = workspace / "backend"
    corpus_dir = workspace / "all-agents-sessions"
    backend_root.mkdir(parents=True, exist_ok=True)
    corpus_dir.mkdir(parents=True, exist_ok=True)
    corpus_path = corpus_dir / "all-agents-sessions.memory.jsonl"
    corpus_path.write_text("", encoding="utf-8")

    ip = str(corpus_path)
    br = str(backend_root)

    # --- Setup: create a SmartList tree ---
    print("=== Setup: Create SmartList bucket tree ===")
    for path in [
        "smartlist/project/alpha",
        "smartlist/project/alpha/sub1",
        "smartlist/project/alpha/sub2",
        "smartlist/project/alpha/sub2/deep1",
        "smartlist/project/beta",
        "smartlist/project/beta/sub1",
    ]:
        result = run_kernel(br, "smartlist-create", "--input", ip, "--path", path)
        ensure_ok(result, f"create {path}")
    print("  tree created")

    # Bootstrap locality for inject_context
    from swarm.locality import bootstrap_locality, assign_home_node
    bootstrap_locality(ip, backend_root=br)
    assign_home_node(ip, "agent-0", "smartlist/project/alpha", backend_root=br)

    # --- Test 1: summarize without budget (existing behavior) ---
    print("\n=== Test 1: summarize_subtree (no budget) ===")
    summary = summarize_subtree(ip, "smartlist/project", max_depth=3, backend_root=br)
    require(len(summary) > 0, "expected non-empty summary")
    require("alpha" in summary, "expected 'alpha' in summary")
    require("beta" in summary, "expected 'beta' in summary")
    print(f"  full summary ({_estimate_tokens(summary)} tokens):\n{summary}")

    # --- Test 2: summarize with generous budget (should return full) ---
    print("\n=== Test 2: summarize_subtree (large budget) ===")
    full_tokens = _estimate_tokens(summary)
    budgeted = summarize_subtree(ip, "smartlist/project", max_depth=3,
                                  token_budget=full_tokens + 100, backend_root=br)
    require(budgeted == summary, "large budget should return identical summary")
    print("  large budget: returned full summary (ok)")

    # --- Test 3: summarize with tight budget (should truncate) ---
    print("\n=== Test 3: summarize_subtree (tight budget) ===")
    tight = summarize_subtree(ip, "smartlist/project", max_depth=3,
                               token_budget=10, backend_root=br)
    require(len(tight) > 0, "expected non-empty tight summary")
    tight_tokens = _estimate_tokens(tight)
    print(f"  tight summary ({tight_tokens} tokens):\n{tight}")
    # The tight summary should be shorter than the full one
    require(len(tight) < len(summary), "tight summary should be shorter than full")

    # --- Test 4: inject_context produces both sections ---
    print("\n=== Test 4: inject_context ===")
    ctx = inject_context(
        ip, "agent-0", "smartlist/project/alpha",
        sibling_paths=["smartlist/project/beta"],
        backend_root=br,
    )
    require("Local Neighborhood" in ctx, "missing local section")
    require("LOD summary" in ctx, "missing LOD section")
    require("Siblings" in ctx or "Children" in ctx, "neighborhood should show siblings or children")
    print(f"  context block ({len(ctx)} chars, ~{_estimate_tokens(ctx)} tokens)")

    # --- Test 4b: inject_context with token budget ---
    print("\n=== Test 4b: inject_context with token budget ===")
    ctx_full_tokens = _estimate_tokens(ctx)
    ctx_budgeted = inject_context(
        ip, "agent-0", "smartlist/project/alpha",
        sibling_paths=["smartlist/project/beta"],
        token_budget=ctx_full_tokens + 200,
        backend_root=br,
    )
    require("Local Neighborhood" in ctx_budgeted, "budgeted: missing local section")
    require("LOD summary" in ctx_budgeted, "budgeted: missing LOD section")
    print(f"  large budget: ok ({_estimate_tokens(ctx_budgeted)} tokens)")

    ctx_tight = inject_context(
        ip, "agent-0", "smartlist/project/alpha",
        sibling_paths=["smartlist/project/beta"],
        token_budget=20,
        backend_root=br,
    )
    require("Local Neighborhood" in ctx_tight, "tight: must always include local section")
    print(f"  tight budget: ok ({_estimate_tokens(ctx_tight)} tokens)")

    # --- Test 4c: inject_context skips home_node in siblings ---
    print("\n=== Test 4c: inject_context deduplicates home from siblings ===")
    ctx_dedup = inject_context(
        ip, "agent-0", "smartlist/project/alpha",
        sibling_paths=["smartlist/project/alpha", "smartlist/project/beta"],
        backend_root=br,
    )
    # alpha should not appear in the LOD section since it's the home node
    lod_section = ctx_dedup.split("## Distant Regions")[1] if "## Distant Regions" in ctx_dedup else ""
    require("alpha" not in lod_section, "home node should not appear in LOD section")
    print("  home node correctly excluded from LOD")

    # --- Test 5: empty subtree ---
    print("\n=== Test 5: empty subtree ===")
    empty = summarize_subtree(ip, "smartlist/project/beta/sub1", max_depth=2, backend_root=br)
    # sub1 has no children, so it should just show the node itself
    require(len(empty) > 0, "expected non-empty result for leaf node")
    print(f"  leaf summary: {empty}")

    print("\nresult=ok")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\nFAILED: {e}", file=sys.stderr)
        import traceback; traceback.print_exc()
        sys.exit(1)
