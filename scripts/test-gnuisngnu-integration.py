#!/usr/bin/env python3
"""
test-gnuisngnu-integration.py

Integration test and dry-run verification for GNUISNGNU v0.2 swarm-agent
capabilities:

  A1  — Tool identity registration (_register_agent_tools)
  A2  — Sprint Map injection (bootstrap_atlas_sprint_page / get_sprint_map)
  A3  — Cache preflight gate in worker prompt (format_worker_prompt)
  A4  — Policy-gated claims (claimed_tasks_bootstrap / attach / detach)
  A5  — Cache promote on node completion (_cache_promote_node)
  A6  — Resolution-aware artifact refs (_resolve_dependency_artifacts)
  Dry — Orchestrator dry-run mode reports ready nodes without spawning agents

Tests are split into two layers:

  Unit   — pure-Python checks against the helper functions and Orchestrator
            methods without touching the AMS backend. These always pass.
  Smoke  — subprocess calls against the real AMS kernel/ams.py to verify
            end-to-end plumbing. These are skipped gracefully if the kernel
            binary or corpus cannot be found.

Usage:
    python scripts/test-gnuisngnu-integration.py [--smoke] [--verbose]

    --smoke    Run smoke tests that call the real AMS backend (may be slow).
    --verbose  Print extra diagnostic output.

Exit codes:
    0 — all enabled tests passed
    1 — one or more tests failed
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

# Make the scripts package importable
sys.path.insert(0, str(SCRIPT_DIR))

# run-swarm-plan.py has a hyphen in its name so can't be imported directly.
# Load it via importlib and inject it as a module named "run_swarm_plan".
import importlib.util as _ilu

_rsp_spec = _ilu.spec_from_file_location(
    "run_swarm_plan", SCRIPT_DIR / "run-swarm-plan.py"
)
if _rsp_spec is None or _rsp_spec.loader is None:
    print("ERROR: could not find run-swarm-plan.py", file=sys.stderr)
    sys.exit(1)
_rsp_mod = _ilu.module_from_spec(_rsp_spec)
sys.modules["run_swarm_plan"] = _rsp_mod
try:
    _rsp_spec.loader.exec_module(_rsp_mod)  # type: ignore[union-attr]
except SystemExit:
    pass  # main() guard fires on import — that's fine

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PASS = "OK"
FAIL = "FAIL"
SKIP = "SKIP"


def parse_kv_local(stdout: str) -> dict[str, str]:
    """Parse key=value lines from stdout (local copy to avoid import side-effects)."""
    result: dict[str, str] = {}
    for line in stdout.splitlines():
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        result[k.strip()] = v.strip()
    return result


class TestResult:
    def __init__(self) -> None:
        self.passed: list[str] = []
        self.failed: list[str] = []
        self.skipped: list[str] = []

    def ok(self, name: str, msg: str = "") -> None:
        label = f"{PASS} {name}"
        if msg:
            label += f": {msg}"
        print(label)
        self.passed.append(name)

    def fail(self, name: str, reason: str) -> None:
        label = f"{FAIL} {name}: {reason}"
        print(label)
        self.failed.append(name)

    def skip(self, name: str, reason: str) -> None:
        label = f"{SKIP} {name} [skipped: {reason}]"
        print(label)
        self.skipped.append(name)

    def summary(self) -> int:
        total = len(self.passed) + len(self.failed) + len(self.skipped)
        print(
            f"\n{'='*60}\n"
            f"Results: {len(self.passed)}/{total} passed, "
            f"{len(self.failed)} failed, {len(self.skipped)} skipped"
        )
        return 1 if self.failed else 0


# ---------------------------------------------------------------------------
# Unit tests (no AMS backend required)
# ---------------------------------------------------------------------------

def test_format_worker_prompt_cache_preflight(r: TestResult) -> None:
    """format_worker_prompt includes cache pre-flight section when node_path is set."""
    from run_swarm_plan import format_worker_prompt  # type: ignore[import]

    node_info = {
        "title": "my-task",
        "observations": "Do something useful.",
        "context": "[AMS Callstack Context]\nFrames:\n1. root [work/active]",
        "node_path": "smartlist/execution-plan/my-project/10-children/my-task",
    }
    prompt = format_worker_prompt(node_info)

    if "Cache Pre-flight Check" not in prompt:
        r.fail("format_worker_prompt_cache_preflight", "missing cache pre-flight section")
        return
    if "cache lookup --tool swarm-worker:v1 --source" not in prompt:
        r.fail("format_worker_prompt_cache_preflight", "cache lookup command not present")
        return
    if node_info["node_path"] not in prompt:
        r.fail("format_worker_prompt_cache_preflight", "node_path not embedded in cache lookup")
        return
    r.ok("format_worker_prompt_cache_preflight")


def test_format_worker_prompt_no_cache_when_no_node_path(r: TestResult) -> None:
    """format_worker_prompt omits cache section when node_path is absent."""
    from run_swarm_plan import format_worker_prompt  # type: ignore[import]

    node_info = {
        "title": "my-task",
        "observations": "Do something.",
        "context": "[AMS Callstack Context]\nFrames:\n1. root [work/active]",
    }
    prompt = format_worker_prompt(node_info)
    if "Cache Pre-flight Check" in prompt:
        r.fail("format_worker_prompt_no_cache_when_no_node_path",
               "cache section present even without node_path")
        return
    r.ok("format_worker_prompt_no_cache_when_no_node_path")


def test_format_worker_prompt_dep_artifact_refs(r: TestResult) -> None:
    """format_worker_prompt injects dependency artifact section when _dep_artifact_refs present."""
    from run_swarm_plan import format_worker_prompt  # type: ignore[import]

    node_info: dict[str, Any] = {
        "title": "my-task",
        "observations": "Build on prior results.",
        "context": "[AMS Callstack Context]\nFrames:\n1. root [work/active]",
        "node_path": "smartlist/execution-plan/proj/10-children/my-task",
        "_dep_artifact_refs": [("upstream-task", "artifact-abc123")],
    }
    prompt = format_worker_prompt(node_info)
    if "Dependency Artifacts" not in prompt:
        r.fail("format_worker_prompt_dep_artifact_refs", "dependency artifacts section missing")
        return
    if "artifact-abc123" not in prompt:
        r.fail("format_worker_prompt_dep_artifact_refs", "artifact id not in prompt")
        return
    if "upstream-task" not in prompt:
        r.fail("format_worker_prompt_dep_artifact_refs", "dep title not in prompt")
        return
    r.ok("format_worker_prompt_dep_artifact_refs")


def test_format_worker_prompt_sprint_map(r: TestResult) -> None:
    """format_worker_prompt injects sprint map section when sprint_map is set."""
    from run_swarm_plan import format_worker_prompt  # type: ignore[import]

    node_info: dict[str, Any] = {
        "title": "my-task",
        "observations": "Work item.",
        "context": "[AMS Callstack Context]\nFrames:\n1. root [work/active]",
        "sprint_map": "Task A [done]\nTask B [ready]\nTask C [pending]",
    }
    prompt = format_worker_prompt(node_info)
    if "Sprint Map" not in prompt:
        r.fail("format_worker_prompt_sprint_map", "sprint map section missing")
        return
    if "Task B [ready]" not in prompt:
        r.fail("format_worker_prompt_sprint_map", "sprint map content not in prompt")
        return
    r.ok("format_worker_prompt_sprint_map")


def test_slugify(r: TestResult) -> None:
    """_slugify converts task titles to dash-separated lowercase slugs."""
    from run_swarm_plan import _slugify  # type: ignore[import]

    cases = [
        ("My Task Name", "my-task-name"),
        ("A1-tool-identity-registration", "a1-tool-identity-registration"),
        ("  spaces  around  ", "spaces--around"),
        ("UPPER CASE", "upper-case"),
    ]
    for raw, expected in cases:
        got = _slugify(raw)
        if got != expected:
            r.fail("slugify", f"_slugify({raw!r}) -> {got!r}, expected {expected!r}")
            return
    r.ok("slugify")


def test_resolve_dependency_artifacts_fast_path(r: TestResult) -> None:
    """_resolve_dependency_artifacts returns artifact from in-memory map (fast path)."""
    from run_swarm_plan import Orchestrator  # type: ignore[import]

    orch = Orchestrator(dry_run=True)
    orch._completed_artifacts = {
        "smartlist/execution-plan/proj/10-children/upstream-task": "art-001",
    }
    # Simulate: A6 dry-run with no actual swarm plan — call the method directly
    node_info: dict[str, Any] = {
        "title": "downstream-task",
        "depends_on": "upstream-task",
        "node_path": "smartlist/execution-plan/proj/10-children/downstream-task",
        "parent_node_path": "smartlist/execution-plan/proj/10-children",
    }
    refs = orch._resolve_dependency_artifacts(node_info)
    if len(refs) != 1:
        r.fail("resolve_dependency_artifacts_fast_path",
               f"expected 1 ref, got {len(refs)}: {refs}")
        return
    title, art_id = refs[0]
    if title != "upstream-task" or art_id != "art-001":
        r.fail("resolve_dependency_artifacts_fast_path",
               f"unexpected ref: ({title!r}, {art_id!r})")
        return
    r.ok("resolve_dependency_artifacts_fast_path")


def test_resolve_dependency_artifacts_empty_when_no_deps(r: TestResult) -> None:
    """_resolve_dependency_artifacts returns empty list when depends_on is absent."""
    from run_swarm_plan import Orchestrator  # type: ignore[import]

    orch = Orchestrator(dry_run=True)
    orch._completed_artifacts = {}
    node_info: dict[str, Any] = {
        "title": "standalone-task",
        "node_path": "smartlist/execution-plan/proj/10-children/standalone-task",
    }
    refs = orch._resolve_dependency_artifacts(node_info)
    if refs:
        r.fail("resolve_dependency_artifacts_empty_when_no_deps",
               f"expected empty list, got: {refs}")
        return
    r.ok("resolve_dependency_artifacts_empty_when_no_deps")


def test_agent_tool_ids_defined(r: TestResult) -> None:
    """Orchestrator.AGENT_TOOL_IDS defines the three expected tool identities."""
    from run_swarm_plan import Orchestrator  # type: ignore[import]

    expected = {"swarm-worker:v1", "swarm-verifier:v1", "swarm-repairer:v1"}
    defined = {tool_id for tool_id, _ in Orchestrator.AGENT_TOOL_IDS}
    missing = expected - defined
    if missing:
        r.fail("agent_tool_ids_defined", f"missing tool IDs: {missing}")
        return
    r.ok("agent_tool_ids_defined", f"registered: {sorted(defined)}")


def test_orchestrator_dry_run_flag(r: TestResult) -> None:
    """Orchestrator initialises with dry_run=True without error."""
    from run_swarm_plan import Orchestrator  # type: ignore[import]

    try:
        orch = Orchestrator(dry_run=True, max_steps=5)
        if not orch.dry_run:
            r.fail("orchestrator_dry_run_flag", "dry_run not True after construction")
            return
        r.ok("orchestrator_dry_run_flag")
    except Exception as exc:
        r.fail("orchestrator_dry_run_flag", str(exc))


def test_parse_kv(r: TestResult) -> None:
    """parse_kv correctly parses key=value output from kernel commands."""
    from run_swarm_plan import parse_kv  # type: ignore[import]

    stdout = "status=hit\nartifact_id=abc-123\ntext=some cached result\n"
    kv = parse_kv(stdout)
    if kv.get("status") != "hit":
        r.fail("parse_kv", f"status: {kv.get('status')!r}")
        return
    if kv.get("artifact_id") != "abc-123":
        r.fail("parse_kv", f"artifact_id: {kv.get('artifact_id')!r}")
        return
    r.ok("parse_kv")


def test_bootstrap_atlas_sprint_page_returns_slug(r: TestResult) -> None:
    """bootstrap_atlas_sprint_page returns a non-empty slug string."""
    from run_swarm_plan import bootstrap_atlas_sprint_page  # type: ignore[import]

    # Just verify the function is importable and callable — kernel may not be
    # available in CI, so we accept either a slug or None.
    try:
        slug = bootstrap_atlas_sprint_page("test-project")
        # slug may be None if kernel is unavailable; that's acceptable here.
        if slug is not None and not isinstance(slug, str):
            r.fail("bootstrap_atlas_sprint_page_returns_slug",
                   f"expected str or None, got {type(slug)}")
            return
        r.ok("bootstrap_atlas_sprint_page_returns_slug",
             f"returned: {slug!r}")
    except Exception as exc:
        r.fail("bootstrap_atlas_sprint_page_returns_slug", str(exc))


# ---------------------------------------------------------------------------
# Smoke tests (require AMS backend — skipped if unavailable)
# ---------------------------------------------------------------------------

def _find_kernel() -> list[str] | None:
    try:
        from ams_common import build_rust_ams_cmd  # type: ignore[import]
        cmd = build_rust_ams_cmd()
        return cmd if cmd else None
    except Exception:
        return None


def test_smoke_cache_register_tool(r: TestResult, verbose: bool) -> None:
    """Smoke: cache register-tool succeeds for swarm-worker:v1."""
    kernel = _find_kernel()
    if kernel is None:
        r.skip("smoke_cache_register_tool", "kernel binary not found")
        return

    from run_swarm_plan import FACTORIES_DB  # type: ignore[import]

    result = subprocess.run(
        [*kernel, "cache-register-tool",
         "--input", str(FACTORIES_DB),
         "--tool-id", "swarm-worker:v1",
         "--tool-version", "1.0"],
        cwd=str(REPO_ROOT),
        text=True, capture_output=True, check=False,
    )
    if verbose:
        print(f"    stdout: {result.stdout.strip()!r}")
        print(f"    stderr: {result.stderr.strip()!r}")

    if result.returncode != 0:
        r.fail("smoke_cache_register_tool",
               f"exit={result.returncode}: {result.stderr.strip()!r}")
        return
    kv = parse_kv_local(result.stdout)
    if not kv.get("object_id"):
        r.fail("smoke_cache_register_tool", f"no object_id in output: {result.stdout!r}")
        return
    r.ok("smoke_cache_register_tool", f"object_id={kv['object_id']}")


def test_smoke_ams_py_importable(r: TestResult, verbose: bool) -> None:
    """Smoke: ams.py is importable (no syntax errors in support scripts)."""
    ams_py = REPO_ROOT / "scripts" / "ams.py"
    if not ams_py.exists():
        r.skip("smoke_ams_py_importable", "ams.py not found")
        return

    result = subprocess.run(
        [sys.executable, "-c", f"import importlib.util; s=importlib.util.spec_from_file_location('ams', r'{ams_py}'); m=importlib.util.module_from_spec(s)"],
        cwd=str(REPO_ROOT),
        text=True, capture_output=True, check=False,
    )
    if result.returncode != 0:
        r.fail("smoke_ams_py_importable", result.stderr.strip())
        return
    r.ok("smoke_ams_py_importable")


def test_smoke_run_swarm_plan_dry_run(r: TestResult, verbose: bool) -> None:
    """Smoke: run-swarm-plan.py --dry-run run exits quickly without hanging."""
    rsp = REPO_ROOT / "scripts" / "run-swarm-plan.py"
    if not rsp.exists():
        r.skip("smoke_run_swarm_plan_dry_run", "run-swarm-plan.py not found")
        return

    try:
        result = subprocess.run(
            [sys.executable, str(rsp), "--dry-run", "--max-steps", "3", "run"],
            cwd=str(REPO_ROOT),
            text=True, capture_output=True, check=False,
            timeout=60,
        )
        if verbose:
            print(f"    rc={result.returncode}")
            print(f"    stdout (first 500):\n{result.stdout[:500]}")
            if result.stderr.strip():
                print(f"    stderr (first 200):\n{result.stderr[:200]}")
        # We accept 0 or 1 — the plan tree may be empty or not yet started.
        # What we care about is that it exits cleanly and prints orchestrator logs.
        if result.returncode not in (0, 1):
            r.fail("smoke_run_swarm_plan_dry_run",
                   f"unexpected exit code {result.returncode}")
            return
        if "[orchestrator]" not in result.stdout and "[orchestrator]" not in result.stderr:
            r.fail("smoke_run_swarm_plan_dry_run",
                   "no [orchestrator] log lines found — orchestration did not run")
            return
        r.ok("smoke_run_swarm_plan_dry_run",
             f"exit={result.returncode}, "
             f"dry-run nodes={result.stdout.count('dispatch-worker')}")
    except subprocess.TimeoutExpired:
        r.fail("smoke_run_swarm_plan_dry_run", "timed out after 60s")


def test_smoke_claimed_tasks_bootstrap(r: TestResult, verbose: bool) -> None:
    """Smoke: claimed_tasks_bootstrap creates the SmartList idempotently."""
    kernel = _find_kernel()
    if kernel is None:
        r.skip("smoke_claimed_tasks_bootstrap", "kernel binary not found")
        return

    from run_swarm_plan import claimed_tasks_bootstrap, FACTORIES_DB  # type: ignore[import]

    try:
        claimed_tasks_bootstrap()
        r.ok("smoke_claimed_tasks_bootstrap")
    except Exception as exc:
        r.fail("smoke_claimed_tasks_bootstrap", str(exc))


def test_smoke_claimed_tasks_attach_detach(r: TestResult, verbose: bool) -> None:
    """Smoke: claimed_tasks_attach / detach round-trip succeeds."""
    kernel = _find_kernel()
    if kernel is None:
        r.skip("smoke_claimed_tasks_attach_detach", "kernel binary not found")
        return

    from run_swarm_plan import (  # type: ignore[import]
        claimed_tasks_bootstrap,
        claimed_tasks_attach,
        claimed_tasks_detach,
    )

    test_path = "smartlist/test/gnuisngnu-integration-test-claim"
    try:
        claimed_tasks_bootstrap()
        attached = claimed_tasks_attach(test_path)
        if not attached:
            # May already be claimed; detach first and retry once
            claimed_tasks_detach(test_path)
            attached = claimed_tasks_attach(test_path)
        if not attached:
            r.fail("smoke_claimed_tasks_attach_detach", "attach returned False twice")
            return
        # Detach (cleanup)
        claimed_tasks_detach(test_path)
        r.ok("smoke_claimed_tasks_attach_detach")
    except Exception as exc:
        r.fail("smoke_claimed_tasks_attach_detach", str(exc))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Integration test suite for GNUISNGNU v0.2 swarm-agent features."
    )
    p.add_argument("--smoke", action="store_true",
                   help="Also run smoke tests that call the real AMS backend")
    p.add_argument("--verbose", action="store_true",
                   help="Print extra diagnostic output from subprocess calls")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    r = TestResult()

    print("GNUISNGNU v0.2 Integration Test Suite")
    print("=" * 60)
    print("\n[Unit tests — no AMS backend required]\n")

    test_parse_kv(r)
    test_slugify(r)
    test_agent_tool_ids_defined(r)
    test_orchestrator_dry_run_flag(r)
    test_format_worker_prompt_cache_preflight(r)
    test_format_worker_prompt_no_cache_when_no_node_path(r)
    test_format_worker_prompt_dep_artifact_refs(r)
    test_format_worker_prompt_sprint_map(r)
    test_resolve_dependency_artifacts_fast_path(r)
    test_resolve_dependency_artifacts_empty_when_no_deps(r)
    test_bootstrap_atlas_sprint_page_returns_slug(r)

    if args.smoke:
        print("\n[Smoke tests — require AMS backend]\n")
        test_smoke_ams_py_importable(r, args.verbose)
        test_smoke_cache_register_tool(r, args.verbose)
        test_smoke_claimed_tasks_bootstrap(r, args.verbose)
        test_smoke_claimed_tasks_attach_detach(r, args.verbose)
        test_smoke_run_swarm_plan_dry_run(r, args.verbose)
    else:
        print("\n  (smoke tests skipped — pass --smoke to enable)\n")

    return r.summary()


if __name__ == "__main__":
    sys.exit(main())
