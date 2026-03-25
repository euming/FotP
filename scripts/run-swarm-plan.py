#!/usr/bin/env python3
"""Orchestrate execution of an AMS swarm-plan plan tree using agent swarms.

This script is the orchestrator loop for plan trees built by build-fep-tool-plan.py
(or any other plan builder that uses the AMS swarm-plan). It:

1. Reads the swarm-plan tree and finds all ready nodes (dependencies satisfied)
2. Spawns worker agents in parallel via `claude --team worker`
3. Polls for agent completion, advances completed nodes, dispatches new ready nodes
4. On error (interrupt): spawns a repairer agent, then resumes and continues
5. Repeats until the tree is exhausted

Uses a policy-gated SmartList (claimed-tasks) for concurrency safety across parallel agents.

Usage:
  python scripts/orchestrate-plan.py run [--max-steps N] [--dry-run]
  python scripts/orchestrate-plan.py status
  python scripts/orchestrate-plan.py next
  python scripts/orchestrate-plan.py complete-and-advance --return-text "..."

The orchestrator uses `scripts/ams.bat swarm-plan` commands and relies on the
worker.yml and repairer.yml team definitions in .claude/teams/.
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
AMS_BAT = REPO_ROOT / "scripts" / "ams.bat"
FACTORIES_DB = REPO_ROOT / "shared-memory" / "system-memory" / "factories" / "factories.memory.jsonl"
# Compiled snapshot — used as migration source (the .jsonl seed is not a valid snapshot)
FACTORIES_SNAPSHOT = REPO_ROOT / "shared-memory" / "system-memory" / "factories" / "factories.memory.ams.json"

# Module-level resolved plan store path — set once at startup by resolve_plan_db().
_RESOLVED_PLAN_DB: "str | None" = None
# Module-level plan name — set alongside _RESOLVED_PLAN_DB so run_ams_swarm_plan()
# can inject --project <name> into every ams.py swarm-plan call.
_CURRENT_PLAN_NAME: "str | None" = None

sys.path.insert(0, str(SCRIPT_DIR))
from swarm.pool import AgentPool
from swarm.messaging import (
    bootstrap_message_queue, send_message, send_to_inbox, broadcast, read_inbox,
    bootstrap_system_channel, system_broadcast, fire_triggers,
)
from swarm.dashboard_rich import RichDashboard as Dashboard, AgentStatus, StreamParser, Phase

SYSTEM_CHANNEL = "system"

# Default model assignments per agent team.  Override at runtime with
# --team-model worker=claude-haiku-4-5-20251001  (repeatable flag).
DEFAULT_TEAM_MODELS: dict[str, str] = {
    # Orchestrator itself is the current process — not spawned.
    "worker":             "claude-sonnet-4-6",
    "verifier":           "claude-haiku-4-5-20251001",
    "repairer":           "claude-sonnet-4-6",
    "bug-reporter":       "claude-haiku-4-5-20251001",
    "bugreport-repairer": "claude-sonnet-4-6",
    "decomposer":         "claude-sonnet-4-6",
    "planner":            "claude-sonnet-4-6",
    "reviewer":           "claude-sonnet-4-6",
}

# Default agent driver per team.  "claude" = claude CLI; "codex" = codex CLI.
# Override at runtime with --agent-driver worker=codex  (repeatable flag).
DEFAULT_TEAM_DRIVERS: dict[str, str] = {
    "worker":             "claude",
    "verifier":           "claude",
    "repairer":           "claude",
    "bug-reporter":       "claude",
    "bugreport-repairer": "claude",
    "decomposer":         "claude",
    "planner":            "claude",
    "reviewer":           "claude",
}

# Default effort levels per agent team (low | medium | high | max).
# Override at runtime with --team-effort worker=low  (repeatable flag).
# Effort rationale:
#   low    — tight local instructions, no ambiguity (most workers)
#   medium — some judgement needed but scope is clear
#   high   — open-ended reasoning, planning, or diagnosis
DEFAULT_TEAM_EFFORTS: dict[str, str] = {
    "worker":             "low",
    "verifier":           "low",
    "repairer":           "medium",
    "bug-reporter":       "low",
    "bugreport-repairer": "medium",
    "decomposer":         "high",
    "planner":            "high",
    "reviewer":           "low",
}

# Permission mode per team.  Analysis-only roles (decomposer, planner, verifier,
# reviewer) get "plan" mode — they can read code and run tests but cannot edit files.
# Code-writing roles (worker, repairer) get "bypassPermissions" for full write access.
DEFAULT_TEAM_PERMISSIONS: dict[str, str] = {
    "worker":             "bypassPermissions",
    "verifier":           "plan",
    "repairer":           "bypassPermissions",
    "bug-reporter":       "plan",
    "bugreport-repairer": "bypassPermissions",
    "decomposer":         "plan",
    "planner":            "plan",
    "reviewer":           "plan",
}


class ModeGateError(RuntimeError):
    """Raised when a swarm-plan command is blocked by a plan_mode gate."""
    def __init__(self, command: str, required_mode: str, actual_mode: str):
        self.command = command
        self.required_mode = required_mode
        self.actual_mode = actual_mode
        super().__init__(
            f"[orchestrator] ERROR: Plan is in plan_mode={actual_mode}. "
            f"Command '{command}' requires plan_mode={required_mode}. "
            f"Run `ams.bat swarm-plan enter-{required_mode}` first."
        )


def _check_mode_gate(result: "subprocess.CompletedProcess[str]", cmd: str) -> None:
    """Raise ModeGateError if the result stderr contains a MODE_GATE error."""
    if result.returncode != 0 and "MODE_GATE" in result.stderr:
        m_required = re.search(r"requires plan_mode=(\w+)", result.stderr)
        m_actual = re.search(r"is in plan_mode=(\w+)", result.stderr)
        required = m_required.group(1) if m_required else "unknown"
        actual = m_actual.group(1) if m_actual else "unknown"
        raise ModeGateError(cmd, required, actual)


def run_ams(*args: str) -> subprocess.CompletedProcess[str]:
    """Run an ams.bat command and return the result."""
    cmd = [sys.executable, str(REPO_ROOT / "scripts" / "ams.py"), *args]
    return subprocess.run(
        cmd, cwd=str(REPO_ROOT), text=True, capture_output=True, check=False,
        encoding="utf-8", errors="replace",
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )


def run_ams_swarm_plan(*subargs: str) -> subprocess.CompletedProcess[str]:
    """Run `ams.py swarm-plan <subargs> [--project <name>]`, injecting the resolved
    project name so ams.py always targets the correct per-plan store instead of
    relying on global active-plan detection (which can be wrong when multiple
    per-plan stores each carry their own active-state marker).

    NOTE: --project must come AFTER the subcommand name so argparse routes it to
    the subcommand's own namespace (outer-level --project is ignored by Rust dispatch).
    """
    plan = _CURRENT_PLAN_NAME
    if plan and subargs:
        # Insert --project <name> after the first positional (subcommand name)
        subcmd = subargs[0]
        rest = subargs[1:]
        return run_ams("swarm-plan", subcmd, "--project", plan, *rest)
    return run_ams("swarm-plan", *subargs)


def run_kernel(*args: str) -> subprocess.CompletedProcess[str]:
    """Run an ams-core-kernel command directly (bypassing ams.py)."""
    from ams_common import build_rust_ams_cmd
    cmd = build_rust_ams_cmd(*args)
    if cmd is None:
        # Return a fake failed result if binary not found
        return subprocess.CompletedProcess(args=list(args), returncode=1, stdout="", stderr="ams-core-kernel not found")
    return subprocess.run(
        cmd, cwd=str(REPO_ROOT), text=True, capture_output=True, check=False,
        encoding="utf-8", errors="replace",
    )


def _per_plan_store_has_roots(db_path: str) -> bool:
    """Return True if a per-plan store has recognizable execution-plan roots."""
    result = run_kernel("swarm-plan-list", "--input", db_path)
    return bool(result.stdout.strip()) and "(no execution plan roots found)" not in result.stdout


def _auto_migrate_plan(plan_name: str, dest_path: str) -> bool:
    """Attempt to migrate plan_name from factories into dest_path.

    Returns True on success (per-plan store now has valid roots), False otherwise.
    Failures are logged as warnings and never abort orchestration.
    """
    # Prefer the JSONL write log as migration source: it has all writes including those
    # made after the last snapshot compilation. The Rust kernel compiles it on-the-fly
    # via resolve_authoritative_snapshot_input, which handles both .jsonl and .ams.json.
    # Fall back to the snapshot only if the JSONL is missing.
    factories_src = str(FACTORIES_DB) if FACTORIES_DB.exists() else str(FACTORIES_SNAPSHOT)
    if not Path(factories_src).exists():
        print(f"[orchestrator] WARNING: factories store not found; cannot auto-migrate {plan_name!r}", file=sys.stderr)
        return False
    print(f"[orchestrator] per-plan store for {plan_name!r} not found — auto-migrating from factories...")
    result = run_kernel(
        "swarm-plan-migrate",
        "--from", factories_src,
        "--to", dest_path,
        "--project", plan_name,
    )
    if result.returncode != 0:
        print(f"[orchestrator] WARNING: auto-migration of {plan_name!r} failed: {result.stderr.strip()!r}", file=sys.stderr)
        return False
    if _per_plan_store_has_roots(dest_path):
        print(f"[orchestrator] auto-migration of {plan_name!r} succeeded: {dest_path}")
        return True
    print(f"[orchestrator] WARNING: auto-migration of {plan_name!r} wrote store but verification failed", file=sys.stderr)
    return False


def resolve_plan_db(plan_name: "str | None" = None) -> str:
    """Return the plan store path to use for this orchestrator session.

    ARCHITECTURE NOTE: FACTORIES_DB is for SmartList templates only (structural
    blueprints, like C++ class definitions). It must NEVER be used as a fallback
    store for execution plans. Plans belong exclusively in per-plan stores under
    shared-memory/system-memory/swarm-plans/<plan>.memory.jsonl.

    Resolution order:
    1. If plan_name is explicitly provided, use that plan's per-plan store.
       Verify it has valid roots; if not, error — do NOT fall back to factories.
    2. If the module-level _RESOLVED_PLAN_DB has already been set (set at
       orchestrator startup), return it directly.
    3. Otherwise auto-detect the active plan via active_swarm_plan_name(),
       verify the per-plan store has roots, error if not found.
    4. If no active plan is found, raise — do NOT fall back to factories.
    """
    global _RESOLVED_PLAN_DB, _CURRENT_PLAN_NAME
    if plan_name:
        from ams_common import swarm_plan_db_path, push_plan_stack
        path = swarm_plan_db_path(plan_name)
        if _per_plan_store_has_roots(path):
            _RESOLVED_PLAN_DB = path
            _CURRENT_PLAN_NAME = plan_name
            # Ensure the plan is active in its own store so ready-nodes works.
            run_kernel("swarm-plan-switch", "--input", path, "--project", plan_name,
                       "--actor-id", "orchestrator")
            push_plan_stack(plan_name)
            return path
        # Per-plan store missing or empty — try auto-migration from factories snapshot.
        # This is a one-time bootstrap only; after migration the per-plan store is authoritative.
        if _auto_migrate_plan(plan_name, path):
            _RESOLVED_PLAN_DB = path
            _CURRENT_PLAN_NAME = plan_name
            # Activate the plan in its freshly-migrated store.
            run_kernel("swarm-plan-switch", "--input", path, "--project", plan_name,
                       "--actor-id", "orchestrator")
            push_plan_stack(plan_name)
            return path
        # Migration failed — the plan data is not in a per-plan store.
        # Do NOT fall back to FACTORIES_DB: factories is for templates, not plan state.
        raise RuntimeError(
            f"[orchestrator] ERROR: Per-plan store for {plan_name!r} not found and migration failed. "
            f"Run `ams.bat swarm-plan load-plan --file <plan.json>` to create the per-plan store. "
            f"Do NOT use FACTORIES_DB as a fallback — it is for SmartList templates only."
        )
    if _RESOLVED_PLAN_DB is not None:
        return _RESOLVED_PLAN_DB
    from ams_common import active_swarm_plan_name, swarm_plan_db_path
    active = active_swarm_plan_name()
    if active:
        path = swarm_plan_db_path(active)
        if _per_plan_store_has_roots(path):
            _RESOLVED_PLAN_DB = path
            _CURRENT_PLAN_NAME = active
            return path
        # Per-plan store missing or empty — try auto-migration
        if _auto_migrate_plan(active, path):
            _RESOLVED_PLAN_DB = path
            _CURRENT_PLAN_NAME = active
            return path
        # Migration failed — do NOT fall back to factories
        raise RuntimeError(
            f"[orchestrator] ERROR: Per-plan store for active plan {active!r} not found and migration failed. "
            f"Do NOT use FACTORIES_DB as a fallback — it is for SmartList templates only."
        )
    raise RuntimeError(
        "[orchestrator] ERROR: No active swarm-plan found. "
        "Run `ams.bat swarm-plan load-plan --file <plan.json>` to load a plan, "
        "then `python scripts/orchestrate-plan.py run --plan <name>`."
    )


CLAIMED_TASKS_PATH = "smartlist/swarm/claimed-tasks"


# ---------------------------------------------------------------------------
# Orchestrator Self-Healing — Repair Log + Pre-flight Checks (orchestrator-self-healing)
# ---------------------------------------------------------------------------

class RepairLog:
    """Persistent repair log stored as JSONL in the per-plan store directory.

    Each entry records the timestamp, failure type, fix applied, node affected,
    and outcome (resolved / escalated / failed).  The log is appended-only and
    survives orchestrator restarts so recurring failure patterns are visible
    across sessions.
    """

    def __init__(self, plan_db_path: str) -> None:
        plan_dir = Path(plan_db_path).parent
        self._path = plan_dir / "repair-log.jsonl"

    @property
    def path(self) -> Path:
        return self._path

    def write(
        self,
        *,
        failure_type: str,
        fix_applied: str,
        node_affected: str,
        outcome: str,
    ) -> None:
        """Append one repair event to the log (best-effort; failures are silent)."""
        entry = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "failure_type": failure_type,
            "fix_applied": fix_applied,
            "node_affected": node_affected,
            "outcome": outcome,
        }
        try:
            with open(self._path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry) + "\n")
        except Exception:
            pass  # repair log is advisory; never abort orchestration on write error

    def recent(self, n: int = 10) -> list[dict]:
        """Return the *n* most recent repair log entries (oldest first)."""
        if not self._path.exists():
            return []
        try:
            lines = self._path.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:
            return []
        entries: list[dict] = []
        for line in lines[-n:]:
            try:
                entries.append(json.loads(line))
            except Exception:
                pass
        return entries

    def summary(self) -> str:
        """One-line summary of recent repair events for TUI display."""
        recent = self.recent(5)
        if not recent:
            return "repair-log: empty"
        counts: dict[str, int] = {}
        for e in recent:
            counts[e.get("outcome", "?")] = counts.get(e.get("outcome", "?"), 0) + 1
        detail = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        return f"repair-log: {len(recent)} recent entries ({detail})"


def _is_plan_db_stale(db_path: str, max_age_seconds: float = 3600.0) -> bool:
    """Return True if the per-plan JSONL is missing or its mtime is suspiciously old.

    A store that hasn't been written to in over *max_age_seconds* may be from a
    prior run whose writes were lost — flag it so the caller can decide whether
    to re-seed.
    """
    p = Path(db_path)
    if not p.exists():
        return True
    try:
        age = time.time() - p.stat().st_mtime
        return age > max_age_seconds
    except Exception:
        return True


def _diagnose_zero_steps(plan_db_path: str, ready_nodes: list[dict]) -> dict:
    """Diagnose why the orchestrator executed 0 steps and emit a structured report.

    Checks four known failure modes in order:
    1. ghost_node    — cursor points to a node that no longer exists in the store
    2. stale_store   — per-plan JSONL is missing or has not been written recently
    3. false_cache   — a cache entry prevents dispatch of a ready node
    4. missing_jsonl — per-plan store file does not exist at all

    Returns a diagnosis dict::

        {
          "failure_mode": "<ghost_node|stale_store|false_cache|missing_jsonl|none>",
          "details": "<human-readable explanation>",
          "affected_nodes": ["node-path", ...],
        }

    The dict is also emitted as JSON to stderr so CI logs capture it.
    """
    report: dict = {"failure_mode": "none", "details": "", "affected_nodes": []}

    # 1. missing_jsonl (fastest check)
    if not Path(plan_db_path).exists():
        report["failure_mode"] = "missing_jsonl"
        report["details"] = f"Per-plan store not found: {plan_db_path}"
        _emit_diagnosis(report)
        return report

    # 2. stale_store
    if _is_plan_db_stale(plan_db_path, max_age_seconds=7200.0):
        report["failure_mode"] = "stale_store"
        p = Path(plan_db_path)
        try:
            age_min = (time.time() - p.stat().st_mtime) / 60.0
            report["details"] = f"Per-plan store last modified {age_min:.1f} min ago (threshold 120 min): {plan_db_path}"
        except Exception as exc:
            report["details"] = f"Could not stat per-plan store ({exc}): {plan_db_path}"
        _emit_diagnosis(report)
        return report

    # 3. ghost_node — check via swarm-plan context
    ctx_result = run_ams_swarm_plan("context")
    if ctx_result.returncode == 0:
        ctx_text = ctx_result.stdout or ""
        active_path_line = next(
            (l for l in ctx_text.splitlines() if l.strip().startswith("active_node_path=")),
            None,
        )
        if active_path_line:
            active_path = active_path_line.split("=", 1)[1].strip()
            # Verify this path still exists by checking ready-nodes
            ready_paths = {n.get("node_path", "") for n in ready_nodes}
            if active_path and active_path not in ready_paths:
                # Check if it matches any known node in the store
                list_result = run_kernel("swarm-plan-list", "--input", plan_db_path)
                if active_path not in (list_result.stdout or ""):
                    report["failure_mode"] = "ghost_node"
                    report["details"] = (
                        f"Active node path {active_path!r} not found in store. "
                        f"Ghost node is blocking cursor from advancing."
                    )
                    report["affected_nodes"] = [active_path]
                    _emit_diagnosis(report)
                    return report

    # 4. false_cache — check cache entries for ready nodes vs store mtime
    store_mtime = 0.0
    try:
        store_mtime = Path(plan_db_path).stat().st_mtime
    except Exception:
        pass

    for node in ready_nodes:
        node_path = node.get("node_path", "")
        if not node_path:
            continue
        lookup = run_kernel(
            "cache-lookup",
            "--input", plan_db_path,
            "--tool-id", "swarm-worker:v1",
            "--source-id", node_path,
        )
        if lookup.returncode == 0 and "status=hit" in (lookup.stdout or ""):
            # Cache hit on a ready (incomplete) node — likely a stale/false hit
            report["failure_mode"] = "false_cache"
            report["details"] = (
                f"Ready node {node_path!r} has a cache hit but is not marked complete. "
                f"False cache hit may be preventing worker dispatch."
            )
            report["affected_nodes"].append(node_path)
            # Report first hit found; continue checking for more
            break

    if report["failure_mode"] != "none":
        _emit_diagnosis(report)
        return report

    # No known failure mode matched
    report["failure_mode"] = "none"
    report["details"] = "No known failure mode detected — 0 steps may be expected (empty plan or all blocked deps)."
    _emit_diagnosis(report)
    return report


def _emit_diagnosis(report: dict) -> None:
    """Emit the diagnosis report as JSON to stderr."""
    print(
        json.dumps({"orchestrator_diagnosis": report}, indent=2),
        file=sys.stderr,
        flush=True,
    )


def claimed_tasks_bootstrap() -> None:
    """Create the claimed-tasks SmartList with unique_members policy.

    Idempotent — safe to call on every orchestrator startup.
    """
    db = resolve_plan_db()
    # Create the bucket (idempotent)
    run_kernel("smartlist-create", "--input", db, "--path", CLAIMED_TASKS_PATH, "--actor-id", "orchestrator")
    # Set unique_members policy so duplicate attach calls are rejected
    container_id = f"smartlist-members:{CLAIMED_TASKS_PATH}"
    run_kernel(
        "policy-set", "--input", db,
        "--container-id", container_id,
        "--field", "unique_members",
        "--value", "true",
        "--actor-id", "orchestrator",
    )


def claimed_tasks_clear() -> None:
    """Detach all stale claimed-task entries at orchestrator startup.

    When an orchestrator exits (idle-break, crash, SIGKILL) without cleaning
    up, its claimed nodes remain in the SmartList and block the next run from
    dispatching those same nodes.  Calling this once at startup releases all
    stale claims since no agents are running yet.
    """
    ready = get_ready_nodes()
    for node in ready:
        node_path = node.get("node_path", "")
        if node_path:
            claimed_tasks_detach(node_path)


def claimed_tasks_attach(node_path: str) -> bool:
    """Attach node_path to the claimed-tasks list.

    Returns True on success. Returns False if the policy rejected the attach
    (duplicate member — another orchestrator already claimed this node).
    """
    db = resolve_plan_db()
    result = run_kernel(
        "smartlist-attach", "--input", db,
        "--path", CLAIMED_TASKS_PATH,
        "--member-ref", node_path,
        "--actor-id", "orchestrator",
    )
    return result.returncode == 0


def claimed_tasks_detach(node_path: str) -> None:
    """Detach node_path from the claimed-tasks list (best-effort)."""
    db = resolve_plan_db()
    run_kernel(
        "smartlist-detach", "--input", db,
        "--path", CLAIMED_TASKS_PATH,
        "--member-ref", node_path,
        "--actor-id", "orchestrator",
    )


def parse_kv(stdout: str) -> dict[str, str]:
    """Parse key=value lines from swarm-plan command output."""
    values: dict[str, str] = {}
    for line in stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def parse_context_field(context: str, field: str) -> str | None:
    """Extract a key=value field from callstack context output."""
    for line in context.splitlines():
        line = line.strip()
        if line.startswith(f"{field}="):
            return line.split("=", 1)[1].strip()
    return None


def get_context() -> str | None:
    """Return the current swarm-plan context text, or None if empty."""
    result = run_ams_swarm_plan("context")
    if result.returncode != 0:
        return None
    text = (result.stdout or "").strip()
    if not text or "[AMS Callstack Context]" not in text:
        return None
    return text


def get_active_node_info() -> dict[str, str] | None:
    """Get the active node path and observations from context."""
    context = get_context()
    if context is None:
        return None

    info: dict[str, str] = {"context": context}

    # Parse the active frame title from the last numbered frame line
    for line in context.splitlines():
        line = line.strip()
        # Lines like "3. p1b-extend-csharp-ingest [work/active]"
        if line and line[0].isdigit() and "." in line:
            parts = line.split(".", 1)
            if len(parts) == 2:
                title_part = parts[1].strip()
                bracket_idx = title_part.rfind("[")
                if bracket_idx > 0:
                    info["title"] = title_part[:bracket_idx].strip()

    # Extract observations
    observations: list[str] = []
    in_obs = False
    for line in context.splitlines():
        if "Active observations:" in line:
            in_obs = True
            continue
        if in_obs:
            if line.startswith("- "):
                observations.append(line[2:])
            elif line.startswith("---") or line.startswith("[End"):
                break
    if observations:
        info["observations"] = "\n".join(observations)

    # Parse node metadata (has_children, node_kind, active_node_kind, policy_kind)
    for line in context.splitlines():
        stripped = line.strip()
        if stripped.startswith("has_children="):
            info["has_children"] = stripped.split("=", 1)[1]
        elif stripped.startswith("node_kind="):
            info["node_kind"] = stripped.split("=", 1)[1]
        elif stripped.startswith("active_node_kind="):
            info["active_node_kind"] = stripped.split("=", 1)[1]
        elif stripped.startswith("policy_kind="):
            info["policy_kind"] = stripped.split("=", 1)[1]

    return info


def pop_node(return_text: str, actor_id: str = "orchestrator") -> dict[str, str]:
    """Pop the active node with a return summary."""
    result = run_ams_swarm_plan("pop", "--return-text", return_text, "--actor-id", actor_id)
    if result.returncode != 0:
        raise RuntimeError(f"swarm-plan pop failed: {result.stderr}")
    return parse_kv(result.stdout)


def advance(actor_id: str = "orchestrator") -> dict[str, str]:
    """Advance the cursor to the next ready node."""
    result = run_ams_swarm_plan("advance", "--actor-id", actor_id)
    if result.returncode != 0:
        raise RuntimeError(f"swarm-plan advance failed: {result.stderr}")
    return parse_kv(result.stdout)


def interrupt_for_repair(
    reason: str,
    error_output: str,
    context: str,
    attempted_fix: str,
    repair_hint: str,
    actor_id: str = "orchestrator",
) -> dict[str, str]:
    """Interrupt the active node and create a repair policy node."""
    result = run_ams_swarm_plan(
        "interrupt",
        "--policy", "repair",
        "--reason", reason,
        "--error-output", error_output,
        "--context", context,
        "--attempted-fix", attempted_fix,
        "--repair-hint", repair_hint,
        "--actor-id", actor_id,
    )
    if result.returncode != 0:
        raise RuntimeError(f"swarm-plan interrupt failed: {result.stderr}")
    return parse_kv(result.stdout)


def resume_after_repair(actor_id: str = "orchestrator") -> dict[str, str]:
    """Resume the interrupted node after repair completes."""
    result = run_ams_swarm_plan("resume", "--actor-id", actor_id)
    if result.returncode != 0:
        raise RuntimeError(f"swarm-plan resume failed: {result.stderr}")
    return parse_kv(result.stdout)


def interrupt_for_decompose_lookahead(node_path: str) -> bool:
    """Claim a ready node for look-ahead decomposition, blocking worker dispatch.

    Uses the same claimed-tasks primitive that prevents double-dispatch of workers,
    mirroring how interrupt_for_repair() pauses execution of the active line.
    Returns True if the claim was acquired; False if already claimed.
    """
    return claimed_tasks_attach(node_path)


def release_decompose_lookahead(node_path: str) -> None:
    """Release a look-ahead decomposition claim, allowing normal dispatch to proceed."""
    claimed_tasks_detach(node_path)


def pause_node(
    reason: str,
    error_output: str = "",
    context: str = "",
    actor_id: str = "orchestrator",
) -> dict[str, str]:
    """Pause the active execution line by inserting an interrupt with 'pause' policy.

    The node stays paused until a bug-report or repair agent resumes it.
    This is called by the orchestrator when it detects subagent failure,
    shifting detection responsibility from the worker to the parent.
    """
    result = run_ams(
        "swarm-plan", "interrupt",
        "--policy", "pause",
        "--reason", reason,
        "--error-output", error_output,
        "--context", context,
        "--attempted-fix", "",
        "--repair-hint", "Orchestrator-detected failure. Inspect error output and agent logs.",
        "--actor-id", actor_id,
    )
    if result.returncode != 0:
        raise RuntimeError(f"swarm-plan pause (interrupt --policy pause) failed: {result.stderr}")
    return parse_kv(result.stdout)


def get_ready_nodes() -> list[dict[str, str]]:
    """Get all nodes whose dependencies are satisfied and are ready for dispatch."""
    result = run_ams_swarm_plan("ready-nodes")
    if result.returncode != 0:
        return []
    nodes: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if line == "---":
            if current:
                nodes.append(current)
                current = {}
            continue
        if line.startswith("node_path="):
            current["node_path"] = line.split("=", 1)[1]
        elif line.startswith("title="):
            current["title"] = line.split("=", 1)[1]
        elif line.startswith("depends_on="):
            current["depends_on"] = line.split("=", 1)[1]
        elif line.startswith("parent_node_path="):
            current["parent_node_path"] = line.split("=", 1)[1]
        elif line.startswith("may_decompose="):
            current["may_decompose"] = line.split("=", 1)[1]
        elif line.startswith("role="):
            current["role"] = line.split("=", 1)[1]
        elif line.startswith("observations="):
            current["observations_count"] = line.split("=", 1)[1]
        elif line.startswith("  ") and current:
            current.setdefault("observations", "")
            current["observations"] += line.strip() + "\n"
    if current:
        nodes.append(current)
    return nodes


def complete_specific_node(node_path: str, return_text: str, actor_id: str = "orchestrator") -> dict[str, str]:
    """Complete a specific node by path (for parallel dispatch)."""
    result = run_ams_swarm_plan("complete-node", "--node-path", node_path, "--return-text", return_text, "--actor-id", actor_id)
    if result.returncode != 0:
        raise RuntimeError(f"swarm-plan complete-node failed: {result.stderr}")
    return parse_kv(result.stdout)


def get_node_context(node_path: str) -> dict[str, str]:
    """Build a node_info dict for a specific ready node (for spawning workers)."""
    # Get the observations for this specific node by reading its context
    result = run_ams_swarm_plan("ready-nodes")
    if result.returncode != 0:
        return {"title": node_path.rsplit("/", 1)[-1], "node_path": node_path}
    # Parse all ready nodes and find the matching one
    nodes = get_ready_nodes()
    for node in nodes:
        if node.get("node_path") == node_path:
            # Enrich with context from the main context command
            context = get_context() or ""
            node["context"] = context
            return node
    return {"title": node_path.rsplit("/", 1)[-1], "node_path": node_path, "context": get_context() or ""}


def observe(title: str, text: str, actor_id: str = "orchestrator") -> None:
    """Write an observation on the active node."""
    run_ams_swarm_plan("observe", "--title", title, "--text", text, "--actor-id", actor_id)


def batch_ops(ops: list[dict], actor_id: str = "orchestrator") -> list[dict]:
    """Execute a batch of swarm-plan operations with a single lock acquisition.

    Each op is a dict with a ``cmd`` key (push, pop, observe, complete-node, advance,
    resume, switch, park, interrupt) plus operation-specific fields.

    Falls back to executing ops individually if the Rust batch command is unavailable.
    """
    from ams import _run_rust_batch
    from ams_common import corpus_db

    # Use the per-plan store when one has been resolved (swarm-plan operations must
    # target the same store that get_ready_nodes() reads from, otherwise complete-node
    # writes to one DB while ready-nodes reads from another and the completion is lost).
    db_path = _RESOLVED_PLAN_DB or corpus_db("all")
    try:
        results = _run_rust_batch(db_path, ops, actor_id=actor_id)
        if results is not None:
            return results
    except ModeGateError:
        raise  # propagate mode gate errors — do not fall through
    except Exception:
        pass  # fall through to individual execution

    # Fallback: execute each op individually via ams.bat
    results = []
    for op in ops:
        cmd = op.get("cmd", "")
        if cmd == "observe":
            observe(op["title"], op["text"], actor_id)
            results.append({"action": "observe"})
        elif cmd == "complete-node":
            r = complete_specific_node(op["node_path"], op.get("return_text", ""), actor_id)
            results.append(r)
        elif cmd == "pop":
            r = run_ams_swarm_plan("pop", "--actor-id", actor_id)
            _check_mode_gate(r, "pop")
            results.append(parse_kv(r.stdout) if r.returncode == 0 else {"action": "error"})
        elif cmd == "advance":
            r = advance(actor_id)
            results.append(r)
        else:
            # For other ops, shell out individually
            args = [cmd]
            for k, v in op.items():
                if k == "cmd":
                    continue
                args += [f"--{k.replace('_', '-')}", str(v)]
            args += ["--actor-id", actor_id]
            r = run_ams_swarm_plan(*args)
            _check_mode_gate(r, cmd)
            results.append(parse_kv(r.stdout) if r.returncode == 0 else {"action": "error"})
    return results


def write_note_to_bucket(bucket: str, title: str, text: str, actor_id: str = "orchestrator") -> None:
    """Write a SmartList note to an arbitrary bucket path."""
    result = run_ams_swarm_plan("write-note", "--bucket", bucket, "--title", title, "--text", text, "--actor-id", actor_id)
    if result.returncode != 0:
        raise RuntimeError(f"write-note failed: {result.stderr}")



def _slugify(name: str) -> str:
    """Convert a name to a safe atlas slug (lowercase, hyphens, no spaces)."""
    return re.sub(r"[^a-z0-9\-]", "-", name.lower()).strip("-")


def bootstrap_atlas_sprint_page(project_name: str) -> str | None:
    """Register a named Atlas for the current sprint project and return its slug.

    Defines the Atlas over the ``smartlist/execution-plan/<project>`` bucket
    using two scale levels:
      - Scale 0 (coarsest): the execution-plan root — one-line sprint summary
      - Scale 1 (fine):     individual task nodes inside the plan

    If the Atlas already exists the define call is idempotent (Rust returns
    the existing record).  Returns the atlas slug on success, None on failure.
    """
    slug = _slugify(project_name)
    plan_bucket = f"smartlist/execution-plan/{project_name}"
    result = run_ams(
        "atlas", "define", slug,
        "--description", f"Sprint atlas for {project_name}",
        "--scale", f"0:{plan_bucket}",
        "--scale", f"1:{plan_bucket}/*",
    )
    if result.returncode != 0:
        return None
    return slug


def get_sprint_map(atlas_slug: str, scale: int = 0) -> str:
    """Retrieve the coarse-scale atlas map for a sprint.

    Calls ``atlas list-at-scale <slug> <scale>`` and returns the stdout
    summary text.  Returns an empty string on failure.
    """
    result = run_ams("atlas", "list-at-scale", atlas_slug, str(scale))
    if result.returncode != 0:
        return ""
    return (result.stdout or "").strip()


def format_worker_prompt(node_info: dict[str, str]) -> str:
    """Build the prompt for a worker agent from node context."""
    title = node_info.get("title", "unknown-task")
    observations = node_info.get("observations", "No specific instructions.")
    context = node_info.get("context", "")
    sprint_map = node_info.get("sprint_map", "")
    node_path = node_info.get("node_path", "")
    parent_path = node_info.get("parent_node_path", "")
    plan_name = _CURRENT_PLAN_NAME or ""

    sprint_map_section = ""
    if sprint_map:
        sprint_map_section = f"""
## Sprint Map (coarse-scale atlas view)
{sprint_map}
"""

    cache_preflight_section = ""
    if node_path:
        cache_preflight_section = f"""
## Cache Pre-flight Check (GNUISNGNU v0.2)

Before doing any implementation work, check whether this node's result is already
cached from a previous run:

```
scripts\\ams.bat cache lookup --tool swarm-worker:v1 --source {node_path}
```

- If the command returns `status=hit` with a `text=` payload, **return that cached
  result directly** without re-implementing anything. Print `CACHE_HIT` on its own
  line followed by the cached text as your final output and stop.
- If the command returns `status=miss`, `status=stale`, or fails, proceed with
  normal implementation below.

This check prevents redundant work when swarm plans are re-run after a failure or
when multiple sprints touch the same nodes.

The orchestrator automatically emits FEP cache-signal telemetry (P7-C2):
- A `hit` signal is emitted when `CACHE_HIT` is detected in your output.
- A `miss` signal is emitted when you complete normally (non-cache path).
These signals feed the FEP dream-schedule surprise scoring so high-miss clusters
are prioritised for re-dreaming.
"""

    # A6: inject resolved dependency artifact refs so the worker can reference
    # outputs from completed upstream nodes without re-running them.
    dep_artifact_refs: list[tuple[str, str]] = node_info.get("_dep_artifact_refs", [])  # type: ignore[assignment]
    dep_artifacts_section = ""
    if dep_artifact_refs:
        lines = "\n".join(
            f"  - **{dep_title}**: artifact `{art_id}`"
            for dep_title, art_id in dep_artifact_refs
        )
        dep_artifacts_section = f"""
## Dependency Artifacts (GNUISNGNU v0.2 — resolution-aware refs)

The following upstream tasks have completed and their results are cached as
artifacts.  Reference these IDs when your work builds on prior node output:

{lines}

To read an artifact's content:
```
scripts\\ams.bat cache lookup --tool swarm-worker:v1 --source <dep-node-path>
```
"""

    return f"""You are executing a plan node from the AMS swarm-plan.

## Task: {title}

## Implementation Spec
{observations}
{sprint_map_section}{dep_artifacts_section}{cache_preflight_section}
## Full Callstack Context
{context}

## Instructions

1. Read the implementation spec above carefully.
2. Implement the changes described in the spec. Focus on writing code.
3. When done, print a summary of what was implemented as your final output.

CRITICAL RULES:
- Do NOT run `ams.bat` commands EXCEPT the child-done notification below.
- Do NOT run any `swarm-plan` commands (pop, push, advance, context, observe, complete-node).
- The context above is sufficient. Do not try to fetch more context.
- Focus entirely on reading existing code and making the required changes.
- When DONE, notify your parent node with a summary of what you accomplished:
    scripts\\ams.bat swarm-plan --project {plan_name} child-done --parent-path "{parent_path}" --title "{title}" --text "<summary of what you implemented and verified>"
"""


def format_verifier_prompt(node_info: dict[str, str], return_text: str) -> str:
    """Build the prompt for a verifier agent that checks worker output."""
    title = node_info.get("title", "unknown-task")
    observations = node_info.get("observations", "No spec available.")
    return f"""You are verifying whether a worker agent genuinely completed its task.

## Task: {title}

## Task Spec (what was supposed to be done)
{observations}

## Worker Return Text (what the worker claimed)
{return_text}

## Instructions

Check whether the deliverables described in the spec actually exist in the codebase.
Use Glob, Grep, and Read to inspect the actual files and logic. Do NOT modify any files.

**On re-running tests:** If the worker reported passing test output AND the spec's
acceptance criteria involve tests, read the relevant source files to verify the logic
is correct — but do NOT re-run the same test suite unless you see something suspicious
in the code. Re-running a test that just passed on an unchanged codebase wastes time
without adding confidence. Your skepticism should focus on whether the *code is correct*,
not on whether the test runner produces the same output twice in a row.

Output your verdict as the FIRST line:
- VERIFIED: <summary>
- FAKE SUCCESS: <what is missing>
- UNVERIFIABLE: <why>
"""


def format_repairer_prompt(node_info: dict[str, str]) -> str:
    """Build the prompt for a repairer agent from the repair policy context."""
    context = node_info.get("context", "")
    return f"""You are a repair agent. Your execution context is already loaded.

## Current Callstack Context
{context}

## Instructions

Follow the repairer.yml protocol:
1. Read the repair metadata from swarm-plan context (repair_hint, error_output, etc.)
2. Search memory for prior fixes: `scripts\\ams.bat search "<error keywords>"`
3. Diagnose the root cause and record: `scripts\\ams.bat swarm-plan observe --title "diagnosis" --text "<root cause>" --actor-id claude-team-repairer`
4. Apply the fix and record: `scripts\\ams.bat swarm-plan observe --title "fix" --text "<what was changed>" --actor-id claude-team-repairer`
5. Verify the fix works
6. Pop with result: `scripts\\ams.bat swarm-plan pop --return-text "REPAIRED: <summary>" --actor-id claude-team-repairer`

If the fix failed: `scripts\\ams.bat swarm-plan pop --return-text "REPAIR FAILED: <reason>" --actor-id claude-team-repairer`

Do NOT push, resume, or interrupt. You are already on the policy node.
"""


def format_bug_reporter_prompt(node_info: dict[str, str], failed_title: str, stderr_tail: str) -> str:
    """Build the prompt for a bug-reporter agent from pause policy context."""
    context = node_info.get("context", "")
    return f"""You are a bug-report agent. A subagent has failed and the execution line is paused.

## Failed Node: {failed_title}

## Error Output (last 500 chars)
{stderr_tail or "(no stderr captured)"}

## Current Callstack Context
{context}

## Instructions

Follow the bug-reporter.yml protocol:
1. Read the pause metadata from swarm-plan context (reason, error_output, etc.)
2. Search memory for similar past bugs: `scripts\\ams.bat search "<error keywords>"`
3. Check existing bug reports: `dotnet run --project tools/memoryctl -- bugreport-list --db .ams --status open`
4. Diagnose the root cause and record: `scripts\\ams.bat swarm-plan observe --title "diagnosis" --text "<root cause>" --actor-id claude-team-bug-reporter`
5. Create a BugReport Node via memoryctl:
   ```
   dotnet run --project tools/memoryctl -- bugreport-create --db .ams \\
     --source-agent "<failed agent>" --parent-agent "orchestrator" \\
     --error-output "<error text>" --stack-context "<context>" \\
     --severity <critical|high|medium|low> \\
     --recommended-fix-plan "<fix plan>" --durable \\
     --created-by claude-team-bug-reporter
   ```
6. Record the bug_id: `scripts\\ams.bat swarm-plan observe --title "bug-report-created" --text "bug_id=<id>" --actor-id claude-team-bug-reporter`
7. Pop with result: `scripts\\ams.bat swarm-plan pop --return-text "BUG REPORTED: <bug_id> severity=<sev> — <summary>" --actor-id claude-team-bug-reporter`

If the failure is a false positive: `scripts\\ams.bat swarm-plan pop --return-text "BUG SKIPPED: false positive — <reason>" --actor-id claude-team-bug-reporter`

Do NOT push, resume, interrupt, or fix the bug. You only diagnose and report.
"""


def format_decomposer_prompt(node_info: dict[str, str]) -> str:
    """Build the prompt for a decomposer agent.

    Handles three modes from decomposer.yml:
      Mode A — interrupt-response (active policy node, classic flow)
      Mode C — look-ahead (target node identified by lookahead_node_path)
      Analysis-only — role=decomposer nodes dispatched via ready-nodes (lead-engineer pattern)
    Mode B is handled by swarm_plan_new() in ams.py directly.
    """
    context = node_info.get("context", "")
    observations = node_info.get("observations", "").strip()

    # Mode C: look-ahead decomposition — target node is not yet active
    lookahead_path = node_info.get("lookahead_node_path", "")
    if lookahead_path:
        lookahead_title = node_info.get("lookahead_node_title", "")
        lookahead_desc = node_info.get("lookahead_node_description", "")
        is_stub = node_info.get("stub_detected") == "true"

        if is_stub:
            stub_preamble = (
                "NOTE: This node was auto-detected as an architectural stub — it has a "
                "brief title and no recorded observations. It was added as a high-level "
                "placeholder during early planning and has not yet been decomposed into "
                "concrete subtasks. Decomposition is MANDATORY for stub nodes; "
                "NO DECOMPOSE is not an option.\n\n"
                "Use the swarm context (parent node, sibling nodes, project goals) to "
                "infer the intended scope and generate concrete, actionable subtasks.\n"
            )
            step3 = (
                "3. This is a stub — decomposition is mandatory. Write the JSON plan to "
                "scripts/plans/<name>.json, then print exactly:\n"
                "   PLAN FILE: scripts/plans/<filename>.json"
            )
        else:
            stub_preamble = "Your job: decide whether the task needs decomposition, and if so, produce the plan.\n"
            step3 = (
                "3. If the task is simple enough for one worker in one pass, print exactly:\n"
                "   NO DECOMPOSE: <one-line reason>\n"
                "   ...and stop. Do not write any plan file.\n"
                "4. Otherwise: write the JSON plan to scripts/plans/<name>.json, then print exactly:\n"
                "   PLAN FILE: scripts/plans/<filename>.json"
            )

        return f"""You are being invoked in Mode C (look-ahead decomposition).

The orchestrator has speculatively claimed this node before dispatching a worker.
{stub_preamble}
## Target Node

TARGET NODE PATH: {lookahead_path}
TARGET NODE TITLE: {lookahead_title}
TARGET NODE DESCRIPTION:
{lookahead_desc}

## Current Swarm Context
{context}

## Instructions

Follow the Mode C protocol in decomposer.yml:
1. Read the target node description above carefully (do NOT call swarm-plan context — the node is not active yet)
2. Run the 4-step reasoning process (map capabilities, identify gaps, concurrency analysis, synthesize plan)
{step3}

Do NOT call swarm-plan pop, resume, push, or interrupt.
The orchestrator handles all state transitions after reading your stdout.

CHECKPOINTING: After each major analysis step, save your progress:
  scripts\\ams.bat swarm-plan observe --title "checkpoint:<step>" --text "<findings so far>"
This ensures your analysis survives if you are interrupted or time out.
"""

    # Analysis-only decomposer: dispatched via ready-nodes with a node_path
    # (e.g. lead-engineer pattern). These nodes analyse and record observations
    # but do NOT produce a PLAN FILE.
    node_path = node_info.get("node_path", "")
    title = node_info.get("title", "")
    plan_name = _CURRENT_PLAN_NAME or ""
    parent_path = node_info.get("parent_node_path", "")
    if node_path and not node_info.get("lookahead_node_path"):
        return f"""You are an analysis-only decomposer (lead-engineer pattern).

## Task: {title}

## Implementation Spec (includes any prior checkpoints — resume from where they left off)
{observations or "No specific instructions."}

## Full Callstack Context
{context}

## Instructions

1. Analyse the problem described in the spec above.
2. Read source code, run tests, search AMS memory as needed.
3. Record your findings and prescriptions as observations:
     scripts\\ams.bat swarm-plan observe --title "checkpoint:<step>" --text "<findings>"
4. **CHECKPOINT FREQUENTLY** — after each major analysis step, save progress via
   the observe command above. If you are interrupted or time out, the next agent
   will receive your checkpoints and can resume without redoing work.
5. When done, print a summary of your analysis and prescription as your final output.
6. Notify completion:
     scripts\\ams.bat swarm-plan --project {plan_name} child-done --parent-path "{parent_path}" --title "{title}" --text "<summary>"

Do NOT call swarm-plan pop, push, or interrupt.
Do NOT produce a PLAN FILE unless you are explicitly decomposing into subtasks.
"""

    # Mode A: interrupt-response (classic flow — active decompose policy node)
    return f"""You are a decomposer agent (the "beetle"). Your execution context is already loaded.

## Current Callstack Context
{context}

## Instructions

Follow the Mode A protocol in decomposer.yml:
1. Read the decompose metadata from swarm-plan context (reason, subtask_hints, etc.)
2. Search AMS memory for similar past decompositions: `scripts\\ams.bat search "<task keywords>"`
3. Read source code to understand the actual scope of the task
4. Produce a JSON plan file in `scripts/plans/` with concrete subtasks
5. Print exactly: `PLAN FILE: scripts/plans/<filename>.json` to stdout
6. Pop with result: `scripts\\ams.bat swarm-plan pop --return-text "Produced plan: <N> subtasks in scripts/plans/<file>.json" --actor-id claude-team-decomposer`

CHECKPOINTING: After each major analysis step, save progress:
  scripts\\ams.bat swarm-plan observe --title "checkpoint:<step>" --text "<findings so far>"
This ensures your work survives if you are interrupted or time out.

Do NOT push, resume, or interrupt. You are already on the policy node.
"""


def format_bugreport_repairer_prompt(node_info: dict[str, str], bug_id: str) -> str:
    """Build the prompt for a repairer agent that reads a BugReport Node."""
    context = node_info.get("context", "")
    return f"""You are a repair agent. A BugReport Node has been filed for the failure you need to fix.

## BugReport ID: {bug_id}

## Current Callstack Context
{context}

## Instructions

1. Read the BugReport to understand the failure:
   ```
   dotnet run --project tools/memoryctl -- bugreport-show --db .ams --bug-id {bug_id}
   ```
2. The BugReport contains: error_output, stack_context, recommended_fix_plan, severity, and attempted_fixes.
   Follow the recommended_fix_plan as your starting point.
3. Search memory for prior fixes: `scripts\\ams.bat search "<error keywords>"`
4. Update the BugReport status to in-repair:
   ```
   dotnet run --project tools/memoryctl -- bugreport-update-status --db .ams --bug-id {bug_id} --status in-repair
   ```
5. Diagnose and record: `scripts\\ams.bat swarm-plan observe --title "diagnosis" --text "<root cause>" --actor-id claude-team-repairer`
6. Apply the fix and record: `scripts\\ams.bat swarm-plan observe --title "fix" --text "<what was changed>" --actor-id claude-team-repairer`
7. Verify the fix works.
8. On success, update BugReport status to resolved:
   ```
   dotnet run --project tools/memoryctl -- bugreport-update-status --db .ams --bug-id {bug_id} --status resolved
   ```
   Pop with: `scripts\\ams.bat swarm-plan pop --return-text "REPAIRED: {bug_id} — <summary>" --actor-id claude-team-repairer`
9. On failure: `scripts\\ams.bat swarm-plan pop --return-text "REPAIR FAILED: {bug_id} — <reason>" --actor-id claude-team-repairer`

Do NOT push, resume, or interrupt. You are already on the policy node.
"""


class PerformanceTracker:
    """Phase-4 adaptive scheduling: track per-node timing, compute prediction error,
    learn duration abstractions, and recommend adaptive concurrency limits.

    4a — performance tracking: record start/end wall-clock times per node.
    4b — prediction error: compare predicted duration (rolling average) to actual.
    4c — abstraction learning: group nodes by name-prefix cluster; maintain
         per-cluster rolling mean and standard deviation.
    4d — adaptive concurrency: lower the concurrency recommendation when prediction
         error is high (surprises indicate overload or instability).
    """

    # Minimum observations before adaptive concurrency kicks in.
    MIN_SAMPLES_FOR_ADAPTATION = 3
    # Relative prediction-error threshold above which we reduce concurrency.
    # abs(actual - predicted) / predicted > this → scale down.
    # LLM agent tasks have inherently high duration variance (20s–300s for the same
    # logical task type), so a 50% threshold is far too sensitive. Use 2.0 (200%) to
    # only reduce on genuine runaway outliers, not normal LLM variance.
    HIGH_ERROR_THRESHOLD = 2.0
    # Absolute floor and ceiling for recommended concurrency.
    # Floor of 3 ensures independent tasks still run in parallel even after high-error
    # samples. The adaptive system should damp concurrency, not serialize it.
    MIN_CONCURRENCY = 3
    MAX_CONCURRENCY = 8
    # Default concurrency before enough data is collected.
    DEFAULT_CONCURRENCY = 4

    def __init__(self) -> None:
        # node_title → start monotonic time
        self._start_times: dict[str, float] = {}
        # cluster_key → list of observed durations (seconds)
        self._cluster_durations: dict[str, list[float]] = {}
        # All completed samples: list of (title, duration, predicted, error_ratio)
        self._samples: list[dict] = []
        # Current adaptive concurrency recommendation
        self._recommended_concurrency: int = self.DEFAULT_CONCURRENCY

    @staticmethod
    def _cluster_key(title: str) -> str:
        """Map a node title to a cluster key by stripping trailing digits/suffixes.

        Examples:
          "4a-performance-tracking" → "4a"
          "build-fep-tool-plan"     → "build"
          "step-3-verify"           → "step"
        """
        # Strip leading step numbers (e.g. "4a", "01", "phase-1")
        parts = title.replace("_", "-").split("-")
        if parts:
            return parts[0]
        return title

    def record_start(self, title: str) -> None:
        """Record that *title* started executing now."""
        self._start_times[title] = time.monotonic()

    def record_complete(self, title: str, success: bool = True) -> float | None:
        """Record that *title* finished. Returns the observed duration in seconds.

        Updates cluster rolling statistics and recomputes adaptive concurrency.
        Only successful completions contribute to the learning model.
        """
        start = self._start_times.pop(title, None)
        if start is None:
            return None
        duration = time.monotonic() - start
        if not success:
            return duration  # don't learn from failures

        cluster = self._cluster_key(title)
        history = self._cluster_durations.setdefault(cluster, [])

        # 4b — compute prediction error against current cluster mean
        predicted: float | None = None
        error_ratio: float = 0.0
        if history:
            predicted = sum(history) / len(history)
            if predicted > 0:
                error_ratio = abs(duration - predicted) / predicted

        # 4c — update cluster rolling history (cap at 20 to limit memory)
        history.append(duration)
        if len(history) > 20:
            history.pop(0)

        self._samples.append({
            "title": title,
            "cluster": cluster,
            "duration": duration,
            "predicted": predicted,
            "error_ratio": error_ratio,
        })

        # 4d — update adaptive concurrency after enough samples
        self._update_concurrency()
        return duration

    def _update_concurrency(self) -> None:
        """Recompute recommended concurrency from recent prediction errors (4d)."""
        if len(self._samples) < self.MIN_SAMPLES_FOR_ADAPTATION:
            return

        # Look at the last N samples for error signal
        recent = self._samples[-10:]
        errors = [s["error_ratio"] for s in recent if s["predicted"] is not None]
        if not errors:
            return

        avg_error = sum(errors) / len(errors)

        if avg_error > self.HIGH_ERROR_THRESHOLD:
            # High surprise → reduce concurrency to stabilise
            self._recommended_concurrency = max(
                self.MIN_CONCURRENCY,
                self._recommended_concurrency - 1,
            )
        elif avg_error < self.HIGH_ERROR_THRESHOLD * 0.3:
            # Low surprise → safely increase concurrency
            self._recommended_concurrency = min(
                self.MAX_CONCURRENCY,
                self._recommended_concurrency + 1,
            )
        # else: mid-range error — hold current level

    @property
    def recommended_concurrency(self) -> int:
        """Current adaptive concurrency limit for the orchestrator dispatch loop."""
        return self._recommended_concurrency

    def summary(self) -> str:
        """Return a one-line human-readable summary for logging."""
        n = len(self._samples)
        if n == 0:
            return "perf-tracker: no samples yet"
        recent_errors = [
            s["error_ratio"] for s in self._samples[-10:] if s["predicted"] is not None
        ]
        avg_err_pct = (sum(recent_errors) / len(recent_errors) * 100) if recent_errors else 0.0
        return (
            f"perf-tracker: n={n} samples, "
            f"avg_pred_err={avg_err_pct:.1f}%, "
            f"recommended_concurrency={self._recommended_concurrency}"
        )


class AgentHandle:
    """Tracks a running worker agent process and its claimed-tasks entry."""

    # Base stall timeout (seconds). Scaled by effort level:
    #   low    → 1x (10 min)
    #   medium → 2x (20 min)
    #   high   → 3x (30 min)
    BASE_STALL_TIMEOUT_SECONDS: float = 600.0  # 10 minutes
    _EFFORT_MULTIPLIERS: dict[str, float] = {"low": 1.0, "medium": 2.0, "high": 3.0}

    def __init__(self, node_title: str, process: subprocess.Popen[str], claim_key: str, agent_ref: str | None = None, task_path: str | None = None, team: str = "worker", node_path: str | None = None, stream_parser: StreamParser | None = None, agent_status: AgentStatus | None = None, effort: str | None = None):
        self.node_title = node_title
        self.process = process
        self.claim_key = claim_key
        self.agent_ref = agent_ref
        self.task_path = task_path
        self.team = team
        self.node_path = node_path  # SmartList path for parallel complete
        self.stream_parser = stream_parser
        self.agent_status = agent_status
        self.started_at = time.monotonic()
        multiplier = self._EFFORT_MULTIPLIERS.get(effort or "low", 1.0)
        self.stall_timeout = self.BASE_STALL_TIMEOUT_SECONDS * multiplier

    def is_running(self) -> bool:
        return self.process.poll() is None

    def returncode(self) -> int | None:
        return self.process.poll()

    def get_stdout(self) -> str:
        """Return agent stdout — from stream parser if available, else raw pipe.

        When stream_parser is None (e.g. Codex agents that emit plain text
        rather than Claude stream-json), falls back to reading directly from
        the process stdout pipe.  This path is Codex-compatible.
        """
        if self.stream_parser:
            self.stream_parser.join(timeout=5.0)
            return self.stream_parser.result_text
        # Plain-text path (e.g. Codex agents that don't use stream-json)
        if self.process.stdout:
            try:
                return self.process.stdout.read() or ""
            except Exception:
                return ""
        return ""

    def get_stderr(self) -> str:
        """Return agent stderr from the raw pipe.

        Works for both Claude and Codex agents — neither routes stderr through
        StreamParser, so this always reads directly from the process pipe.
        """
        if self.process.stderr:
            try:
                return self.process.stderr.read() or ""
            except Exception:
                return ""
        return ""


class Orchestrator:
    """Drives the plan tree execution loop with parallel agent dispatch."""

    def __init__(self, max_steps: int = 200, dry_run: bool = False, input_path: str | None = None, backend_root: str | None = None, team_models: dict[str, str] | None = None, team_efforts: dict[str, str] | None = None, team_drivers: dict[str, str] | None = None, decompose_lookahead: bool = False):
        self.max_steps = max_steps
        self.dry_run = dry_run
        self.decompose_lookahead = decompose_lookahead
        self.steps_completed = 0
        self._tree_complete = False
        self.running_agents: dict[str, AgentHandle] = {}  # node_title -> AgentHandle
        self._repair_attempts: dict[str, int] = {}  # node_title -> attempt count
        self._repair_audit_trails: dict[str, str] = {}  # node_title -> audit bucket path
        self._lookahead_agents: dict[str, AgentHandle] = {}   # node_path -> AgentHandle (Mode C decomposers)
        self._lookahead_skip: set[str] = set()               # node_paths: NO DECOMPOSE received
        self._may_decompose_nodes: set[str] = set()          # node titles with may_decompose=true
        self.input_path = input_path
        self.backend_root = backend_root
        self._decomposed_nodes: set[str] = set()  # node titles created by decomposition
        self.sprint_atlas: str | None = None  # slug of the Atlas registered at startup
        self.perf_tracker = PerformanceTracker()  # Phase-4 adaptive scheduling
        self.team_models: dict[str, str] = {**DEFAULT_TEAM_MODELS, **(team_models or {})}
        self.team_efforts: dict[str, str] = {**DEFAULT_TEAM_EFFORTS, **(team_efforts or {})}
        self.team_drivers: dict[str, str] = {**DEFAULT_TEAM_DRIVERS, **(team_drivers or {})}
        self.dashboard = Dashboard()
        self._all_node_titles: dict[str, str] = {}  # title -> last known status (for tree panel)
        self._seed_node_titles_from_plan()  # pre-populate from plan JSON so blocked nodes show up
        self.pool: AgentPool | None = None
        self._system_channel_ready = False
        if input_path:
            self.pool = AgentPool(input_path, backend_root=backend_root)
            try:
                bootstrap_system_channel(input_path, backend_root=backend_root)
                self._system_channel_ready = True
            except Exception:
                pass  # channel may already exist
            # Ensure orchestrator inbox exists for receiving worker notifications
            try:
                from swarm.messaging import ensure_inbox
                ensure_inbox(input_path, "orchestrator", backend_root=backend_root)
            except Exception:
                pass

    def log(self, message: str) -> None:
        print(f"[orchestrator] {message}", flush=True)
        if hasattr(self, "dashboard"):
            self.dashboard.add_log(message)

    def _seed_node_titles_from_plan(self) -> None:
        """Pre-populate _all_node_titles from the plan JSON so every node (including
        blocked/future ones) appears in the dashboard tree from the first render.

        Looks for a matching plan file in plans/ and scripts/plans/ using the active
        plan name.  Silently skips if no file is found (incremental accumulation
        then serves as the fallback).
        """
        plan_name = _CURRENT_PLAN_NAME
        if not plan_name:
            return
        candidates = [
            REPO_ROOT / "plans" / f"{plan_name}.json",
            SCRIPT_DIR / "plans" / f"{plan_name}.json",
            REPO_ROOT / "plans" / f"{plan_name}.yaml",
            SCRIPT_DIR / "plans" / f"{plan_name}.yaml",
        ]
        for path in candidates:
            if not path.exists():
                continue
            try:
                import json as _json
                import yaml as _yaml  # type: ignore[import]
                text = path.read_text(encoding="utf-8")
                data = _json.loads(text) if path.suffix == ".json" else _yaml.safe_load(text)
                for node in data.get("nodes", []):
                    title = node.get("title") or node.get("id") or node.get("name")
                    if title and title not in self._all_node_titles:
                        self._all_node_titles[title] = "blocked"
                self.log(f"[dashboard] seeded {len(self._all_node_titles)} nodes from {path.name}")
                return
            except Exception as exc:
                self.log(f"[dashboard] could not seed nodes from {path.name}: {exc}")
                return

    def _bootstrap_claimed_tasks(self) -> None:
        """Create the claimed-tasks SmartList with unique_members policy at startup.

        Also clears stale claims from any previous orchestrator run that exited
        without cleanup (idle-break, crash, SIGKILL), so every fresh start
        begins with an empty claimed-tasks list.
        """
        try:
            claimed_tasks_clear()  # release stale claims before bootstrap
            claimed_tasks_bootstrap()
            self.log(f"Policy-gated claimed-tasks list initialised at '{CLAIMED_TASKS_PATH}'")
        except Exception as exc:
            self.log(f"WARNING: claimed-tasks bootstrap failed: {exc} — continuing without it")

    def _broadcast_system(self, subject: str, body: str) -> None:
        """Send a system-channel broadcast to all running agents (best-effort)."""
        if not self.input_path or not self._system_channel_ready:
            return
        try:
            system_broadcast(
                self.input_path, "orchestrator", subject, body,
                backend_root=self.backend_root,
            )
        except Exception:
            pass

    @staticmethod
    def _build_codex_cmd(prompt: str) -> list[str]:
        """Build the command list to spawn a Codex agent non-interactively."""
        # codex CLI: https://github.com/openai/codex
        # -q = quiet (no interactive prompts), --full-auto = approve all tool calls
        return ["codex", "--full-auto", "-q", prompt]

    def _spawn_agent(self, node_info: dict[str, str], team: str = "worker") -> AgentHandle | None:
        """Spawn a claude or codex agent for the given node."""
        title = node_info.get("title", "unknown")
        # Use node_path as the canonical claim key; fall back to title slug.
        claim_key = node_info.get("node_path") or f"swarm/{title}"

        # Policy-gated claim: attach node to claimed-tasks SmartList.
        # The unique_members policy rejects duplicates — if attach fails,
        # another orchestrator already owns this node; skip dispatch.
        if not claimed_tasks_attach(claim_key):
            self.log(f"Claimed-tasks attach rejected for '{title}' — skipping (already claimed)")
            return None

        # Build prompt
        if team == "worker":
            prompt = format_worker_prompt(node_info)
        elif team == "decomposer":
            prompt = format_decomposer_prompt(node_info)
        elif team == "bug-reporter":
            stderr_tail = node_info.get("_stderr_tail", "")
            prompt = format_bug_reporter_prompt(node_info, title, stderr_tail)
        elif team == "bugreport-repairer":
            bug_id = node_info.get("_bug_id", "")
            prompt = format_bugreport_repairer_prompt(node_info, bug_id)
        elif team == "verifier":
            return_text = node_info.get("_return_text", "")
            prompt = format_verifier_prompt(node_info, return_text)
        else:
            prompt = format_repairer_prompt(node_info)

        # Build command based on agent driver
        driver = self.team_drivers.get(team, "claude")
        if driver == "codex":
            cmd = self._build_codex_cmd(prompt)
            model = None
            effort = None
            self.log(f"Spawning {team} codex agent for: {title}")
        else:
            perm_mode = DEFAULT_TEAM_PERMISSIONS.get(team, "bypassPermissions")
            cmd = ["claude", "-p", prompt, "--permission-mode", perm_mode,
                   "--output-format", "stream-json", "--verbose"]
            model = self.team_models.get(team)
            if model:
                cmd.extend(["--model", model])
            effort = self.team_efforts.get(team)
            if effort:
                cmd.extend(["--effort", effort])
            self.log(f"Spawning {team} agent (model={model or 'default'}, effort={effort or 'default'}) for: {title}")

        # Decomposers/planners need AMS access for incremental checkpointing
        # (swarm-plan observe). Workers skip hooks to avoid hangs.
        if team in ("decomposer", "planner"):
            worker_env = {**os.environ}
        else:
            worker_env = {**os.environ, "AMS_HOOK_SKIP": "1"}
        try:
            process = subprocess.Popen(
                cmd,
                cwd=str(REPO_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=worker_env,
            )
        except FileNotFoundError:
            if driver == "codex":
                self.log(f"ERROR: 'codex' CLI not found. Install the OpenAI Codex CLI.")
            else:
                self.log(f"ERROR: 'claude' CLI not found. Install Claude Code.")
            claimed_tasks_detach(claim_key)
            return None

        # Set up live status tracking via stream-json parsing
        # Codex agents emit plain text (not stream-json), so skip StreamParser for them.
        agent_status = AgentStatus(
            title=title, team=team,
            model=model or "default", effort=effort or "",
        )
        if driver != "codex":
            stream_parser = StreamParser(process.stdout, agent_status)
            stream_parser.start()
        else:
            stream_parser = None
        self.dashboard.register(agent_status)

        # Allocate from agent pool if available
        agent_ref = None
        task_path = f"smartlist/swarm/task/{title}"
        if self.pool:
            try:
                agent_ref = self.pool.allocate(task_path)
            except Exception as exc:
                self.log(f"Pool allocate failed for {title}: {exc}")
            if agent_ref is None:
                self.log(f"Agent pool exhausted — cannot dispatch {title}")
                claimed_tasks_detach(claim_key)
                return None

        handle = AgentHandle(title, process, claim_key, agent_ref=agent_ref, task_path=task_path, team=team, node_path=node_info.get("node_path"), stream_parser=stream_parser, agent_status=agent_status, effort=effort)
        self.running_agents[title] = handle
        # Phase-4a: record start time for performance tracking
        if team == "worker":
            self.perf_tracker.record_start(title)

        # Send dispatch notification via message queue
        if self.input_path and agent_ref:
            try:
                send_to_inbox(
                    self.input_path, "orchestrator", agent_ref,
                    subject=f"dispatch:{title}",
                    body=f"You have been assigned to task: {title}",
                    backend_root=self.backend_root,
                )
            except Exception:
                pass  # messaging is best-effort

        return handle

    def _cleanup_agent(self, handle: AgentHandle) -> None:
        """Release pool slot and claimed-tasks entry for a completed agent."""
        if self.pool and handle.agent_ref and handle.task_path:
            try:
                self.pool.release(handle.agent_ref, handle.task_path)
            except Exception as exc:
                self.log(f"Pool release failed for {handle.node_title}: {exc}")
        # Release the policy-gated claim so the slot is available again.
        claimed_tasks_detach(handle.claim_key)
        self.running_agents.pop(handle.node_title, None)

    MAX_REPAIR_DEPTH = 3  # max escalation levels before surfacing to user
    MAX_DECOMPOSE_DEPTH = 3  # max nested decomposition levels before forcing worker execution

    def _get_decompose_depth(self, context: str) -> int:
        """Count how many ancestor frames in the callstack were created by decomposition.

        Nodes tagged in ``_decomposed_nodes`` (populated when a decomposer
        finishes and its subtasks are loaded) are counted.  The depth tells us
        how many nested decomposition levels we are currently inside.
        """
        depth = 0
        for line in context.splitlines():
            line = line.strip()
            # Frame lines look like: "2. node-title [work/active]"
            if line and line[0].isdigit() and "." in line:
                parts = line.split(".", 1)
                if len(parts) == 2:
                    title_part = parts[1].strip()
                    bracket_idx = title_part.rfind("[")
                    frame_title = title_part[:bracket_idx].strip() if bracket_idx > 0 else title_part.strip()
                    if frame_title in self._decomposed_nodes:
                        depth += 1
        return depth

    def _try_descend(self) -> bool:
        """Try to advance into a child node. Returns True if descended, False if at a leaf."""
        try:
            adv_result = advance()
            action = adv_result.get("action", "")
            if action == "tree-complete":
                self._tree_complete = True
                return False
            new_path = adv_result.get("active_node_path", "")
            if new_path:
                self.log(f"Descended to: {new_path}")
                return True
            return False
        except RuntimeError:
            return False

    def _get_repair_audit_bucket(self, title: str, node_info: dict[str, str]) -> str:
        """Get or create the repair audit trail bucket path for a node."""
        if title in self._repair_audit_trails:
            return self._repair_audit_trails[title]
        # Build audit bucket under the node's SmartList path
        context = node_info.get("context", "")
        node_path = parse_context_field(context, "active_node_path")
        if not node_path:
            # Parse from frame lines (e.g. "3. title [work/active] path")
            for line in context.splitlines():
                line = line.strip()
                if "[" in line and "]" in line and line[0:1].isdigit():
                    after_bracket = line.split("]", 1)[-1].strip()
                    if after_bracket.startswith("smartlist/"):
                        node_path = after_bracket
                        break
        if not node_path:
            node_path = f"smartlist/execution-plan/repair-orphans/{title}"
        audit_bucket = f"{node_path}/40-repair-audit"
        self._repair_audit_trails[title] = audit_bucket
        return audit_bucket

    def _record_repair_audit(
        self,
        title: str,
        node_info: dict[str, str],
        attempt: int,
        depth: int,
        *,
        reason: str = "",
        error_output: str = "",
        attempted_fix: str = "",
        repair_hint: str = "",
        outcome: str = "",
    ) -> None:
        """Record a repair attempt as a SmartList note in the node's audit trail."""
        audit_bucket = self._get_repair_audit_bucket(title, node_info)
        parts = [f"attempt={attempt}, depth={depth}"]
        if reason:
            parts.append(f"reason: {reason}")
        if error_output:
            parts.append(f"error: {error_output[:500]}")
        if attempted_fix:
            parts.append(f"attempted_fix: {attempted_fix}")
        if repair_hint:
            parts.append(f"hint: {repair_hint}")
        if outcome:
            parts.append(f"outcome: {outcome}")
        obs = node_info.get("observations", "")
        if obs:
            parts.append(f"context_observations: {obs[:300]}")
        note_text = "\n".join(parts)
        try:
            write_note_to_bucket(audit_bucket, f"repair-attempt-{attempt}", note_text, "orchestrator")
        except Exception as exc:
            self.log(f"Failed to record repair audit: {exc}")

    def _handle_interrupt(self, title: str, node_info: dict[str, str], depth: int = 0) -> None:
        """Handle an interrupted node: dispatch repairer, or escalate to user."""
        # Track per-node repair attempts to prevent infinite repairer loops
        attempts = self._repair_attempts.get(title, 0) + 1
        self._repair_attempts[title] = attempts

        # Extract repair metadata from the interrupt
        context = node_info.get("context", "")
        reason = ""
        error_output = ""
        attempted_fix = ""
        repair_hint = ""
        for line in context.splitlines():
            stripped = line.strip()
            if stripped.startswith("reason="):
                reason = stripped.split("=", 1)[1]
            elif stripped.startswith("error_output="):
                error_output = stripped.split("=", 1)[1]
            elif stripped.startswith("attempted_fix="):
                attempted_fix = stripped.split("=", 1)[1]
            elif stripped.startswith("repair_hint="):
                repair_hint = stripped.split("=", 1)[1]

        # Record this attempt in the audit trail
        self._record_repair_audit(
            title, node_info, attempts, depth,
            reason=reason, error_output=error_output,
            attempted_fix=attempted_fix, repair_hint=repair_hint,
            outcome="dispatching repairer" if (depth < self.MAX_REPAIR_DEPTH and attempts <= self.MAX_REPAIR_DEPTH) else "escalating to user",
        )

        if depth >= self.MAX_REPAIR_DEPTH or attempts > self.MAX_REPAIR_DEPTH:
            self.log(f"ESCALATION TO USER: repair failed after {max(depth, attempts)} attempts for '{title}'")
            self.log(f"Context: {node_info.get('context', 'none')[:200]}")
            audit_path = self._get_repair_audit_bucket(title, node_info)
            escalation_text = (
                f"Repair escalated to user after {attempts} attempt(s). "
                f"Audit trail: {audit_path}\n"
                f"Last failure reason: {reason or '(not recorded)'}\n"
                f"Last error: {error_output[:300] or '(not recorded)'}\n"
                f"Last attempted fix: {attempted_fix or '(not recorded)'}\n"
                f"Hint: {repair_hint or '(none)'}"
            )
            observe("escalation", escalation_text, "orchestrator")
            return

        self.log(f"Node '{title}' was interrupted — dispatching repairer (depth={depth}, attempt={attempts})")
        repairer_info = get_active_node_info()
        if repairer_info is None:
            self.log(f"ERROR: no active node info for repair dispatch")
            return
        self._spawn_agent(repairer_info, team="repairer")

    def _handle_repair_result(self, title: str, rc: int, depth: int = 0) -> None:
        """Check repairer output and either resume or escalate."""
        # Check if the repairer popped with REPAIR FAILED
        node_info = get_active_node_info()
        if node_info is None:
            return

        context = node_info.get("context", "")
        attempts = self._repair_attempts.get(title, 0)

        # If still interrupted after repairer finished, the repair failed — escalate
        active_kind = parse_context_field(context, "active_node_kind")
        if active_kind in ("interrupt", "policy"):
            # Record the failure in the audit trail
            self._record_repair_audit(
                title, node_info, attempts, depth,
                reason=f"Repairer could not fix '{title}'",
                error_output=f"Repairer exited with rc={rc}, node still in {active_kind} state",
                outcome="repair failed, escalating to parent",
            )
            # Try to resume first, then re-interrupt the parent
            try:
                resume_after_repair()
                self.log(f"Repair failed for '{title}' — escalating to parent")
                interrupt_for_repair(
                    reason=f"Repair escalation from child '{title}'",
                    error_output=f"Repairer could not fix '{title}' after depth={depth}",
                    context=f"Escalation chain at depth {depth}",
                    attempted_fix=f"Repairer agent dispatched {depth + 1} time(s)",
                    repair_hint="Examine child failure and attempt fix at higher level, or escalate to user",
                )
                self._handle_interrupt(title, get_active_node_info() or {}, depth + 1)
            except RuntimeError as exc:
                self.log(f"Escalation failed: {exc} — surfacing to user")
                audit_bucket = self._get_repair_audit_bucket(title, node_info)
                observe("escalation", f"Cannot escalate further: {exc}. Audit trail: {audit_bucket}. Manual intervention required.", "orchestrator")
        else:
            # Repair succeeded — record success in audit trail
            self._record_repair_audit(
                title, node_info, attempts, depth,
                outcome="repair succeeded",
            )
            try:
                resume_result = resume_after_repair()
                self.log(f"Repair succeeded, resumed: {resume_result.get('active_node_path', '')}")
            except RuntimeError as exc:
                self.log(f"Resume after repair failed: {exc}")

    @staticmethod
    def _parse_bug_id(stdout: str) -> str | None:
        """Extract a bug_id from agent stdout (looks for 'smartlist-bugreport:' prefix)."""
        match = re.search(r"(smartlist-bugreport:[0-9a-f]+)", stdout or "")
        return match.group(1) if match else None

    def _handle_worker_completion(
        self, title: str, handle: AgentHandle, rc: int | None,
        stderr_tail: str, inbox_failed_titles: set[str],
    ) -> None:
        """Handle a worker (or legacy repairer) agent finishing."""
        worker_failed = (rc is not None and rc != 0) or title in inbox_failed_titles

        # In parallel dispatch, workers operate on specific node paths (not the
        # active cursor).  Reading get_active_node_info() returns the *root* node
        # — not the worker's node — so checking active_node_kind would
        # misinterpret root state as the worker's state and wrongly spawn a
        # repairer on the root.  Only check global active-node state when the
        # worker was dispatched via the cursor (no node_path).
        if handle.node_path:
            # Parallel-dispatch path: skip global active-node interrupt check
            node_info = get_node_context(handle.node_path)
            context_text = node_info.get("context", "") if node_info else ""
            active_kind = None  # not applicable for path-based workers
        else:
            node_info = get_active_node_info()
            context_text = node_info.get("context", "") if node_info else ""
            active_kind = parse_context_field(context_text, "active_node_kind")

        if not handle.node_path and node_info and active_kind in ("interrupt", "policy"):
            # Worker already self-interrupted — handle via existing repair flow
            self._handle_interrupt(title, node_info)
        elif worker_failed:
            # Phase-4a: record failed completion (does not update learning model)
            self.perf_tracker.record_complete(title, success=False)
            # Decomposers don't write code — failure means analysis was interrupted,
            # not that code is broken. Re-queue for retry instead of spawning repairer.
            if handle.team in ("decomposer", "planner"):
                self.log(f"Decomposer/planner '{title}' failed (exit={rc}) — will retry on next dispatch cycle (no repairer needed)")
                return
            if handle.node_path:
                # Parallel-dispatch path: can't pause/interrupt a specific child node
                # via the global cursor. Log and re-queue for retry on next dispatch cycle.
                self.log(f"Parent-detected failure for '{title}' (exit={rc}) on path {handle.node_path} — will retry on next cycle")
            else:
                # Cursor-based sequential path: pause line, spawn bug-reporter
                self.log(f"Parent-detected failure for '{title}' (exit={rc}) — pausing execution line")
                try:
                    pause_node(
                        reason=f"Subagent for '{title}' failed with exit code {rc}",
                        error_output=stderr_tail or f"exit code {rc}",
                        context=context_text[:300],
                        actor_id="orchestrator",
                    )
                    self.log(f"Execution line paused for '{title}' — spawning bug-reporter agent")
                    bug_node_info = get_active_node_info()
                    if bug_node_info:
                        bug_node_info["_stderr_tail"] = stderr_tail
                        self._spawn_agent(bug_node_info, team="bug-reporter")
                except RuntimeError as exc:
                    self.log(f"Failed to pause node for '{title}': {exc}")
                    if node_info:
                        self._handle_interrupt(title, node_info)
        else:
            # Worker completed normally — check if this is a decomposer that
            # produced a sub-plan (route to decomposer handler, not verifier).
            if handle.team in ("decomposer", "planner"):
                stdout_text = handle.get_stdout() or ""
                if "PLAN FILE:" in stdout_text:
                    self.log(f"Decomposer '{title}' produced a plan — routing to decomposer handler")
                    self._handle_decomposer_completion(title, handle)
                    return

            # Regular worker completed normally — verify before marking done
            # Read the worker's return text from its receipts/stdout
            return_text = ""
            try:
                stdout_text = handle.get_stdout()
                # Extract the last meaningful line as return text
                for line in reversed(stdout_text.strip().splitlines()):
                    if line.strip():
                        return_text = line.strip()
                        break
            except Exception:
                pass

            # Phase-4a/4b/4c/4d: record completion for adaptive scheduling
            duration = self.perf_tracker.record_complete(title, success=True)
            if duration is not None:
                self.log(f"[perf] '{title}' completed in {duration:.1f}s — {self.perf_tracker.summary()}")

            # FEP hook point: detect cache hits reported by the worker.
            # Workers signal a cache hit by including "CACHE_HIT" anywhere in
            # their output.  We emit telemetry so the FEP dream-schedule can
            # track hit/miss rates per tool and node path (P7-C2).
            if "CACHE_HIT" in (stdout_text or ""):
                self._emit_fep_cache_signal(handle.node_path or title, is_hit=True)
                self.log(f"[cache-hit] Worker '{title}' reported cached result — routing through verifier to confirm ({handle.node_path})")
                # Do NOT skip the verifier for cache hits: a false cache hit (stale entry
                # where work was never done) would be marked complete with no check.
                # Fall through to the normal verifier dispatch below.

            # Get node context for verification
            verify_node_info = get_node_context(handle.node_path) if handle.node_path else (node_info or {})
            if verify_node_info is None:
                verify_node_info = {}
            verify_node_info["_return_text"] = return_text

            self.log(f"Worker '{title}' reported success — spawning verifier")
            self._spawn_agent(verify_node_info, team="verifier")

    def _handle_verifier_completion(self, title: str, handle: AgentHandle) -> None:
        """Handle verifier agent finishing: check verdict, complete or re-dispatch."""
        stdout_text = handle.get_stdout()

        # Parse verdict — scan all lines for a known keyword prefix so that
        # preamble (tool echoes, thinking text) before the verdict is ignored.
        # Strip leading markdown decoration (**, *, #) that agents sometimes add.
        _VERDICT_PREFIXES = ("VERIFIED", "FAKE SUCCESS", "UNVERIFIABLE")
        verdict = ""
        verdict_detail = ""
        import re as _re
        _md_strip = _re.compile(r'^[*#\s]+')
        for line in stdout_text.splitlines():
            stripped = line.strip()
            candidate = _md_strip.sub("", stripped).upper()
            for p in _VERDICT_PREFIXES:
                if candidate.startswith(p):
                    verdict = stripped  # keep original for display
                    break
            if verdict:
                break
        # Capture full output as detail (no truncation here — slicing happens at storage sites)
        verdict_detail = stdout_text.strip()

        # Normalize verdict for keyword matching: strip markdown bold/heading markers
        # Agents often output **VERIFIED:** or ## VERIFIED: instead of plain VERIFIED:
        verdict_normalized = _md_strip.sub("", verdict).strip() if verdict else ""

        if verdict_normalized.upper().startswith("VERIFIED"):
            self.log(f"Verifier confirmed: '{title}' — {verdict}")
            self._record_repair_audit(
                title, get_active_node_info() or {}, 0, 0,
                outcome=f"VERIFIED: {verdict_detail[:1000]}",
            )
            # Batch observe + complete-node + advance into a single lock acquisition
            if handle.node_path:
                try:
                    results = batch_ops([
                        {"cmd": "observe", "title": f"verified:{title}", "text": verdict_detail[:4000]},
                        {"cmd": "complete-node", "node_path": handle.node_path,
                         "return_text": f"Verified and completed: {title}"},
                        {"cmd": "advance"},
                    ], "orchestrator")
                    adv = results[-1] if results else {}
                    if adv.get("action") == "tree-complete":
                        self._tree_complete = True
                    else:
                        self.log(f"Advanced to: {adv.get('active_node_path', '(root)')}")
                    self.log(f"Completed node: {title} ({handle.node_path})")
                    # P7-C2: emit miss signal — node completed via real work (not cache)
                    self._emit_fep_cache_signal(handle.node_path, is_hit=False)
                    # A3: promote completed result as a cache artifact
                    self._cache_promote_node(handle.node_path, verdict_detail[:4000])
                except RuntimeError as exc:
                    self.log(f"complete-node failed for {title}: {exc}")
                    try:
                        adv_result = advance()
                        if adv_result.get("action") == "tree-complete":
                            self._tree_complete = True
                    except RuntimeError:
                        pass
            else:
                try:
                    adv_result = advance()
                    if adv_result.get("action") == "tree-complete":
                        self._tree_complete = True
                    else:
                        self.log(f"Advanced to: {adv_result.get('active_node_path', '')}")
                except RuntimeError as exc:
                    self.log(f"Advance failed: {exc}")

        elif verdict_normalized.upper().startswith("FAKE SUCCESS"):
            self.log(f"Verifier caught fake success for '{title}': {verdict}")
            # Record the diagnosis in the audit trail
            self._record_repair_audit(
                title, get_active_node_info() or {}, 0, 0,
                reason=f"Verifier detected fake success: {verdict}",
                error_output=verdict_detail[:4000],
                outcome="re-dispatching worker after fake success diagnosis",
            )
            observe(f"fake-success:{title}", verdict_detail[:4000], "orchestrator")
            # Re-dispatch a worker with the diagnosis context baked in
            node_info = get_node_context(handle.node_path) if handle.node_path else get_active_node_info()
            if node_info is None:
                node_info = {}
            # Append the verifier's diagnosis to the observations so the new worker knows what to fix
            existing_obs = node_info.get("observations", "")
            node_info["observations"] = (
                f"{existing_obs}\n\n"
                f"## PREVIOUS ATTEMPT FAILED VERIFICATION\n"
                f"The previous worker claimed success but verification found:\n"
                f"{verdict_detail}\n\n"
                f"You MUST complete the missing deliverables described above."
            )
            self.log(f"Re-dispatching worker for '{title}' with verification feedback")
            self._spawn_agent(node_info, team="worker")

        elif verdict_normalized.upper().startswith("UNVERIFIABLE"):
            self.log(f"Verifier: spec too vague for '{title}' — completing with warning")
            observe(f"unverifiable:{title}", verdict_detail[:4000], "orchestrator")
            self._record_repair_audit(
                title, get_active_node_info() or {}, 0, 0,
                outcome=f"UNVERIFIABLE: {verdict_detail[:1000]}",
            )
            if handle.node_path:
                try:
                    complete_specific_node(handle.node_path, f"Completed (unverifiable spec): {title}")
                    # A3: promote completed result as a cache artifact
                    self._cache_promote_node(handle.node_path, verdict_detail[:4000])
                except RuntimeError:
                    pass
            else:
                try:
                    advance()
                except RuntimeError:
                    pass
        else:
            # Verifier didn't produce a parseable verdict — treat as verified with warning
            self.log(f"Verifier produced unparseable output for '{title}' — completing with warning")
            observe(f"verify-unparseable:{title}", f"Verifier output: {verdict_detail[:4000]}", "orchestrator")
            self._record_repair_audit(
                title, get_active_node_info() or {}, 0, 0,
                outcome=f"UNPARSEABLE: {verdict_detail[:1000]}",
            )
            if handle.node_path:
                try:
                    complete_specific_node(handle.node_path, f"Completed (verify inconclusive): {title}")
                    # A3: promote completed result as a cache artifact
                    self._cache_promote_node(handle.node_path, verdict_detail[:4000])
                except RuntimeError:
                    pass
            else:
                try:
                    advance()
                except RuntimeError:
                    pass

    def _handle_bug_reporter_completion(self, title: str, handle: AgentHandle) -> None:
        """Handle bug-reporter agent finishing: parse bug_id, spawn repairer."""
        # Read stdout to find the bug_id
        stdout_text = handle.get_stdout()

        bug_id = self._parse_bug_id(stdout_text)

        if bug_id:
            self.log(f"Bug-reporter created BugReport {bug_id} — spawning repairer")
            observe("bug-report-filed", f"bug_id={bug_id} for failed node '{title}'", "orchestrator")
            # Spawn repairer with BugReport context
            repairer_info = get_active_node_info()
            if repairer_info:
                repairer_info["_bug_id"] = bug_id
                self._spawn_agent(repairer_info, team="bugreport-repairer")
            else:
                self.log(f"No active node after bug-reporter — cannot spawn repairer")
        else:
            # Bug-reporter may have skipped (false positive) or crashed
            if "BUG SKIPPED" in stdout_text:
                self.log(f"Bug-reporter determined false positive for '{title}' — resuming")
                try:
                    resume_after_repair()
                    adv_result = advance()
                    if adv_result.get("action") == "tree-complete":
                        self._tree_complete = True
                    else:
                        self.log(f"Resumed and advanced to: {adv_result.get('active_node_path', '')}")
                except RuntimeError as exc:
                    self.log(f"Resume after false-positive failed: {exc}")
            else:
                self.log(f"Bug-reporter finished without creating a BugReport for '{title}' — escalating")
                node_info = get_active_node_info()
                if node_info:
                    self._handle_interrupt(title, node_info)

    def _handle_bugreport_repairer_completion(self, title: str, handle: AgentHandle) -> None:
        """Handle repairer (BugReport-driven) finishing: resume paused line on success."""
        stdout_text = handle.get_stdout()

        repair_succeeded = "REPAIRED:" in stdout_text and "REPAIR FAILED:" not in stdout_text

        if repair_succeeded:
            self.log(f"BugReport repairer succeeded for '{title}' — resuming execution line")
            try:
                resume_result = resume_after_repair()
                self.log(f"Resumed: {resume_result.get('active_node_path', '')}")
                # Advance past the now-fixed node
                adv_result = advance()
                if adv_result.get("action") == "tree-complete":
                    self.log("Plan tree fully executed!")
                    self._tree_complete = True
                else:
                    self.log(f"Advanced to: {adv_result.get('active_node_path', '')}")
            except RuntimeError as exc:
                self.log(f"Resume after BugReport repair failed: {exc}")
        else:
            self.log(f"BugReport repairer failed for '{title}' — escalating")
            node_info = get_active_node_info()
            if node_info:
                self._handle_interrupt(title, node_info)

    def _handle_decomposer_completion(self, title: str, handle: AgentHandle) -> None:
        """Handle decomposer agent finishing: parse plan file, resume, load subtasks, advance."""
        # Read stdout to find the plan file path
        stdout_text = handle.get_stdout()

        # Parse 'PLAN FILE: <path>' from stdout
        plan_match = re.search(r"PLAN FILE:\s*(\S+)", stdout_text)
        if not plan_match:
            # No plan file produced.  Analysis-only decomposers (e.g. lead-engineer
            # pattern) record observations and complete without a sub-plan.
            # Complete directly if meaningful output exists; retry if empty.
            rc = handle.returncode()
            if rc is not None and rc != 0:
                self.log(f"Decomposer for '{title}' failed (exit={rc}) without PLAN FILE — will retry on next cycle")
                return
            # Analysis-only decomposer (lead-engineer pattern) completed.
            # Check if it produced meaningful output: either checkpoint
            # observations on its node or stdout content.
            stdout_text_check = (handle.get_stdout() or "").strip()
            has_stdout = len(stdout_text_check) > 50  # non-trivial output
            # Check for child-done signal in stdout (the prompt tells them to call child-done)
            has_child_done = "child-done" in stdout_text_check.lower() or "checkpoint:" in stdout_text_check.lower()
            if has_stdout or has_child_done:
                self.log(f"Analysis-only decomposer for '{title}' completed with output — completing node directly")
                # Complete the node directly — no verifier needed for analysis nodes.
                # The downstream dependent nodes will consume the observations.
                if handle.node_path:
                    try:
                        summary = stdout_text_check[-500:] if has_stdout else f"Analysis complete for {title}"
                        results = batch_ops([
                            {"cmd": "observe", "title": f"analysis:{title}", "text": summary[:4000]},
                            {"cmd": "complete-node", "node_path": handle.node_path,
                             "return_text": f"Analysis complete: {title}"},
                            {"cmd": "advance"},
                        ], "orchestrator")
                        adv = results[-1] if results else {}
                        if adv.get("action") == "tree-complete":
                            self._tree_complete = True
                        self.log(f"Completed analysis node: {title}")
                    except RuntimeError as exc:
                        self.log(f"complete-node failed for analysis {title}: {exc}")
                else:
                    try:
                        adv_result = advance()
                        if adv_result.get("action") == "tree-complete":
                            self._tree_complete = True
                    except RuntimeError:
                        pass
            else:
                self.log(f"Analysis-only decomposer for '{title}' produced no meaningful output — will retry on next cycle")
            return

        plan_path = plan_match.group(1)
        self.log(f"Decomposer produced plan: {plan_path}")

        # (1) Resume to clear the decompose interrupt
        try:
            resume_after_repair()
            self.log(f"Resumed after decompose interrupt for '{title}'")
        except RuntimeError as exc:
            self.log(f"Resume after decompose failed: {exc}")
            return

        # (2) Load the plan as children of the current active node
        result = run_ams("swarm-plan", "load-plan", "--file", plan_path, "--into-active")
        if result.returncode != 0:
            self.log(f"load-plan failed: {result.stderr}")
            observe("decompose-error", f"load-plan --into-active failed for {plan_path}: {result.stderr[:200]}", "orchestrator")
            return

        self.log(f"Loaded subtasks from {plan_path} into active node")

        # Build a detailed decomposition audit trail
        subtask_summary = f"Decomposed '{title}' via {plan_path}"
        try:
            plan_full_path = REPO_ROOT / plan_path if not Path(plan_path).is_absolute() else Path(plan_path)
            with open(plan_full_path, "r", encoding="utf-8") as f:
                plan_data = json.load(f)
            subtasks = plan_data.get("nodes", [])
            if subtasks:
                breakdown_lines = [f"- {n.get('title', '?')}: {n.get('description', '')[:120]}" for n in subtasks]
                subtask_summary = f"Subtask breakdown ({len(subtasks)} tasks):\n" + "\n".join(breakdown_lines)
        except Exception:
            pass  # fall back to basic summary
        observe(f"decomposed: {title}", subtask_summary, "orchestrator")

        # Tag the current node as decomposition-created so depth guard can track it
        self._decomposed_nodes.add(title)

        # (3) Advance into the first ready child
        try:
            adv_result = advance()
            action = adv_result.get("action", "")
            if action == "tree-complete":
                self.log("Plan tree fully executed!")
                self._tree_complete = True
            else:
                self.log(f"Advanced into first subtask: {adv_result.get('active_node_path', '')}")
        except RuntimeError as exc:
            self.log(f"Advance after decompose failed: {exc}")

    # -------------------------------------------------------------------------
    # Look-ahead decomposition (--decompose-lookahead)
    # -------------------------------------------------------------------------

    def _update_may_decompose_from_ready(self, ready: list[dict]) -> None:
        """Populate _may_decompose_nodes from ready-nodes output (may_decompose field)."""
        for node in ready:
            if node.get("may_decompose") == "true":
                title = node.get("title", "")
                if title:
                    self._may_decompose_nodes.add(title)

    # Semantic patterns that indicate an open-ended goal node requiring JIT decomposition.
    # Each pattern is matched against the lowercased description (prefix match on word boundary).
    _STUB_SEMANTIC_PREFIXES: tuple[str, ...] = (
        "build a",
        "build an",
        "implement a",
        "implement an",
        "create a",
        "create an",
        "design a",
        "design an",
        "analyze and fix",
        "analyse and fix",
        "investigate and fix",
        "add a system",
        "build a system",
        "develop a",
        "develop an",
        "write a system",
        "create a command that",
        "build a command that",
        "implement a command that",
    )

    @classmethod
    def _is_stub_node(cls, node: dict) -> bool:
        """Return True if a ready node looks like an architectural stub needing decomposition.

        A stub is a high-level placeholder added during early planning that has no
        substantive description yet (brief title, zero recorded observations).  Rather
        than dispatching a worker that will spin on nothing, the orchestrator sends a
        Mode C decomposer to expand it using parent context first.

        Heuristic (conservative — avoids false positives on real leaf tasks):
          Structural:  title is short (< 60 chars) AND no observations recorded.
          Semantic:    description begins with an open-ended goal phrase such as
                       "Build a ...", "Implement a ...", "Create a command that ...",
                       "Design a system that ...", "Analyze and fix ...", etc.
          Explicit:    may_decompose=true is already handled by levels 1/2 — this
                       method returns False for those to avoid double-counting.
        """
        already_flagged = node.get("may_decompose", "false") == "true"
        if already_flagged:
            return False

        title = (node.get("title") or "").strip()
        obs_count = int(node.get("observations_count") or "0")

        description = (node.get("description") or "").strip()

        # Structural heuristic: short title + zero observations + blank/missing description.
        # Any non-trivial description (> 10 chars) means this is a concrete task, not a placeholder.
        if len(title) < 60 and obs_count == 0 and len(description) <= 10:
            return True

        # Semantic heuristic: open-ended goal phrases in description
        for prefix in cls._STUB_SEMANTIC_PREFIXES:
            if description.lower().startswith(prefix):
                return True

        return False

    def _get_lookahead_candidates(self, ready: list[dict]) -> list[dict]:
        """Return look-ahead decomposition candidates from the ready set.

        Level 1: all ready nodes with may_decompose=true.
        Level 2: first ready child of each level-1 node (parent_node_path in level-1 paths).
        Level 3: auto-detected stub nodes (brief title, no observations) — only when
                 decompose_lookahead is enabled.
        All levels are subsets of 'ready' — no extra queries needed.
        """
        level1_paths = {
            n.get("node_path", "") for n in ready
            if n.get("title", "") in self._may_decompose_nodes
        }
        candidates: list[dict] = []
        seen: set[str] = set()
        for node in ready:
            title = node.get("title", "")
            path = node.get("node_path", "")
            if not path or path in seen:
                continue
            # Level 1: node itself is flagged
            is_level1 = title in self._may_decompose_nodes
            # Level 2: node's parent is a level-1 candidate (first child of upcoming node)
            is_level2 = node.get("parent_node_path", "") in level1_paths
            # Level 3: auto-detected stub — brief title, no observations recorded yet
            is_stub = self.decompose_lookahead and self._is_stub_node(node)
            if is_level1 or is_level2 or is_stub:
                if is_stub and not is_level1 and not is_level2:
                    node = dict(node, stub_detected="true")  # tag for prompt builder
                candidates.append(node)
                seen.add(path)
        return candidates

    def _dispatch_lookahead_decomposers(self, ready: list[dict]) -> None:
        """Claim and spawn Mode C decomposers for look-ahead candidates."""
        self._update_may_decompose_from_ready(ready)
        for node in self._get_lookahead_candidates(ready):
            node_path = node.get("node_path", "")
            title = node.get("title", "unknown")
            if not node_path:
                continue
            # Skip if already decomposing, decomposed, or worker already dispatched
            if node_path in self._lookahead_agents:
                continue
            if node_path in self._lookahead_skip:
                continue
            if title in self.running_agents:
                continue
            # Claim the node (same primitive as worker dispatch — blocks double-dispatch)
            if not interrupt_for_decompose_lookahead(node_path):
                self.log(f"[lookahead] Claim rejected for '{title}' — already owned")
                continue
            # Build Mode C node_info
            node_info = {
                **node,
                "lookahead_node_path": node_path,
                "lookahead_node_title": title,
                "lookahead_node_description": node.get("observations", ""),
                "context": get_context() or "",
            }
            handle = self._spawn_agent(node_info, team="decomposer")
            if handle:
                # _spawn_agent also calls claimed_tasks_attach internally — that's OK,
                # the policy-gated SmartList deduplicates; we hold our own claim already.
                self._lookahead_agents[node_path] = handle
                self.log(f"[lookahead] Spawned Mode C decomposer for '{title}'")
            else:
                # Spawn failed — release our claim so the node can be dispatched normally
                release_decompose_lookahead(node_path)

    def _poll_lookahead_agents(self) -> None:
        """Check finished look-ahead decomposers and process their output."""
        finished = [
            (path, handle)
            for path, handle in self._lookahead_agents.items()
            if not handle.is_running()
        ]
        for node_path, handle in finished:
            del self._lookahead_agents[node_path]
            title = handle.node_title
            stdout_text = handle.get_stdout()

            # Check for NO DECOMPOSE
            no_decompose = re.search(r"NO DECOMPOSE:\s*(.+)", stdout_text)
            if no_decompose:
                reason = no_decompose.group(1).strip()
                self.log(f"[lookahead] NO DECOMPOSE for '{title}': {reason}")
                self._lookahead_skip.add(node_path)
                release_decompose_lookahead(node_path)
                continue

            # Parse PLAN FILE
            plan_match = re.search(r"PLAN FILE:\s*(\S+)", stdout_text)
            if not plan_match:
                self.log(f"[lookahead] Decomposer for '{title}' emitted neither PLAN FILE nor NO DECOMPOSE — releasing claim")
                self._lookahead_skip.add(node_path)
                release_decompose_lookahead(node_path)
                continue

            plan_path = plan_match.group(1)
            self.log(f"[lookahead] Decomposer produced plan for '{title}': {plan_path}")

            # Inject sub-plan as children of the target node (without activating it)
            result = run_ams("swarm-plan", "load-plan", "--file", plan_path, "--into-node", node_path)
            if result.returncode != 0:
                self.log(f"[lookahead] load-plan --into-node failed for '{title}': {result.stderr[:200]}")
                self._lookahead_skip.add(node_path)
            else:
                self.log(f"[lookahead] Loaded subtasks from {plan_path} into '{title}'")
                observe(f"lookahead-decomposed: {title}", f"Pre-decomposed via {plan_path}", "orchestrator")
                self._decomposed_nodes.add(title)

            # Release the claim — children are now in 'ready' state, normal dispatch takes over
            release_decompose_lookahead(node_path)

    def poll_and_advance(self) -> list[str]:
        """Check orchestrator inbox for worker completion messages, then handle them.

        Workers send task-complete or task-failed messages to the orchestrator's
        inbox when they pop or interrupt.  This replaces the old approach of
        polling process exit codes — the inbox is the primary completion signal.

        Falls back to process exit polling for agents that didn't send an inbox
        message (e.g. if the worker crashed before sending).
        """
        completed: list[str] = []
        self._tree_complete = getattr(self, "_tree_complete", False)

        # --- Phase 1: Read orchestrator inbox for completion messages ---
        inbox_titles: set[str] = set()
        _inbox_failed_titles: set[str] = set()
        if self.input_path:
            try:
                messages = read_inbox(self.input_path, "orchestrator", backend_root=self.backend_root)
                for msg in messages:
                    name = msg.get("name", "")
                    # Match messages like msg:worker-0->orchestrator:task-complete:title
                    if "->orchestrator:" not in name:
                        continue
                    # Extract subject from title: msg:{sender}->orchestrator:{subject}
                    try:
                        subject_part = name.split("->orchestrator:", 1)[1]
                    except (IndexError, ValueError):
                        continue
                    if subject_part.startswith("task-complete:") or subject_part.startswith("task-failed:"):
                        kind, task_title = subject_part.split(":", 1)
                        if task_title in self.running_agents:
                            inbox_titles.add(task_title)
                            if kind == "task-failed":
                                _inbox_failed_titles.add(task_title)
                            self.log(f"Inbox: received {kind} for '{task_title}'")
            except Exception as exc:
                self.log(f"Inbox read failed (falling back to process poll): {exc}")

        # --- Phase 2: Handle inbox-notified completions + fallback poll ---
        for title, handle in list(self.running_agents.items()):
            # Primary signal: inbox message arrived
            got_inbox = title in inbox_titles
            # Fallback: process exited (covers crashes / missing inbox sends)
            proc_done = not handle.is_running()

            # Stall detection: kill worker if it exceeded the timeout
            if not got_inbox and not proc_done:
                elapsed = time.monotonic() - handle.started_at
                if elapsed > handle.stall_timeout:
                    self.log(f"STALL: '{title}' has been running for {elapsed:.0f}s (limit {handle.stall_timeout:.0f}s, team={handle.team}) — killing")
                    # Dump tool audit trail before killing
                    if handle.agent_status:
                        audit = handle.agent_status.format_tool_audit()
                        self.log(audit)
                    handle.process.kill()
                    handle.process.wait(timeout=10)
                    proc_done = True
                    # Fall through to handle as a failed completion
                else:
                    continue

            rc = handle.returncode()
            self.log(f"Agent for '{title}' finished (inbox={got_inbox}, exit={rc})")
            # Log tool audit trail for all completed agents
            if handle.agent_status and handle.agent_status.tool_log:
                self.log(handle.agent_status.format_tool_audit())
                self._emit_tool_audit(handle, stall_killed=not got_inbox and proc_done)

            # Capture stderr before cleanup (for parent-side failure diagnostics)
            _stderr_tail = ""
            if rc and rc != 0 and handle.process.stderr:
                try:
                    _stderr_tail = (handle.process.stderr.read() or "")[-500:]
                except Exception:
                    pass

            self._cleanup_agent(handle)
            completed.append(title)
            self.steps_completed += 1

            # Fire cross-project triggers on task completion (best-effort)
            if self.input_path:
                try:
                    fire_triggers(self.input_path, title, "task-complete", backend_root=self.backend_root)
                except Exception:
                    pass

            # --- Route completion based on agent team type ---
            agent_team = handle.team

            if agent_team == "decomposer":
                self._handle_decomposer_completion(title, handle)
            elif agent_team == "bug-reporter":
                self._handle_bug_reporter_completion(title, handle)
            elif agent_team == "bugreport-repairer":
                self._handle_bugreport_repairer_completion(title, handle)
            elif agent_team == "verifier":
                self._handle_verifier_completion(title, handle)
            else:
                # Worker or legacy repairer — use existing logic
                self._handle_worker_completion(title, handle, rc, _stderr_tail, _inbox_failed_titles)

        return completed

    # -------------------------------------------------------------------------
    # FEP tool audit trail emission
    # -------------------------------------------------------------------------

    def _emit_tool_audit(self, handle: "AgentHandle", stall_killed: bool = False) -> None:
        """Emit tool-call objects for each entry in handle.agent_status.tool_log.

        Writes one tool-call AMS object per log entry, including the duration_s
        provenance field, so the FEP pipeline can surface slow tools.
        Failures are best-effort — logged as warnings and do not abort orchestration.
        """
        if not self.input_path:
            return
        status = handle.agent_status
        if not status or not status.tool_log:
            return
        actor_id = f"swarm-{handle.team}:v1" if handle.team else "swarm-worker:v1"
        import datetime
        for entry in status.tool_log:
            try:
                ts_unix, tool_name, duration_s = entry
                ts_iso = datetime.datetime.fromtimestamp(
                    ts_unix, tz=datetime.timezone.utc
                ).isoformat().replace("+00:00", "Z")
                is_error = stall_killed
                result_preview = f"{duration_s:.1f}s"
                result = run_kernel(
                    "emit-tool-call",
                    "--input", self.input_path,
                    "--tool-name", tool_name,
                    "--is-error", "true" if is_error else "false",
                    "--result-preview", result_preview,
                    "--actor-id", actor_id,
                    "--duration-s", f"{duration_s:.6f}",
                    "--ts", ts_iso,
                )
                if result.returncode != 0:
                    self.log(
                        f"[warn] emit_tool_audit failed for '{tool_name}': {result.stderr[:200]}"
                    )
            except Exception as exc:
                self.log(f"[warn] emit_tool_audit exception for entry {entry!r}: {exc}")

    # -------------------------------------------------------------------------
    # GNUISNGNU v0.2 — Tool Identity Registration (A1)
    # -------------------------------------------------------------------------

    # Stable tool IDs for each swarm agent role.  Each role gets its own
    # Tool Identity Object in the AMS substrate and a dedicated cache SmartList
    # that survives session boundaries.
    AGENT_TOOL_IDS: list[tuple[str, str]] = [
        ("swarm-worker:v1",   "1.0"),
        ("swarm-verifier:v1", "1.0"),
        ("swarm-repairer:v1", "1.0"),
    ]

    def _register_agent_tools(self) -> dict[str, str]:
        """Register swarm agent roles as Tool Identity Objects via the AMS cache.

        Calls ``ams.py cache register-tool`` for each role and returns a mapping
        of ``tool_id -> object_id`` for use in subsequent cache-promote calls
        (A3).  Registration is idempotent — safe to call on every orchestrator
        start.  Failures are logged as warnings and do not abort orchestration.
        """
        tool_object_ids: dict[str, str] = {}

        for tool_id, tool_version in self.AGENT_TOOL_IDS:
            result = run_ams(
                "cache", "register-tool",
                "--tool-id", tool_id,
                "--tool-version", tool_version,
            )
            if result.returncode != 0:
                self.log(
                    f"[tool-identity] WARNING: failed to register {tool_id}: "
                    f"{result.stderr.strip()!r}"
                )
                continue

            # The CLI emits key=value pairs; extract the canonical object_id.
            kv = parse_kv(result.stdout)
            object_id = kv.get("object_id", f"cache-tool:{tool_id}")
            tool_object_ids[tool_id] = object_id
            self.log(f"[tool-identity] Registered {tool_id} -> {object_id}")

        return tool_object_ids

    # -------------------------------------------------------------------------
    # P7-C2 — FEP Cache Signal Emission
    # -------------------------------------------------------------------------

    def _emit_fep_cache_signal(self, source_id: str, *, is_hit: bool) -> None:
        """Fire-and-forget FEP cache-signal emission (P7-C2).

        Emits a 'tool-call' Object recording hit or miss for the
        ``swarm-worker:v1`` tool.  The signal is consumed by the dream-schedule
        to prioritise re-dreaming high-miss clusters.

        Non-blocking: failures are silently ignored so orchestration is never
        interrupted.
        """
        db = resolve_plan_db()
        outcome = "hit" if is_hit else "miss"
        result = run_kernel(
            "fep-cache-signal-emit",
            "--input", db,
            "--query", source_id,
            "--is-hit", "true" if is_hit else "false",
        )
        if result.returncode == 0:
            self.log(f"[fep-cache-signal] {outcome} emitted for {source_id!r}")
        else:
            # Non-fatal: log at DEBUG level only
            self.log(f"[fep-cache-signal] WARNING: emit failed for {source_id!r}: {result.stderr.strip()!r}")

    # -------------------------------------------------------------------------
    # GNUISNGNU v0.2 — Cache Promote on Completion (A3)
    # -------------------------------------------------------------------------

    def _cache_promote_node(self, node_path: str, return_text: str) -> str | None:
        """Promote a completed node's result as a cache artifact (A3).

        Calls ``ams-core-kernel cache-promote`` with the swarm-worker tool
        identity and the node_path as source_id.  The return_text is stored
        as the ``in_situ_ref`` so future cache lookups can recover the result
        without re-running the worker.

        Returns the artifact_id on success, None on failure.
        Failures are logged as warnings and never abort orchestration.
        """
        db = resolve_plan_db()
        # Truncate return_text to a safe length for the CLI arg
        ref = return_text[:2000] if return_text else "completed"
        result = run_kernel(
            "cache-promote",
            "--input", db,
            "--tool-id", "swarm-worker:v1",
            "--tool-version", "1.0",
            "--source-id", node_path,
            "--in-situ-ref", ref,
            "--actor-id", "orchestrator",
        )
        if result.returncode != 0:
            self.log(
                f"[cache-promote] WARNING: failed for {node_path}: "
                f"{result.stderr.strip()!r}"
            )
            return None
        else:
            kv = parse_kv(result.stdout)
            artifact_id = kv.get("artifact_id", "?")
            self.log(f"[cache-promote] Promoted {node_path} -> artifact {artifact_id}")
            # A6: store for resolution-aware artifact ref injection into dependent workers
            self._completed_artifacts[node_path] = artifact_id
            return artifact_id

    # -------------------------------------------------------------------------
    # GNUISNGNU v0.2 — Resolution-Aware Artifact Refs (A6)
    # -------------------------------------------------------------------------

    def _resolve_dependency_artifacts(self, node_info: dict[str, str]) -> list[tuple[str, str]]:
        """Return (dep_title, artifact_id) pairs for each completed dependency of a node.

        Uses the ``depends_on`` field (comma-separated sibling titles) from the
        ready-nodes output to find dependency node paths, then looks up their
        artifact_ids — first from the in-memory ``_completed_artifacts`` map
        (fast path) and then via a live ``cache-lookup`` call (recovery path for
        artifacts promoted in previous orchestrator sessions).

        Returns an empty list if the node has no dependencies or if no artifacts
        are found.  Failures are logged as warnings and never abort dispatch.
        """
        depends_on_raw = node_info.get("depends_on", "").strip()
        if not depends_on_raw:
            return []

        parent_path = node_info.get("parent_node_path", "").strip()
        if not parent_path:
            # Derive parent from node_path if not provided
            node_path = node_info.get("node_path", "")
            parent_path = node_path.rsplit("/", 1)[0] if "/" in node_path else ""
        if not parent_path:
            return []

        dep_titles = [t.strip() for t in depends_on_raw.split(",") if t.strip()]
        results: list[tuple[str, str]] = []

        for title in dep_titles:
            # Build the expected sibling node path (slugified title under same parent)
            slug = re.sub(r"[^a-z0-9\-]", "-", title.lower()).strip("-")
            dep_path = f"{parent_path}/{slug}"

            # Fast path: already in our in-memory completed_artifacts map
            artifact_id = self._completed_artifacts.get(dep_path)
            if artifact_id:
                results.append((title, artifact_id))
                continue

            # Recovery path: query the AMS cache directly for artifacts promoted
            # in a prior session (the resolution engine surfaces valid hits).
            db = resolve_plan_db()
            lookup = run_kernel(
                "cache-lookup",
                "--input", db,
                "--tool-id", "swarm-worker:v1",
                "--source-id", dep_path,
            )
            if lookup.returncode == 0:
                for line in lookup.stdout.splitlines():
                    line = line.strip()
                    if line.startswith("artifact_id="):
                        recovered_id = line.split("=", 1)[1]
                        self._completed_artifacts[dep_path] = recovered_id
                        results.append((title, recovered_id))
                        break
                    # Also handle the indented format from cache-lookup
                    if "artifact_id=" in line:
                        parts = line.split()
                        for part in parts:
                            if part.startswith("artifact_id="):
                                recovered_id = part.split("=", 1)[1]
                                self._completed_artifacts[dep_path] = recovered_id
                                results.append((title, recovered_id))
                                break
                        if results and results[-1][0] == title:
                            break

        return results

    def _bootstrap_completed_artifacts(self) -> None:
        """Pre-populate _completed_artifacts from prior-session completions.

        On a fresh orchestrator start (or restart after crash), _completed_artifacts
        is empty.  Nodes that completed in a previous session won't appear in
        ready-nodes, aren't in running_agents, and so the dashboard shows them as
        "blocked" instead of "done".

        Reads the compiled AMS snapshot (.memory.ams.json) directly and extracts
        cache_artifact objects whose invocation_canonical_key matches swarm-worker:v1.
        The key encodes the source_id (node path), so we can re-populate
        _completed_artifacts without any additional subprocess calls.
        """
        import json as _json
        try:
            from ams_common import swarm_plan_snapshot_path
            plan_name = _CURRENT_PLAN_NAME
            if not plan_name:
                return
            snapshot_path = swarm_plan_snapshot_path(plan_name)
            if not Path(snapshot_path).exists():
                return
            with open(snapshot_path, encoding="utf-8") as f:
                snapshot = _json.load(f)
        except Exception as exc:
            self.log(f"[bootstrap-artifacts] Could not read snapshot: {exc}")
            return

        recovered = 0
        for obj in snapshot.get("objects", []):
            if obj.get("objectKind") != "cache_artifact":
                continue
            prov = obj.get("semanticPayload", {}).get("provenance", {})
            key = prov.get("invocation_canonical_key", "")
            artifact_id = prov.get("artifact_id", "") or obj.get("objectId", "")
            # key format: "swarm-worker:v1@<version>:<source_id>"
            if not key.startswith("swarm-worker:v1@"):
                continue
            # Extract source_id after the first ':'  following the version
            at_idx = key.index("@")
            colon_idx = key.index(":", at_idx)
            source_id = key[colon_idx + 1:]
            if source_id and artifact_id and source_id not in self._completed_artifacts:
                self._completed_artifacts[source_id] = artifact_id
                recovered += 1
        if recovered:
            self.log(f"[bootstrap-artifacts] Recovered {recovered} prior-session artifact(s).")

    def _bootstrap_atlas(self) -> None:
        """Register a sprint Atlas at orchestrator startup.

        Derives the project name from the active swarm-plan context root title,
        then calls ``bootstrap_atlas_sprint_page`` to define (or re-use) an
        Atlas keyed on that project.  The resulting slug is stored in
        ``self.sprint_atlas`` so workers can use it for coarse-map navigation.
        """
        context = get_context()
        if not context:
            return
        # Extract root project name from first numbered frame line
        project_name: str | None = None
        for line in context.splitlines():
            line = line.strip()
            if line and line[0].isdigit() and "." in line:
                parts = line.split(".", 1)
                if len(parts) == 2:
                    title_part = parts[1].strip()
                    bracket_idx = title_part.rfind("[")
                    candidate = title_part[:bracket_idx].strip() if bracket_idx > 0 else title_part
                    if candidate:
                        project_name = candidate
                        break  # use the root (first) frame
        if not project_name:
            return
        slug = bootstrap_atlas_sprint_page(project_name)
        if slug:
            self.sprint_atlas = slug
            self.log(f"Sprint atlas registered: '{slug}' (project='{project_name}')")
        else:
            self.log(f"Sprint atlas bootstrap failed for project '{project_name}' — continuing without it")

    def _recover_dangling_frames(self) -> None:
        """Pop any dangling frames left by a previous crashed orchestrator.

        When the orchestrator crashes mid-run, it may leave leaf nodes pushed
        onto the frame stack that are already complete (their work was done but
        the pop never happened). On the next run, get_ready_nodes() shows them
        as ready again but the swarm-plan rejects a second push, causing a stall.

        Detection: if the current active frame's node_path matches a node that
        is NOT in the ready list (i.e. was already completed) and has no running
        worker, pop it automatically.
        """
        for _ in range(10):  # bound loop — at most 10 dangling frames
            ctx = get_context()
            if not ctx:
                break
            # Parse the active node path from context output
            active_line = next(
                (l for l in ctx.splitlines() if l.strip().startswith("active_node_path=")),
                None,
            )
            if not active_line:
                break
            active_path = active_line.split("=", 1)[1].strip()
            if not active_path:
                break
            # If this path is NOT in ready-nodes it's either still pending deps
            # or already completed. Check: does it appear as a ready node?
            ready = get_ready_nodes()
            ready_paths = {n.get("node_path", "") for n in ready}
            if active_path in ready_paths:
                break  # legitimately ready — let the main loop handle it
            # Active frame is not ready (deps unmet or already done). Pop it.
            self.log(f"[recovery] Popping dangling frame: {active_path}")
            try:
                pop_node(f"Auto-recovered dangling frame: {active_path}")
            except RuntimeError:
                break  # can't pop further (e.g. at root)

    # -------------------------------------------------------------------------
    # Orchestrator Self-Healing (orchestrator-self-healing)
    # -------------------------------------------------------------------------

    def _preflight_check_node(self, node: dict, plan_db_path: str) -> list[str]:
        """Run pre-flight validation for a node before dispatching a worker.

        Checks:
        1. Node path exists in the per-plan store (not a ghost reference).
        2. Per-plan JSONL is present and not stale.
        3. No false cache hit is masking this node as already-complete.

        Returns a list of warning strings (empty = all clear).
        Failed checks are also appended to the repair log.
        """
        warnings: list[str] = []
        node_path = node.get("node_path", "")

        # 1. Per-plan JSONL present and not stale
        if _is_plan_db_stale(plan_db_path, max_age_seconds=7200.0):
            w = f"Pre-flight: per-plan store may be stale for node '{node.get('title', node_path)}'"
            warnings.append(w)
            if hasattr(self, "_repair_log"):
                self._repair_log.write(
                    failure_type="stale_store",
                    fix_applied="logged_warning",
                    node_affected=node_path,
                    outcome="warning_only",
                )

        # 2. False cache hit check
        if node_path:
            lookup = run_kernel(
                "cache-lookup",
                "--input", plan_db_path,
                "--tool-id", "swarm-worker:v1",
                "--source-id", node_path,
            )
            if lookup.returncode == 0 and "status=hit" in (lookup.stdout or ""):
                w = (
                    f"Pre-flight: false cache hit detected for '{node.get('title', node_path)}' "
                    f"({node_path}) — node is ready but cache says it's done"
                )
                warnings.append(w)
                if hasattr(self, "_repair_log"):
                    self._repair_log.write(
                        failure_type="false_cache",
                        fix_applied="logged_warning",
                        node_affected=node_path,
                        outcome="warning_only",
                    )

        return warnings

    def _auto_remediate(self, diagnosis: dict, plan_db_path: str) -> bool:
        """Apply an automatic fix for the diagnosed failure mode.

        Handles four failure modes:
        - ghost_node    → advance cursor past the ghost (mark skipped)
        - stale_store   → re-seed from factories/plan source
        - false_cache   → invalidate the false cache entries
        - missing_jsonl → create per-plan store and reload plan

        Returns True if a fix was applied (caller should retry orchestration).
        Updates the repair log with the outcome.
        Escalates to user (logs a clear message) if fix fails or is unknown.
        """
        failure_mode = diagnosis.get("failure_mode", "none")
        affected_nodes = diagnosis.get("affected_nodes", [])
        repair_log: RepairLog | None = getattr(self, "_repair_log", None)

        if failure_mode == "none":
            return False

        self.log(f"[self-healing] Auto-remediating failure mode: {failure_mode}")

        # ----------------------------------------------------------------
        # ghost_node: advance cursor past the ghost
        # ----------------------------------------------------------------
        if failure_mode == "ghost_node":
            for ghost_path in affected_nodes:
                self.log(f"[self-healing] Ghost node detected: {ghost_path} — advancing cursor")
                try:
                    adv = advance()
                    action = adv.get("action", "")
                    if action == "tree-complete":
                        self._tree_complete = True
                    outcome = f"advanced; new_path={adv.get('active_node_path', '(root)')}"
                    self.log(f"[self-healing] Ghost node cleared: {outcome}")
                    if repair_log:
                        repair_log.write(
                            failure_type="ghost_node",
                            fix_applied="advance_cursor",
                            node_affected=ghost_path,
                            outcome="resolved",
                        )
                    return True
                except RuntimeError as exc:
                    self.log(f"[self-healing] Could not advance past ghost node: {exc}")
                    if repair_log:
                        repair_log.write(
                            failure_type="ghost_node",
                            fix_applied="advance_cursor",
                            node_affected=ghost_path,
                            outcome=f"failed: {exc}",
                        )
            return False

        # ----------------------------------------------------------------
        # false_cache: invalidate the false cache entries for affected nodes
        # ----------------------------------------------------------------
        if failure_mode == "false_cache":
            fixed_any = False
            for node_path in affected_nodes:
                self.log(f"[self-healing] Invalidating false cache hit for: {node_path}")
                result = run_kernel(
                    "cache-invalidate",
                    "--input", plan_db_path,
                    "--tool-id", "swarm-worker:v1",
                    "--source-id", node_path,
                    "--actor-id", "orchestrator",
                )
                if result.returncode == 0:
                    self.log(f"[self-healing] Cache invalidated for {node_path}")
                    if repair_log:
                        repair_log.write(
                            failure_type="false_cache",
                            fix_applied="cache_invalidate",
                            node_affected=node_path,
                            outcome="resolved",
                        )
                    fixed_any = True
                else:
                    self.log(f"[self-healing] Cache invalidation failed for {node_path}: {result.stderr.strip()!r}")
                    if repair_log:
                        repair_log.write(
                            failure_type="false_cache",
                            fix_applied="cache_invalidate",
                            node_affected=node_path,
                            outcome=f"failed: {result.stderr.strip()[:200]}",
                        )
            return fixed_any

        # ----------------------------------------------------------------
        # stale_store: re-seed from plan source (load-plan --into-active)
        # ----------------------------------------------------------------
        if failure_mode == "stale_store":
            plan_name = _CURRENT_PLAN_NAME
            if not plan_name:
                self.log("[self-healing] Cannot re-seed stale store: plan name unknown")
                if repair_log:
                    repair_log.write(
                        failure_type="stale_store",
                        fix_applied="none",
                        node_affected=plan_db_path,
                        outcome="escalated: plan name unknown",
                    )
                return False
            # Look for the original plan JSON/YAML in plans/ or scripts/plans/
            candidates = [
                REPO_ROOT / "plans" / f"{plan_name}.json",
                SCRIPT_DIR / "plans" / f"{plan_name}.json",
                REPO_ROOT / "plans" / f"{plan_name}.yaml",
                SCRIPT_DIR / "plans" / f"{plan_name}.yaml",
            ]
            plan_file = next((c for c in candidates if c.exists()), None)
            if plan_file is None:
                self.log(f"[self-healing] Cannot re-seed stale store: no plan file found for '{plan_name}'")
                if repair_log:
                    repair_log.write(
                        failure_type="stale_store",
                        fix_applied="none",
                        node_affected=plan_db_path,
                        outcome="escalated: no plan file found",
                    )
                return False
            self.log(f"[self-healing] Re-seeding stale store from: {plan_file}")
            result = run_ams("swarm-plan", "load-plan", "--file", str(plan_file))
            if result.returncode == 0:
                self.log("[self-healing] Re-seed succeeded — plan store refreshed")
                if repair_log:
                    repair_log.write(
                        failure_type="stale_store",
                        fix_applied=f"load_plan from {plan_file.name}",
                        node_affected=plan_db_path,
                        outcome="resolved",
                    )
                return True
            self.log(f"[self-healing] Re-seed failed: {result.stderr.strip()!r}")
            if repair_log:
                repair_log.write(
                    failure_type="stale_store",
                    fix_applied=f"load_plan from {plan_file.name}",
                    node_affected=plan_db_path,
                    outcome=f"failed: {result.stderr.strip()[:200]}",
                )
            return False

        # ----------------------------------------------------------------
        # missing_jsonl: create empty store and reload plan
        # ----------------------------------------------------------------
        if failure_mode == "missing_jsonl":
            plan_name = _CURRENT_PLAN_NAME
            if not plan_name:
                self.log("[self-healing] Cannot create missing store: plan name unknown")
                if repair_log:
                    repair_log.write(
                        failure_type="missing_jsonl",
                        fix_applied="none",
                        node_affected=plan_db_path,
                        outcome="escalated: plan name unknown",
                    )
                return False
            candidates = [
                REPO_ROOT / "plans" / f"{plan_name}.json",
                SCRIPT_DIR / "plans" / f"{plan_name}.json",
                REPO_ROOT / "plans" / f"{plan_name}.yaml",
                SCRIPT_DIR / "plans" / f"{plan_name}.yaml",
            ]
            plan_file = next((c for c in candidates if c.exists()), None)
            if plan_file is None:
                self.log(f"[self-healing] Cannot create missing store: no plan file found for '{plan_name}'")
                if repair_log:
                    repair_log.write(
                        failure_type="missing_jsonl",
                        fix_applied="none",
                        node_affected=plan_db_path,
                        outcome="escalated: no plan file found",
                    )
                return False
            # Create the store directory if needed
            Path(plan_db_path).parent.mkdir(parents=True, exist_ok=True)
            self.log(f"[self-healing] Creating missing per-plan store and loading: {plan_file}")
            result = run_ams("swarm-plan", "load-plan", "--file", str(plan_file))
            if result.returncode == 0:
                self.log("[self-healing] Missing store created and plan loaded")
                if repair_log:
                    repair_log.write(
                        failure_type="missing_jsonl",
                        fix_applied=f"load_plan from {plan_file.name}",
                        node_affected=plan_db_path,
                        outcome="resolved",
                    )
                return True
            self.log(f"[self-healing] Store creation/load failed: {result.stderr.strip()!r}")
            if repair_log:
                repair_log.write(
                    failure_type="missing_jsonl",
                    fix_applied=f"load_plan from {plan_file.name}",
                    node_affected=plan_db_path,
                    outcome=f"failed: {result.stderr.strip()[:200]}",
                )
            return False

        # Unknown failure mode
        self.log(f"[self-healing] No auto-remediation available for failure mode: {failure_mode}")
        self.log(f"[self-healing] Manual intervention required. Diagnosis: {diagnosis.get('details', '')}")
        if repair_log:
            repair_log.write(
                failure_type=failure_mode,
                fix_applied="none",
                node_affected=str(affected_nodes),
                outcome="escalated: unknown failure mode",
            )
        return False

    def run(self) -> int:
        """Main orchestration loop with parallel dispatch."""
        try:
            return self._run_inner()
        except ModeGateError as e:
            self.log(str(e))
            return 1

    def _refresh_dashboard_tree(self, ready: list[dict]) -> None:
        """Build a node list from known state and push it to the dashboard tree panel."""
        running_titles: set[str] = set(self.running_agents.keys())
        completed_titles: set[str] = {
            path.rsplit("/", 1)[-1]
            for path in self._completed_artifacts
        }
        ready_titles: set[str] = {n.get("title", "") for n in ready}

        # Register newly seen titles
        for title in ready_titles | running_titles | completed_titles:
            if title and title not in self._all_node_titles:
                self._all_node_titles[title] = "pending"

        # Assign current statuses
        nodes: list[dict] = []
        for title in self._all_node_titles:
            if title in completed_titles:
                status = "done"
            elif title in running_titles:
                status = "running"
            elif title in ready_titles:
                status = "ready"
            else:
                status = "blocked"
            nodes.append({"title": title, "status": status})

        self.dashboard.update_tree(nodes)

    def _ensure_execute_mode(self) -> None:
        """If the active plan is in plan_mode=edit, automatically switch to execute."""
        ctx = get_context()
        if ctx and "plan_mode=edit" in ctx:
            self.log("Plan is in plan_mode=edit — auto-switching to execute mode.")
            db = resolve_plan_db()
            result = run_kernel("swarm-plan-enter-execute", "--input", db, "--actor-id", "orchestrator")
            if result.returncode == 0:
                self.log("Switched to plan_mode=execute.")
            else:
                self.log(f"WARNING: enter-execute failed: {result.stderr.strip()}")

    def _run_inner(self) -> int:
        """Inner orchestration loop (called by run())."""
        self.log("Starting parallel plan tree orchestration...")
        # Auto-switch from edit → execute if the plan was freshly loaded.
        self._ensure_execute_mode()
        # Recover from previous crash: pop any dangling frames before dispatching.
        self._recover_dangling_frames()
        # Register GNUISNGNU v0.2 tool identity objects (A1).
        # Stored on self so later cache-promote calls (A3) can reference them.
        self._tool_object_ids: dict[str, str] = self._register_agent_tools()
        # A6: track node_path → artifact_id for completed nodes (resolution-aware refs)
        self._completed_artifacts: dict[str, str] = {}
        self._bootstrap_completed_artifacts()
        self._bootstrap_atlas()
        self._bootstrap_claimed_tasks()
        self._broadcast_system("orchestration-start", "Plan tree execution beginning.")

        # Self-healing: initialize repair log and remediation state
        db_path = resolve_plan_db()
        self._repair_log = RepairLog(db_path)
        self.log(f"[self-healing] Repair log: {self._repair_log.path}")
        self._remediation_attempts = 0  # count auto-repair attempts this session

        no_progress = 0
        dry_run_nodes: list[dict] = []
        dry_run_seen: set[str] = set()  # node_paths already enqueued in dry-run

        for step in range(self.max_steps):
            dispatched = False

            # Check termination: tree complete and no agents running
            if self._tree_complete and not self.running_agents:
                self.log("Plan tree complete (all nodes done).")
                break

            # Poll running agents for completions
            completed = []
            if self.running_agents:
                completed = self.poll_and_advance()
            if self.decompose_lookahead:
                self._poll_lookahead_agents()

            # Re-check after polling
            if self._tree_complete and not self.running_agents:
                self.log("Plan tree complete (all nodes done).")
                break

            # --- Parallel dispatch: find ALL ready nodes and spawn workers for each ---
            ready = get_ready_nodes()
            if not ready and not self.running_agents and not self._lookahead_agents:
                self.log("No ready nodes and no running agents. Plan tree complete.")
                self._tree_complete = True
                break

            # --- Look-ahead decomposition pass ---
            if self.decompose_lookahead and ready:
                self._poll_lookahead_agents()
                self._dispatch_lookahead_decomposers(ready)
                # Re-fetch ready nodes: look-ahead may have injected children
                ready = get_ready_nodes()

            # --- Update dashboard plan tree ---
            self._refresh_dashboard_tree(ready)

            # Phase-4d: enforce adaptive concurrency limit before dispatching
            adaptive_limit = self.perf_tracker.recommended_concurrency
            for node in ready:
                title = node.get("title", "unknown")
                node_path = node.get("node_path", "")

                # Skip if already running
                if title in self.running_agents:
                    continue

                # Skip if look-ahead decomposer is still running on this node
                if node_path in self._lookahead_agents:
                    continue

                # Phase-4d: cap concurrent workers at adaptive limit.
                # Non-worker roles (decomposer, verifier) bypass the cap so
                # lead-engineer analysis can run concurrently with workers.
                node_role_hint = node.get("role", "")
                is_non_worker = node_role_hint and node_role_hint in self.team_models and node_role_hint != "worker"
                if not is_non_worker:
                    worker_count = sum(
                        1 for h in self.running_agents.values() if h.team == "worker"
                    )
                    if worker_count >= adaptive_limit:
                        self.log(
                            f"[adaptive-concurrency] at limit={adaptive_limit} workers "
                            f"({self.perf_tracker.summary()}) — deferring '{title}'"
                        )
                        continue

                # Enrich node_info with context for the worker prompt
                context = get_context() or ""
                node["context"] = context

                # Attach coarse sprint map if an Atlas was registered at startup
                if self.sprint_atlas:
                    node["sprint_map"] = get_sprint_map(self.sprint_atlas, scale=0)

                # A6: inject resolved dependency artifact refs into the worker prompt
                dep_refs = self._resolve_dependency_artifacts(node)
                if dep_refs:
                    node["_dep_artifact_refs"] = dep_refs  # type: ignore[assignment]
                    self.log(f"[artifact-refs] Injecting {len(dep_refs)} dep artifact(s) into '{title}': "
                             + ", ".join(f"{t}={a}" for t, a in dep_refs))

                # Pre-flight checks before dispatching
                if hasattr(self, "_repair_log"):
                    preflight_warnings = self._preflight_check_node(node, db_path)
                    for w in preflight_warnings:
                        self.log(f"[preflight] WARNING: {w}")

                if self.dry_run:
                    if node_path in dry_run_seen:
                        continue
                    dry_run_seen.add(node_path)
                    self.log(f"  [dry-run] Would spawn worker for: {title}")
                    self.log(f"  Observations: {node.get('observations', 'none')[:100]}...")
                    dry_run_nodes.append({
                        "action": "dispatch-worker",
                        "step": step + 1,
                        "node_title": title,
                        "node_path": node_path,
                    })
                    dispatched = True
                    continue

                # Determine team from node role (explicit), stub detection, or default.
                node_role = node.get("role", "")
                if node_role and node_role in self.team_models:
                    # Explicit role from plan YAML — dispatch as that team.
                    team = node_role
                elif self.decompose_lookahead and self._is_stub_node(node):
                    # Auto-detected stub — dispatch as decomposer.
                    node = dict(node, stub_detected="true")
                    team = "decomposer"
                else:
                    team = "worker"
                handle = self._spawn_agent(node, team=team)
                if handle is None:
                    self.log(f"Failed to spawn agent for {title}, will retry")
                else:
                    dispatched = True

            # Track idle iterations to prevent infinite loops.
            # Running agents are not idle — only count iterations where
            # nothing was dispatched, nothing completed, AND no agents
            # are still running.
            if not dispatched and not completed and not self.running_agents:
                no_progress += 1
                if no_progress >= 20:
                    self.log("No progress after 20 idle iterations — breaking.")
                    break
            else:
                no_progress = 0

            # Render dashboard and wait before polling again
            if self.running_agents:
                self.dashboard.render(step=step + 1, max_steps=self.max_steps)
                time.sleep(5.0)

        # Print dry-run results
        for node in dry_run_nodes:
            print(json.dumps(node, indent=2))

        # Zero-step detector: if nothing was dispatched/completed, diagnose and
        # auto-remediate once then retry the full orchestration loop.
        if self.steps_completed == 0 and not self.dry_run and not self._tree_complete:
            MAX_AUTO_REMEDIATION = 2
            self.log(
                f"[self-healing] 0 steps executed — running zero-step diagnosis "
                f"(attempt {self._remediation_attempts + 1}/{MAX_AUTO_REMEDIATION})"
            )
            ready_for_diag = get_ready_nodes()
            diagnosis = _diagnose_zero_steps(db_path, ready_for_diag)
            failure_mode = diagnosis.get("failure_mode", "none")
            self.log(f"[self-healing] Diagnosis: {failure_mode} — {diagnosis.get('details', '')}")

            if failure_mode != "none" and self._remediation_attempts < MAX_AUTO_REMEDIATION:
                self._remediation_attempts += 1
                fixed = self._auto_remediate(diagnosis, db_path)
                if fixed:
                    self.log(f"[self-healing] Remediation applied — retrying orchestration")
                    # Reset loop state and re-enter
                    self._tree_complete = False
                    self.steps_completed = 0
                    no_progress = 0  # would be out of scope here; just log
                    # Re-run the inner loop once more (tail-recursion bounded by _remediation_attempts)
                    return self._run_inner()
                else:
                    self.log(
                        f"[self-healing] Remediation did not fix the issue — "
                        f"escalating to user. Diagnosis: {diagnosis}"
                    )
                    if hasattr(self, "_repair_log"):
                        self._repair_log.write(
                            failure_type=failure_mode,
                            fix_applied="attempted_auto_remediation",
                            node_affected=str(diagnosis.get("affected_nodes", [])),
                            outcome="escalated: fix did not resolve 0-step exit",
                        )
            elif self._remediation_attempts >= MAX_AUTO_REMEDIATION:
                self.log(
                    f"[self-healing] Max auto-remediation attempts ({MAX_AUTO_REMEDIATION}) "
                    f"reached — manual intervention required."
                )
                if hasattr(self, "_repair_log"):
                    self._repair_log.write(
                        failure_type=failure_mode,
                        fix_applied="max_attempts_reached",
                        node_affected=str(diagnosis.get("affected_nodes", [])),
                        outcome="escalated: max attempts",
                    )

        # Wait for remaining agents
        while self.running_agents:
            self.dashboard.render(step=self.max_steps, max_steps=self.max_steps)
            time.sleep(2.0)
            self.poll_and_advance()

        self.dashboard.clear()

        self._broadcast_system("orchestration-complete", f"Plan tree done. Steps: {self.steps_completed}")
        self.log(f"Orchestration complete. Steps executed: {self.steps_completed}")

        # Log repair log summary for TUI visibility
        if hasattr(self, "_repair_log"):
            self.log(f"[self-healing] {self._repair_log.summary()}")

        # Auto-pop the root node so the plan is marked [completed] in swarm-plan list.
        # Only pop if the active node is still the root (node_kind=root) — guards against
        # edge cases where the tree exited mid-flight or the root was already popped.
        try:
            node_info = get_active_node_info()
            if node_info and node_info.get("node_kind") == "root":
                pop_node(f"All nodes complete. Steps executed: {self.steps_completed}")
                self.log("Root node popped — plan marked [completed].")
        except Exception as exc:
            self.log(f"Warning: could not auto-pop root node: {exc}")

        return 0

    def handle_worker_completion(self, return_text: str) -> dict:
        """Called after a worker pops successfully. Advances to next node."""
        self.log(f"Worker completed: {return_text[:80]}")
        result = advance()
        action = result.get("action", "")
        if action == "tree-complete":
            self.log("Plan tree fully executed!")
            return {"status": "tree-complete"}
        new_node = result.get("active_node_path", "")
        self.log(f"Advanced to: {new_node}")
        return {"status": "advanced", "active_node_path": new_node}

    def handle_worker_error(
        self,
        reason: str,
        error_output: str,
        context: str,
        attempted_fix: str,
        repair_hint: str,
    ) -> dict:
        """Called when a worker interrupts for repair. Spawns repairer."""
        self.log(f"Worker hit error: {reason}")
        node_info = get_active_node_info()
        if node_info is None:
            return {"status": "error", "message": "No active node after interrupt"}

        repairer_prompt = format_repairer_prompt(node_info)
        self.log("Dispatching repairer agent...")
        return {
            "status": "repair-dispatched",
            "prompt": repairer_prompt,
        }

    def handle_repair_completion(self, return_text: str) -> dict:
        """Called after repairer pops. Resumes interrupted work, then advances."""
        self.log(f"Repairer finished: {return_text[:80]}")
        if return_text.startswith("REPAIR FAILED:"):
            self.log("Repair failed — manual intervention needed.")
            return {"status": "repair-failed", "message": return_text}

        resume_result = resume_after_repair()
        self.log(f"Resumed: {resume_result.get('active_node_path', '')}")
        return {"status": "resumed", "active_node_path": resume_result.get("active_node_path", "")}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="scripts/orchestrate-plan.py",
        description="Orchestrate AMS swarm-plan plan tree execution via agent swarms.",
    )
    parser.add_argument("--max-steps", type=int, default=200, help="Maximum orchestration steps (each step = 5s poll interval)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be dispatched without executing")
    parser.add_argument(
        "--team-model", action="append", default=[], metavar="TEAM=MODEL",
        help="Override model for a team, e.g. --team-model worker=claude-haiku-4-5-20251001 (repeatable)",
    )
    parser.add_argument(
        "--team-effort", action="append", default=[], metavar="TEAM=EFFORT",
        help="Override effort for a team (low|medium|high|max), e.g. --team-effort verifier=low (repeatable)",
    )
    parser.add_argument(
        "--agent-driver", metavar="TEAM=DRIVER", action="append", default=[],
        help="Override agent driver for a team. E.g. --agent-driver worker=codex",
    )
    sub = parser.add_subparsers(dest="action")

    run_cmd = sub.add_parser("run", help="Run the full parallel orchestration loop")
    run_cmd.add_argument("--plan", default=None, metavar="PLAN_NAME", help="Explicitly target this plan's per-plan store (default: auto-detect active plan)")
    run_cmd.add_argument("--decompose-lookahead", action="store_true", help="Enable look-ahead decomposition: pre-decompose may_decompose=true nodes before dispatching workers")
    run_cmd.add_argument("--dry-run-stubs", action="store_true", help="Print nodes that would be flagged as stubs (structural or semantic) without dispatching any workers, then exit")
    sub.add_parser("status", help="Show current plan tree status")
    sub.add_parser("next", help="Dispatch the next worker")
    sub.add_parser("advance", help="Pop current node and advance (after worker completes)")

    advance_cmd = sub.add_parser("complete-and-advance", help="Pop active node with return text, then advance")
    advance_cmd.add_argument("--return-text", required=True)
    advance_cmd.add_argument("--actor-id", default="orchestrator")

    sub.add_parser("repair-resume", help="Resume after repairer completes")

    return parser.parse_args()


def cmd_status() -> int:
    """Show current plan tree execution status."""
    info = get_active_node_info()
    if info is None:
        print("Plan tree: no active node (complete or not started)")
        return 0
    print(f"Active node: {info.get('title', 'unknown')}")
    print(f"Observations: {info.get('observations', 'none')}")
    print("---")
    print(info.get("context", ""))
    return 0


def cmd_next() -> int:
    """Generate the worker prompt for the current active node."""
    info = get_active_node_info()
    if info is None:
        print("No active node to dispatch.")
        return 1
    prompt = format_worker_prompt(info)
    print(prompt)
    return 0


def cmd_complete_and_advance(return_text: str, actor_id: str) -> int:
    """Pop current node, then advance to next."""
    pop_result = pop_node(return_text, actor_id)
    print(f"Popped: {pop_result.get('completed', '')}")
    adv_result = advance(actor_id)
    action = adv_result.get("action", "")
    if action == "tree-complete":
        print("Plan tree fully executed!")
        return 0
    print(f"Advanced to: {adv_result.get('active_node_path', '')}")
    # Show the new context
    ctx = get_context()
    if ctx:
        print("---")
        print(ctx)
    return 0


def cmd_dry_run_stubs() -> int:
    """Print which ready nodes would be flagged as stubs without dispatching any workers."""
    ready = get_ready_nodes()
    if not ready:
        print("No ready nodes found.")
        return 0

    flagged: list[dict] = []
    clean: list[dict] = []
    for node in ready:
        # Enrich with may_decompose from the node field (already parsed by get_ready_nodes)
        if Orchestrator._is_stub_node(node) or node.get("may_decompose", "false") == "true":
            flagged.append(node)
        else:
            clean.append(node)

    print(f"Stub detection results ({len(ready)} ready nodes):")
    print()
    if flagged:
        print(f"  FLAGGED AS STUBS ({len(flagged)}):")
        for node in flagged:
            title = node.get("title", "<unknown>")
            reason = ""
            if node.get("may_decompose", "false") == "true":
                reason = "explicit may_decompose=true"
            else:
                obs_count = int(node.get("observations_count") or "0")
                t = (node.get("title") or "").strip()
                desc = (node.get("description") or "").strip().lower()
                if len(t) < 60 and obs_count == 0:
                    reason = "structural: short title + no observations"
                else:
                    for prefix in Orchestrator._STUB_SEMANTIC_PREFIXES:
                        if desc.startswith(prefix):
                            reason = f"semantic: description starts with '{prefix}'"
                            break
            print(f"    - {title!r}  [{reason}]")
    else:
        print("  No stubs detected.")
    print()
    if clean:
        print(f"  CLEAN LEAF TASKS ({len(clean)}):")
        for node in clean:
            print(f"    - {node.get('title', '<unknown>')!r}")
    return 0


def cmd_repair_resume() -> int:
    """Resume after repairer pops."""
    result = resume_after_repair()
    print(f"Resumed: {result.get('active_node_path', '')}")
    ctx = get_context()
    if ctx:
        print("---")
        print(ctx)
    return 0


def main() -> int:
    args = parse_args()

    # Parse --team-model and --team-effort overrides
    team_models: dict[str, str] = {}
    for pair in args.team_model:
        if "=" not in pair:
            print(f"Invalid --team-model format (expected TEAM=MODEL): {pair}", file=sys.stderr)
            return 1
        k, v = pair.split("=", 1)
        team_models[k] = v

    team_efforts: dict[str, str] = {}
    for pair in args.team_effort:
        if "=" not in pair:
            print(f"Invalid --team-effort format (expected TEAM=EFFORT): {pair}", file=sys.stderr)
            return 1
        k, v = pair.split("=", 1)
        if v not in ("low", "medium", "high", "max"):
            print(f"Invalid effort level '{v}' — must be low|medium|high|max", file=sys.stderr)
            return 1
        team_efforts[k] = v

    team_drivers: dict[str, str] = {}
    for pair in args.agent_driver:
        if "=" not in pair:
            print(f"Invalid --agent-driver format (expected TEAM=DRIVER): {pair}", file=sys.stderr)
            return 1
        k, v = pair.split("=", 1)
        team_drivers[k] = v

    if args.action == "run":
        plan_name = getattr(args, "plan", None)
        plan_store = resolve_plan_db(plan_name)
        print(f"[orchestrator] using plan store: {plan_store}")
        if getattr(args, "dry_run_stubs", False):
            return cmd_dry_run_stubs()
        orch = Orchestrator(
            max_steps=args.max_steps,
            dry_run=args.dry_run,
            decompose_lookahead=getattr(args, "decompose_lookahead", False),
            team_models=team_models or None,
            team_efforts=team_efforts or None,
            team_drivers=team_drivers or None,
        )
        return orch.run()
    if args.action == "status":
        return cmd_status()
    if args.action == "next":
        return cmd_next()
    if args.action == "complete-and-advance":
        return cmd_complete_and_advance(args.return_text, args.actor_id)
    if args.action == "repair-resume":
        return cmd_repair_resume()

    # Default: run the orchestrator
    plan_name = getattr(args, "plan", None)
    plan_store = resolve_plan_db(plan_name)
    print(f"[orchestrator] using plan store: {plan_store}")
    orch = Orchestrator(
        max_steps=args.max_steps,
        dry_run=args.dry_run,
        heartbeat_interval=args.heartbeat_interval,
        team_models=team_models or None,
        team_efforts=team_efforts or None,
        team_drivers=team_drivers or None,
    )
    return orch.run()


if __name__ == "__main__":
    sys.exit(main())
