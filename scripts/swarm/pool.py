from __future__ import annotations

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ams_common import build_rust_ams_cmd, rust_backend_env

from .selection import RoundRobinSelector
from .locality import assign_home_node, get_home_node as _get_home_node

DEFAULT_HOME_PREFIX = "smartlist/agent-pool/home"


def _run_kernel(backend_root: str | None, *args: str) -> subprocess.CompletedProcess[str]:
    cmd = build_rust_ams_cmd(*args)
    if cmd is None:
        raise RuntimeError("unable to locate the Rust AMS kernel binary or Cargo project")
    return subprocess.run(
        cmd,
        env=rust_backend_env(backend_root),
        text=True,
        capture_output=True,
        check=False,
    )


def _parse_kv(stdout: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


class AgentPool:
    def __init__(self, input_path: str, backend_root: str | None = None):
        self.input_path = input_path
        self.backend_root = backend_root
        self._selector = RoundRobinSelector()

    def allocate(
        self,
        task_path: str,
        agent_ref: str | None = None,
        home_node: str | None = None,
    ) -> str | None:
        """Allocate an agent to a task and assign it a home node.

        If *agent_ref* is None, the round-robin selector picks one.
        If *home_node* is None, defaults to ``{DEFAULT_HOME_PREFIX}/{agent_ref}``.
        """
        if agent_ref is None:
            free = self.free_agents()
            agent_ref = self._selector.select(free, {})
        if agent_ref is None:
            return None

        result = _run_kernel(
            self.backend_root,
            "agent-pool-allocate",
            "--input", self.input_path,
            "--agent-ref", agent_ref,
            "--task-path", task_path,
        )
        if result.returncode != 0:
            raise RuntimeError(f"agent-pool-allocate failed: {result.stderr}")

        # Assign home node in the locality graph
        if home_node is None:
            home_node = f"{DEFAULT_HOME_PREFIX}/{agent_ref}"
        assign_home_node(self.input_path, agent_ref, home_node, self.backend_root)

        return agent_ref

    def home_node(self, agent_ref: str) -> str | None:
        """Return the home-node path for *agent_ref*, or None if unassigned."""
        return _get_home_node(self.input_path, agent_ref, self.backend_root)

    def release(self, agent_ref: str, task_path: str) -> None:
        """Release an agent back to the free pool."""
        result = _run_kernel(
            self.backend_root,
            "agent-pool-release",
            "--input", self.input_path,
            "--agent-ref", agent_ref,
            "--task-path", task_path,
        )
        if result.returncode != 0:
            raise RuntimeError(f"agent-pool-release failed: {result.stderr}")

    def status(self) -> dict:
        """Call agent-pool-status, parse kv output."""
        result = _run_kernel(
            self.backend_root,
            "agent-pool-status",
            "--input", self.input_path,
        )
        if result.returncode != 0:
            raise RuntimeError(f"agent-pool-status failed: {result.stderr}")
        return _parse_kv(result.stdout)

    def free_agents(self) -> list[str]:
        """Return list of free agent refs from status."""
        st = self.status()
        agents_str = st.get("free_agents", "")
        if not agents_str:
            return []
        return [a.strip() for a in agents_str.split(",") if a.strip()]
