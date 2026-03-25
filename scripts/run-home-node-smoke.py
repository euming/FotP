#!/usr/bin/env python3
"""Smoke test for home-node assignment (Task 3a1 of LLM Swarm Computer)."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from ams_common import repo_root
from swarm.registry import bootstrap_agent_pool
from swarm.pool import AgentPool, DEFAULT_HOME_PREFIX
from swarm.locality import bootstrap_locality, get_home_node


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main() -> int:
    workspace = Path(tempfile.mkdtemp(prefix="ams-home-node-smoke-"))
    backend_root = workspace / "backend"
    corpus_dir = workspace / "all-agents-sessions"
    backend_root.mkdir(parents=True, exist_ok=True)
    corpus_dir.mkdir(parents=True, exist_ok=True)
    corpus_path = corpus_dir / "all-agents-sessions.memory.jsonl"
    corpus_path.write_text("", encoding="utf-8")

    input_path = str(corpus_path)
    br = str(backend_root)

    print("=== Step 1: Bootstrap pool (4 slots) + locality buckets ===")
    agent_ids = bootstrap_agent_pool(input_path, pool_size=4, backend_root=br)
    require(len(agent_ids) == 4, f"expected 4 agent IDs, got {len(agent_ids)}")
    bootstrap_locality(input_path, backend_root=br)
    print(f"  agents: {agent_ids}")

    pool = AgentPool(input_path, backend_root=br)

    print("\n=== Step 2: Allocate agent with default home node ===")
    slot0 = agent_ids[0]
    allocated = pool.allocate("smartlist/test/task-a", agent_ref=slot0)
    require(allocated == slot0, f"expected {slot0}, got {allocated}")

    expected_home = f"{DEFAULT_HOME_PREFIX}/{slot0}"
    home = pool.home_node(slot0)
    print(f"  home_node({slot0}) = {home}")
    require(home == expected_home, f"expected {expected_home}, got {home}")

    print("\n=== Step 3: Allocate agent with explicit home node ===")
    slot1 = agent_ids[1]
    custom_home = "smartlist/custom/region-west/slot1"
    allocated = pool.allocate("smartlist/test/task-b", agent_ref=slot1, home_node=custom_home)
    require(allocated == slot1, f"expected {slot1}, got {allocated}")

    home = pool.home_node(slot1)
    print(f"  home_node({slot1}) = {home}")
    require(home == custom_home, f"expected {custom_home}, got {home}")

    print("\n=== Step 4: Unallocated agent has no home node ===")
    slot2 = agent_ids[2]
    home = pool.home_node(slot2)
    print(f"  home_node({slot2}) = {home}")
    require(home is None, f"expected None for unallocated agent, got {home}")

    print("\n=== Step 5: Verify via locality module directly ===")
    direct = get_home_node(input_path, slot0, backend_root=br)
    require(direct == expected_home, f"direct lookup mismatch: {direct} != {expected_home}")
    print(f"  direct get_home_node({slot0}) = {direct}  [matches]")

    print("\nresult=ok")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\nFAILED: {e}", file=sys.stderr)
        sys.exit(1)
