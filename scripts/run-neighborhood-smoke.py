#!/usr/bin/env python3
"""Smoke test for neighborhood reader (Task 3a2 of LLM Swarm Computer)."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from ams_common import repo_root
from swarm.registry import bootstrap_agent_pool
from swarm.pool import AgentPool, DEFAULT_HOME_PREFIX
from swarm.locality import (
    bootstrap_locality,
    assign_home_node,
    get_home_node,
    read_neighborhood,
    _browse_entries,
    _run_kernel,
    ASSIGNMENTS_BUCKET,
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main() -> int:
    workspace = Path(tempfile.mkdtemp(prefix="ams-neighborhood-smoke-"))
    backend_root = workspace / "backend"
    corpus_dir = workspace / "all-agents-sessions"
    backend_root.mkdir(parents=True, exist_ok=True)
    corpus_dir.mkdir(parents=True, exist_ok=True)
    corpus_path = corpus_dir / "all-agents-sessions.memory.jsonl"
    corpus_path.write_text("", encoding="utf-8")

    input_path = str(corpus_path)
    br = str(backend_root)

    print("=== Step 1: Bootstrap pool + locality ===")
    agent_ids = bootstrap_agent_pool(input_path, pool_size=3, backend_root=br)
    require(len(agent_ids) == 3, f"expected 3 agent IDs, got {len(agent_ids)}")
    bootstrap_locality(input_path, backend_root=br)
    print(f"  agents: {agent_ids}")

    # Create a small graph tree: smartlist/test-region with 3 child buckets
    region = "smartlist/test-region"
    child_paths = [f"{region}/zone-a", f"{region}/zone-b", f"{region}/zone-c"]
    for cp in child_paths:
        result = _run_kernel(br, "smartlist-create", "--input", input_path, "--path", cp)
        require(result.returncode == 0, f"failed to create {cp}: {result.stderr}")
    print(f"  created region buckets: {child_paths}")

    # Add a note (observation) to zone-a
    result = _run_kernel(
        br,
        "smartlist-note",
        "--input", input_path,
        "--title", "obs-1",
        "--text", json.dumps({"info": "zone-a observation"}),
        "--buckets", child_paths[0],
        "--note-id", "obs-1",
    )
    require(result.returncode == 0, f"failed to add note to zone-a: {result.stderr}")
    print("  added observation note to zone-a")

    print("\n=== Step 2: Read neighborhood of zone-a ===")
    nbr = read_neighborhood(input_path, child_paths[0], backend_root=br)
    print(f"  node: {nbr['node']}")
    print(f"  parent: {nbr['parent']}")
    print(f"  siblings: {nbr['siblings']}")
    print(f"  children: {nbr['children']}")
    print(f"  observations: {nbr['observations']}")

    require(nbr["node"] == child_paths[0], f"node mismatch: {nbr['node']}")
    require(nbr["parent"] == region, f"parent mismatch: {nbr['parent']}")

    # Siblings should include zone-b and zone-c but not zone-a
    sib_names = [e.get("name") for e in nbr["siblings"]]
    require("zone-b" in sib_names, f"zone-b not in siblings: {sib_names}")
    require("zone-c" in sib_names, f"zone-c not in siblings: {sib_names}")
    require("zone-a" not in sib_names, f"zone-a should not be in its own siblings: {sib_names}")

    # Observations should include our note
    obs_names = [e.get("name") for e in nbr["observations"]]
    require(len(nbr["observations"]) >= 1, f"expected at least 1 observation, got {nbr['observations']}")
    print(f"  observation names: {obs_names}")

    print("\n=== Step 3: Read neighborhood of region (no parent) ===")
    nbr_root = read_neighborhood(input_path, region, backend_root=br)
    print(f"  node: {nbr_root['node']}")
    print(f"  parent: {nbr_root['parent']}")
    print(f"  children count: {len(nbr_root['children'])}")

    # Region has parent "smartlist" but 3 child buckets
    require(nbr_root["parent"] == "smartlist", f"parent mismatch: {nbr_root['parent']}")
    child_names = [e.get("name") for e in nbr_root["children"]]
    require("zone-a" in child_names, f"zone-a not in children: {child_names}")
    require("zone-b" in child_names, f"zone-b not in children: {child_names}")
    require("zone-c" in child_names, f"zone-c not in children: {child_names}")

    print("\n=== Step 4: Neighborhood via agent home node ===")
    pool = AgentPool(input_path, backend_root=br)
    slot0 = agent_ids[0]
    pool.allocate("smartlist/test/task-x", agent_ref=slot0, home_node=child_paths[1])
    home = pool.home_node(slot0)
    require(home == child_paths[1], f"home mismatch: {home}")

    nbr_agent = read_neighborhood(input_path, home, backend_root=br)
    require(nbr_agent["node"] == child_paths[1], f"agent neighborhood node mismatch")
    sib_names_agent = [e.get("name") for e in nbr_agent["siblings"]]
    require("zone-a" in sib_names_agent, f"zone-a not in agent siblings")
    require("zone-c" in sib_names_agent, f"zone-c not in agent siblings")
    print(f"  agent {slot0} home={home}, siblings={sib_names_agent}")

    print("\nresult=ok")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\nFAILED: {e}", file=sys.stderr)
        sys.exit(1)
