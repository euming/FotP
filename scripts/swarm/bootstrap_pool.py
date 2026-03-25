#!/usr/bin/env python3
"""Bootstrap the agent pool with N agent slots."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from swarm.registry import bootstrap_agent_pool


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bootstrap the agent pool with N agent slots.",
    )
    parser.add_argument("--input", required=True, help="Path to the corpus .memory.jsonl file")
    parser.add_argument("--pool-size", type=int, default=8, help="Number of agent slots (default: 8)")
    parser.add_argument("--backend-root", default=None, help="Optional backend root directory")
    args = parser.parse_args()

    ids = bootstrap_agent_pool(args.input, args.pool_size, args.backend_root)
    for agent_id in ids:
        print(f"created={agent_id}")
    print(f"pool_size={len(ids)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
