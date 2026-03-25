#!/usr/bin/env python3
"""Entry-point shim for the LLM Swarm Computer orchestrator.

Delegates to run-swarm-plan.py, which contains the full implementation.

Usage:
  python scripts/orchestrate-plan.py run [--max-steps N] [--dry-run]
  python scripts/orchestrate-plan.py status
  python scripts/orchestrate-plan.py next
  python scripts/orchestrate-plan.py complete-and-advance --return-text "..."

See run-swarm-plan.py for full documentation.
"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

if __name__ == "__main__":
    target = Path(__file__).resolve().parent / "run-swarm-plan.py"
    sys.argv[0] = str(target)
    runpy.run_path(str(target), run_name="__main__")
