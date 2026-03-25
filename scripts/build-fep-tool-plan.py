#!/usr/bin/env python3
"""Build the FEP Tool Usage Learning execution plan as a native AMS callstack tree.

All nodes are created under smartlist/execution-plan/fep-tool-usage-learning.
The first executable leaf (p1a-extend-converter) is set to "active"; everything
else starts as "ready".  Agents traverse the tree via `callstack context`.
"""
from __future__ import annotations

import argparse
import sys

from ams import (
    create_runtime_node,
    load_runtime_snapshot,
    set_root_active_path,
)
from ams_common import corpus_db, rust_backend_env

# ---------------------------------------------------------------------------
# Node specs: (name, description)
# ---------------------------------------------------------------------------

ROOT_DESC = (
    "Close the feedback loop: capture tool usage in AMS, learn patterns via FEP, "
    "detect anomalies, auto-trigger repairer to fix CLAUDE.md/hooks/team definitions. "
    "Dependencies flow linearly: P1 > P2 > P3 > P4 > P5."
)

# Phase 1 -------------------------------------------------------------------
P1_DESC = "Capture tool call metadata from raw JSONL session exports into the AMS object graph."

P1A_DESC = (
    "File: scripts/convert-claude.py. Add extract_tool_events(obj) that walks assistant "
    "message content for type==\"tool_use\" blocks and pairs with type==\"tool_result\" "
    "by tool_use_id. Emit sidecar tool_event records. Truncation: Bash=command(200ch), "
    "Read=file_path, Glob/Grep=pattern+path, Edit/Write=file_path+50ch, "
    "Agent=description+subagent_type. Result preview: 200ch or error message."
)

P1B_DESC = (
    "Files: tools/memoryctl/src/ChatEvent.cs (add ToolCallEvent record), "
    "ChatLog.cs (add TryParseToolEventLine), ChatIngestor.cs (attach tool-call "
    "objects to session containers), src/MemoryGraph.Infrastructure.AMS/"
    "AmsGraphStoreAdapter.cs (add AttachToolCall()). New objectKind: tool-call."
)

P1C_DESC = (
    "Round-trip sample JSONL through converter → verify tool_event records. "
    "Ingest synthetic session → verify tool-call objects in AMS snapshot "
    "linked to correct session container."
)

# Phase 2 -------------------------------------------------------------------
P2_DESC = "Build FEP priors from ingested tool-call objects."

P2A_DESC = (
    "File: rust/ams-core-kernel/src/tool_outcome.rs. Add "
    "classify_agent_tool_outcome(record): is_error=Error, empty/no-match "
    "result=Null, else=Success. Reuses existing ToolOutcome enum and "
    "ToolOutcomeDistribution. Context key = tool_name."
)

P2B_DESC = (
    "File: rust/ams-core-kernel/src/fep_bootstrap.rs. Add "
    "bootstrap_agent_tool_priors(snapshot) — walks tool-call objects, "
    "classifies each, builds per-tool distributions. Store under "
    "fep:agent-tool:{tool_name} in existing priors container."
)

P2C_DESC = (
    "Add fep-bootstrap-agent-tools subcommand to Rust AMS kernel CLI. "
    "Loads snapshot, calls bootstrap, writes priors, reports stats."
)

P2D_DESC = (
    "Unit tests: classify correctness, bootstrap distribution correctness, "
    "free energy higher for unexpected failures."
)

# Phase 3 -------------------------------------------------------------------
P3_DESC = "Detect tool-usage anomalies via FEP free-energy thresholds."

P3A_DESC = (
    "New file: rust/ams-core-kernel/src/tool_anomaly.rs. "
    "detect_tool_anomalies(snapshot, priors, since, threshold) — walks "
    "recent tool-call objects, computes free energy per call against priors, "
    "returns those exceeding threshold (default 2.0)."
)

P3B_DESC = (
    "Write anomalies as SmartList notes under smartlist/fep-tool-anomalies. "
    'Title: "FEP anomaly: {tool_name} {outcome} (FE={fe:.2})". '
    "Structured provenance JSON."
)

P3C_DESC = (
    "Add step in scripts/sync-all-agent-memory.bat after ingest: "
    "ams-core-kernel fep-detect-tool-anomalies --input <db> "
    "--since <last-run> --threshold 2.0"
)

P3D_DESC = (
    "Synthetic tool-call objects with known priors → expected anomalies "
    "detected. Normal calls → no anomalies. SmartList notes created."
)

# Phase 4 -------------------------------------------------------------------
P4_DESC = "Auto-trigger repairer agent when FEP anomalies exceed thresholds."

P4A_DESC = (
    "New file: scripts/fep-repair-trigger.py. Reads unprocessed anomaly "
    "notes from smartlist/fep-tool-anomalies. When FE>3.0 or 3+ same-tool "
    "anomalies: maps tool+failure→repair target, calls callstack interrupt "
    "--policy repair. Marks processed."
)

P4B_DESC = (
    "File: .claude/teams/repairer.yml. Add FEP-anomaly handling: search "
    "memory for tool+error, read target file, fix stale reference, verify "
    "by re-running failing tool call."
)

P4C_DESC = (
    "File: scripts/ams.py in callstack_resume. After completed FEP-triggered "
    "repair, call Rust kernel to shift prior back toward Success."
)

P4D_DESC = (
    "End-to-end: broken CLAUDE.md ref → sessions hit error → anomaly "
    "detected → repair triggered → belief updated."
)

# Phase 5 -------------------------------------------------------------------
P5_DESC = "Continuous online learning: update beliefs after every session."

P5A_DESC = (
    "File: .claude/hooks/session-end-ingest.py. After sync, run "
    "ams-core-kernel fep-update-agent-tool-beliefs --input <db> "
    "--since <session-start>."
)

P5B_DESC = (
    "Apply existing decay_precision from active_inference.rs between "
    "sessions. Prevents stale priors from suppressing new anomalies."
)

# ---------------------------------------------------------------------------
# Tree structure definition
# ---------------------------------------------------------------------------

TREE: list[tuple[str, str, str, list]] = [
    # (name, kind, description, children)
    # Children list uses the same tuple structure recursively.
    ("fep-tool-usage-learning", "work", ROOT_DESC, [
        ("p1-capture-tool-metadata", "work", P1_DESC, [
            ("p1a-extend-converter", "work", P1A_DESC, []),
            ("p1b-extend-csharp-ingest", "work", P1B_DESC, []),
            ("p1c-test-ingest-pipeline", "work", P1C_DESC, []),
        ]),
        ("p2-fep-tool-priors", "work", P2_DESC, [
            ("p2a-classify-agent-tool-outcome", "work", P2A_DESC, []),
            ("p2b-bootstrap-agent-tool-priors", "work", P2B_DESC, []),
            ("p2c-cli-command", "work", P2C_DESC, []),
            ("p2d-test-priors", "work", P2D_DESC, []),
        ]),
        ("p3-anomaly-detection", "work", P3_DESC, [
            ("p3a-tool-anomaly-module", "work", P3A_DESC, []),
            ("p3b-emit-smartlist-notes", "work", P3B_DESC, []),
            ("p3c-wire-post-ingest", "work", P3C_DESC, []),
            ("p3d-test-anomalies", "work", P3D_DESC, []),
        ]),
        ("p4-repair-feedback-loop", "work", P4_DESC, [
            ("p4a-repair-trigger-script", "work", P4A_DESC, []),
            ("p4b-extend-repairer-yml", "work", P4B_DESC, []),
            ("p4c-post-repair-belief-update", "work", P4C_DESC, []),
            ("p4d-test-repair-loop", "work", P4D_DESC, []),
        ]),
        ("p5-continuous-learning", "work", P5_DESC, [
            ("p5a-online-belief-update", "work", P5A_DESC, []),
            ("p5b-precision-decay", "work", P5B_DESC, []),
        ]),
    ]),
]

FIRST_LEAF = "p1a-extend-converter"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="scripts/build-fep-tool-plan.py",
        description="Build the FEP Tool Usage Learning execution plan tree in AMS.",
    )
    parser.add_argument("--corpus", default="all", help="AMS corpus (default: all)")
    parser.add_argument("--actor-id", default="plan-builder", help="Actor ID for node ownership")
    parser.add_argument("--backend-root", default=None, help="Override backend root")
    parser.add_argument("--dry-run", action="store_true", help="Print the tree without creating nodes")
    return parser.parse_args()


def build_tree(
    db_path: str,
    snapshot: dict | None,
    owner: str,
    backend_root: str | None,
    nodes: list[tuple[str, str, str, list]],
    parent_path: str | None,
    root_path: str | None,
    first_leaf_path: list[str],  # mutable out-param: [0] = first leaf path
    dry_run: bool,
    depth: int = 0,
) -> list[str]:
    """Recursively create callstack nodes. Returns list of created node paths."""
    created: list[str] = []
    for name, kind, description, children in nodes:
        is_leaf = len(children) == 0
        is_first_leaf = is_leaf and name == FIRST_LEAF
        state = "active" if is_first_leaf else "ready"

        indent = "  " * depth
        print(f"{indent}{name} [{kind}/{state}]")

        if dry_run:
            # Still recurse to print children
            if children:
                build_tree(
                    db_path, snapshot, owner, backend_root,
                    children, f"(parent:{name})", root_path,
                    first_leaf_path, dry_run, depth + 1,
                )
            continue

        node_path = create_runtime_node(
            db_path,
            snapshot,
            name=name,
            owner=owner,
            kind=kind,
            state=state,
            next_command="callstack pop",
            parent_node_path=parent_path,
            root_path=root_path,
            backend_root=backend_root,
            description=description,
            resume_policy="next-sibling",
        )
        created.append(node_path)

        # Track the root path for children
        effective_root = root_path or node_path

        if is_first_leaf:
            first_leaf_path.append(node_path)

        if children:
            # Reload snapshot so unique_node_path sees newly created nodes
            _, snapshot = load_runtime_snapshot(db_path, backend_root)
            child_paths = build_tree(
                db_path, snapshot, owner, backend_root,
                children, node_path, effective_root,
                first_leaf_path, dry_run, depth + 1,
            )
            created.extend(child_paths)
            # Reload again after children are created
            _, snapshot = load_runtime_snapshot(db_path, backend_root)

    return created


def main() -> int:
    args = parse_args()

    if args.dry_run:
        print("DRY RUN — tree structure:\n")
        build_tree("", None, args.actor_id, None, TREE, None, None, [], True)
        return 0

    db_path = corpus_db(args.corpus)
    _, snapshot = load_runtime_snapshot(db_path, args.backend_root)

    print(f"corpus: {args.corpus}")
    print(f"db_path: {db_path}")
    print(f"actor_id: {args.actor_id}")
    print()

    first_leaf_path: list[str] = []
    created = build_tree(
        db_path, snapshot, args.actor_id, args.backend_root,
        TREE, None, None, first_leaf_path, False,
    )

    root_path = created[0] if created else None

    # Set root's active_node_path to the first leaf
    if root_path and first_leaf_path:
        set_root_active_path(
            db_path, root_path, first_leaf_path[0],
            args.actor_id, args.backend_root,
        )
        print(f"\nroot_path={root_path}")
        print(f"active_node_path={first_leaf_path[0]}")
    else:
        print("\nWARNING: could not set active path", file=sys.stderr)

    print(f"nodes_created={len(created)}")
    print("result=ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
