"""
Ingest the codebase atlas into AMS knowledge entries.

Each section of the atlas becomes a ke entry so agents can navigate
the project graph by scope rather than scanning files.

Usage:
    python scripts/ingest-codebase-atlas.py
"""

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
SCRIPT = str(REPO_ROOT / "scripts" / "ams.bat")


def ke_write(scope: str, kind: str, text: str, summary: str = "", confidence: float = 0.9, tags: list[str] | None = None):
    args = [SCRIPT, "ke", "write", "--scope", scope, "--kind", kind, "--text", text, "--confidence", str(confidence)]
    if summary:
        args += ["--summary", summary]
    for tag in (tags or []):
        args += ["--tag", tag]
    result = subprocess.run(args, capture_output=True, text=True, cwd=str(REPO_ROOT))
    ok = result.returncode == 0
    print(f"  {'OK' if ok else 'FAIL'} {scope} [{kind}]")
    if not ok:
        print(f"    stderr: {result.stderr.strip()[:200]}")
    return ok


ENTRIES = [
    # ── Top-level atlas ────────────────────────────────────────────────────────
    dict(
        scope="concept:codebase-atlas",
        kind="purpose",
        summary="One-sentence synthesis + 10-layer stack overview",
        text=(
            "NetworkGraphMemory is a self-hosting agent memory platform where C# application layers, "
            "a growing Rust kernel, and Python/shell operations work together to turn raw conversations "
            "into ordered AMS memory, project that memory through Atlas/SmartList abstractions, and feed "
            "it back into runtime/swarm execution.\n\n"
            "The repo is structured as 10 logical layers:\n"
            "  1. card-binder-core — portable memory lineage / legacy compatibility\n"
            "  2. src/AMS.Core — atomic in-memory substrate (.NET)\n"
            "  3. src/MemoryGraph.Abstractions — stable interface boundary\n"
            "  4. src/MemoryGraph.Application — memory use-case services\n"
            "  5. src/MemoryGraph.Infrastructure.AMS + Legacy — backend adapters\n"
            "  6. src/MemoryGraph.Serialization — wire-format boundary\n"
            "  7. src/NGM.Runtime.* — swarm/agent runtime MVP\n"
            "  8. tools/memoryctl + ngm-agent — operator and runtime CLIs\n"
            "  9. rust/ams-core-kernel — lower-jitter Rust kernel (strategic read/query path)\n"
            " 10. scripts/ — ingestion, dreaming, orchestration, validation glue\n\n"
            "Strategic stack: portable graph → AMS substrate → retrieval/Atlas views → agent runtime → operational pipelines.\n\n"
            "To drill into any layer query: scripts/ams.bat ke read <scope>, e.g. ke read src/AMS.Core"
        ),
        tags=["atlas", "architecture"],
    ),

    # ── Layer 1: card-binder-core ─────────────────────────────────────────────
    dict(
        scope="card-binder-core",
        kind="purpose",
        summary="Legacy Card/MemAnchor/TagLink types and JSONL compatibility",
        text=(
            "Preserves the original portable memory model. Defines the legacy memory types "
            "(Card, MemAnchor, TagLink) and JSONL handling that newer AMS and memoryctl layers "
            "compile in directly to preserve exact semantics at import/export boundaries. "
            "This is the oldest conceptual layer — not the strategic end-state, but it still "
            "anchors compatibility and parity with existing memory files and upstream tools."
        ),
        tags=["atlas", "legacy"],
    ),

    # ── Layer 2: AMS.Core ─────────────────────────────────────────────────────
    dict(
        scope="src/AMS.Core",
        kind="purpose",
        summary="Core in-memory AMS substrate: objects, containers, SmartList ordering, snapshots",
        text=(
            "Represents memory as an ordered graph substrate rather than flat cards or binder hits. "
            "Key files: AmsStore.cs (objects and containers with doubly-linked LinkNodeRecord memberships "
            "that preserve SmartList ordering), Persistence.cs (deterministic snapshot serialization), "
            "TimeContainerBuilder.cs (year→month→day→minute hierarchy overlay), "
            "Integration.cs (compiles a deterministic prompt/context slice `memory.md`), "
            "Services.cs, FramePlaceEpisode.cs, Disambiguation.cs (ordered frames, referent tracking, dreaming passes). "
            "This is the lowest .NET-native implementation layer beneath all adapters and application services."
        ),
        tags=["atlas", "substrate"],
    ),

    # ── Layer 3: MemoryGraph.Abstractions ─────────────────────────────────────
    dict(
        scope="src/MemoryGraph.Abstractions",
        kind="purpose",
        summary="Stable interface boundary between use-cases and storage engines",
        text=(
            "Prevents the rest of the system from hard-coding against a single storage engine. "
            "IMemoryGraphStore — core card/memAnchor graph API. "
            "IRetrievalGraphStore — adds typed retrieval nodes and edges. "
            "IMemoryQueryEngine, IMemoryMaintenanceService, IMemorySerializer — isolate retrieval, upkeep, and wire-format. "
            "Contracts.cs — all cross-layer DTOs: MemoryQueryContext, retrieval-frame fingerprints, routes, episodes. "
            "RetrievalGraphConventions.cs — standardizes node/edge taxonomy for deterministic retrieval scaffolding."
        ),
        tags=["atlas", "abstractions"],
    ),

    # ── Layer 4: MemoryGraph.Application ─────────────────────────────────────
    dict(
        scope="src/MemoryGraph.Application",
        kind="purpose",
        summary="Reusable memory use-cases: ingest, retrieval, prompt build, route-memory",
        text=(
            "Makes memory operations reusable use-cases rather than CLI-specific code paths. "
            "IngestService — card/payload/memAnchor upserts. "
            "RetrievalService — basic query engine with context-aware boosting from lineage, artifacts, "
            "interfaces, role, mode, and failure bucket. "
            "PromptBuildService — converts retrieval hits or AMS selections into prompt-ready context text. "
            "MaintenanceService — topology hygiene and derived maintenance. "
            "RetrievalGraphService and RouteMemoryService — frame nodes capture retrieval context, "
            "route nodes capture ranking paths, episode nodes record wins/weak/fallback/misses, "
            "later retrievals bias toward historically successful routes. "
            "This is the business-logic layer directly above abstractions and below CLIs/runtime."
        ),
        tags=["atlas", "application"],
    ),

    # ── Layer 5a: Infrastructure.AMS ─────────────────────────────────────────
    dict(
        scope="src/MemoryGraph.Infrastructure.AMS",
        kind="purpose",
        summary="AMS backend adapter: maps IMemoryGraphStore/IRetrievalGraphStore onto AmsStore",
        text=(
            "Makes AMS the canonical backend while preserving older semantics. "
            "Adapts IMemoryGraphStore and IRetrievalGraphStore onto AmsStore. "
            "Encodes cards, memAnchors, sessions, retrieval nodes, and retrieval edges as AMS object/container structures. "
            "Reuses card-binder-core types at the mapping boundary so import/export behavior matches the legacy model. "
            "This is the primary forward-path backend."
        ),
        tags=["atlas", "infrastructure"],
    ),

    # ── Layer 5b: Infrastructure.Legacy ──────────────────────────────────────
    dict(
        scope="src/MemoryGraph.Infrastructure.Legacy",
        kind="purpose",
        summary="Legacy backend adapter kept for rollback parity during AMS migration",
        text=(
            "Keeps a rollback-compatible backend during migration. Wraps older graph behavior with "
            "the same IMemoryGraphStore interface, especially preserving idempotency and insertion-order expectations. "
            "Not the forward path — use AMS adapter instead. Exists as a parity reference and safety net."
        ),
        tags=["atlas", "infrastructure", "legacy"],
    ),

    # ── Layer 6: Serialization ────────────────────────────────────────────────
    dict(
        scope="src/MemoryGraph.Serialization",
        kind="purpose",
        summary="Wire-format isolation: JSONL serializers for legacy and newer formats",
        text=(
            "Isolates wire compatibility from storage evolution. Translates between in-memory graph state "
            "and portable JSONL formats. This is what lets the repo modernize internals without breaking "
            "memory files and fixtures that upstream tools consume. Shared side boundary used by CLIs and tests."
        ),
        tags=["atlas", "serialization"],
    ),

    # ── Layer 7: NGM.Runtime ─────────────────────────────────────────────────
    dict(
        scope="src/NGM.Runtime.Application",
        kind="purpose",
        summary="Agent runtime MVP: session lifecycle, turn coordination, route-memory, tool dispatch",
        text=(
            "Moves from 'memory database + tools' toward an actual agent operating layer. "
            "RuntimeSessionService — persists and restores sessions. "
            "RuntimeConversationCoordinator — runs the turn loop: open planning frame → attach user message → "
            "assemble retrieval context → dispatch tools or emit stub response → record observations → "
            "persist assistant reply → write route-memory episodes → close frame on completion. "
            "RuntimeContextAssembler — retrieved memory, injection policy, retrieval trace, optional route-memory bias. "
            "CallstackPlanningAdapter — shells out to AMS wrapper for planning operations. "
            "TaskExecutionCoordinator — tracks active frame path around planning calls. "
            "CapabilityMirrorService — stores feature equivalence/gap knowledge for comparing NGM with other agent systems. "
            "RuntimeToolCoordinator — bridge to shell execution, memory query, session inspect, spawn, capability lookup. "
            "This is the swarm-facing application layer above MemoryGraph services."
        ),
        tags=["atlas", "runtime", "swarm"],
    ),

    dict(
        scope="src/NGM.Runtime.Abstractions",
        kind="purpose",
        summary="Runtime contract interfaces: planning, retrieval, context assembly, tool recording, orchestration",
        text=(
            "Describes agent-runtime seams. Interfaces for planning, retrieval, context assembly, "
            "tool recording, sessioning, and orchestration. The stable seam between use-cases and runtime implementations."
        ),
        tags=["atlas", "runtime"],
    ),

    # ── Layer 8: CLIs ─────────────────────────────────────────────────────────
    dict(
        scope="tools/memoryctl",
        kind="purpose",
        summary="Main operator/human CLI for memory operations: ingest, query, SmartList, threads, viewer",
        text=(
            "Remains the main human/operator command surface for memory operations. "
            "Program.cs — parses commands and optional backend selection. "
            "CompositionRoot.cs — decides whether commands run against AMS or legacy backend. "
            "Command modules cover: ingest, query, prompt build, transcript handling, SmartLists, threads, "
            "sessions, route replay, bug reports, atlas navigation, and more. "
            "Viewer assets are embedded directly so memory state can be rendered as HTML."
        ),
        tags=["atlas", "cli", "operator"],
    ),

    dict(
        scope="tools/ngm-agent",
        kind="purpose",
        summary="Agent runtime CLI: run, resume, inspect, task spawn over runtime service graph",
        text=(
            "Provides a runtime-facing CLI rather than an operator-maintenance CLI. "
            "Thin commands call the runtime service graph for run, resume, sessions, show-session, "
            "inspect, and future task spawn operations."
        ),
        tags=["atlas", "cli", "runtime"],
    ),

    # ── Layer 9: Rust kernel ──────────────────────────────────────────────────
    dict(
        scope="rust/ams-core-kernel",
        kind="purpose",
        summary="Rust kernel: deterministic read/query path, Atlas, SmartList, callstack, dreaming, FEP",
        text=(
            "Building a lower-jitter kernel and Rust-first read path. Module groups:\n"
            "  substrate: model, store, persistence, invariants, inspect, importer\n"
            "  retrieval: retrieval, agent_query, lesson_retrieval, retrieval_inspect, short_term, "
            "freshness, route_memory, route_replay, search_cache\n"
            "  Atlas/SmartList: smartlist_write, atlas, atlas_multi_scale, projdir, knowledge_entry\n"
            "  dreaming/summarization: dream, dream_cluster, dream_shortcut, dream_generate_md\n"
            "  planning/orchestration: callstack, taskgraph_write, agent_pool, session_gc\n"
            "  diagnosis/policy/FEP: policy, bugreport, tool_anomaly, tool_outcome, fep_bootstrap, "
            "fep_cache_signal, active_inference\n"
            "  migration safety: parity, shadow, corpus_inspect, operator_inspect\n\n"
            "This is the strategic kernel: wider than .NET AMS.Core, handling Atlas and Swarm behaviors too. "
            "Current hybrid boundary: Rust owns most read/query wrapper paths; C# still owns broader mutation/admin and pipeline surfaces."
        ),
        tags=["atlas", "rust", "kernel"],
    ),

    # ── Layer 10: Scripts ─────────────────────────────────────────────────────
    dict(
        scope="scripts",
        kind="purpose",
        summary="Operator automation: ingestion, dreaming, swarm orchestration, validation, Atlas pipeline",
        text=(
            "Keeps the whole memory system continuously fed, refreshed, validated, and inspectable. Script families:\n"
            "  wrapper/contract: scripts/ams, ams.bat, ams.py, ams_common.py — unified command contract for agents/tools\n"
            "  ingest pipeline: ingest-all-claude-projects.py, ingest-all-codex.py, sync-all-agent-memory.sh — "
            "discover raw sessions, normalize to chat-event JSONL, feed memoryctl ingest\n"
            "  dreaming/summarization: embed-dream-cards.py, segment-sprints.py, generate-claude-md.py, build_roadmap.py\n"
            "  swarm/runtime orchestration: orchestrate-plan.py, run-swarm-plan.py, run-callstack-swarm.py, smoke tests\n"
            "  Rust cutover/validation: run-rust-shadow-validation.py, run-basic-shared-swarm-smoke.py\n"
            "  Atlas and browsing: build_proj_dir.py, browser wrappers, inspection helpers\n"
            "  repair/reliability: repair-ams-backing-objects.py, reliability-gates.py, fep-repair-trigger.py, health reports"
        ),
        tags=["atlas", "scripts", "operations"],
    ),

    # ── Other folders ─────────────────────────────────────────────────────────
    dict(
        scope="shared-memory",
        kind="purpose",
        summary="Repo-owned AMS data: shared corpora, SmartList notes, execution-plan state, architecture snapshots",
        text=(
            "Persists the project's own memory substrate and SmartList state — effectively the repo's "
            "self-hosting data layer. Stores shared corpora, SmartList/execution-plan artifacts, and "
            "architecture memory snapshots that demonstrate how the platform wants to be used on itself. "
            "Per-plan stores live in shared-memory/system-memory/swarm-plans/. "
            "The factories store (shared-memory/system-memory/factories/) holds SmartList templates ONLY."
        ),
        tags=["atlas", "data"],
    ),

    dict(
        scope="docs",
        kind="purpose",
        summary="Human control plane: north-star docs, target architecture, runtime MVP, migration plans",
        text=(
            "Defines intent and keeps the moving architecture legible. Not secondary — in this repo, "
            "docs actively govern design: north-star layering, runtime MVP ownership, database topology, "
            "retrieval diagnosis, invariants, and migration stages all live here. "
            "Key files: docs/architecture/ngm-north-star.md, target-architecture.md, ngm-runtime-mvp.md, "
            "agent-memory-tools.md. If you want to know whether a subsystem is accidental or deliberate, "
            "the answer is usually in docs/architecture/."
        ),
        tags=["atlas", "docs"],
    ),

    dict(
        scope="tests",
        kind="purpose",
        summary="Quality gates: unit, parity, serialization, characterization, smoke, shadow validation tests",
        text=(
            "Enforces behavioral contracts across all layers. Unit, parity, serialization, command, "
            "runtime, and agent tests. Parity and migration correctness are first-class concerns here — "
            "the test stack exists specifically to keep the multi-path migration (legacy → AMS → Rust) honest."
        ),
        tags=["atlas", "tests"],
    ),

    # ── Cross-cutting concerns ────────────────────────────────────────────────
    dict(
        scope="concept:migration-parity",
        kind="decision",
        summary="Three-path migration: legacy → C# AMS → Rust kernel; duplication is intentional scaffolding",
        text=(
            "The codebase is in a deliberate migration state. Many concerns exist in three forms:\n"
            "  1. Legacy-compatible path — still matters for corpus/fixtures/operator expectations\n"
            "  2. C# AMS path — canonical substrate-backed implementation in the managed stack\n"
            "  3. Rust cutover path — increasingly preferred for read/query and orchestration-intensive surfaces\n\n"
            "That duplication is architectural scaffolding, not random drift. "
            "Tests and shadow validation are what keep the migration honest. "
            "When you see three implementations of the same concern, look for the Rust path as the strategic direction."
        ),
        tags=["atlas", "architecture", "migration"],
    ),

    dict(
        scope="concept:atlas-middle-layer",
        kind="decision",
        summary="Atlas/SmartList is the key middle abstraction between storage and agents",
        text=(
            "The most important conceptual through-line is not 'storage' or 'agents'; it is the "
            "Atlas/SmartList middle layer between them. It addresses:\n"
            "  multi-scale navigation, short-term vs. durable visibility, rollups and projection,\n"
            "  failure-taxonomy crosscuts, capability mirroring, scoped retrieval surfaces, context compression.\n\n"
            "This middle layer appears as: C# retrieval-graph DTOs and route-memory records, "
            "Rust SmartList write APIs and atlas modules, shared-memory SmartList snapshots, "
            "project-directory atlas indexing (projdir/proj_dir.db), "
            "and docs that explicitly name NGM → Atlas/SmartList → Swarm as the canonical stack."
        ),
        tags=["atlas", "architecture"],
    ),

    dict(
        scope="concept:human-legibility-vs-determinism",
        kind="decision",
        summary="Dual goal: human-legible contextual memory AND deterministic inspectable substrate",
        text=(
            "The repo consistently pursues two goals simultaneously: "
            "memory should feel human-legible and contextually coherent; "
            "substrate operations should be deterministic, inspectable, and testable. "
            "Evidence: SmartList ordering and linked-node invariants, prompt/context compilation, "
            "traceable route-memory episodes, operator inspect tools, atlas views and rollups, "
            "parity and characterization tests. "
            "The system is trying to be both a memory substrate and a debugger-friendly machine."
        ),
        tags=["atlas", "architecture"],
    ),

    dict(
        scope="concept:ops-first-culture",
        kind="decision",
        summary="Research + product hybrid: docs govern design, extensive smoke tests, practical agent wrappers",
        text=(
            "This repo mixes product code with research notebooks and rollout scaffolding. "
            "Signals: architecture notes describing current chasms and future layers, "
            "extensive smoke tests and shadow validation, scripts for embedding/dreaming/browsing, "
            "plans for FEP/anomaly detection/locality/swarm messaging, "
            "practical wrappers so agents use the same stable contract while internals evolve. "
            "'Where it is' in the stack is often dual: one part is a shipping interface, another is a proving ground."
        ),
        tags=["atlas", "culture"],
    ),

    # ── Data lifecycle pipelines ──────────────────────────────────────────────
    dict(
        scope="concept:session-to-memory-pipeline",
        kind="data-model",
        summary="Raw transcripts → chat_event JSONL → memoryctl ingest → AMS memory → dream/HTML artifacts",
        text=(
            "Session-to-memory pipeline:\n"
            "  1. Session logs discovered from tool-specific folders (Claude/Codex)\n"
            "  2. Conversion scripts normalize to chat_event JSONL\n"
            "  3. memoryctl ingest materializes into AMS-backed memory files\n"
            "  4. Dream/maintain passes derive summaries, freshness, stereotypes, report artifacts\n"
            "  5. HTML/browser outputs and agent-facing markdown (CLAUDE.local.md) regenerated\n\n"
            "Lives in: scripts/, tools/memoryctl/, src/MemoryGraph.Application/, src/MemoryGraph.Infrastructure.AMS/"
        ),
        tags=["atlas", "pipeline"],
    ),

    dict(
        scope="concept:retrieval-pipeline",
        kind="data-model",
        summary="Query + context signals → boosted hits → route-memory bias → inspectable retrieval trace",
        text=(
            "Retrieval pipeline (avoiding flat keyword retrieval; context-routed recall):\n"
            "  1. Request provides query text plus optional context signals\n"
            "  2. RetrievalService builds effective query and base hits\n"
            "  3. Context boosts cards connected to lineage, artifacts, interfaces, failure buckets\n"
            "  4. RouteMemoryService optionally biases toward prior successful retrieval episodes\n"
            "  5. RuntimeContextAssembler formats a trace so the retrieval decision remains inspectable\n\n"
            "Lives in: src/MemoryGraph.Application/, src/NGM.Runtime.Application/, rust/ams-core-kernel/"
        ),
        tags=["atlas", "pipeline", "retrieval"],
    ),

    dict(
        scope="concept:planning-swarm-pipeline",
        kind="data-model",
        summary="AMS callstack/swarm-plan → bounded agent frames → observations → Rust agent-pool/messaging",
        text=(
            "Planning/swarm pipeline (agents work in bounded scopes instead of giant prompts):\n"
            "  1. AMS wrapper exposes callstack/swarm-plan commands\n"
            "  2. Runtime services use commands to open frames, record observations, close frames\n"
            "  3. Rust modules provide deeper execution-plan, ready-node, agent-pool, locality, messaging\n"
            "  4. Smoke scripts validate the full path under shared backend conditions\n\n"
            "Lives in: scripts/ams*, src/NGM.Runtime.Application/, rust/ams-core-kernel/"
        ),
        tags=["atlas", "pipeline", "swarm"],
    ),

    dict(
        scope="concept:atlas-projection-pipeline",
        kind="data-model",
        summary="SmartLists + rollups + projdir SQLite → multi-scale Atlas views → HTML browser artifacts",
        text=(
            "Atlas/projection pipeline (views at multiple scales, not forced through one retrieval granularity):\n"
            "  1. SmartLists and buckets define the navigable hierarchy\n"
            "  2. Rollups summarize lower-level detail\n"
            "  3. Atlas objects define scale levels and navigation surfaces\n"
            "  4. projdir indexes the repo tree into searchable/projectable SQLite (proj_dir.db)\n"
            "  5. Viewer commands and browser artifacts turn memory graph into human-browsable pages\n\n"
            "Lives in: rust/ams-core-kernel (atlas, atlas_multi_scale, smartlist_write, projdir), "
            "tools/memoryctl atlas commands, and projection scripts"
        ),
        tags=["atlas", "pipeline", "atlas"],
    ),

    # ── Tech stack ────────────────────────────────────────────────────────────
    dict(
        scope="concept:tech-stack",
        kind="decision",
        summary="C# for application/control, Rust for substrate/query kernel, Python/shell for pipelines",
        text=(
            "Tech stack:\n"
            "  C# / .NET 8-9 — main application/control implementation. Strong existing codebase, "
            "CLI ergonomics, clean interface layering, legacy compatibility.\n"
            "  Rust — strategic substrate/query/orchestration kernel. Deterministic performance, "
            "safer concurrency, lower-level control for SmartLists, retrieval, and swarm plumbing.\n"
            "  Python + shell — fast scripting for ingestion, orchestration, maintenance, validation.\n"
            "  Data formats: JSONL, AMS snapshot JSON, HTML artifacts, SQLite (proj_dir.db).\n"
            "  Test stack: xUnit, characterization fixtures, smoke scripts, shadow validation.\n\n"
            "Practical interpretation: C# is still the main application; Rust is the strategic kernel; "
            "Python/shell keep the operational loops moving while the core evolves."
        ),
        tags=["atlas", "tech-stack"],
    ),

    # ── Navigation guide ──────────────────────────────────────────────────────
    dict(
        scope="concept:atlas-navigation",
        kind="prerequisites",
        summary="How to navigate the project graph via ke: start at concept:codebase-atlas, then drill by scope",
        text=(
            "To navigate the project using this knowledge graph:\n\n"
            "  1. Start with overview:  scripts/ams.bat ke read concept:codebase-atlas\n"
            "  2. Drill into a layer:   scripts/ams.bat ke read src/AMS.Core\n"
            "  3. Cross-cutting topics: scripts/ams.bat ke read concept:migration-parity\n"
            "                           scripts/ams.bat ke read concept:atlas-middle-layer\n"
            "                           scripts/ams.bat ke read concept:tech-stack\n"
            "  4. Data pipelines:       scripts/ams.bat ke read concept:retrieval-pipeline\n"
            "                           scripts/ams.bat ke read concept:session-to-memory-pipeline\n"
            "                           scripts/ams.bat ke read concept:planning-swarm-pipeline\n"
            "  5. Search by tag:        scripts/ams.bat ke search atlas\n\n"
            "Available layer scopes (ke read <scope>):\n"
            "  card-binder-core, src/AMS.Core, src/MemoryGraph.Abstractions, src/MemoryGraph.Application,\n"
            "  src/MemoryGraph.Infrastructure.AMS, src/MemoryGraph.Infrastructure.Legacy,\n"
            "  src/MemoryGraph.Serialization, src/NGM.Runtime.Abstractions, src/NGM.Runtime.Application,\n"
            "  tools/memoryctl, tools/ngm-agent, rust/ams-core-kernel, scripts, shared-memory, docs, tests\n\n"
            "Do NOT glob/grep the entire repo to understand project structure — use this knowledge graph instead."
        ),
        tags=["atlas", "navigation"],
    ),
]


def main():
    print(f"Writing {len(ENTRIES)} knowledge entries to AMS...\n")
    ok_count = 0
    fail_count = 0
    for entry in ENTRIES:
        tags = entry.pop("tags", [])
        success = ke_write(**entry, tags=tags)
        if success:
            ok_count += 1
        else:
            fail_count += 1

    print(f"\nDone: {ok_count} written, {fail_count} failed")
    if fail_count:
        sys.exit(1)


if __name__ == "__main__":
    main()
