# Rust vs C# AMS Asymmetry Status

## Current State

The repo-owned AMS wrapper now defaults to Rust for the common operator read path:

- `search`
- `read`
- `sessions`
- `thread`
- `handoff`

Real-corpus Rust-vs-C# shadow validation is green on the saved representative
cutover set:

- `all`
- `project`
- `claude`
- `codex`

Rust also enables route recording by default on the Rust `search` path. When a
query produces a routable episode, it writes through the authoritative Rust
route-write service and can mirror to the legacy sibling
`*.route-memory.jsonl` sidecar for migration comparison.

## What Is Complete

### 1. Read-Path Cutover

Rust now owns the repo wrapper for retrieval and inspection reads.

The wrapper still keeps automatic C# fallback so operator workflows do not fail
hard if the Rust path regresses.

### 2. Retrieval-State Writeback Contract

For the repo wrapper, the canonical Rust retrieval-state write contract is:

- query corpus from `<corpus>.memory.jsonl`
- append authoritative mutations to:
  - `<corpus>.route-write-log.jsonl`
  - `<corpus>.route-write-state.json`
- optionally mirror route-memory episodes to the sibling
  `<corpus>.route-memory.jsonl`

This is the active Rust write path for retrieval-side learning when a query
produces a routable episode. The legacy sidecar is now a migration mirror, not
the primary write target.

### 3. Session / Thread / Handoff Inspection Parity

Rust now provides the operator-facing read commands needed by the short wrapper:

- session listing
- session transcript inspection
- task-graph status inspection
- SmartList inspection for handoff reading

## Intentional Remaining Asymmetries

These are no longer "unknown gaps." They are explicit hybrid boundaries.

### 1. Wrapper / Admin Write Cutover

Rust now has authoritative write-service support for:

- `smartlist-create`
- `smartlist-note`
- `smartlist-attach`
- `smartlist-rollup`
- `smartlist-visibility`
- `thread-start`
- `thread-push-tangent`
- `thread-checkpoint`
- `thread-pop`
- `thread-archive`

What still remains hybrid is the wrapper/admin ownership boundary:

- the repo-owned short AMS wrapper has not yet switched these write commands to Rust
- C# still provides the established operator/admin entrypoints for these commands

Reason:

- retrieval/read-path cutover was the first priority
- SmartList and thread writes are now implemented in Rust, but have not yet been
  promoted to the default operator command surface

### 2. Maintenance / Projection Pipeline

C# still owns:

- `agent-maintain`
- `retrieval-graph-materialize`
- lesson synchronization
- SmartList summary generation
- broader maintenance/projection flows under `memoryctl`

Reason:

- the pipeline is operationally stable today
- the user-facing urgency was retrieval cutover, not maintenance replatforming
- there is no current evidence that immediate Rust ownership of this lane is
  required for day-to-day use

### 3. Full Backend Retirement

Rust is the default read path, but C# is still retained as:

- fallback for wrapper resilience
- mutation/admin implementation surface
- maintenance/projection runtime

This is now an intentional hybrid architecture, not an unresolved accident.

## Implementable Next Plan

Only pursue these if there is a concrete payoff:

1. Port SmartList/thread mutation commands to Rust if operator friction appears.
2. Port maintenance/projection steps to Rust only behind parity harnesses.
3. Retire C# fallback only after a longer real-corpus Rust soak.

## Success Criterion

The asymmetry plan is considered complete when:

- the repo-owned default wrapper runs on Rust for daily retrieval/inspection
- Rust retrieval-state writeback has a defined operational contract
- the remaining C# ownership is explicit and intentional
- future work is optional optimization, not a blocking cutover requirement
