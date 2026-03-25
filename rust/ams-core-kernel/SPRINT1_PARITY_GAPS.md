# Sprint 1 Parity Gaps

Sprint 1 proves the Rust kernel can import and validate the core AMS substrate, but it does not yet provide full AMS memory retrieval parity.

Completed in Sprint 1:

- substrate records for objects, containers, and link nodes
- deterministic snapshot serialize and deserialize
- import of real C# `.ams.json` snapshots and sibling resolution from `.memory.jsonl`
- append-only mutation log replay
- invariant validation
- kernel stress command

Explicitly not complete in Sprint 1:

- kernel read and inspect surface beyond summary validation
- corpus import and materialization for retrieval workloads
- text and token retrieval
- SmartList scoped retrieval behavior
- active-thread lineage-aware context
- route-memory bias and ranking parity with `agent-query`
- service or API cutover for AMS retrieval

Known kernel-level limitations still acceptable at Sprint 1 close:

- replay parity is proven for representative snapshot fixtures, not yet for large real corpus snapshots
- mutation log coverage is focused on substrate state, not higher-layer derived structures
- stress verification is structural, not performance-benchmarked against the C# runtime
