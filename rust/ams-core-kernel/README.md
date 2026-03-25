# ams-core-kernel

First Rust milestone for `AMS.Core`.

Scope:

- `ObjectRecord`, `ContainerRecord`, `LinkNodeRecord`
- O(1)-style linked-list mutations for SmartLists
- deterministic snapshot serialization and roundtrip
- append-only mutation log replay
- invariant validation
- corpus materialization for legacy `.memory.jsonl` cards, binders, taglinks, and payloads
- a thin CLI/importer boundary

Non-goals for this crate:

- segmentation
- time hierarchy builders
- frame/place/episode factories
- disambiguation ledgers
- dreaming or other derived structure builders

Example commands:

```powershell
cargo run -- validate-snapshot --input path\to\snapshot.ams.json
cargo run -- validate-snapshot --input path\to\store.memory.jsonl
cargo run -- roundtrip-snapshot --input path\to\snapshot.ams.json --output out.json
cargo run -- list-objects --input path\to\snapshot.ams.json
cargo run -- list-containers --input path\to\snapshot.ams.json
cargo run -- list-link-nodes --input path\to\snapshot.ams.json --container-id ctr:ordered
cargo run -- show-container --input path\to\snapshot.ams.json --id ctr:ordered --direction both
cargo run -- memberships --input path\to\snapshot.ams.json --object-id obj:a
cargo run -- snapshot-diff --left left.ams.json --right right.ams.json
cargo run -- replay-log --input path\to\mutations.jsonl --output out.json
cargo run -- stress --iterations 2000 --output stress.json
cargo run -- corpus-summary --input path\to\store.memory.jsonl
cargo run -- list-cards --input path\to\store.memory.jsonl --state active
cargo run -- show-card --input path\to\store.memory.jsonl --id 11111111-1111-1111-1111-111111111111
cargo run -- list-binders --input path\to\store.memory.jsonl --contains search
cargo run -- show-binder --input path\to\store.memory.jsonl --id aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
cargo run -- query-cards --input path\to\store.memory.jsonl --q "search cache" --top 5 --explain
cargo run -- query-cards --input path\to\store.memory.jsonl --q "local retrieval scope contract" --current-node child-thread --parent-node parent-thread --role architect --mode design --artifact src/MemoryGraph.Application/RetrievalService.cs --explain
```

Replay route-memory episodes against a live query or an offline replay file:

```powershell
cargo run -- query-cards --input path\to\store.memory.jsonl --q "retrieval cache contract" --current-node child-thread --role implementer --mode build --route-replay path\to\route-replay.jsonl --explain
cargo run -- route-replay --input path\to\store.memory.jsonl --replay path\to\route-replay.jsonl --out path\to\route-replay.results.jsonl --top 5
```

Use the Rust compatibility-facing retrieval surface:

```powershell
cargo run -- agent-query --input path\to\store.memory.jsonl --q "search cache" --top 8 --explain
cargo run -- agent-query --input path\to\store.memory.jsonl --q "retrieval cache contract" --current-node child-thread --role implementer --mode build --route-replay path\to\route-replay.jsonl --json
cargo run -- agent-query --input path\to\store.memory.jsonl --q "search cache" --current-node child-thread --role implementer --mode build --no-active-thread-context --record-route --explain
```

`agent-query --record-route` appends a sibling `*.route-memory.jsonl` sidecar next to the input corpus.
Subsequent `agent-query` runs auto-load that sidecar and reuse persisted route-memory episodes.

Run the parity/cutover validator against fixture cases:

```powershell
cargo run -- parity-validate --input ..\..\tools\memoryctl\tests\fixtures\mixed-card-states.memory.jsonl --cases tests\fixtures\agent-query-parity-cases.jsonl --out parity-report.jsonl
```

Run a Rust-vs-C# shadow comparison on AMS-backed corpora:

```powershell
cargo run -- shadow-validate --input path\to\all-agents-sessions.memory.jsonl --cases tests\fixtures\agent-query-parity-cases.jsonl --out shadow-report.jsonl
cargo run -- shadow-validate --input path\to\all-agents-sessions.memory.jsonl --cases tests\fixtures\agent-query-parity-cases.jsonl --out shadow-report.jsonl --assert-match
```

`shadow-validate` shells out to the existing C# `agent-query` path and writes JSONL diff reports.
Fixture-only `.memory.jsonl` files that are not valid AMS-backed C# stores will be reported as unsupported on the C# side instead of crashing the run.
With `--assert-match`, the command exits non-zero if any shadow case differs.

See `SPRINT1_PARITY_GAPS.md` for the explicit boundaries and remaining work after the first kernel-parity sprint.
See `SPRINT8_CUTOVER_GAPS.md` for the remaining blockers before Rust can replace the default C# retrieval path.
