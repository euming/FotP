# Golden Fixture DBs for `memoryctl` characterization

These fixture DBs are intentionally small and deterministic so characterization tests can lock **current legacy behavior**.

## Files

- `mixed-card-states.memory.jsonl`
  - Covers mixed card states: `Active`, `Tombstoned`, `Retracted`.
  - Includes payloads and taglink metadata for scoring/explain output.
- `duplicate-link-attempts.memory.jsonl`
  - Repeats the same `(card_id, binder_id)` link multiple times (with/without `meta`) to exercise idempotent link behavior.
- `missing-optional-payloads.memory.jsonl`
  - Includes cards with no payload record and payload records missing optional fields (`title`, `text`, `source`, `updated_at`).

## Reuse guidance

- Keep IDs stable; golden output comparisons depend on deterministic IDs/order.
- Append new scenarios as additional fixture files (do not mutate existing fixtures unless intentionally re-baselining goldens).
- Pair each fixture with explicit golden outputs in `../golden/`.

## Regenerating paired goldens

From `tools/memoryctl`:

- `dotnet run -- validate --db tests/fixtures/<fixture>.memory.jsonl`
- `dotnet run -- query --db tests/fixtures/<fixture>.memory.jsonl --q "<query>" --top 10 --explain`
- `dotnet run -- prompt --db tests/fixtures/<fixture>.memory.jsonl --q "<query>" --top 10`
- `dotnet run -- export-graph --db tests/fixtures/duplicate-link-attempts.memory.jsonl --out tests/golden/duplicate-link-attempts.export-graph.json`
