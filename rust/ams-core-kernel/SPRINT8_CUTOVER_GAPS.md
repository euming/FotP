# Cutover Status

## Read Path

Read-path cutover is complete for the repo-owned wrapper:

- `scripts\ams.bat search`
- `scripts\ams.bat read`
- `scripts\ams.bat sessions`
- `scripts\ams.bat thread`
- `scripts\ams.bat handoff`

These commands now default to Rust, with automatic C# fallback for resilience.

## Validation Status

Saved real-corpus Rust-vs-C# shadow validation is green on:

- `all`
- `project`
- `claude`
- `codex`

## Retrieval Writeback

Rust route recording is enabled on the Rust `search` path by default. When a
query produces a routable episode, it appends an authoritative mutation to the
Rust route-write log and can mirror to the sibling `*.route-memory.jsonl`
sidecar during migration.

## Remaining Gaps

There are no remaining blocking gaps for the repo retrieval cutover.

What remains is intentional hybrid ownership:

- SmartList/admin mutation commands still run through C#
- maintenance/projection commands still run through C#
- C# remains as wrapper fallback during the Rust soak period

See `RUST_CSHARP_ASYMMETRY_PLAN.md` for the explicit hybrid boundary and the
optional future porting plan.
