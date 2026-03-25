# memoryctl

A small .NET CLI for **portable Card/MemAnchor/TagLink memory files**.

This tool is intended as an immediate, practical way to *use* memory exports:
- validate JSONL files
- query/retrieve relevant Cards for a text prompt
- emit prompt-ready bundles

## Build/run
From this folder:

```bat
cd tools\memoryctl

dotnet run -- validate --db path\to\memory.jsonl

dotnet run -- query --db path\to\memory.jsonl --q "webrtc opus" --top 10

dotnet run -- prompt --db path\to\memory.jsonl --q "surprise policy" --top 20

dotnet run -- prompt --db path\to\memory.jsonl --binder "Topic: roadmap" --top 20

dotnet run -- add --db path\to\memory.jsonl --title "Note" --text "Some text" --memAnchor "Topic: Memory System" --source "manual"

dotnet run -- suggest-memAnchors --db path\to\memory.jsonl --q "webrtc" --top 10

dotnet run -- maintain --db path\to\memory.jsonl --card <guid> --top 10

dotnet run -- memanchor-page --db C:\Users\eumin\.openclaw\workspace\memory\memory_graph\memory.jsonl --memAnchor "Topic: roadmap" --out C:\Users\eumin\.openclaw\workspace\memory\memanchor_pages\Topic_roadmap.html
```

## User manual (OpenClaw context compaction)
- `docs/user-manual-openclaw-context-compaction.md`

## Developer guide (command intent + internals)
- `docs/developer-guide-memoryctl-openclaw.md`

## File format
This tool expects the base `card-memAnchor` JSONL format (v1) and also supports an **optional** payload record:

- `type=format` (required first line)
- `type=card` / `memAnchor` / `taglink`
- `type=card_payload` (optional)

`card_payload` carries the text that makes memory usable immediately:

```json
{"type":"card_payload","card_id":"<guid>","title":"...","text":"...","source":"...","updated_at":"2026-02-02T00:00:00Z"}
```

The payload is stored once per CardId (canonical), regardless of how many memAnchors it belongs to.

## Characterization tests (fixtures vs golden)
From this folder:

```bat
cd tools\memoryctl

:: PowerShell runner (recommended)
powershell -NoProfile -ExecutionPolicy Bypass -File tests\characterization\run-characterization.ps1

:: or cmd shim
tests\characterization\run-characterization.cmd
```

What it does:
- Runs `validate`, `query --explain`, `prompt`, and `export-graph` commands against fixture DBs in `tests/fixtures/`.
- Compares each output against baselines in `tests/golden/`.
- Fails with non-zero exit and unified diff if behavior drifts.

## Command performance regression checks
From repo root:

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File tools/memoryctl/tests/performance/run-command-performance-checks.ps1
```

Details and baseline policy:
- `docs/testing/memoryctl-command-performance-baseline.md`
- `tools/memoryctl/tests/performance/command-performance-baseline.json`

## Design notes
- No NuGet dependencies.
- Uses the `card-memAnchor-core` source files via direct compilation include.
- Retrieval is deterministic v0 (keyword scoring + taglink relevance).
