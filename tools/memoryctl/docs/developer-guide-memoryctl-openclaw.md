# memoryctl Developer Guide (OpenClaw Memory Compaction + Retrieval)

This guide is for developers extending or integrating `memoryctl` into OpenClaw workflows.

It explains each command at two levels:

1. **High-level intent**: what this command is meant to do.
2. **Low-level mechanics**: what code path and data operations it performs.

For each command, we also include the **inciting purpose**: the practical compaction/retrieval problem it addresses.

---

## System context: why these commands exist

OpenClaw memory is a graph of:

- **Cards**: atomic memory units.
- **MemAnchors**: named groupings/scopes (topic, channel, chat, run, etc.).
- **TagLinks**: weighted links between cards and memAnchors.
- **Payloads**: title/text/source metadata that powers retrieval.

In practice, OpenClaw has two recurring problems:

- **Retrieval precision problem**: too many cards can match loosely, which dilutes prompt quality.
- **Compaction budget problem**: useful context can exceed LLM token budgets unless condensed/scoped.

`memoryctl` commands are organized around solving these two problems while preserving traceability.

---

## Runtime architecture (how commands execute)

Most graph-oriented commands route through a runtime composed of:

- `MemoryJsonlReader.Load` to parse `memory.jsonl`.
- `ICommandRuntimeFactory` (`AmsCommandRuntimeFactory` by default, `LegacyCommandRuntimeFactory` fallback).
- `IngestService` for card/memAnchor/link writes.
- `RetrievalService` with `LegacyScoringQueryEngine` scoring.

CLI dispatch is centralized in `Program.ExecuteCommand`, with optional `--backend` selection and `--shadow-compare` for read-only parity checks (`legacy` vs `ams`).

---

## Command reference: intent + internals + practical problem

## Core retrieval and compaction commands

### `validate`
- **Inciting purpose**: stop bad memory files from polluting retrieval/injection runs.
- **High-level intent**: quickly verify DB parseability and format compatibility.
- **Low-level mechanics**: calls `MemoryJsonlReader.Load(dbPath)` and prints `OK` on success; no graph mutation.

### `query`
- **Inciting purpose**: identify the strongest candidate cards for a user query before prompt assembly.
- **High-level intent**: ranked retrieval with optional memAnchor scoping and explainability.
- **Low-level mechanics**:
  - Loads runtime + graph/payload dictionaries.
  - Optional `--memAnchor` produces candidate card set by case-insensitive **contains** match on memAnchor names.
  - Uses `RetrievalService.Query(...)` over all cards, then filters/takes `top`.
  - Scoring comes from `LegacyScoringQueryEngine`: token overlap in payload text/title, memAnchor names, and link relevance metadata.
  - Prints score summary and optional per-component explain fields.

### `prompt`
- **Inciting purpose**: transform retrieval hits into prompt-injectable context blocks under budget pressure.
- **High-level intent**: emit deterministic, readable memory sections for downstream LLM calls.
- **Low-level mechanics**:
  - Uses retrieval pipeline similar to `query`.
  - `--binder/--memAnchor` filters are exact-name (case-insensitive) match sets.
  - Renders either:
    - default `MEMORY (retrieved cards)` format with numbered entries and memAnchor labels, or
    - roadmap-specialized markdown buckets when `Topic: roadmap` is among filters.

### `delta`
- **Inciting purpose**: avoid broad/global retrieval by anchoring context to a channel + chat + current query.
- **High-level intent**: produce scoped, compact “delta context” for the current conversation turn.
- **Low-level mechanics**: delegates to `DeltaContext.Build(...)` with channel/chat/query/top/max-chars/tail options and prints non-empty result.

### `render-memAnchor`
- **Inciting purpose**: reuse a curated memAnchor as a pre-assembled context block.
- **High-level intent**: serialize one memAnchor’s card set into injection text.
- **Low-level mechanics**: calls `BinderRenderer.Render(...)` with max-char and include-id options; outputs exactly the rendered text.

### `build-inject`
- **Inciting purpose**: create a reproducible “injection memAnchor” for one retrieval plan.
- **High-level intent**: materialize a memAnchor representing what should be injected now.
- **Low-level mechanics**:
  - Calls `InjectBinderBuilder.Build(...)`.
  - Build inputs combine delta options (channel/chat/query/top) with planner options (`max-links`, relevance, reason, optional per-run naming).
  - Prints resulting memAnchor name when created/resolved.

### `inject-plan`
- **Inciting purpose**: reduce operator error by bundling plan build + ledger logging + emitted block in one call.
- **High-level intent**: single-shot command for production injection workflow.
- **Low-level mechanics**:
  1. Builds/updates inject memAnchor via `InjectBinderBuilder.Build(...)`.
  2. Renders it via `BinderRenderer.Render(...)`.
  3. Logs injection metadata with `LogInjection(...)` and `InjectionLedger.Append(...)`.
  4. Emits final rendered block to stdout.

### `log-injection`
- **Inciting purpose**: preserve traceability (“what context was injected, when, and why”).
- **High-level intent**: write auditable ledger entries without rebuilding plans.
- **Low-level mechanics**:
  - Renders target memAnchor.
  - Resolves referenced card IDs with `InjectionLedger.CollectCardIdsForBinder(...)`.
  - Stores timestamp, chat/channel metadata, rendered length/hash, reason via `InjectionLedger.Append(...)`.

---

## Graph maintenance and authoring commands

### `add`
- **Inciting purpose**: append new memory quickly from runtime signals or operator notes.
- **High-level intent**: create card + payload + memAnchor links (defaulting to `Conversations`).
- **Low-level mechanics**:
  - Resolves memAnchor IDs from existing DB or allocates new ones.
  - Generates GUID (`--key` => deterministic via `GuidUtil.FromKey("card:" + key)`).
  - If DB exists, updates in-memory graph via `IngestService` first.
  - Persists append-only JSONL records via `MemoryJsonlWriter.AppendCard(...)`.

### `make-memAnchor`
- **Inciting purpose**: when recurring retrieval subsets are discovered, persist them as reusable scopes.
- **High-level intent**: create/link a memAnchor from current query-selected cards.
- **Low-level mechanics**:
  - Retrieves top candidate cards (optionally pre-filtered by memAnchor contains match).
  - Resolves/creates target memAnchor.
  - Appends taglinks for each selected card with supplied relevance/reason.

### `maintain`
- **Inciting purpose**: stale or sparse memAnchor linkage hurts future retrieval quality.
- **High-level intent**: suggest and optionally apply memAnchor links inferred from related cards.
- **Low-level mechanics**:
  - Validates seed card exists and has payload.
  - Uses `Maintenance.FindRelatedCards(...)` + `Maintenance.SuggestBindersFromRelated(...)`.
  - Dry run prints related/suggested lists.
  - `--apply` links new suggestions through `IngestService.LinkCardToMemAnchor(...)` and appends links to JSONL.

### `list-memanchors`
- **Inciting purpose**: discover available retrieval scopes quickly.
- **High-level intent**: list deduplicated memAnchor names.
- **Low-level mechanics**: loads DB, collects memAnchor names, distinct+sorts case-insensitively, prints one per line.

### `suggest-memAnchors`
- **Inciting purpose**: query author often needs likely scoping names but does not remember exact memAnchor taxonomy.
- **High-level intent**: suggest memAnchor names relevant to a free-text query.
- **Low-level mechanics**: tokenizes query, scores each memAnchor name by token contains hits, prints top names with simple score.

### `export-graph`
- **Inciting purpose**: retrieval debugging requires visual/structured inspection of graph topology.
- **High-level intent**: export cards/memAnchors/taglinks as graph JSON.
- **Low-level mechanics**:
  - Loads all nodes/edges.
  - De-duplicates memAnchor nodes by name for view clarity.
  - Writes pretty JSON to `--out` and creates output directory if needed.

### `memanchor-page`
- **Inciting purpose**: give developers/operators human-readable snapshots of a memAnchor cluster.
- **High-level intent**: emit simple HTML page for one memAnchor and its cards.
- **Low-level mechanics**:
  - Resolves memAnchor by exact case-insensitive name.
  - Enumerates cards + payload-derived summary/status/area.
  - Generates cross-links to sibling memAnchor pages.
  - Writes to requested path or default `memanchor_pages` location.

---

## Ingestion, transcript, and sync commands

### `ingest-chatlog`
- **Inciting purpose**: raw conversation events need to become retrievable cards continuously.
- **High-level intent**: incremental chatlog-to-memory ingestion with cursor tracking.
- **Low-level mechanics**:
  - Ensures DB header exists.
  - Calls `ChatIngestor.Ingest(...)` with max/gap/dream options.
  - Optionally runs maintenance-like enrichment (`--dream`).
  - Prints ingestion counts and created card IDs.

### `append-chat-event`
- **Inciting purpose**: standardize event capture and prevent duplicate log rows.
- **High-level intent**: append one chat event to chatlog with dedupe behavior.
- **Low-level mechanics**: constructs `ChatEvent`, appends through `ChatLogWriter.AppendChatEvent(...)`, prints `APPENDED` or `SKIPPED_DUPLICATE`.

### `build-transcript`
- **Inciting purpose**: retrieval/compaction flows need merged chat artifacts from raw user + assistant streams.
- **High-level intent**: generate transcript JSONL and optional markdown/html for a channel/chat.
- **Low-level mechanics**: calls `TranscriptBuilder.Build(...)` with include options; writes output paths and prints them.

### `build-transcript-clean`
- **Inciting purpose**: downstream analysis may require excluding deleted/retracted content.
- **High-level intent**: transcript build with deletion-aware filtering.
- **Low-level mechanics**:
  - Loads deleted index + DB.
  - Builds normalized exclusion set via `DeletedIndex.BuildExcludedTextSet(...)`.
  - Calls `TranscriptBuilder.Build(...)` with exclusion set.

### `sync-rawllm`
- **Inciting purpose**: assistant-side history from session exports must stay synchronized for transcript/retrieval jobs.
- **High-level intent**: incremental sync of assistant responses into raw LLM logs.
- **Low-level mechanics**: runs `SessionLlmSync.SyncTelegramSessionsToRawLlm(...)` and prints session/message/append/dupe counters.

### `sync-rawuser`
- **Inciting purpose**: user-side history needs parity with assistant logs for complete transcripts.
- **High-level intent**: incremental sync of user messages into raw user logs.
- **Low-level mechanics**: runs `SessionUserSync.SyncTelegramSessionsToRawUser(...)` and prints counters.

### `ingest-systemlogs`
- **Inciting purpose**: operational logs often contain root-cause memory that should become retrievable context.
- **High-level intent**: ingest system log entries as memory cards incrementally.
- **Low-level mechanics**: calls `SystemLogIngestor.Ingest(...)` with cursoring and max-entry cap; prints file/read/new/card counts.

---

## Recommended developer workflow (compaction-focused)

1. Validate memory DB (`validate`).
2. Retrieve candidates (`query` or `delta`).
3. Build compact block (`prompt` or `render-memAnchor`).
4. Use one-shot plan (`inject-plan`) in production-like paths.
5. Improve future precision (`maintain --apply`, `make-memAnchor`) when repeated misses appear.

This loop addresses both immediate token-budget pressure (compaction) and medium-term retrieval quality drift (link maintenance).

---

## Backend strategy notes

- Default backend is `ams`; legacy remains rollback-only.
- Use `--shadow-compare` on safe read-only commands (`validate`, `query`, `prompt`, `suggest-memAnchors`, `list-memanchors`, `render-memAnchor`, `delta`, `build-inject`) when validating parity.

