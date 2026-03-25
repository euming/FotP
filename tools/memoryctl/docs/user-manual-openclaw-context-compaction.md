# memoryctl User Manual (OpenClaw Context Compaction)

This manual explains the commands available in `memoryctl` for building compact, high-signal memory context **before** sending a user prompt in OpenClaw.

> Audience: operators and automation scripts that prepare retrieval context.

---

## 1) What `memoryctl` does in this workflow

Use `memoryctl` to:

1. Validate and inspect your memory graph database (`memory.jsonl`).
2. Retrieve candidate memory cards related to an incoming prompt.
3. Build compact memAnchor-based context blocks.
4. Log and replay injection decisions for traceability.

The CLI supports both legacy and AMS graph backends, with AMS as the default.

---

## 2) Running the tool

From repo root, run commands like:

```bash
dotnet run --project tools/memoryctl -- <command> [options]
```

Example:

```bash
dotnet run --project tools/memoryctl -- prompt --db /path/to/memory.jsonl --q "cache workaround" --top 12
```

---

## 3) Global options

These can be used with command invocations:

- `--backend <legacy|ams>`
  - Select graph backend.
  - Default is `ams`.
- `--shadow-compare`
  - Runs supported **read-only** commands on both backends and reports diffs.
  - Useful during migration verification.

---

## 4) Command quick reference

## Core retrieval and compaction commands

### `validate`
Checks DB format and parse validity.

```bash
memoryctl validate --db <path>
```

### `query`
Returns scored card hits for a query string.

```bash
memoryctl query --db <path> --q <query> [--top N] [--memAnchor <name>] [--explain]
```

### `prompt`
Builds a prompt-ready memory block from retrieved cards.

```bash
memoryctl prompt --db <path> [--q <query>] [--top N] [--binder <name>[,<name>...]] [--memAnchor <name>[,<name>...]]
```

### `delta`
Builds scoped retrieval context for a channel/chat/query tuple.

```bash
memoryctl delta --db <path> --channel <name> --chat <label-or-id> --q <text> [--registry <path>] [--top N] [--max-chars N] [--tail <path>] [--tail-max-chars N]
```

### `render-memAnchor`
Renders a memAnchor block suitable for injection.

```bash
memoryctl render-memAnchor --db <path> --memAnchor <exact-name> [--max-chars N] [--ids]
```

### `build-inject`
Builds/updates an injection memAnchor plan for a chat/query context.

```bash
memoryctl build-inject --db <path> --channel <name> --chat <label-or-id> [--registry <path>] --q <text> [--top N] [--max-links N] [--per-run]
```

### `inject-plan`
Runs build + log + emit flow for a complete injection plan in one call.

```bash
memoryctl inject-plan --db <path> --channel <name> --chat <label-or-id> [--registry <path>] --q <text> [--top N] [--max-links N] [--per-run] [--max-chars N] [--ledger <path>] [--reason <text>]
```

### `log-injection`
Logs which memAnchor context was injected and why.

```bash
memoryctl log-injection --db <path> --channel <name> --chat <label-or-id> [--registry <path>] --memAnchor <exact-name> [--ledger <path>] [--max-chars N] [--reason <text>]
```

## Graph maintenance and data operations

### `add`
Adds a new card + payload (+ optional memAnchors) to DB.

```bash
memoryctl add --db <path> --title <text> --text <text> [--memAnchor <name>]... [--source <text>] [--key <stable-key>]
```

### `make-memAnchor`
Creates a memAnchor and links selected cards from query results.

```bash
memoryctl make-memAnchor --db <path> --name <memAnchor> --q <text> [--top N] [--memAnchor <filter>] [--relevance <0..1>] [--reason <text>]
```

### `maintain`
Suggests (or applies) memAnchor links for a target card.

```bash
memoryctl maintain --db <path> --card <guid> [--top N] [--apply] [--reason <text>] [--relevance <0..1>]
```

### `list-memanchors`
Lists memAnchor names in DB.

```bash
memoryctl list-memanchors --db <path>
```

### `suggest-memAnchors`
Suggests memAnchor names relevant to a query.

```bash
memoryctl suggest-memAnchors --db <path> --q <query> [--top N]
```

### `export-graph`
Exports graph structure to output path.

```bash
memoryctl export-graph --db <path> --out <path>
```

### `memanchor-page`
Creates an HTML page for a memAnchor.

```bash
memoryctl memanchor-page --db <path> --memAnchor <name> [--out <path>]
```

## Ingestion/transcript/sync commands

### `ingest-chatlog`
```bash
memoryctl ingest-chatlog --db <path> --chatlog <path> --cursor <path> [--max N] [--gap-min N] [--dream]
```

### `append-chat-event`
```bash
memoryctl append-chat-event --chatlog <path> --channel <name> --chat-id <id> --message-id <id> --direction in|out --text <text> [--author <text>] [--ts <rfc3339>]
```

### `build-transcript`
```bash
memoryctl build-transcript --raw-user <path> --raw-llm <path> --out <path> [--md <path>] [--html <path>] --channel <name> --chat-id <id>
```

### `build-transcript-clean`
```bash
memoryctl build-transcript-clean --raw-user <path> --raw-llm <path> --out <path> [--md <path>] [--html <path>] --channel <name> --chat-id <id> --db <path> --deleted <path>
```

### `sync-rawllm`
```bash
memoryctl sync-rawllm --sessions <sessions.json> --raw-llm-dir <dir> --cursor-dir <dir>
```

### `sync-rawuser`
```bash
memoryctl sync-rawuser --sessions <sessions.json> --raw-user-dir <dir> --cursor-dir <dir>
```

### `ingest-systemlogs`
```bash
memoryctl ingest-systemlogs --db <path> --log-dir <dir> --cursor-dir <dir> [--max N]
```

---

## 5) Recommended OpenClaw compaction workflow

For each incoming user prompt:

1. **Retrieve candidates**
   - Use `query` or `delta`.
2. **Build compact block**
   - Use `prompt` for plain text retrieval output, or `render-memAnchor` for a memAnchor block.
3. **Track injection decisions**
   - Use `inject-plan` (single command) or `build-inject` + `log-injection`.

Example (single-shot plan):

```bash
memoryctl inject-plan \
  --db /workspace/memory/memory.jsonl \
  --channel openclaw \
  --chat main \
  --q "User asks about websocket reconnect bug and previous mitigation" \
  --top 12 \
  --max-links 8 \
  --max-chars 5000 \
  --reason "pre-prompt context compaction"
```

---

## 6) Safety and migration tips

- Run `validate` in CI before retrieval/injection jobs.
- Use `--shadow-compare` with read-only commands (`validate`, `query`, `prompt`, `suggest-memAnchors`, `list-memanchors`, `render-memAnchor`, `delta`, `build-inject`) when checking AMS parity.
- Keep `--max-chars` bounded for deterministic prompt budgets.
- Prefer exact memAnchor naming conventions (`Topic: ...`, `Chat: ...`) for stable filters.

---

## 7) Troubleshooting

- **“Missing required option …”**: confirm required flags for the command.
- **No results from retrieval**: increase `--top`, broaden query terms, or verify memAnchor names with `list-memanchors`.
- **Backend mismatch concerns**: re-run with `--shadow-compare` on a read-only command.
- **Parsing/import errors**: run `validate` and ensure format header is first record.

---

## 8) Alias helper (optional)

If you prefer concise calls, define:

```bash
alias memoryctl='dotnet run --project /workspace/NetworkGraphMemory/tools/memoryctl --'
```

Then use command examples exactly as shown in this manual.
