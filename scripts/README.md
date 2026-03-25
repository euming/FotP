# Memory Scripts - User Manual

This folder contains two related script families:
- Claude maintain flow (per-repo `CLAUDE.local.md` generation)
- Claude/Codex global ingest + dream + HTML browsing flows
- repo browser wrappers, AMS short wrappers, and Codex skill installers

For Codex behavior in this repo, the authoritative runtime instruction file is
the repo-root `AGENTS.md`. Claude-specific files under `CLAUDE*.md` and
`.claude/` remain useful for Claude, but they are not the Codex control plane.

---

## Quick Start: AMS Wrapper

Use the AMS wrapper command for routine agent recall. Hooks and generated guidance resolve that command through `AMS_MEMORY_CMD`; if it is unset, the default Windows form remains `scripts\ams.bat <verb>`. Do not ask users or agents to call `python scripts/ams.py` directly unless debugging the wrapper itself.

Windows:
```powershell
scripts\ams.bat search "memory workflow"
scripts\ams.bat recall "shared swarm smoke handoff"
scripts\ams.bat thread
scripts\ams.bat handoff
scripts\ams.bat sessions --n 10
scripts\ams.bat read 15e406c1
```

Unix/container:
```bash
./scripts/ams search "memory workflow"
./scripts/ams recall "shared swarm smoke handoff"
./scripts/ams thread
./scripts/ams handoff
./scripts/ams sessions --n 10
./scripts/ams read 15e406c1
```

Use `search` for normal front-path retrieval. Use `recall` when you want latent
or background SmartList memory included in the result set. The wrapper also
accepts these `recall` aliases:

- `deep-search`
- `retrieve`
- `latent-search`

The repo-owned read path now uses the Rust AMS kernel by default for:

- `search`
- `read`
- `sessions`
- `thread`
- `handoff`
- `backend` (Rust-only shared backend inspection/recovery)

To force the legacy C# path for comparison or debugging, pass `--engine csharp`
to the relevant command:

```
scripts\ams.bat search "viewer title policy"
scripts\ams.bat sessions --n 10 --engine csharp
scripts\ams.bat search "viewer title policy" --engine csharp
```

If the Rust path errors, the wrapper automatically falls back to the C# engine
and prints a warning.

To point the Rust wrapper at a shared authoritative backend root, pass
`--backend-root <dir>` on wrapper commands that use the Rust path:

```
scripts\ams.bat backend --backend-root C:\ams-shared-backend --assert-clean
scripts\ams.bat thread --backend-root C:\ams-shared-backend
scripts\ams.bat search "memory workflow" --backend-root C:\ams-shared-backend
```

The `backend` command prints the Rust backend target paths and runs recovery
validation against the same shared authority. Use it to confirm that multiple
clients are converging on the same snapshot/log/state contract before broader
cutover.

For the simplest shared-backend concurrency smoke, run:

```
scripts\run-basic-shared-swarm-smoke.bat
scripts\run-basic-shared-swarm-smoke.bat --workspace C:\temp\ams-basic-shared-swarm --keep-workspace
```

This creates one orchestrator root thread plus two worker tangent threads, then
runs the two workers concurrently against the same shared backend root using the
Rust `thread-claim`, `thread-heartbeat`, and `thread-release` flow.

Rust `search` also enables route recording by default on the Rust path. When a
query yields a routable lesson/context episode, it writes to the authoritative
Rust route log (`*.route-write-log.jsonl`) and mirrors to the legacy
`*.route-memory.jsonl` sidecar for comparison during migration. To disable that
for a single query, pass `--no-record-route`.

Use `--corpus project` only when the unified all-agent corpus is noisy and the
task is tightly repo-scoped:

```
scripts\ams.bat search "viewer title policy" --corpus project
```

Install the repo-shipped Codex AMS skill package:

```
scripts\install-codex-ams-skill.bat
```

Restart Codex after installation so the skill is loaded.

For private-repo containers, normalize shell Git before relying on `git pull`.
The workspace UI may have repo access even when in-container Git does not have
credentials or a tracked `main` branch. Use the bootstrap helper to diagnose
that state and, once auth is available, switch cleanly onto `main` while
preserving any unexpected local branch:

Windows:
```powershell
scripts\bootstrap-private-repo.bat --repo-url https://github.com/euming/NetworkGraphMemory
```

Unix/container:
```bash
./scripts/bootstrap-private-repo.sh --repo-url https://github.com/euming/NetworkGraphMemory
```

To inspect shell Git auth without changing branches:

Windows:
```powershell
scripts\bootstrap-private-repo.bat --diagnose-only
```

Unix/container:
```bash
./scripts/bootstrap-private-repo.sh --diagnose-only
```

To harden Claude/Codex memory integrations against environment drift, hooks and generated guidance resolve their command contract through `AMS_MEMORY_CMD`. If unset, they default to `scripts\ams.bat`.

Windows:
```powershell
set AMS_MEMORY_CMD=scripts\ams.bat
python scripts\test-claude-command-contract.py
```

Unix/container:
```bash
export AMS_MEMORY_CMD=./scripts/ams
python scripts/test-claude-command-contract.py
```

This keeps the user-facing contract stable while allowing one override point if the wrapper path ever moves.

Bootstrap another repo with the swarm-plan runtime bundle:

Windows:
```powershell
scripts\init-swarm-project.bat C:\path\to\OtherRepo
```

Unix/container:
```bash
./scripts/init-swarm-project.sh /path/to/OtherRepo
```

This copies the AMS/swarm runtime scaffold into the target repo, generates a repo-local `AGENTS.md`, creates the `shared-memory/` layout, and builds `proj_dir.db` when the target is a Git repository. Re-run with `--force` only when you intentionally want to overwrite conflicting managed files.

The generated repo also includes `docs/agent-memory-bootstrap.md`, which is the
first-run checklist for bringing repo-local AMS stores, KE, `proj_dir.db`, and
the initial Atlas surfaces online in the new project.

Those generated guidance files now come from the static scaffold templates under
`templates/swarm-project/`, not from this repo's live root `AGENTS.md`.

On Windows Git Bash, use `./scripts/ams`, not `scripts\ams.bat`. If the Rust
kernel has not been prebuilt yet, the wrapper now tries to auto-detect the real
MSVC `link.exe` for Cargo fallback builds. If that still fails, do the first
`cargo build --release` from PowerShell or Developer PowerShell so the MSVC
linker wins over Git's `/usr/bin/link.exe`.

---

## Quick Start: Run Everything (Cross-Agent)

```
scripts\sync-all-agent-memory.bat
```

This is the new one-shot command for the shared Claude+Codex corpus. It:

1. Converts Claude + Codex sessions into the unified `all-agents-sessions` raw stream
2. Ingests them into the combined AMS database
3. Rebuilds downstream memory artifacts using the best available path in the current build
4. Runs `agent-maintain` to refresh lesson stereotypes, summaries, and freshness metadata
5. Regenerates embeddings and the HTML memory browser

If the current `memoryctl` build does not provide `dream`, the wrapper reuses the
existing `all-agents-sessions.memory.ams.json` snapshot instead of hard-failing.

Useful variants:

```
scripts\sync-all-agent-memory.bat --no-browser
scripts\sync-all-agent-memory.bat C:\temp\all-agents --no-browser
```

Rust-vs-C# shadow validation is now green on the saved real-corpus cutover set
for `all`, `project`, `claude`, and `codex`. The remaining system-level
asymmetries after the read-path cutover are tracked in
`rust/ams-core-kernel/RUST_CSHARP_ASYMMETRY_PLAN.md`.

Current intentional hybrid boundary:

- Rust owns the repo read/query wrapper path.
- C# still owns SmartList mutation/admin write commands and the broader
  maintenance/projection pipeline.

---

## Quick Start: Always-On Watcher

Run the watcher manually:

```
scripts\watch-all-agent-memory.bat --initial-sync
```

Install the watcher as a Windows logon task:

```
scripts\install-agent-memory-sync-task.bat
```

If Task Scheduler registration is blocked, the installer falls back to a user
Startup-folder launcher and starts the watcher immediately.

Remove it later if needed:

```
scripts\remove-agent-memory-sync-task.bat
```

The watcher polls `%USERPROFILE%\.claude\projects` and `%USERPROFILE%\.codex\sessions`,
waits for changes to settle, then runs `sync-all-agent-memory.bat --no-browser`.

---

## Browser Wrappers

Use the general `agent-browser` wrapper through the repo scripts:

```bat
scripts\agent-browser-wrapper.bat --help
scripts\inspect-latest-ams-page.bat
scripts\inspect-latest-ams-page.bat --snapshot
scripts\inspect-latest-ams-page.bat --annotate
```

Notes:
- the wrapper prefers `npx agent-browser`
- the default AMS page is `output\all-agents-sessions\all-agents-sessions.ams-debug.html`
- local HTML browsing uses `--allow-file-access`

Install the repo-shipped Codex skill package:

```bat
scripts\install-codex-agent-browser-wrapper.bat
```

Restart Codex after installation so the skill is loaded.

---

## Quick Start: Run Everything (Claude)

```
scripts\update-memory.bat
```

This is the main command for routine Claude refresh. It:

1. Deletes stale per-project cursor/database files (unless `--no-wipe`)
2. Re-ingests Claude projects from `%USERPROFILE%\.claude\projects\`
3. Runs Dreaming per project
4. Writes `CLAUDE.local.md` into each project repository root
5. Archives the generated markdown under `memory.archive\claude-local\`
5. Rebuilds the global HTML memory browser

After it finishes, each repo has a fresh `CLAUDE.local.md` and the global
HTML browser (`output/all-claude-projects/all-claude-projects.ams-debug.html`)
is reopened automatically.

---

## Pipeline Overview (Claude Maintain)

```
 Claude Code sessions on disk
 (%USERPROFILE%\.claude\projects\<project>\*.jsonl)
          |
          v
 [1] Convert  -- ingest-all-claude-projects.py / maintain-claude-memory.py
          |      Reads session JSONL, emits chat_event JSONL (raw)
          |      Output: <name>.chat.raw.jsonl
          |
          v
 [2] Ingest -- memoryctl ingest-chatlog
          |    Adds new messages to the AMS memory graph (.memory.jsonl)
          |    Respects a cursor file to skip already-ingested lines
          |    Output: <name>.memory.jsonl + <name>.cursor.json
          |
          v
 [3] Dream  -- memoryctl dream
          |   Clusters messages into topic / thread / decision / invariant nodes
          |   Exports graph snapshot
          |   Output: <name>.memory.ams.json
          |
          v
 [4] Generate -- generate-claude-md.py
               Reads dream winners and session snippets
               Writes CLAUDE.local.md into each repo root
```

---

## Scripts Reference

### `update-memory.bat` (start here for Claude maintain)

Full end-to-end refresh. Deletes per-project databases, then runs
`maintain-claude-memory` (per-project) followed by
`ingest-all-claude-projects` + `dream-all-claude-projects` (global HTML).

```
update-memory.bat
update-memory.bat --no-global-browser
update-memory.bat --project AMS
update-memory.bat --dry-run
update-memory.bat --no-wipe
```

---

### `maintain-claude-memory.bat`

Runs the full per-project pipeline (ingest -> dream -> `CLAUDE.local.md`) for
all Claude projects or a filtered subset. Called by `update-memory.bat`.

```
maintain-claude-memory.bat
maintain-claude-memory.bat --project AMS
maintain-claude-memory.bat --project AMS --dry-run
maintain-claude-memory.bat --topic-k 8 --thread-k 5 --decision-k 5 --invariant-k 5
```

Output per project (inside `scripts/output/per-project/<Name>/`):

| File | Description |
|------|-------------|
| `<Name>.chat.raw.jsonl` | Intermediate chat_event stream |
| `<Name>.memory.jsonl` | AMS memory graph (append-only raw store) |
| `<Name>.cursor.json` | Ingest cursor |
| `<Name>.memory.ams.json` | AMS snapshot after dreaming |
| `<repo-root>/CLAUDE.local.md` | Generated local memory summary |
| `<repo-root>/memory.archive/claude-local/*` | Archived markdown recovery snapshots |

`maintain` is intentionally Claude-only in this repo. A Codex equivalent would
need a defined per-project local summary artifact (name, format, and target
location) before adding `maintain-codex-memory`.

During SmartList-first cutover, treat `CLAUDE.local.md` as a compatibility and
emergency-recovery surface, not the primary runtime memory contract. The
archive copies exist so markdown can be restored manually if AMS retrieval
regresses.

Claude runtime lookup should now prefer the unified AMS wrapper:

```
scripts\ams.bat search "<task keywords>"
```

Use `CLAUDE.local.md` only as emergency fallback when unified retrieval is
unavailable or clearly broken.

Emergency restore:

```
restore-claude-memory.bat C:\path\to\repo --list
restore-claude-memory.bat C:\path\to\repo
restore-claude-memory.bat C:\path\to\repo --name CLAUDE.local.YYYYMMDD-HHMMSS.hash.md
```

---

### `ingest-all-sessions.bat` (generic entrypoint)

Combines sessions into a single AMS database and HTML browser. Does not write
`CLAUDE.local.md` files.

```
ingest-all-sessions.bat
ingest-all-sessions.bat [source]
ingest-all-sessions.bat [source] <output-dir>
ingest-all-sessions.bat [source] <output-dir> <root-dir>
ingest-all-sessions.bat [source] <output-dir> <root-dir> <project-filter>
```

`[source]` values:
- `all` (default) -> merge Claude + Codex into one canonical corpus
- `claude` -> default root `%USERPROFILE%\.claude\projects`, stem `all-claude-projects`
- `codex` -> default root `%USERPROFILE%\.codex\sessions`, stem `all-codex-sessions`

For `source=all`, `root-dir` is the user root that contains both `.claude` and
`.codex` (default `%USERPROFILE%`), and output stem is `all-agents-sessions`.

Output stem depends on source mode:
- All: `all-agents-sessions.*`
- Claude: `all-claude-projects.*`
- Codex: `all-codex-sessions.*`

For agent retrieval, `all-agents-sessions.*` is the default corpus unless you
are debugging a source-specific issue.

---

### Ingest compatibility wrappers

These names are still supported and call `ingest-all-sessions.bat` internally:
- `ingest-all-claude-projects.bat` -> `ingest-all-sessions.bat claude ...`
- `ingest-all-codex.bat` -> `ingest-all-sessions.bat codex ...`

---

### `dream-all-sessions.bat` (generic entrypoint)

Runs Dreaming on a combined database for selected source mode, then regenerates
embeddings and HTML.

```
dream-all-sessions.bat
dream-all-sessions.bat [source]
dream-all-sessions.bat [source] <output-dir>
dream-all-sessions.bat [source] <output-dir> <topic-k> <thread-k> <decision-k> <invariant-k>
dream-all-sessions.bat [source] <output-dir> 5 3 3 3 --dry-run
dream-all-sessions.bat [source] <output-dir> 5 3 3 3 "" <relax-steps>
```

Source behavior:
- `all` (default): mixed-source dream run, LLM title enrichment skipped; deterministic thread-title repair runs instead
- `claude`: keeps optional LLM title enrichment behavior
- `codex`: skips LLM title enrichment by default

Run this after `ingest-all-sessions.bat [source] ...`.

In the current AMS.Core-only build, `memoryctl dream` may be unavailable. When that
happens and an existing `*.memory.ams.json` snapshot is already present, this wrapper
warns and reuses that snapshot so downstream steps can still continue.

`ams-microgpt` / GPT-2 experiments are not part of the runtime AMS pipeline in this
repo. Treat them as offline evaluation infrastructure only.

---

### Dream compatibility wrappers

These names are still supported and call `dream-all-sessions.bat` internally:
- `dream-all-claude-projects.bat` -> `dream-all-sessions.bat claude ...`
- `dream-all-codex-projects.bat` -> `dream-all-sessions.bat codex ...`

---

### `generate-claude-md.py`

Low-level script. Reads a `.memory.ams.json` file and writes
`CLAUDE.local.md`. Called automatically by `maintain-claude-memory.py`.

```
python generate-claude-md.py ^
    --ams-json  scripts/output/per-project/AMS/AMS.memory.ams.json ^
    --project-name AMS ^
    --out-dir C:/Users/eumin/wkspaces/git/AMS

python generate-claude-md.py ... --dry-run
```

---

### `reliability-gates.py`

Runs repeatable reliability gates from `docs/architecture/ams-memory-north-star.md`
against generated corpus artifacts.

```bash
python scripts/reliability-gates.py
python scripts/reliability-gates.py --report-json scripts/output/all-agents-sessions/reliability-gates.report.json
python scripts/reliability-gates.py --skip-recall
```

Default inputs:

| Input | Default path |
|------|---------------|
| chat raw JSONL | `scripts/output/all-agents-sessions/all-agents-sessions.chat.raw.jsonl` |
| AMS snapshot | `scripts/output/all-agents-sessions/all-agents-sessions.memory.ams.json` |
| HTML viewer | `scripts/output/all-agents-sessions/all-agents-sessions.ams-debug.html` |
| embeddings sidecar | `scripts/output/all-agents-sessions/all-agents-sessions.memory.embeddings.json` |
| Recall benchmark | `docs/testing/reliability-query-benchmark.json` |

Seed Recall@5 benchmark cases live in:

`docs/testing/reliability-query-benchmark.json`

Update `expected_ids` intentionally whenever canonical dream IDs change after
validated corpus or algorithm updates.

For `agent_query_*` cases, the gate now executes the real `memoryctl agent-query`
command instead of a Python-side approximation. Benchmark cases may include
context fields such as `current_node`, `parent_node`, `grandparent_node`,
`role`, `mode`, `failure_bucket`, `artifacts`, `traversal_budget`,
`no_active_thread_context`, and `agent_expect_scope_lens`.

---

## Why `update-memory.bat` Wipes the Database Each Run

The raw JSONL is sorted by timestamp, but session UUIDs are random and do not
sort chronologically. Old mixed-order data can merge distinct sessions with
incorrect `started_at` metadata. Wiping and re-ingesting prevents this drift.

Manual equivalent:

```
del scripts\output\per-project\*\*.cursor.json
del scripts\output\per-project\*\*.memory.jsonl
```

Pass `--no-wipe` to skip this step.

---

## Dreaming Parameters

| Parameter | Meaning | Default |
|-----------|---------|---------|
| `--topic-k` | Max recurring themes | 5 |
| `--thread-k` | Max active threads | 3 |
| `--decision-k` | Max recorded decisions | 3 |
| `--invariant-k` | Max stable rules/constraints | 3 |

---

## Adding `CLAUDE.local.md` to `.gitignore`

Add this line to keep generated local summaries out of version control:

```
CLAUDE.local.md
```

---

## Requirements

- Python 3.10+ on `PATH`
- .NET 8+ SDK on `PATH`
- This repository with `tools/memoryctl`
