# Agent Memory Bootstrap

Use this checklist when a repository has been scaffolded for AMS/swarm-plan and
the first agent in that repo needs to bring its memory system online.

This document is bootstrap-focused. For the full command reference, see
`docs/agent-memory-tools.md` if your scaffold includes it.

## 1. Pick the wrapper that matches your shell

Windows PowerShell or CMD:

```powershell
scripts\ams.bat callstack context
```

Bash on Linux, macOS, WSL, or Git Bash on Windows:

```bash
./scripts/ams callstack context
```

Rules:

- On Windows Git Bash, use `./scripts/ams`, not `scripts\ams.bat`.
- Do not mix wrapper forms across shells.
- On Windows, the wrapper now attempts to detect the real MSVC `link.exe` and
  hand it to Cargo automatically, even when the shell is Git Bash.
- If `rust/ams-core-kernel/target/release/ams-core-kernel(.exe)` still does not
  build cleanly, do the first `cargo build --release` from PowerShell or
  Developer PowerShell on Windows.

## 2. Know which stores belong to the repo

Repo-local durable stores:

- `shared-memory/shared.memory.jsonl`
- `shared-memory/system-memory/factories/factories.memory.jsonl`
- `shared-memory/system-memory/ke/ke.memory.jsonl`
- `shared-memory/system-memory/swarm-plans/`
- `proj_dir.db`

Machine-local runtime stores:

- Windows: `%LOCALAPPDATA%/FotP/agent-memory/...`
- Unix: `~/.fotp/agent-memory/...`

Important runtime corpora:

- Unified retrieval corpus: `all-agents-sessions/all-agents-sessions.memory.jsonl`
- Focused project corpus: `per-project/FotP/FotP.memory.jsonl`

`search` and `recall` default to the unified `all-agents-sessions` corpus. Use
`--corpus project` when you want the focused `FotP` corpus.

## 3. If the repo is not scaffolded yet

Run the scaffold command from the template repo:

Windows PowerShell or CMD:

```powershell
scripts\init-swarm-project.bat C:\path\to\TargetRepo
```

Bash on Linux, macOS, WSL, or Git Bash on Windows:

```bash
./scripts/init-swarm-project.sh /path/to/TargetRepo
```

This copies the AMS runtime bundle, generates repo-local guidance files,
creates the `shared-memory/` tree, and builds `proj_dir.db` when the target is
a git repository.

## 4. First-run bootstrap inside the target repo

1. Build the directory index and inspect it.

```powershell
scripts\ams.bat proj-dir build
scripts\ams.bat proj-dir context --depth 2
scripts\ams.bat proj-dir search README
```

```bash
./scripts/ams proj-dir build
./scripts/ams proj-dir context --depth 2
./scripts/ams proj-dir search README
```

2. Prebuild the Rust kernel once if needed.

```powershell
cd rust\ams-core-kernel
cargo build --release
```

```bash
cd rust/ams-core-kernel
cargo build --release
```

3. Bootstrap callstack state for the current task if the stack is cold.

```powershell
scripts\ams.bat thread
scripts\ams.bat handoff
scripts\ams.bat search "<task keywords>"
scripts\ams.bat callstack push "<task>" --description "<plan summary>"
```

```bash
./scripts/ams thread
./scripts/ams handoff
./scripts/ams search "<task keywords>"
./scripts/ams callstack push "<task>" --description "<plan summary>"
```

4. Seed the knowledge-entry store.

```powershell
scripts\ams.bat ke bootstrap
scripts\ams.bat ke write --scope concept:codebase-atlas --kind purpose --summary "Top-level map of FotP" --text "<high-level architecture summary>" --tag atlas --confidence 0.9
```

```bash
./scripts/ams ke bootstrap
./scripts/ams ke write --scope concept:codebase-atlas --kind purpose --summary "Top-level map of FotP" --text "<high-level architecture summary>" --tag atlas --confidence 0.9
```

Minimum expectation:

- Run `ke bootstrap` after `proj_dir.db` exists.
- Add at least one repo-overview entry and tag it with `atlas`.
- Add more `ke write` entries as you verify modules, commands, data models, and
  failure modes.

5. Verify the memory surfaces before doing normal work.

```powershell
scripts\ams.bat callstack context
scripts\ams.bat ke read concept:codebase-atlas
scripts\ams.bat ke search atlas
scripts\ams.bat proj-dir doc README.md
```

```bash
./scripts/ams callstack context
./scripts/ams ke read concept:codebase-atlas
./scripts/ams ke search atlas
./scripts/ams proj-dir doc README.md
```

## 5. Atlas expectations in a fresh repo

In a fresh repo, the first practical Atlas surfaces are:

- `proj_dir.db` for file and markdown navigation
- `ke bootstrap` output for directory-level purpose entries
- manual `ke write --tag atlas` entries for the repo overview and important
  modules

The richer codebase-atlas experience in the template repo is project-specific.
In a new repo, start with `proj-dir` and KE first, then optionally create a
project-specific Atlas source document and ingest script if you want the same
multi-layer map.

## 6. What success looks like

The bootstrap is working when all of the following are true:

- `callstack context` returns without path/tool errors
- `proj-dir build` creates `proj_dir.db`
- `ke bootstrap` writes entries into
  `shared-memory/system-memory/ke/ke.memory.jsonl`
- `ke read concept:codebase-atlas` returns your repo-overview entry
- `proj-dir search` and `proj-dir doc` can navigate the repo

After that point, normal agent work should follow the repo-local `AGENTS.md`
and use AMS memory first instead of ad hoc markdown recall.
