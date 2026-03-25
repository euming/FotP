# Agent Instructions: FotP

## Execution Context

Your primary execution context comes from the AMS callstack. Use the AMS wrapper
command that matches your shell. In hooks and generated guidance,
`AMS_MEMORY_CMD` is the override point; if it is unset, the default Windows
form stays `scripts\ams.bat <verb>`. Do not call `python scripts/ams.py`
directly unless you are debugging the wrapper itself.

Shell contract:

- Windows PowerShell or CMD: use `scripts\ams.bat <verb>`
- Bash on Linux, macOS, WSL, or Git Bash on Windows: use `./scripts/ams <verb>`
- Do not run `scripts\ams.bat` from Bash on Windows. It can inherit Git Bash
  path/tooling resolution and break Rust fallback builds.
- On Windows, the wrapper now tries to force Cargo to use the real MSVC
  `link.exe` automatically during Rust fallback builds. If that still fails, do
  the first `cargo build --release` from PowerShell or Developer PowerShell.
- New-repo bootstrap instructions live in `docs/agent-memory-bootstrap.md`.

```powershell
scripts\ams.bat callstack context
```

```bash
./scripts/ams callstack context
```

If the callstack is empty (cold start), bootstrap with the same wrapper command
family for your environment:

```powershell
scripts\ams.bat thread
scripts\ams.bat search "<task keywords>"
scripts\ams.bat callstack push "<task>" --actor-id <your-agent-id>
```

```bash
./scripts/ams thread
./scripts/ams search "<task keywords>"
./scripts/ams callstack push "<task>" --actor-id <your-agent-id>
```

## Memory-First Workflow

Before starting any non-trivial task, query AMS memory first. Do not use
markdown files or prose summaries as the primary recall surface when AMS
retrieval is available.

Always query memory for:

- Bug fixes in familiar subsystems
- Architectural or design decisions
- Requests that reference prior work
- Work on ingest, dreaming, retrieval, memoryctl, viewer, Atlas, or
  agent-memory behavior

Skip the lookup only for: time/date checks, pure formatting, obvious one-shot
shell commands.

After completing non-trivial tasks, explicitly state that memory was checked,
whether relevant hits were found, and whether you drilled down into sessions.

### Retrieval Commands

The Windows and Unix wrapper commands are equivalent. Keep Windows examples
first in shared docs, with the Unix equivalent immediately below.

```powershell
scripts\ams.bat search "<keywords>"
scripts\ams.bat recall "<keywords>"
scripts\ams.bat sessions --n 20
scripts\ams.bat read <guid-prefix>
```

```bash
./scripts/ams search "<keywords>"
./scripts/ams recall "<keywords>"
./scripts/ams sessions --n 20
./scripts/ams read <guid-prefix>
```

Use short, concrete task keywords rather than full prose prompts. The unified
`all-agents-sessions` corpus is the default. Use `--corpus project` only for
focused debugging when the unified corpus is noisy.

### Handoff Retrieval

If a task is being handed off or resumed by a different agent, inspect handoff
memory before proceeding:

```powershell
scripts\ams.bat thread
scripts\ams.bat handoff
```

```bash
./scripts/ams thread
./scripts/ams handoff
```

Treat handoff memory as an early retrieval lane, not an afterthought.

## Planning

Use the AMS callstack, not flat plan files, for all non-trivial planning.

Flat plans lose hierarchy. Work naturally forms a tree: main task -> subtasks
-> tangents -> observations. The callstack preserves this structure, makes it
queryable via `callstack context`, and persists it across sessions so future
agents inherit the full tree of work including interrupts and tangents.

### Workflow

Start a task:

```powershell
scripts\ams.bat callstack push "<task-name>" --description "<plan summary>"
```

```bash
./scripts/ams callstack push "<task-name>" --description "<plan summary>"
```

Start a subtask or tangent:

```powershell
scripts\ams.bat callstack push "<subtask-name>"
```

```bash
./scripts/ams callstack push "<subtask-name>"
```

Record observations or findings:

```powershell
scripts\ams.bat callstack observe --title "<title>" --text "<finding>"
```

```bash
./scripts/ams callstack observe --title "<title>" --text "<finding>"
```

Complete a task:

```powershell
scripts\ams.bat callstack pop --return-text "<result summary>"
```

```bash
./scripts/ams callstack pop --return-text "<result summary>"
```

### Checking Current State

```powershell
scripts\ams.bat callstack context
```

```bash
./scripts/ams callstack context
```

## Compatibility Fallbacks

- `CLAUDE.local.md` and `docs/architecture/active-thread.md` are compatibility
  views, not primary sources.
- `.claude/hooks/` auto-injects callstack context when active and falls back to
  task graph plus search when not.
