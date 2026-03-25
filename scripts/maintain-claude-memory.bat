@echo off
setlocal

:: ============================================================
:: maintain-claude-memory.bat
::
:: Automatically maintain CLAUDE.local.md memory files for every
:: Claude Code project by running the full AMS pipeline:
::
::   1. Convert Claude Code session files to chat_event JSONL
::   2. Ingest into a per-project AMS memory database
::   3. Run the Dreaming pipeline (topic / thread / decision / invariant)
::   4. Write CLAUDE.local.md into each project's repository root
::
:: CLAUDE.local.md is loaded automatically by Claude Code alongside
:: the committed CLAUDE.md, giving the agent synthesized context from
:: past session history without touching version-controlled files.
::
:: Usage:
::   maintain-claude-memory.bat
::   maintain-claude-memory.bat --project AMS
::   maintain-claude-memory.bat --project AMS --dry-run
::   maintain-claude-memory.bat --output-root D:\memory\output
::   maintain-claude-memory.bat --topic-k 8 --thread-k 5 --decision-k 5 --invariant-k 5
::   maintain-claude-memory.bat --projects-dir "%USERPROFILE%\.claude\projects"
::
:: Arguments are passed through unchanged to maintain-claude-memory.py.
:: Run with --dry-run to see what would be generated without writing files.
::
:: Output per project:
::   scripts\output\per-project\<Name>\<Name>.chat.raw.jsonl
::   scripts\output\per-project\<Name>\<Name>.memory.jsonl
::   scripts\output\per-project\<Name>\<Name>.memory.ams.json
::   <repo-root>\CLAUDE.local.md   ← the useful output
::
:: Requirements:
::   - Python 3.10+ on PATH
::   - .NET 8 SDK on PATH
::   - NetworkGraphMemory repo with memoryctl tool
:: ============================================================

set PYTHON=python
set SCRIPT=%~dp0maintain-claude-memory.py

echo.
echo === maintain-claude-memory ===
echo.

%PYTHON% "%SCRIPT%" %*
if errorlevel 1 (
    echo.
    echo ERROR: Pipeline failed. See output above for details.
    exit /b 1
)

echo.
echo === Done ===
echo CLAUDE.local.md has been written into each project repository.
echo Claude Code will load it automatically alongside CLAUDE.md.
echo A timestamped recovery copy is also stored under each repo's memory.archive\claude-local\ folder.
echo.
echo TIP: Add "CLAUDE.local.md" to each repo's .gitignore to keep
echo      auto-generated files out of version control.
echo.
