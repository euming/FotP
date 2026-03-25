#!/usr/bin/env bash
# sync-all-agent-memory.sh
#
# Unix/Linux/container equivalent of sync-all-agent-memory.bat
# Ingests and dreams all agent sessions, producing:
#   all-agents-sessions.memory.jsonl  (and downstream AMS artifacts)
#
# Usage:
#   ./scripts/sync-all-agent-memory.sh [output-dir] [OPTIONS]
#   bash ./scripts/sync-all-agent-memory.sh [output-dir] [OPTIONS]
#
# Options:
#   --claude-root <path>   Claude projects dir (default: $CLAUDE_SESSIONS_ROOT or ~/.claude/projects)
#   --codex-root  <path>   Codex sessions dir  (default: $CODEX_SESSIONS_ROOT  or ~/.codex/sessions)
#   --raw         <path>   Skip discovery; use this .chat.raw.jsonl file directly
#   --no-browser           Suppress opening the final HTML window (default on Linux)
#   --help / -h            Show this help
#
# Environment overrides:
#   AMS_OUTPUT_ROOT        Override default output directory root
#   CLAUDE_SESSIONS_ROOT   Override default Claude projects directory
#   CODEX_SESSIONS_ROOT    Override default Codex sessions directory

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_NAME="$(basename "$REPO_ROOT")"
PROJECT_SLUG="$(printf '%s' "$PROJECT_NAME" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9._-]/-/g; s/^[.-]*//; s/[.-]*$//')"
if [ -z "$PROJECT_SLUG" ]; then
    PROJECT_SLUG="ams"
fi

# --- Resolve output dir ---
if [ -n "${AMS_OUTPUT_ROOT:-}" ]; then
    DEFAULT_OUTDIR="$AMS_OUTPUT_ROOT/all-agents-sessions"
else
    DEFAULT_OUTDIR="$HOME/.${PROJECT_SLUG}/agent-memory/all-agents-sessions"
fi

OUTDIR="$DEFAULT_OUTDIR"
NO_BROWSER=1  # default to no-browser on Linux/container
RAW_OVERRIDE=""
CLAUDE_ROOT="${CLAUDE_SESSIONS_ROOT:-${HOME}/.claude/projects}"
CODEX_ROOT="${CODEX_SESSIONS_ROOT:-${HOME}/.codex/sessions}"

# --- Parse args ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-browser)
            NO_BROWSER=1
            shift
            ;;
        --claude-root)
            CLAUDE_ROOT="${2:?--claude-root requires a path argument}"
            shift 2
            ;;
        --codex-root)
            CODEX_ROOT="${2:?--codex-root requires a path argument}"
            shift 2
            ;;
        --raw)
            RAW_OVERRIDE="${2:?--raw requires a path argument}"
            shift 2
            ;;
        --help|-h)
            sed -n '2,/^[^#]/{ /^#/{ s/^# \?//; p }; /^[^#]/q }' "$0"
            exit 0
            ;;
        --*)
            echo "ERROR: Unknown option \"$1\"." >&2
            exit 1
            ;;
        *)
            OUTDIR="$1"
            shift
            ;;
    esac
done

# Strip trailing slash
OUTDIR="${OUTDIR%/}"
mkdir -p "$OUTDIR"

STEM="all-agents-sessions"
DB="$OUTDIR/$STEM.memory.jsonl"
LOCKDIR="$OUTDIR/.sync.lock"

# --- Lock ---
if ! mkdir "$LOCKDIR" 2>/dev/null; then
    echo "Sync already running for $OUTDIR - skipping."
    exit 0
fi
cleanup() { rmdir "$LOCKDIR" 2>/dev/null || true; }
trap cleanup EXIT

export AMS_SKIP_HTML=1
export AMS_NO_BROWSER=$NO_BROWSER

echo ""
echo "=== Sync All Agent Memory ==="
echo "OutDir      : $OUTDIR"
echo "Claude root : $CLAUDE_ROOT"
echo "Codex root  : $CODEX_ROOT"
if [ -n "$RAW_OVERRIDE" ]; then
    echo "Raw input   : $RAW_OVERRIDE (skipping discovery)"
fi
echo ""

# --- Locate MemoryCtl ---
MEMORYCTL_EXE="$REPO_ROOT/tools/memoryctl/bin/Release/net9.0/MemoryCtl"
if [ ! -f "$MEMORYCTL_EXE" ]; then
    MEMORYCTL_EXE="$REPO_ROOT/tools/memoryctl/bin/Debug/net9.0/MemoryCtl"
fi
MEMORYCTL_CSPROJ="$REPO_ROOT/tools/memoryctl/MemoryCtl.csproj"

run_memoryctl() {
    if [ -f "$MEMORYCTL_EXE" ]; then
        "$MEMORYCTL_EXE" "$@"
    elif [ -f "$MEMORYCTL_CSPROJ" ]; then
        dotnet run --project "$MEMORYCTL_CSPROJ" -- "$@"
    else
        echo "ERROR: MemoryCtl not found. Build it with: cd tools/memoryctl && dotnet build" >&2
        return 1
    fi
}

# --- Locate ams-core-kernel ---
AMS_KERNEL="$REPO_ROOT/rust/ams-core-kernel/target/release/ams-core-kernel"
if [ ! -f "$AMS_KERNEL" ]; then
    AMS_KERNEL="$REPO_ROOT/rust/ams-core-kernel/target/debug/ams-core-kernel"
fi

RAW="$OUTDIR/$STEM.chat.raw.jsonl"
CURSOR="$OUTDIR/$STEM.cursor.json"

# --- Step 1: Ingest (or use --raw bypass) ---
if [ -n "$RAW_OVERRIDE" ]; then
    echo "[1/3] Using provided raw input (skipping discovery)..."
    if [ ! -f "$RAW_OVERRIDE" ]; then
        echo "ERROR: --raw file not found: $RAW_OVERRIDE" >&2
        exit 1
    fi
    cp "$RAW_OVERRIDE" "$RAW"
else
    echo "[1/3] Discovering and converting sessions (source=all)..."
    RAW_CLAUDE="$OUTDIR/$STEM.claude.chat.raw.jsonl"
    RAW_CODEX="$OUTDIR/$STEM.codex.chat.raw.jsonl"

    HAVE_CLAUDE=0
    HAVE_CODEX=0

    if [ -d "$CLAUDE_ROOT" ]; then
        echo "  [claude] root: $CLAUDE_ROOT"
        python3 "$SCRIPT_DIR/ingest-all-claude-projects.py" \
            --projects-dir "$CLAUDE_ROOT" \
            --out "$RAW_CLAUDE" && HAVE_CLAUDE=1 \
            || echo "  [claude] WARNING: conversion failed - skipping claude input."
    else
        echo "  [claude] WARNING: root not found - skipping: $CLAUDE_ROOT"
    fi

    if [ -d "$CODEX_ROOT" ]; then
        echo "  [codex] root: $CODEX_ROOT"
        python3 "$SCRIPT_DIR/ingest-all-codex.py" \
            --sessions-dir "$CODEX_ROOT" \
            --out "$RAW_CODEX" && HAVE_CODEX=1 \
            || echo "  [codex] WARNING: conversion failed - skipping codex input."
    else
        echo "  [codex] WARNING: root not found - skipping: $CODEX_ROOT"
    fi

    # Merge raw inputs
    if [ "$HAVE_CLAUDE" -eq 1 ] && [ "$HAVE_CODEX" -eq 1 ]; then
        cat "$RAW_CLAUDE" "$RAW_CODEX" > "$RAW"
    elif [ "$HAVE_CLAUDE" -eq 1 ]; then
        cp "$RAW_CLAUDE" "$RAW"
    elif [ "$HAVE_CODEX" -eq 1 ]; then
        cp "$RAW_CODEX" "$RAW"
    else
        echo "" >&2
        echo "ERROR: No session sources found. Nothing to ingest." >&2
        echo "" >&2
        echo "To fix this, use one of:" >&2
        echo "  --claude-root <path>   Point to a Claude projects directory" >&2
        echo "  --codex-root  <path>   Point to a Codex sessions directory" >&2
        echo "  --raw         <path>   Provide a pre-built .chat.raw.jsonl directly" >&2
        echo "" >&2
        echo "Or set environment variables:" >&2
        echo "  CLAUDE_SESSIONS_ROOT=<path>  ./scripts/sync-all-agent-memory.sh" >&2
        echo "  CODEX_SESSIONS_ROOT=<path>   ./scripts/sync-all-agent-memory.sh" >&2
        exit 1
    fi
fi

echo "[2/3] Running MemoryCtl ingest..."
run_memoryctl ingest-chatlog --db "$DB" --chatlog "$RAW" --cursor "$CURSOR" --max 25000 --gap-min 120

echo "[3/3] Running Dreaming pipeline..."
run_memoryctl dream --db "$DB" --topic-k 5 --thread-k 3 --decision-k 3 --invariant-k 3 \
    || echo "WARNING: Dream pipeline failed (non-fatal if AMS snapshot exists)."

echo "Refreshing agent-memory summaries and freshness..."
run_memoryctl agent-maintain --db "$DB"

# --- FEP pipeline (non-fatal, skip if kernel absent) ---
if [ -f "$AMS_KERNEL" ]; then
    echo "Running FEP tool bootstrap + anomaly detection..."
    "$AMS_KERNEL" fep-bootstrap-agent-tools --input "$DB" \
        || echo "WARNING: Tool prior bootstrap failed (non-fatal)."
    "$AMS_KERNEL" fep-detect-tool-anomalies --input "$DB" --since last-run --threshold 2.0 \
        || echo "WARNING: Tool anomaly detection failed (non-fatal)."
else
    echo "SKIP: ams-core-kernel binary not found, skipping FEP pipeline."
fi

echo "Running FEP repair trigger..."
python3 "$SCRIPT_DIR/fep-repair-trigger.py" --db "$DB" 2>/dev/null \
    || echo "WARNING: FEP repair trigger failed (non-fatal)."

echo "Generating FEP tool health report..."
python3 "$SCRIPT_DIR/fep-tool-health-report.py" --db "$DB" 2>/dev/null \
    || echo "WARNING: FEP tool health report failed (non-fatal)."

echo "Sync complete."
