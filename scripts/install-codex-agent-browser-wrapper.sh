#!/usr/bin/env bash
set -euo pipefail

CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$SCRIPT_DIR/../codex-skills/agent-browser-wrapper"
DEST="$CODEX_HOME/skills/agent-browser-wrapper"

if [ ! -f "$SRC/SKILL.md" ]; then
  echo "ERROR: repo skill source not found at \"$SRC\""
  exit 1
fi

rm -rf "$DEST"
mkdir -p "$DEST"
cp -r "$SRC/." "$DEST/"

echo "Installed Codex skill to \"$DEST\""
echo "Restart Codex to pick up new skills."
