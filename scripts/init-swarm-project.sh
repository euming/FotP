#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
INIT_PY="$SCRIPT_DIR/init-swarm-project.py"

if [ ! -f "$INIT_PY" ]; then
  echo "ERROR: init-swarm-project.py not found at \"$INIT_PY\"." >&2
  exit 1
fi

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$INIT_PY" "$@"
fi

if command -v python >/dev/null 2>&1; then
  exec python "$INIT_PY" "$@"
fi

echo "ERROR: Python launcher not found. Install Python 3 or run the script with python3." >&2
exit 1
