#!/usr/bin/env bash
# Build ams-sqlite-vtable (Linux) and copy artifact to dist/
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CRATE_DIR="$REPO_ROOT/rust/ams-sqlite-vtable"
DIST_DIR="$REPO_ROOT/dist"

echo "Building ams-sqlite-vtable (release)..."
(cd "$CRATE_DIR" && cargo build --release)

mkdir -p "$DIST_DIR"
cp "$CRATE_DIR/target/release/libams_vtable.so" "$DIST_DIR/libams_vtable.so"
echo "Artifact copied to dist/libams_vtable.so"
echo "Done."
