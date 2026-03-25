#!/usr/bin/env python3
"""Smoke test for the shared artifact store (Phase 3 of LLM Swarm Computer)."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

from ams_common import build_rust_ams_cmd, repo_root, rust_backend_env

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(repo_root())

sys.path.insert(0, str(SCRIPT_DIR))
from swarm.artifacts import (
    bootstrap_artifact_store, store_artifact, list_artifacts,
    read_artifact, retrieve_artifact, find_artifacts_by_author,
)


def run_kernel(backend_root: str, *args: str) -> subprocess.CompletedProcess[str]:
    cmd = build_rust_ams_cmd(*args)
    if cmd is None:
        raise RuntimeError("unable to locate the Rust AMS kernel binary or Cargo project")
    return subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        env=rust_backend_env(backend_root),
        text=True,
        capture_output=True,
        check=False,
    )


def ensure_ok(result: subprocess.CompletedProcess[str], label: str) -> None:
    if result.returncode == 0:
        return
    raise RuntimeError(
        f"{label} failed with exit={result.returncode}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main() -> int:
    temp_dir = tempfile.TemporaryDirectory(prefix="ams-artifact-store-smoke-")
    workspace = Path(temp_dir.name)

    backend_root = workspace / "backend"
    corpus_dir = workspace / "all-agents-sessions"
    backend_root.mkdir(parents=True, exist_ok=True)
    corpus_dir.mkdir(parents=True, exist_ok=True)
    corpus_path = corpus_dir / "all-agents-sessions.memory.jsonl"
    corpus_path.write_text("", encoding="utf-8")

    input_path = str(corpus_path)
    br = str(backend_root)

    print("=== Step 1: Bootstrap artifact store namespace 'test-ns' ===")
    bucket = bootstrap_artifact_store(input_path, "test-ns", backend_root=br)
    require(bucket == "smartlist/artifact-store/test-ns", f"unexpected bucket path: {bucket}")
    print(f"  created bucket: {bucket}")

    print("\n=== Step 2: Store first artifact ===")
    note1 = store_artifact(
        input_path,
        namespace="test-ns",
        title="design-doc-v1",
        content="Architecture design document content here.",
        author="worker-agent-0",
        task_path="smartlist/execution-plan/phase-3/3c1",
        backend_root=br,
        timestamp="2026-03-18T12:00:00Z",
    )
    require(len(note1) > 0, "expected non-empty note_id for artifact 1")
    print(f"  stored artifact 1, note_id={note1}")

    print("\n=== Step 3: Store second artifact ===")
    note2 = store_artifact(
        input_path,
        namespace="test-ns",
        title="test-results-run-42",
        content="All 15 tests passed.",
        author="worker-agent-1",
        task_path="smartlist/execution-plan/phase-3/3c2",
        backend_root=br,
        timestamp="2026-03-18T12:05:00Z",
    )
    require(len(note2) > 0, "expected non-empty note_id for artifact 2")
    print(f"  stored artifact 2, note_id={note2}")

    print("\n=== Step 4: Verify bucket has 2 members ===")
    result = run_kernel(br, "smartlist-browse", "--input", input_path, "--path", "smartlist/artifact-store/test-ns")
    ensure_ok(result, "smartlist-list")
    lines = [l for l in result.stdout.strip().splitlines() if l.strip()]
    print(f"  bucket contents ({len(lines)} lines):\n    " + "\n    ".join(lines[:10]))
    require(len(lines) >= 2, f"expected >= 2 members in bucket, got {len(lines)}")

    print("\n=== Step 5: Bootstrap second namespace ===")
    bucket2 = bootstrap_artifact_store(input_path, "logs", backend_root=br)
    require(bucket2 == "smartlist/artifact-store/logs", f"unexpected bucket path: {bucket2}")
    print(f"  created bucket: {bucket2}")

    note3 = store_artifact(
        input_path,
        namespace="logs",
        title="build-log-1",
        content="Build succeeded.",
        author="ci-agent",
        task_path="smartlist/execution-plan/phase-3/build",
        backend_root=br,
    )
    require(len(note3) > 0, "expected non-empty note_id for artifact 3")
    print(f"  stored artifact in logs namespace, note_id={note3}")

    print("\n=== Step 6: list_artifacts ===")
    refs = list_artifacts(input_path, "test-ns", backend_root=br)
    print(f"  test-ns artifacts: {refs}")
    require(len(refs) >= 2, f"expected >= 2 artifacts in test-ns, got {len(refs)}")

    print("\n=== Step 7: read_artifact by note_id ===")
    art1 = read_artifact(input_path, note1, backend_root=br)
    print(f"  read artifact 1: {art1}")
    require(art1 is not None, "expected to read artifact 1 by note_id")
    require(art1.get("content") == "Architecture design document content here.",
            f"unexpected content: {art1.get('content')}")

    print("\n=== Step 8: retrieve_artifact by namespace+title ===")
    art_by_title = retrieve_artifact(input_path, "test-ns", "design-doc-v1", backend_root=br)
    print(f"  retrieved by title: {art_by_title}")
    require(art_by_title is not None, "expected to retrieve artifact by title 'design-doc-v1'")
    require(art_by_title.get("content") == "Architecture design document content here.",
            f"unexpected content from retrieve: {art_by_title.get('content')}")

    print("\n=== Step 9: retrieve_artifact for non-existent title ===")
    missing = retrieve_artifact(input_path, "test-ns", "does-not-exist", backend_root=br)
    require(missing is None, f"expected None for missing title, got {missing}")
    print("  correctly returned None")

    print("\n=== Step 10: find_artifacts_by_author ===")
    by_author = find_artifacts_by_author(input_path, "test-ns", "worker-agent-0", backend_root=br)
    print(f"  artifacts by worker-agent-0: {len(by_author)}")
    require(len(by_author) >= 1, "expected at least 1 artifact by worker-agent-0")

    print("\n=== Step 11: list_artifacts on empty/missing namespace ===")
    empty_refs = list_artifacts(input_path, "nonexistent-ns", backend_root=br)
    require(len(empty_refs) == 0, f"expected 0 artifacts in nonexistent-ns, got {len(empty_refs)}")
    print("  correctly returned empty list")

    print("\nresult=ok")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\nFAILED: {e}", file=sys.stderr)
        sys.exit(1)
