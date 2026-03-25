"""Shared artifact store backed by SmartList bucket tree.

Artifacts live at smartlist/artifact-store/{namespace}.  Each artifact is a
SmartList note whose text carries JSON provenance (author agent, task path,
timestamp).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ams_common import build_rust_ams_cmd, rust_backend_env

import subprocess

ARTIFACT_STORE_ROOT = "smartlist/artifact-store"


def _resolve_snapshot(input_path: str, backend_root: str | None) -> str | None:
    """Find the AMS snapshot file from the backend root or input path.

    The kernel stores snapshots at ``{backend_root}/{corpus}/memory.ams.json``
    where *corpus* is the parent directory name of the input jsonl file.
    """
    if backend_root:
        ip = Path(input_path)
        corpus = ip.parent.name  # e.g. "all-agents-sessions"
        candidate = Path(backend_root) / corpus / "memory.ams.json"
        if candidate.exists():
            return str(candidate)
        # Simple single-store layout (no corpus subdir)
        candidate = Path(backend_root) / "store" / "memory.ams.json"
        if candidate.exists():
            return str(candidate)
    return None


def _run_kernel(backend_root: str | None, *args: str) -> subprocess.CompletedProcess[str]:
    cmd = build_rust_ams_cmd(*args)
    if cmd is None:
        raise RuntimeError("unable to locate the Rust AMS kernel binary or Cargo project")
    return subprocess.run(
        cmd,
        env=rust_backend_env(backend_root),
        text=True,
        capture_output=True,
        check=False,
    )


def _parse_kv(stdout: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def bootstrap_artifact_store(
    input_path: str,
    namespace: str,
    backend_root: str | None = None,
) -> str:
    """Create the bucket tree for a namespace under the artifact store.

    Creates:
      smartlist/artifact-store/{namespace}

    Returns the bucket path.
    """
    bucket_path = f"{ARTIFACT_STORE_ROOT}/{namespace}"
    result = _run_kernel(
        backend_root,
        "smartlist-create",
        "--input", input_path,
        "--path", bucket_path,
    )
    if result.returncode != 0:
        raise RuntimeError(f"smartlist-create {bucket_path} failed: {result.stderr}")
    return bucket_path


def store_artifact(
    input_path: str,
    namespace: str,
    title: str,
    content: str,
    author: str,
    task_path: str,
    backend_root: str | None = None,
    timestamp: str | None = None,
) -> str:
    """Store an artifact note in the given namespace bucket.

    Returns the note ID assigned by the kernel.
    """
    bucket_path = f"{ARTIFACT_STORE_ROOT}/{namespace}"
    ts = timestamp or datetime.now(timezone.utc).isoformat()

    text = json.dumps({
        "content": content,
        "provenance": {
            "author": author,
            "task_path": task_path,
            "timestamp": ts,
        },
    })

    result = _run_kernel(
        backend_root,
        "smartlist-note",
        "--input", input_path,
        "--title", title,
        "--text", text,
        "--buckets", bucket_path,
    )
    if result.returncode != 0:
        raise RuntimeError(f"smartlist-note in {bucket_path} failed: {result.stderr}")

    data = _parse_kv(result.stdout)
    return data.get("note_id", "")


# ---------------------------------------------------------------------------
# Higher-level artifact API (3c2)
# ---------------------------------------------------------------------------


def list_artifacts(
    input_path: str,
    namespace: str,
    backend_root: str | None = None,
) -> list[str]:
    """List all artifact note refs in a namespace bucket."""
    bucket_path = f"{ARTIFACT_STORE_ROOT}/{namespace}"
    result = _run_kernel(
        backend_root,
        "smartlist-browse",
        "--input", input_path,
        "--path", bucket_path,
    )
    if result.returncode != 0:
        return []
    refs: list[str] = []
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        # Parse "object_id=<id> kind=... name=..." lines from smartlist-browse
        if line.startswith("object_id="):
            oid = line.split()[0].split("=", 1)[1]
            refs.append(oid)
        elif "=" not in line:
            # Plain ID line
            refs.append(line)
    return refs


def read_artifact(
    input_path: str,
    note_id: str,
    backend_root: str | None = None,
) -> dict | None:
    """Read a single artifact by note_id. Returns parsed JSON or None.

    Reads the AMS snapshot directly since the kernel has no single-note-read
    command.
    """
    snapshot = _resolve_snapshot(input_path, backend_root)
    if snapshot is None:
        return None
    try:
        data = json.loads(Path(snapshot).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    for obj in data.get("objects", []):
        if obj.get("objectId") == note_id:
            text = (
                obj.get("semanticPayload", {})
                .get("provenance", {})
                .get("text", "")
            )
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {"raw": text}
    return None


def retrieve_artifact(
    input_path: str,
    namespace: str,
    title: str,
    backend_root: str | None = None,
) -> dict | None:
    """Retrieve an artifact by namespace and title.

    Searches the namespace bucket for a note matching the given title,
    then reads and returns its parsed JSON content (including provenance).
    Returns None if not found.
    """
    bucket_path = f"{ARTIFACT_STORE_ROOT}/{namespace}"
    # Use smartlist-search to find notes matching the title
    result = _run_kernel(
        backend_root,
        "smartlist-search",
        "--input", input_path,
        "--path", bucket_path,
        "--query", title,
    )
    if result.returncode != 0:
        # Fallback: iterate all notes and match title manually
        refs = list_artifacts(input_path, namespace, backend_root=backend_root)
        for ref in refs:
            artifact = read_artifact(input_path, ref, backend_root=backend_root)
            if artifact is None:
                continue
            # Check if the ref line contains the title (smartlist-browse output
            # typically includes the note title)
            if title in ref:
                artifact["note_id"] = ref
                return artifact
        # Second pass: read each artifact and check provenance or content
        # for a title match via the snapshot
        snapshot = _resolve_snapshot(input_path, backend_root)
        if snapshot:
            return _search_snapshot_by_title(snapshot, namespace, title)
        return None

    # Parse search results — expect lines with note IDs
    lines = [l.strip() for l in result.stdout.strip().splitlines() if l.strip()]
    if not lines:
        return None
    # Take the first (best) match
    note_id = lines[0].split()[0] if lines[0] else lines[0]
    artifact = read_artifact(input_path, note_id, backend_root=backend_root)
    if artifact is not None:
        artifact["note_id"] = note_id
    return artifact


def _search_snapshot_by_title(
    snapshot_path: str,
    namespace: str,
    title: str,
) -> dict | None:
    """Search the AMS snapshot file for an artifact matching namespace and title."""
    try:
        data = json.loads(Path(snapshot_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    bucket_path = f"{ARTIFACT_STORE_ROOT}/{namespace}"
    for obj in data.get("objects", []):
        # Check if this object belongs to the right bucket
        buckets = obj.get("semanticPayload", {}).get("provenance", {}).get("buckets", [])
        obj_title = obj.get("semanticPayload", {}).get("provenance", {}).get("title", "")
        if not isinstance(buckets, list):
            buckets = []
        in_bucket = any(bucket_path in b for b in buckets)
        # Also check via tags or path membership
        if not in_bucket:
            tags = obj.get("tags", [])
            in_bucket = any(bucket_path in t for t in tags) if isinstance(tags, list) else False
        if (in_bucket or not buckets) and obj_title == title:
            text = (
                obj.get("semanticPayload", {})
                .get("provenance", {})
                .get("text", "")
            )
            try:
                result = json.loads(text)
            except json.JSONDecodeError:
                result = {"raw": text}
            result["note_id"] = obj.get("objectId", "")
            return result
    return None


def find_artifacts_by_author(
    input_path: str,
    namespace: str,
    author: str,
    backend_root: str | None = None,
) -> list[dict]:
    """Find all artifacts in a namespace created by a specific author."""
    refs = list_artifacts(input_path, namespace, backend_root=backend_root)
    results: list[dict] = []
    for ref in refs:
        artifact = read_artifact(input_path, ref, backend_root=backend_root)
        if artifact and artifact.get("provenance", {}).get("author") == author:
            artifact["note_id"] = ref
            results.append(artifact)
    return results
