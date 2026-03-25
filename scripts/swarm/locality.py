"""Home-node locality: assign agents to graph nodes and read neighborhoods.

Each agent gets a "home node" in the SmartList graph. The neighborhood reader
returns the local subgraph around that home node so agents can reason about
nearby structure without loading the full graph.

Bucket layout:
  smartlist/agent-locality/assignments   — one note per agent mapping agent→node
  smartlist/agent-locality/neighborhoods — cached neighborhood snapshots
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ams_common import build_rust_ams_cmd, rust_backend_env

LOCALITY_ROOT = "smartlist/agent-locality"
ASSIGNMENTS_BUCKET = f"{LOCALITY_ROOT}/assignments"
NEIGHBORHOODS_BUCKET = f"{LOCALITY_ROOT}/neighborhoods"


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


def bootstrap_locality(input_path: str, backend_root: str | None = None) -> None:
    """Create the locality bucket tree."""
    for bucket_path in [ASSIGNMENTS_BUCKET, NEIGHBORHOODS_BUCKET]:
        result = _run_kernel(
            backend_root,
            "smartlist-create",
            "--input", input_path,
            "--path", bucket_path,
        )
        if result.returncode != 0:
            raise RuntimeError(f"smartlist-create {bucket_path} failed: {result.stderr}")


def assign_home_node(
    input_path: str,
    agent_ref: str,
    home_node_path: str,
    backend_root: str | None = None,
) -> str:
    """Assign an agent to a home node in the graph.

    Stores a note in the assignments bucket. The note title encodes the mapping
    as ``{agent_ref}={home_node_path}`` so it can be recovered from browse output
    (the kernel has no single-note read command).
    Returns the note_id.
    """
    note_id = f"home-node:{agent_ref}"
    title = f"{agent_ref}={home_node_path}"
    text = json.dumps({
        "agent_ref": agent_ref,
        "home_node_path": home_node_path,
    })

    result = _run_kernel(
        backend_root,
        "smartlist-note",
        "--input", input_path,
        "--title", title,
        "--text", text,
        "--buckets", ASSIGNMENTS_BUCKET,
        "--note-id", note_id,
    )
    if result.returncode != 0:
        raise RuntimeError(f"assign_home_node failed for {agent_ref}: {result.stderr}")
    data = _parse_kv(result.stdout)
    return data.get("note_id", note_id)


def get_home_node(
    input_path: str,
    agent_ref: str,
    backend_root: str | None = None,
) -> str | None:
    """Look up the home node path for an agent. Returns None if not assigned.

    Parses the ``title`` field from ``smartlist-browse`` output. Titles are
    encoded as ``{agent_ref}={home_node_path}`` by :func:`assign_home_node`.
    """
    result = _run_kernel(
        backend_root,
        "smartlist-browse",
        "--input", input_path,
        "--path", ASSIGNMENTS_BUCKET,
    )
    if result.returncode != 0:
        return None

    # Browse lines look like: object_id=... kind=... name={title}
    # The title is encoded as {agent_ref}={home_node_path}
    prefix = f"name={agent_ref}="
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        for token in line.split():
            if token.startswith(prefix):
                return token[len(prefix):]
    return None


def _browse_entries(
    input_path: str,
    path: str,
    backend_root: str | None = None,
) -> list[dict]:
    """Browse a SmartList path and return parsed entries.

    Each entry is a dict with keys extracted from the browse output line
    (typically: object_id, kind, name).
    """
    result = _run_kernel(
        backend_root,
        "smartlist-browse",
        "--input", input_path,
        "--path", path,
    )
    if result.returncode != 0:
        return []

    entries: list[dict] = []
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("count="):
            continue
        entry: dict[str, str] = {}
        for token in line.split():
            if "=" in token:
                k, v = token.split("=", 1)
                entry[k] = v
        if entry:
            entries.append(entry)
    return entries


def _read_note_text(
    input_path: str,
    bucket_path: str,
    note_name: str,
    backend_root: str | None = None,
) -> str | None:
    """Read the text content of a note by browsing its parent bucket.

    The Rust kernel exposes note text via smartlist-browse on the note path.
    Falls back to returning None if the note cannot be read.
    """
    note_path = f"{bucket_path}/{note_name}"
    result = _run_kernel(
        backend_root,
        "smartlist-browse",
        "--input", input_path,
        "--path", note_path,
    )
    if result.returncode != 0:
        return None
    text = result.stdout.strip()
    return text if text else None


def read_neighborhood(
    input_path: str,
    node_path: str,
    backend_root: str | None = None,
) -> dict:
    """Read the local graph neighborhood around a SmartList node.

    Given a node path (typically an agent's home node), returns:
      - node: the target node path
      - parent: parent bucket path (derived from path)
      - siblings: list of sibling entries under the parent (excluding self)
      - children: list of direct child entries under the node
      - observations: list of note entries found directly under the node

    Each entry in siblings/children/observations is a dict with keys from
    the smartlist-browse output (object_id, kind, name).
    """
    neighborhood: dict = {
        "node": node_path,
        "parent": None,
        "siblings": [],
        "children": [],
        "observations": [],
    }

    # Derive parent from path
    parts = node_path.rsplit("/", 1)
    node_name = parts[-1] if len(parts) == 2 else None
    if len(parts) == 2:
        parent_path = parts[0]
        neighborhood["parent"] = parent_path

        # Browse parent to find siblings
        parent_entries = _browse_entries(input_path, parent_path, backend_root)
        neighborhood["siblings"] = [
            e for e in parent_entries if e.get("name") != node_name
        ]

    # Browse the node itself to find children and observations
    children = _browse_entries(input_path, node_path, backend_root)
    for entry in children:
        kind = entry.get("kind", "")
        if "note" in kind:
            neighborhood["observations"].append(entry)
        else:
            neighborhood["children"].append(entry)

    return neighborhood
