from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ams_common import build_rust_ams_cmd, rust_backend_env


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


def bootstrap_agent_pool(input_path: str, pool_size: int = 8, backend_root: str | None = None) -> list[str]:
    """Create registry/free/allocated buckets + N agent slot notes.

    Returns list of agent object IDs (note IDs).
    """
    # Create the three buckets
    for bucket_path in [
        "smartlist/agent-pool/registry",
        "smartlist/agent-pool/free",
        "smartlist/agent-pool/allocated",
    ]:
        result = _run_kernel(
            backend_root,
            "smartlist-create",
            "--input", input_path,
            "--path", bucket_path,
        )
        if result.returncode != 0:
            raise RuntimeError(f"smartlist-create {bucket_path} failed: {result.stderr}")

    agent_ids: list[str] = []
    for i in range(pool_size):
        note_id = f"agent-pool-slot:{i}"
        title = f"agent-slot-{i}"
        text = json.dumps({
            "slot_index": i,
            "agent_kind": "worker",
            "capabilities": ["general"],
        })

        result = _run_kernel(
            backend_root,
            "smartlist-note",
            "--input", input_path,
            "--title", title,
            "--text", text,
            "--buckets", "smartlist/agent-pool/registry",
            "--note-id", note_id,
        )
        if result.returncode != 0:
            raise RuntimeError(f"smartlist-note {note_id} failed: {result.stderr}")
        data = _parse_kv(result.stdout)
        obj_id = data.get("note_id", note_id)

        # Attach to free list
        result = _run_kernel(
            backend_root,
            "smartlist-attach",
            "--input", input_path,
            "--path", "smartlist/agent-pool/free",
            "--member-ref", obj_id,
        )
        if result.returncode != 0:
            raise RuntimeError(f"smartlist-attach free {obj_id} failed: {result.stderr}")

        agent_ids.append(obj_id)

    return agent_ids
