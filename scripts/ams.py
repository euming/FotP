#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import sys
from pathlib import Path

# Ensure stdout/stderr can handle Unicode on Windows (cp1252 default chokes on
# characters like → that appear in callstack data).
if sys.platform == "win32":
    for stream in ("stdout", "stderr"):
        s = getattr(sys, stream)
        if hasattr(s, "reconfigure"):
            s.reconfigure(encoding="utf-8", errors="replace")

from ams_common import (
    SUPPORTED_CORPORA,
    active_swarm_plan_name,
    breakpoint_factory_path,
    build_memoryctl_cmd,
    build_rust_ams_cmd,
    corpus_candidates,
    corpus_db,
    factories_db_path,
    ke_db_path,
    handoff_smartlist_path,
    normalize_session_id,
    repo_root,
    rust_backend_env,
    shared_memory_db_path,
    swarm_plan_db_path,
    swarm_plan_snapshot_path,
)

EXECUTION_PLAN_ROOT = "smartlist/execution-plan"
NODE_BUCKET_SEGMENTS = ("00-node", "10-children", "20-observations", "30-receipts", "90-archive")


def _per_plan_store_has_roots(db_path: str) -> bool:
    """Return True if a per-plan store has recognizable execution-plan roots."""
    cmd = build_rust_ams_cmd("swarm-plan-list", "--input", db_path)
    if cmd is None:
        return False
    import subprocess as _sp
    result = _sp.run(cmd, cwd=repo_root(), capture_output=True, text=True)
    # Output is non-empty and not the "no roots" placeholder
    return bool(result.stdout.strip()) and "(no execution plan roots found)" not in result.stdout


def _swarm_plan_list_all(args) -> int:
    """List all swarm plans by aggregating across all per-plan stores.

    This avoids the recursion that would occur if we called active_swarm_plan_name()
    (which internally runs 'ams.bat swarm-plan list').  Each per-plan store contains
    exactly one project, so we run swarm-plan-list on each store and print results.
    Also queries the factories store for any plans NOT yet migrated to per-plan stores,
    deduplicating by plan name to avoid showing the same plan twice during migration.
    """
    from ams_common import list_swarm_plan_stores, swarm_plan_db_path as _sp_db_path

    seen: set[str] = set()

    # Per-plan stores (post-migration) — primary source
    for name in sorted(list_swarm_plan_stores()):
        db_path = _sp_db_path(name)
        if _per_plan_store_has_roots(db_path):
            _try_rust_swarm_plan(db_path, args)
            seen.add(name)

    # Query factories for plans not yet migrated to per-plan stores.
    # Capture output and filter out plan names already shown from per-plan stores.
    if seen:
        cmd = build_rust_ams_cmd("swarm-plan-list", "--input", factories_db_path())
        if cmd is not None:
            import subprocess as _sp
            result = _sp.run(cmd, cwd=repo_root(), capture_output=True, text=True)
            for line in result.stdout.splitlines():
                # Plan name is the first word on each output line (before status markers)
                plan_name = line.strip().split()[0] if line.strip() else ""
                if plan_name and plan_name not in seen:
                    print(line)
    else:
        # No per-plan stores yet — show everything from factories
        _try_rust_swarm_plan(factories_db_path(), args)
    return 0


def _swarm_plan_db(plan_name: "str | None" = None) -> str:
    """Return the store path for a named (or active) plan.

    ARCHITECTURE NOTE: FACTORIES_DB is for SmartList templates only (like C++ class
    definitions). It must NOT be used to store execution plans or runtime state.
    Execution plans live in per-plan stores under
    shared-memory/system-memory/swarm-plans/<plan>.memory.jsonl.

    If plan_name is explicitly provided, the per-plan store path is returned
    directly — even if the store is new/empty — so that load-plan writes to
    the correct store rather than falling back to factories.

    Auto-detection (plan_name=None) still falls back to factories when no
    per-plan store with roots is found, preserving legacy plan visibility.
    """
    explicit = plan_name is not None
    name = plan_name or active_swarm_plan_name()
    if name:
        candidate = swarm_plan_db_path(name)
        if explicit or _per_plan_store_has_roots(candidate):
            # Explicit name: trust the per-plan store unconditionally.
            # Auto-detected: only use per-plan store if it already has roots.
            return candidate
        # Auto-detected plan has no roots in its per-plan store — it may still
        # live in factories (pre-migration plan). Fall back to factories.
        return factories_db_path()
    import sys as _sys
    print(
        "WARNING: No active swarm-plan found; falling back to factories store (deprecated).",
        file=_sys.stderr,
    )
    return factories_db_path()


def run_memoryctl(*args: str) -> int:
    cmd = build_memoryctl_cmd(*args)
    if cmd is None:
        print("ERROR: unable to locate MemoryCtl.exe or tools/memoryctl/MemoryCtl.csproj.", file=sys.stderr)
        return 1
    return subprocess.run(cmd, cwd=repo_root()).returncode


def run_rust_ams(*args: str, backend_root: str | None = None) -> int:
    cmd = build_rust_ams_cmd(*args)
    if cmd is None:
        print("ERROR: unable to locate the Rust AMS kernel binary or Cargo project.", file=sys.stderr)
        return 1
    _warn_if_cargo_build(cmd)
    return subprocess.run(cmd, cwd=repo_root(), env=rust_backend_env(backend_root)).returncode


def _warn_if_cargo_build(cmd: list[str]) -> None:
    """Emit a one-time notice when falling back to `cargo run` (slow first build)."""
    if cmd and cmd[0] == "cargo":
        print(
            "[ams] ams-core-kernel binary not found — falling back to cargo run "
            "(first build may take 1-3 minutes).\n"
            "[ams] To pre-build: cd rust/ams-core-kernel && cargo build --release",
            file=sys.stderr,
        )


def run_rust_ams_capture(*args: str, backend_root: str | None = None) -> subprocess.CompletedProcess[str]:
    cmd = build_rust_ams_cmd(*args)
    if cmd is None:
        raise RuntimeError("unable to locate the Rust AMS kernel binary or Cargo project.")
    _warn_if_cargo_build(cmd)
    return subprocess.run(
        cmd,
        cwd=repo_root(),
        env=rust_backend_env(backend_root),
        text=True,
        capture_output=True,
    )


def run_rust_ams_checked(*args: str, backend_root: str | None = None) -> dict[str, str]:
    result = run_rust_ams_capture(*args, backend_root=backend_root)
    if result.returncode != 0:
        raise RuntimeError(
            f"Rust AMS command failed: {' '.join(args)}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return parse_kv_output(result.stdout)


def _try_rust_swarm_plan(db_path: str, args) -> int | None:
    """Try to dispatch a swarm-plan subcommand via the Rust ams-core-kernel binary.

    Returns the exit code (int) if the Rust binary was available and the subcommand is
    supported, or None if the caller should fall back to the Python implementation.
    """
    cmd = build_rust_ams_cmd("--version")  # probe binary availability
    if cmd is None:
        return None  # no binary — fall back to Python for everything

    subcmd = args.callstack_command

    # Build the Rust CLI argument list for each supported subcommand.
    rust_args: list[str] | None = None

    if subcmd == "context":
        rust_args = ["swarm-plan-context", "--input", db_path]
        if getattr(args, "max_chars", None) is not None:
            rust_args += ["--max-chars", str(args.max_chars)]
        if getattr(args, "project", None):
            rust_args += ["--project", args.project]

    elif subcmd == "list":
        rust_args = ["swarm-plan-list", "--input", db_path]

    elif subcmd == "show":
        rust_args = ["swarm-plan-show", "--input", db_path]
        if getattr(args, "project", None):
            rust_args += ["--project", args.project]

    elif subcmd == "push":
        rust_args = ["swarm-plan-push", "--input", db_path, args.name]
        if getattr(args, "description", None):
            rust_args += ["--description", args.description]
        if getattr(args, "depends_on", None):
            rust_args += ["--depends-on", args.depends_on]
        if getattr(args, "actor_id", None):
            rust_args += ["--actor-id", args.actor_id]

    elif subcmd == "pop":
        rust_args = ["swarm-plan-pop", "--input", db_path]
        if getattr(args, "return_text", None):
            rust_args += ["--return-text", args.return_text]
        if getattr(args, "actor_id", None):
            rust_args += ["--actor-id", args.actor_id]

    elif subcmd == "observe":
        rust_args = ["swarm-plan-observe", "--input", db_path,
                     "--title", args.title, "--text", args.text]
        if getattr(args, "actor_id", None):
            rust_args += ["--actor-id", args.actor_id]

    elif subcmd == "child-done":
        # Write observation to a specific parent node (not the active node).
        # Uses swarm-plan-observe with --node-path to target the parent.
        obs_title = f"child-done:{args.status}:{args.title}"
        rust_args = ["swarm-plan-observe", "--input", db_path,
                     "--title", obs_title, "--text", args.text,
                     "--node-path", args.parent_path]
        if getattr(args, "actor_id", None):
            rust_args += ["--actor-id", args.actor_id]

    elif subcmd == "interrupt":
        rust_args = ["swarm-plan-interrupt", "--input", db_path,
                     "--policy", getattr(args, "policy", "repair"),
                     "--reason", getattr(args, "reason", ""),
                     "--error-output", getattr(args, "error_output", ""),
                     "--context", getattr(args, "context", ""),
                     "--attempted-fix", getattr(args, "attempted_fix", ""),
                     "--repair-hint", getattr(args, "repair_hint", ""),
                     "--subtask-hints", getattr(args, "subtask_hints", "")]
        if getattr(args, "actor_id", None):
            rust_args += ["--actor-id", args.actor_id]

    elif subcmd == "resume":
        rust_args = ["swarm-plan-resume", "--input", db_path]
        if getattr(args, "actor_id", None):
            rust_args += ["--actor-id", args.actor_id]

    elif subcmd == "advance":
        rust_args = ["swarm-plan-advance", "--input", db_path]
        if getattr(args, "actor_id", None):
            rust_args += ["--actor-id", args.actor_id]

    elif subcmd == "switch":
        rust_args = ["swarm-plan-switch", "--input", db_path, args.name]
        if getattr(args, "actor_id", None):
            rust_args += ["--actor-id", args.actor_id]

    elif subcmd == "park":
        rust_args = ["swarm-plan-park", "--input", db_path]
        if getattr(args, "project", None):
            rust_args += ["--project", args.project]
        if getattr(args, "actor_id", None):
            rust_args += ["--actor-id", args.actor_id]

    elif subcmd == "complete-node":
        rust_args = ["swarm-plan-complete-node", "--input", db_path,
                     "--node-path", args.node_path]
        if getattr(args, "return_text", None):
            rust_args += ["--return-text", args.return_text]
        if getattr(args, "actor_id", None):
            rust_args += ["--actor-id", args.actor_id]

    elif subcmd == "ready-nodes":
        rust_args = ["swarm-plan-ready-nodes", "--input", db_path]
        if getattr(args, "project", None):
            rust_args += ["--project", args.project]

    elif subcmd == "enter-edit":
        rust_args = ["swarm-plan-enter-edit", "--input", db_path]
        if getattr(args, "project", None):
            rust_args += ["--project", args.project]
        if getattr(args, "actor_id", None):
            rust_args += ["--actor-id", args.actor_id]

    elif subcmd == "enter-execute":
        rust_args = ["swarm-plan-enter-execute", "--input", db_path]
        if getattr(args, "project", None):
            rust_args += ["--project", args.project]
        if getattr(args, "actor_id", None):
            rust_args += ["--actor-id", args.actor_id]

    elif subcmd == "load-plan":
        if getattr(args, "into_node", None):
            return None  # --into-node not supported by Rust kernel; fall through to Python
        rust_args = ["swarm-plan-load-plan", "--input", db_path, "--file", str(args.file)]
        if getattr(args, "into_active", False):
            rust_args.append("--into-active")
        if getattr(args, "actor_id", None):
            rust_args += ["--actor-id", args.actor_id]

    if rust_args is None:
        return None  # unsupported by Rust — fall back to Python

    return run_rust_ams(*rust_args, backend_root=getattr(args, "backend_root", None))


def _run_rust_batch(db_path: str, ops: list[dict], actor_id: str | None = None,
                    backend_root: str | None = None) -> list[dict] | None:
    """Execute a batch of swarm-plan operations via the Rust binary.

    Returns a list of result dicts on success, or None if the Rust binary is
    unavailable (caller should fall back to executing ops individually).
    """
    cmd = build_rust_ams_cmd("--version")
    if cmd is None:
        return None

    rust_args = ["swarm-plan-batch", "--input", db_path, "--ops", "-"]
    if actor_id:
        rust_args += ["--actor-id", actor_id]

    full_cmd = build_rust_ams_cmd(*rust_args)
    if full_cmd is None:
        return None

    ops_json = json.dumps(ops)
    result = subprocess.run(
        full_cmd,
        cwd=repo_root(),
        env=rust_backend_env(backend_root),
        text=True,
        capture_output=True,
        input=ops_json,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Rust batch command failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return json.loads(result.stdout)


def recall_rank(result: dict, source_name: str) -> float:
    hits = result.get("hits", [])
    short_term = result.get("short_term", [])
    fallback = result.get("fallback", [])
    score = 0.0
    if hits:
        score += 100.0 + float(hits[0].get("score", 0.0))
        score += len(hits) * 10.0
    if short_term:
        score += len(short_term) * 5.0
    if fallback:
        score += len(fallback) * 3.0
    if any(str(hit.get("source_kind", "")).startswith("smartlist") for hit in short_term):
        score += 20.0
    if any(str(hit.get("source_kind", "")).startswith("smartlist") for hit in fallback):
        score += 20.0
    if source_name == "factories":
        score += 5.0
    return score


def search_cache_lookup(query_input: str, query: str, backend_root: str | None) -> str | None:
    """Return cached markdown for *query* from the corpus at *query_input*, or None on miss.

    The ``text=`` field in the Rust output may span multiple lines (it is the full
    markdown result).  We therefore parse it specially: once we see the ``text=``
    prefix we collect that first line and all subsequent lines as the value.
    """
    try:
        result = run_rust_ams_capture(
            "search-cache-lookup",
            "--input", query_input,
            "--query", query,
            backend_root=backend_root,
        )
        if result.returncode != 0:
            return None
        lines = result.stdout.splitlines()
        status = None
        text_lines: list[str] = []
        in_text = False
        for line in lines:
            if in_text:
                text_lines.append(line)
            elif line.startswith("status="):
                status = line.split("=", 1)[1].strip()
            elif line.startswith("text="):
                in_text = True
                text_lines.append(line.split("=", 1)[1])
        if status == "hit" and text_lines:
            return "\n".join(text_lines)
    except Exception:
        pass
    return None


def search_cache_promote(query_input: str, query: str, text: str, backend_root: str | None) -> None:
    """Promote *text* as the cached result for *query* in the corpus at *query_input*."""
    try:
        run_rust_ams_capture(
            "search-cache-promote",
            "--input", query_input,
            "--query", query,
            "--text", text,
            backend_root=backend_root,
        )
    except Exception:
        pass


def run_rust_recall(
    query_input: str,
    query: str,
    top: int,
    explain: bool,
    backend_root: str | None,
    record_route: bool,
) -> int:
    # P5-C3: check the Layer 4 search cache before doing the expensive embedding pass.
    # Cache is keyed by normalised query + corpus version hash (see search_cache.rs).
    # Only cache when not in explain mode (explain output includes per-hit debug lines
    # that are not meaningful to cache) and not recording a route (route recording has
    # side-effects that should happen on real runs).
    use_cache = not explain and not record_route
    if use_cache:
        cached = search_cache_lookup(query_input, query, backend_root)
        if cached is not None:
            print(cached, end="" if cached.endswith("\n") else "\n")
            return 0

    candidates: list[tuple[str, dict]] = []
    inputs = [("corpus", query_input), ("factories", factories_db_path())]
    for source_name, explicit_input in inputs:
        rust_cmd = [
            "agent-query",
            "--input",
            explicit_input,
            "--q",
            query,
            "--top",
            str(top),
            "--json",
            "--include-latent",
        ]
        if explain:
            rust_cmd.append("--explain")
        if record_route and source_name == "corpus":
            rust_cmd.append("--record-route")
        result = run_rust_ams_capture(*rust_cmd, backend_root=backend_root)
        if result.returncode != 0:
            print(result.stdout, end="")
            print(result.stderr, end="", file=sys.stderr)
            raise RuntimeError(f"rust recall query failed for source '{source_name}'")
        candidates.append((source_name, json.loads(result.stdout)))

    best_source, best_result = max(candidates, key=lambda item: recall_rank(item[1], item[0]))
    markdown = best_result["markdown"]
    print(markdown, end="" if markdown.endswith("\n") else "\n")
    if best_source != "corpus":
        print(f"# RecallSource\nsource={best_source}", file=sys.stderr)

    # Promote result to cache (best-effort; only for the corpus source to keep
    # the cached payload deterministic across re-runs with the same corpus state).
    if use_cache and best_source == "corpus":
        search_cache_promote(query_input, query, markdown, backend_root)

    return 0


def ensure_corpus_available(corpus: str) -> str | None:
    db_path = corpus_db(corpus)
    if os.path.exists(db_path):
        return db_path

    searched = "\n".join(f"  - {candidate}" for candidate in corpus_candidates(corpus))
    print(
        "ERROR: AMS corpus not found.\n"
        f"Corpus: {corpus}\n"
        "Looked in:\n"
        f"{searched}\n"
        + (
            "Rebuild it with: scripts\\sync-all-agent-memory.bat --no-browser"
            if sys.platform == "win32"
            else "Rebuild it with: ./scripts/sync-all-agent-memory.sh --no-browser"
        ),
        file=sys.stderr,
    )
    return None


def parse_kv_output(stdout: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "node"


def last_path_segment(path: str) -> str:
    return path.rsplit("/", 1)[-1]


def bucket_object_id(path: str) -> str:
    return f"smartlist-bucket:{path}"


def node_meta_path(node_path: str) -> str:
    return f"{node_path}/00-node"


def node_children_path(node_path: str) -> str:
    return f"{node_path}/10-children"


def node_observations_path(node_path: str) -> str:
    return f"{node_path}/20-observations"


def node_receipts_path(node_path: str) -> str:
    return f"{node_path}/30-receipts"


def node_archive_path(node_path: str) -> str:
    return f"{node_path}/90-archive"


def read_snapshot(snapshot_path: Path) -> dict:
    return json.loads(snapshot_path.read_text(encoding="utf-8-sig"))


def snapshot_indexes(snapshot: dict) -> tuple[dict[str, dict], dict[str, dict], dict[str, dict]]:
    objects = {obj["objectId"]: obj for obj in snapshot.get("objects", [])}
    containers = {container["containerId"]: container for container in snapshot.get("containers", [])}
    links = {link["linkNodeId"]: link for link in snapshot.get("linkNodes", [])}
    return objects, containers, links


def _value_to_string(value: object) -> str:
    if isinstance(value, str):
        return value
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def bucket_fields(snapshot: dict, path: str) -> dict[str, str]:
    objects, _, _ = snapshot_indexes(snapshot)
    obj = objects.get(bucket_object_id(path))
    if obj is None:
        return {}
    provenance = ((obj.get("semanticPayload") or {}).get("provenance") or {})
    return {key: _value_to_string(value) for key, value in provenance.items()}


def iter_container_members(snapshot: dict, container_id: str) -> list[str]:
    _, containers, links = snapshot_indexes(snapshot)
    container = containers.get(container_id)
    if container is None:
        return []
    members: list[str] = []
    current = container.get("headLinknodeId")
    visited: set[str] = set()
    while current:
        if current in visited:
            break
        visited.add(current)
        link = links.get(current)
        if link is None:
            break
        members.append(link["objectId"])
        current = link.get("nextLinknodeId")
    return members


def iter_children(snapshot: dict, node_path: str) -> list[str]:
    objects, _, _ = snapshot_indexes(snapshot)
    children: list[str] = []
    for member_object_id in iter_container_members(snapshot, f"smartlist-members:{node_children_path(node_path)}"):
        obj = objects.get(member_object_id)
        if obj is None:
            continue
        provenance = ((obj.get("semanticPayload") or {}).get("provenance") or {})
        path = provenance.get("path")
        if isinstance(path, str):
            children.append(path)
    return children


def execution_roots(snapshot: dict) -> list[tuple[str, dict[str, str]]]:
    objects, _, _ = snapshot_indexes(snapshot)
    roots: list[tuple[str, dict[str, str]]] = []
    prefix = f"{EXECUTION_PLAN_ROOT}/"
    for obj in objects.values():
        provenance = ((obj.get("semanticPayload") or {}).get("provenance") or {})
        path = provenance.get("path")
        if not isinstance(path, str) or not path.startswith(prefix) or not path.endswith("/00-node"):
            continue
        fields = {key: _value_to_string(value) for key, value in provenance.items()}
        if fields.get("parent_node_path", ""):
            continue
        roots.append((path[: -len("/00-node")], fields))
    roots.sort(key=lambda item: item[0])
    return roots


def find_active_node(snapshot: dict, project: str | None = None) -> tuple[str, dict[str, str]] | None:
    roots = execution_roots(snapshot)

    if project:
        slug = slugify(project)
        roots = [(p, f) for p, f in roots if last_path_segment(p) == slug]

    for _, root_fields in roots:
        active_path = root_fields.get("active_node_path", "").strip()
        if active_path:
            active_fields = bucket_fields(snapshot, node_meta_path(active_path))
            if active_fields:
                return active_path, active_fields

    # Collect root paths that are parked (no active_node_path) to exclude from fallback
    parked_root_paths = set()
    for root_path, root_fields in roots:
        if not root_fields.get("active_node_path", "").strip():
            parked_root_paths.add(root_path)

    objects, _, _ = snapshot_indexes(snapshot)
    prefix = f"{EXECUTION_PLAN_ROOT}/"
    active_candidates: list[tuple[str, dict[str, str]]] = []
    for obj in objects.values():
        provenance = ((obj.get("semanticPayload") or {}).get("provenance") or {})
        path = provenance.get("path")
        if not isinstance(path, str) or not path.startswith(prefix) or not path.endswith("/00-node"):
            continue
        fields = {key: _value_to_string(value) for key, value in provenance.items()}
        if fields.get("state") == "active":
            node_path = path[: -len("/00-node")]
            if project:
                root_segment = node_path[len(prefix):].split("/")[0]
                if root_segment != slug:
                    continue
            # Skip nodes under parked roots
            is_under_parked = any(node_path == r or node_path.startswith(r + "/") for r in parked_root_paths)
            if is_under_parked:
                continue
            active_candidates.append((node_path, fields))
    active_candidates.sort(key=lambda item: item[0])
    return active_candidates[0] if active_candidates else None


def unique_node_path(snapshot: dict | None, parent_node_path: str | None, name: str) -> str:
    base = slugify(name)
    if parent_node_path:
        prefix = f"{node_children_path(parent_node_path)}/{base}"
    else:
        prefix = f"{EXECUTION_PLAN_ROOT}/{base}"
    if snapshot is None:
        return prefix

    objects, _, _ = snapshot_indexes(snapshot)
    candidate = prefix
    suffix = 2
    while bucket_object_id(candidate) in objects:
        candidate = f"{prefix}-{suffix}"
        suffix += 1
    return candidate


def runtime_snapshot_path(db_path: str, backend_root: str | None) -> Path:
    status = run_rust_ams_checked("backend-status", "--input", db_path, backend_root=backend_root)
    snapshot_path = status.get("snapshot_path")
    if not snapshot_path:
        raise RuntimeError("backend-status did not return snapshot_path")
    return Path(snapshot_path)


def load_runtime_snapshot(db_path: str, backend_root: str | None) -> tuple[Path, dict | None]:
    snapshot_path = runtime_snapshot_path(db_path, backend_root)
    if not snapshot_path.exists():
        return snapshot_path, None
    return snapshot_path, read_snapshot(snapshot_path)


def write_note(db_path: str, bucket_path: str, title: str, text: str, actor_id: str, backend_root: str | None) -> None:
    run_rust_ams_checked(
        "smartlist-note",
        "--input",
        db_path,
        "--title",
        title,
        "--text",
        text,
        "--buckets",
        bucket_path,
        "--actor-id",
        actor_id,
        backend_root=backend_root,
    )


def run_smartlist_bucket_set(
    db_path: str,
    path: str,
    fields: dict[str, str],
    actor_id: str,
    backend_root: str | None,
) -> None:
    args = ["smartlist-bucket-set", "--input", db_path, "--path", path, "--actor-id", actor_id]
    for key, value in fields.items():
        args.extend(["--field", f"{key}={value}"])
    run_rust_ams_checked(*args, backend_root=backend_root)


def create_runtime_node(
    db_path: str,
    snapshot: dict | None,
    *,
    name: str,
    owner: str,
    kind: str,
    state: str,
    next_command: str,
    parent_node_path: str | None,
    root_path: str | None,
    backend_root: str | None,
    description: str | None = None,
    resume_policy: str = "next-sibling",
    extra_fields: dict[str, str] | None = None,
) -> str:
    node_path = unique_node_path(snapshot, parent_node_path, name)
    create_paths = [node_path, *(f"{node_path}/{segment}" for segment in NODE_BUCKET_SEGMENTS)]
    for path in create_paths:
        run_rust_ams_checked("smartlist-create", "--input", db_path, "--path", path, "--actor-id", owner, backend_root=backend_root)

    effective_root = root_path or node_path
    fields = {
        "kind": kind,
        "state": state,
        "owner": owner,
        "title": name,
        "next_command": next_command,
        "resume_policy": resume_policy,
        "root_path": effective_root,
        "parent_node_path": parent_node_path or "",
        "node_path": node_path,
    }
    if extra_fields:
        fields.update(extra_fields)
    run_smartlist_bucket_set(db_path, node_meta_path(node_path), fields, owner, backend_root)

    if description:
        write_note(
            db_path,
            node_observations_path(node_path),
            f"{kind}:{name}",
            description,
            owner,
            backend_root,
        )
    return node_path


def set_node_fields(
    db_path: str,
    node_path: str,
    fields: dict[str, str],
    actor_id: str,
    backend_root: str | None,
) -> None:
    run_smartlist_bucket_set(db_path, node_meta_path(node_path), fields, actor_id, backend_root)


def set_node_state(db_path: str, node_path: str, state: str, actor_id: str, backend_root: str | None) -> None:
    set_node_fields(db_path, node_path, {"state": state}, actor_id, backend_root)


def set_root_active_path(
    db_path: str,
    root_path: str,
    active_node_path: str,
    actor_id: str,
    backend_root: str | None,
) -> None:
    set_node_fields(db_path, root_path, {"active_node_path": active_node_path}, actor_id, backend_root)


def active_path_frames(snapshot: dict, node_path: str) -> list[tuple[str, dict[str, str]]]:
    frames: list[tuple[str, dict[str, str]]] = []
    current = node_path
    visited: set[str] = set()
    while current and current not in visited:
        visited.add(current)
        fields = bucket_fields(snapshot, node_meta_path(current))
        if not fields:
            break
        frames.append((current, fields))
        current = fields.get("parent_node_path", "").strip()
    frames.reverse()
    return frames


def resolve_runtime_root(active_node_path: str, active_fields: dict[str, str]) -> str:
    return active_fields.get("root_path") or active_node_path


def read_observation_texts(snapshot: dict, node_path: str) -> list[str]:
    objects, _, _ = snapshot_indexes(snapshot)
    texts: list[str] = []
    for member_object_id in iter_container_members(snapshot, f"smartlist-members:{node_observations_path(node_path)}"):
        obj = objects.get(member_object_id)
        if obj is None:
            continue
        payload = obj.get("semanticPayload") or {}
        text = payload.get("text", "")
        if not text:
            provenance = payload.get("provenance") or {}
            text = provenance.get("text", "")
        if text:
            texts.append(text)
    return texts


def read_latest_receipt_text(snapshot: dict, node_path: str) -> str:
    objects, _, _ = snapshot_indexes(snapshot)
    members = iter_container_members(snapshot, f"smartlist-members:{node_receipts_path(node_path)}")
    if not members:
        return ""
    obj = objects.get(members[-1])
    if obj is None:
        return ""
    payload = obj.get("semanticPayload") or {}
    text = payload.get("text", "")
    if not text:
        provenance = payload.get("provenance") or {}
        text = provenance.get("text", "")
    return text


def callstack_list(db_path: str, backend_root: str | None) -> int:
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    if snapshot is None:
        print("(no execution plan roots found)")
        return 0
    roots = execution_roots(snapshot)
    if not roots:
        print("(no execution plan roots found)")
        return 0
    print("Projects:")
    for root_path, root_fields in roots:
        name = last_path_segment(root_path)
        state = root_fields.get("state", "ready")
        active_path = root_fields.get("active_node_path", "").strip()
        active_label = ""
        if active_path:
            active_leaf = last_path_segment(active_path)
            active_label = f"  cursor={active_leaf}"
        elif state not in ("completed",):
            state = "parked"
        print(f"  {name} [{state}]{active_label}")
    return 0


def callstack_switch(db_path: str, backend_root: str | None, project: str, actor_id: str) -> int:
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    if snapshot is None:
        print("ERROR: no SmartList runtime snapshot found.", file=sys.stderr)
        return 1

    slug = slugify(project)
    roots = execution_roots(snapshot)

    # Park all other roots (clear their active_node_path and set state to parked)
    target_root = None
    for root_path, root_fields in roots:
        root_name = last_path_segment(root_path)
        if root_name == slug:
            target_root = (root_path, root_fields)
        else:
            active_path = root_fields.get("active_node_path", "").strip()
            if active_path:
                set_node_state(db_path, active_path, "parked", actor_id, backend_root)
                set_root_active_path(db_path, root_path, "", actor_id, backend_root)

    if target_root is None:
        print(f"ERROR: no project root named '{slug}' found. Use 'callstack push {project}' to create it.", file=sys.stderr)
        return 1

    root_path, root_fields = target_root
    active_path = root_fields.get("active_node_path", "").strip()
    if not active_path:
        # Resume: find the deepest parked node in this tree, or default to root
        # For now, just resume at root level
        set_root_active_path(db_path, root_path, root_path, actor_id, backend_root)
        set_node_state(db_path, root_path, "active", actor_id, backend_root)
        active_path = root_path
    else:
        # Ensure the active node is marked active (in case it was parked)
        active_fields = bucket_fields(snapshot, node_meta_path(active_path))
        if active_fields and active_fields.get("state") == "parked":
            set_node_state(db_path, active_path, "active", actor_id, backend_root)

    print(f"action=switch\nproject={slug}\nactive_node_path={active_path}")
    from ams_common import push_plan_stack
    push_plan_stack(slug)
    return 0


def callstack_park(db_path: str, backend_root: str | None, actor_id: str, project: str | None = None) -> int:
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    if snapshot is None:
        print("ERROR: no SmartList runtime snapshot found.", file=sys.stderr)
        return 1

    if project:
        slug = slugify(project)
        roots = execution_roots(snapshot)
        for root_path, root_fields in roots:
            if last_path_segment(root_path) == slug:
                active_path = root_fields.get("active_node_path", "").strip()
                if active_path:
                    set_node_state(db_path, active_path, "parked", actor_id, backend_root)
                set_root_active_path(db_path, root_path, "", actor_id, backend_root)
                print(f"action=park\nproject={slug}")
                return 0
        print(f"ERROR: no project root named '{slug}' found.", file=sys.stderr)
        return 1

    # Park whatever is currently active
    active = find_active_node(snapshot)
    if active is None:
        print("(nothing to park — no active callstack)")
        return 0
    active_path, active_fields = active
    root_path = resolve_runtime_root(active_path, active_fields)
    # Set the active node's state to parked so fallback scan doesn't pick it up
    set_node_state(db_path, active_path, "parked", actor_id, backend_root)
    set_root_active_path(db_path, root_path, "", actor_id, backend_root)
    name = last_path_segment(root_path)
    print(f"action=park\nproject={name}")
    return 0


def callstack_ready_nodes(db_path: str, backend_root: str | None, project: str | None = None) -> int:
    """List all nodes whose dependencies are satisfied and are ready for dispatch."""
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    if snapshot is None:
        print("(no execution plan found)")
        return 0
    active = find_active_node(snapshot, project)
    if active is None:
        print("(no active project)")
        return 0
    active_path, active_fields = active
    root_path = resolve_runtime_root(active_path, active_fields)

    # Find all ready nodes in the tree (recursive scan)
    all_ready = _collect_all_ready_nodes(snapshot, root_path)
    if not all_ready:
        print("(no ready nodes — tree may be complete)")
        return 0
    for node_path in all_ready:
        fields = bucket_fields(snapshot, node_meta_path(node_path))
        title = fields.get("title", last_path_segment(node_path)) if fields else last_path_segment(node_path)
        obs = read_observation_texts(snapshot, node_path)
        print(f"node_path={node_path}")
        print(f"title={title}")
        if obs:
            print(f"observations={len(obs)}")
            for o in obs[:3]:
                print(f"  {o[:200]}")
        print("---")
    return 0


def _collect_all_ready_nodes(snapshot: dict, parent_path: str) -> list[str]:
    """Recursively collect all ready leaf nodes in the tree."""
    result: list[str] = []
    children = iter_children(snapshot, parent_path)
    if not children:
        return result
    for child in children:
        if _is_node_ready(snapshot, child, children):
            # Check if this node has ready children itself (descend)
            sub_ready = _collect_all_ready_nodes(snapshot, child)
            if sub_ready:
                result.extend(sub_ready)
            else:
                result.append(child)
        else:
            child_fields = bucket_fields(snapshot, node_meta_path(child))
            if child_fields and child_fields.get("state") == "active":
                sub_ready = _collect_all_ready_nodes(snapshot, child)
                result.extend(sub_ready)
    return result


def callstack_load_plan(db_path: str, backend_root: str | None, plan_file: str, actor_id: str, into_active: bool = False, into_node_path: str | None = None) -> int:
    """Load a YAML plan file into the callstack as a new project with dependency-wired nodes."""
    try:
        import yaml
    except ImportError:
        # Fallback: parse simple YAML-like format manually, or try json
        pass

    plan_path = Path(plan_file)
    if not plan_path.exists():
        print(f"ERROR: plan file not found: {plan_file}", file=sys.stderr)
        return 1

    text = plan_path.read_text(encoding="utf-8")

    # Try YAML first, fall back to JSON
    plan = None
    try:
        import yaml
        plan = yaml.safe_load(text)
    except ImportError:
        try:
            plan = json.loads(text)
        except json.JSONDecodeError:
            print("ERROR: plan file must be YAML (requires PyYAML) or JSON.", file=sys.stderr)
            return 1
    except Exception as e:
        print(f"ERROR: failed to parse plan file: {e}", file=sys.stderr)
        return 1

    project_name = plan.get("project")
    if not project_name:
        print("ERROR: plan file must have a 'project' field.", file=sys.stderr)
        return 1
    nodes = plan.get("nodes", [])
    if not nodes:
        print("ERROR: plan file must have at least one node.", file=sys.stderr)
        return 1

    # Validate: detect phase/milestone grouping nodes that aren't actionable work.
    # A grouping node has no depends_on but other nodes depend on it — it acts as a
    # gate/header, not a concrete task. load-plan creates all nodes as flat siblings,
    # so grouping nodes become dispatched as worker tasks (wasting a full agent pass).
    all_titles = {n.get("title") for n in nodes if n.get("title")}
    depended_on: set[str] = set()
    for n in nodes:
        deps = n.get("depends_on", [])
        if isinstance(deps, str):
            deps = [d.strip() for d in deps.split(",") if d.strip()]
        for d in deps:
            depended_on.add(d)
    for n in nodes:
        title = n.get("title", "")
        deps = n.get("depends_on", [])
        if isinstance(deps, str):
            deps = [d.strip() for d in deps.split(",") if d.strip()]
        if not deps and title in depended_on:
            print(f"WARNING: node '{title}' has no depends_on but other nodes depend on it. "
                  f"This looks like a phase/milestone grouping node, not actionable work. "
                  f"Remove it and wire dependencies directly between leaf tasks.", file=sys.stderr)

    if into_node_path:
        # Load nodes as children of a specific node (by path), without disturbing the active cursor
        _, snapshot = load_runtime_snapshot(db_path, backend_root)
        if snapshot is None:
            print("ERROR: no SmartList runtime snapshot found.", file=sys.stderr)
            return 1
        node_fields = bucket_fields(snapshot, node_meta_path(into_node_path))
        if not node_fields:
            print(f"ERROR: target node not found: {into_node_path}", file=sys.stderr)
            return 1
        project_root = into_node_path
        root_path_value = resolve_runtime_root(into_node_path, node_fields)
    elif into_active:
        # Load nodes as children of the current active node
        _, snapshot = load_runtime_snapshot(db_path, backend_root)
        if snapshot is None:
            print("ERROR: no SmartList runtime snapshot found.", file=sys.stderr)
            return 1
        active = find_active_node(snapshot)
        if active is None:
            print("ERROR: no active callstack node to load plan into.", file=sys.stderr)
            return 1
        parent_path, parent_fields = active
        project_root = parent_path
        root_path_value = resolve_runtime_root(parent_path, parent_fields)
    else:
        # Park current project if any
        _, snapshot = load_runtime_snapshot(db_path, backend_root)
        if snapshot is not None:
            active = find_active_node(snapshot)
            if active is not None:
                active_path, active_fields = active
                root_path = resolve_runtime_root(active_path, active_fields)
                set_node_state(db_path, active_path, "parked", actor_id, backend_root)
                set_root_active_path(db_path, root_path, "", actor_id, backend_root)

        # Create project root
        _, snapshot = load_runtime_snapshot(db_path, backend_root)
        project_root = create_runtime_node(
            db_path, snapshot, name=project_name, owner=actor_id,
            kind="work", state="active", next_command="callstack push",
            parent_node_path=None, root_path=None, backend_root=backend_root,
            description=plan.get("description"),
            extra_fields={"active_node_path": ""},
        )
        set_root_active_path(db_path, project_root, project_root, actor_id, backend_root)
        root_path_value = project_root

    # Create all child nodes in "ready" state
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    node_paths: dict[str, str] = {}  # title -> node_path
    for node_def in nodes:
        title = node_def.get("title")
        if not title:
            print("WARNING: skipping node without title", file=sys.stderr)
            continue
        description = node_def.get("description", "")
        depends_on = node_def.get("depends_on", "")
        if isinstance(depends_on, list):
            depends_on = ", ".join(depends_on)

        extra = {}
        if depends_on:
            extra["depends_on"] = depends_on
        if node_def.get("may_decompose"):
            extra["may_decompose"] = "true"
        if node_def.get("role"):
            extra["role"] = node_def["role"]

        child_path = unique_node_path(snapshot, project_root, title)
        create_paths = [child_path, *(f"{child_path}/{seg}" for seg in NODE_BUCKET_SEGMENTS)]
        for path in create_paths:
            run_rust_ams_checked("smartlist-create", "--input", db_path, "--path", path, "--actor-id", actor_id, backend_root=backend_root)

        fields = {
            "kind": "work", "state": "ready", "owner": actor_id,
            "title": title,
            "resume_policy": "next-sibling",
            "root_path": root_path_value, "parent_node_path": project_root,
            "node_path": child_path,
        }
        if extra:
            fields.update(extra)
        run_smartlist_bucket_set(db_path, node_meta_path(child_path), fields, actor_id, backend_root)

        if description:
            write_note(db_path, node_observations_path(child_path), f"work:{title}", description, actor_id, backend_root)

        node_paths[title] = child_path
        # Reload snapshot after each node creation
        _, snapshot = load_runtime_snapshot(db_path, backend_root)

    print(f"action=load-plan\nproject={project_name}\nroot={project_root}\nnodes={len(node_paths)}")
    for title, path in node_paths.items():
        print(f"  {title} -> {path}")
    from ams_common import push_plan_stack
    push_plan_stack(project_name)
    return 0


def swarm_plan_new(intent: str, effort: str = "high", dry_run: bool = False) -> int:
    """Generate and load a swarm plan from a user intent string.

    1. Pre-loads AMS search context for the intent keywords.
    2. Spawns the decomposer agent (claude -p <prompt>) with the intent + context.
    3. Parses the agent stdout for 'PLAN FILE: <path>'.
    4. Calls callstack_load_plan to instantiate the plan (unless --dry-run).
    """
    import subprocess

    repo_root = Path(__file__).resolve().parent.parent
    ams_bat = repo_root / "scripts" / "ams.bat"

    # --- Step 1: Pre-load search context for the intent ---
    keywords = " ".join(intent.split()[:6])  # first 6 words as keyword hint
    ctx_parts: list[str] = []

    def _run_search(cmd: list[str]) -> str:
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, encoding="utf-8",
                errors="replace", cwd=str(repo_root), timeout=30,
            )
            return result.stdout.strip()
        except Exception:
            return ""

    ams_search_out = _run_search([str(ams_bat), "search", keywords])
    proj_dir_out = _run_search([str(ams_bat), "proj-dir", "search", keywords])

    context_block = ""
    if ams_search_out:
        context_block += f"\n\n## AMS Memory Search Results (keywords: {keywords!r})\n\n{ams_search_out}"
    if proj_dir_out:
        context_block += f"\n\n## Project Directory Search Results\n\n{proj_dir_out}"

    # --- Step 2: Build the decomposer prompt ---
    prompt = f"""You are being invoked in Mode B (intent-driven entry point).

## User Intent

{intent}
{context_block}

Follow the 4-step reasoning process described in your instructions:
1. Map to Capabilities — build on the search results above, read relevant source files.
2. Identify Gaps — determine what new work is needed.
3. Concurrency & Dependency Analysis — design for maximal agent parallelism.
4. Synthesize Plan — write the JSON plan file to scripts/plans/<name>.json.

Then print exactly one line:
PLAN FILE: scripts/plans/<filename>.json
"""

    # --- Step 3: Spawn the decomposer agent ---
    cmd = [
        "claude", "-p", prompt,
        "--permission-mode", "bypassPermissions",
        "--output-format", "text",
    ]
    if effort:
        cmd.extend(["--effort", effort])

    print(f"[swarm-plan new] Spawning decomposer (effort={effort}) for intent: {intent!r}")
    try:
        result = subprocess.run(
            cmd,
            capture_output=False,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(repo_root),
            stdout=subprocess.PIPE,
            stderr=None,
        )
        agent_output = result.stdout or ""
    except FileNotFoundError:
        print("ERROR: 'claude' CLI not found. Install Claude Code to use swarm-plan new.", file=sys.stderr)
        return 1

    # --- Step 4: Parse PLAN FILE: from agent stdout ---
    plan_file_path: str | None = None
    for line in agent_output.splitlines():
        line = line.strip()
        if line.startswith("PLAN FILE:"):
            plan_file_path = line[len("PLAN FILE:"):].strip()
            break

    if not plan_file_path:
        print("ERROR: decomposer did not emit a 'PLAN FILE:' line.", file=sys.stderr)
        print("--- decomposer output ---", file=sys.stderr)
        print(agent_output, file=sys.stderr)
        return 1

    print(f"[swarm-plan new] Plan file: {plan_file_path}")

    if dry_run:
        print("[swarm-plan new] --dry-run: skipping load-plan.")
        return 0

    # --- Step 5: Load the plan ---
    plan_abs = (repo_root / plan_file_path).resolve() if not Path(plan_file_path).is_absolute() else Path(plan_file_path)
    db_path = _swarm_plan_db()
    return callstack_load_plan(db_path, None, str(plan_abs), actor_id="decomposer")


def swarm_plan_children(db_path: str, node_filter: str) -> int:
    """List children of a node matching *node_filter* with titles and descriptions.

    Reads the AMS JSON snapshot directly and walks the SmartList structure:
      parent-bucket/10-children container -> child buckets -> 00-node (title)
                                                           -> 20-observations (description text)
    """
    try:
        with open(db_path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"ERROR: cannot read {db_path}: {exc}", file=sys.stderr)
        return 1

    objs_by_id: dict[str, dict] = {o["objectId"]: o for o in data.get("objects", [])}
    lns_by_container: dict[str, list[str]] = {}
    for ln in data.get("linkNodes", []):
        cid = ln["containerId"]
        lns_by_container.setdefault(cid, []).append(ln["objectId"])

    # Find the children container matching the filter.
    # We look for containers like .../<node_filter>/10-children
    matches: list[tuple[str, str]] = []  # (parent_display, children_container_id)
    for container in data.get("containers", []):
        cid = container["containerId"]
        if node_filter in cid and cid.endswith("/10-children"):
            # The parent is the path segment before /10-children
            parent_path = cid.replace("smartlist-members:", "").replace("/10-children", "")
            # Only match direct children containers (not nested ones)
            if parent_path.count("/10-children/") <= 1:
                parent_slug = parent_path.rsplit("/", 1)[-1]
                matches.append((parent_slug, cid))

    if not matches:
        print(f"No node matching '{node_filter}' found.", file=sys.stderr)
        return 1

    for parent_slug, children_container in matches:
        child_bucket_ids = lns_by_container.get(children_container, [])
        if not child_bucket_ids:
            continue

        print(f"=== {parent_slug} ({len(child_bucket_ids)} children) ===\n")

        for bid in child_bucket_ids:
            node_path = bid.replace("smartlist-bucket:", "")
            # Get title from 00-node
            node_id = f"smartlist-bucket:{node_path}/00-node"
            node_obj = objs_by_id.get(node_id, {})
            prov = node_obj.get("semanticPayload", {}).get("provenance", {})
            title = prov.get("title", node_path.rsplit("/", 1)[-1])
            state = prov.get("state", "")

            # Get description from 20-observations
            obs_container = f"smartlist-members:{node_path}/20-observations"
            obs_ids = lns_by_container.get(obs_container, [])
            desc = ""
            for oid in obs_ids:
                o = objs_by_id.get(oid, {})
                text = o.get("semanticPayload", {}).get("provenance", {}).get("text", "")
                if text:
                    desc = text
                    break

            state_tag = f" [{state}]" if state else ""
            print(f"  {title}{state_tag}")
            if desc:
                print(f"    {desc}")
            print()

    return 0


def callstack_insert(
    db_path: str,
    backend_root: str | None,
    name: str,
    actor_id: str,
    parent: str | None = None,
    description: str | None = None,
    depends_on: str | None = None,
) -> int:
    """Insert a child node under a specified parent without moving the cursor.

    Unlike push (which moves the cursor into the new child), insert creates the
    node in 'ready' state and leaves the active cursor unchanged. This makes it
    safe for agents to add multiple sibling subtasks without push/pop gymnastics.
    """
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    if snapshot is None:
        print("ERROR: no SmartList runtime snapshot found.", file=sys.stderr)
        return 1

    # Resolve parent: explicit path, or fall back to active node
    if parent:
        parent_path = parent
        parent_fields = bucket_fields(snapshot, node_meta_path(parent_path))
        if not parent_fields:
            print(f"ERROR: parent node not found: {parent}", file=sys.stderr)
            return 1
    else:
        active = find_active_node(snapshot)
        if active is None:
            print("ERROR: no active node and no --parent specified.", file=sys.stderr)
            return 1
        parent_path, parent_fields = active

    root_path = resolve_runtime_root(parent_path, parent_fields)

    extra = {}
    if depends_on:
        extra["depends_on"] = depends_on

    child_path = create_runtime_node(
        db_path,
        snapshot,
        name=name,
        owner=actor_id,
        kind="work",
        state="ready",
        next_command="callstack pop",
        parent_node_path=parent_path,
        root_path=root_path,
        backend_root=backend_root,
        description=description,
        extra_fields=extra or None,
    )
    print(f"action=insert\nnode_path={child_path}\nparent_path={parent_path}")
    return 0


def callstack_remove(
    db_path: str,
    backend_root: str | None,
    node_path: str,
    actor_id: str,
) -> int:
    """Remove a node from the callstack without completing it.

    Only removes nodes in 'ready' state (never started). Refuses to remove
    active, completed, or in-progress nodes. Does not move the cursor.
    """
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    if snapshot is None:
        print("ERROR: no SmartList runtime snapshot found.", file=sys.stderr)
        return 1

    fields = bucket_fields(snapshot, node_meta_path(node_path))
    if not fields:
        print(f"ERROR: node not found: {node_path}", file=sys.stderr)
        return 1

    state = fields.get("state", "")
    if state not in ("ready", "parked"):
        print(f"ERROR: can only remove nodes in 'ready' or 'parked' state, not '{state}'.", file=sys.stderr)
        return 1

    # Check for children — refuse to remove nodes that have children
    children_path = node_children_path(node_path)
    children_bucket = snapshot.get(children_path, {})
    child_members = children_bucket.get("members", []) if isinstance(children_bucket, dict) else []
    if child_members:
        print(f"ERROR: node has {len(child_members)} children. Remove children first or use --force (not implemented).", file=sys.stderr)
        return 1

    # Mark as removed by setting state
    set_node_fields(db_path, node_path, {"state": "removed", "removed_by": actor_id}, actor_id, backend_root)
    print(f"action=remove\nnode_path={node_path}\nprevious_state={state}")
    return 0


def callstack_annotate(
    db_path: str,
    backend_root: str | None,
    node_path: str,
    title: str,
    text: str,
    actor_id: str,
) -> int:
    """Write an observation to a specific node (not just the active one)."""
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    if snapshot is None:
        print("ERROR: no SmartList runtime snapshot found.", file=sys.stderr)
        return 1

    fields = bucket_fields(snapshot, node_meta_path(node_path))
    if not fields:
        print(f"ERROR: node not found: {node_path}", file=sys.stderr)
        return 1

    write_note(db_path, node_observations_path(node_path), title, text, actor_id, backend_root)
    print(f"action=annotate\nnode_path={node_path}\ntitle={title}")
    return 0


def callstack_context(db_path: str, backend_root: str | None, max_chars: int, project: str | None = None) -> int:
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    if snapshot is None:
        return 0
    active = find_active_node(snapshot, project)
    if active is None:
        return 0
    active_node_path, _ = active
    frames = active_path_frames(snapshot, active_node_path)
    if not frames:
        return 0

    lines: list[str] = ["[AMS Callstack Context]", "Frames:"]
    for index, (frame_path, fields) in enumerate(frames, start=1):
        title = frame_display_title(frame_path, fields)
        kind = fields.get("kind", "work")
        state = fields.get("state", "ready")
        lines.append(f"{index}. {title} [{kind}/{state}]")

    observations = read_observation_texts(snapshot, active_node_path)
    if observations:
        lines.append("---")
        lines.append("Active observations:")
        for obs in observations:
            lines.append(f"- {obs}")

    if len(frames) >= 2:
        parent_path, _ = frames[-2]
        receipt = read_latest_receipt_text(snapshot, parent_path)
        if receipt:
            lines.append("---")
            lines.append("Parent receipt:")
            lines.append(f"- {receipt}")

    # Emit node metadata for the active (last) frame
    active_children = iter_children(snapshot, active_node_path)
    has_children = len(active_children) > 0
    if len(frames) <= 1:
        node_kind = "root"
    elif has_children:
        node_kind = "branch"
    else:
        node_kind = "leaf"
    lines.append("---")
    lines.append(f"has_children={str(has_children).lower()}")
    lines.append(f"node_kind={node_kind}")

    # Emit the active node's kind (work/interrupt/policy) so consumers can
    # detect interrupts structurally instead of substring-matching "interrupt".
    _, active_fields = frames[-1]
    active_node_kind = active_fields.get("kind", "work")
    lines.append(f"active_node_kind={active_node_kind}")

    # If the active node is a policy node, surface the policy_kind.
    policy_kind = active_fields.get("policy_kind", "").strip()
    if policy_kind:
        lines.append(f"policy_kind={policy_kind}")

    # If the active node carries repair metadata, surface the hint.
    repair_hint = active_fields.get("repair_hint", "").strip()
    if repair_hint:
        lines.append(f"repair_hint={repair_hint}")

    lines.append("[End callstack context]")
    output = "\n".join(lines)
    if len(output) > max_chars:
        output = output[: max_chars - 15] + "\n[...truncated]"
    print(output)
    return 0


def callstack_observe(
    db_path: str,
    backend_root: str | None,
    title: str,
    text: str,
    actor_id: str,
) -> int:
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    if snapshot is None:
        print("ERROR: no SmartList runtime snapshot found.", file=sys.stderr)
        return 1
    active = find_active_node(snapshot)
    if active is None:
        print("ERROR: no active SmartList callstack frame.", file=sys.stderr)
        return 1
    active_node_path, _ = active
    write_note(db_path, node_observations_path(active_node_path), title, text, actor_id, backend_root)
    print(f"action=observe\nnode_path={active_node_path}\ntitle={title}")
    return 0


def frame_display_title(frame_path: str, fields: dict[str, str]) -> str:
    return last_path_segment(frame_path)


def callstack_show(db_path: str, backend_root: str | None, project: str | None = None) -> int:
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    if snapshot is None:
        print("(empty call stack - no active SmartList runtime)")
        return 0
    active = find_active_node(snapshot, project)
    if active is None:
        print("(empty call stack - no active SmartList runtime)")
        return 0
    active_node_path, _ = active
    for index, (frame_path, fields) in enumerate(active_path_frames(snapshot, active_node_path), start=1):
        title = frame_display_title(frame_path, fields)
        kind = fields.get("kind", "work")
        state = fields.get("state", "ready")
        print(f"{index}. {title} [{kind}/{state}] {frame_path}")
    return 0


def callstack_push(
    db_path: str,
    backend_root: str | None,
    name: str,
    description: str | None,
    actor_id: str,
    depends_on: str | None = None,
) -> int:
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    active = find_active_node(snapshot) if snapshot is not None else None
    if active is None:
        root_path = create_runtime_node(
            db_path,
            snapshot,
            name=name,
            owner=actor_id,
            kind="work",
            state="active",
            next_command="callstack push",
            parent_node_path=None,
            root_path=None,
            backend_root=backend_root,
            description=description,
            extra_fields={"active_node_path": ""},
        )
        set_root_active_path(db_path, root_path, root_path, actor_id, backend_root)
        print(f"action=push-root\nnode_path={root_path}")
        return 0

    current_node_path, current_fields = active
    current_kind = current_fields.get("kind", "work")
    if current_kind == "interrupt":
        print("ERROR: callstack push cannot attach a generic work child under an active interrupt.", file=sys.stderr)
        return 1
    child_kind = "work"
    parent_state = "ready"
    root_path = resolve_runtime_root(current_node_path, current_fields)
    set_node_state(db_path, current_node_path, parent_state, actor_id, backend_root)
    extra = {}
    if depends_on:
        extra["depends_on"] = depends_on
    child_path = create_runtime_node(
        db_path,
        snapshot,
        name=name,
        owner=actor_id,
        kind=child_kind,
        state="active",
        next_command="callstack pop",
        parent_node_path=current_node_path,
        root_path=root_path,
        backend_root=backend_root,
        description=description,
        extra_fields=extra or None,
    )
    set_root_active_path(db_path, root_path, child_path, actor_id, backend_root)
    print(f"action=push\nnode_path={child_path}\nkind={child_kind}\nparent_path={current_node_path}")
    return 0


def callstack_pop(
    db_path: str,
    backend_root: str | None,
    return_text: str | None,
    actor_id: str,
) -> int:
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    if snapshot is None:
        print("ERROR: no SmartList runtime snapshot found.", file=sys.stderr)
        return 1
    active = find_active_node(snapshot)
    if active is None:
        print("ERROR: no active SmartList callstack frame.", file=sys.stderr)
        return 1

    node_path, fields = active
    kind = fields.get("kind", "work")
    if kind not in {"work", "policy"}:
        print(f"ERROR: callstack pop only supports work or policy nodes, not '{kind}'.", file=sys.stderr)
        return 1

    title = fields.get("title") or last_path_segment(node_path)
    receipt = return_text or f"Completed {title}"
    write_note(db_path, node_receipts_path(node_path), f"return:{title}", receipt, actor_id, backend_root)
    set_node_state(db_path, node_path, "completed", actor_id, backend_root)

    parent_node_path = fields.get("parent_node_path", "").strip()
    root_path = resolve_runtime_root(node_path, fields)
    if parent_node_path:
        set_node_state(db_path, parent_node_path, "active", actor_id, backend_root)
        set_root_active_path(db_path, root_path, parent_node_path, actor_id, backend_root)
        print(f"action=pop\ncompleted={node_path}\nactive_node_path={parent_node_path}")
    else:
        set_root_active_path(db_path, root_path, "", actor_id, backend_root)
        print(f"action=pop-root\ncompleted={node_path}\nactive_node_path=")
    return 0


def callstack_complete_node(
    db_path: str,
    backend_root: str | None,
    node_path: str,
    return_text: str | None,
    actor_id: str,
) -> int:
    """Complete a specific node by path (for parallel dispatch — doesn't require it to be the active cursor)."""
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    if snapshot is None:
        print("ERROR: no SmartList runtime snapshot found.", file=sys.stderr)
        return 1
    fields = bucket_fields(snapshot, node_meta_path(node_path))
    if not fields:
        print(f"ERROR: node not found at {node_path}", file=sys.stderr)
        return 1

    title = fields.get("title") or last_path_segment(node_path)
    receipt = return_text or f"Completed {title}"
    write_note(db_path, node_receipts_path(node_path), f"return:{title}", receipt, actor_id, backend_root)
    set_node_state(db_path, node_path, "completed", actor_id, backend_root)
    print(f"action=complete-node\ncompleted={node_path}\ntitle={title}")
    return 0


def callstack_interrupt(
    db_path: str,
    backend_root: str | None,
    actor_id: str,
    policy_kind: str,
    reason: str,
    error_output: str,
    context: str,
    attempted_fix: str,
    repair_hint: str,
    subtask_hints: str = "",
) -> int:
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    if snapshot is None:
        print("ERROR: no SmartList runtime snapshot found.", file=sys.stderr)
        return 1
    active = find_active_node(snapshot)
    if active is None:
        print("ERROR: no active SmartList callstack frame.", file=sys.stderr)
        return 1

    interrupted_node_path, interrupted_fields = active
    if interrupted_fields.get("kind", "work") != "work":
        print("ERROR: callstack interrupt requires an active work node.", file=sys.stderr)
        return 1
    parent_node_path = interrupted_fields.get("parent_node_path", "").strip()
    if not parent_node_path:
        print("ERROR: cannot interrupt the root execution node.", file=sys.stderr)
        return 1

    root_path = resolve_runtime_root(interrupted_node_path, interrupted_fields)
    interrupt_title = f"interrupt-{last_path_segment(interrupted_node_path)}"
    interrupt_description = "\n".join(
        line
        for line in (
            f"reason={reason}" if reason else "",
            f"policy_kind={policy_kind}",
            f"context={context}" if context else "",
        )
        if line
    )
    interrupt_path = create_runtime_node(
        db_path,
        snapshot,
        name=interrupt_title,
        owner=actor_id,
        kind="interrupt",
        state="active",
        next_command="callstack resume",
        parent_node_path=parent_node_path,
        root_path=root_path,
        backend_root=backend_root,
        description=interrupt_description or None,
        extra_fields={
            "reason": reason,
            "context": context,
            "policy_kind": policy_kind,
            "interrupted_node_path": interrupted_node_path,
        },
    )

    parent_children_path = node_children_path(parent_node_path)
    run_rust_ams_checked(
        "smartlist-detach",
        "--input",
        db_path,
        "--path",
        parent_children_path,
        "--member-ref",
        interrupt_path,
        "--actor-id",
        actor_id,
        backend_root=backend_root,
    )
    run_rust_ams_checked(
        "smartlist-attach-before",
        "--input",
        db_path,
        "--path",
        parent_children_path,
        "--member-ref",
        interrupt_path,
        "--before-member-ref",
        interrupted_node_path,
        "--actor-id",
        actor_id,
        backend_root=backend_root,
    )

    set_node_state(db_path, interrupted_node_path, "paused", actor_id, backend_root)
    set_root_active_path(db_path, root_path, interrupt_path, actor_id, backend_root)

    active_path = interrupt_path
    policy_path = ""
    if policy_kind == "repair":
        set_node_state(db_path, interrupt_path, "running-policy", actor_id, backend_root)
        policy_description = "\n".join(
            line
            for line in (
                f"repair_hint={repair_hint}" if repair_hint else "",
                f"attempted_fix={attempted_fix}" if attempted_fix else "",
                f"error_output={error_output}" if error_output else "",
            )
            if line
        )
        policy_path = create_runtime_node(
            db_path,
            None,
            name=f"repair-{last_path_segment(interrupted_node_path)}",
            owner=actor_id,
            kind="policy",
            state="active",
            next_command="callstack pop",
            parent_node_path=interrupt_path,
            root_path=root_path,
            backend_root=backend_root,
            description=policy_description or repair_hint or f"Repair work for {last_path_segment(interrupted_node_path)}",
            extra_fields={
                "policy_kind": "repair",
                "interrupted_node_path": interrupted_node_path,
                "repair_hint": repair_hint,
                "attempted_fix": attempted_fix,
                "error_output": error_output,
            },
        )
        set_root_active_path(db_path, root_path, policy_path, actor_id, backend_root)
        active_path = policy_path
    elif policy_kind == "decompose":
        set_node_state(db_path, interrupt_path, "running-policy", actor_id, backend_root)
        hints_list = [h.strip() for h in subtask_hints.split(",") if h.strip()] if subtask_hints else []
        policy_description = "\n".join(
            line
            for line in (
                f"reason={reason}" if reason else "",
                f"subtask_hints={','.join(hints_list)}" if hints_list else "",
            )
            if line
        )
        policy_path = create_runtime_node(
            db_path,
            None,
            name=f"decompose-{last_path_segment(interrupted_node_path)}",
            owner=actor_id,
            kind="policy",
            state="active",
            next_command="callstack pop",
            parent_node_path=interrupt_path,
            root_path=root_path,
            backend_root=backend_root,
            description=policy_description or f"Decompose {last_path_segment(interrupted_node_path)} into subtasks",
            extra_fields={
                "policy_kind": "decompose",
                "interrupted_node_path": interrupted_node_path,
                "subtask_hints": ",".join(hints_list),
                "reason": reason,
            },
        )
        set_root_active_path(db_path, root_path, policy_path, actor_id, backend_root)
        active_path = policy_path

    print(
        f"action=interrupt\ninterrupt_path={interrupt_path}\ninterrupted_node_path={interrupted_node_path}\n"
        f"policy_kind={policy_kind}\npolicy_path={policy_path}\nactive_node_path={active_path}"
    )
    return 0


def callstack_resume(db_path: str, backend_root: str | None, actor_id: str) -> int:
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    if snapshot is None:
        print("ERROR: no SmartList runtime snapshot found.", file=sys.stderr)
        return 1
    active = find_active_node(snapshot)
    if active is None:
        print("ERROR: no active SmartList callstack frame.", file=sys.stderr)
        return 1

    interrupt_path, interrupt_fields = active
    if interrupt_fields.get("kind") != "interrupt":
        print("ERROR: callstack resume requires the active node to be an interrupt.", file=sys.stderr)
        return 1

    parent_node_path = interrupt_fields.get("parent_node_path", "").strip()
    interrupted_node_path = interrupt_fields.get("interrupted_node_path", "").strip()
    if not parent_node_path or not interrupted_node_path:
        print("ERROR: interrupt metadata is incomplete; missing parent_node_path or interrupted_node_path.", file=sys.stderr)
        return 1

    title = interrupt_fields.get("title") or last_path_segment(interrupt_path)
    root_path = resolve_runtime_root(interrupt_path, interrupt_fields)
    write_note(
        db_path,
        node_receipts_path(interrupt_path),
        f"resume:{title}",
        f"Resolved interrupt and resumed {interrupted_node_path}",
        actor_id,
        backend_root,
    )
    set_node_state(db_path, interrupt_path, "archived", actor_id, backend_root)
    run_rust_ams_checked(
        "smartlist-move",
        "--input",
        db_path,
        "--source-path",
        node_children_path(parent_node_path),
        "--target-path",
        node_archive_path(parent_node_path),
        "--member-ref",
        interrupt_path,
        "--actor-id",
        actor_id,
        backend_root=backend_root,
    )
    set_node_state(db_path, interrupted_node_path, "active", actor_id, backend_root)
    set_root_active_path(db_path, root_path, interrupted_node_path, actor_id, backend_root)

    # Post-repair FEP belief update: if this was an FEP-triggered repair,
    # nudge the tool's prior toward Success to reflect the successful fix.
    _maybe_fep_belief_update(db_path, interrupt_fields, backend_root)

    print(f"action=resume\narchived_interrupt_path={interrupt_path}\nactive_node_path={interrupted_node_path}")
    return 0


def _maybe_fep_belief_update(
    db_path: str,
    interrupt_fields: dict[str, str],
    backend_root: str | None,
) -> None:
    """If the interrupt was FEP-triggered, update the tool's prior toward Success."""
    policy_kind = interrupt_fields.get("policy_kind", "").strip()
    reason = interrupt_fields.get("reason", "").strip().lower()
    context = interrupt_fields.get("context", "").strip()

    # Only act on FEP-triggered repair interrupts
    if policy_kind != "repair":
        return
    if "fep" not in reason and "anomal" not in reason:
        return

    # Extract tool_name from context (format: "tool=Bash, outcome=Error, ...")
    tool_name = ""
    for part in context.split(","):
        part = part.strip()
        if part.startswith("tool="):
            tool_name = part[len("tool="):].strip()
            break

    if not tool_name:
        return

    # Call the Rust kernel to shift this tool's prior toward Success
    rc = run_rust_ams(
        "fep-update-tool-belief",
        "--input", db_path,
        "--tool-name", tool_name,
        "--outcome", "Success",
        "--precision", "1.0",
        backend_root=backend_root,
    )
    if rc == 0:
        print(f"fep_belief_update=success tool={tool_name} outcome=Success")
    else:
        print(f"WARNING: FEP belief update failed for tool={tool_name} (rc={rc})", file=sys.stderr)


def _is_node_ready(snapshot: dict, node_path: str, siblings: list[str] | None = None) -> bool:
    """Check if a node is ready, considering both state and depends_on constraints."""
    fields = bucket_fields(snapshot, node_meta_path(node_path))
    if fields.get("state") != "ready":
        return False
    depends_on = fields.get("depends_on", "").strip()
    if not depends_on:
        return True
    # Resolve depends_on titles to sibling paths and check completion
    dep_titles = {t.strip() for t in depends_on.split(",") if t.strip()}
    if siblings is None:
        parent = fields.get("parent_node_path", "").strip()
        siblings = iter_children(snapshot, parent) if parent else []
    for sibling in siblings:
        sib_fields = bucket_fields(snapshot, node_meta_path(sibling))
        sib_title = sib_fields.get("title", "")
        if sib_title in dep_titles and sib_fields.get("state") != "completed":
            return False
    return True


def ready_nodes(snapshot: dict, parent_path: str) -> list[str]:
    """Return all children of parent_path that are ready (state=ready + deps satisfied)."""
    children = iter_children(snapshot, parent_path)
    return [c for c in children if _is_node_ready(snapshot, c, children)]


def _find_next_ready_sibling(snapshot: dict, node_path: str) -> str | None:
    """Return the path of the next 'ready' sibling after *node_path*, or None."""
    fields = bucket_fields(snapshot, node_meta_path(node_path))
    parent = fields.get("parent_node_path", "").strip()
    if not parent:
        return None
    siblings = iter_children(snapshot, parent)
    found_self = False
    for sibling in siblings:
        if sibling == node_path:
            found_self = True
            continue
        if found_self:
            if _is_node_ready(snapshot, sibling, siblings):
                return sibling
    return None


def _descend_to_first_ready_leaf(snapshot: dict, node_path: str) -> str:
    """Walk down from *node_path* into its first ready child, recursively."""
    children = iter_children(snapshot, node_path)
    for child in children:
        child_fields = bucket_fields(snapshot, node_meta_path(child))
        if child_fields.get("state") == "ready":
            return _descend_to_first_ready_leaf(snapshot, child)
    return node_path


def callstack_advance(db_path: str, backend_root: str | None, actor_id: str) -> int:
    """Advance the callstack cursor to the next ready node after the current active node.

    Logic:
    1. If the active node has ready children, descend into the first one.
    2. Otherwise, find the next ready sibling.
    3. If no sibling, walk up to parent and repeat.
    4. If the tree is exhausted, report completion.
    """
    _, snapshot = load_runtime_snapshot(db_path, backend_root)
    if snapshot is None:
        print("ERROR: no SmartList runtime snapshot found.", file=sys.stderr)
        return 1
    active = find_active_node(snapshot)
    if active is None:
        print("action=tree-complete\nactive_node_path=")
        return 0

    active_node_path, active_fields = active
    root_path = resolve_runtime_root(active_node_path, active_fields)

    # Try children first (descend into branch nodes), respecting depends_on
    children = iter_children(snapshot, active_node_path)
    for child in children:
        if _is_node_ready(snapshot, child, children):
            target = _descend_to_first_ready_leaf(snapshot, child)
            set_node_state(db_path, target, "active", actor_id, backend_root)
            set_root_active_path(db_path, root_path, target, actor_id, backend_root)
            print(f"action=advance\nfrom={active_node_path}\nactive_node_path={target}")
            return 0

    # Walk up the tree looking for the next ready sibling (or uncle, etc.)
    # As we climb, mark exhausted parent nodes as completed.
    completed_nodes: list[str] = []
    cursor = active_node_path
    visited: set[str] = set()
    while cursor and cursor not in visited:
        visited.add(cursor)
        next_sib = _find_next_ready_sibling(snapshot, cursor)
        if next_sib:
            # Mark the current cursor as completed if it's a parent we're
            # leaving (not the original active node — that was already popped).
            if cursor != active_node_path:
                set_node_state(db_path, cursor, "completed", actor_id, backend_root)
                completed_nodes.append(cursor)
            target = _descend_to_first_ready_leaf(snapshot, next_sib)
            set_node_state(db_path, target, "active", actor_id, backend_root)
            set_root_active_path(db_path, root_path, target, actor_id, backend_root)
            completed_msg = f"\ncompleted_parents={','.join(completed_nodes)}" if completed_nodes else ""
            print(f"action=advance\nfrom={active_node_path}\nactive_node_path={target}{completed_msg}")
            return 0
        # No more siblings — this parent's children are all done; mark it completed
        cursor_fields = bucket_fields(snapshot, node_meta_path(cursor))
        parent = cursor_fields.get("parent_node_path", "").strip()
        if not parent:
            break
        # Mark the exhausted node as completed before climbing
        if cursor != active_node_path:
            set_node_state(db_path, cursor, "completed", actor_id, backend_root)
            completed_nodes.append(cursor)
        cursor = parent

    # No more ready nodes — tree is complete. Mark remaining ancestors completed.
    if cursor and cursor != active_node_path:
        set_node_state(db_path, cursor, "completed", actor_id, backend_root)
        completed_nodes.append(cursor)
    set_root_active_path(db_path, root_path, "", actor_id, backend_root)
    completed_msg = f"\ncompleted_parents={','.join(completed_nodes)}" if completed_nodes else ""
    print(f"action=tree-complete\ncompleted_from={active_node_path}\nactive_node_path={completed_msg}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog=r"scripts\ams.bat" if sys.platform == "win32" else "./scripts/ams",
        description="Short AMS wrapper for agent-facing memory lookup commands.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    search = subparsers.add_parser("search", help="Run agent-query against a configured corpus.")
    search.add_argument("keywords", nargs="+", help="Short concrete search keywords.")
    search.add_argument("--corpus", choices=SUPPORTED_CORPORA, default="all")
    search.add_argument("--backend-root")
    search.add_argument("--top", type=int, default=8)
    search.add_argument("--explain", action="store_true")
    search.add_argument("--record-route", dest="record_route", action="store_true", default=None)
    search.add_argument("--no-record-route", dest="record_route", action="store_false")
    search.add_argument(
        "--engine",
        choices=("rust", "csharp"),
        default=os.environ.get("AMS_SEARCH_ENGINE", "rust"),
        help="Retrieval engine for search. Defaults to rust.",
    )

    def add_recall_parser(name: str, help_text: str) -> None:
        recall = subparsers.add_parser(name, help=help_text)
        recall.add_argument("keywords", nargs="+", help="Short concrete search keywords.")
        recall.add_argument("--corpus", choices=SUPPORTED_CORPORA, default="all")
        recall.add_argument("--backend-root")
        recall.add_argument("--top", type=int, default=8)
        recall.add_argument("--explain", action="store_true")
        recall.add_argument("--record-route", dest="record_route", action="store_true", default=None)
        recall.add_argument("--no-record-route", dest="record_route", action="store_false")
        recall.add_argument(
            "--engine",
            choices=("rust", "csharp"),
            default=os.environ.get("AMS_RECALL_ENGINE", "rust"),
            help="Retrieval engine for recall. Defaults to rust.",
        )

    add_recall_parser("recall", "Run latent-inclusive agent-query against a configured corpus.")
    add_recall_parser("deep-search", "Alias for recall.")
    add_recall_parser("retrieve", "Alias for recall.")
    add_recall_parser("latent-search", "Alias for recall.")

    read = subparsers.add_parser("read", help="Read a chat session by guid or guid prefix.")
    read.add_argument("target", help="Session guid, prefix, or chat-session:<guid> ref.")
    read.add_argument("--corpus", choices=SUPPORTED_CORPORA, default="all")
    read.add_argument("--backend-root")
    read.add_argument(
        "--engine",
        choices=("rust", "csharp"),
        default=os.environ.get("AMS_READ_ENGINE", "rust"),
        help="Inspection engine for read. Defaults to rust.",
    )

    sessions = subparsers.add_parser("sessions", help="List recent chat sessions.")
    sessions.add_argument("--corpus", choices=SUPPORTED_CORPORA, default="all")
    sessions.add_argument("--backend-root")
    sessions.add_argument("--since")
    sessions.add_argument("--n", type=int, default=20)
    sessions.add_argument(
        "--engine",
        choices=("rust", "csharp"),
        default=os.environ.get("AMS_SESSIONS_ENGINE", "rust"),
        help="Inspection engine for sessions. Defaults to rust.",
    )

    thread = subparsers.add_parser("thread", help="Show the current AMS task graph thread status.")
    thread.add_argument("--corpus", choices=SUPPORTED_CORPORA, default="all")
    thread.add_argument("--backend-root")
    thread.add_argument(
        "--engine",
        choices=("rust", "csharp"),
        default=os.environ.get("AMS_THREAD_ENGINE", "rust"),
        help="Inspection engine for thread. Defaults to rust.",
    )

    handoff = subparsers.add_parser("handoff", help="Inspect the tracked AMS handoff factory.")
    handoff.add_argument("--depth", type=int, default=3)
    handoff.add_argument("--backend-root")
    handoff.add_argument(
        "--engine",
        choices=("rust", "csharp"),
        default=os.environ.get("AMS_HANDOFF_ENGINE", "rust"),
        help="Inspection engine for handoff. Defaults to rust.",
    )

    breakpoint = subparsers.add_parser("breakpoint", help="Inspect the SmartList interrupt factory contract.")
    breakpoint.add_argument("--depth", type=int, default=3)
    breakpoint.add_argument("--backend-root")
    breakpoint.add_argument(
        "--engine",
        choices=("rust", "csharp"),
        default=os.environ.get("AMS_BREAKPOINT_ENGINE", "rust"),
        help="Inspection engine for breakpoint. Defaults to rust.",
    )

    backend = subparsers.add_parser("backend", help="Show Rust shared-backend targeting and recovery status.")
    backend.add_argument("--corpus", choices=SUPPORTED_CORPORA, default="all")
    backend.add_argument("--backend-root")
    backend.add_argument("--assert-clean", action="store_true")

    bug_list = subparsers.add_parser("bugreport-list", help="List bug reports from the global registry.")
    bug_list.add_argument("--corpus", choices=SUPPORTED_CORPORA, default="all")
    bug_list.add_argument("--backend-root")
    bug_list.add_argument("--status", choices=["open", "in-repair", "resolved"], default=None)

    bug_search = subparsers.add_parser("bugreport-search", help="Search bug reports by keyword.")
    bug_search.add_argument("query", help="Search keywords")
    bug_search.add_argument("--corpus", choices=SUPPORTED_CORPORA, default="all")
    bug_search.add_argument("--backend-root")
    bug_search.add_argument("--status", choices=["open", "in-repair", "resolved"], default=None)

    bug_show = subparsers.add_parser("bugreport-show", help="Show a specific bug report by ID.")
    bug_show.add_argument("bug_id", help="Bug report ID (smartlist-bugreport:...)")
    bug_show.add_argument("--corpus", choices=SUPPORTED_CORPORA, default="all")
    bug_show.add_argument("--backend-root")

    # "swarm-plan" is the primary name; "callstack" is a backward-compat alias
    callstack = subparsers.add_parser("swarm-plan", aliases=["callstack"], help="Operate the SmartList-first swarm plan runtime.")
    callstack.add_argument("--corpus", choices=SUPPORTED_CORPORA, default="all")
    callstack.add_argument("--backend-root")
    callstack.add_argument("--project", help="Scope swarm-plan operations to a named project root.")
    callstack_subparsers = callstack.add_subparsers(dest="callstack_command", required=True)

    show_cmd = callstack_subparsers.add_parser("show", help="Render the active SmartList callstack.")
    show_cmd.add_argument("--project", default=None)
    callstack_subparsers.add_parser("list", help="List all project roots in the execution plan.")

    switch_cmd = callstack_subparsers.add_parser("switch", help="Switch active project (parks all others).")
    switch_cmd.add_argument("name", help="Project name to switch to.")
    switch_cmd.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "codex"))

    park_cmd = callstack_subparsers.add_parser("park", help="Suspend the active project without completing it.")
    park_cmd.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "codex"))

    push = callstack_subparsers.add_parser("push", help="Push a child execution node.")
    push.add_argument("name", help="Display name for the new execution node.")
    push.add_argument("--description")
    push.add_argument("--depends-on", help="Comma-separated sibling titles that must complete before this node is ready.")
    push.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "codex"))
    push.add_argument("--project", default=None)

    pop = callstack_subparsers.add_parser("pop", help="Complete the active work or policy node.")
    pop.add_argument("--return-text")
    pop.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "codex"))
    pop.add_argument("--project", default=None)

    interrupt = callstack_subparsers.add_parser("interrupt", help="Insert a generic interrupt before the active work node.")
    interrupt.add_argument("--policy", default="repair")
    interrupt.add_argument("--reason", default="interrupt")
    interrupt.add_argument("--error-output", default="")
    interrupt.add_argument("--context", default="")
    interrupt.add_argument("--attempted-fix", default="")
    interrupt.add_argument("--repair-hint", default="")
    interrupt.add_argument("--subtask-hints", default="")
    interrupt.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "codex"))
    interrupt.add_argument("--project", default=None)

    resume = callstack_subparsers.add_parser("resume", help="Archive the interrupt and resume its interrupted sibling.")
    resume.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "codex"))
    resume.add_argument("--project", default=None)

    advance = callstack_subparsers.add_parser("advance", help="Advance the cursor to the next ready node in the execution tree.")
    advance.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "codex"))
    advance.add_argument("--project", default=None)

    context_cmd = callstack_subparsers.add_parser("context", help="Emit compact execution context for hook injection.")
    context_cmd.add_argument("--max-chars", type=int, default=2000)
    context_cmd.add_argument("--project", default=None)

    observe_cmd = callstack_subparsers.add_parser("observe", help="Write observation to active node.")
    observe_cmd.add_argument("--title", required=True)
    observe_cmd.add_argument("--text", required=True)
    observe_cmd.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "codex"))
    observe_cmd.add_argument("--project", default=None)

    child_done_cmd = callstack_subparsers.add_parser("child-done", help="Notify parent node that a child worker is done. Writes an observation to the specified parent path.")
    child_done_cmd.add_argument("--parent-path", required=True, help="SmartList path of the parent node to notify.")
    child_done_cmd.add_argument("--title", required=True, help="Child node title (used as observation title prefix).")
    child_done_cmd.add_argument("--text", required=True, help="Summary of what the child accomplished.")
    child_done_cmd.add_argument("--status", choices=["done", "failed", "partial"], default="done", help="Completion status.")
    child_done_cmd.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "worker"))
    child_done_cmd.add_argument("--project", default=None)

    ready_nodes_cmd = callstack_subparsers.add_parser("ready-nodes", help="List all nodes whose dependencies are satisfied and are ready for dispatch.")
    ready_nodes_cmd.add_argument("--project", default=None)

    complete_node_cmd = callstack_subparsers.add_parser("complete-node", help="Complete a specific node by path (for parallel dispatch).")
    complete_node_cmd.add_argument("--node-path", required=True, help="SmartList path of the node to complete.")
    complete_node_cmd.add_argument("--return-text", default=None)
    complete_node_cmd.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "orchestrator"))
    complete_node_cmd.add_argument("--project", default=None)

    enter_edit_cmd = callstack_subparsers.add_parser("enter-edit", help="Switch the active plan to edit mode (pauses dispatch).")
    enter_edit_cmd.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "orchestrator"))
    enter_edit_cmd.add_argument("--project", default=None)

    enter_execute_cmd = callstack_subparsers.add_parser("enter-execute", help="Switch the active plan to execute mode (enables dispatch).")
    enter_execute_cmd.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "orchestrator"))
    enter_execute_cmd.add_argument("--project", default=None)

    load_plan_cmd = callstack_subparsers.add_parser("load-plan", help="Load a YAML plan file as a callstack project with dependency edges.")
    load_plan_cmd.add_argument("--file", required=True, help="Path to the plan YAML file.")
    load_plan_cmd.add_argument("--into-active", action="store_true", help="Load nodes as children of the current active node instead of creating a new project root.")
    load_plan_cmd.add_argument("--into-node", default=None, metavar="NODE_PATH", help="Load nodes as children of a specific node path (look-ahead decomposition).")
    load_plan_cmd.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "orchestrator"))
    load_plan_cmd.add_argument("--project", default=None, help="Override target project store (default: auto-detected from plan file's 'project' field).")

    write_note_cmd = callstack_subparsers.add_parser("write-note", help="Write a SmartList note to an arbitrary bucket path.")
    write_note_cmd.add_argument("--bucket", required=True, help="SmartList bucket path to write the note into.")
    write_note_cmd.add_argument("--title", required=True)
    write_note_cmd.add_argument("--text", required=True)
    write_note_cmd.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "orchestrator"))
    write_note_cmd.add_argument("--project", default=None)

    tag_cmd = callstack_subparsers.add_parser("tag", help="Tag a completed swarm-plan to a project knowledge bucket (writes a receipt note to shared memory).")
    tag_cmd.add_argument("plan_name", help="Swarm-plan project name (e.g. p3-incremental-dreaming).")
    tag_cmd.add_argument("bucket_path", help="Target SmartList bucket path (e.g. smartlist/project/ngm/dreaming).")
    tag_cmd.add_argument("--summary", default=None, help="One-line summary of what the plan accomplished.")
    tag_cmd.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "orchestrator"))

    insert_cmd = callstack_subparsers.add_parser("insert", help="Insert a child node under a parent without moving the cursor.")
    insert_cmd.add_argument("name", help="Name/title of the new node.")
    insert_cmd.add_argument("--parent", default=None, help="SmartList path of the parent node. Defaults to active node.")
    insert_cmd.add_argument("--description", default=None, help="Initial observation/description for the node.")
    insert_cmd.add_argument("--depends-on", default=None, help="Comma-separated list of sibling titles this node depends on.")
    insert_cmd.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "orchestrator"))

    remove_cmd = callstack_subparsers.add_parser("remove", help="Remove a ready/parked node without completing it.")
    remove_cmd.add_argument("--node-path", required=True, help="SmartList path of the node to remove.")
    remove_cmd.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "orchestrator"))

    annotate_cmd = callstack_subparsers.add_parser("annotate", help="Write an observation to a specific node (not just active).")
    annotate_cmd.add_argument("--node-path", required=True, help="SmartList path of the node to annotate.")
    annotate_cmd.add_argument("--title", required=True)
    annotate_cmd.add_argument("--text", required=True)
    annotate_cmd.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "orchestrator"))
    annotate_cmd.add_argument("--project", default=None)

    children_cmd = callstack_subparsers.add_parser("children", help="List child nodes of a given node with titles and descriptions.")
    children_cmd.add_argument("node_filter", help="Substring to match against node paths (e.g. 'level-4-tiles').")

    new_cmd = callstack_subparsers.add_parser("new", help="Generate and load a swarm plan from a user intent string via the decomposer agent.")
    new_cmd.add_argument("intent", help="Natural-language description of what you want to accomplish.")
    new_cmd.add_argument("--effort", default="high", help="Claude effort level for the decomposer agent (default: high).")
    new_cmd.add_argument("--dry-run", action="store_true", help="Spawn the decomposer and print the plan path but do not load it.")

    # atlas: Rust kernel — substrate Atlas commands (page, search, expand, define, show, list, navigate)
    atlas = subparsers.add_parser("atlas", help="Atlas memory tool commands.")
    atlas.add_argument("--corpus", choices=SUPPORTED_CORPORA, default="all")
    atlas_sub = atlas.add_subparsers(dest="atlas_command", required=True)

    atlas_page = atlas_sub.add_parser("page", help="Render an Atlas page by object ID.")
    atlas_page.add_argument("page_id", help="Object ID or prefix (e.g. 'atlas:0').")

    atlas_search = atlas_sub.add_parser("search", help="Keyword search across Atlas objects.")
    atlas_search.add_argument("query", nargs="+", help="Search keywords.")
    atlas_search.add_argument("--top", type=int, default=20)

    atlas_expand = atlas_sub.add_parser("expand", help="Expand a reference to its full object context.")
    atlas_expand.add_argument("ref_id", help="Ref ID or prefix to expand.")

    atlas_define = atlas_sub.add_parser("define", help="Define a named multi-scale Atlas.")
    atlas_define.add_argument("name", help="Unique atlas name slug.")
    atlas_define.add_argument("--description", default=None)
    atlas_define.add_argument("--scale", dest="scales", action="append", default=[],
                              metavar="N:path1,path2",
                              help="Scale level: 'N:bucket/path1,bucket/path2'. Repeat for each scale.")

    atlas_show_p = atlas_sub.add_parser("show", help="Show metadata for a named Atlas.")
    atlas_show_p.add_argument("name", help="Atlas name.")

    atlas_sub.add_parser("list", help="List all Atlases in the substrate.")

    atlas_list_scale = atlas_sub.add_parser("list-at-scale", help="List objects at a given Atlas scale.")
    atlas_list_scale.add_argument("name", help="Atlas name.")
    atlas_list_scale.add_argument("scale", type=int, help="Scale index (0 = coarsest).")

    atlas_nav = atlas_sub.add_parser("navigate", help="Coarse-to-fine navigation for an object in an Atlas.")
    atlas_nav.add_argument("name", help="Atlas name.")
    atlas_nav.add_argument("id", help="Object ID.")

    # cache: GNUISNGNU v0.2 tool-identity and artifact cache commands
    cache = subparsers.add_parser("cache", help="GNUISNGNU v0.2 cache commands (tool identity, promote, lookup).")
    cache_sub = cache.add_subparsers(dest="cache_command", required=True)

    cache_rt = cache_sub.add_parser("register-tool", help="Register a Tool Identity Object.")
    cache_rt.add_argument("--tool-id", required=True, help="Stable tool identifier (e.g. swarm-worker:v1).")
    cache_rt.add_argument("--tool-version", default="1.0", help="Tool version string.")
    cache_rt.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "orchestrator"))

    cache_rs = cache_sub.add_parser("register-source", help="Register a Source Identity Object.")
    cache_rs.add_argument("--source-id", required=True, help="Stable source identifier (e.g. a node_path).")
    cache_rs.add_argument("--fingerprint", default=None, help="Optional content fingerprint.")
    cache_rs.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "orchestrator"))

    cache_pr = cache_sub.add_parser("promote", help="Promote a tool output as a cached artifact.")
    cache_pr.add_argument("--tool", required=True, help="Tool identity slug.")
    cache_pr.add_argument("--tool-version", default="1.0", help="Tool version string.")
    cache_pr.add_argument("--source", required=True, help="Source identity slug.")
    cache_pr.add_argument("--text", required=True, help="Return text / result to cache.")
    cache_pr.add_argument("--actor-id", default=os.environ.get("AMS_ACTOR_ID", "orchestrator"))

    cache_lu = cache_sub.add_parser("lookup", help="Look up a cached artifact.")
    cache_lu.add_argument("--tool", required=True, help="Tool identity slug.")
    cache_lu.add_argument("--source", required=True, help="Source identity slug.")
    cache_lu.add_argument("--format", default="text", choices=["text", "json"], help="Output format.")

    # ke: Agent Knowledge Cache
    ke = subparsers.add_parser("ke", help="Agent Knowledge Cache — semantic knowledge entries.")
    ke_sub = ke.add_subparsers(dest="ke_command", required=True)
    ke_write_cmd = ke_sub.add_parser("write", help="Write a knowledge cache entry.")
    ke_write_cmd.add_argument("--scope", required=True, help="Repo-relative path or concept slug.")
    ke_write_cmd.add_argument("--kind", required=True,
                              choices=["purpose", "api", "data-model", "failure-modes", "decision", "prerequisites", "test-guide"])
    ke_write_cmd.add_argument("--text", required=True, help="Prose explanation.")
    ke_write_cmd.add_argument("--summary", default=None)
    ke_write_cmd.add_argument("--tag", action="append", default=[], dest="tags", metavar="TAG")
    ke_write_cmd.add_argument("--confidence", type=float, default=0.8)
    ke_write_cmd.add_argument("--watch", action="append", default=[], dest="watch_paths", metavar="PATH")
    ke_write_cmd.add_argument("--actor-id", default=None, dest="actor_id")
    ke_read_cmd = ke_sub.add_parser("read", help="Read knowledge cache entries for a scope.")
    ke_read_cmd.add_argument("scope", help="Repo-relative path or concept slug.")
    ke_read_cmd.add_argument("--include-stale", action="store_true", dest="include_stale")
    ke_search_cmd = ke_sub.add_parser("search", help="Search knowledge cache entries.")
    ke_search_cmd.add_argument("query", nargs="+", help="Search terms.")
    ke_search_cmd.add_argument("--top", type=int, default=10)
    ke_search_cmd.add_argument("--scope", default=None)
    ke_search_cmd.add_argument("--kind", default=None,
                               choices=["purpose", "api", "data-model", "failure-modes", "decision", "prerequisites", "test-guide", None])
    ke_search_cmd.add_argument("--include-stale", action="store_true", dest="include_stale")
    ke_sem_cmd = ke_sub.add_parser("sem", help="Semantic nearest-neighbor search for a KE scope.")
    ke_sem_cmd.add_argument("scope", help="Anchor KE scope, e.g. concept:current-project-dag")
    ke_sem_cmd.add_argument("--kind", default=None,
                            choices=["purpose", "api", "data-model", "failure-modes", "decision", "prerequisites", "test-guide", None],
                            help="Anchor kind. Default: auto-pick a preferred kind for the scope.")
    ke_sem_cmd.add_argument("--top", type=int, default=8, help="Number of nearest neighbors to return.")
    ke_sem_cmd.add_argument("--json", action="store_true", dest="json_output", help="Emit sqlite JSON output directly.")
    ke_context_cmd = ke_sub.add_parser("context", help="Format knowledge cache as agent-injectable context block.")
    ke_context_cmd.add_argument("--scope", default=None)
    ke_context_cmd.add_argument("--max-entries", type=int, default=20, dest="max_entries")
    ke_context_cmd.add_argument("--max-chars", type=int, default=3000, dest="max_chars")
    ke_bootstrap_cmd = ke_sub.add_parser("bootstrap", help="Pre-populate purpose entries from tracked markdown files.")
    ke_bootstrap_cmd.add_argument("--overwrite", action="store_true")

    # proj-dir: project directory index
    proj_dir = subparsers.add_parser("proj-dir", help="Project directory index (proj_dir.db).")
    proj_dir_sub = proj_dir.add_subparsers(dest="proj_dir_command", required=True)
    proj_dir_sub.add_parser("build", help="Rebuild proj_dir.db from git.")
    tree_cmd = proj_dir_sub.add_parser("tree", help="Show directory tree.")
    tree_cmd.add_argument("path", nargs="?", default="", help="Subtree root path.")
    tree_cmd.add_argument("--depth", type=int, default=3)
    search_pd = proj_dir_sub.add_parser("search", help="FTS5 search across paths/heads/docs.")
    search_pd.add_argument("query", nargs="+")
    doc_cmd = proj_dir_sub.add_parser("doc", help="Print indexed .md content.")
    doc_cmd.add_argument("path", help="Relative file path.")
    proj_dir_sub.add_parser("stats", help="File counts and sizes by extension.")
    context_cmd_pd = proj_dir_sub.add_parser("context", help="Compact onboarding dump for agent injection.")
    context_cmd_pd.add_argument("--depth", type=int, default=2)

    dream_p = subparsers.add_parser(
        "dream",
        help="Run the full dreaming pipeline: dream-schedule → dream-cluster → dream-generate-md.",
    )
    dream_p.add_argument(
        "--out",
        default=None,
        help="Output path for CLAUDE.local.md (default: <repo-root>/CLAUDE.local.md).",
    )
    dream_p.add_argument(
        "--max-touches", type=int, default=100,
        help="Maximum dream-touch calls during dream-schedule (default: 100).",
    )
    dream_p.add_argument(
        "--max-topics", type=int, default=10,
        help="Maximum topic clusters in generated Markdown (default: 10).",
    )
    dream_p.add_argument(
        "--max-sessions", type=int, default=20,
        help="Maximum recent sessions in generated Markdown (default: 20).",
    )

    fep = subparsers.add_parser("fep", help="FEP cache signal commands (P7).")
    fep_sub = fep.add_subparsers(dest="fep_command", required=True)

    fep_report = fep_sub.add_parser("cache-report", help="Show FEP feedback-loop status: signal summary, cluster surprise, dream schedule preview, recommendations.")
    fep_report.add_argument("--window-hours", type=int, default=24, help="Sliding window in hours (default: 24).")

    prune = subparsers.add_parser("prune", help="Session and tombstone pruning commands (P6).")
    prune_sub = prune.add_subparsers(dest="prune_command", required=True)

    prune_tombstones = prune_sub.add_parser("tombstones", help="Ghost tombstones older than --max-age-days.")
    prune_tombstones.add_argument("--max-age-days", type=int, default=30, help="Expire tombstones older than N days (default: 30).")
    prune_tombstones.add_argument("--corpus", choices=SUPPORTED_CORPORA, default="all")

    prune_sessions = prune_sub.add_parser("sessions", help="Prune sessions older than --older-than-days using session-prune-batch.")
    prune_sessions.add_argument("--older-than-days", type=int, default=90, help="Prune sessions older than N days (default: 90).")
    prune_sessions.add_argument("--corpus", choices=SUPPORTED_CORPORA, default="all")

    # roadmap: project concept and sprint roadmap
    roadmap = subparsers.add_parser("roadmap", help="Project roadmap — concepts, sprints, and status.")
    roadmap_sub = roadmap.add_subparsers(dest="roadmap_command", required=True)
    roadmap_sub.add_parser("context", help="Show active + planned concepts with sprint counts.")
    roadmap_sub.add_parser("list", help="List all concept slugs with their status.")
    roadmap_concept = roadmap_sub.add_parser("concept", help="Show concept details.")
    roadmap_concept.add_argument("slug", help="Concept slug (e.g. ams, ngm, swarm).")
    roadmap_status = roadmap_sub.add_parser("status", help="List all items of a given status.")
    roadmap_status.add_argument("status", choices=["planned", "active", "done", "parked", "canceled"],
                                help="Status to filter by.")

    atlas_cmd = roadmap_sub.add_parser("atlas", help="Zoom-level Atlas view of the roadmap.")
    atlas_cmd.add_argument("level", choices=["overview", "mid", "detail"],
                           help="Zoom level: overview | mid | detail")
    atlas_cmd.add_argument("slug", nargs="?", default=None,
                           help="Concept slug (required for detail level)")

    layer_cmd = roadmap_sub.add_parser("layer", help="Show concepts at a dependency layer.")
    layer_cmd.add_argument("layer_num", type=int, help="Layer number (0-6)")

    theme_cmd = roadmap_sub.add_parser("theme", help="Show concepts in a theme.")
    theme_cmd.add_argument("theme_slug", help="Theme slug (e.g. memory-fabric, retrieval)")

    deps_cmd = roadmap_sub.add_parser("deps", help="Show dependency edges for a concept.")
    deps_cmd.add_argument("dep_slug", help="Concept slug")

    # batch-verify: parallel verification of all completed nodes in a swarm-plan
    batch_verify = subparsers.add_parser(
        "batch-verify",
        help="Batch-verify all completed nodes in a swarm-plan via parallel Claude subagents.",
    )
    batch_verify.add_argument("plan_name", help="Plan name (e.g. insights-action-items)")
    batch_verify.add_argument("--workers", "-w", type=int, default=3,
                              help="Max parallel verifier agents (default: 3)")
    batch_verify.add_argument("--output", "-o", type=str, default=None,
                              help="Write markdown report to this file (default: stdout)")
    batch_verify.add_argument("--model", type=str, default=None,
                              help="Claude model for verifiers")
    batch_verify.add_argument("--timeout", type=int, default=180,
                              help="Per-agent timeout in seconds (default: 180)")
    batch_verify.add_argument("--list", action="store_true",
                              help="List completed nodes without running verifiers")

    # sql: run a SQL query against AMS virtual tables via sqlite3 CLI
    sql_cmd = subparsers.add_parser("sql", help="Run a SQL query against AMS virtual tables.")
    sql_cmd.add_argument("query", help="SQL query to execute")
    sql_cmd.add_argument("--json", action="store_true", help="Output results as a JSON array")

    # sem_search: semantic free-text search over KE entries
    sem_search_cmd = subparsers.add_parser("sem_search", help="Semantic free-text search over AKC/KE entries.")
    sem_search_cmd.add_argument("query", help="Free-text semantic query")
    sem_search_cmd.add_argument("--top", type=int, default=8, help="Number of semantic matches to return.")
    sem_search_cmd.add_argument("--json", action="store_true", help="Output results as a JSON array")

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.command in {"search", "recall", "deep-search", "retrieve", "latent-search"}:
        db_path = ensure_corpus_available(args.corpus)
        if db_path is None:
            return 1
        query = " ".join(args.keywords)
        record_route = args.record_route
        if record_route is None:
            record_route = os.environ.get("AMS_RECORD_ROUTE", "1") != "0" if args.engine == "rust" else False
        include_latent = args.command != "search"
        if args.engine == "rust":
            if include_latent:
                try:
                    return run_rust_recall(
                        db_path,
                        query,
                        args.top,
                        args.explain,
                        args.backend_root,
                        record_route,
                    )
                except RuntimeError as exc:
                    print(f"WARNING: {exc}", file=sys.stderr)
                    print(
                        f"WARNING: Rust AMS {args.command} failed; falling back to C# MemoryCtl for this query.",
                        file=sys.stderr,
                    )

            # P5: Layer 4 cache for the plain `search` command (non-latent, non-explain,
            # non-route-recording).  Cache is keyed by normalised query + corpus version.
            use_search_cache = not include_latent and not args.explain and not record_route
            if use_search_cache:
                cached = search_cache_lookup(db_path, query, args.backend_root)
                if cached is not None:
                    print(cached, end="" if cached.endswith("\n") else "\n")
                    return 0

            rust_cmd = [
                "agent-query",
                "--input",
                db_path,
                "--q",
                query,
                "--top",
                str(args.top),
            ]
            if args.explain:
                rust_cmd.append("--explain")
            if record_route:
                rust_cmd.append("--record-route")
            if include_latent:
                rust_cmd.append("--include-latent")

            if use_search_cache:
                # Capture output so we can promote it to the cache on success.
                captured = run_rust_ams_capture(*rust_cmd, backend_root=args.backend_root)
                if captured.returncode == 0:
                    print(captured.stdout, end="" if captured.stdout.endswith("\n") else "\n")
                    search_cache_promote(db_path, query, captured.stdout, args.backend_root)
                    return 0
                print(captured.stdout, end="")
                print(captured.stderr, end="", file=sys.stderr)
            else:
                result = run_rust_ams(*rust_cmd, backend_root=args.backend_root)
                if result == 0:
                    return 0

            print(
                f"WARNING: Rust AMS {args.command} failed; falling back to C# MemoryCtl for this query.",
                file=sys.stderr,
            )

        cmd = [
            "agent-query",
            "--db",
            db_path,
            "--q",
            query,
            "--top",
            str(args.top),
        ]
        if args.explain:
            cmd.append("--explain")
        if record_route:
            cmd.append("--record-route")
        return run_memoryctl(*cmd)

    if args.command == "read":
        db_path = ensure_corpus_available(args.corpus)
        if db_path is None:
            return 1
        target = normalize_session_id(args.target)
        disallowed_prefixes = ("chat-msg:", "task-thread:", "smartlist-note:", "smartlist-bucket:")
        if args.target.startswith(disallowed_prefixes):
            print(
                f"ERROR: '{args.target}' is not a chat session ref. Use search/thread/handoff first.",
                file=sys.stderr,
            )
            return 2
        if args.engine == "rust":
            result = run_rust_ams("show-session", "--input", db_path, "--id", target, backend_root=args.backend_root)
            if result == 0:
                return 0
            print(
                "WARNING: Rust AMS read failed; falling back to C# MemoryCtl for this query.",
                file=sys.stderr,
            )
        return run_memoryctl("show-session", "--db", db_path, "--id", target)

    if args.command == "sessions":
        db_path = ensure_corpus_available(args.corpus)
        if db_path is None:
            return 1
        if args.engine == "rust":
            rust_cmd = ["list-sessions", "--input", db_path, "--n", str(args.n)]
            if args.since:
                rust_cmd.extend(["--since", args.since])
            result = run_rust_ams(*rust_cmd, backend_root=args.backend_root)
            if result == 0:
                return 0
            print(
                "WARNING: Rust AMS sessions failed; falling back to C# MemoryCtl for this query.",
                file=sys.stderr,
            )
        cmd = ["list-sessions", "--db", db_path, "--n", str(args.n)]
        if args.since:
            cmd.extend(["--since", args.since])
        return run_memoryctl(*cmd)

    if args.command == "thread":
        db_path = ensure_corpus_available(args.corpus)
        if db_path is None:
            return 1
        if args.engine == "rust":
            result = run_rust_ams("thread-status", "--input", db_path, backend_root=args.backend_root)
            if result == 0:
                return 0
            print(
                "WARNING: Rust AMS thread inspection failed; falling back to C# MemoryCtl for this query.",
                file=sys.stderr,
            )
        return run_memoryctl("thread-status", "--db", db_path)

    if args.command == "handoff":
        if args.engine == "rust":
            result = run_rust_ams(
                "smartlist-inspect",
                "--input",
                factories_db_path(),
                "--path",
                handoff_smartlist_path(),
                "--depth",
                str(args.depth),
                backend_root=args.backend_root,
            )
            if result == 0:
                return 0
            print(
                "WARNING: Rust AMS handoff inspection failed; falling back to C# MemoryCtl for this query.",
                file=sys.stderr,
            )
        return run_memoryctl(
            "smartlist-inspect",
            "--db",
            factories_db_path(),
            "--path",
            handoff_smartlist_path(),
            "--depth",
            str(args.depth),
        )

    if args.command == "breakpoint":
        if args.engine == "rust":
            result = run_rust_ams(
                "smartlist-inspect",
                "--input",
                factories_db_path(),
                "--path",
                breakpoint_factory_path(),
                "--depth",
                str(args.depth),
                backend_root=args.backend_root,
            )
            if result == 0:
                return 0
            print(
                "WARNING: Rust AMS breakpoint inspection failed; falling back to C# MemoryCtl for this query.",
                file=sys.stderr,
            )
        return run_memoryctl(
            "smartlist-inspect",
            "--db",
            factories_db_path(),
            "--path",
            breakpoint_factory_path(),
            "--depth",
            str(args.depth),
        )

    if args.command == "backend":
        db_path = ensure_corpus_available(args.corpus)
        if db_path is None:
            return 1
        rust_cmd = ["backend-status", "--input", db_path]
        result = run_rust_ams(*rust_cmd, backend_root=args.backend_root)
        if result != 0:
            return result
        recover_cmd = ["backend-recover-validate", "--input", db_path]
        if args.assert_clean:
            recover_cmd.append("--assert-clean")
        return run_rust_ams(*recover_cmd, backend_root=args.backend_root)

    if args.command in ("swarm-plan", "callstack"):
        # 'list' is special: aggregate across all per-plan stores to avoid recursion
        # (active_swarm_plan_name calls 'ams.bat swarm-plan list', which would recurse here).
        if args.callstack_command == "list":
            return _swarm_plan_list_all(args)
        # For load-plan, auto-detect the project name from the plan file's "project" field
        # so the correct per-plan store is targeted. The per-plan store is the authoritative
        # location for all plan data — FACTORIES_DB must not be used for plan storage.
        if args.callstack_command == "load-plan" and not getattr(args, "project", None):
            try:
                import json as _json
                with open(args.file) as _f:
                    _plan = _json.load(_f)
                _detected = _plan.get("project")
                if _detected:
                    args.project = _detected
            except Exception:
                pass
        db_path = _swarm_plan_db(getattr(args, "project", None))
        try:
            # Commands supported by the Rust binary — try Rust first, fall back to Python.
            rc = _try_rust_swarm_plan(db_path, args)
            if rc is not None:
                # On successful switch or load-plan, update the plan stack so
                # active_swarm_plan_name() returns the right plan by default.
                if rc == 0 and args.callstack_command in ("switch", "load-plan"):
                    from ams_common import push_plan_stack
                    plan_name = None
                    if args.callstack_command == "switch":
                        plan_name = getattr(args, "name", None)
                    elif args.callstack_command == "load-plan":
                        plan_name = getattr(args, "project", None)
                        if not plan_name:
                            # Detect project name from plan file
                            import json as _json
                            try:
                                with open(args.file, encoding="utf-8") as _f:
                                    plan_name = _json.load(_f).get("project")
                            except Exception:
                                pass
                    if plan_name:
                        push_plan_stack(plan_name)
                return rc
            # Python-only commands (write-note, insert, remove, annotate) fall through here.
            if args.callstack_command == "write-note":
                write_note(db_path, args.bucket, args.title, args.text, args.actor_id, args.backend_root)
                print(f"action=write-note\nbucket={args.bucket}\ntitle={args.title}")
                return 0
            if args.callstack_command == "tag":
                # Writes to shared memory, not the per-plan store.
                shared_db = shared_memory_db_path()
                rust_args = [
                    "swarm-plan-tag", "--input", shared_db,
                    "--plan-name", args.plan_name,
                    "--bucket-path", args.bucket_path,
                ]
                if getattr(args, "summary", None):
                    rust_args += ["--summary", args.summary]
                rust_args += ["--actor-id", args.actor_id]
                return run_rust_ams(*rust_args, backend_root=getattr(args, "backend_root", None))
            if args.callstack_command == "insert":
                return callstack_insert(db_path, args.backend_root, args.name, args.actor_id, args.parent, args.description, args.depends_on)
            if args.callstack_command == "remove":
                return callstack_remove(db_path, args.backend_root, args.node_path, args.actor_id)
            if args.callstack_command == "annotate":
                return callstack_annotate(db_path, args.backend_root, args.node_path, args.title, args.text, args.actor_id)
            if args.callstack_command == "children":
                # children reads the compiled snapshot, not the JSONL store
                plan_name = active_swarm_plan_name()
                snapshot = swarm_plan_snapshot_path(plan_name) if plan_name else db_path
                return swarm_plan_children(snapshot, args.node_filter)
            if args.callstack_command == "new":
                return swarm_plan_new(args.intent, effort=args.effort, dry_run=args.dry_run)
            if args.callstack_command == "load-plan":
                # Python-only path: --into-node was set (Rust returned None above)
                return callstack_load_plan(
                    db_path, args.backend_root, args.file, args.actor_id,
                    into_active=getattr(args, "into_active", False),
                    into_node_path=getattr(args, "into_node", None),
                )
        except RuntimeError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
        print(f"ERROR: unsupported callstack command '{args.callstack_command}'.", file=sys.stderr)
        return 2

    if args.command == "bugreport-list":
        db_path = ensure_corpus_available(args.corpus)
        if db_path is None:
            return 1
        cmd_args = ["bugreport-list", "--db", db_path]
        if hasattr(args, "status") and args.status:
            cmd_args += ["--status", args.status]
        return run_memoryctl(*cmd_args)

    if args.command == "bugreport-search":
        db_path = ensure_corpus_available(args.corpus)
        if db_path is None:
            return 1
        cmd_args = ["bugreport-search", "--db", db_path, "--query", args.query]
        if hasattr(args, "status") and args.status:
            cmd_args += ["--status", args.status]
        return run_memoryctl(*cmd_args)

    if args.command == "bugreport-show":
        db_path = ensure_corpus_available(args.corpus)
        if db_path is None:
            return 1
        return run_memoryctl("bugreport-show", "--db", db_path, "--bug-id", args.bug_id)

    if args.command == "atlas":
        db_path = ensure_corpus_available(args.corpus)
        if db_path is None:
            return 1
        sub = args.atlas_command
        if sub == "page":
            return run_rust_ams("atlas-page", "--input", db_path, "--id", args.page_id)
        if sub == "search":
            q = " ".join(args.query)
            return run_rust_ams("atlas-search", "--input", db_path, "--q", q, "--top", str(args.top))
        if sub == "expand":
            return run_rust_ams("atlas-expand", "--input", db_path, "--id", args.ref_id)
        if sub == "define":
            rust_args = ["atlas-define", "--input", db_path, "--name", args.name]
            if args.description:
                rust_args += ["--description", args.description]
            for s in args.scales:
                rust_args += ["--scale", s]
            return run_rust_ams(*rust_args)
        if sub == "show":
            return run_rust_ams("atlas-show", "--input", db_path, "--name", args.name)
        if sub == "list":
            return run_rust_ams("atlas-list", "--input", db_path)
        if sub == "list-at-scale":
            return run_rust_ams("atlas-list-at-scale", "--input", db_path, "--name", args.name, "--scale", str(args.scale))
        if sub == "navigate":
            return run_rust_ams("atlas-navigate", "--input", db_path, "--name", args.name, "--id", args.id)
        print(f"ERROR: unsupported atlas command '{sub}'.", file=sys.stderr)
        return 2

    if args.command == "cache":
        db_path = factories_db_path()
        sub = args.cache_command
        if sub == "register-tool":
            return run_rust_ams(
                "cache-register-tool", "--input", db_path,
                "--tool-id", args.tool_id,
                "--tool-version", args.tool_version,
                "--actor-id", args.actor_id,
            )
        if sub == "register-source":
            rust_args = [
                "cache-register-source", "--input", db_path,
                "--source-id", args.source_id,
                "--actor-id", args.actor_id,
            ]
            if args.fingerprint:
                rust_args += ["--fingerprint", args.fingerprint]
            return run_rust_ams(*rust_args)
        if sub == "promote":
            return run_rust_ams(
                "cache-promote", "--input", db_path,
                "--tool-id", args.tool,
                "--tool-version", args.tool_version,
                "--source-id", args.source,
                "--in-situ-ref", args.text,
                "--actor-id", args.actor_id,
            )
        if sub == "lookup":
            return run_rust_ams(
                "cache-lookup", "--input", db_path,
                "--tool-id", args.tool,
                "--source-id", args.source,
                "--format", args.format,
            )
        print(f"ERROR: unsupported cache command '{sub}'.", file=sys.stderr)
        return 2

    if args.command == "ke":
        return handle_ke(args)

    if args.command == "proj-dir":
        return handle_proj_dir(args)

    if args.command == "roadmap":
        return handle_roadmap(args)

    if args.command == "dream":
        db_path = factories_db_path()
        out_path = args.out or str(Path(repo_root()) / "CLAUDE.local.md")

        rc = run_rust_ams(
            "dream-schedule", "--input", db_path,
            "--max-touches", str(args.max_touches),
        )
        if rc != 0:
            return rc

        rc = run_rust_ams("dream-cluster", "--input", db_path)
        if rc != 0:
            return rc

        return run_rust_ams(
            "dream-generate-md", "--input", db_path,
            "--out", out_path,
            "--max-topics", str(args.max_topics),
            "--max-sessions", str(args.max_sessions),
        )

    if args.command == "fep":
        db_path = factories_db_path()
        sub = args.fep_command
        if sub == "cache-report":
            return run_rust_ams(
                "fep-cache-report", "--input", db_path,
                "--window-hours", str(args.window_hours),
            )
        print(f"ERROR: unsupported fep command '{sub}'.", file=sys.stderr)
        return 2

    if args.command == "prune":
        return handle_prune(args)

    if args.command == "batch-verify":
        return handle_batch_verify(args)

    if args.command == "sql":
        return handle_sql(args)

    if args.command == "sem_search":
        return handle_sem_search(args)

    print(f"ERROR: unsupported command '{args.command}'.", file=sys.stderr)
    return 2


def handle_ke(args: argparse.Namespace) -> int:
    sub = args.ke_command
    db = ke_db_path()  # dedicated KE store — NOT factories (factories = SmartList templates only)

    if sub == "write":
        rust_args = [
            "ke-write", "--input", db,
            "--scope", args.scope,
            "--kind", args.kind,
            "--text", args.text,
            "--confidence", str(args.confidence),
        ]
        if args.summary:
            rust_args += ["--summary", args.summary]
        for tag in args.tags:
            rust_args += ["--tag", tag]
        for watch in args.watch_paths:
            rust_args += ["--watch", watch]
        if args.actor_id:
            rust_args += ["--actor-id", args.actor_id]
        return run_rust_ams(*rust_args)

    if sub == "read":
        rust_args = ["ke-read", "--input", db, "--scope", args.scope]
        if args.include_stale:
            rust_args.append("--include-stale")
        return run_rust_ams(*rust_args)

    if sub == "search":
        rust_args = ["ke-search", "--input", db, "--top", str(args.top)]
        for term in args.query:
            rust_args += ["--query", term]
        if args.scope:
            rust_args += ["--scope", args.scope]
        if args.kind:
            rust_args += ["--kind", args.kind]
        if args.include_stale:
            rust_args.append("--include-stale")
        return run_rust_ams(*rust_args)

    if sub == "sem":
        return handle_ke_sem(args)

    if sub == "context":
        rust_args = [
            "ke-context", "--input", db,
            "--max-entries", str(args.max_entries),
            "--max-chars", str(args.max_chars),
        ]
        if args.scope:
            rust_args += ["--scope", args.scope]
        return run_rust_ams(*rust_args)

    if sub == "bootstrap":
        rust_args = ["ke-bootstrap", "--input", db, "--repo-root", str(repo_root())]
        if args.overwrite:
            rust_args.append("--overwrite")
        return run_rust_ams(*rust_args)

    print(f"ERROR: unsupported ke command '{sub}'.", file=sys.stderr)
    return 2


def handle_proj_dir(args: argparse.Namespace) -> int:
    sub = args.proj_dir_command
    db = factories_db_path()

    if sub == "build":
        # Full build sequence: ingest → build-dirs → stats → register-atlas
        rc = run_rust_ams("projdir-ingest", "--input", db, "--repo-root", str(repo_root()))
        if rc != 0:
            return rc
        rc = run_rust_ams("projdir-build-dirs", "--input", db)
        if rc != 0:
            return rc
        rc = run_rust_ams("projdir-stats", "--input", db)
        if rc != 0:
            return rc
        return run_rust_ams("projdir-register-atlas", "--input", db)

    if sub == "tree":
        rust_args = ["projdir-tree", "--input", db, "--depth", str(args.depth)]
        if args.path:
            rust_args += ["--path", args.path]
        return run_rust_ams(*rust_args)

    if sub == "search":
        query_terms = args.query if isinstance(args.query, list) else [args.query]
        rust_args = ["projdir-search", "--input", db]
        for term in query_terms:
            rust_args += ["--query", term]
        return run_rust_ams(*rust_args)

    if sub == "doc":
        return run_rust_ams("projdir-doc", "--input", db, "--path", args.path)

    if sub == "stats":
        return run_rust_ams("projdir-stats", "--input", db)

    if sub == "context":
        return run_rust_ams("projdir-context", "--input", db, "--depth", str(args.depth))

    print(f"ERROR: unsupported proj-dir command '{sub}'.", file=sys.stderr)
    return 2


def _load_roadmap_store() -> list[dict]:
    """Read roadmap.memory.jsonl and return all AMS object dicts."""
    import json
    path = Path(repo_root()) / "shared-memory" / "system-memory" / "roadmap.memory.jsonl"
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("objects", [])


def _roadmap_semantic(node: dict) -> dict:
    """Extract semanticPayload dict from a node (handles nested structure)."""
    payload = node.get("semanticPayload") or {}
    if isinstance(payload, str):
        import json
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}
    return payload


def handle_roadmap(args: argparse.Namespace) -> int:
    """Handle `ams.bat roadmap` subcommands."""
    sub = args.roadmap_command
    nodes = _load_roadmap_store()

    if not nodes:
        roadmap_path = Path(repo_root()) / "shared-memory" / "system-memory" / "roadmap.memory.jsonl"
        print(f"[roadmap] No roadmap store found at {roadmap_path}.")
        print("[roadmap] Run `ams.bat roadmap context` after the store is seeded.")
        return 0

    # Build a lookup by objectKind (top-level field), payload carries the data
    concepts: list[dict] = []
    sprints: list[dict] = []
    for node in nodes:
        ok = node.get("objectKind", "")
        sp = _roadmap_semantic(node)
        if ok == "roadmap_concept":
            concepts.append(sp)
        elif ok == "roadmap_sprint":
            sprints.append(sp)

    if sub == "list":
        if not concepts:
            print("[roadmap] No concept nodes found.")
            return 0
        for c in sorted(concepts, key=lambda x: x.get("slug", "")):
            slug = c.get("slug", "?")
            status = c.get("status", "unknown")
            name = c.get("name", slug)
            print(f"{slug:<20} {status:<10} {name}")
        return 0

    if sub == "context":
        active_statuses = {"active", "planned"}
        shown = [c for c in concepts if c.get("status") in active_statuses]
        if not shown:
            print("[roadmap] No active or planned concepts.")
            return 0
        for c in sorted(shown, key=lambda x: (x.get("status", ""), x.get("slug", ""))):
            slug = c.get("slug", "?")
            status = c.get("status", "?")
            name = c.get("name", slug)
            intent = c.get("intent", "")
            active_sprints = [s for s in sprints
                              if s.get("concept_ref") == slug
                              and s.get("status") in {"active"}]
            sprint_count = len(active_sprints)
            print(f"## {name} [{status}]")
            if intent:
                print(f"   {intent}")
            if sprint_count:
                print(f"   Active sprints: {sprint_count}")
            print()
        return 0

    if sub == "concept":
        slug = args.slug
        match = next((c for c in concepts if c.get("slug") == slug), None)
        if not match:
            print(f"[roadmap] Concept '{slug}' not found. Use `ams.bat roadmap list` to see available slugs.")
            return 1
        name = match.get("name", slug)
        status = match.get("status", "unknown")
        intent = match.get("intent", "")
        summary = match.get("summary", "")
        tdd_layer = match.get("tdd_layer", "")
        doc_refs = match.get("doc_refs") or []
        relevance = match.get("relevance") or []

        print(f"# {name}  [{status}]")
        if tdd_layer:
            print(f"TDD Layer: {tdd_layer}")
        print()
        if intent:
            print(f"**Intent:** {intent}")
        if summary:
            print(f"**Summary:** {summary}")
        if doc_refs:
            print()
            print("**Docs:**")
            for ref in doc_refs:
                print(f"  - {ref}")
        if relevance:
            print()
            print("**Relevance notes:**")
            for note in relevance:
                print(f"  - {note}")

        concept_sprints = [s for s in sprints if s.get("concept_ref") == slug]
        if concept_sprints:
            print()
            print("**Sprints:**")
            for s in concept_sprints:
                ref = s.get("swarm_plan_ref", "?")
                s_status = s.get("status", "?")
                title = s.get("title", ref)
                print(f"  [{s_status}] {title} ({ref})")
        return 0

    if sub == "status":
        target_status = args.status
        matching_concepts = [c for c in concepts if c.get("status") == target_status]
        matching_sprints = [s for s in sprints if s.get("status") == target_status]

        if not matching_concepts and not matching_sprints:
            print(f"[roadmap] No items with status '{target_status}'.")
            return 0

        if matching_concepts:
            print(f"## Concepts [{target_status}]")
            for c in matching_concepts:
                print(f"  {c.get('slug', '?'):<20} {c.get('name', '')}")

        if matching_sprints:
            print(f"\n## Sprints [{target_status}]")
            for s in matching_sprints:
                ref = s.get("swarm_plan_ref", "?")
                title = s.get("title", ref)
                concept = s.get("concept_ref", "")
                print(f"  {ref:<30} {concept:<15} {title}")
        return 0

    if sub == "atlas":
        level = args.level
        # Sort concepts by layer asc, then slug alphabetically
        def _layer_key(c: dict):
            return (c.get("layer") if c.get("layer") is not None else 999, c.get("slug", ""))
        sorted_concepts = sorted(concepts, key=_layer_key)

        if level == "overview":
            for c in sorted_concepts:
                lyr = c.get("layer", "?")
                slug = c.get("slug", "?")
                status = c.get("status", "?")
                intent = (c.get("intent") or "")[:80]
                print(f"[L{lyr}] {slug:<20} {status:<10} {intent}")
            return 0

        if level == "mid":
            for c in sorted_concepts:
                lyr = c.get("layer", "?")
                slug = c.get("slug", "?")
                status = c.get("status", "?")
                intent = (c.get("intent") or "")[:80]
                print(f"[L{lyr}] {slug:<20} {status:<10} {intent}")
                active_sprints = [s for s in sprints
                                  if s.get("concept_ref") == slug
                                  and s.get("status") in {"active", "planned"}]
                for s in active_sprints:
                    ref = s.get("swarm_plan_ref", "?")
                    s_status = s.get("status", "?")
                    title = s.get("title", ref)
                    print(f"    [{s_status}] {title} ({ref})")
            return 0

        if level == "detail":
            slug = args.slug
            if not slug:
                print("[roadmap] atlas detail requires a concept slug.", file=sys.stderr)
                return 1
            # Collect notes
            notes: list[dict] = []
            for node in nodes:
                if node.get("objectKind") == "roadmap_note":
                    sp = _roadmap_semantic(node)
                    notes.append(sp)
            match = next((c for c in concepts if c.get("slug") == slug), None)
            if not match:
                print(f"[roadmap] Concept '{slug}' not found.", file=sys.stderr)
                return 1
            name = match.get("name", slug)
            status = match.get("status", "unknown")
            intent = match.get("intent", "")
            summary = match.get("summary", "")
            tdd_layer = match.get("tdd_layer", "")
            doc_refs = match.get("doc_refs") or []
            relevance = match.get("relevance") or []
            print(f"# {name}  [{status}]")
            if tdd_layer:
                print(f"TDD Layer: {tdd_layer}")
            print()
            if intent:
                print(f"**Intent:** {intent}")
            if summary:
                print(f"**Summary:** {summary}")
            if doc_refs:
                print()
                print("**Docs:**")
                for ref in doc_refs:
                    print(f"  - {ref}")
            if relevance:
                print()
                print("**Relevance notes:**")
                for note in relevance:
                    print(f"  - {note}")
            concept_sprints = [s for s in sprints if s.get("concept_ref") == slug]
            if concept_sprints:
                print()
                print("**Sprints:**")
                for s in concept_sprints:
                    ref = s.get("swarm_plan_ref", "?")
                    s_status = s.get("status", "?")
                    title = s.get("title", ref)
                    print(f"  [{s_status}] {title} ({ref})")
            concept_notes = [n for n in notes if n.get("concept_ref") == slug]
            if concept_notes:
                print()
                print("**Notes:**")
                for n in concept_notes:
                    title = n.get("title", "")
                    text = n.get("text", "")
                    print(f"  - {title}: {text}" if title else f"  - {text}")
            return 0

        print(f"ERROR: unsupported atlas level '{level}'.", file=sys.stderr)
        return 2

    if sub == "layer":
        target_layer = args.layer_num
        matched = [c for c in concepts if c.get("layer") == target_layer]
        if not matched:
            print(f"[roadmap] No concepts at layer {target_layer}.")
            return 0
        for c in sorted(matched, key=lambda x: x.get("slug", "")):
            slug = c.get("slug", "?")
            name = c.get("name", slug)
            intent = (c.get("intent") or "")[:80]
            concept_sprints = [s for s in sprints if s.get("concept_ref") == slug]
            print(f"{slug:<20} {name:<25} sprints:{len(concept_sprints)}  {intent}")
        return 0

    if sub == "theme":
        theme_slug = args.theme_slug
        # Load theme definitions from store if present
        theme_names: dict[str, str] = {
            "memory-fabric": "Memory Fabric",
            "retrieval": "Retrieval & Abstraction",
            "execution": "Execution & Orchestration",
            "learning": "Learning & Adaptation",
            "simulation": "Simulation & Scale",
            "agent-os": "Agent OS",
        }
        for node in nodes:
            if node.get("objectKind") == "roadmap_theme":
                sp = _roadmap_semantic(node)
                sl = sp.get("slug", "")
                nm = sp.get("name", sl)
                if sl:
                    theme_names[sl] = nm

        theme_name = theme_names.get(theme_slug, theme_slug)
        matched = [c for c in concepts if theme_slug in (c.get("themes") or [])]
        if not matched:
            print(f"[roadmap] No concepts in theme '{theme_slug}'.")
            return 0
        print(f"## Theme: {theme_name}")
        for c in sorted(matched, key=lambda x: (x.get("layer") if x.get("layer") is not None else 999, x.get("slug", ""))):
            slug = c.get("slug", "?")
            name = c.get("name", slug)
            intent = (c.get("intent") or "")[:80]
            print(f"  {slug:<20} {name:<25} {intent}")
        return 0

    if sub == "deps":
        dep_slug = args.dep_slug
        match = next((c for c in concepts if c.get("slug") == dep_slug), None)
        if not match:
            print(f"[roadmap] Concept '{dep_slug}' not found.", file=sys.stderr)
            return 1
        depends_on = match.get("depends_on") or []
        enables = match.get("enables") or []
        print(f"Depends on: {', '.join(depends_on) if depends_on else '(none)'}")
        print(f"Enables:    {', '.join(enables) if enables else '(none)'}")
        return 0

    print(f"ERROR: unsupported roadmap command '{sub}'.", file=sys.stderr)
    return 2


def handle_batch_verify(args: argparse.Namespace) -> int:
    """Delegate `ams batch-verify` to scripts/batch-verify-plan.py."""
    script = Path(__file__).resolve().parent / "batch-verify-plan.py"
    cmd = [sys.executable, str(script), args.plan_name]
    cmd += ["--workers", str(args.workers)]
    if args.output:
        cmd += ["--output", args.output]
    if args.model:
        cmd += ["--model", args.model]
    cmd += ["--timeout", str(args.timeout)]
    if args.list:
        cmd.append("--list")
    result = subprocess.run(cmd, cwd=str(Path(__file__).resolve().parent.parent))
    return result.returncode


def handle_prune(args: argparse.Namespace) -> int:
    """Handle `ams.bat prune tombstones` and `ams.bat prune sessions`."""
    import tempfile

    sub = args.prune_command
    db = corpus_db(getattr(args, "corpus", "all"))
    if db is None:
        print("ERROR: could not locate the AMS corpus database.", file=sys.stderr)
        return 1

    if sub == "tombstones":
        max_age = args.max_age_days
        rc = run_rust_ams("session-tombstone-expire", "--input", db, "--max-age-days", str(max_age))
        return rc

    if sub == "sessions":
        older_than = args.older_than_days
        # Find session_ref objects older than `older_than` days using corpus-inspect.
        cmd = build_rust_ams_cmd("corpus-inspect", "--input", db)
        if cmd is None:
            print("WARNING: ams-core-kernel binary not found; skipping session prune.", file=sys.stderr)
            return 0
        try:
            import datetime
            result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
            if result.returncode != 0:
                print(f"WARNING: corpus-inspect failed (rc={result.returncode}); skipping session prune.", file=sys.stderr)
                return 0

            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=older_than)
            candidate_ids: list[str] = []
            for line in result.stdout.splitlines():
                # corpus-inspect emits lines like: object_id=... object_kind=... created_at=...
                if "object_kind=session_ref" not in line and "object_kind=session" not in line:
                    continue
                parts = dict(
                    p.split("=", 1) for p in line.split() if "=" in p
                )
                oid = parts.get("object_id", "")
                created_at_str = parts.get("created_at", "")
                if not oid or not created_at_str:
                    continue
                try:
                    created_at = datetime.datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
                    if created_at < cutoff:
                        candidate_ids.append(oid)
                except ValueError:
                    continue

            if not candidate_ids:
                print(f"prune-sessions: no sessions older than {older_than} days", flush=True)
                return 0

            with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as tmp:
                tmp.write("\n".join(candidate_ids))
                tmp_path = tmp.name

            try:
                rc = run_rust_ams("session-prune-batch", "--input", db, "--ids-file", tmp_path)
            finally:
                try:
                    import os as _os
                    _os.unlink(tmp_path)
                except OSError:
                    pass
            return rc

        except Exception as exc:
            print(f"WARNING: session prune failed: {exc}; skipping.", file=sys.stderr)
            return 0

    print(f"ERROR: unsupported prune command '{sub}'.", file=sys.stderr)
    return 2


def handle_sql(args: argparse.Namespace) -> int:
    """Run a SQL query against AMS virtual tables via sqlite3 CLI."""
    import shutil

    sqlite3_bin = shutil.which("sqlite3")
    if sqlite3_bin is None:
        print(
            "ERROR: sqlite3 not found on PATH.\n"
            "Install SQLite tools and ensure sqlite3 is accessible.\n"
            "  Windows: winget install SQLite.SQLite\n"
            "  macOS:   brew install sqlite\n"
            "  Linux:   apt-get install sqlite3",
            file=sys.stderr,
        )
        return 1

    # Resolve path to the vtable extension relative to repo root
    repo_root = Path(__file__).parent.parent
    vtable_ext = repo_root / "dist" / "libams_vtable"  # sqlite3 appends .dll/.so/.dylib

    load_cmd = f".load {vtable_ext.as_posix()}"

    cmd = [sqlite3_bin]
    if args.json:
        cmd += ["-json"]
    cmd += ["-cmd", load_cmd, ":memory:", args.query]

    result = subprocess.run(cmd)
    return result.returncode


def _ke_ams_json_path() -> Path:
    return Path(ke_db_path()).with_name("ke.memory.ams.json")


def _normalize_smartlist_segment(segment: str) -> str:
    normalized = re.sub(r"[^a-z0-9-]+", "-", segment.strip().lower()).strip("-")
    if not normalized:
        raise RuntimeError(f"invalid SmartList path segment '{segment}'")
    return normalized


def _normalize_smartlist_path(path: str) -> str:
    normalized = path.replace("\\", "/")
    parts = [_normalize_smartlist_segment(segment) for segment in normalized.split("/") if segment.strip()]
    if not parts:
        raise RuntimeError("SmartList path is required")
    if parts[0] != "smartlist":
        parts.insert(0, "smartlist")
    return "/".join(parts)


def _semantic_embed_query(text: str) -> list[float]:
    import urllib.error
    import urllib.request

    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")

    payload = json.dumps({"model": "text-embedding-3-small", "input": text}).encode("utf-8")
    req = urllib.request.Request(
        "https://api.openai.com/v1/embeddings",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI returned HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"failed to reach OpenAI embeddings API: {exc}") from exc
    return [float(v) for v in data["data"][0]["embedding"]]


def _cosine_distance(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 1.0
    return 1.0 - (dot / (norm_a * norm_b))


def _load_ke_semantic_store() -> tuple[dict, dict[str, dict], dict[str, list[str]], dict[str, list[str]]]:
    ke_file = _ke_ams_json_path()
    if not ke_file.exists():
        raise RuntimeError(f"KE semantic store not found: {ke_file}")

    with open(ke_file, encoding="utf-8") as f:
        store = json.load(f)

    objects_by_id = {obj.get("objectId", ""): obj for obj in store.get("objects", [])}
    link_nodes_by_id = {
        node.get("linkNodeId", ""): node
        for node in store.get("linkNodes", [])
        if node.get("linkNodeId")
    }
    members_by_container: dict[str, list[str]] = {}
    containers_by_object: dict[str, list[str]] = {}
    for container in store.get("containers", []):
        container_id = container.get("containerId", "")
        if not container_id:
            continue
        members: list[str] = []
        current = container.get("headLinknodeId")
        seen: set[str] = set()
        while current and current not in seen:
            seen.add(current)
            node = link_nodes_by_id.get(current)
            if not node:
                break
            object_id = node.get("objectId")
            if object_id:
                members.append(object_id)
                containers_by_object.setdefault(object_id, []).append(container_id)
            current = node.get("nextLinknodeId")
        members_by_container[container_id] = members

    return store, objects_by_id, members_by_container, containers_by_object


def _provenance_source_paths(prov: dict) -> list[str]:
    paths: list[str] = []
    for path in prov.get("source_paths", []) or []:
        if isinstance(path, str) and path not in paths:
            paths.append(path)
    for fp in prov.get("freshness_fingerprints", []) or []:
        if not isinstance(fp, str):
            continue
        parts = fp.split(":", 3)
        if len(parts) == 4 and parts[0] == "file" and parts[1] not in paths:
            paths.append(parts[1])
    bootstrap_source = prov.get("bootstrap_source")
    if isinstance(bootstrap_source, str) and bootstrap_source not in paths:
        paths.append(bootstrap_source)
    return paths


def _file_ref_path(member_id: str, obj: dict) -> str:
    payload = obj.get("semanticPayload") or {}
    prov = payload.get("provenance") or {}
    path = prov.get("path")
    if isinstance(path, str) and path:
        return path
    in_situ = obj.get("inSituRef")
    if isinstance(in_situ, str) and in_situ:
        return in_situ
    if member_id.startswith("file:"):
        return member_id[len("file:"):]
    return member_id


def _ke_sources_for_object(
    object_id: str,
    obj: dict,
    objects_by_id: dict[str, dict],
    members_by_container: dict[str, list[str]],
    containers_by_object: dict[str, list[str]],
) -> tuple[list[str], list[str]]:
    source_paths: list[str] = []
    relation_bucket_paths: list[str] = []
    for container_id in containers_by_object.get(object_id, []):
        if not container_id.startswith("smartlist-members:smartlist/ke-source/"):
            continue
        relation_bucket_paths.append(container_id[len("smartlist-members:"):])
        for member_id in members_by_container.get(container_id, []):
            if member_id == object_id:
                continue
            member_obj = objects_by_id.get(member_id, {})
            if member_id.startswith("file:") or member_obj.get("objectKind") == "file":
                path = _file_ref_path(member_id, member_obj)
                if path not in source_paths:
                    source_paths.append(path)

    if not source_paths:
        prov = (obj.get("semanticPayload") or {}).get("provenance") or {}
        source_paths = _provenance_source_paths(prov)

    relation_bucket_paths = sorted(set(relation_bucket_paths))
    return source_paths, relation_bucket_paths


def _ke_semantic_rows() -> list[dict[str, object]]:
    _, objects_by_id, members_by_container, containers_by_object = _load_ke_semantic_store()
    rows: list[dict[str, object]] = []
    for obj in objects_by_id.values():
        if obj.get("objectKind") != "knowledge-entry":
            continue
        payload = obj.get("semanticPayload") or {}
        embedding = payload.get("embedding")
        if not embedding:
            continue
        prov = payload.get("provenance") or {}
        scope = prov.get("scope", "")
        kind = prov.get("kind", "")
        summary = payload.get("summary") or ""
        text = obj.get("inSituRef", "") or ""
        snippet = summary if summary else text[:220]
        source_paths, relation_bucket_paths = _ke_sources_for_object(
            obj.get("objectId", ""),
            obj,
            objects_by_id,
            members_by_container,
            containers_by_object,
        )
        rows.append({
            "object_id": obj.get("objectId", ""),
            "scope": scope,
            "kind": kind,
            "snippet": snippet,
            "embedding": [float(v) for v in embedding],
            "source_paths": source_paths,
            "relation_bucket_paths": relation_bucket_paths,
        })
    return rows


def _preferred_ke_kind_order(kind: str) -> int:
    order = {
        "purpose": 0,
        "decision": 1,
        "data-model": 2,
        "api": 3,
        "prerequisites": 4,
        "failure-modes": 5,
        "test-guide": 6,
    }
    return order.get(kind, 99)


def _format_semantic_rows(rows: list[dict[str, object]], json_output: bool) -> int:
    rendered = []
    for row in rows:
        rendered.append({
            "scope": row["scope"],
            "kind": row["kind"],
            "snippet": row["snippet"],
            "dist": row["dist"],
            "source_paths": row.get("source_paths", []),
            "relation_bucket_paths": row.get("relation_bucket_paths", []),
        })

    if json_output:
        print(json.dumps(rendered, ensure_ascii=False))
    else:
        for row in rendered:
            sources = ",".join(row["source_paths"]) if row["source_paths"] else "-"
            print(f"{row['scope']}|{row['kind']}|{row['snippet']}|{row['dist']}|sources={sources}")
    return 0


def handle_ke_sem(args: argparse.Namespace) -> int:
    try:
        rows = _ke_semantic_rows()
    except Exception as exc:
        print(f"ERROR: failed to read KE semantic store: {exc}", file=sys.stderr)
        return 1

    candidates = [row for row in rows if row["scope"] == args.scope]
    if args.kind:
        candidates = [row for row in candidates if row["kind"] == args.kind]
    if not candidates:
        anchor_desc = f"{args.scope} [{args.kind}]" if args.kind else args.scope
        print(f"ERROR: no semantic anchor found for {anchor_desc}", file=sys.stderr)
        return 1

    if args.kind:
        anchor = candidates[0]
    else:
        anchor = sorted(candidates, key=lambda row: (_preferred_ke_kind_order(str(row["kind"])),))[0]

    results: list[dict[str, object]] = []
    for row in rows:
        if row["object_id"] == anchor["object_id"]:
            continue
        scored = dict(row)
        scored["dist"] = round(_cosine_distance(anchor["embedding"], row["embedding"]), 4)
        scored.pop("embedding", None)
        results.append(scored)

    results.sort(key=lambda row: row["dist"])
    return _format_semantic_rows(results[: args.top], args.json_output)


def handle_sem_search(args: argparse.Namespace) -> int:
    """Run a semantic free-text search over KE entries and surface related file refs."""
    try:
        query_embedding = _semantic_embed_query(args.query)
        rows = _ke_semantic_rows()
    except Exception as exc:
        print(f"ERROR: semantic search failed: {exc}", file=sys.stderr)
        return 1

    results: list[dict[str, object]] = []
    for row in rows:
        scored = dict(row)
        scored["dist"] = round(_cosine_distance(query_embedding, row["embedding"]), 4)
        scored.pop("embedding", None)
        results.append(scored)

    results.sort(key=lambda row: row["dist"])
    return _format_semantic_rows(results[: args.top], args.json)


if __name__ == "__main__":
    sys.exit(main())
