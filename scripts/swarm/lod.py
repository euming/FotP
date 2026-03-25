"""Level-of-Detail (LOD) summarizer and context injector.

The LOD system gives agents compressed views of distant graph regions while
providing full detail for their local neighborhood. This mimics how GPUs use
mipmaps — nearby textures are high-res, distant ones are low-res.

- summarize_subtree: produces a compressed text summary of a SmartList subtree
- inject_context: builds a context block for an agent combining full local
  neighborhood + LOD summaries for distant regions
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ams_common import build_rust_ams_cmd, rust_backend_env

from .locality import read_neighborhood


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


def _estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 characters per token for English text."""
    return max(1, len(text) // 4)


def summarize_subtree(
    input_path: str,
    node_path: str,
    max_depth: int = 2,
    token_budget: int | None = None,
    backend_root: str | None = None,
) -> str:
    """Produce a compressed text summary of a SmartList subtree.

    Walks the subtree up to max_depth levels, collecting titles and
    structure. Returns a human-readable summary string suitable for
    injection into agent context windows.

    If token_budget is provided, the summary is progressively truncated
    to fit within the budget (approximate). When the full tree exceeds
    the budget, deeper levels are collapsed first, then lines are
    trimmed from the bottom.
    """
    lines: list[str] = []
    _walk_subtree(input_path, node_path, 0, max_depth, lines, backend_root)
    if not lines:
        return f"[empty subtree at {node_path}]"

    full_summary = "\n".join(lines)

    if token_budget is None:
        return full_summary

    # If within budget, return as-is
    if _estimate_tokens(full_summary) <= token_budget:
        return full_summary

    # Progressive truncation: reduce max_depth until it fits
    for reduced_depth in range(max_depth - 1, 0, -1):
        lines = []
        _walk_subtree(input_path, node_path, 0, reduced_depth, lines, backend_root)
        candidate = "\n".join(lines)
        if _estimate_tokens(candidate) <= token_budget:
            return candidate

    # Still too large: hard-truncate lines from the bottom
    result_lines: list[str] = []
    used = 0
    for line in lines:
        line_tokens = _estimate_tokens(line + "\n")
        if used + line_tokens > token_budget:
            result_lines.append(f"  [...truncated to ~{token_budget} tokens]")
            break
        result_lines.append(line)
        used += line_tokens

    return "\n".join(result_lines) if result_lines else f"[subtree at {node_path}, budget too small]"


def _walk_subtree(
    input_path: str,
    path: str,
    current_depth: int,
    max_depth: int,
    lines: list[str],
    backend_root: str | None,
) -> None:
    """Recursively walk a subtree collecting summary lines."""
    indent = "  " * current_depth
    # Get the node's short name from its path
    short_name = path.rsplit("/", 1)[-1] if "/" in path else path
    lines.append(f"{indent}- {short_name}")

    if current_depth >= max_depth:
        # At max depth, just indicate there may be more
        result = _run_kernel(
            backend_root,
            "smartlist-browse",
            "--input", input_path,
            "--path", path,
        )
        if result.returncode == 0:
            children = [l.strip() for l in result.stdout.strip().splitlines()
                        if l.strip() and not l.strip().startswith("count=")]
            if children:
                lines.append(f"{indent}  [...{len(children)} children]")
        return

    # Browse children
    result = _run_kernel(
        backend_root,
        "smartlist-browse",
        "--input", input_path,
        "--path", path,
    )
    if result.returncode != 0:
        return

    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("count="):
            continue
        # Extract name and build child path
        child_name = _extract_browse_name(line)
        child_path = f"{path}/{child_name}" if child_name else line
        _walk_subtree(input_path, child_path, current_depth + 1, max_depth, lines, backend_root)


def _extract_browse_name(browse_line: str) -> str | None:
    """Extract the 'name' field from a smartlist-browse output line."""
    for token in browse_line.split():
        if token.startswith("name="):
            return token.split("=", 1)[1]
    return None


def _format_neighborhood(neighborhood: dict) -> str:
    """Format a neighborhood dict as a human-readable text block."""
    lines: list[str] = []
    lines.append(f"Home node: {neighborhood['node']}")
    if neighborhood["parent"]:
        lines.append(f"Parent: {neighborhood['parent']}")
    if neighborhood["siblings"]:
        lines.append(f"Siblings ({len(neighborhood['siblings'])}):")
        for sib in neighborhood["siblings"]:
            name = sib.get("name", str(sib))
            lines.append(f"  - {name}")
    if neighborhood["children"]:
        lines.append(f"Children ({len(neighborhood['children'])}):")
        for child in neighborhood["children"]:
            name = child.get("name", str(child))
            lines.append(f"  - {name}")
    else:
        lines.append("Children: (none)")
    if neighborhood["observations"]:
        lines.append(f"Observations ({len(neighborhood['observations'])}):")
        for obs in neighborhood["observations"]:
            name = obs.get("name", str(obs))
            lines.append(f"  - {name}")
    return "\n".join(lines)


def inject_context(
    input_path: str,
    agent_ref: str,
    home_node_path: str,
    sibling_paths: list[str] | None = None,
    lod_depth: int = 2,
    token_budget: int | None = None,
    backend_root: str | None = None,
) -> str:
    """Build a context block for an agent combining local detail + LOD summaries.

    The context has two sections:
    1. LOCAL: full neighborhood around the agent's home node (verbatim)
    2. LOD: compressed summaries of sibling/distant subtrees

    If *token_budget* is provided, the local neighborhood always takes priority.
    The remaining budget is split evenly across distant LOD summaries. When no
    budget is given, everything is included without truncation.

    Returns a formatted context string ready for prompt injection.
    """
    sections: list[str] = []

    # Section 1: Local neighborhood (full detail, always included)
    neighborhood = read_neighborhood(
        input_path, home_node_path, backend_root=backend_root,
    )
    local_text = _format_neighborhood(neighborhood)
    sections.append("## Local Neighborhood (full detail)")
    sections.append(local_text)

    # Compute remaining budget for LOD section
    lod_budget: int | None = None
    if token_budget is not None:
        local_tokens = _estimate_tokens("\n".join(sections))
        lod_budget = max(0, token_budget - local_tokens)

    # Section 2: LOD summaries for distant regions
    distant = [p for p in (sibling_paths or []) if p != home_node_path]
    if distant:
        sections.append("")
        sections.append("## Distant Regions (LOD summary)")

        per_region_budget: int | None = None
        if lod_budget is not None and distant:
            per_region_budget = max(1, lod_budget // len(distant))

        for sib_path in distant:
            summary = summarize_subtree(
                input_path, sib_path, max_depth=lod_depth,
                token_budget=per_region_budget, backend_root=backend_root,
            )
            sections.append(f"### {sib_path}")
            sections.append(summary)

    return "\n".join(sections)
