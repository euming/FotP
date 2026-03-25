#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
AMS_WRAPPER_BAT = REPO_ROOT / "scripts" / "ams.bat"
AMS_WRAPPER_SH = REPO_ROOT / "scripts" / "ams"
AMS_WRAPPER_PY = REPO_ROOT / "scripts" / "ams.py"
AMS_MEMORY_CMD_ENV = "AMS_MEMORY_CMD"
DEFAULT_AMS_MEMORY_CMD = r"scripts\ams.bat" if sys.platform == "win32" else "./scripts/ams"
MEMORYCTL_PROJECT = REPO_ROOT / "tools" / "memoryctl" / "MemoryCtl.csproj"
_MEMORYCTL_EXE_NAME = "MemoryCtl.exe" if sys.platform == "win32" else "MemoryCtl"
MEMORYCTL_EXE_CANDIDATES = [
    REPO_ROOT / "tools" / "memoryctl" / "bin" / "Release" / "net9.0" / _MEMORYCTL_EXE_NAME,
    REPO_ROOT / "tools" / "memoryctl" / "bin" / "Debug" / "net9.0" / _MEMORYCTL_EXE_NAME,
    REPO_ROOT / "scripts" / "output" / "all-agents-sessions" / _MEMORYCTL_EXE_NAME,
]
RUST_AMS_KERNEL_DIR = REPO_ROOT / "rust" / "ams-core-kernel"
_RUST_AMS_EXE_NAME = "ams-core-kernel.exe" if sys.platform == "win32" else "ams-core-kernel"
RUST_AMS_EXE = RUST_AMS_KERNEL_DIR / "target" / "release" / _RUST_AMS_EXE_NAME
SUPPORTED_CORPORA = ("all", "project", "claude", "codex")
LEGACY_OUTPUT_ROOT = REPO_ROOT / "scripts" / "output"
FACTORIES_DB_PATH = (
    REPO_ROOT
    / "shared-memory"
    / "system-memory"
    / "factories"
    / "factories.memory.jsonl"
)
KE_DB_PATH = (
    REPO_ROOT
    / "shared-memory"
    / "system-memory"
    / "ke"
    / "ke.memory.jsonl"
)
SWARM_PLANS_DIR = REPO_ROOT / "shared-memory" / "system-memory" / "swarm-plans"
SHARED_MEMORY_DB_PATH = REPO_ROOT / "shared-memory" / "shared.memory.jsonl"
HANDOFF_SMARTLIST_PATH = "smartlist/architecture/agent-mirroring/cross-agent-handoff-factory"
BREAKPOINT_FACTORY_PATH = "smartlist/architecture/agent-mirroring/breakpoint-factory"
DEPRECATED_CLAUDE_COMMAND_PATTERNS = (
    "semantic-query.py",
    "memoryctl list-sessions",
    "memoryctl show-session",
    "dotnet run --project",
)


PROJ_DIR_DB = REPO_ROOT / "proj_dir.db"


def proj_dir_db_path() -> Path:
    return PROJ_DIR_DB


def repo_root() -> str:
    return str(REPO_ROOT)


def repo_name() -> str:
    override = (
        os.environ.get("AMS_REPO_NAME")
        or os.environ.get("AMS_PROJECT_NAME")
        or os.environ.get("AMS_PRODUCT_NAME")
    )
    if override and override.strip():
        return override.strip()
    return REPO_ROOT.name


def _unix_storage_slug(name: str) -> str:
    slug = re.sub(r"[^a-z0-9._-]+", "-", name.strip().lower()).strip(".-")
    return slug or "ams"


def _project_corpus_relative_path() -> Path:
    project = repo_name()
    return Path("per-project") / project / f"{project}.memory.jsonl"


def corpus_relative_path(corpus: str) -> Path:
    normalized = corpus.lower()
    if normalized == "all":
        return Path("all-agents-sessions") / "all-agents-sessions.memory.jsonl"
    if normalized == "project":
        return _project_corpus_relative_path()
    if normalized == "claude":
        return Path("all-claude-projects") / "all-claude-projects.memory.jsonl"
    if normalized == "codex":
        return Path("all-codex-sessions") / "all-codex-sessions.memory.jsonl"
    supported = ", ".join(SUPPORTED_CORPORA)
    raise ValueError(f"unsupported corpus '{corpus}' (expected one of: {supported})")


def _persistent_output_root() -> Path:
    override = os.environ.get("AMS_OUTPUT_ROOT")
    if override:
        return Path(override).expanduser()

    product_name = repo_name()
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / product_name / "agent-memory"

    return Path.home() / f".{_unix_storage_slug(product_name)}" / "agent-memory"


def corpus_candidates(corpus: str) -> list[Path]:
    normalized = corpus.lower()
    if normalized not in SUPPORTED_CORPORA:
        supported = ", ".join(SUPPORTED_CORPORA)
        raise ValueError(f"unsupported corpus '{corpus}' (expected one of: {supported})")

    relative = corpus_relative_path(normalized)
    return [
        _persistent_output_root() / relative,
        LEGACY_OUTPUT_ROOT / relative,
    ]


def corpus_db(corpus: str) -> str:
    candidates = corpus_candidates(corpus)
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return str(candidates[0])


def factories_db_path() -> str:
    return str(FACTORIES_DB_PATH)


def ke_db_path() -> str:
    """Return the dedicated knowledge-entry store path.

    Knowledge entries (ke write/read/search/context/bootstrap) belong here,
    NOT in the factories store. Factories is for SmartList templates only.
    """
    return str(KE_DB_PATH)


def shared_memory_db_path() -> str:
    """Return the shared memory write-service JSONL store path."""
    return str(SHARED_MEMORY_DB_PATH)


def swarm_plan_db_path(plan_name: str) -> str:
    """Return the per-plan JSONL store path for the given plan name."""
    return str(SWARM_PLANS_DIR / f"{plan_name}.memory.jsonl")


def swarm_plan_snapshot_path(plan_name: str) -> str:
    """Return the compiled snapshot path used by Rust commands."""
    return str(SWARM_PLANS_DIR / f"{plan_name}.memory.ams.json")


def list_swarm_plan_stores() -> list[str]:
    """Return plan names for all per-plan store files in SWARM_PLANS_DIR.

    Looks for both *.memory.jsonl (JSONL write log) and *.memory.ams.json
    (compiled Rust snapshot). Plans created by load-plan via the Rust path
    may only have the .ams.json snapshot; both formats are valid.
    Deduplicates so plans with both files appear only once.
    """
    if not SWARM_PLANS_DIR.exists():
        return []
    seen: set[str] = set()
    for p in SWARM_PLANS_DIR.iterdir():
        name = p.name
        if name.endswith(".memory.jsonl"):
            seen.add(name[: -len(".memory.jsonl")])
        elif name.endswith(".memory.ams.json"):
            seen.add(name[: -len(".memory.ams.json")])
    return sorted(seen)


def _plan_stack_path() -> Path:
    """Path to the plan-stack file that tracks the most-recently-activated plan."""
    return SWARM_PLANS_DIR / ".plan-stack.json"


def push_plan_stack(plan_name: str) -> None:
    """Push a plan name onto the plan stack (most recent = top).

    Removes any existing occurrence of the name first, so the same plan
    pushed twice just moves it to the top.
    """
    import json

    path = _plan_stack_path()
    stack: list[str] = []
    if path.exists():
        try:
            stack = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            stack = []
    # Remove duplicates, then push to top
    stack = [s for s in stack if s != plan_name]
    stack.append(plan_name)
    path.write_text(json.dumps(stack), encoding="utf-8")


def pop_plan_stack() -> "str | None":
    """Return the top plan from the stack without removing it (peek)."""
    import json

    path = _plan_stack_path()
    if not path.exists():
        return None
    try:
        stack = json.loads(path.read_text(encoding="utf-8"))
        return stack[-1] if stack else None
    except Exception:
        return None


def active_swarm_plan_name() -> "str | None":
    """Return the name of the active swarm-plan project.

    Checks the plan stack first — this tracks the most-recently-activated
    plan via ``push_plan_stack()`` calls in ``load-plan`` and ``switch``.
    Falls back to scanning all stores for ``[active]`` markers if the
    stack is empty.
    """
    import subprocess

    # Fast path: plan stack has an explicit top entry.
    top = pop_plan_stack()
    if top:
        return top

    # Fallback: scan all stores (legacy behavior, first match).
    try:
        result = subprocess.run(
            build_ams_wrapper_cmd("swarm-plan", "list"),
            capture_output=True,
            text=True,
            timeout=30,
        )
        for line in result.stdout.splitlines():
            if "[active]" in line:
                name = line.split("[active]")[0].strip()
                if name:
                    return name
    except Exception:
        pass
    return None


def handoff_smartlist_path() -> str:
    return HANDOFF_SMARTLIST_PATH


def breakpoint_factory_path() -> str:
    return BREAKPOINT_FACTORY_PATH


def memory_command_label() -> str:
    return os.environ.get(AMS_MEMORY_CMD_ENV, DEFAULT_AMS_MEMORY_CMD)


def build_ams_wrapper_cmd(*args: str) -> list[str]:
    override = os.environ.get(AMS_MEMORY_CMD_ENV)
    if override:
        return [*shlex.split(override, posix=False), *args]
    if sys.platform == "win32" and AMS_WRAPPER_BAT.exists():
        return [str(AMS_WRAPPER_BAT), *args]
    if AMS_WRAPPER_SH.exists():
        return [str(AMS_WRAPPER_SH), *args]
    if AMS_WRAPPER_BAT.exists():
        return [str(AMS_WRAPPER_BAT), *args]
    return [sys.executable, str(AMS_WRAPPER_PY), *args]


def claude_local_drilldown_lines() -> list[str]:
    cmd = memory_command_label()
    return [
        f"Execution context: `{cmd} callstack context`",
        f"Memory retrieval: `{cmd} search` / `{cmd} recall`",
        f"Atlas lookup: `{cmd} atlas page <id>` / `{cmd} atlas search <query>`",
        f"Wrapper override: `{AMS_MEMORY_CMD_ENV}`",
        "",
    ]


def validate_claude_local_contract(content: str) -> list[str]:
    violations: list[str] = []
    for pattern in DEPRECATED_CLAUDE_COMMAND_PATTERNS:
        if pattern in content:
            violations.append(f"deprecated command surfaced in CLAUDE.local.md: {pattern}")
    required = (
        " search ",
        " recall ",
    )
    normalized = content.replace("`", " ").replace("\r", "")
    for token in required:
        if token not in normalized:
            violations.append(f"missing wrapper guidance token in CLAUDE.local.md: {token.strip()}")
    return violations


def build_memoryctl_cmd(*args: str) -> list[str] | None:
    for exe in MEMORYCTL_EXE_CANDIDATES:
        if exe.exists():
            return [str(exe), *args]
    if MEMORYCTL_PROJECT.exists():
        return ["dotnet", "run", "--project", str(MEMORYCTL_PROJECT), "--", *args]
    return None


def build_rust_ams_cmd(*args: str) -> list[str] | None:
    if RUST_AMS_EXE.exists():
        return [str(RUST_AMS_EXE), *args]
    cargo_toml = RUST_AMS_KERNEL_DIR / "Cargo.toml"
    if cargo_toml.exists():
        return ["cargo", "run", "--manifest-path", str(cargo_toml), "--release", "--", *args]
    return None


def _iter_msvc_linker_candidates() -> list[Path]:
    candidates: list[Path] = []

    program_files_x86 = Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)"))
    vswhere = program_files_x86 / "Microsoft Visual Studio" / "Installer" / "vswhere.exe"
    if vswhere.exists():
        try:
            result = subprocess.run(
                [
                    str(vswhere),
                    "-latest",
                    "-products",
                    "*",
                    "-requires",
                    "Microsoft.VisualStudio.Component.VC.Tools.x86.x64",
                    "-property",
                    "installationPath",
                ],
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
            install_path = result.stdout.strip()
            if result.returncode == 0 and install_path:
                root = Path(install_path) / "VC" / "Tools" / "MSVC"
                candidates.extend(
                    sorted(root.glob("*/bin/Hostx64/x64/link.exe"), reverse=True)
                )
        except Exception:
            pass

    fallback_roots = [
        Path(r"C:\Program Files\Microsoft Visual Studio"),
        Path(r"C:\Program Files (x86)\Microsoft Visual Studio"),
    ]
    for root in fallback_roots:
        if not root.exists():
            continue
        candidates.extend(
            sorted(root.glob("*/*/VC/Tools/MSVC/*/bin/Hostx64/x64/link.exe"), reverse=True)
        )

    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate).lower()
        if key in seen or not candidate.exists():
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _detect_msvc_linker() -> Path | None:
    override = os.environ.get("AMS_MSVC_LINKER") or os.environ.get(
        "CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER"
    )
    if override:
        path = Path(override)
        if path.exists():
            return path
    candidates = _iter_msvc_linker_candidates()
    return candidates[0] if candidates else None


def rust_backend_env(backend_root: str | None) -> dict[str, str]:
    env = os.environ.copy()
    if backend_root:
        env["AMS_WRITE_BACKEND_ROOT"] = backend_root
    if sys.platform == "win32":
        linker = _detect_msvc_linker()
        if linker is not None:
            env["CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER"] = str(linker)
            linker_dir = str(linker.parent)
            current_path = env.get("PATH", "")
            parts = current_path.split(os.pathsep) if current_path else []
            normalized = {part.lower() for part in parts}
            if linker_dir.lower() not in normalized:
                env["PATH"] = os.pathsep.join([linker_dir, *parts]) if parts else linker_dir
    return env


def normalize_session_id(target: str) -> str:
    if target.startswith("chat-session:"):
        return target.split(":", 1)[1]
    return target
