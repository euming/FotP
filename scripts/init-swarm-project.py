#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
SOURCE_REPO = SCRIPT_DIR.parent
SCAFFOLD_TEMPLATE_ROOT = SOURCE_REPO / "templates" / "swarm-project"

COPY_TREES = (
    Path("scripts"),
    Path(".claude") / "hooks",
    Path(".claude") / "teams",
    Path("tools") / "memoryctl",
    Path("rust") / "ams-core-kernel",
)

COPY_FILES = (
    Path(".claude") / "settings.json",
    Path("dist") / "libams_vtable.dll",
)

GENERATED_TEXT_FILES = (
    Path("AGENTS.md"),
    Path("docs") / "agent-memory-bootstrap.md",
)

STATE_DIRS = (
    Path("shared-memory"),
    Path("shared-memory") / "agent-memory",
    Path("shared-memory") / "system-memory",
    Path("shared-memory") / "system-memory" / "factories",
    Path("shared-memory") / "system-memory" / "ke",
    Path("shared-memory") / "system-memory" / "swarm-plans",
)

STATE_FILES = (
    Path("shared-memory") / "shared.memory.jsonl",
    Path("shared-memory") / "system-memory" / "factories" / "factories.memory.jsonl",
    Path("shared-memory") / "system-memory" / "ke" / "ke.memory.jsonl",
)

MANIFEST_PATH = Path(".swarm-project") / "bootstrap-manifest.json"

IGNORED_DIR_NAMES = {
    "__pycache__",
    ".pytest_cache",
    "bin",
    "obj",
    "output",
    "target",
}

IGNORED_FILE_NAMES = {
    "settings.local.json",
}

IGNORED_SUFFIXES = {
    ".log",
    ".pyc",
    ".pyo",
    ".tmp",
}


class BootstrapError(RuntimeError):
    pass


@dataclass
class CopyResult:
    copied: list[str]
    skipped_identical: list[str]
    warnings: list[str]


def _is_ignored(rel_path: Path) -> bool:
    if any(part in IGNORED_DIR_NAMES for part in rel_path.parts):
        return True
    if rel_path.name in IGNORED_FILE_NAMES:
        return True
    if rel_path.suffix.lower() in IGNORED_SUFFIXES:
        return True
    return False


def _read_bytes(path: Path) -> bytes:
    return path.read_bytes()


def _unix_storage_slug(name: str) -> str:
    slug = re.sub(r"[^a-z0-9._-]+", "-", name.strip().lower()).strip(".-")
    return slug or "ams"


def _ensure_parent(path: Path, dry_run: bool) -> None:
    if dry_run:
        return
    path.parent.mkdir(parents=True, exist_ok=True)


def _copy_file(
    source: Path,
    target: Path,
    *,
    force: bool,
    dry_run: bool,
    result: CopyResult,
) -> None:
    if target.exists():
        if target.is_dir():
            raise BootstrapError(f"Cannot overwrite directory with file: {target}")
        if _read_bytes(source) == _read_bytes(target):
            result.skipped_identical.append(target.as_posix())
            return
        if not force:
            raise BootstrapError(f"Refusing to overwrite existing file without --force: {target}")
    _ensure_parent(target, dry_run)
    if not dry_run:
        shutil.copy2(source, target)
    result.copied.append(target.as_posix())


def _copy_tree(
    source_root: Path,
    target_root: Path,
    *,
    force: bool,
    dry_run: bool,
    result: CopyResult,
) -> None:
    for source in sorted(source_root.rglob("*")):
        rel = source.relative_to(SOURCE_REPO)
        if _is_ignored(rel):
            continue
        target = target_root / rel
        if source.is_dir():
            if not dry_run:
                target.mkdir(parents=True, exist_ok=True)
            continue
        _copy_file(source, target, force=force, dry_run=dry_run, result=result)


def _render_repo_template(source_rel_path: Path, target_repo_name: str) -> str:
    source_text = (SCAFFOLD_TEMPLATE_ROOT / source_rel_path).read_text(encoding="utf-8")
    repo_slug = _unix_storage_slug(target_repo_name)
    return (
        source_text
        .replace("__REPO_NAME__", target_repo_name)
        .replace("__REPO_SLUG__", repo_slug)
    )


def _write_text_file(
    target: Path,
    content: str,
    *,
    force: bool,
    dry_run: bool,
    result: CopyResult,
) -> None:
    encoded = content.encode("utf-8")
    if target.exists():
        if target.is_dir():
            raise BootstrapError(f"Cannot overwrite directory with file: {target}")
        if _read_bytes(target) == encoded:
            result.skipped_identical.append(target.as_posix())
            return
        if not force:
            raise BootstrapError(f"Refusing to overwrite existing file without --force: {target}")
    _ensure_parent(target, dry_run)
    if not dry_run:
        target.write_bytes(encoded)
    result.copied.append(target.as_posix())


def _touch_state_file(path: Path, dry_run: bool) -> None:
    if path.exists():
        return
    if dry_run:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


def _ensure_state_layout(target_root: Path, dry_run: bool) -> None:
    for rel in STATE_DIRS:
        path = target_root / rel
        if dry_run:
            continue
        path.mkdir(parents=True, exist_ok=True)
    for rel in STATE_FILES:
        _touch_state_file(target_root / rel, dry_run=dry_run)


def _write_manifest(target_root: Path, copied: CopyResult, dry_run: bool) -> None:
    manifest = {
        "source_repo": str(SOURCE_REPO),
        "target_repo": str(target_root),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "copied": copied.copied,
        "skipped_identical": copied.skipped_identical,
        "state_dirs": [str(path).replace("\\", "/") for path in STATE_DIRS],
        "state_files": [str(path).replace("\\", "/") for path in STATE_FILES],
    }
    manifest_path = target_root / MANIFEST_PATH
    if dry_run:
        return
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _is_git_repo(path: Path) -> bool:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=path,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def _build_proj_dir(target_root: Path) -> str:
    script = target_root / "scripts" / "build_proj_dir.py"
    if not script.exists():
        return "skipped: scripts/build_proj_dir.py not present"
    if not _is_git_repo(target_root):
        return "skipped: target is not a git repository"

    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=target_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip() or "unknown error"
        return f"failed: {stderr}"
    return result.stdout.strip() or "completed"


def bootstrap_target(target_root: Path, *, force: bool, dry_run: bool, skip_proj_dir_build: bool) -> CopyResult:
    result = CopyResult(copied=[], skipped_identical=[], warnings=[])
    if not dry_run:
        target_root.mkdir(parents=True, exist_ok=True)

    for rel in COPY_TREES:
        source = SOURCE_REPO / rel
        if not source.exists():
            result.warnings.append(f"missing source tree: {rel.as_posix()}")
            continue
        _copy_tree(source, target_root, force=force, dry_run=dry_run, result=result)

    for rel in COPY_FILES:
        source = SOURCE_REPO / rel
        if not source.exists():
            result.warnings.append(f"missing source file: {rel.as_posix()}")
            continue
        _copy_file(source, target_root / rel, force=force, dry_run=dry_run, result=result)

    for rel in GENERATED_TEXT_FILES:
        rendered = _render_repo_template(rel, target_root.name)
        _write_text_file(
            target_root / rel,
            rendered,
            force=force,
            dry_run=dry_run,
            result=result,
        )

    _ensure_state_layout(target_root, dry_run=dry_run)
    _write_manifest(target_root, result, dry_run=dry_run)

    if not dry_run and not skip_proj_dir_build:
        proj_dir_status = _build_proj_dir(target_root)
        result.warnings.append(f"proj-dir build: {proj_dir_status}")

    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scaffold another repository with the AMS/swarm-plan runtime bundle, "
            "using this repo as the template source."
        )
    )
    parser.add_argument("target", type=Path, help="Target repository or directory to initialize.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite conflicting managed files when they differ from the template.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be copied without writing files.",
    )
    parser.add_argument(
        "--skip-proj-dir-build",
        action="store_true",
        help="Skip rebuilding proj_dir.db in the target repo.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    target_root = args.target.resolve()
    if target_root == SOURCE_REPO or SOURCE_REPO in target_root.parents:
        print(
            f"ERROR: target must be outside the template repo: {target_root}",
            file=sys.stderr,
        )
        return 1

    try:
        result = bootstrap_target(
            target_root,
            force=args.force,
            dry_run=args.dry_run,
            skip_proj_dir_build=args.skip_proj_dir_build,
        )
    except BootstrapError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    mode = "dry-run" if args.dry_run else "initialized"
    print(f"[init-swarm-project] {mode}: {target_root}")
    print(f"  copied files      : {len(result.copied)}")
    print(f"  identical skipped : {len(result.skipped_identical)}")
    if result.warnings:
        print("  notes:")
        for warning in result.warnings:
            print(f"    - {warning}")
    if result.copied:
        print("  sample paths:")
        for path in result.copied[:10]:
            print(f"    - {path}")
        if len(result.copied) > 10:
            print(f"    - ... {len(result.copied) - 10} more")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
