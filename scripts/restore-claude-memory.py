#!/usr/bin/env python3
"""
restore-claude-memory.py

Restore a generated CLAUDE.local.md from the per-repo memory.archive stash.

This is the emergency fallback surface during SmartList-first cutover:
the live runtime path should use AMS retrieval, but archived markdown snapshots
remain available for manual recovery if memory behavior regresses.
"""

import argparse
import shutil
import sys
from pathlib import Path


def list_archives(archive_dir: Path) -> list[Path]:
    return sorted(
        (
            path for path in archive_dir.glob('CLAUDE.local.*.md')
            if path.is_file()
        ),
        key=lambda p: p.name,
        reverse=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Restore CLAUDE.local.md from memory.archive.'
    )
    parser.add_argument(
        '--repo-dir',
        type=Path,
        required=True,
        help='Repository root containing memory.archive/claude-local/',
    )
    parser.add_argument(
        '--name',
        default='',
        help='Specific archived filename to restore. Defaults to latest snapshot.',
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='List available archived snapshots and exit.',
    )
    args = parser.parse_args()

    archive_dir = args.repo_dir / 'memory.archive' / 'claude-local'
    if not archive_dir.is_dir():
        print(f'ERROR: archive directory not found: {archive_dir}', file=sys.stderr)
        sys.exit(1)

    archives = list_archives(archive_dir)
    if not archives:
        print(f'ERROR: no archived CLAUDE.local snapshots found in {archive_dir}', file=sys.stderr)
        sys.exit(1)

    if args.list:
        for path in archives:
            print(path.name)
        return

    if args.name:
        source_path = archive_dir / args.name
        if not source_path.is_file():
            print(f'ERROR: archive snapshot not found: {source_path}', file=sys.stderr)
            sys.exit(1)
    else:
        source_path = archives[0]

    target_path = args.repo_dir / 'CLAUDE.local.md'
    shutil.copyfile(source_path, target_path)
    print(f'Restored {target_path} from {source_path.name}')


if __name__ == '__main__':
    main()
