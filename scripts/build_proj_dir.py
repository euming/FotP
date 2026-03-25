#!/usr/bin/env python3
"""Build proj_dir.db — a SQLite index of the project directory tree."""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path, PurePosixPath

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "proj_dir.db"

SCHEMA = """\
CREATE TABLE IF NOT EXISTS files (
    id        INTEGER PRIMARY KEY,
    path      TEXT NOT NULL UNIQUE,
    dir       TEXT NOT NULL,
    name      TEXT NOT NULL,
    ext       TEXT,
    size      INTEGER NOT NULL,
    mtime     REAL NOT NULL,
    is_dir    INTEGER NOT NULL DEFAULT 0,
    depth     INTEGER NOT NULL,
    head      TEXT,
    content   TEXT
);

CREATE INDEX IF NOT EXISTS idx_files_ext ON files(ext);
CREATE INDEX IF NOT EXISTS idx_files_dir ON files(dir);
"""

FTS_SCHEMA = """\
CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
    path, head, content, content=files, content_rowid=id
);
"""

FTS_REBUILD = """\
INSERT INTO files_fts(files_fts) VALUES('rebuild');
"""

BINARY_EXTS = frozenset({
    "exe", "dll", "pdb", "obj", "bin", "zip", "gz", "tar", "png", "jpg",
    "jpeg", "gif", "ico", "bmp", "woff", "woff2", "ttf", "eot", "svg",
    "mp3", "mp4", "avi", "mov", "pdf", "nupkg", "snupkg", "db", "sqlite",
    "lock",
})

HEAD_EXTS = frozenset({
    "cs", "py", "rs", "json", "yaml", "yml", "toml", "bat", "sh", "ps1",
    "js", "ts", "tsx", "jsx", "css", "html", "xml", "csproj", "sln",
    "fsproj", "props", "targets",
})

HEAD_LINES = 50


def git_files() -> list[str]:
    """Get tracked + untracked-but-not-ignored files via git."""
    tracked = subprocess.check_output(
        ["git", "ls-files"], cwd=REPO_ROOT, text=True
    ).splitlines()
    untracked = subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard"],
        cwd=REPO_ROOT, text=True,
    ).splitlines()
    return [f for f in tracked + untracked if f.strip()]


def read_head(filepath: Path, n: int = HEAD_LINES) -> str | None:
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            lines = []
            for i, line in enumerate(f):
                if i >= n:
                    break
                lines.append(line)
            return "".join(lines) if lines else None
    except (OSError, UnicodeDecodeError):
        return None


def read_full(filepath: Path) -> str | None:
    try:
        return filepath.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def collect_dirs(files: list[str]) -> set[str]:
    """Collect all unique directory paths from the file list."""
    dirs: set[str] = set()
    for f in files:
        parts = PurePosixPath(f).parts
        for i in range(1, len(parts)):
            dirs.add("/".join(parts[:i]))
    return dirs


def build(db_path: Path | None = None) -> Path:
    if db_path is None:
        db_path = DB_PATH

    files = git_files()
    dirs = collect_dirs(files)

    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    conn.executescript(SCHEMA)
    conn.executescript(FTS_SCHEMA)

    # Insert directories
    for d in sorted(dirs):
        p = PurePosixPath(d)
        conn.execute(
            "INSERT INTO files (path, dir, name, ext, size, mtime, is_dir, depth) "
            "VALUES (?, ?, ?, NULL, 0, 0, 1, ?)",
            (d, str(p.parent) if str(p.parent) != "." else "", p.name, len(p.parts)),
        )

    # Insert files
    for rel in files:
        p = PurePosixPath(rel)
        ext = p.suffix.lstrip(".").lower() if p.suffix else None

        if ext in BINARY_EXTS:
            continue

        abs_path = REPO_ROOT / rel
        try:
            stat = abs_path.stat()
        except OSError:
            continue

        head = None
        content = None
        if ext == "md":
            content = read_full(abs_path)
            head = content[:5000] if content and len(content) > 5000 else content
        elif ext in HEAD_EXTS:
            head = read_head(abs_path)

        dir_part = str(p.parent) if str(p.parent) != "." else ""
        conn.execute(
            "INSERT OR REPLACE INTO files (path, dir, name, ext, size, mtime, is_dir, depth, head, content) "
            "VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)",
            (rel, dir_part, p.name, ext, stat.st_size, stat.st_mtime, len(p.parts), head, content),
        )

    conn.executescript(FTS_REBUILD)
    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    file_count = conn.execute("SELECT COUNT(*) FROM files WHERE is_dir=0").fetchone()[0]
    dir_count = conn.execute("SELECT COUNT(*) FROM files WHERE is_dir=1").fetchone()[0]
    conn.close()

    print(f"proj_dir.db built: {file_count} files, {dir_count} directories ({total} total rows)")
    return db_path


if __name__ == "__main__":
    build()
