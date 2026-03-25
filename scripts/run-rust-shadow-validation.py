#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from ams_common import SUPPORTED_CORPORA, corpus_db, repo_root


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(repo_root())
KERNEL_DIR = REPO_ROOT / "rust" / "ams-core-kernel"
DEFAULT_CASES = SCRIPT_DIR / "shadow-cases" / "rust-cutover-real-corpus.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="scripts\\run-rust-shadow-validation.py",
        description="Run Rust-vs-C# shadow validation against real AMS corpora and summarize mismatches.",
    )
    parser.add_argument(
        "--corpus",
        choices=SUPPORTED_CORPORA,
        action="append",
        help="Corpus to validate. May be repeated. Defaults to all.",
    )
    parser.add_argument(
        "--cases",
        type=Path,
        default=DEFAULT_CASES,
        help="Shadow-validation case file. Defaults to the repo-owned real-corpus case set.",
    )
    parser.add_argument(
        "--assert-match",
        action="store_true",
        help="Exit non-zero if any mismatch report is produced.",
    )
    return parser.parse_args()


def resolve_kernel_command() -> list[str]:
    _exe_name = "ams-core-kernel.exe" if sys.platform == "win32" else "ams-core-kernel"
    release_exe = KERNEL_DIR / "target" / "release" / _exe_name
    if release_exe.exists():
        return [str(release_exe)]
    return ["cargo", "run", "--release", "--"]


def default_report_path(db_path: Path) -> Path:
    name = db_path.name
    if name.endswith(".memory.jsonl"):
        return db_path.with_name(name[: -len(".memory.jsonl")] + ".shadow-report.jsonl")
    return db_path.with_name(name + ".shadow-report.jsonl")


def load_reports(path: Path) -> list[dict]:
    reports: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped:
            reports.append(json.loads(stripped))
    return reports


def main() -> int:
    args = parse_args()
    corpora = args.corpus or ["all"]
    cases_path = args.cases if args.cases.is_absolute() else (REPO_ROOT / args.cases)
    if not cases_path.exists():
        print(f"ERROR: shadow case file not found: {cases_path}", file=sys.stderr)
        return 2

    kernel_cmd = resolve_kernel_command()
    total_mismatches = 0

    for corpus in corpora:
        db_path = Path(corpus_db(corpus))
        if not db_path.exists():
            print(f"ERROR: corpus '{corpus}' not found at {db_path}", file=sys.stderr)
            return 2

        report_path = default_report_path(db_path)
        cmd = [
            *kernel_cmd,
            "shadow-validate",
            "--input",
            str(db_path),
            "--cases",
            str(cases_path),
            "--out",
            str(report_path),
        ]

        print(f"[shadow] corpus={corpus} db={db_path}")
        result = subprocess.run(cmd, cwd=KERNEL_DIR)
        if result.returncode != 0:
            print(f"[shadow] command failed for corpus={corpus} exit={result.returncode}", file=sys.stderr)
            return result.returncode

        reports = load_reports(report_path)
        mismatches = sum(1 for report in reports if not report.get("passed", False))
        total_mismatches += mismatches
        print(
            f"[shadow] corpus={corpus} cases={len(reports)} mismatches={mismatches} report={report_path}"
        )
        for report in reports:
            if report.get("passed", False):
                continue
            differences = report.get("differences", [])
            print(f"  - {report.get('case_name')}: {' | '.join(differences)}")

    if args.assert_match and total_mismatches > 0:
        print(f"ERROR: shadow validation found {total_mismatches} mismatched case(s).", file=sys.stderr)
        return 3

    return 0


if __name__ == "__main__":
    sys.exit(main())
