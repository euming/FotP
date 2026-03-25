#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path
import os


SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent
BOOTSTRAP_SH = SCRIPTS_DIR / "bootstrap-private-repo.sh"


class TestBootstrapPrivateRepoScript(unittest.TestCase):
    def test_script_exists(self) -> None:
        self.assertTrue(BOOTSTRAP_SH.exists())

    def test_script_is_executable_in_git(self) -> None:
        self.assertTrue(
            os.access(BOOTSTRAP_SH, os.X_OK),
            f"Expected executable bit on filesystem for {BOOTSTRAP_SH}",
        )

    def test_help_mentions_repo_branch_and_diagnose(self) -> None:
        result = subprocess.run(
            ["bash", str(BOOTSTRAP_SH), "--help"],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--repo-url", result.stdout)
        self.assertIn("--branch", result.stdout)
        self.assertIn("--diagnose-only", result.stdout)

    def test_diagnose_only_succeeds_without_network(self) -> None:
        result = subprocess.run(
            ["bash", str(BOOTSTRAP_SH), "--diagnose-only"],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Shell Git diagnostics:", result.stdout)
        self.assertIn("Diagnose-only mode", result.stdout)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
