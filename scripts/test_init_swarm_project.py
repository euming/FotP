#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent
INIT_SCRIPT = SCRIPTS_DIR / "init-swarm-project.py"


class TestInitSwarmProject(unittest.TestCase):
    def test_dry_run_does_not_write_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "SampleRepo"
            target.mkdir()
            result = subprocess.run(
                [sys.executable, str(INIT_SCRIPT), str(target), "--dry-run"],
                capture_output=True,
                text=True,
                cwd=str(REPO_ROOT),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse((target / "AGENTS.md").exists())
            self.assertIn("[init-swarm-project] dry-run", result.stdout)

    def test_initializes_target_repo_scaffold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "SampleRepo"
            target.mkdir()
            (target / "README.md").write_text("# SampleRepo\n", encoding="utf-8")
            subprocess.run(
                ["git", "init"],
                cwd=str(target),
                capture_output=True,
                text=True,
                check=True,
            )

            result = subprocess.run(
                [sys.executable, str(INIT_SCRIPT), str(target)],
                capture_output=True,
                text=True,
                cwd=str(REPO_ROOT),
            )
            self.assertEqual(result.returncode, 0, result.stderr)

            self.assertTrue((target / "AGENTS.md").exists())
            agents_text = (target / "AGENTS.md").read_text(encoding="utf-8")
            self.assertIn("SampleRepo", agents_text)
            self.assertNotIn("__REPO_NAME__", agents_text)
            self.assertNotIn("__REPO_SLUG__", agents_text)
            self.assertTrue((target / "docs" / "agent-memory-bootstrap.md").exists())
            bootstrap_text = (target / "docs" / "agent-memory-bootstrap.md").read_text(encoding="utf-8")
            self.assertIn(
                "SampleRepo",
                bootstrap_text,
            )
            self.assertNotIn("__REPO_NAME__", bootstrap_text)
            self.assertNotIn("__REPO_SLUG__", bootstrap_text)
            self.assertTrue((target / "scripts" / "ams.bat").exists())
            self.assertTrue((target / "scripts" / "run-swarm-plan.py").exists())
            self.assertTrue((target / ".claude" / "teams" / "worker.yml").exists())
            self.assertTrue((target / "tools" / "memoryctl" / "MemoryCtl.csproj").exists())
            self.assertTrue((target / "rust" / "ams-core-kernel" / "Cargo.toml").exists())
            self.assertTrue((target / "shared-memory" / "system-memory" / "factories" / "factories.memory.jsonl").exists())
            self.assertTrue((target / "shared-memory" / "system-memory" / "ke" / "ke.memory.jsonl").exists())
            self.assertTrue((target / "proj_dir.db").exists())

            manifest_path = target / ".swarm-project" / "bootstrap-manifest.json"
            self.assertTrue(manifest_path.exists())
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["target_repo"], str(target.resolve()))
            self.assertGreater(len(manifest["copied"]), 0)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
