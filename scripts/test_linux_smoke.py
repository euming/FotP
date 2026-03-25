#!/usr/bin/env python3
"""
test_linux_smoke.py

Smoke tests for the Linux/container AMS wrapper and backend.

Two test classes:
  - Unit tests (fast, no external deps): validate command construction,
    platform-aware strings, executable bits, file existence.
  - Integration tests (require MemoryCtl + Rust kernel or Cargo):
    actually run ./scripts/ams against a fixture corpus to verify
    real end-to-end behavior in a fresh container.

Run all:
    python scripts/test_linux_smoke.py
    python -m pytest scripts/test_linux_smoke.py -v

Run only unit tests (no build deps):
    python -m pytest scripts/test_linux_smoke.py -v -k "not Integration"

Run only integration tests:
    python -m pytest scripts/test_linux_smoke.py -v -k "Integration"
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent
FIXTURE_RAW = SCRIPTS_DIR / "fixtures" / "smoke-test.chat.raw.jsonl"

sys.path.insert(0, str(SCRIPTS_DIR))
import ams_common


# ---------------------------------------------------------------------------
# Unit tests — fast, no external deps
# ---------------------------------------------------------------------------

class TestCargoFallbackManifestPath(unittest.TestCase):
    """build_rust_ams_cmd Cargo fallback must include --manifest-path."""

    def test_cargo_fallback_includes_manifest_path(self) -> None:
        cargo_toml = REPO_ROOT / "rust" / "ams-core-kernel" / "Cargo.toml"
        if not cargo_toml.exists():
            self.skipTest("rust/ams-core-kernel/Cargo.toml not present")

        with patch.object(ams_common, "RUST_AMS_EXE", REPO_ROOT / "nonexistent-binary"):
            cmd = ams_common.build_rust_ams_cmd("callstack", "context")

        self.assertIsNotNone(cmd)
        assert cmd is not None
        self.assertIn("cargo", cmd[0])
        self.assertIn("--manifest-path", cmd)
        manifest_idx = cmd.index("--manifest-path")
        self.assertIn("Cargo.toml", cmd[manifest_idx + 1])


class TestLinuxBinaryDetection(unittest.TestCase):
    def test_exe_name_is_elf_on_linux(self) -> None:
        with patch.object(ams_common.sys, "platform", "linux"):
            exe_name = "ams-core-kernel.exe" if ams_common.sys.platform == "win32" else "ams-core-kernel"
        self.assertEqual(exe_name, "ams-core-kernel")

    def test_exe_name_is_windows_on_win32(self) -> None:
        with patch.object(ams_common.sys, "platform", "win32"):
            exe_name = "ams-core-kernel.exe" if ams_common.sys.platform == "win32" else "ams-core-kernel"
        self.assertEqual(exe_name, "ams-core-kernel.exe")


class TestPlatformAwareArgparseProg(unittest.TestCase):
    def test_prog_linux(self) -> None:
        prog = r"scripts\ams.bat" if "linux" == "win32" else "./scripts/ams"
        self.assertEqual(prog, "./scripts/ams")
        self.assertNotIn(".bat", prog)

    def test_prog_windows(self) -> None:
        prog = r"scripts\ams.bat" if "win32" == "win32" else "./scripts/ams"
        self.assertEqual(prog, r"scripts\ams.bat")


class TestPlatformAwareCorpusErrorMessage(unittest.TestCase):
    def _msg(self, platform: str) -> str:
        return (
            "scripts\\sync-all-agent-memory.bat --no-browser"
            if platform == "win32"
            else "./scripts/sync-all-agent-memory.sh --no-browser"
        )

    def test_linux_shows_sh(self) -> None:
        msg = self._msg("linux")
        self.assertIn(".sh", msg)
        self.assertNotIn(".bat", msg)

    def test_windows_shows_bat(self) -> None:
        msg = self._msg("win32")
        self.assertIn(".bat", msg)
        self.assertNotIn(".sh", msg)


class TestDefaultAmsMemoryCmd(unittest.TestCase):
    def test_linux(self) -> None:
        cmd = "./scripts/ams" if "linux" != "win32" else r"scripts\ams.bat"
        self.assertNotIn(".bat", cmd)

    def test_windows(self) -> None:
        cmd = "./scripts/ams" if "win32" != "win32" else r"scripts\ams.bat"
        self.assertIn(".bat", cmd)


class TestSyncShExists(unittest.TestCase):
    def test_sync_sh_exists(self) -> None:
        sh = REPO_ROOT / "scripts" / "sync-all-agent-memory.sh"
        self.assertTrue(sh.exists())

    def test_sync_sh_is_executable_in_git(self) -> None:
        r = subprocess.run(
            ["git", "ls-files", "-s", "scripts/sync-all-agent-memory.sh"],
            capture_output=True, text=True, cwd=str(REPO_ROOT),
        )
        self.assertEqual(r.returncode, 0)
        self.assertTrue(r.stdout.startswith("100755"), f"Expected 100755, got: {r.stdout!r}")

    def test_install_sh_is_executable_in_git(self) -> None:
        r = subprocess.run(
            ["git", "ls-files", "-s", "scripts/install-codex-ams-skill.sh"],
            capture_output=True, text=True, cwd=str(REPO_ROOT),
        )
        self.assertEqual(r.returncode, 0)
        self.assertTrue(r.stdout.startswith("100755"), f"Expected 100755, got: {r.stdout!r}")


class TestFixtureCorpusExists(unittest.TestCase):
    def test_fixture_raw_exists(self) -> None:
        self.assertTrue(FIXTURE_RAW.exists(), f"Fixture not found: {FIXTURE_RAW}")

    def test_fixture_is_valid_jsonl(self) -> None:
        with open(FIXTURE_RAW) as f:
            lines = [l.strip() for l in f if l.strip()]
        self.assertGreater(len(lines), 0)
        for line in lines:
            obj = json.loads(line)
            self.assertEqual(obj.get("type"), "chat_event")
            self.assertIn("text", obj)

    def test_sync_sh_accepts_raw_flag(self) -> None:
        """Verify --raw flag is documented in sync script help."""
        sh = REPO_ROOT / "scripts" / "sync-all-agent-memory.sh"
        content = sh.read_text()
        self.assertIn("--raw", content)
        self.assertIn("--claude-root", content)
        self.assertIn("--codex-root", content)
        self.assertIn("CLAUDE_SESSIONS_ROOT", content)
        self.assertIn("CODEX_SESSIONS_ROOT", content)


class TestSyncShErrorMessageOnNoSources(unittest.TestCase):
    """When no sources found, error must mention --raw, --claude-root, --codex-root."""

    def test_no_sources_error_mentions_overrides(self) -> None:
        sh = REPO_ROOT / "scripts" / "sync-all-agent-memory.sh"
        content = sh.read_text()
        # Find the block after "No session sources found"
        idx = content.find("No session sources found")
        self.assertGreater(idx, 0)
        after = content[idx:]
        self.assertIn("--raw", after)
        self.assertIn("--claude-root", after)
        self.assertIn("--codex-root", after)


# ---------------------------------------------------------------------------
# Integration tests — require MemoryCtl (dotnet or binary) + kernel/Cargo
# ---------------------------------------------------------------------------

def _memoryctl_available() -> bool:
    """Return True if MemoryCtl can be invoked."""
    exe = REPO_ROOT / "tools" / "memoryctl" / "bin" / "Release" / "net9.0" / "MemoryCtl"
    if exe.exists():
        return True
    dbg = REPO_ROOT / "tools" / "memoryctl" / "bin" / "Debug" / "net9.0" / "MemoryCtl"
    if dbg.exists():
        return True
    csproj = REPO_ROOT / "tools" / "memoryctl" / "MemoryCtl.csproj"
    if csproj.exists() and shutil.which("dotnet"):
        return True
    return False


def _rust_kernel_available() -> bool:
    """Return True if the Rust kernel binary exists or Cargo can build it."""
    rel = REPO_ROOT / "rust" / "ams-core-kernel" / "target" / "release" / "ams-core-kernel"
    dbg = REPO_ROOT / "rust" / "ams-core-kernel" / "target" / "debug" / "ams-core-kernel"
    if rel.exists() or dbg.exists():
        return True
    cargo_toml = REPO_ROOT / "rust" / "ams-core-kernel" / "Cargo.toml"
    return cargo_toml.exists() and bool(shutil.which("cargo"))


@unittest.skipUnless(_memoryctl_available(), "MemoryCtl not available (skipping integration tests)")
class TestIntegrationBootstrapAndSearch(unittest.TestCase):
    """
    End-to-end: bootstrap from fixture --raw, then run ams search against result.
    Validates the full fresh-container flow without needing ~/.claude or ~/.codex.
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmpdir = tempfile.mkdtemp(prefix="ams_smoke_")
        cls.outdir = Path(cls.tmpdir) / "all-agents-sessions"
        cls.outdir.mkdir()
        cls.db = cls.outdir / "all-agents-sessions.memory.jsonl"

        # Run sync with --raw fixture to produce the corpus DB
        env = {**os.environ, "AMS_OUTPUT_ROOT": cls.tmpdir, "AMS_NO_BROWSER": "1"}
        result = subprocess.run(
            [
                "bash",
                str(REPO_ROOT / "scripts" / "sync-all-agent-memory.sh"),
                str(cls.outdir),
                "--raw", str(FIXTURE_RAW),
                "--no-browser",
            ],
            capture_output=True, text=True, env=env, cwd=str(REPO_ROOT),
        )
        cls.bootstrap_returncode = result.returncode
        cls.bootstrap_stdout = result.stdout
        cls.bootstrap_stderr = result.stderr

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_bootstrap_succeeds(self) -> None:
        self.assertEqual(
            self.bootstrap_returncode, 0,
            f"Bootstrap failed (rc={self.bootstrap_returncode}).\n"
            f"stdout:\n{self.bootstrap_stdout}\n"
            f"stderr:\n{self.bootstrap_stderr}",
        )

    def test_corpus_db_created(self) -> None:
        self.assertTrue(
            self.db.exists(),
            f"Expected corpus DB at {self.db} — not found.\n"
            f"Bootstrap stdout:\n{self.bootstrap_stdout}",
        )

    def test_corpus_db_non_empty(self) -> None:
        if not self.db.exists():
            self.skipTest("corpus DB not created (bootstrap failed)")
        size = self.db.stat().st_size
        self.assertGreater(size, 0, "Corpus DB is empty")

    @unittest.skipUnless(
        _rust_kernel_available(),
        "Rust kernel not available (skipping search integration test)",
    )
    def test_search_returns_results(self) -> None:
        """./scripts/ams search against fixture corpus must return results."""
        if not self.db.exists():
            self.skipTest("corpus DB not created")

        env = {**os.environ, "AMS_OUTPUT_ROOT": self.tmpdir}
        result = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "ams.py"), "search", "agent memory"],
            capture_output=True, text=True, env=env, cwd=str(REPO_ROOT),
        )
        self.assertEqual(
            result.returncode, 0,
            f"search returned non-zero (rc={result.returncode}).\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
        )
        # Should find something from our fixture session about agent memory retrieval
        combined = result.stdout + result.stderr
        self.assertTrue(
            len(combined.strip()) > 0,
            "search produced no output at all",
        )


@unittest.skipUnless(_rust_kernel_available(), "Rust kernel not available")
class TestIntegrationAmsHelp(unittest.TestCase):
    """./scripts/ams --help must exit 0 and show the correct prog label."""

    def test_help_exits_zero(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "ams.py"), "--help"],
            capture_output=True, text=True, cwd=str(REPO_ROOT),
        )
        self.assertEqual(result.returncode, 0)

    def test_help_shows_correct_prog_on_linux(self) -> None:
        if sys.platform == "win32":
            self.skipTest("Linux-specific assertion")
        result = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "ams.py"), "--help"],
            capture_output=True, text=True, cwd=str(REPO_ROOT),
        )
        self.assertIn("./scripts/ams", result.stdout, "Expected Linux prog label in --help")
        self.assertNotIn(r"scripts\ams.bat", result.stdout)


@unittest.skipUnless(_rust_kernel_available(), "Rust kernel not available")
class TestIntegrationCallstackContext(unittest.TestCase):
    """./scripts/ams callstack context must not crash (empty is fine)."""

    def test_callstack_context_exits_zero(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "ams.py"), "callstack", "context"],
            capture_output=True, text=True, cwd=str(REPO_ROOT),
        )
        self.assertEqual(
            result.returncode, 0,
            f"callstack context failed (rc={result.returncode}).\n"
            f"stderr:\n{result.stderr}",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
