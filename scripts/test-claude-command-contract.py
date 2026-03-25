#!/usr/bin/env python3
from __future__ import annotations

import os
import unittest
import importlib.util
from pathlib import Path
import subprocess
from unittest.mock import patch

import ams_common
from ams_common import validate_claude_local_contract


REPO_ROOT = Path(__file__).resolve().parents[1]
HOOK_PATH = REPO_ROOT / ".claude" / "hooks" / "memory-context-hook.py"
GENERATOR_PATH = REPO_ROOT / "scripts" / "generate-claude-md.py"
GENERATOR_SPEC = importlib.util.spec_from_file_location("generate_claude_md_module", GENERATOR_PATH)
if GENERATOR_SPEC is None or GENERATOR_SPEC.loader is None:
    raise RuntimeError(f"unable to load {GENERATOR_PATH}")
GENERATOR_MODULE = importlib.util.module_from_spec(GENERATOR_SPEC)
GENERATOR_SPEC.loader.exec_module(GENERATOR_MODULE)
generate_claude_local_md = GENERATOR_MODULE.generate_claude_local_md


class ClaudeCommandContractTests(unittest.TestCase):
    def test_build_ams_wrapper_cmd_prefers_shell_wrapper_on_posix(self) -> None:
        with (
            patch.object(ams_common.sys, "platform", "linux"),
            patch.object(ams_common, "AMS_WRAPPER_SH", REPO_ROOT / "scripts" / "ams"),
            patch.object(ams_common, "AMS_WRAPPER_BAT", REPO_ROOT / "scripts" / "ams.bat"),
        ):
            cmd = ams_common.build_ams_wrapper_cmd("swarm-plan", "list")
        self.assertEqual([str(REPO_ROOT / "scripts" / "ams"), "swarm-plan", "list"], cmd)

    def test_active_swarm_plan_name_uses_wrapper_builder(self) -> None:
        with (
            patch.object(ams_common, "build_ams_wrapper_cmd", return_value=["wrapper", "swarm-plan", "list"]) as wrapper_cmd,
            patch("subprocess.run", return_value=subprocess.CompletedProcess(
                args=["wrapper", "swarm-plan", "list"],
                returncode=0,
                stdout="  demo-plan [active]\n",
                stderr="",
            )),
        ):
            active = ams_common.active_swarm_plan_name()
        wrapper_cmd.assert_called_once_with("swarm-plan", "list")
        self.assertEqual("demo-plan", active)

    def test_generated_claude_local_uses_wrapper_contract(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            content = generate_claude_local_md(
                dream_objects={"topic": [], "thread": [], "decision": [], "invariant": []},
                project_name="NetworkGraphMemory",
                session_count=0,
            )
        violations = validate_claude_local_contract(content)
        self.assertEqual([], violations)
        self.assertIn(r"scripts\ams.bat search", content)
        self.assertIn(r"scripts\ams.bat recall", content)
        self.assertIn("Wrapper override: `AMS_MEMORY_CMD`", content)

    def test_generated_claude_local_can_follow_env_override(self) -> None:
        with patch.dict(os.environ, {"AMS_MEMORY_CMD": r"C:\tools\ams-proxy.bat"}, clear=False):
            content = generate_claude_local_md(
                dream_objects={"topic": [], "thread": [], "decision": [], "invariant": [],},
                project_name="NetworkGraphMemory",
                session_count=0,
            )
        self.assertIn(r"C:\tools\ams-proxy.bat search", content)
        self.assertIn(r"C:\tools\ams-proxy.bat recall", content)
        self.assertIn("Wrapper override: `AMS_MEMORY_CMD`", content)

    def test_generated_claude_local_atlas_label_can_follow_env_override(self) -> None:
        with patch.dict(os.environ, {"AMS_MEMORY_CMD": r"C:\tools\ams-proxy.bat"}, clear=False):
            content = generate_claude_local_md(
                dream_objects={"topic": [{"label": "atlas"}], "thread": [], "decision": [], "invariant": []},
                project_name="NetworkGraphMemory",
                session_count=1,
                atlas_summary="atlas:0",
            )
        self.assertIn(r"C:\tools\ams-proxy.bat atlas page atlas:0", content)

    def test_hook_uses_wrapper_not_raw_memoryctl(self) -> None:
        hook_source = HOOK_PATH.read_text(encoding="utf-8")
        self.assertIn("build_ams_wrapper_cmd", hook_source)
        self.assertNotIn("build_memoryctl_cmd", hook_source)


if __name__ == "__main__":
    unittest.main()
