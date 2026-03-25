#!/usr/bin/env python3
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import scripts.ams_common as ams_common


class TestAmsCommon(unittest.TestCase):
    def test_detect_msvc_linker_prefers_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            linker = Path(tmp) / "link.exe"
            linker.write_text("", encoding="utf-8")
            with mock.patch.dict(os.environ, {"AMS_MSVC_LINKER": str(linker)}, clear=False):
                detected = ams_common._detect_msvc_linker()
            self.assertEqual(detected, linker)

    def test_rust_backend_env_sets_msvc_linker_on_windows(self) -> None:
        fake_linker = Path(r"C:\VS\link.exe")
        with mock.patch.object(ams_common, "_detect_msvc_linker", return_value=fake_linker):
            with mock.patch.object(ams_common.sys, "platform", "win32"):
                env = ams_common.rust_backend_env(None)
        self.assertEqual(
            env["CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER"],
            str(fake_linker),
        )
        self.assertTrue(env["PATH"].split(os.pathsep)[0].endswith(r"C:\VS"))


if __name__ == "__main__":
    raise SystemExit(unittest.main())
