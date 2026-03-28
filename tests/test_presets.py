from __future__ import annotations

import tempfile
import unittest

from app.errors import AppError
from app.presets import build_preset_request, maybe_expand_directory_request


class PresetTests(unittest.TestCase):
    def test_build_onboard_request_includes_required_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            prompt = build_preset_request("onboard", tmpdir)

        self.assertIn("온보딩 보고서", prompt)
        self.assertIn("기술 스택", prompt)
        self.assertIn(tmpdir, prompt)

    def test_maybe_expand_directory_request_turns_path_into_onboarding_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            prompt = maybe_expand_directory_request(tmpdir)

        self.assertIn("프로젝트 경로", prompt)
        self.assertIn("온보딩 보고서", prompt)

    def test_build_preset_request_rejects_missing_path(self) -> None:
        with self.assertRaises(AppError):
            build_preset_request("stack", "/path/that/does/not/exist")


if __name__ == "__main__":
    unittest.main()
