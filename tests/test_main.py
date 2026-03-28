from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import main

class FakeStdin(io.StringIO):
    def __init__(self, value: str, *, is_tty: bool) -> None:
        super().__init__(value)
        self._is_tty = is_tty

    def isatty(self) -> bool:
        return self._is_tty


class MainTests(unittest.TestCase):
    def test_parse_args_allows_empty_invocation(self) -> None:
        args = main.parse_args([])
        self.assertIsNone(args.request)
        self.assertFalse(args.stdin)
        self.assertFalse(args.interactive)

    def test_run_enters_interactive_shell_without_request(self) -> None:
        fake_stdin = FakeStdin("", is_tty=True)

        with patch.object(main.sys, "stdin", fake_stdin):
            with patch("main.run_interactive_shell", return_value=0) as mock_shell:
                exit_code = main.run([])

        self.assertEqual(exit_code, 0)
        mock_shell.assert_called_once()

    def test_run_lists_servers_from_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "mcp.json"
            config_path.write_text(
                json.dumps(
                    {
                        "servers": {
                            "local-fs": {
                                "command": "node",
                                "args": ["server.js"],
                                "env": {},
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main.run(["--config", str(config_path), "--list-servers"])

        self.assertEqual(exit_code, 0)
        self.assertIn("local-fs", stdout.getvalue())

    def test_execute_and_print_relays_saved_paths(self) -> None:
        with patch(
            "main.run_request_once",
            return_value=(
                "작업을 완료했습니다.",
                Path("outputs/run.json"),
                Path("outputs/run.md"),
            ),
        ):
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main.execute_and_print(
                    request="README 찾아줘",
                    config_path="dummy",
                    server_name="local-fs",
                    mode="safe",
                    model="openai/gpt-oss-20b",
                    output_dir=Path("outputs"),
                )

        self.assertEqual(exit_code, 0)
        self.assertIn("작업을 완료했습니다.", stdout.getvalue())
        self.assertIn("outputs/run.json", stdout.getvalue())

    def test_run_converts_directory_request_to_onboarding_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("main.run_request_once") as mock_run_request:
                mock_run_request.return_value = (
                    "분석 완료",
                    Path("outputs/run.json"),
                    Path("outputs/run.md"),
                )
                exit_code = main.run([tmpdir])

        self.assertEqual(exit_code, 0)
        request_text = mock_run_request.call_args.kwargs["request"]
        self.assertIn("온보딩 보고서", request_text)
        self.assertIn(tmpdir, request_text)

    def test_handle_shell_command_runs_onboard_preset(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("main.execute_and_print", return_value=0) as mock_execute:
                should_exit, server_name, mode, model, output_dir = main.handle_shell_command(
                    raw=f"/onboard {tmpdir}",
                    config_path="dummy",
                    server_name="local-fs",
                    mode="safe",
                    model="llama-3.3-70b-versatile",
                    output_dir=Path("outputs"),
                )

        self.assertFalse(should_exit)
        self.assertEqual(server_name, "local-fs")
        self.assertEqual(mode, "safe")
        self.assertEqual(model, "llama-3.3-70b-versatile")
        self.assertEqual(output_dir, Path("outputs"))
        request_text = mock_execute.call_args.kwargs["request"]
        self.assertIn("온보딩 보고서", request_text)
        self.assertIn(tmpdir, request_text)


if __name__ == "__main__":
    unittest.main()
