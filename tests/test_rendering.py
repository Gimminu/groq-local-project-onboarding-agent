from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from app.rendering import write_trace_files
from app.schema import AgentRunTrace, ToolStepTrace


class RenderingTests(unittest.TestCase):
    def test_write_trace_files_creates_json_and_markdown(self) -> None:
        trace = AgentRunTrace.create(
            request="README를 찾아줘",
            server_name="local-fs",
            model="openai/gpt-oss-20b",
            mode="safe",
            available_tools=["fs_list", "fs_read"],
            steps=[
                ToolStepTrace(
                    step_number=1,
                    reasoning="먼저 목록 확인",
                    tool_name="fs_list",
                    arguments={"path": "."},
                    is_error=False,
                    text="README.md",
                    structured_content={"items": ["README.md"]},
                )
            ],
            final_answer="README.md를 찾았습니다.",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path, markdown_path = write_trace_files(trace, Path(tmpdir))

            self.assertTrue(json_path.exists())
            self.assertTrue(markdown_path.exists())
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["final_answer"], "README.md를 찾았습니다.")
            markdown = markdown_path.read_text(encoding="utf-8")
            self.assertIn("Groq Project Onboarding Run", markdown)
            self.assertIn("README.md", markdown)
            self.assertTrue(json_path.name.startswith("project_onboarding_"))


if __name__ == "__main__":
    unittest.main()
