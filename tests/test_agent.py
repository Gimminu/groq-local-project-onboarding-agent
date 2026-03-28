from __future__ import annotations

import unittest

from app.agent import AutomationAgent
from app.config import ServerConfig
from app.schema import PlannerDecision, ToolCallResult, ToolDescriptor


class FakePlanner:
    def __init__(self, decisions: list[PlannerDecision]) -> None:
        self.decisions = decisions
        self.calls = []

    def decide(self, **kwargs) -> PlannerDecision:
        self.calls.append(kwargs)
        if not self.decisions:
            raise AssertionError("더 이상 준비된 decision이 없습니다.")
        return self.decisions.pop(0)


class FakeClient:
    def __init__(self, server_config: ServerConfig) -> None:
        self.server_config = server_config
        self.calls = []
        self.tools = [
            ToolDescriptor("fs_list", None, "list files", {}),
            ToolDescriptor("fs_read", None, "read file", {}),
            ToolDescriptor("fs_write", None, "write file", {}),
        ]

    async def __aenter__(self) -> "FakeClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def list_tools(self):
        return self.tools

    async def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        return ToolCallResult(
            is_error=False,
            text="README.md\npackage.json",
            structured_content={"items": ["README.md", "package.json"]},
        )


class AgentTests(unittest.IsolatedAsyncioTestCase):
    async def test_agent_executes_tool_then_returns_final_answer(self) -> None:
        planner = FakePlanner(
            [
                PlannerDecision(
                    action="tool_call",
                    reasoning="먼저 파일 목록을 확인해야 한다.",
                    tool_name="fs_list",
                    arguments={"path": "."},
                    final_answer=None,
                ),
                PlannerDecision(
                    action="final",
                    reasoning="이제 답을 정리할 수 있다.",
                    tool_name=None,
                    arguments={},
                    final_answer="README.md와 package.json을 찾았습니다.",
                ),
            ]
        )
        agent = AutomationAgent(planner=planner, client_cls=FakeClient, max_tool_steps=4)
        server_config = ServerConfig(
            name="local-fs",
            command="node",
            args=["server.js"],
            env={},
        )

        trace = await agent.execute_request(
            request="현재 루트에서 핵심 파일을 찾아줘",
            server_config=server_config,
            mode="safe",
            model="openai/gpt-oss-20b",
        )

        self.assertEqual(trace.final_answer, "README.md와 package.json을 찾았습니다.")
        self.assertEqual(len(trace.steps), 1)
        self.assertEqual(trace.steps[0].tool_name, "fs_list")

    async def test_agent_marks_disallowed_tool_as_error_step(self) -> None:
        planner = FakePlanner(
            [
                PlannerDecision(
                    action="tool_call",
                    reasoning="파일을 쓰고 싶다.",
                    tool_name="fs_write",
                    arguments={"path": "notes.txt", "content": "hello"},
                    final_answer=None,
                ),
                PlannerDecision(
                    action="final",
                    reasoning="허용되지 않아 종료한다.",
                    tool_name=None,
                    arguments={},
                    final_answer="safe 모드에서는 fs_write를 사용할 수 없습니다.",
                ),
            ]
        )
        agent = AutomationAgent(planner=planner, client_cls=FakeClient, max_tool_steps=4)
        server_config = ServerConfig(
            name="local-fs",
            command="node",
            args=["server.js"],
            env={},
        )

        trace = await agent.execute_request(
            request="notes.txt를 만들어줘",
            server_config=server_config,
            mode="safe",
            model="openai/gpt-oss-20b",
        )

        self.assertTrue(trace.steps[0].is_error)
        self.assertIn("허용되지 않은 tool", trace.steps[0].text)


if __name__ == "__main__":
    unittest.main()
