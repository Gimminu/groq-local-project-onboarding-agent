from __future__ import annotations

from app.config import ServerConfig
from app.errors import AppError
from app.mcp_client import StdioMCPToolClient
from app.policy import filter_tools_for_mode, is_tool_allowed
from app.schema import AgentRunTrace, PlannerDecision, ToolStepTrace


class AutomationAgent:
    def __init__(
        self,
        planner,
        client_cls=StdioMCPToolClient,
        max_tool_steps: int = 6,
    ) -> None:
        self.planner = planner
        self.client_cls = client_cls
        self.max_tool_steps = max_tool_steps

    async def list_available_tools(
        self,
        server_config: ServerConfig,
        mode: str,
    ):
        async with self.client_cls(server_config) as client:
            tools = await client.list_tools()
        return filter_tools_for_mode(tools, mode)

    async def execute_request(
        self,
        *,
        request: str,
        server_config: ServerConfig,
        mode: str,
        model: str,
    ) -> AgentRunTrace:
        if self.planner is None:
            raise AppError("이 작업에는 Groq 플래너가 필요합니다.")

        normalized_request = request.strip()
        if not normalized_request:
            raise AppError("실행할 요청이 비어 있습니다.")

        async with self.client_cls(server_config) as client:
            discovered_tools = await client.list_tools()
            available_tools = filter_tools_for_mode(discovered_tools, mode)
            if not available_tools:
                raise AppError(
                    f"server={server_config.name}, mode={mode} 조합에서 사용할 수 있는 tool이 없습니다."
                )

            tool_history: list[ToolStepTrace] = []

            for step_number in range(1, self.max_tool_steps + 1):
                decision = self.planner.decide(
                    request=normalized_request,
                    tools=available_tools,
                    tool_history=tool_history,
                    server_name=server_config.name,
                    mode=mode,
                    remaining_steps=self.max_tool_steps - step_number + 1,
                )

                if decision.action == "final":
                    return AgentRunTrace.create(
                        request=normalized_request,
                        server_name=server_config.name,
                        model=model,
                        mode=mode,
                        available_tools=[tool.name for tool in available_tools],
                        steps=tool_history,
                        final_answer=decision.final_answer or "",
                    )

                tool_history.append(
                    await self._execute_tool_step(
                        step_number=step_number,
                        decision=decision,
                        available_tools=available_tools,
                        client=client,
                        mode=mode,
                    )
                )

            forced_final = self.planner.decide(
                request=normalized_request,
                tools=available_tools,
                tool_history=tool_history,
                server_name=server_config.name,
                mode=mode,
                remaining_steps=0,
                force_final=True,
            )
            final_answer = forced_final.final_answer or self._fallback_answer(tool_history)
            return AgentRunTrace.create(
                request=normalized_request,
                server_name=server_config.name,
                model=model,
                mode=mode,
                available_tools=[tool.name for tool in available_tools],
                steps=tool_history,
                final_answer=final_answer,
            )

    async def _execute_tool_step(
        self,
        *,
        step_number: int,
        decision: PlannerDecision,
        available_tools,
        client,
        mode: str,
    ) -> ToolStepTrace:
        tool_name = decision.tool_name or ""
        if not is_tool_allowed(tool_name, mode, available_tools):
            return ToolStepTrace(
                step_number=step_number,
                reasoning=decision.reasoning,
                tool_name=tool_name,
                arguments=decision.arguments,
                is_error=True,
                text=f"허용되지 않은 tool입니다: {tool_name} (mode={mode})",
                structured_content=None,
            )

        result = await client.call_tool(tool_name, decision.arguments)
        return ToolStepTrace(
            step_number=step_number,
            reasoning=decision.reasoning,
            tool_name=tool_name,
            arguments=decision.arguments,
            is_error=result.is_error,
            text=result.text,
            structured_content=result.structured_content,
        )

    def _fallback_answer(self, tool_history: list[ToolStepTrace]) -> str:
        if not tool_history:
            return "실행 가능한 tool 결과가 없어 요청을 마무리하지 못했습니다."
        last_step = tool_history[-1]
        return (
            "최대 step 수에 도달해 중간 결과만 정리합니다.\n"
            f"- 마지막 tool: {last_step.tool_name}\n"
            f"- 마지막 결과: {last_step.text}"
        )
