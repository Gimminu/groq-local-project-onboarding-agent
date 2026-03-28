from __future__ import annotations

import json
from typing import Any

from app.schema import ToolDescriptor, ToolStepTrace

SYSTEM_PROMPT = """
너는 로컬 프로젝트 온보딩용 MCP 에이전트다.
주어진 MCP tool 목록만 사용해서 사용자의 요청을 단계적으로 해결하라.

규칙:
- 응답 JSON의 decision은 use_mcp_tool 또는 respond 중 하나여야 한다.
- 한 번에 하나의 tool만 호출하라.
- 제공되지 않은 tool 이름을 만들지 마라.
- 입력 스키마에 맞는 tool 인자만 만들어라.
- 이 프로젝트의 기본 목적은 로컬 프로젝트를 읽고 온보딩 보고서를 만드는 것이다.
- 파일 삭제, 대량 변경, 원격 실행처럼 위험할 수 있는 작업은 현재 mode에서 허용된 tool이 있을 때만 수행하라.
- 근거 없는 추측 대신 먼저 fs_list / fs_read 같은 탐색 tool을 우선 사용하라.
- README, requirements.txt, pyproject.toml, package.json, Makefile, Dockerfile, .env.example, src/app/main 진입 파일을 우선 확인하라.
- 실행 명령, 환경 변수, 핵심 파일 추천은 반드시 파일 근거를 바탕으로 정리하라.
- 충분한 정보가 모이면 바로 respond로 끝내라.
- user_reply는 한국어로 간결하게 작성하라.
- 가능하면 user_reply를 다음 관점으로 정리하라: 프로젝트 소개, 기술 스택, 실행법, 핵심 파일, 위험 요소.
- selected_tool_arguments_json에는 tool 인자 JSON 객체를 문자열로 넣어라. 예: "{\"path\": \".\"}"
- 절대 name / arguments 형식의 tool-calling 출력이나 함수호출 형식을 사용하지 마라.
- 결과는 반드시 JSON만 반환하라.
""".strip()


def build_messages(
    *,
    request: str,
    tools: list[ToolDescriptor],
    tool_history: list[ToolStepTrace],
    server_name: str,
    mode: str,
    remaining_steps: int,
    force_final: bool,
    validation_feedback: str | None = None,
) -> list[dict[str, str]]:
    tools_blob = json.dumps(
        [tool.to_prompt_dict() for tool in tools],
        ensure_ascii=False,
        indent=2,
    )
    history_blob = json.dumps(
        [_history_dict(step) for step in tool_history],
        ensure_ascii=False,
        indent=2,
    )

    instructions = [
        f"현재 MCP 서버 이름: {server_name}",
        f"현재 mode: {mode}",
        f"남은 tool 호출 기회: {remaining_steps}",
        "tool이 없어도 답할 수 있으면 respond를 사용하라.",
        "프로젝트 분석에서는 경로와 파일명을 근거로 답하고, 없는 내용을 추측하지 마라.",
    ]
    if force_final:
        instructions.append("이번 턴은 반드시 decision=respond 로 응답하라.")
    if validation_feedback:
        instructions.append(
            "이전 응답에 검증 오류가 있었다. "
            f"다음 오류를 반드시 수정하라: {validation_feedback}"
        )

    user_prompt = "\n".join(
        [
            *instructions,
            "",
            "[사용자 요청]",
            request.strip(),
            "",
            "[사용 가능한 MCP Tools]",
            tools_blob,
            "",
            "[지금까지의 Tool 실행 기록]",
            history_blob,
        ]
    )

    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]


def _history_dict(step: ToolStepTrace) -> dict[str, Any]:
    structured = step.structured_content
    if structured is not None:
        structured_repr = json.dumps(structured, ensure_ascii=False)
        if len(structured_repr) > 800:
            structured_repr = structured_repr[:800] + "...(truncated)"
    else:
        structured_repr = None

    text = step.text
    if len(text) > 800:
        text = text[:800] + "...(truncated)"

    return {
        "step_number": step.step_number,
        "reasoning": step.reasoning,
        "tool_name": step.tool_name,
        "arguments": step.arguments,
        "is_error": step.is_error,
        "text": text,
        "structured_content": structured_repr,
    }
