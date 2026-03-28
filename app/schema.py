from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.errors import AppError

DEFAULT_MODEL = "llama-3.3-70b-versatile"
STRICT_MODELS = {
    "openai/gpt-oss-20b",
    "openai/gpt-oss-120b",
}

PLANNER_DECISION_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["decision"],
    "properties": {
        "decision": {
            "type": "string",
            "enum": ["use_mcp_tool", "respond"],
        },
        "reason": {"type": "string", "minLength": 1},
        "selected_tool": {
            "type": ["string", "null"],
        },
        "selected_tool_arguments_json": {
            "type": ["string", "null"],
        },
        "user_reply": {
            "type": ["string", "null"],
        },
    },
}


@dataclass(frozen=True)
class ToolDescriptor:
    name: str
    title: str | None
    description: str | None
    input_schema: dict[str, Any] | None

    def to_prompt_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "title": self.title,
            "description": self.description,
            "input_schema": self.input_schema or {},
        }


@dataclass(frozen=True)
class PlannerDecision:
    action: str
    reasoning: str
    tool_name: str | None
    arguments: dict[str, Any]
    final_answer: str | None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PlannerDecision":
        if not isinstance(data, dict):
            raise AppError("플래너 응답은 JSON 객체여야 합니다.")

        raw_decision_value = data.get("decision", data.get("action"))
        if raw_decision_value is None:
            raise AppError("플래너 응답에 decision 또는 action 필드가 필요합니다.")

        raw_decision = _required_string(raw_decision_value, "decision").lower()
        if raw_decision == "use_mcp_tool":
            action = "tool_call"
        elif raw_decision == "respond":
            action = "final"
        elif raw_decision == "tool_call":
            action = "tool_call"
        elif raw_decision == "final":
            action = "final"
        else:
            raise AppError("decision은 use_mcp_tool/respond 또는 tool_call/final 이어야 합니다.")

        reasoning = _coerce_reasoning(
            data.get("reason", data.get("reasoning"))
        )
        tool_name = _optional_string(
            data.get("selected_tool", data.get("tool_name", data.get("name"))),
            "selected_tool",
        )
        arguments = _coerce_arguments(
            data.get(
                "selected_tool_arguments_json",
                data.get("arguments"),
            )
        )
        final_answer = _optional_string(
            data.get("user_reply", data.get("final_answer")),
            "user_reply",
        )

        if action == "tool_call" and not tool_name:
            raise AppError("tool_call action에는 tool_name이 필요합니다.")
        if action == "final" and not final_answer:
            raise AppError("final action에는 final_answer가 필요합니다.")

        return cls(
            action=action,
            reasoning=reasoning,
            tool_name=tool_name,
            arguments=arguments,
            final_answer=final_answer,
        )


@dataclass(frozen=True)
class ToolCallResult:
    is_error: bool
    text: str
    structured_content: Any | None


@dataclass(frozen=True)
class ToolStepTrace:
    step_number: int
    reasoning: str
    tool_name: str
    arguments: dict[str, Any]
    is_error: bool
    text: str
    structured_content: Any | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "step_number": self.step_number,
            "reasoning": self.reasoning,
            "tool_name": self.tool_name,
            "arguments": self.arguments,
            "is_error": self.is_error,
            "text": self.text,
            "structured_content": self.structured_content,
        }


@dataclass(frozen=True)
class AgentRunTrace:
    created_at: str
    request: str
    server_name: str
    model: str
    mode: str
    available_tools: list[str]
    steps: list[ToolStepTrace]
    final_answer: str

    @classmethod
    def create(
        cls,
        *,
        request: str,
        server_name: str,
        model: str,
        mode: str,
        available_tools: list[str],
        steps: list[ToolStepTrace],
        final_answer: str,
    ) -> "AgentRunTrace":
        return cls(
            created_at=datetime.now(timezone.utc).isoformat(),
            request=request,
            server_name=server_name,
            model=model,
            mode=mode,
            available_tools=available_tools,
            steps=steps,
            final_answer=final_answer,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "created_at": self.created_at,
            "request": self.request,
            "server_name": self.server_name,
            "model": self.model,
            "mode": self.mode,
            "available_tools": self.available_tools,
            "steps": [step.to_dict() for step in self.steps],
            "final_answer": self.final_answer,
        }


def _required_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AppError(f"{field_name}는 비어 있지 않은 문자열이어야 합니다.")
    return value.strip()


def _optional_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise AppError(f"{field_name}는 문자열 또는 null이어야 합니다.")
    stripped = value.strip()
    return stripped or None


def _parse_arguments_json(value: str | None) -> dict[str, Any]:
    if value is None:
        return {}
    try:
        parsed = json.loads(value)
    except Exception as exc:
        raise AppError("selected_tool_arguments_json는 JSON 객체 문자열이어야 합니다.") from exc
    if not isinstance(parsed, dict):
        raise AppError("selected_tool_arguments_json는 JSON 객체 문자열이어야 합니다.")
    return parsed


def _coerce_reasoning(value: Any) -> str:
    if value is None:
        return "모델이 별도 reasoning 필드를 제공하지 않았습니다."
    if isinstance(value, str) and value.strip():
        return value.strip()
    return "모델이 별도 reasoning 필드를 제공하지 않았습니다."


def _coerce_arguments(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return {}
        return _parse_arguments_json(stripped)
    raise AppError("tool arguments는 JSON 객체 또는 JSON 객체 문자열이어야 합니다.")
