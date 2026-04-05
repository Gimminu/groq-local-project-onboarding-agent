from __future__ import annotations

import json
from typing import Any

try:
    from groq import Groq
except ImportError:  # pragma: no cover - exercised when dependency is absent
    Groq = None

from app.errors import AppError
from app.llm.planner import BasePlanner
from app.prompting import build_messages
from app.schema import (
    DEFAULT_MODEL,
    PLANNER_DECISION_JSON_SCHEMA,
    STRICT_MODELS,
    PlannerDecision,
    ToolDescriptor,
    ToolStepTrace,
)


class GroqPlanner(BasePlanner):
    def __init__(
        self,
        api_key: str | None,
        model: str = DEFAULT_MODEL,
        client: Any | None = None,
    ) -> None:
        if not api_key:
            raise AppError(
                "GROQ_API_KEY가 설정되지 않았습니다. "
                "`.env` 파일이나 셸 환경변수에 API 키를 추가하세요."
            )
        super().__init__(api_key=api_key, model=model)

        if client is not None:
            self.client = client
            return

        if Groq is None:
            raise AppError(
                "`groq` 패키지가 설치되지 않았습니다. "
                "`pip install -r requirements.txt`를 먼저 실행하세요."
            )
        self.client = Groq(api_key=api_key)

    def decide(
        self,
        *,
        request: str,
        tools: list[ToolDescriptor],
        tool_history: list[ToolStepTrace],
        server_name: str,
        mode: str,
        remaining_steps: int,
        force_final: bool = False,
    ) -> PlannerDecision:
        if self.model in STRICT_MODELS:
            content = self._create_completion(
                messages=build_messages(
                    request=request,
                    tools=tools,
                    tool_history=tool_history,
                    server_name=server_name,
                    mode=mode,
                    remaining_steps=remaining_steps,
                    force_final=force_final,
                ),
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "automation_decision",
                        "schema": PLANNER_DECISION_JSON_SCHEMA,
                        "strict": True,
                    },
                },
            )
            return PlannerDecision.from_dict(self._parse_json(content))

        validation_feedback: str | None = None
        for _ in range(2):
            content = self._create_completion(
                messages=build_messages(
                    request=request,
                    tools=tools,
                    tool_history=tool_history,
                    server_name=server_name,
                    mode=mode,
                    remaining_steps=remaining_steps,
                    force_final=force_final,
                    validation_feedback=validation_feedback,
                ),
                response_format={"type": "json_object"},
            )
            parsed = self._parse_json(content)
            try:
                return PlannerDecision.from_dict(parsed)
            except AppError as exc:
                validation_feedback = str(exc)

        raise AppError(
            "플래너 응답 검증에 실패했습니다. "
            f"마지막 오류: {validation_feedback or '알 수 없는 오류'}"
        )

    def _create_completion(
        self,
        *,
        messages: list[dict[str, str]],
        response_format: dict[str, Any],
    ) -> str:
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                response_format=response_format,
            )
        except Exception as exc:  # pragma: no cover - depends on network/API
            msg = str(exc)
            if _is_odd_200_error(msg):
                raise AppError(f"Groq API 호출에 실패했습니다 [provider_internal_200]: {exc}") from exc
            if _is_rate_limited(msg):
                raise AppError(f"Groq API 호출에 실패했습니다 [rate_limit]: {exc}") from exc
            raise AppError(f"Groq API 호출에 실패했습니다: {exc}") from exc

        try:
            content = response.choices[0].message.content
        except Exception as exc:
            raise AppError("Groq 응답에서 message.content를 읽을 수 없습니다.") from exc

        if isinstance(content, str):
            stripped = content.strip()
            if not stripped:
                raise AppError("모델 응답이 비어 있습니다.")
            return stripped

        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, dict) and isinstance(item.get("text"), str):
                    parts.append(item["text"])
                elif hasattr(item, "text") and isinstance(item.text, str):
                    parts.append(item.text)
            joined = "".join(parts).strip()
            if joined:
                return joined

        raise AppError("모델 응답 형식을 해석할 수 없습니다.")

    def _parse_json(self, content: str) -> dict[str, Any]:
        candidate = _extract_json_candidate(content)
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError as exc:
            raise AppError("모델이 유효한 JSON 객체를 반환하지 않았습니다.") from exc

        if not isinstance(parsed, dict):
            raise AppError("모델 응답은 JSON 객체여야 합니다.")
        return parsed


def _extract_json_candidate(content: str) -> str:
    stripped = content.strip()

    if stripped.startswith("```"):
        lines = [line for line in stripped.splitlines() if not line.startswith("```")]
        stripped = "\n".join(lines).strip()

    if stripped.startswith("{") and stripped.endswith("}"):
        return stripped

    start = stripped.find("{")
    end = stripped.rfind("}")
    if start != -1 and end != -1 and start < end:
        return stripped[start : end + 1]

    return stripped


def _is_rate_limited(message: str) -> bool:
    normalized = message.lower()
    return any(signal in normalized for signal in ("429", "too many requests", "rate limit", "rate_limit"))


def _is_odd_200_error(message: str) -> bool:
    normalized = message.lower()
    signals = ("error code: 200", "status code 200", "status code: 200")
    return any(signal in normalized for signal in signals)
