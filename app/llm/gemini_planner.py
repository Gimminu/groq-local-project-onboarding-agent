"""Gemini API 기반 LLM 플래너."""

from __future__ import annotations

import json
import warnings
from typing import Any

try:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        import google.generativeai as genai
except ImportError:  # pragma: no cover - exercised when dependency is absent
    genai = None

from app.errors import AppError
from app.llm.planner import BasePlanner
from app.prompting import build_messages
from app.schema import (
    DEFAULT_MODEL,
    PLANNER_DECISION_JSON_SCHEMA,
    PlannerDecision,
    ToolDescriptor,
    ToolStepTrace,
)


class GeminiPlanner(BasePlanner):
    """Gemini API를 기반으로 하는 LLM 플래너."""

    def __init__(
        self,
        api_key: str | None,
        model: str = "gemini-2.0-flash",
    ) -> None:
        """
        GeminiPlanner 초기화.

        Args:
            api_key: Gemini API 키
            model: 사용할 Gemini 모델 (기본값: gemini-2.0-flash)
        """
        if not api_key:
            raise AppError(
                "GEMINI_API_KEY가 설정되지 않았습니다. "
                "`.env` 파일이나 셸 환경변수에 API 키를 추가하세요."
            )
        super().__init__(api_key=api_key, model=model)

        if genai is None:
            raise AppError(
                "`google-generativeai` 패키지가 설치되지 않았습니다. "
                "`pip install -r requirements.txt`를 먼저 실행하세요."
            )

        genai.configure(api_key=api_key)
        self.client = genai.GenerativeModel(model)

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
        """플래너 결정 수행."""
        validation_feedback: str | None = None
        for attempt in range(2):
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
            )
            parsed = self._parse_json(content)
            try:
                return PlannerDecision.from_dict(parsed)
            except AppError as exc:
                validation_feedback = str(exc)
                if attempt == 1:
                    raise AppError(
                        "플래너 응답 검증에 실패했습니다. "
                        f"마지막 오류: {validation_feedback}"
                    ) from exc

        raise AppError("플래너 응답 검증에 재시도 후에도 실패했습니다.")

    def _create_completion(
        self,
        *,
        messages: list[dict[str, str]],
    ) -> str:
        """API 호출을 통해 완성 응답 생성."""
        try:
            # Gemini 형식으로 변환 (system + user messages)
            history: list[dict[str, str]] = []
            system_message = None

            for msg in messages:
                role = msg.get("role", "user")
                content = msg.get("content", "")

                if role == "system":
                    system_message = content
                else:
                    history.append({
                        "role": "user" if role == "user" else "model",
                        "parts": [content],
                    })

            # Gemini API 호출
            response = self.client.generate_content(
                contents=history,
                generation_config={
                    "temperature": 1,
                    "response_mime_type": "application/json",
                },
            )
        except Exception as exc:  # pragma: no cover - depends on network/API
            msg = str(exc)
            if _is_gemini_quota(msg):
                raise AppError(f"Gemini API 호출에 실패했습니다 [quota]: {exc}") from exc
            if _is_rate_limited(msg):
                raise AppError(f"Gemini API 호출에 실패했습니다 [rate_limit]: {exc}") from exc
            raise AppError(f"Gemini API 호출에 실패했습니다: {exc}") from exc

        try:
            content = response.text
        except Exception as exc:
            raise AppError("Gemini 응답에서 text를 읽을 수 없습니다.") from exc

        if not content or not content.strip():
            raise AppError("모델 응답이 비어 있습니다.")

        return content.strip()

    def _parse_json(self, content: str) -> dict[str, Any]:
        """응답 내용에서 JSON 파싱."""
        candidate = _extract_json_candidate(content)
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError as exc:
            raise AppError("모델이 유효한 JSON 객체를 반환하지 않았습니다.") from exc

        if not isinstance(parsed, dict):
            raise AppError("모델 응답은 JSON 객체여야 합니다.")
        return parsed


def _extract_json_candidate(content: str) -> str:
    """응답 내용에서 JSON 객체 추출."""
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


def _is_gemini_quota(message: str) -> bool:
    normalized = message.lower()
    signals = ("resource_exhausted", "quota exceeded", "statuscode.resource_exhausted", "429")
    return any(signal in normalized for signal in signals)
