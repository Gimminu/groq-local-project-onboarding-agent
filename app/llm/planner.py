"""추상 LLM 플래너 인터페이스."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from app.schema import PlannerDecision, ToolDescriptor, ToolStepTrace


class BasePlanner(ABC):
    """LLM 기반 플래너의 추상 기본 클래스."""

    def __init__(self, api_key: str | None, model: str) -> None:
        """
        플래너 초기화.

        Args:
            api_key: API 키
            model: 사용할 모델 이름
        """
        self.api_key = api_key
        self.model = model

    @abstractmethod
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
        """
        다음 작업을 결정합니다.

        Args:
            request: 사용자 요청
            tools: 사용 가능한 툴 목록
            tool_history: 이전 툴 실행 이력
            server_name: MCP 서버 이름
            mode: 실행 모드
            remaining_steps: 남은 단계 수
            force_final: True면 최종 답변을 강제

        Returns:
            플래너 결정사항
        """
        ...
