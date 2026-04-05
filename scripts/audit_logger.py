"""감사 로깅 및 기록 시스템."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from monitor_config import AUDIT_LOG_DIR, AUDIT_LOG_FILE, HEALTH_CHECK_FILE


class AuditLogger:
    """모든 폴더 변화를 기록하는 감사 로거."""

    def __init__(self) -> None:
        """AuditLogger 초기화."""
        AUDIT_LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.log_file = AUDIT_LOG_FILE
        self.health_file = HEALTH_CHECK_FILE

    def _timestamp(self) -> str:
        """타임스탬프 생성."""
        return datetime.now().isoformat(timespec="seconds")

    def log_event(
        self,
        event_type: str,
        details: dict[str, Any],
        status: str = "OK",
        severity: str = "INFO",
    ) -> None:
        """
        폴더 변화 이벤트 기록.

        Args:
            event_type: 이벤트 유형 (file_created, folder_moved, api_check 등)
            details: 이벤트 세부사항
            status: 상태 (OK, WARNING, ERROR, PROTECTED)
            severity: 심각도 (INFO, WARNING, ERROR)
        """
        entry = {
            "timestamp": self._timestamp(),
            "event_type": event_type,
            "status": status,
            "severity": severity,
            "details": details,
        }

        # 파일에 기록
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

        # 콘솔 출력 (심각한 항목만)
        if severity in ("WARNING", "ERROR"):
            timestamp = entry["timestamp"]
            msg = f"[{timestamp}] [{severity}] {event_type}: {details.get('message', '')}"
            print(msg)

    def log_file_change(self, path: Path, change_type: str, size: int = 0) -> None:
        """
        파일 변화 기록.

        Args:
            path: 파일 경로
            change_type: 변화 유형 (created, modified, deleted, moved)
            size: 파일 크기 (바이트)
        """
        self.log_event(
            event_type="file_change",
            details={
                "path": str(path),
                "change_type": change_type,
                "size_bytes": size,
            },
            status="OK",
            severity="INFO",
        )

    def log_folder_analysis(
        self,
        summary: dict[str, Any],
        anomalies: Optional[list[dict[str, Any]]] = None,
    ) -> None:
        """
        정기적 폴더 분석 결과 기록.

        Args:
            summary: 분석 요약 (파일 수, 총 크기 등)
            anomalies: 이상 항목 목록
        """
        severity = "WARNING" if anomalies else "INFO"
        self.log_event(
            event_type="folder_analysis",
            details={
                "summary": summary,
                "anomalies_count": len(anomalies or []),
                "anomalies": anomalies or [],
            },
            status="OK",
            severity=severity,
        )

    def log_api_health(self, api_name: str, status: str, details: dict) -> None:
        """
        API 헬스 체크 결과 기록.

        Args:
            api_name: API 이름 (groq, gemini)
            status: 상태 (OK, ERROR)
            details: 세부사항
        """
        self.log_event(
            event_type="api_health_check",
            details={
                "api": api_name,
                **details,
            },
            status=status,
            severity="ERROR" if status == "ERROR" else "INFO",
        )

    def log_protection_event(self, path: Path, attempted_action: str) -> None:
        """
        보호된 폴더 접근 시도 기록.

        Args:
            path: 보호된 경로
            attempted_action: 시도된 작업 (move, delete, modify)
        """
        self.log_event(
            event_type="protection_trigger",
            details={
                "protected_path": str(path),
                "attempted_action": attempted_action,
                "message": f"보호된 폴더 접근 차단: {attempted_action}",
            },
            status="PROTECTED",
            severity="WARNING",
        )

    def record_health_status(self, status: dict[str, Any]) -> None:
        """
        현재 시스템 헬스 상태를 JSON 파일에 기록.

        Args:
            status: 상태 딕셔너리
        """
        health_data = {
            "timestamp": self._timestamp(),
            "status": status,
        }
        with open(self.health_file, "w", encoding="utf-8") as f:
            json.dump(health_data, f, ensure_ascii=False, indent=2)

    def get_recent_logs(self, limit: int = 20) -> list[dict[str, Any]]:
        """
        최근 로그 조회.

        Args:
            limit: 조회할 최대 라인 수

        Returns:
            최근 로그 항목 목록
        """
        if not self.log_file.exists():
            return []

        logs = []
        with open(self.log_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    logs.append(json.loads(line.strip()))
                except json.JSONDecodeError:
                    continue

        return logs[-limit:]

    def summarize_audit(self) -> dict[str, Any]:
        """
        감사 로그 요약.

        Returns:
            요약 정보
        """
        logs = self.get_recent_logs(limit=1000)
        if not logs:
            return {"total_events": 0, "by_type": {}, "by_severity": {}}

        by_type: dict[str, int] = {}
        by_severity: dict[str, int] = {}

        for log in logs:
            event_type = log.get("event_type", "unknown")
            severity = log.get("severity", "INFO")

            by_type[event_type] = by_type.get(event_type, 0) + 1
            by_severity[severity] = by_severity.get(severity, 0) + 1

        return {
            "total_events": len(logs),
            "by_type": by_type,
            "by_severity": by_severity,
            "period_start": logs[0]["timestamp"] if logs else None,
            "period_end": logs[-1]["timestamp"] if logs else None,
        }
