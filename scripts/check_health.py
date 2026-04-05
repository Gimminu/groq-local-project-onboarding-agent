"""API 및 시스템 헬스 체크."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from audit_logger import AuditLogger
from monitor_config import (
    API_CONFIG,
    PROTECTED_PATHS,
    WATCH_ROOTS,
)


class HealthChecker:
    """API 및 폴더 구조 헬스 체크."""

    def __init__(self) -> None:
        """HealthChecker 초기화."""
        self.logger = AuditLogger()
        self.issues: list[dict[str, Any]] = []

    def check_api_keys(self) -> dict[str, Any]:
        """
        API 키 설정 검증.

        Returns:
            API 상태 정보
        """
        api_status = {}

        for provider, config in API_CONFIG.items():
            api_key = os.getenv(config["env_key"])

            if not api_key:
                status = "ERROR"
                message = f"{config['name']} API 키가 설정되지 않았습니다."
                self.issues.append({
                    "type": "missing_api_key",
                    "provider": provider,
                    "message": message,
                })
            else:
                # 키 길이로 기본 검증 (실제 API 호출은 비용 발생)
                if len(api_key) < 10:
                    status = "ERROR"
                    message = f"{config['name']} API 키가 너무 짧습니다."
                    self.issues.append({
                        "type": "invalid_api_key",
                        "provider": provider,
                        "message": message,
                    })
                else:
                    status = "OK"
                    message = f"{config['name']} API 키가 설정되어 있습니다."
                    # 안전상 키의 앞 8자와 뒤 8자만 표시
                    masked_key = f"{api_key[:8]}...{api_key[-8:]}"

            api_status[provider] = {
                "status": status,
                "message": message,
                "key_configured": api_key is not None,
            }

            self.logger.log_api_health(provider, status, {
                "message": message,
            })

        return api_status

    def check_protected_paths(self) -> dict[str, Any]:
        """
        보호된 폴더 경로 검증.

        Returns:
            경로 상태 정보
        """
        protected_status = {}

        for path in PROTECTED_PATHS:
            exists = path.exists()
            is_dir = path.is_dir() if exists else False

            status = "OK" if (exists and is_dir) else "WARNING"
            message = (
                f"✓ 보호됨: {path}" if (exists and is_dir)
                else f"⚠ 경로 확인 필요: {path}"
            )

            if status == "WARNING":
                self.issues.append({
                    "type": "protected_path_issue",
                    "path": str(path),
                    "message": message,
                })

            protected_status[str(path)] = {
                "status": status,
                "exists": exists,
                "is_directory": is_dir,
                "message": message,
            }

        return protected_status

    def check_watch_roots(self) -> dict[str, Any]:
        """
        Watch 대상 폴더 검증.

        Returns:
            Watch 폴더 상태
        """
        watch_status = {}

        for root in WATCH_ROOTS:
            exists = root.exists()
            is_dir = root.is_dir() if exists else False

            if not exists:
                status = "ERROR"
                message = f"폴더가 없습니다: {root}"
                self.issues.append({
                    "type": "missing_watch_root",
                    "path": str(root),
                    "message": message,
                })
            elif not is_dir:
                status = "ERROR"
                message = f"파일입니다 (폴더가 아님): {root}"
                self.issues.append({
                    "type": "invalid_watch_root",
                    "path": str(root),
                    "message": message,
                })
            else:
                status = "OK"
                message = f"✓ 모니터링 중: {root}"

            try:
                file_count = len(list(root.glob("*"))) if exists and is_dir else 0
            except PermissionError:
                file_count = -1
                status = "WARNING"
                message = f"⚠ 접근 권한 없음: {root}"

            watch_status[str(root)] = {
                "status": status,
                "exists": exists,
                "is_directory": is_dir,
                "file_count": file_count,
                "message": message,
            }

        return watch_status

    def check_folder_structure(self) -> dict[str, Any]:
        """
        프로젝트 폴더 구조 검증.

        Returns:
            폴더 구조 상태
        """
        workspace_root = Path.home() / "Documents" / "projects" / "workspace" / "mcp-workspace" / "code" / "groq-mcp-mac-agent"

        expected_dirs = [
            "app",
            "app/llm",
            "app/index_v2",
            "tests",
            "scripts",
        ]

        structure_status = {}
        all_ok = True

        for dir_path in expected_dirs:
            full_path = workspace_root / dir_path
            exists = full_path.exists()
            is_dir = full_path.is_dir() if exists else False

            status = "OK" if (exists and is_dir) else "ERROR"
            if status == "ERROR":
                all_ok = False
                self.issues.append({
                    "type": "missing_directory",
                    "path": str(full_path),
                    "message": f"디렉토리 없음: {dir_path}",
                })

            structure_status[dir_path] = {
                "status": status,
                "exists": exists,
                "is_directory": is_dir,
            }

        return {
            "workspace_root": str(workspace_root),
            "all_ok": all_ok,
            "directories": structure_status,
        }

    def run_full_check(self) -> dict[str, Any]:
        """
        전체 헬스 체크 실행.

        Returns:
            종합 체크 결과
        """
        self.issues = []

        result = {
            "timestamp": self._get_timestamp(),
            "api_keys": self.check_api_keys(),
            "protected_paths": self.check_protected_paths(),
            "watch_roots": self.check_watch_roots(),
            "folder_structure": self.check_folder_structure(),
            "issues": self.issues,
            "overall_status": "OK" if not self.issues else "WARNING",
        }

        # 헬스 상태 기록
        self.logger.record_health_status(result)

        return result

    @staticmethod
    def _get_timestamp() -> str:
        """현재 타임스탬프 반환."""
        from datetime import datetime
        return datetime.now().isoformat(timespec="seconds")

    def print_report(self, result: dict[str, Any]) -> None:
        """
        체크 결과 출력.

        Args:
            result: 체크 결과
        """
        print("\n" + "=" * 70)
        print("🔍 시스템 헬스 체크 리포트")
        print("=" * 70)

        print(f"\n⏰ 시간: {result['timestamp']}")
        print(f"📊 상태: {result['overall_status']}")

        print("\n🔑 API 키:")
        for api_name, status in result["api_keys"].items():
            icon = "✓" if status["status"] == "OK" else "✗"
            print(f"  {icon} {api_name}: {status['message']}")

        print("\n🛡️  보호된 경로:")
        for path, info in result["protected_paths"].items():
            icon = "✓" if info["status"] == "OK" else "⚠"
            print(f"  {icon} {Path(path).name}: {info['message']}")

        print("\n📁 Watch 폴더:")
        for path, info in result["watch_roots"].items():
            icon = "✓" if info["status"] == "OK" else "⚠"
            file_count = f"({info['file_count']} 항목)" if info["file_count"] >= 0 else "(접근 불가)"
            print(f"  {icon} {Path(path).name}: {info['message']} {file_count}")

        print("\n📂 폴더 구조:")
        struct = result["folder_structure"]
        icon = "✓" if struct["all_ok"] else "✗"
        print(f"  {icon} {struct['workspace_root']}")

        if result["issues"]:
            print("\n⚠️  발견된 문제:")
            for i, issue in enumerate(result["issues"], 1):
                print(f"  {i}. [{issue['type']}] {issue['message']}")
        else:
            print("\n✅ 모든 항목이 정상입니다!")

        print("\n" + "=" * 70 + "\n")
