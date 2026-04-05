"""
메인 모니터링 스크립트.

사용법:
  python scripts/monitor.py --check-health          # 헬스 체크 한 번 실행
  python scripts/monitor.py --watch                 # 이벤트 기반 모니터링 (실시간)
  python scripts/monitor.py --report                # 감사 보고서 출력
  python scripts/monitor.py --status                # 현재 상태 보기
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - fallback for minimal environments
    def _fallback_load_dotenv(dotenv_path=None, override: bool = False) -> bool:
        candidate = Path(dotenv_path) if dotenv_path else Path(".env")
        if not candidate.exists():
            return False
        loaded = False
        for raw_line in candidate.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"'")
            if not key:
                continue
            if override or key not in os.environ:
                os.environ[key] = value
            loaded = True
        return loaded
    load_dotenv = _fallback_load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

# 현재 스크립트 디렉토리를 경로에 추가
sys.path.insert(0, str(Path(__file__).parent))

from audit_logger import AuditLogger
from check_health import HealthChecker
from monitor_config import AUDIT_LOG_DIR


def run_health_check() -> None:
    """헬스 체크 한 번 실행."""
    print("🔍 헬스 체크 중...")
    checker = HealthChecker()
    result = checker.run_full_check()
    checker.print_report(result)


def print_status() -> None:
    """현재 시스템 상태 출력."""
    logger = AuditLogger()

    if not logger.health_file.exists():
        print("❌ 헬스 체크 기록이 없습니다. 먼저 헬스 체크를 실행하세요.")
        print("   python scripts/monitor.py --check-health")
        return

    import json
    with open(logger.health_file, "r", encoding="utf-8") as f:
        health_data = json.load(f)

    status = health_data["status"]
    timestamp = health_data["timestamp"]

    print("\n" + "=" * 70)
    print("📊 현재 시스템 상태")
    print("=" * 70)
    print(f"\n⏰ 마지막 갱신: {timestamp}")
    print(f"📈 전체 상태: {status['overall_status']}")

    if status["issues"]:
        print(f"\n⚠️  문제 발견: {len(status['issues'])}건")
        for issue in status["issues"]:
            print(f"  - [{issue['type']}] {issue['message']}")
    else:
        print("\n✅ 문제 없음")

    print("\n" + "=" * 70 + "\n")


def print_audit_report() -> None:
    """감사 로그 보고서 출력."""
    logger = AuditLogger()
    summary = logger.summarize_audit()

    print("\n" + "=" * 70)
    print("📋 감사 로그 보고서")
    print("=" * 70)

    if summary["total_events"] == 0:
        print("\n기록된 이벤트가 없습니다.")
        return

    print(f"\n📊 총 이벤트: {summary['total_events']}건")

    if summary["period_start"]:
        print(f"📅 기간: {summary['period_start']} ~ {summary['period_end']}")

    print("\n📈 이벤트 유형별:")
    for event_type, count in sorted(summary["by_type"].items(), key=lambda x: x[1], reverse=True):
        print(f"  - {event_type}: {count}건")

    print("\n⚠️  심각도별:")
    for severity, count in sorted(summary["by_severity"].items()):
        print(f"  - {severity}: {count}건")

    print("\n📜 최근 10개 이벤트:")
    recent = logger.get_recent_logs(limit=10)
    for i, log in enumerate(recent, 1):
        timestamp = log["timestamp"]
        event_type = log["event_type"]
        severity = log["severity"]
        details = log["details"].get("message", log["details"].get("path", ""))
        print(f"  {i}. [{timestamp}] {event_type} ({severity}) - {details}")

    print("\n" + "=" * 70 + "\n")


def watch_folder_changes() -> None:
    """이벤트 기반 파일 변화 감지 (실시간)."""
    try:
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler
    except ImportError:
        print("❌ watchdog 패키지가 설치되지 않았습니다.")
        print("   pip install watchdog")
        return

    from monitor_config import WATCH_ROOTS, PROTECTED_PATHS, IGNORE_PATTERNS

    logger = AuditLogger()

    class FolderChangeHandler(FileSystemEventHandler):
        """파일 변화 이벤트 핸들러."""

        @staticmethod
        def _to_path(raw: object) -> Path:
            if isinstance(raw, bytes):
                return Path(raw.decode(errors="ignore"))
            return Path(str(raw))

        def _should_ignore(self, path: Path) -> bool:
            """무시할 파일인지 확인."""
            path_str = str(path)

            # 보호된 폴더 확인
            for protected in PROTECTED_PATHS:
                if protected in path.parents or protected == path:
                    # 보호된 폴더 내 변화는 경고만
                    logger.log_protection_event(path, "detected")
                    return True

            # 패턴 매칭
            for pattern in IGNORE_PATTERNS:
                if pattern in path_str:
                    return True

            return False

        def on_created(self, event):
            """파일/폴더 생성."""
            if event.is_directory:
                return
            src_path = self._to_path(event.src_path)
            if not self._should_ignore(src_path):
                logger.log_file_change(src_path, "created", 0)
                print(f"✨ 생성됨: {src_path.name}")

        def on_deleted(self, event):
            """파일/폴더 삭제."""
            if event.is_directory:
                return
            src_path = self._to_path(event.src_path)
            if not self._should_ignore(src_path):
                logger.log_file_change(src_path, "deleted", 0)
                print(f"🗑️  삭제됨: {src_path.name}")

        def on_modified(self, event):
            """파일 수정."""
            if event.is_directory:
                return
            src_path = self._to_path(event.src_path)
            if not self._should_ignore(src_path):
                logger.log_file_change(src_path, "modified", 0)

    handler = FolderChangeHandler()
    observer = Observer()

    print("👀 파일 변화 감시 중... (Ctrl+C로 종료)")
    print(f"Watch 대상: {', '.join([str(r) for r in WATCH_ROOTS])}\n")

    for root in WATCH_ROOTS:
        if root.exists():
            observer.schedule(handler, str(root), recursive=True)

    observer.start()

    try:
        observer.join()
    except KeyboardInterrupt:
        print("\n👋 감시 종료")
        observer.stop()


def main() -> int:
    """메인 진입점."""
    parser = argparse.ArgumentParser(
        description="Groq-MCP 폴더 모니터링 & 관리 시스템"
    )
    parser.add_argument(
        "--check-health",
        action="store_true",
        help="헬스 체크 한 번 실행",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="실시간 파일 변화 감시 (이벤트 기반)",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="감사 로그 보고서 출력",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="현재 상태 확인",
    )

    args = parser.parse_args()

    if args.check_health:
        run_health_check()
    elif args.watch:
        watch_folder_changes()
    elif args.report:
        print_audit_report()
    elif args.status:
        print_status()
    else:
        # 기본값: 헬스 체크 + 상태
        run_health_check()
        print_status()

    return 0


if __name__ == "__main__":
    sys.exit(main())
