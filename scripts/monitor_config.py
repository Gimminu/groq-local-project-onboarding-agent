"""모니터링 및 관리 시스템 설정."""

from pathlib import Path
from typing import Set

# 기본 경로
HOME = Path.home()
WORKSPACE_ROOT = HOME / "Documents" / "projects" / "workspace" / "mcp-workspace" / "code" / "groq-mcp-mac-agent"
AUDIT_LOG_DIR = WORKSPACE_ROOT / ".audit"
AUDIT_LOG_FILE = AUDIT_LOG_DIR / "audit_trail.log"
HEALTH_CHECK_FILE = AUDIT_LOG_DIR / "health_status.json"

# Watch 대상 (노트북/다운로드 폴더)
WATCH_ROOTS = [
    HOME / "Downloads",
    HOME / "Desktop",
    HOME / "Documents",
]

# 제외 패턴 (프로젝트 폴더, 캐시 등)
IGNORE_PATTERNS: Set[str] = {
    ".git",
    ".gitignore",
    "__pycache__",
    ".pytest_cache",
    "*.pyc",
    "node_modules",
    ".DS_Store",
    "*.swp",
    ".venv",
    "venv",
}

# 프로젝트 폴더 보호 (절대 이동/삭제 금지)
PROTECTED_PATHS = {
    HOME / "Documents" / "areas",
    HOME / "Documents" / "projects",
    HOME / "Documents" / "resources",
    HOME / "Documents" / "review",
}

# 파일 크기 임계값 (이 크기 이상의 파일만 감지)
FILE_SIZE_THRESHOLD = 5 * 1024 * 1024  # 5MB

# 모니터링 설정
MONITORING_CONFIG = {
    "periodic_check_interval_hours": 24,  # 하루 1회 정기 점검
    "weekly_check_day": "Monday",  # 주 1회는 월요일
    "event_debounce_seconds": 2,  # 이벤트 디바운싱 (2초)
    "max_audit_log_lines": 10000,  # 로그 최대 라인 수
    "dry_run_enabled": True,  # 기본적으로 드라이런 모드
}

# API 설정
API_CONFIG = {
    "groq": {
        "name": "Groq",
        "env_key": "GROQ_API_KEY",
        "free_tier_monthly_quota": 1000,  # 대략적인 추정값
    },
    "gemini": {
        "name": "Google Gemini",
        "env_key": "GEMINI_API_KEY",
        "free_tier_monthly_quota": 15000,  # 요청 수 기준
    },
}

# 상태 파일
STATUS_LEVELS = {
    "OK": "정상",
    "WARNING": "경고",
    "ERROR": "에러",
    "PROTECTED": "보호됨",
}
