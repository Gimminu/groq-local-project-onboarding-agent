from __future__ import annotations

import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from app.errors import AppError
from app.organizer_types import OrganizerConfig, OrganizerDecision, OrganizerPlan, PARA_ROOTS

PROJECT_MARKERS = {
    ".git",
    "package.json",
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "requirements.txt",
    "pyproject.toml",
    "poetry.lock",
    "Pipfile",
    "Pipfile.lock",
    "Cargo.toml",
    "go.mod",
    "build.gradle",
    "pom.xml",
    "Gemfile",
    "composer.json",
    "Makefile",
    "Dockerfile",
    "platformio.ini",
}
PROJECT_MARKER_DIRS = {
    ".git",
    ".idea",
    ".pio",
    ".vscode",
    "app",
    "include",
    "lib",
    "src",
    "test",
    "tests",
}
CODE_EXTENSIONS = {
    ".py",
    ".js",
    ".ts",
    ".tsx",
    ".jsx",
    ".java",
    ".kt",
    ".cpp",
    ".cc",
    ".c",
    ".h",
    ".hpp",
    ".cs",
    ".rb",
    ".go",
    ".rs",
    ".php",
    ".swift",
    ".scala",
    ".sh",
    ".zsh",
    ".bash",
    ".ipynb",
    ".sql",
    ".ino",
}
SENSITIVE_EXTENSIONS = {
    ".env",
    ".ini",
    ".cfg",
    ".conf",
    ".toml",
    ".yaml",
    ".yml",
    ".lock",
    ".pem",
    ".key",
    ".crt",
    ".mobileconfig",
}
TEXT_REFERENCE_EXTENSIONS = {
    ".py",
    ".js",
    ".ts",
    ".tsx",
    ".jsx",
    ".java",
    ".md",
    ".txt",
    ".json",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".cfg",
    ".env",
    ".sh",
}
DOCUMENT_EXTENSIONS = {".pdf", ".doc", ".docx", ".ppt", ".pptx", ".txt", ".rtf", ".md", ".hwp", ".hwpx"}
DATA_EXTENSIONS = {".csv", ".tsv", ".xls", ".xlsx", ".json", ".parquet"}
MEDIA_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
    ".mp4",
    ".mov",
    ".avi",
    ".m4v",
    ".mp3",
    ".wav",
    ".m4a",
}
ARCHIVE_EXTENSIONS = {".zip", ".rar", ".7z", ".tar", ".gz", ".bz2"}
TEMP_DOWNLOAD_EXTENSIONS = {".crdownload", ".download", ".part", ".partial"}
TEMP_DOWNLOAD_PREFIXES = ("unconfirmed ",)
SYSTEM_HIDDEN_NAMES = {
    ".DS_Store",
    ".localized",
    ".Spotlight-V100",
    ".TemporaryItems",
    ".Trashes",
}
GENERIC_FOLDER_NAMES = {
    "folder",
    "new folder",
    "untitled folder",
    "docs",
    "documents",
    "pdf",
    "pdfs",
    "data",
    "files",
    "downloads",
}
ACTIVE_KEYWORDS = {
    "project",
    "capstone",
    "assignment",
    "draft",
    "meeting",
    "실습",
    "과제",
    "프로젝트",
    "캡스톤",
    "발표",
}
OUTPUT_KEYWORDS = {"output", "result", "report", "export", "generated", "submission", "산출물", "결과"}
AREA_KEYWORDS = {
    "Finance": {"finance", "invoice", "tax", "receipt", "budget", "bank", "card", "payment", "유가", "영수증"},
    "Health": {"health", "medical", "hospital", "insurance", "diagnosis", "healthcare", "건강", "병원"},
    "Career": {"resume", "cv", "portfolio", "interview", "career", "job", "offer", "employment", "채용"},
    "Education": {"course", "lecture", "study", "exam", "certificate", "class", "수업", "강의", "기사", "자격증"},
    "Admin": {"contract", "agreement", "application", "form", "consent", "proposal", "서식", "신청서", "동의서"},
    "Personal": {"photo", "family", "travel", "personal", "wishlist", "memo", "개인"},
}
RESEARCH_KEYWORDS = {"paper", "research", "study", "survey", "논문", "연구"}
READING_KEYWORDS = {"book", "guide", "manual", "reference", "ebook", "교재", "매뉴얼"}
INSTALLER_KEYWORDS = {"installer", "setup", "install", "pkg", "dmg"}
SENSITIVE_NAME_KEYWORDS = {"password", "secret", "token", "wallet", "credential", "auth", "private", "api_key"}
PROJECTISH_FILE_NAMES = {
    ".gitignore",
    ".npmrc",
    ".python-version",
    ".tool-versions",
    "Dockerfile",
    "docker-compose.yml",
    "c_cpp_properties.json",
    "extensions.json",
    "launch.json",
    "tasks.json",
}
PROJECTISH_EXTENSIONS = CODE_EXTENSIONS | {".ino", ".elf", ".hex", ".map", ".dblite"}
ZERO_BYTE_DOWNLOAD_EXTENSIONS = (
    DOCUMENT_EXTENSIONS
    | DATA_EXTENSIONS
    | MEDIA_EXTENSIONS
    | ARCHIVE_EXTENSIONS
    | {".crx", ".dmg", ".iso", ".pkg"}
)


class FolderOrganizer:
    def __init__(self, config: OrganizerConfig, api_key: str | None = None):
        self.config = config
        self.api_key = api_key
        self.repo_root = Path(__file__).resolve().parents[1]
        self.source_root = config.source_root.expanduser().resolve()
        self.target_root = config.target_root.expanduser().resolve()
        self.protected_paths = self._default_protected_paths()
        self.provider_used = "heuristic"
        self.project_roots = self._discover_project_roots()

        if not self.source_root.exists():
            raise AppError(f"정리할 폴더를 찾을 수 없습니다: {self.source_root}")
        if not self.source_root.is_dir():
            raise AppError(f"정리 대상은 폴더여야 합니다: {self.source_root}")
        if self._is_strict_project_dir(self.source_root) and not self.config.allow_project_root:
            raise AppError(
                "source_root 자체가 프로젝트 폴더로 보입니다. 상위 폴더를 선택하거나 --allow-project-root를 사용하세요."
            )

    def snapshot(self) -> dict[str, tuple[bool, int, int]]:
        snapshot: dict[str, tuple[bool, int, int]] = {}
        for candidate in self._iter_top_level_candidates():
            try:
                stat = candidate.stat()
            except OSError:
                continue
            snapshot[candidate.name] = (
                candidate.is_dir(),
                stat.st_mtime_ns,
                getattr(stat, "st_size", 0),
            )
        return snapshot

    def has_actionable_items(self, plan: OrganizerPlan) -> bool:
        return any(item.status in {"planned", "manual_review"} for item in plan.decisions)

    def build_plan(self, command: str) -> OrganizerPlan:
        decisions: list[OrganizerDecision] = []
        fallback_reason = None
        if self.config.provider != "heuristic":
            fallback_reason = "현재 자동 정리는 안정성을 위해 heuristic 우선 모드로 실행됩니다."

        if command == "watch":
            for candidate in self._iter_top_level_candidates():
                decision = self._build_watch_decision(candidate)
                if decision is not None:
                    decisions.append(decision)
        else:
            seen: set[Path] = set()
            for candidate in self._classification_roots():
                resolved = candidate.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                decisions.extend(self._build_classification_decisions(candidate, group_hint=None))

        return OrganizerPlan(
            command=command,
            source_root=self.source_root,
            target_root=self.target_root,
            requested_provider=self.config.provider,
            provider_used=self.provider_used,
            decisions=decisions,
            llm_fallback_reason=fallback_reason,
        )

    def apply_plan(self, plan: OrganizerPlan) -> OrganizerPlan:
        for item in plan.decisions:
            if item.action != "move" or item.destination_path is None:
                if item.status == "planned":
                    item.status = "skipped"
                continue
            try:
                item.destination_path.parent.mkdir(parents=True, exist_ok=True)
                destination = self._unique_destination(item.destination_path)
                shutil.move(str(item.source_path), str(destination))
                item.destination_path = destination
                item.status = "applied"
                self._cleanup_empty_parents(item.source_path.parent)
            except Exception as exc:  # pragma: no cover - filesystem dependent
                item.status = "error"
                item.error = str(exc)
        return plan

    def _classification_roots(self) -> list[Path]:
        roots: list[Path] = []
        inbox_root = self.source_root / "00_Inbox"
        if inbox_root.exists():
            roots.extend(self._visible_children(inbox_root))
        roots.extend(self._iter_top_level_candidates())
        return roots

    def _iter_top_level_candidates(self) -> list[Path]:
        candidates: list[Path] = []
        for child in self._visible_children(self.source_root):
            if child.name in PARA_ROOTS:
                continue
            if self._is_protected_path(child):
                continue
            candidates.append(child)
        return sorted(candidates, key=lambda path: path.name.lower())

    def _visible_children(self, directory: Path) -> list[Path]:
        try:
            children = list(directory.iterdir())
        except OSError:
            return []
        return [child for child in children if child.name not in SYSTEM_HIDDEN_NAMES]

    def _build_watch_decision(self, source_path: Path) -> OrganizerDecision | None:
        if self._is_incomplete_download(source_path):
            return self._skip_decision(source_path, "다운로드가 끝나지 않은 임시 항목이라 완료 후 다시 확인합니다.")

        if self._is_too_new(source_path):
            return self._skip_decision(source_path, "최근에 바뀐 항목이라 다음 이벤트에서 다시 확인합니다.")

        if source_path.is_symlink():
            return self._manual_review(source_path, "심볼릭 링크는 자동 이동하지 않습니다.", "high", 0.0)

        if self._is_project_dir(source_path):
            if self.config.project_mode == "wrap":
                destination = self.target_root / "01_Projects" / source_path.name
                return self._move_decision(
                    source_path,
                    destination,
                    "프로젝트 폴더는 내부를 건드리지 않고 01_Projects로만 이동합니다.",
                    confidence=0.98,
                    para_root="01_Projects",
                    bucket_name="",
                    risk_level="low",
                )
            return self._manual_review(source_path, "프로젝트 폴더는 수동 검토가 필요합니다.", "high", 0.0)

        if source_path.is_dir():
            destination = self.target_root / "00_Inbox" / source_path.name
            return self._move_decision(
                source_path,
                destination,
                "새 폴더는 먼저 00_Inbox로 적재한 뒤 일괄 정리합니다.",
                confidence=0.88,
                para_root="00_Inbox",
                bucket_name="",
                risk_level="medium",
            )

        if self._is_blocked_file(source_path):
            return self._manual_review(
                source_path,
                "코드/설정/참조 가능성이 있는 파일은 자동 이동하지 않습니다.",
                "high",
                0.0,
            )

        destination = self.target_root / "00_Inbox" / source_path.name
        return self._move_decision(
            source_path,
            destination,
            "새 파일은 먼저 00_Inbox로 적재합니다.",
            confidence=0.92,
            para_root="00_Inbox",
            bucket_name="",
            risk_level="low",
        )

    def _build_classification_decisions(self, source_path: Path, group_hint: str | None) -> list[OrganizerDecision]:
        if not source_path.exists():
            return []
        if self._is_incomplete_download(source_path):
            return [self._skip_decision(source_path, "다운로드가 끝나지 않은 임시 항목이라 잠시 보류합니다.")]
        if self._is_too_new(source_path):
            return [self._skip_decision(source_path, "최근에 바뀐 항목이라 잠시 보류합니다.")]
        if source_path.is_symlink():
            return [self._manual_review(source_path, "심볼릭 링크는 자동 이동하지 않습니다.", "high", 0.0)]
        if self._is_protected_path(source_path):
            return [self._skip_decision(source_path, "보호된 경로라 자동 이동하지 않습니다.")]

        if source_path.is_dir():
            if self._is_project_dir(source_path):
                if self.config.project_mode == "wrap":
                    destination = self.target_root / "01_Projects" / source_path.name
                    return [
                        self._move_decision(
                            source_path,
                            destination,
                            "프로젝트 폴더는 내부를 건드리지 않고 01_Projects로만 이동합니다.",
                            confidence=0.98,
                            para_root="01_Projects",
                            bucket_name="",
                            risk_level="low",
                        )
                    ]
                return [self._manual_review(source_path, "프로젝트 폴더는 수동 검토가 필요합니다.", "high", 0.0)]

            children = self._visible_children(source_path)
            if not children:
                destination = self.target_root / "04_Archive" / "Empty_Folders" / source_path.name
                return [
                    self._move_decision(
                        source_path,
                        destination,
                        "비어 있는 폴더는 04_Archive/Empty_Folders로 이동합니다.",
                        confidence=0.93,
                        para_root="04_Archive",
                        bucket_name="Empty_Folders",
                        risk_level="low",
                    )
                ]

            next_group = group_hint or self._group_label(source_path.name)
            decisions: list[OrganizerDecision] = []
            for child in sorted(children, key=lambda path: path.name.lower()):
                decisions.extend(self._build_classification_decisions(child, next_group))
            return decisions

        return [self._classify_file(source_path, group_hint)]

    def _classify_file(self, source_path: Path, group_hint: str | None) -> OrganizerDecision:
        if self._is_blocked_file(source_path):
            return self._manual_review(
                source_path,
                "코드/설정/참조 가능성이 있는 파일은 자동 이동하지 않습니다.",
                "high",
                0.0,
            )

        if self._has_sensitive_name(source_path):
            return self._manual_review(
                source_path,
                "민감 정보로 보이는 이름이라 수동 검토가 필요합니다.",
                "high",
                0.25,
            )

        ext = source_path.suffix.lower()
        if ext in {".hwp", ".hwpx"}:
            return self._manual_review(
                source_path,
                "지원되지 않는 문서 형식이라 파일명만으로는 안전하게 분류하기 어렵습니다.",
                "medium",
                0.4,
            )
        area = self._classify_area(source_path, group_hint)
        if area is not None:
            destination = self.target_root / "02_Areas" / area / self._target_name(source_path, area, 0.92)
            return self._move_decision(
                source_path,
                destination,
                f"파일명 키워드를 기준으로 02_Areas/{area}로 분류했습니다.",
                confidence=0.92,
                para_root="02_Areas",
                bucket_name=area,
                risk_level="low",
                rename_applied=destination.name != source_path.name,
            )

        project_bucket = self._classify_project_bucket(source_path, group_hint)
        if project_bucket is not None:
            project_name, leaf = project_bucket
            bucket_name = f"{project_name}/{leaf}"
            destination = self.target_root / "01_Projects" / project_name / leaf / self._target_name(source_path, leaf, 0.87)
            return self._move_decision(
                source_path,
                destination,
                "최근 작업 파일 또는 프로젝트성 이름으로 판단해 01_Projects로 분류했습니다.",
                confidence=0.87,
                para_root="01_Projects",
                bucket_name=bucket_name,
                risk_level="medium",
                rename_applied=destination.name != source_path.name,
            )

        resource_bucket = self._classify_resource_bucket(source_path)
        if resource_bucket is not None:
            bucket_name = resource_bucket
            if group_hint:
                bucket_name = f"{resource_bucket}/{group_hint}"
            confidence = 0.9 if ext in DOCUMENT_EXTENSIONS | DATA_EXTENSIONS | MEDIA_EXTENSIONS else 0.78
            destination = self.target_root / "03_Resources" / resource_bucket
            if group_hint:
                destination = destination / group_hint
            destination = destination / self._target_name(source_path, resource_bucket, confidence)
            return self._move_decision(
                source_path,
                destination,
                f"파일 유형과 이름을 기준으로 03_Resources/{bucket_name}로 분류했습니다.",
                confidence=confidence,
                para_root="03_Resources",
                bucket_name=bucket_name,
                risk_level="low" if confidence >= 0.85 else "medium",
                rename_applied=destination.name != source_path.name,
            )

        destination = self.target_root / "04_Archive" / self._target_name(source_path, "Archive", 0.72)
        return self._move_decision(
            source_path,
            destination,
            "활성 프로젝트나 장기 영역으로 확신하기 어려워 04_Archive로 보관합니다.",
            confidence=0.72,
            para_root="04_Archive",
            bucket_name="",
            risk_level="medium",
            rename_applied=destination.name != source_path.name,
        )

    def _classify_area(self, source_path: Path, group_hint: str | None) -> str | None:
        haystack = f"{source_path.stem} {group_hint or ''}".lower()
        for area, keywords in AREA_KEYWORDS.items():
            if any(keyword.lower() in haystack for keyword in keywords):
                return area
        return None

    def _classify_project_bucket(self, source_path: Path, group_hint: str | None) -> tuple[str, str] | None:
        haystack = f"{source_path.stem} {group_hint or ''}".lower()
        is_recent = self._is_recent(source_path)
        has_project_signal = is_recent and any(keyword in haystack for keyword in ACTIVE_KEYWORDS)
        if not has_project_signal and group_hint and group_hint.lower() not in GENERIC_FOLDER_NAMES:
            has_project_signal = any(keyword in group_hint.lower() for keyword in ACTIVE_KEYWORDS)
        if not has_project_signal:
            return None
        project_name = self._project_name(group_hint, source_path)
        return project_name, self._project_leaf(source_path)

    def _classify_resource_bucket(self, source_path: Path) -> str | None:
        ext = source_path.suffix.lower()
        stem = source_path.stem.lower()
        if ext in DATA_EXTENSIONS:
            return "Datasets"
        if ext in MEDIA_EXTENSIONS:
            return "Media"
        if ext in DOCUMENT_EXTENSIONS:
            if any(token in stem for token in RESEARCH_KEYWORDS):
                return "Research"
            if any(token in stem for token in READING_KEYWORDS):
                return "Reading"
            return "Documents"
        if ext in ARCHIVE_EXTENSIONS:
            if any(token in stem for token in INSTALLER_KEYWORDS):
                return "Installers"
            return "Misc"
        return None

    def _project_leaf(self, source_path: Path) -> str:
        ext = source_path.suffix.lower()
        stem = source_path.stem.lower()
        if ext in DATA_EXTENSIONS:
            return "Data"
        if ext in MEDIA_EXTENSIONS:
            return "Assets"
        if any(token in stem for token in OUTPUT_KEYWORDS):
            return "Output"
        return "Documents"

    def _target_name(self, source_path: Path, category: str, confidence: float) -> str:
        if source_path.is_dir():
            return source_path.name
        if self.config.rename_mode == "keep":
            return source_path.name
        if confidence < 0.85 and source_path.suffix.lower() in {".hwp", ".hwpx"}:
            return source_path.name
        stem = self._normalize_label(source_path.stem)
        if self.config.rename_mode == "semantic" and confidence >= 0.85:
            date_part = datetime.fromtimestamp(source_path.stat().st_mtime).strftime("%Y-%m-%d")
            category_part = self._normalize_label(category)
            return f"{date_part}_{category_part}_{stem}_v1{source_path.suffix.lower()}"
        if self.config.rename_mode == "normalize":
            return f"{stem}{source_path.suffix.lower()}"
        return source_path.name

    def _project_name(self, group_hint: str | None, source_path: Path) -> str:
        if group_hint and group_hint.lower() not in GENERIC_FOLDER_NAMES:
            return self._safe_path_segment(group_hint)
        stem = source_path.stem
        for keyword in ACTIVE_KEYWORDS:
            if keyword in stem.lower():
                return self._safe_path_segment(stem)
        return "Active_Work"

    def _group_label(self, raw_name: str) -> str | None:
        label = self._safe_path_segment(raw_name)
        if not label or label.lower() in GENERIC_FOLDER_NAMES:
            return None
        return label

    def _is_incomplete_download(self, source_path: Path) -> bool:
        return self._is_temp_download_artifact(source_path) or self._is_zero_byte_download_placeholder(source_path)

    def _is_temp_download_artifact(self, source_path: Path) -> bool:
        lowered = source_path.name.lower()
        if any(lowered.startswith(prefix) for prefix in TEMP_DOWNLOAD_PREFIXES):
            return True
        return source_path.suffix.lower() in TEMP_DOWNLOAD_EXTENSIONS

    def _is_zero_byte_download_placeholder(self, source_path: Path) -> bool:
        if not source_path.is_file() or source_path.suffix.lower() not in ZERO_BYTE_DOWNLOAD_EXTENSIONS:
            return False
        try:
            if source_path.stat().st_size != 0:
                return False
            siblings = list(source_path.parent.iterdir())
        except OSError:
            return False
        return any(
            sibling != source_path and self._is_temp_download_artifact(sibling)
            for sibling in siblings
        )

    def _is_blocked_file(self, source_path: Path) -> bool:
        ext = source_path.suffix.lower()
        if source_path.name.startswith(".") or source_path.name in PROJECTISH_FILE_NAMES:
            return True
        if ext in CODE_EXTENSIONS or ext in SENSITIVE_EXTENSIONS or ext in PROJECTISH_EXTENSIONS:
            return True
        return self._has_reference_risk(source_path)

    def _has_reference_risk(self, source_path: Path) -> bool:
        if not source_path.is_file():
            return False
        probe_terms = {source_path.name, source_path.stem}
        examined = 0
        for project_root in self.project_roots:
            if source_path.resolve().is_relative_to(project_root):
                return True
            for candidate in project_root.rglob("*"):
                if examined >= 64:
                    return False
                if not candidate.is_file() or candidate.suffix.lower() not in TEXT_REFERENCE_EXTENSIONS:
                    continue
                try:
                    if candidate.stat().st_size > 256_000:
                        continue
                    content = candidate.read_text(encoding="utf-8", errors="ignore")
                except OSError:
                    continue
                examined += 1
                if any(term and term in content for term in probe_terms):
                    return True
        return False

    def _discover_project_roots(self) -> list[Path]:
        roots: list[Path] = []
        try:
            children = list(self.source_root.iterdir())
        except OSError:
            return roots
        for child in children:
            if child.name in PARA_ROOTS or child.name in SYSTEM_HIDDEN_NAMES:
                continue
            if child.is_dir() and self._is_project_dir(child):
                roots.append(child.resolve())
        return roots

    def _is_strict_project_dir(self, path: Path) -> bool:
        if not path.is_dir():
            return False
        try:
            child_names = {child.name for child in path.iterdir()}
        except OSError:
            return False
        return bool(child_names & PROJECT_MARKERS)

    def _is_project_dir(self, path: Path) -> bool:
        if self._is_strict_project_dir(path):
            return True
        inspected = 0
        projectish_hits = 0
        code_hits = 0
        for candidate in path.rglob("*"):
            name = candidate.name
            if name in SYSTEM_HIDDEN_NAMES:
                continue
            if candidate.is_dir():
                if name in PROJECT_MARKER_DIRS:
                    return True
                continue
            if not candidate.is_file():
                continue
            inspected += 1
            if name in PROJECT_MARKERS or name in PROJECTISH_FILE_NAMES:
                projectish_hits += 1
            elif candidate.suffix.lower() in PROJECTISH_EXTENSIONS:
                projectish_hits += 1
                code_hits += 1
            if projectish_hits >= 2 or code_hits >= 3:
                return True
            if inspected >= self.config.sample_limit:
                break
        return False

    def _is_protected_path(self, source_path: Path) -> bool:
        try:
            resolved = source_path.resolve()
        except OSError:
            return False
        return any(resolved == protected or protected in resolved.parents for protected in self.protected_paths)

    def _default_protected_paths(self) -> set[Path]:
        protected = {self.repo_root.resolve()}
        output_dir = self.config.output_dir.expanduser()
        if not output_dir.is_absolute():
            output_dir = (self.repo_root / output_dir).resolve()
        protected.add(output_dir.resolve())
        return protected

    def _is_recent(self, source_path: Path) -> bool:
        try:
            stat = source_path.stat()
        except OSError:
            return False
        window = timedelta(days=self.config.active_window_days)
        touched = max(stat.st_mtime, getattr(stat, "st_atime", stat.st_mtime))
        return datetime.now() - datetime.fromtimestamp(touched) <= window

    def _is_too_new(self, source_path: Path) -> bool:
        try:
            age_seconds = datetime.now().timestamp() - source_path.stat().st_mtime
        except OSError:
            return False
        return age_seconds < self.config.min_age_seconds

    def _has_sensitive_name(self, source_path: Path) -> bool:
        lowered = source_path.name.lower()
        return any(keyword in lowered for keyword in SENSITIVE_NAME_KEYWORDS)

    def _manual_review(
        self,
        source_path: Path,
        reason: str,
        risk_level: str,
        confidence: float,
    ) -> OrganizerDecision:
        return OrganizerDecision(
            source_path=source_path,
            destination_path=None,
            action="manual_review",
            status="manual_review",
            reason=reason,
            confidence=confidence,
            risk_level=risk_level,
            para_root="manual_review",
            bucket_name="",
            review_required=True,
            blocked_reason=reason,
            provider_used=self.provider_used,
        )

    def _skip_decision(self, source_path: Path, reason: str) -> OrganizerDecision:
        return OrganizerDecision(
            source_path=source_path,
            destination_path=None,
            action="skip",
            status="skipped",
            reason=reason,
            confidence=0.0,
            risk_level="medium",
            para_root="skip",
            bucket_name="",
            review_required=False,
            blocked_reason=reason,
            provider_used=self.provider_used,
        )

    def _move_decision(
        self,
        source_path: Path,
        destination_path: Path,
        reason: str,
        confidence: float,
        para_root: str,
        bucket_name: str,
        risk_level: str,
        rename_applied: bool = False,
    ) -> OrganizerDecision:
        destination_path = self._trim_depth(destination_path)
        if source_path.resolve() == destination_path.resolve():
            return self._skip_decision(source_path, "이미 정리 대상 위치에 있습니다.")
        return OrganizerDecision(
            source_path=source_path,
            destination_path=destination_path,
            action="move",
            status="planned",
            reason=reason,
            confidence=confidence,
            risk_level=risk_level,
            para_root=para_root,
            bucket_name=bucket_name,
            review_required=risk_level != "low" or confidence < 0.85,
            rename_applied=rename_applied,
            provider_used=self.provider_used,
        )

    def _trim_depth(self, destination_path: Path) -> Path:
        try:
            relative_parts = destination_path.relative_to(self.target_root).parts
        except ValueError:
            return destination_path
        if len(relative_parts) <= self.config.max_depth:
            return destination_path
        prefix = relative_parts[: self.config.max_depth - 1]
        return self.target_root.joinpath(*prefix, destination_path.name)

    def _unique_destination(self, destination_path: Path) -> Path:
        if not destination_path.exists():
            return destination_path
        if destination_path.is_dir():
            stem = destination_path.name
            suffix = ""
        else:
            stem = destination_path.stem
            suffix = destination_path.suffix
        counter = 2
        while True:
            candidate = destination_path.with_name(f"{stem}_{counter}{suffix}")
            if not candidate.exists():
                return candidate
            counter += 1

    def _cleanup_empty_parents(self, parent: Path) -> None:
        while parent != self.source_root and self.source_root in parent.parents:
            try:
                children = list(parent.iterdir())
            except OSError:
                return
            visible_children = [child for child in children if child.name not in SYSTEM_HIDDEN_NAMES]
            if visible_children:
                return
            for child in children:
                if child.name in SYSTEM_HIDDEN_NAMES and child.is_file():
                    try:
                        child.unlink()
                    except OSError:
                        pass
            try:
                parent.rmdir()
            except OSError:
                return
            parent = parent.parent

    def _normalize_label(self, value: str) -> str:
        cleaned = re.sub(r"\((copy|\d+)\)", "", value, flags=re.IGNORECASE)
        cleaned = re.sub(r"[\s/]+", "_", cleaned.strip())
        cleaned = re.sub(r"_+", "_", cleaned).strip("._")
        return cleaned or "untitled"

    def _safe_path_segment(self, value: str) -> str:
        cleaned = self._normalize_label(value)
        cleaned = cleaned.replace(":", "_")
        return cleaned[:80]
