from __future__ import annotations

from fnmatch import fnmatchcase
from dataclasses import dataclass, field
from pathlib import Path
import sys
from typing import Any

_DATACLASS_SLOTS = {"slots": True} if sys.version_info >= (3, 10) else {}


def _normalize_segment_runtime(value: str, delimiter: str, max_length: int) -> str:
    from app.index_v2.naming import normalize_segment

    return normalize_segment(value, delimiter, max_length)

ACTION_TYPES = ("move", "rename", "archive", "quarantine", "flag_for_review", "ignore")
CLASSIFICATION_SOURCES = ("rule", "heuristic", "llm", "path")
DEFAULT_SPACES = ("work", "personal", "learning", "shared", "ops")
DEFAULT_STREAMS = ("inbox", "projects", "areas", "resources", "archive", "review", "system")
DEFAULT_DOMAINS = (
    "unknown",
    "apps",
    "embedded",
    "workspace",
    "experiments",
    "legacy-review",
    "education",
    "admin",
    "coding",
    "finance",
    "research",
    "legal",
    "templates",
)
DEFAULT_BANNED_DOMAIN_NAMES = (
    "general",
    "misc",
    "other",
    "temp",
    "stuff",
    "category",
    "categories",
    "etc",
    "unknown",
    "unsorted",
    "일반",
    "기타",
    "임시",
    "카테고리",
    "미분류",
    "폴더",
    "새폴더",
)
DEFAULT_ASSET_TYPES = (
    "misc",
    "docs",
    "slides",
    "notes",
    "forms",
    "code",
    "data",
    "output",
    "assets",
    "exports",
    "installers",
    "archives",
)
DEFAULT_PROJECT_MARKERS = (
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
    ".venv",
    "venv",
    "node_modules",
)
DEFAULT_OUTPUT_TOKENS = ("output", "outputs", "dist", "build", "export", "exports", "generated", "result", "results")
DEFAULT_GENERIC_TOKENS = (
    "downloads",
    "desktop",
    "documents",
    "general",
    "temp",
    "other",
    "stuff",
    "category",
    "categories",
    "etc",
    "folder",
    "new-folder",
    "new_folder",
    "untitled",
    "files",
    "misc",
    "unknown",
    "unsorted",
    "다운로드",
    "바탕화면",
    "문서",
    "일반",
    "기타",
    "임시",
    "카테고리",
    "폴더",
    "새폴더",
    "미분류",
    "미정리",
)
DEFAULT_IGNORE_NAMES = (".DS_Store", ".localized", ".Spotlight-V100", ".TemporaryItems", ".Trashes")
DEFAULT_DOMAIN_ALIASES = {
    "apps": ("app", "apps", "service", "agent", "tool", "앱"),
    "embedded": ("embedded", "arduino", "platformio", "robot", "firmware", "iot", "임베디드"),
    "workspace": ("workspace", "hub", "suite", "작업공간"),
    "experiments": ("experiment", "experiments", "lab", "sandbox", "prototype", "pytorch", "실험"),
    "legacy-review": ("legacy", "bundle", "mixed", "review", "dump", "레거시"),
    "education": ("education", "edu", "course", "class", "lecture", "study", "수업", "강의", "과제", "실습", "교육"),
    "admin": ("admin", "application", "form", "contract", "agreement", "consent", "서식", "신청서", "동의서", "행정"),
    "coding": ("coding", "code", "dev", "program", "programming", "repo", "개발", "코드"),
    "finance": ("finance", "invoice", "tax", "receipt", "budget", "payment", "영수증", "금융", "세금"),
    "research": ("research", "paper", "survey", "study", "논문", "연구"),
    "legal": ("legal", "law", "policy", "terms", "규정", "법", "법률"),
    "templates": ("template", "templates", "sample", "samples", "양식", "템플릿", "샘플"),
}
DEFAULT_ASSET_ALIASES = {
    "docs": ("doc", "docs", "document", "documents", "pdf", "hwp", "hwpx", "문서"),
    "slides": ("slide", "slides", "ppt", "pptx", "deck", "발표", "프레젠테이션"),
    "notes": ("note", "notes", "memo", "readme", "md", "txt", "메모", "노트"),
    "forms": ("form", "forms", "application", "consent", "agreement", "서식", "신청서"),
    "code": ("code", "src", "source", "repo", "project", "코드", "소스"),
    "data": ("data", "dataset", "datasets", "csv", "json", "xlsx", "데이터"),
    "output": ("output", "outputs", "dist", "build", "generated", "result", "결과물"),
    "assets": ("asset", "assets", "media", "image", "video", "사진", "영상", "미디어"),
    "exports": ("export", "exports", "내보내기"),
    "installers": ("installer", "installers", "setup", "pkg", "dmg", "exe", "msi", "설치파일"),
    "archives": ("archive", "archives", "zip", "rar", "7z", "tar", "gz", "압축", "압축파일"),
}
DIRECT_PROJECT_DOMAINS = ("apps", "embedded", "experiments", "legacy-review")
DEFAULT_ADAPTIVE_RESERVED_TOP_LEVEL_NAMES = DEFAULT_STREAMS
DEFAULT_ADAPTIVE_BLOCKED_TOP_LEVEL_NAMES = (
    "download",
    "downloads",
    "desktop",
    "direct",
    "imported-archives",
    "null",
    "tmp",
    "temp",
    "review",
)
DEFAULT_ADAPTIVE_BLOCKED_TOP_LEVEL_FRAGMENTS = ("cache", "artifact", "artifacts")


@dataclass(**_DATACLASS_SLOTS)
class NamingPolicy:
    delimiter: str = "kebab-case"
    max_stem_length: int = 80
    max_segment_length: int = 40
    max_path_length: int = 240
    unknown_domain: str = "unknown"
    unsorted_focus: str = "unsorted"
    misc_asset_type: str = "misc"


@dataclass(**_DATACLASS_SLOTS)
class GroqPolicy:
    enabled: bool = True
    model: str = "llama-3.1-8b-instant"
    confidence_threshold: float = 0.75
    max_siblings: int = 20


@dataclass(**_DATACLASS_SLOTS)
class OllamaPolicy:
    base_url: str = "http://127.0.0.1:11434"
    model: str = "qwen2.5:3b-instruct"
    healthcheck_timeout_seconds: int = 2


@dataclass(**_DATACLASS_SLOTS)
class LLMPolicy:
    preferred_provider: str = "groq"
    fallback_to_other_cloud: bool = True
    fallback_to_ollama: bool = True
    enable_for_watch: bool = False
    enable_llm_first: bool = False
    max_items_per_watch_tick: int = 2
    retry_attempts: int = 2
    min_request_interval_seconds: dict[str, int] = field(
        default_factory=lambda: {"groq": 3, "gemini": 5, "ollama": 0}
    )
    backoff_seconds: tuple[int, ...] = (15, 45)
    provider_cooldown_seconds: int = 600
    ollama: OllamaPolicy = field(default_factory=OllamaPolicy)


@dataclass(**_DATACLASS_SLOTS)
class WatchPolicy:
    poll_interval_seconds: int = 10
    stable_observation_seconds: int = 30
    staging_age_seconds: int = 300
    backlog_rescan_seconds: int = 300


@dataclass(**_DATACLASS_SLOTS)
class ArchivePolicy:
    stale_days: int = 180
    manifest_hashes: bool = True


@dataclass(**_DATACLASS_SLOTS)
class DeletionPolicy:
    installer_grace_days: int = 30
    export_stale_days: int = 60
    quarantine_ttl_days: int = 30


@dataclass(**_DATACLASS_SLOTS)
class ServicePolicy:
    tick_interval_seconds: int = 60
    maintenance_interval_seconds: int = 3600
    archive_interval_seconds: int = 86400
    startup_apply: bool = True
    startup_archive: bool = False


@dataclass(**_DATACLASS_SLOTS)
class ReportingPolicy:
    write_noop_watch_reports: bool = False
    watch_retention_days: int = 14
    watch_max_report_pairs: int = 200


@dataclass(**_DATACLASS_SLOTS)
class CleanupPolicy:
    remove_metadata_artifacts: bool = True
    prune_empty_stream_roots: bool = True
    prune_empty_watch_dirs: bool = True
    noncode_name_repair_streams: tuple[str, ...] = ("areas", "resources", "archive", "review")


@dataclass(**_DATACLASS_SLOTS)
class RepairCodeNamesPolicy:
    auto_apply_scopes: tuple[str, ...] = ("projects/legacy-review",)


@dataclass(**_DATACLASS_SLOTS)
class RepairDefaults:
    fallback_focus: str = "unsorted"

    @property
    def general_focus(self) -> str:
        return self.fallback_focus


@dataclass(**_DATACLASS_SLOTS)
class DomainGatePolicy:
    min_items_in_window: int = 3
    window_days: int = 14
    min_confidence: float = 0.85
    min_independent_signals: int = 2
    max_new_domains_per_week: int = 2
    banned_domain_names: tuple[str, ...] = DEFAULT_BANNED_DOMAIN_NAMES


@dataclass(**_DATACLASS_SLOTS)
class DomainPolicy:
    pinned_domains: tuple[str, ...] = DEFAULT_DOMAINS
    allow_dynamic_domains: bool = True
    new_domain_gate: DomainGatePolicy = field(default_factory=DomainGatePolicy)


@dataclass(**_DATACLASS_SLOTS)
class AdaptivePlacementPolicy:
    enabled: bool = False
    hidden_review_relative: str = "adaptive-review"
    max_existing_folder_depth: int = 3
    min_existing_folder_score: int = 2
    reserved_top_level_names: tuple[str, ...] = DEFAULT_ADAPTIVE_RESERVED_TOP_LEVEL_NAMES
    blocked_top_level_names: tuple[str, ...] = DEFAULT_ADAPTIVE_BLOCKED_TOP_LEVEL_NAMES
    blocked_top_level_fragments: tuple[str, ...] = DEFAULT_ADAPTIVE_BLOCKED_TOP_LEVEL_FRAGMENTS
    review_layout: str = "asset-first"
    stabilize_existing_spaces_items: bool = True
    auto_drain_hidden_review: bool = True
    hidden_review_drain_interval_seconds: int = 900
    hidden_review_max_items_per_tick: int = 12


@dataclass(**_DATACLASS_SLOTS)
class MigrationRule:
    legacy_root_name: str
    stream: str
    domain: str | None = None
    asset_type: str | None = None
    focus_mode: str = "inherit"
    review_only: bool = False
    pattern: str | None = None


@dataclass(**_DATACLASS_SLOTS)
class PatternOverride:
    pattern: str
    stream: str | None = None
    domain: str | None = None
    focus: str | None = None
    asset_type: str | None = None


@dataclass(**_DATACLASS_SLOTS)
class IndexOrganizerConfig:
    config_path: Path
    spaces_root: Path
    history_root: Path
    state_dir: Path
    watch_roots: tuple[Path, ...] = ()
    migration_roots: tuple[Path, ...] = ()
    scan_roots: tuple[Path, ...] = ()
    root_spaces: dict[Path, str] = field(default_factory=dict)
    spaces: tuple[str, ...] = DEFAULT_SPACES
    streams: tuple[str, ...] = DEFAULT_STREAMS
    domains: tuple[str, ...] = DEFAULT_DOMAINS
    asset_types: tuple[str, ...] = DEFAULT_ASSET_TYPES
    domain_policy: DomainPolicy = field(default_factory=DomainPolicy)
    adaptive_placement: AdaptivePlacementPolicy = field(default_factory=AdaptivePlacementPolicy)
    runtime_approved_domains: tuple[str, ...] = ()
    default_space: str = "personal"
    include_space_level: bool = False
    move_uncertain_items: bool = False
    shallow_structure: bool = True
    preferred_depth: int = 3
    max_depth_limit: int = 5
    avoid_middle_folders: tuple[str, ...] = ("code", "misc", "temp", "category", "categories")
    protection_level: str = "strict"
    naming: NamingPolicy = field(default_factory=NamingPolicy)
    groq: GroqPolicy = field(default_factory=GroqPolicy)
    llm: LLMPolicy = field(default_factory=LLMPolicy)
    watch: WatchPolicy = field(default_factory=WatchPolicy)
    archive: ArchivePolicy = field(default_factory=ArchivePolicy)
    deletion: DeletionPolicy = field(default_factory=DeletionPolicy)
    service: ServicePolicy = field(default_factory=ServicePolicy)
    reporting: ReportingPolicy = field(default_factory=ReportingPolicy)
    cleanup: CleanupPolicy = field(default_factory=CleanupPolicy)
    review_mode: str = "flat"
    protected_project_internal_roots: tuple[str, ...] = (
        "projects/*/*/code",
        "projects/*/*/code/*",
        "projects/*/*/code/**",
    )
    protected_stream_roots: tuple[str, ...] = ("projects",)
    repair_code_names: RepairCodeNamesPolicy = field(default_factory=RepairCodeNamesPolicy)
    repair_defaults: RepairDefaults = field(default_factory=RepairDefaults)
    migration_rules: tuple[MigrationRule, ...] = ()
    pattern_overrides: tuple[PatternOverride, ...] = ()
    domain_aliases: dict[str, tuple[str, ...]] = field(default_factory=lambda: dict(DEFAULT_DOMAIN_ALIASES))
    asset_aliases: dict[str, tuple[str, ...]] = field(default_factory=lambda: dict(DEFAULT_ASSET_ALIASES))
    project_markers: tuple[str, ...] = DEFAULT_PROJECT_MARKERS
    output_tokens: tuple[str, ...] = DEFAULT_OUTPUT_TOKENS
    generic_tokens: tuple[str, ...] = DEFAULT_GENERIC_TOKENS
    ignore_names: tuple[str, ...] = DEFAULT_IGNORE_NAMES

    @property
    def database_path(self) -> Path:
        return self.state_dir / "index-organizer-v2.sqlite3"

    @property
    def reports_dir(self) -> Path:
        return self.state_dir / "reports"

    @property
    def quarantine_root(self) -> Path:
        return self.state_dir / "quarantine"

    @property
    def adaptive_review_root(self) -> Path:
        raw = Path(str(self.adaptive_placement.hidden_review_relative)).expanduser()
        if raw.is_absolute():
            return raw
        return self.state_dir / raw

    @property
    def service_logs_dir(self) -> Path:
        return self.state_dir / "service-logs"

    @property
    def service_state_path(self) -> Path:
        return self.state_dir / "service-state.json"

    def ensure_directories(self) -> None:
        for path in (
            self.spaces_root,
            self.history_root,
            self.state_dir,
            self.reports_dir,
            self.quarantine_root,
            self.adaptive_review_root,
            self.service_logs_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def all_scan_roots(self) -> tuple[Path, ...]:
        ordered: list[Path] = []
        for path in (self.spaces_root, *self.watch_roots, *self.migration_roots, *self.scan_roots):
            if path not in ordered:
                ordered.append(path)
        return tuple(ordered)

    def allowed_domains(self) -> tuple[str, ...]:
        ordered: list[str] = []
        for value in (*self.domain_policy.pinned_domains, *self.runtime_approved_domains):
            normalized = str(value).strip()
            if normalized and normalized not in ordered:
                ordered.append(normalized)
        return tuple(ordered)

    def banned_domain_names(self) -> tuple[str, ...]:
        ordered: list[str] = []
        for value in (
            *self.domain_policy.new_domain_gate.banned_domain_names,
            *self.generic_tokens,
            self.naming.unknown_domain,
            self.naming.unsorted_focus,
            self.naming.misc_asset_type,
        ):
            normalized = str(value).strip().lower()
            if normalized and normalized not in ordered:
                ordered.append(normalized)
        return tuple(ordered)

    def adaptive_mode_enabled(self) -> bool:
        return bool(self.adaptive_placement.enabled)

    def managed_root_names(self) -> tuple[str, ...]:
        ordered: list[str] = []
        for value in (
            *self.streams,
            *self.protected_stream_roots,
            *self.adaptive_placement.reserved_top_level_names,
        ):
            normalized = str(value).strip()
            if normalized and normalized not in ordered:
                ordered.append(normalized)
        return tuple(ordered)

    def destination_root_for(self, classification: "ClassificationResult") -> Path:
        destination_root = str(classification.metadata.get("destination_root") or "spaces")
        if destination_root == "review_staging":
            return self.adaptive_review_root
        return self.spaces_root

    def destination_relative_dir_for(self, classification: "ClassificationResult") -> Path:
        relative = classification.relative_dir(review_mode=self.review_mode)
        destination_root = str(classification.metadata.get("destination_root") or "spaces")
        if destination_root == "review_staging" and relative.parts and relative.parts[0] == "review":
            if len(relative.parts) == 1:
                return Path(".")
            if self.adaptive_placement.review_layout == "asset-first":
                parts = relative.parts[1:]
                if len(parts) == 1:
                    return Path(parts[0])
                focus_parts = parts[:-1]
                asset_type = parts[-1]
                return Path(asset_type, *focus_parts)
            return Path(*relative.parts[1:])
        return relative

    def canonical_depth(self, relative: Path | None = None) -> int:
        if self.shallow_structure:
            return max(2, min(self.max_depth_limit, self.preferred_depth))
        if self.review_mode in {"single-inbox", "flat"} and relative is not None:
            parts = relative.parts
            if parts and parts[0] in self.spaces and len(parts) >= 2 and parts[1] == "review":
                return 3
            if parts and parts[0] == "review":
                return 2
        if relative is not None and relative.parts and relative.parts[0] in self.spaces:
            return 5
        return 5 if self.include_space_level else 4

    def canonical_relative_dir(self, classification: "ClassificationResult") -> Path:
        if classification.target_path:
            return Path(classification.target_path)
        if classification.stream == "review":
            if self.review_mode in {"single-inbox", "flat"}:
                focus = _normalize_segment_runtime(
                    classification.focus,
                    self.naming.delimiter,
                    self.naming.max_segment_length,
                )
                review_focus = focus and focus not in {"", "review", "inbox", "misc", "unsorted", "general", "bundle"}
                parts = ["review"]
                if review_focus:
                    parts.extend([focus, classification.asset_type])
                else:
                    parts.append(classification.asset_type)
                if self.include_space_level:
                    parts.insert(0, classification.space)
                return Path(*parts)
            return classification.relative_dir(
                include_space=self.include_space_level,
                review_mode=self.review_mode,
            )

        focus = classification.focus
        if not focus or focus in {
            self.naming.unsorted_focus,
            self.naming.misc_asset_type,
            self.naming.unknown_domain,
        }:
            focus = self.repair_defaults.fallback_focus
        if focus == classification.domain:
            focus = self.repair_defaults.fallback_focus

        if self.shallow_structure and focus == self.repair_defaults.fallback_focus:
            parts = [classification.stream, classification.domain, classification.asset_type]
        else:
            parts = [classification.stream, classification.domain, focus, classification.asset_type]

        if self.include_space_level:
            parts.insert(0, classification.space)
        return Path(*parts)

    def parse_canonical_relative(self, relative: Path) -> tuple[str, str, str, str, str] | None:
        parts = relative.parts
        space = self.default_space
        if parts and parts[0] in self.spaces:
            space = parts[0]
            parts = parts[1:]
        if self.review_mode in {"single-inbox", "flat"}:
            if len(parts) >= 2 and parts[0] == "review" and parts[1] in self.asset_types:
                asset_type = parts[1]
                focus = "review"
                return space, "review", "review", focus, asset_type
            if len(parts) >= 3 and parts[0] == "review" and parts[2] in self.asset_types:
                return space, "review", "review", parts[1], parts[2]
        if len(parts) >= 3 and parts[0] == "projects" and parts[1] in DIRECT_PROJECT_DOMAINS:
            return space, "projects", parts[1], parts[2], "code"
        allowed_domains = set(self.allowed_domains())
        if len(parts) >= 3 and parts[0] in {"areas", "resources", "archive"} and parts[1] in allowed_domains and parts[2] in self.asset_types:
            return space, parts[0], parts[1], self.repair_defaults.fallback_focus, parts[2]
        if len(parts) >= 4 and parts[0] in self.streams and parts[1] in allowed_domains and parts[3] in self.asset_types:
            return space, parts[0], parts[1], parts[2], parts[3]
        return None

    def is_protected_project_internal(self, path: Path) -> bool:
        try:
            relative = path.relative_to(self.spaces_root)
        except ValueError:
            return False
        parts = relative.parts
        if len(parts) >= 2:
            top_level = self.spaces_root / parts[0]
            if parts[0] not in self.streams and _looks_like_project_root_runtime(top_level, self.project_markers):
                return True
        if len(parts) >= 4 and parts[0] == "projects" and parts[1] in DIRECT_PROJECT_DOMAINS:
            return True
        if len(parts) >= 5 and parts[0] == "projects" and parts[1] == "workspace" and parts[3] == "code":
            return True
        relative_value = relative.as_posix()
        for pattern in self.protected_project_internal_roots:
            if fnmatchcase(relative_value, pattern):
                return True
        return False

    def matches_repair_code_names_scope(self, path: Path) -> bool:
        try:
            relative = path.relative_to(self.spaces_root)
        except ValueError:
            return False
        relative_value = relative.as_posix()
        for raw_pattern in self.repair_code_names.auto_apply_scopes:
            pattern = raw_pattern.strip("/")
            if not pattern:
                continue
            if any(char in pattern for char in "*?[]"):
                if fnmatchcase(relative_value, pattern) or fnmatchcase(relative_value, f"{pattern}/**"):
                    return True
                continue
            if relative_value == pattern or relative_value.startswith(f"{pattern}/"):
                return True
        return False


@dataclass(**_DATACLASS_SLOTS)
class IndexedNode:
    path: Path
    kind: str
    size: int
    ext: str
    mtime: float
    ctime: float
    parent_path: Path | None
    sha256: str | None = None
    is_symlink: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "kind": self.kind,
            "size": self.size,
            "ext": self.ext,
            "mtime": self.mtime,
            "ctime": self.ctime,
            "parent_path": str(self.parent_path) if self.parent_path else None,
            "sha256": self.sha256,
            "is_symlink": self.is_symlink,
        }


@dataclass(**_DATACLASS_SLOTS)
class NormalizationResult:
    filename: str
    removed_date_token: str | None = None
    version_token: str | None = None
    assumptions: tuple[str, ...] = ()
    redundant_tokens_removed: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "filename": self.filename,
            "removed_date_token": self.removed_date_token,
            "version_token": self.version_token,
            "assumptions": list(self.assumptions),
            "redundant_tokens_removed": list(self.redundant_tokens_removed),
        }


@dataclass(**_DATACLASS_SLOTS)
class ClassificationResult:
    placement_mode: str = "direct"
    target_path: str | None = None
    create_folders: tuple[str, ...] = ()
    confidence: float = 0.0
    rationale: str = ""
    source: str = ""
    alternatives: tuple[dict[str, Any], ...] = ()
    review_required: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)
    space: str | None = None
    stream: str | None = None
    domain: str | None = None
    focus: str | None = None
    asset_type: str | None = None

    def __post_init__(self) -> None:
        derived = _derive_classification_dimensions(
            target_path=self.target_path,
            metadata=self.metadata,
            placement_mode=self.placement_mode,
            review_required=self.review_required,
        )
        if self.space is None:
            self.space = derived["space"]
        if self.stream is None:
            self.stream = derived["stream"]
        if self.domain is None:
            self.domain = derived["domain"]
        if self.focus is None:
            self.focus = derived["focus"]
        if self.asset_type is None:
            self.asset_type = derived["asset_type"]

    def relative_dir(self, *, include_space: bool = False, review_mode: str = "standard") -> Path:
        if self.placement_mode == "review_only" or self.review_required:
            if self.target_path:
                return Path(self.target_path)
            return Path("review")
        if self.target_path:
            return Path(self.target_path)
        return Path(".")

    def to_dict(self) -> dict[str, Any]:
        return {
            "placement_mode": self.placement_mode,
            "target_path": self.target_path,
            "create_folders": list(self.create_folders),
            "confidence": round(self.confidence, 4),
            "rationale": self.rationale,
            "source": self.source,
            "alternatives": list(self.alternatives),
            "review_required": self.review_required,
            "metadata": self.metadata,
            "space": self.space,
            "stream": self.stream,
            "domain": self.domain,
            "focus": self.focus,
            "asset_type": self.asset_type,
        }


def _derive_classification_dimensions(
    *,
    target_path: str | None,
    metadata: dict[str, Any],
    placement_mode: str,
    review_required: bool,
) -> dict[str, str]:
    if metadata:
        derived = {
            "space": str(metadata.get("derived_space") or metadata.get("space") or "personal"),
            "stream": str(metadata.get("derived_stream") or metadata.get("stream") or "unknown"),
            "domain": str(metadata.get("derived_domain") or metadata.get("domain") or "unknown"),
            "focus": str(metadata.get("derived_focus") or metadata.get("focus") or "unsorted"),
            "asset_type": str(metadata.get("derived_asset_type") or metadata.get("asset_type") or "misc"),
        }
        if derived["stream"] != "unknown":
            return derived

    parts = [part for part in str(target_path or "").replace("\\", "/").split("/") if part]
    derived = {
        "space": "personal",
        "stream": "unknown",
        "domain": "unknown",
        "focus": "unsorted",
        "asset_type": "misc",
    }

    if (placement_mode == "review_only" or review_required) and not parts:
        derived["stream"] = "review"
        derived["domain"] = "review"
        return derived

    if not parts:
        return derived

    head = parts[0]
    derived["stream"] = head
    if head == "review":
        derived["domain"] = "review"
        if len(parts) >= 2 and parts[1] in DEFAULT_ASSET_TYPES:
            derived["asset_type"] = parts[1]
            derived["focus"] = "review"
        elif len(parts) >= 3 and parts[2] in DEFAULT_ASSET_TYPES:
            derived["focus"] = parts[1]
            derived["asset_type"] = parts[2]
        return derived

    if head == "projects":
        if len(parts) >= 2:
            derived["domain"] = parts[1]
        if len(parts) >= 3:
            derived["focus"] = parts[2]
        derived["asset_type"] = "code"
        return derived

    if head in {"areas", "resources", "archive"}:
        if len(parts) >= 2:
            derived["domain"] = parts[1]
        if len(parts) >= 3 and parts[2] in DEFAULT_ASSET_TYPES:
            derived["focus"] = "unsorted"
            derived["asset_type"] = parts[2]
            return derived
        if len(parts) >= 4 and parts[3] in DEFAULT_ASSET_TYPES:
            derived["focus"] = parts[2]
            derived["asset_type"] = parts[3]
            return derived
        if len(parts) >= 3:
            derived["focus"] = parts[2]
        if parts[-1] in DEFAULT_ASSET_TYPES:
            derived["asset_type"] = parts[-1]
        return derived

    if head == "system":
        derived["domain"] = "system"
        derived["focus"] = "system"
        return derived

    if len(parts) >= 2:
        derived["domain"] = parts[1]
    if len(parts) >= 3:
        derived["focus"] = parts[2]
    if parts[-1] in DEFAULT_ASSET_TYPES:
        derived["asset_type"] = parts[-1]
    return derived


def _looks_like_project_root_runtime(path: Path, project_markers: tuple[str, ...]) -> bool:
    if not path.exists() or not path.is_dir():
        return False
    try:
        child_names = {child.name for child in path.iterdir()}
    except OSError:
        return False
    if child_names & set(project_markers):
        return True
    return bool(child_names & {"src", "app", ".pio"})


@dataclass(**_DATACLASS_SLOTS)
class PlannedAction:
    action_type: str
    source_path: Path | None
    destination_path: Path | None
    reason: str
    confidence: float
    review_required: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)
    status: str = "planned"
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "action_type": self.action_type,
            "source_path": str(self.source_path) if self.source_path else None,
            "destination_path": str(self.destination_path) if self.destination_path else None,
            "reason": self.reason,
            "confidence": round(self.confidence, 4),
            "review_required": self.review_required,
            "metadata": self.metadata,
            "status": self.status,
            "error": self.error,
        }


@dataclass(**_DATACLASS_SLOTS)
class ActionPlan:
    command: str
    created_at: str
    scanned_roots: tuple[Path, ...]
    actions: list[PlannedAction]
    metadata: dict[str, Any] = field(default_factory=dict)

    def summary(self) -> dict[str, Any]:
        summary: dict[str, int] = {"total": len(self.actions)}
        for action in self.actions:
            key = action.action_type
            summary[key] = summary.get(key, 0) + 1
            status_key = f"status_{action.status}"
            summary[status_key] = summary.get(status_key, 0) + 1
        return summary

    def to_dict(self) -> dict[str, Any]:
        return {
            "command": self.command,
            "created_at": self.created_at,
            "scanned_roots": [str(path) for path in self.scanned_roots],
            "summary": self.summary(),
            "actions": [action.to_dict() for action in self.actions],
            "metadata": self.metadata,
        }
