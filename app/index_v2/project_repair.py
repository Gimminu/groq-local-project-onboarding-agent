from __future__ import annotations

import os
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sys

from app.index_v2.naming import normalize_segment
from app.index_v2.types import ActionPlan, IndexOrganizerConfig, PlannedAction

ASSET_DIR_NAMES = {"assets", "code", "data", "docs", "misc", "notes", "output", "slides"}
IGNORABLE_TOP_LEVEL = {".DS_Store"}
PROTECTED_EMPTY_DIR_NAMES = {
    ".git",
    ".gradle",
    ".pio",
    ".pytest_cache",
    ".venv",
    "node_modules",
    "venv",
    "__pycache__",
    "build",
}

_DATACLASS_SLOTS = {"slots": True} if sys.version_info >= (3, 10) else {}


@dataclass(frozen=True, **_DATACLASS_SLOTS)
class ProjectRepairRule:
    stream: str
    domain: str
    focus: str
    move_mode: str = "whole"
    reason: str = ""


def build_project_repair_plan(config: IndexOrganizerConfig) -> ActionPlan:
    source_root = config.spaces_root / "projects" / "coding"
    timestamp = datetime.now(timezone.utc).isoformat()
    actions: list[PlannedAction] = []
    if not source_root.exists():
        return ActionPlan(command="repair-projects", created_at=timestamp, scanned_roots=(source_root,), actions=[])

    for child in sorted(source_root.iterdir(), key=lambda path: path.name.lower()):
        if child.name in IGNORABLE_TOP_LEVEL:
            continue
        rule = PROJECT_REPAIR_RULES.get(_repair_key(child.name))
        if rule is None:
            actions.append(
                PlannedAction(
                    action_type="flag_for_review",
                    source_path=child,
                    destination_path=None,
                    reason="no deterministic repair-projects rule for current top-level folder",
                    confidence=0.0,
                    review_required=True,
                )
            )
            continue
        destination_root = config.spaces_root / rule.stream / rule.domain / _nfc(rule.focus)
        if rule.move_mode == "whole":
            actions.append(
                PlannedAction(
                    action_type="move",
                    source_path=child,
                    destination_path=destination_root,
                    reason=rule.reason or "move top-level project root into repaired canonical domain",
                    confidence=1.0,
                    metadata={"repair_rule": rule.focus},
                )
            )
            continue

        actions.extend(_collapsed_actions(child, destination_root, config, rule))

    return ActionPlan(command="repair-projects", created_at=timestamp, scanned_roots=(source_root,), actions=actions)


def cleanup_project_repair_source(config: IndexOrganizerConfig) -> list[Path]:
    source_root = config.spaces_root / "projects" / "coding"
    if not source_root.exists():
        return []

    cleaned: list[Path] = []
    for ds_store in sorted(source_root.rglob(".DS_Store"), reverse=True):
        try:
            ds_store.unlink()
            cleaned.append(ds_store)
        except OSError:
            continue

    for current_root, _, _ in os.walk(source_root, topdown=False):
        candidate = Path(current_root)
        if _should_skip_empty_cleanup(candidate):
            continue
        try:
            entries = [entry for entry in candidate.iterdir() if entry.name not in config.ignore_names]
        except OSError:
            continue
        if entries:
            continue
        try:
            candidate.rmdir()
            cleaned.append(candidate)
        except OSError:
            continue
    return cleaned


def _collapsed_actions(
    source_root: Path,
    destination_root: Path,
    config: IndexOrganizerConfig,
    rule: ProjectRepairRule,
) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    source_key = normalize_segment(source_root.name, config.naming.delimiter, config.naming.max_segment_length)
    for entry in sorted(source_root.iterdir(), key=lambda path: path.name.lower()):
        if entry.name in config.ignore_names:
            continue
        if entry.is_file():
            actions.append(
                PlannedAction(
                    action_type="move",
                    source_path=entry,
                    destination_path=destination_root / "misc" / entry.name,
                    reason=rule.reason or "move loose file into repaired misc bucket",
                    confidence=1.0,
                    metadata={"repair_rule": rule.focus},
                )
            )
            continue

        if entry.name not in ASSET_DIR_NAMES:
            actions.append(
                PlannedAction(
                    action_type="move",
                    source_path=entry,
                    destination_path=destination_root / "misc" / entry.name,
                    reason=rule.reason or "move non-asset child into repaired misc bucket",
                    confidence=1.0,
                    metadata={"repair_rule": rule.focus},
                )
            )
            continue

        effective_root = _unwrap_redundant_child(
            entry,
            focus_name=rule.focus,
            source_key=source_key,
            asset_name=entry.name,
            config=config,
        )
        meaningful_children = [child for child in sorted(effective_root.iterdir(), key=lambda path: path.name.lower()) if child.name not in config.ignore_names]
        if not meaningful_children:
            continue
        for child in meaningful_children:
            actions.append(
                PlannedAction(
                    action_type="move",
                    source_path=child,
                    destination_path=destination_root / entry.name / child.name,
                    reason=rule.reason or "collapse redundant wrapper and move asset children into repaired canonical domain",
                    confidence=1.0,
                    metadata={"repair_rule": rule.focus, "collapsed_from": str(effective_root)},
                )
            )
    return actions


def _unwrap_redundant_child(
    root: Path,
    *,
    focus_name: str,
    source_key: str,
    asset_name: str,
    config: IndexOrganizerConfig,
) -> Path:
    current = root
    focus_key = normalize_segment(focus_name, config.naming.delimiter, config.naming.max_segment_length)
    asset_key = normalize_segment(asset_name, config.naming.delimiter, config.naming.max_segment_length)
    while True:
        try:
            children = [child for child in current.iterdir() if child.name not in config.ignore_names]
        except OSError:
            return current
        files = [child for child in children if child.is_file()]
        dirs = [child for child in children if child.is_dir()]
        if files or len(dirs) != 1:
            return current
        child = dirs[0]
        child_key = normalize_segment(child.name, config.naming.delimiter, config.naming.max_segment_length)
        if child.name.startswith("~$") or child_key in {focus_key, source_key, asset_key}:
            current = child
            continue
        return current


def _should_skip_empty_cleanup(path: Path) -> bool:
    for part in path.parts:
        if part in PROTECTED_EMPTY_DIR_NAMES:
            return True
        if part.startswith("cmake-build-"):
            return True
    return False


def _repair_key(value: str) -> str:
    return normalize_segment(unicodedata.normalize("NFKC", value), "kebab-case", 80)


def _nfc(value: str) -> str:
    return unicodedata.normalize("NFC", value)


PROJECT_REPAIR_RULES: dict[str, ProjectRepairRule] = {
    _repair_key("arduino"): ProjectRepairRule("projects", "embedded", "arduino", reason="re-home Arduino project into embedded domain"),
    _repair_key("pio"): ProjectRepairRule("projects", "embedded", "pio", reason="re-home PlatformIO project into embedded domain"),
    _repair_key("robot"): ProjectRepairRule("projects", "embedded", "robot", reason="re-home robot project into embedded domain"),
    _repair_key("talkfile-ilrc-물류로봇"): ProjectRepairRule("projects", "embedded", "talkfile-ilrc-물류로봇", reason="re-home logistics robot project into embedded domain"),
    _repair_key("pdf-quiz-app"): ProjectRepairRule("projects", "apps", "pdf-quiz-app", reason="re-home active app project into apps domain"),
    _repair_key("lms-summarize"): ProjectRepairRule("projects", "apps", "lms-summarize", reason="re-home active app project into apps domain"),
    _repair_key("openai-realtime-transcribe"): ProjectRepairRule("projects", "apps", "openai-realtime-transcribe", reason="re-home active app project into apps domain"),
    _repair_key("ppt-auto-agent"): ProjectRepairRule("projects", "apps", "ppt-auto-agent", reason="re-home active app project into apps domain"),
    _repair_key("capstone-design"): ProjectRepairRule("projects", "apps", "capstone-design", reason="re-home active app project into apps domain"),
    _repair_key("personalbloguidesign-main"): ProjectRepairRule("projects", "apps", "personal-blog-ui-design", reason="normalize blog UI project name and re-home into apps domain"),
    _repair_key("mcp-workspace"): ProjectRepairRule("projects", "workspace", "mcp-workspace", reason="preserve multi-repo workspace under workspace domain"),
    _repair_key("c-project"): ProjectRepairRule("projects", "experiments", "c-project", reason="re-home local experiment into experiments domain"),
    _repair_key("pytorch"): ProjectRepairRule("projects", "experiments", "pytorch", reason="re-home local experiment into experiments domain"),
    _repair_key("groq-mcp-mac-agent-2"): ProjectRepairRule("projects", "legacy-review", "groq-mcp-mac-agent-copy", reason="keep duplicate project copy under legacy review"),
    _repair_key("mcp"): ProjectRepairRule("projects", "legacy-review", "mcp-legacy-organizer", reason="move mixed legacy organizer bundle under legacy review"),
    _repair_key("output"): ProjectRepairRule("projects", "legacy-review", "output-dumps", move_mode="collapsed", reason="collapse redundant output wrapper and move dumps under legacy review"),
    _repair_key("web-file"): ProjectRepairRule("projects", "legacy-review", "web-file-bundle", reason="keep mixed web bundle under legacy review"),
    _repair_key("vscode"): ProjectRepairRule("resources", "templates", "vscode-settings", reason="move editor settings bundle into templates"),
    _repair_key("26-1-coding"): ProjectRepairRule("areas", "education", "26-1-coding", move_mode="collapsed", reason="move coursework material into education area"),
    _repair_key("고급-프로그래밍-설계"): ProjectRepairRule("areas", "education", "고급-프로그래밍-설계", move_mode="collapsed", reason="move coursework material into education area"),
    _repair_key("선형대수-01분반-3"): ProjectRepairRule("areas", "education", "선형대수-01분반-3", move_mode="collapsed", reason="move coursework material into education area"),
    _repair_key("캡스톤"): ProjectRepairRule("areas", "education", "캡스톤", move_mode="collapsed", reason="move coursework material into education area"),
    _repair_key("고프설-발표"): ProjectRepairRule("areas", "education", "고급-프로그래밍-설계-발표", move_mode="collapsed", reason="move presentation material into education area"),
    _repair_key("실습파일-사전배포"): ProjectRepairRule("resources", "education", "실습파일-사전배포", move_mode="collapsed", reason="move practice material into education resources"),
    _repair_key("유가"): ProjectRepairRule("resources", "research", "유가-데이터", move_mode="collapsed", reason="move market data bundle into research resources"),
}
