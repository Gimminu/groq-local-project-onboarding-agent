from __future__ import annotations

import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from app.index_v2.classifier import (
    ARCHIVE_EXTENSIONS,
    ASSET_EXTENSIONS,
    CODE_EXTENSIONS,
    DATA_EXTENSIONS,
    DOC_EXTENSIONS,
    FORM_KEYWORDS,
    INSTALLER_EXTENSIONS,
    NOTE_EXTENSIONS,
    SLIDE_EXTENSIONS,
)
from app.index_v2.content_hints import extract_content_hint, infer_bundle_focus_name
from app.index_v2.focus_rules import infer_focus_from_path
from app.index_v2.naming import normalize_filename, normalize_segment
from app.index_v2.project_repair import PROTECTED_EMPTY_DIR_NAMES, _unwrap_redundant_child
from app.index_v2.types import (
    DIRECT_PROJECT_DOMAINS,
    ActionPlan,
    ClassificationResult,
    IndexOrganizerConfig,
    PlannedAction,
)

IGNORABLE_NAMES = {".DS_Store"}
JETBRAINS_PROJECT_FILES = {"editor.xml", "modules.xml", "vcs.xml", "workspace.xml"}
IDEA_CONFIG_DIR = "idea-config"
WRAPPER_DIR_KEYS = {
    "admin",
    "archive",
    "area",
    "areas",
    "coding",
    "education",
    "import",
    "imports",
    "inbox",
    "misc",
    "reading",
    "resource",
    "resources",
    "review",
}
SUSPICIOUS_CODE_NAME_PATTERN = re.compile(r"(?i)(?:\[(?:file|code|image|note|reference|spreadsheet)\]|\buncertain\b|\(uncertain\)|^untitled(?:[\W_]|$))")
CODE_NAME_PREFIX_PATTERN = re.compile(r"^\s*\d+[\s._-]+")
CODE_AUDIT_SKIP_NAMES = {
    ".git",
    ".gradle",
    ".idea",
    ".kotlin",
    ".pio",
    ".pytest_cache",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    "node_modules",
    "venv",
}
METADATA_ARTIFACT_PATTERN = re.compile(r"(?i)(?:^\.ds_store$|^thumbs\.db$|ds store)")
NONCODE_NAME_REPAIR_PATTERN = re.compile(r"(?i)(?:^\~\$|^untitled(?:[\W_]|$)|^\d{4}-\d{2}-\d{2}[_\-\s]|^\d{8}[_\-\s]|^\s*documents[_\-]|uncertain|\[(?:file|image|code|note|reference|spreadsheet)\])")
DOWNLOAD_WRAPPER_HINTS = {"downloads", "downloads-2", "desktop", "user-data", "user_data"}
WORKING_RULE_HINTS = ("working-rules", "desktop-working-rules", "publish-workflow", "repo-publish", "publish-repo", "workflow")
ORGANIZER_TEST_HINTS = ("usability-depth-test", "usability_depth_test", "usability-test", "depth-test")
ROOT_REVIEW_WRAPPER_HINTS = {"download", "downloads", "다운로드", "tmp", "temp", "null", "direct"}
SUFFIXED_ASSET_DIR_PATTERN = re.compile(
    r"^(?P<asset>misc|docs|slides|notes|forms|code|data|output|assets|exports|installers|archives)-\d+$"
)
PROJECT_WRAPPER_DOMAINS = (*DIRECT_PROJECT_DOMAINS, "workspace")


def build_tree_repair_plan(config: IndexOrganizerConfig) -> ActionPlan:
    timestamp = datetime.now(timezone.utc).isoformat()
    actions: list[PlannedAction] = []
    scanned_roots = (config.spaces_root,)
    if not config.adaptive_mode_enabled():
        scanned_roots = (
            config.spaces_root,
            config.spaces_root / "review",
            config.spaces_root / "areas",
            config.spaces_root / "resources",
            config.spaces_root / "archive",
            config.spaces_root / "projects",
            config.spaces_root / "system",
        )
    actions.extend(_review_repair_actions(config))
    if not config.adaptive_mode_enabled():
        actions.extend(_top_level_loose_root_actions(config))
    actions.extend(_legacy_wrapper_actions(config))
    actions.extend(_shallow_nonproject_actions(config))
    actions.extend(_merge_suffixed_asset_dir_actions(config))
    actions.extend(_merge_project_suffixed_asset_dir_actions(config))
    if not config.adaptive_mode_enabled():
        actions.extend(_top_level_project_bucket_actions(config))
    actions.extend(_flatten_nested_project_tree_actions(config))
    actions.extend(_flatten_project_wrapper_actions(config))
    actions.extend(_noncode_stream_repair_actions(config))
    actions.extend(_residual_wrapper_actions(config))
    actions.extend(_code_damage_actions(config, apply_requested=False))
    return ActionPlan(command="repair-tree", created_at=timestamp, scanned_roots=scanned_roots, actions=actions)


def build_code_name_audit_plan(config: IndexOrganizerConfig, *, apply_requested: bool = False) -> ActionPlan:
    timestamp = datetime.now(timezone.utc).isoformat()
    scanned_roots = tuple(_code_asset_roots(config))
    return ActionPlan(
        command="repair-code-names",
        created_at=timestamp,
        scanned_roots=scanned_roots,
        actions=_code_damage_actions(config, apply_requested=apply_requested),
    )


def cleanup_tree_repair_targets(config: IndexOrganizerConfig) -> list[Path]:
    roots = (
        config.spaces_root / "review",
        config.spaces_root / "areas",
        config.spaces_root / "resources",
        config.spaces_root / "archive",
        config.spaces_root / "system",
        config.spaces_root / "projects" / "legacy-review" / "output-dumps" / "output",
    )
    cleaned: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for artifact in sorted(root.rglob("*"), reverse=True):
            if not artifact.is_file() or not _is_metadata_artifact_file(artifact) or config.is_protected_project_internal(artifact):
                continue
            try:
                artifact.unlink()
                cleaned.append(artifact)
            except OSError:
                continue
        for ds_store in sorted(root.rglob(".DS_Store"), reverse=True):
            try:
                ds_store.unlink()
                cleaned.append(ds_store)
            except OSError:
                continue

    for root in roots:
        if not root.exists():
            continue
        for current_root, _, _ in os.walk(root, topdown=False):
            candidate = Path(current_root)
            if _should_skip_empty_cleanup(candidate) or config.is_protected_project_internal(candidate):
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
    cleaned.extend(_cleanup_empty_project_wrappers(config))
    return cleaned


def _review_repair_actions(config: IndexOrganizerConfig) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    review = config.spaces_root / "review"
    unit_roots = (
        review / "imports" / "archive" / "misc",
        review / "imports" / "resources" / "misc",
        review / "admin",
        review / "coding",
    )
    for root in unit_roots:
        if not root.exists():
            continue
        for child in sorted(root.iterdir(), key=lambda path: path.name.lower()):
            if child.name in config.ignore_names:
                continue
            actions.extend(_plan_review_unit(child, config))
    actions.extend(_review_documents_project_actions(config))
    actions.extend(_review_cleanup_actions(config))
    return actions


def _legacy_wrapper_actions(config: IndexOrganizerConfig) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    actions.extend(_general_focus_actions(config, domain="admin", legacy_focus="02-areas", wrapper_name="Admin"))
    actions.extend(_general_focus_actions(config, domain="education", legacy_focus="02-areas", wrapper_name="Education"))
    actions.extend(_collapse_asset_wrapper_actions(config))
    actions.extend(_legacy_output_dump_actions(config))
    return actions


def _code_damage_actions(config: IndexOrganizerConfig, *, apply_requested: bool) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    planned_destinations: set[Path] = set()
    for code_root in _code_asset_roots(config):
        repo_root = _single_repo_root(code_root)
        git_managed = repo_root is not None and (repo_root / ".git").exists()
        tracked_paths = _git_tracked_paths(repo_root) if git_managed and repo_root is not None else set()
        for candidate in _iter_suspicious_code_paths(code_root):
            tracked = _is_git_tracked(candidate, repo_root, tracked_paths)
            if tracked:
                continue
            if apply_requested and config.matches_repair_code_names_scope(candidate):
                normalized_name = _normalize_code_filename(candidate, config)
                destination = candidate.with_name(normalized_name)
                unsafe_auto_rename = (
                    destination.stem == "untitled"
                    or destination in planned_destinations
                    or (destination.exists() and destination != candidate)
                )
                if destination != candidate and not unsafe_auto_rename:
                    planned_destinations.add(destination)
                    actions.append(
                        PlannedAction(
                            action_type="rename",
                            source_path=candidate,
                            destination_path=destination,
                            reason="normalize suspicious legacy code filename without semantic guessing",
                            confidence=0.92,
                            review_required=False,
                            metadata={
                                "code_damage": True,
                                "repo_root": str(repo_root) if repo_root is not None else None,
                                "git_managed": git_managed,
                                "git_tracked": tracked,
                                "legacy_auto_repair": True,
                            },
                        )
                    )
                    continue
            actions.append(
                PlannedAction(
                    action_type="flag_for_review",
                    source_path=candidate,
                    destination_path=None,
                    reason="suspicious renamed filename inside protected project code tree"
                    if not (apply_requested and config.matches_repair_code_names_scope(candidate))
                    else "legacy code filename requires review because safe automatic normalization was not possible",
                    confidence=0.95,
                    review_required=True,
                    metadata={
                        "code_damage": True,
                        "repo_root": str(repo_root) if repo_root is not None else None,
                        "git_managed": git_managed,
                        "git_tracked": tracked,
                    },
                )
            )
    return actions


def _plan_review_unit(source: Path, config: IndexOrganizerConfig) -> list[PlannedAction]:
    if source.is_file():
        return [_review_file_action(source, config)]

    files = _list_files(source, config.ignore_names)
    if not files:
        return []
    asset_types = {_detect_asset_type(path) for path in files}
    if len(asset_types) == 1 and "misc" not in asset_types:
        asset_type = next(iter(asset_types))
        return [_review_file_action(path, config, asset_type=asset_type) for path in files]

    bundle_name = _bundle_focus_name(source, files=files, config=config)
    actions: list[PlannedAction] = []
    for path in files:
        relative = _trim_wrapper_relative(path.relative_to(source), source.name, config)
        asset_type = _detect_asset_type(path)
        classification = _review_classification(config, asset_type=asset_type, focus=bundle_name)
        normalized = normalize_filename(path, classification, config)
        destination = config.spaces_root / "review" / "misc" / bundle_name
        if relative.parent != Path("."):
            destination = destination / relative.parent
        destination = destination / normalized.filename
        actions.append(
            PlannedAction(
                action_type="move",
                source_path=path,
                destination_path=destination,
                reason="move mixed review bundle into flat review bundle area",
                confidence=1.0,
                metadata={"repair_scope": "review", "normalization": normalized.to_dict()},
            )
        )
    return actions


def _review_file_action(source: Path, config: IndexOrganizerConfig, asset_type: str | None = None) -> PlannedAction:
    resolved_asset_type = asset_type or _detect_asset_type(source)
    focus = infer_focus_from_path(
        source,
        stream="review",
        domain="review",
        asset_type=resolved_asset_type,
        config=config,
        hint_text=extract_content_hint(source),
    ) or "review"
    classification = _review_classification(config, resolved_asset_type, focus=focus)
    normalized = normalize_filename(source, classification, config)
    destination = config.spaces_root / config.canonical_relative_dir(classification) / normalized.filename
    return PlannedAction(
        action_type="move",
        source_path=source,
        destination_path=destination,
        reason="flatten legacy review/import wrapper into flat review area",
        confidence=1.0,
        metadata={"repair_scope": "review", "normalization": normalized.to_dict()},
    )


def _review_documents_project_actions(config: IndexOrganizerConfig) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    restore_names = {
        "idea-config.xml": "workspace.xml",
        "idea-config.iml": "Documents.iml",
    }
    bundle_roots = (
        config.spaces_root / "review" / "documents-project",
        config.spaces_root / "review" / "inbox" / "misc" / "documents-project",
        config.spaces_root / "review" / "misc" / "documents-project",
    )
    for bundle_root in bundle_roots:
        if not bundle_root.exists():
            continue
        for child in sorted(bundle_root.rglob("*"), key=lambda path: str(path).lower()):
            if child.name in config.ignore_names or not child.is_file():
                continue
            if child.name in restore_names:
                actions.append(
                    PlannedAction(
                        action_type="move",
                        source_path=child,
                        destination_path=child.with_name(restore_names[child.name]),
                        reason="restore standard JetBrains metadata filename inside documents-project bundle",
                        confidence=1.0,
                        metadata={"repair_scope": "review-documents-project"},
                    )
                )
                continue
            if child.suffix.lower() != ".pages":
                continue
            classification = _review_classification(config, "docs")
            normalized = normalize_filename(child, classification, config)
            actions.append(
                PlannedAction(
                    action_type="move",
                    source_path=child,
                    destination_path=config.spaces_root / "review" / "docs" / normalized.filename,
                    reason="promote Pages documents out of mixed review bundle",
                    confidence=1.0,
                    metadata={"repair_scope": "review-documents-project", "normalization": normalized.to_dict()},
                )
            )
    return actions


def _noncode_stream_repair_actions(config: IndexOrganizerConfig) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    for stream in config.cleanup.noncode_name_repair_streams:
        if stream == "review":
            continue
        stream_root = config.spaces_root / stream
        if not stream_root.exists():
            continue
        actions.extend(_stream_file_repair_actions(stream_root, stream, config))
    return actions


def _residual_wrapper_actions(config: IndexOrganizerConfig) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    roots = (
        config.spaces_root / "resources" / "unknown",
        config.spaces_root / "resources" / "unsorted",
        config.spaces_root / "resources" / "user_data",
        config.spaces_root / "system",
    )
    for root in roots:
        if not root.exists():
            continue
        for source in sorted(root.rglob("*"), key=lambda path: str(path).lower()):
            if not source.is_file():
                continue
            if source.name in config.ignore_names or _is_metadata_artifact_file(source):
                continue
            action = _residual_wrapper_action(source, config)
            if action is not None:
                actions.append(action)
    return actions


def _top_level_loose_root_actions(config: IndexOrganizerConfig) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    managed_root_names = set(config.streams)
    managed_root_names.update(config.protected_stream_roots)
    if not config.spaces_root.exists():
        return actions

    for child in sorted(config.spaces_root.iterdir(), key=lambda path: path.name.lower()):
        if child.name in config.ignore_names:
            continue
        if child.name.startswith("."):
            continue
        if child.name in managed_root_names:
            continue
        if child.is_symlink():
            continue
        if child.is_dir():
            destination = _top_level_loose_dir_destination(child, config)
            if destination is None:
                actions.append(
                    PlannedAction(
                        action_type="flag_for_review",
                        source_path=child,
                        destination_path=None,
                        reason="top-level loose directory is outside the managed root set and could not be repaired deterministically",
                        confidence=0.0,
                        review_required=True,
                        metadata={"repair_scope": "top-level-loose-root"},
                    )
                )
                continue
            actions.append(
                PlannedAction(
                    action_type="move",
                    source_path=child,
                    destination_path=destination,
                    reason="move loose top-level directory into the canonical managed tree",
                    confidence=0.98,
                    metadata={"repair_scope": "top-level-loose-root"},
                )
            )
            continue
        if child.is_file():
            asset_type = _detect_asset_type(child)
            actions.append(
                PlannedAction(
                    action_type="move",
                    source_path=child,
                    destination_path=_top_level_loose_file_destination(child, asset_type, config),
                    reason="move loose top-level file into the review staging area",
                    confidence=0.95,
                    metadata={"repair_scope": "top-level-loose-root", "asset_type": asset_type},
                )
            )
    return actions


def _top_level_project_bucket_actions(config: IndexOrganizerConfig) -> list[PlannedAction]:
    projects_root = config.spaces_root / "projects"
    if not projects_root.exists():
        return []

    allowed = {"apps", "embedded", "workspace", "experiments", "legacy-review"}
    actions: list[PlannedAction] = []
    for child in sorted(projects_root.iterdir(), key=lambda path: path.name.lower()):
        if child.name in config.ignore_names or not child.is_dir():
            continue
        if child.name in allowed:
            continue
        bucket = _project_bucket_for(child, config)
        if bucket is None:
            actions.append(
                PlannedAction(
                    action_type="flag_for_review",
                    source_path=child,
                    destination_path=None,
                    reason="top-level project root is outside typed project buckets and could not be repaired deterministically",
                    confidence=0.0,
                    review_required=True,
                    metadata={"repair_scope": "top-level-project-bucket"},
                )
            )
            continue
        actions.append(
            PlannedAction(
                action_type="move",
                source_path=child,
                destination_path=projects_root / bucket / child.name,
                reason="move top-level project root into typed shallow project bucket",
                confidence=0.95,
                metadata={"repair_scope": "top-level-project-bucket", "target_bucket": bucket},
            )
        )
    return actions


def _top_level_loose_dir_destination(path: Path, config: IndexOrganizerConfig) -> Path | None:
    normalized = normalize_segment(path.name, config.naming.delimiter, config.naming.max_segment_length)
    lowered_name = path.name.lower()
    if normalized in {"libraries", "library"}:
        return config.spaces_root / "resources" / "embedded" / "libraries"
    if _looks_like_top_level_review_wrapper(path.name, normalized=normalized):
        return config.spaces_root / "review" / "misc" / _bundle_focus_name(path, config=config)
    if _looks_like_timestamp_dump(path.name):
        return config.spaces_root / "review" / "misc" / _bundle_focus_name(path, config=config)
    if _looks_like_cache_or_artifact_dir(path.name, normalized=normalized):
        return config.spaces_root / "review" / "misc" / _bundle_focus_name(path, config=config)
    bucket = _project_bucket_for(path, config)
    if bucket is not None:
        return config.spaces_root / "projects" / bucket / path.name
    if normalized:
        return config.spaces_root / "review" / "misc" / _bundle_focus_name(path, config=config)
    if lowered_name:
        return config.spaces_root / "review" / "misc" / _bundle_focus_name(path, config=config)
    return None


def _top_level_loose_file_destination(path: Path, asset_type: str, config: IndexOrganizerConfig) -> Path:
    review_asset = asset_type if asset_type in {"docs", "slides", "notes", "forms", "data", "assets", "misc"} else "misc"
    return config.spaces_root / "review" / review_asset / path.name


def _bundle_focus_name(source: Path, *, config: IndexOrganizerConfig, files: list[Path] | None = None) -> str:
    candidates = files
    if candidates is None:
        candidates = _list_files(source, config.ignore_names)
        if not candidates:
            candidates = [source]
    inferred = infer_bundle_focus_name(
        candidates,
        fallback_name=source.name,
        delimiter=config.naming.delimiter,
        max_segment_length=config.naming.max_segment_length,
        generic_tokens=config.generic_tokens,
    )
    if inferred:
        return inferred
    normalized = normalize_segment(source.name, config.naming.delimiter, config.naming.max_segment_length)
    return normalized or source.name


def _project_bucket_for(path: Path, config: IndexOrganizerConfig) -> str | None:
    text = str(path).lower()
    if "workspace" in text:
        return "workspace"
    if any(token in text for token in ("arduino", "platformio", "robot", "firmware", "iot", "pio")):
        return "embedded"
    if any(token in text for token in ("experiment", "experiments", "lab", "sandbox", "prototype", "pytorch", "c-project")):
        return "experiments"
    try:
        child_names = {child.name.lower() for child in path.iterdir()}
    except OSError:
        child_names = set()
    if {"code", "docs", "assets"} <= child_names or ("code" in child_names and ("docs" in child_names or "assets" in child_names)):
        return "workspace"
    if {"platformio.ini", ".pio"} & child_names:
        return "embedded"
    if any(marker.lower() in child_names for marker in config.project_markers):
        return "apps"
    try:
        for child in path.rglob("*"):
            if child.is_file() and child.suffix.lower() in CODE_EXTENSIONS:
                return "apps"
    except OSError:
        return None
    return None


def _stream_file_repair_actions(stream_root: Path, stream: str, config: IndexOrganizerConfig) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    for current_root, dirnames, filenames in os.walk(stream_root, topdown=True):
        current_path = Path(current_root)
        dirnames[:] = sorted(
            name
            for name in dirnames
            if name not in config.ignore_names and not config.is_protected_project_internal(current_path / name)
        )
        for filename in sorted(filenames):
            source = current_path / filename
            if filename in config.ignore_names or _is_metadata_artifact_file(source):
                continue
            classification = _stream_file_classification(source, stream, config)
            if classification is None:
                continue
            if classification.domain == config.naming.unknown_domain:
                continue
            if _is_temp_runner_file(source):
                actions.append(
                    PlannedAction(
                        action_type="quarantine",
                        source_path=source,
                        destination_path=config.quarantine_root / source.name,
                        reason="temporary runner artifact detected during tree repair",
                        confidence=0.95,
                        metadata={
                            "classification": classification.to_dict(),
                            "proposal_reason": "temp-runner-artifact",
                            "expires_days": config.deletion.quarantine_ttl_days,
                            "repair_scope": "stream-file-repair",
                        },
                    )
                )
                continue
            try:
                relative = source.relative_to(config.spaces_root)
            except ValueError:
                continue
            desired_relative_dir = config.canonical_relative_dir(classification)
            placement_mismatch = relative.parent != desired_relative_dir
            needs_name_repair = _needs_noncode_name_repair(source)
            if not placement_mismatch and not needs_name_repair:
                continue
            normalized_name = source.name
            metadata: dict[str, object] = {"repair_scope": "stream-file-repair"}
            if needs_name_repair:
                normalized = normalize_filename(source, classification, config)
                normalized_name = normalized.filename
                metadata["normalization"] = normalized.to_dict()
            destination = config.spaces_root / desired_relative_dir / normalized_name
            if destination == source:
                continue
            actions.append(
                PlannedAction(
                    action_type="move",
                    source_path=source,
                    destination_path=destination,
                    reason="repair non-code stream asset placement and filename"
                    if needs_name_repair and placement_mismatch
                    else "repair non-code stream asset placement"
                    if placement_mismatch
                    else "repair suspicious non-code filename",
                    confidence=1.0,
                    metadata=metadata,
                )
            )
    return actions


def _review_cleanup_actions(config: IndexOrganizerConfig) -> list[PlannedAction]:
    review_root = config.spaces_root / "review"
    if not review_root.exists():
        return []

    actions: list[PlannedAction] = []
    for current_root, dirnames, filenames in os.walk(review_root, topdown=True):
        current_path = Path(current_root)
        try:
            relative_root = current_path.relative_to(review_root)
        except ValueError:
            relative_root = Path(".")
        if relative_root.parts and relative_root.parts[0] in {"imports", "admin", "coding"}:
            dirnames[:] = []
            continue
        dirnames[:] = sorted(name for name in dirnames if name not in config.ignore_names)
        for filename in sorted(filenames):
            if filename in config.ignore_names:
                continue
            source = current_path / filename
            workspace_target = _review_code_workspace_target(source, config)
            if workspace_target is not None:
                actions.append(
                    PlannedAction(
                        action_type="move",
                        source_path=source,
                        destination_path=workspace_target,
                        reason="move review workspace file into the matching project root",
                        confidence=1.0,
                        metadata={"repair_scope": "review-workspace"},
                    )
                )
                continue
            classification, destination_dir = _review_target(source, review_root, config)
            metadata_like = source.suffix.lower() == ".iml" or source.name in JETBRAINS_PROJECT_FILES
            normalized = normalize_filename(source, classification, config)
            destination = destination_dir / (source.name if metadata_like else normalized.filename)
            if destination == source:
                continue
            actions.append(
                PlannedAction(
                    action_type="move",
                    source_path=source,
                    destination_path=destination,
                    reason="normalize existing review filenames and flatten legacy review wrappers",
                    confidence=1.0,
                    metadata={"repair_scope": "review", "normalization": normalized.to_dict()},
                )
            )
    return actions


def _residual_wrapper_action(source: Path, config: IndexOrganizerConfig) -> PlannedAction | None:
    text = str(source).lower()
    asset_type = _detect_asset_type(source)

    if any(hint in text for hint in ORGANIZER_TEST_HINTS):
        return PlannedAction(
            action_type="quarantine",
            source_path=source,
            destination_path=config.quarantine_root / source.name,
            reason="low-value organizer depth/usability test artifact detected",
            confidence=0.96,
            metadata={
                "repair_scope": "residual-wrapper",
                "proposal_reason": "organizer-test-artifact",
                "expires_days": config.deletion.quarantine_ttl_days,
            },
        )

    if asset_type == "notes" and _looks_like_working_rules_note(source):
        classification = ClassificationResult(
            space=config.default_space,
            stream="resources",
            domain="coding",
            focus=normalize_segment(
                config.repair_defaults.general_focus,
                config.naming.delimiter,
                config.naming.max_segment_length,
            ),
            asset_type="notes",
            confidence=1.0,
            rationale="promote workflow guidance note out of unknown/download wrapper",
            source="repair",
        )
    elif _is_downloadish_wrapper_path(source, config):
        classification = _review_classification(config, asset_type)
    else:
        return None

    normalized = normalize_filename(source, classification, config)
    filename = normalized.filename
    if _looks_like_working_rules_note(source):
        filename = _strip_leading_numeric_prefix(filename)
    destination = (
        config.spaces_root
        / config.canonical_relative_dir(classification)
        / filename
    )
    if destination == source:
        return None
    return PlannedAction(
        action_type="move",
        source_path=source,
        destination_path=destination,
        reason="collapse residual unknown/user/download wrapper into a clearer canonical location",
        confidence=1.0,
        metadata={"repair_scope": "residual-wrapper", "normalization": normalized.to_dict()},
    )


def _general_focus_actions(
    config: IndexOrganizerConfig,
    *,
    domain: str,
    legacy_focus: str,
    wrapper_name: str,
) -> list[PlannedAction]:
    focus_root = config.spaces_root / "areas" / domain / legacy_focus / "misc" / wrapper_name
    if not focus_root.exists():
        return []
    actions: list[PlannedAction] = []
    general_focus = normalize_segment(config.repair_defaults.general_focus, config.naming.delimiter, config.naming.max_segment_length)
    for child in sorted(focus_root.iterdir(), key=lambda path: path.name.lower()):
        if child.name in config.ignore_names or not child.is_file():
            continue
        asset_type = _detect_asset_type(child)
        classification = ClassificationResult(
            space=config.default_space,
            stream="areas",
            domain=domain,
            focus=general_focus,
            asset_type=asset_type,
            confidence=1.0,
            rationale="collapse legacy 02-areas wrapper into general focus",
            source="repair",
        )
        normalized = normalize_filename(child, classification, config)
        destination = config.spaces_root / "areas" / domain / asset_type / normalized.filename
        actions.append(
            PlannedAction(
                action_type="move",
                source_path=child,
                destination_path=destination,
                reason="collapse legacy 02-areas wrapper into general focus",
                confidence=1.0,
                metadata={"repair_scope": "legacy-wrapper", "normalization": normalized.to_dict()},
            )
        )
    return actions


def _shallow_nonproject_actions(config: IndexOrganizerConfig) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    general_focus = normalize_segment(
        config.repair_defaults.general_focus,
        config.naming.delimiter,
        config.naming.max_segment_length,
    )
    for stream in ("areas", "resources", "archive"):
        stream_root = config.spaces_root / stream
        if not stream_root.exists():
            continue
        for domain_root in sorted(stream_root.iterdir(), key=lambda path: path.name.lower()):
            if not domain_root.is_dir():
                continue
            if domain_root.name == config.naming.unknown_domain:
                continue
            general_root = domain_root / general_focus
            if not general_root.exists():
                continue
            for asset_root in sorted(general_root.iterdir(), key=lambda path: path.name.lower()):
                if asset_root.name in config.ignore_names or not asset_root.is_dir():
                    continue
                if asset_root.name not in config.asset_types:
                    continue
                for child in sorted(asset_root.iterdir(), key=lambda path: path.name.lower()):
                    if child.name in config.ignore_names:
                        continue
                    actions.append(
                        PlannedAction(
                            action_type="move",
                            source_path=child,
                            destination_path=domain_root / asset_root.name / child.name,
                            reason="drop generic general wrapper from non-project canonical path",
                            confidence=1.0,
                            metadata={"repair_scope": "shallow-nonproject"},
                        )
                    )
    return actions


def _flatten_project_wrapper_actions(config: IndexOrganizerConfig) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    projects_root = config.spaces_root / "projects"
    if not projects_root.exists():
        return actions

    for domain in PROJECT_WRAPPER_DOMAINS:
        domain_root = projects_root / domain
        if not domain_root.exists():
            continue
        for focus_root in sorted(domain_root.iterdir(), key=lambda path: path.name.lower()):
            if not focus_root.is_dir():
                continue
            code_root = focus_root / "code"
            if not code_root.exists() or not code_root.is_dir():
                continue
            source_key = normalize_segment(focus_root.name, config.naming.delimiter, config.naming.max_segment_length)
            effective_root = _unwrap_redundant_child(
                code_root,
                focus_name=focus_root.name,
                source_key=source_key,
                asset_name="code",
                config=config,
            )
            try:
                children = [
                    child
                    for child in sorted(effective_root.iterdir(), key=lambda path: path.name.lower())
                    if child.name != ".DS_Store"
                ]
            except OSError:
                continue
            for child in children:
                actions.append(
                    PlannedAction(
                        action_type="move",
                        source_path=child,
                        destination_path=focus_root / child.name,
                        reason="flatten redundant code wrapper from direct project root",
                        confidence=1.0,
                        metadata={"repair_scope": "project-wrapper", "collapsed_from": str(effective_root)},
                    )
                )
    return actions


def _flatten_nested_project_tree_actions(config: IndexOrganizerConfig) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    projects_root = config.spaces_root / "projects"
    if not projects_root.exists():
        return actions

    for domain in PROJECT_WRAPPER_DOMAINS:
        focus_root = projects_root / domain
        if not focus_root.exists() or not focus_root.is_dir():
            continue
        nested_root = _nested_projects_root(focus_root)
        if nested_root is None:
            continue
        for child in sorted(nested_root.iterdir(), key=lambda path: path.name.lower()):
            if child.name in config.ignore_names:
                continue
            actions.extend(
                _nested_project_child_actions(
                    child,
                    host_focus_root=focus_root,
                    projects_root=projects_root,
                )
            )
    return actions


def _merge_project_suffixed_asset_dir_actions(config: IndexOrganizerConfig) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    projects_root = config.spaces_root / "projects"
    if not projects_root.exists():
        return actions

    for domain in PROJECT_WRAPPER_DOMAINS:
        domain_root = projects_root / domain
        if not domain_root.exists():
            continue
        for focus_root in sorted(domain_root.iterdir(), key=lambda path: path.name.lower()):
            if not focus_root.is_dir():
                continue
            for child in sorted(focus_root.iterdir(), key=lambda path: path.name.lower()):
                if child.name in config.ignore_names or not child.is_dir():
                    continue
                match = SUFFIXED_ASSET_DIR_PATTERN.match(child.name)
                if match is None:
                    continue
                asset_name = match.group("asset")
                target_root = _project_suffixed_asset_target(focus_root, domain=domain, asset_name=asset_name)
                for item in sorted(child.iterdir(), key=lambda path: path.name.lower()):
                    if item.name == ".DS_Store":
                        continue
                    actions.append(
                        PlannedAction(
                            action_type="move",
                            source_path=item,
                            destination_path=target_root / item.name,
                            reason="merge suffixed project asset directory back into the canonical project folder",
                            confidence=1.0,
                            metadata={
                                "repair_scope": "project-suffixed-asset-dir",
                                "project_domain": domain,
                                "asset_name": asset_name,
                            },
                        )
                    )
    return actions


def _merge_suffixed_asset_dir_actions(config: IndexOrganizerConfig) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    for stream in ("areas", "resources", "archive"):
        stream_root = config.spaces_root / stream
        if not stream_root.exists():
            continue
        for domain_root in sorted(stream_root.iterdir(), key=lambda path: path.name.lower()):
            if not domain_root.is_dir():
                continue
            for child in sorted(domain_root.iterdir(), key=lambda path: path.name.lower()):
                if not child.is_dir():
                    continue
                match = SUFFIXED_ASSET_DIR_PATTERN.match(child.name)
                if match is None:
                    continue
                target_root = domain_root / match.group("asset")
                for item in sorted(child.iterdir(), key=lambda path: path.name.lower()):
                    if item.name == ".DS_Store":
                        continue
                    actions.append(
                        PlannedAction(
                            action_type="move",
                            source_path=item,
                            destination_path=target_root / item.name,
                            reason="merge suffixed asset directory back into canonical asset folder",
                            confidence=1.0,
                            metadata={"repair_scope": "suffixed-asset-dir"},
                        )
                    )
    return actions


def _cleanup_empty_project_wrappers(config: IndexOrganizerConfig) -> list[Path]:
    cleaned: list[Path] = []
    projects_root = config.spaces_root / "projects"
    if not projects_root.exists():
        return cleaned

    wrapper_names = {"code", "docs", "assets", "data", "misc", "notes", "slides", "forms", "output", "projects"}
    for domain in PROJECT_WRAPPER_DOMAINS:
        domain_root = projects_root / domain
        if not domain_root.exists():
            continue
        focus_roots = [domain_root]
        focus_roots.extend(
            focus_root
            for focus_root in sorted(domain_root.iterdir(), key=lambda path: path.name.lower())
            if focus_root.is_dir()
        )
        for focus_root in focus_roots:
            for wrapper_root in _project_wrapper_cleanup_roots(
                focus_root,
                wrapper_names=wrapper_names,
                config=config,
            ):
                if not wrapper_root.exists():
                    continue
                for artifact in sorted(wrapper_root.rglob(".DS_Store"), reverse=True):
                    try:
                        artifact.unlink()
                        cleaned.append(artifact)
                    except OSError:
                        continue
                for current_root, _, _ in os.walk(wrapper_root, topdown=False):
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


def _project_wrapper_cleanup_roots(
    focus_root: Path,
    *,
    wrapper_names: set[str],
    config: IndexOrganizerConfig,
) -> list[Path]:
    roots: list[Path] = []
    seen: set[Path] = set()
    for wrapper_name in wrapper_names:
        candidate = focus_root / wrapper_name
        if candidate in seen:
            continue
        seen.add(candidate)
        roots.append(candidate)
    try:
        children = sorted(focus_root.iterdir(), key=lambda path: path.name.lower())
    except OSError:
        return roots
    for child in children:
        if child.name in config.ignore_names or not child.is_dir():
            continue
        if SUFFIXED_ASSET_DIR_PATTERN.match(child.name) is None:
            continue
        if child in seen:
            continue
        seen.add(child)
        roots.append(child)
    return roots


def _project_suffixed_asset_target(focus_root: Path, *, domain: str, asset_name: str) -> Path:
    if focus_root.name == asset_name:
        return focus_root
    if domain == "workspace":
        return focus_root / asset_name
    if asset_name == "code":
        return focus_root
    return focus_root / asset_name


def _nested_projects_root(focus_root: Path) -> Path | None:
    current = focus_root / "projects"
    if not current.exists() or not current.is_dir():
        return None

    nested = current
    changed = False
    while True:
        next_focus = nested / focus_root.name
        next_projects = next_focus / "projects"
        if not next_projects.exists() or not next_projects.is_dir():
            break
        nested = next_projects
        changed = True
    return nested if changed else None


def _nested_project_child_actions(child: Path, *, host_focus_root: Path, projects_root: Path) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    if child.name in PROJECT_WRAPPER_DOMAINS and child.is_dir():
        for item in sorted(child.iterdir(), key=lambda path: path.name.lower()):
            if item.name == ".DS_Store":
                continue
            actions.append(
                PlannedAction(
                    action_type="move",
                    source_path=item,
                    destination_path=projects_root / child.name / item.name,
                    reason="lift nested repeated project domain wrapper back to the canonical projects root",
                    confidence=1.0,
                    metadata={"repair_scope": "nested-project-tree"},
                )
            )
        return actions

    if child.name == "apps" and child.is_dir():
        for item in sorted(child.iterdir(), key=lambda path: path.name.lower()):
            if item.name == ".DS_Store":
                continue
            actions.append(
                PlannedAction(
                    action_type="move",
                    source_path=item,
                    destination_path=projects_root / "apps" / item.name,
                    reason="lift nested apps bucket back to the canonical projects/apps root",
                    confidence=1.0,
                    metadata={"repair_scope": "nested-project-tree"},
                )
            )
        return actions

    actions.append(
        PlannedAction(
            action_type="move",
            source_path=child,
            destination_path=host_focus_root / child.name,
            reason="lift nested repeated project wrapper content back into the host project domain",
            confidence=1.0,
            metadata={"repair_scope": "nested-project-tree"},
        )
    )
    return actions


def _collapse_asset_wrapper_actions(config: IndexOrganizerConfig) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    for stream_root in (config.spaces_root / "areas", config.spaces_root / "resources"):
        if not stream_root.exists():
            continue
        for focus_root in _focus_roots(stream_root):
            misc_root = focus_root / "misc"
            if not misc_root.exists():
                continue
            actions.extend(_collapse_misc_children(misc_root, focus_root, config))
    return actions


def _collapse_misc_children(misc_root: Path, focus_root: Path, config: IndexOrganizerConfig) -> list[PlannedAction]:
    actions: list[PlannedAction] = []
    for child in sorted(misc_root.iterdir(), key=lambda path: path.name.lower()):
        if child.name in config.ignore_names:
            continue
        if child.is_file():
            continue
        asset_dir_name = _asset_dir_name(child.name)
        if asset_dir_name is not None:
            for item in sorted(child.iterdir(), key=lambda path: path.name.lower()):
                if item.name in config.ignore_names:
                    continue
                actions.append(
                    PlannedAction(
                        action_type="move",
                        source_path=item,
                        destination_path=focus_root / asset_dir_name / item.name,
                        reason="collapse redundant misc asset wrapper",
                        confidence=1.0,
                        metadata={"repair_scope": "legacy-wrapper"},
                    )
                )
            continue

        for asset_wrapper in sorted(child.iterdir(), key=lambda path: path.name.lower()):
            if asset_wrapper.name in config.ignore_names or not asset_wrapper.is_dir():
                continue
            asset_dir_name = _asset_dir_name(asset_wrapper.name)
            if asset_dir_name is None:
                continue
            topic_name = normalize_segment(child.name, config.naming.delimiter, config.naming.max_segment_length)
            for item in sorted(asset_wrapper.iterdir(), key=lambda path: path.name.lower()):
                if item.name in config.ignore_names:
                    continue
                actions.append(
                    PlannedAction(
                        action_type="move",
                        source_path=item,
                        destination_path=focus_root / asset_dir_name / topic_name / item.name,
                        reason="collapse nested misc/topic/asset wrapper",
                        confidence=1.0,
                        metadata={"repair_scope": "legacy-wrapper"},
                    )
                )
    return actions


def _legacy_output_dump_actions(config: IndexOrganizerConfig) -> list[PlannedAction]:
    wrapper_root = config.spaces_root / "projects" / "legacy-review" / "output-dumps" / "output" / "Docs" / "output"
    if not wrapper_root.exists():
        return []
    target_root = config.spaces_root / "projects" / "legacy-review" / "output-dumps" / "output"
    actions: list[PlannedAction] = []
    for child in sorted(wrapper_root.iterdir(), key=lambda path: path.name.lower()):
        if child.name in config.ignore_names:
            continue
        actions.append(
            PlannedAction(
                action_type="move",
                source_path=child,
                destination_path=target_root / child.name,
                reason="collapse redundant output-dumps Docs/output wrapper",
                confidence=1.0,
                metadata={"repair_scope": "legacy-wrapper"},
            )
        )
    return actions


def _focus_roots(stream_root: Path) -> list[Path]:
    roots: list[Path] = []
    for domain_root in sorted(stream_root.iterdir(), key=lambda path: path.name.lower()):
        if not domain_root.is_dir():
            continue
        for focus_root in sorted(domain_root.iterdir(), key=lambda path: path.name.lower()):
            if focus_root.is_dir():
                roots.append(focus_root)
    return roots


def _asset_dir_name(name: str) -> str | None:
    key = normalize_segment(name, "kebab-case", 40)
    if key in {"docs", "doc", "documents"}:
        return "docs"
    if key in {"assets", "asset", "media"}:
        return "assets"
    if key == "data":
        return "data"
    if key == "notes":
        return "notes"
    return None


def _detect_asset_type(path: Path) -> str:
    ext = path.suffix.lower()
    name = path.name.lower()
    if ext in DOC_EXTENSIONS:
        if any(keyword in name for keyword in FORM_KEYWORDS):
            return "forms"
        return "docs"
    if ext in SLIDE_EXTENSIONS:
        return "slides"
    if ext in NOTE_EXTENSIONS:
        return "notes"
    if ext in DATA_EXTENSIONS:
        return "data"
    if ext in ASSET_EXTENSIONS:
        return "assets"
    if ext in INSTALLER_EXTENSIONS:
        return "installers"
    if ext in ARCHIVE_EXTENSIONS:
        return "archives"
    if ext in CODE_EXTENSIONS or ext == ".code-workspace":
        return "code"
    return "misc"

def _stream_file_classification(source: Path, stream: str, config: IndexOrganizerConfig) -> ClassificationResult | None:
    try:
        relative = source.relative_to(config.spaces_root)
    except ValueError:
        return None
    parts = relative.parts
    if len(parts) < 3 or parts[0] != stream:
        return None
    domain = parts[1]
    if domain not in config.allowed_domains():
        return None
    focus = parts[2]
    asset_type = _detect_asset_type(source)
    if focus in config.asset_types:
        focus = normalize_segment(
            config.repair_defaults.general_focus,
            config.naming.delimiter,
            config.naming.max_segment_length,
        )
    inferred_focus = infer_focus_from_path(
        source,
        stream=stream,
        domain=domain,
        asset_type=asset_type,
        config=config,
        hint_text=extract_content_hint(source),
    )
    if inferred_focus:
        focus = inferred_focus
    return ClassificationResult(
        space=config.default_space,
        stream=stream,
        domain=domain,
        focus=focus,
        asset_type=asset_type,
        confidence=1.0,
        rationale="repair file placement within canonical non-code stream",
        source="repair",
    )


def _review_classification(config: IndexOrganizerConfig, asset_type: str, focus: str = "review") -> ClassificationResult:
    return ClassificationResult(
        space=config.default_space,
        stream="review",
        domain="review",
        focus=focus,
        asset_type=asset_type,
        confidence=1.0,
        rationale="repair review target",
        source="repair",
    )


def _looks_like_working_rules_note(source: Path) -> bool:
    if source.suffix.lower() not in NOTE_EXTENSIONS:
        return False
    lowered = normalize_segment(source.stem, "kebab-case", 120)
    return "working-rules" in lowered or any(hint in lowered for hint in WORKING_RULE_HINTS)


def _looks_like_top_level_review_wrapper(name: str, *, normalized: str | None = None) -> bool:
    lowered = name.lower()
    normalized_value = normalized if normalized is not None else normalize_segment(name, "kebab-case", 120)
    if any(hint in lowered for hint in ("다운로드",)):
        return True
    if any(hint in normalized_value for hint in ROOT_REVIEW_WRAPPER_HINTS):
        return True
    return False


def _looks_like_timestamp_dump(name: str) -> bool:
    normalized = normalize_segment(name, "kebab-case", 120)
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}t\d{2}.*(?:json|zip)?$", normalized))


def _looks_like_cache_or_artifact_dir(name: str, *, normalized: str | None = None) -> bool:
    normalized_value = normalized if normalized is not None else normalize_segment(name, "kebab-case", 120)
    return any(token in normalized_value for token in ("cache", "tmp", "temp", "artifact", "artifacts"))


def _is_downloadish_wrapper_path(source: Path, config: IndexOrganizerConfig) -> bool:
    try:
        relative = source.relative_to(config.spaces_root)
    except ValueError:
        return False
    lowered_parts = [part.lower() for part in relative.parts[:-1]]
    if not lowered_parts:
        return False
    if lowered_parts[0] == "system":
        return True
    if lowered_parts[:2] == ["resources", "unsorted"]:
        return True
    if lowered_parts[:2] == ["resources", "unknown"]:
        return True
    return any(part in DOWNLOAD_WRAPPER_HINTS for part in lowered_parts)


def _strip_leading_numeric_prefix(filename: str) -> str:
    stem = Path(filename).stem
    ext = Path(filename).suffix
    stripped = re.sub(r"^\d+(?:[-_])+","", stem)
    return f"{stripped}{ext}" if stripped else filename


def _review_target(
    source: Path,
    review_root: Path,
    config: IndexOrganizerConfig,
) -> tuple[ClassificationResult, Path]:
    relative = source.relative_to(review_root)
    parts = list(relative.parts)
    if parts and parts[0] == "inbox":
        parts = parts[1:]
    if not parts:
        return _review_classification(config, "misc"), source.parent

    if parts[0] == "misc":
        focus_source = parts[1] if len(parts) > 1 else "bundle"
        focus = normalize_segment(focus_source, config.naming.delimiter, config.naming.max_segment_length)
        destination_dir = review_root / "misc" / focus
        extra_parts = list(parts[2:-1]) if len(parts) > 3 else []
        extra_parts = [
            part
            for part in extra_parts
            if normalize_segment(part, config.naming.delimiter, config.naming.max_segment_length) != IDEA_CONFIG_DIR
        ]
        if extra_parts:
            destination_dir = destination_dir / Path(*extra_parts)
        return _review_classification(config, _detect_asset_type(source), focus=focus), destination_dir

    if len(parts) >= 2 and parts[1] in config.asset_types:
        focus = normalize_segment(parts[0], config.naming.delimiter, config.naming.max_segment_length)
        asset_type = parts[1]
        classification = _review_classification(config, asset_type, focus=focus)
        destination_dir = config.spaces_root / config.canonical_relative_dir(classification)
        tail_parts = _clean_review_tail_parts(parts[2:-1], asset_type, config)
        if tail_parts:
            destination_dir = destination_dir / Path(*tail_parts)
        return classification, destination_dir

    asset_type = parts[0] if parts[0] in config.asset_types else _detect_asset_type(source)
    focus = infer_focus_from_path(
        source,
        stream="review",
        domain="review",
        asset_type=asset_type,
        config=config,
        hint_text=extract_content_hint(source),
    ) or "review"
    classification = _review_classification(config, asset_type, focus=focus)
    destination_dir = config.spaces_root / config.canonical_relative_dir(classification)
    if len(parts) > 2:
        tail_parts = _clean_review_tail_parts(parts[1:-1], asset_type, config)
        if tail_parts:
            destination_dir = destination_dir / Path(*tail_parts)
    return classification, destination_dir


def _review_code_workspace_target(source: Path, config: IndexOrganizerConfig) -> Path | None:
    if source.suffix.lower() != ".code-workspace":
        return None
    try:
        relative = source.relative_to(config.spaces_root / "review")
    except ValueError:
        return None
    if not relative.parts:
        return None
    focus = normalize_segment(source.stem, config.naming.delimiter, config.naming.max_segment_length)
    for bucket in ("apps", "embedded", "experiments", "workspace", "legacy-review"):
        candidate = config.spaces_root / "projects" / bucket / focus
        if candidate.exists() and candidate.is_dir():
            return candidate / source.name
    return None


def _clean_review_tail_parts(parts: list[str], asset_type: str, config: IndexOrganizerConfig) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for part in parts:
        normalized = normalize_segment(part, config.naming.delimiter, config.naming.max_segment_length)
        if not normalized:
            continue
        if normalized in {"inbox", "imports", "unsorted", "review"}:
            continue
        if normalized == asset_type:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        cleaned.append(normalized)
    return cleaned


def _trim_wrapper_relative(relative: Path, bundle_name: str, config: IndexOrganizerConfig) -> Path:
    parts = list(relative.parts)
    bundle_key = normalize_segment(bundle_name, config.naming.delimiter, config.naming.max_segment_length)
    while parts:
        head = normalize_segment(parts[0], config.naming.delimiter, config.naming.max_segment_length)
        if head in WRAPPER_DIR_KEYS or head == bundle_key:
            parts.pop(0)
            continue
        break
    if not parts:
        return Path(relative.name)
    return Path(*parts)


def _list_files(root: Path, ignore_names: tuple[str, ...]) -> list[Path]:
    results: list[Path] = []
    for current_root, dirnames, filenames in os.walk(root, topdown=True):
        current_path = Path(current_root)
        dirnames[:] = sorted(name for name in dirnames if name not in ignore_names)
        for filename in sorted(filenames):
            candidate = current_path / filename
            if filename in ignore_names or _is_metadata_artifact_file(candidate):
                continue
            results.append(candidate)
    return results


def _code_asset_roots(config: IndexOrganizerConfig) -> list[Path]:
    projects_root = config.spaces_root / "projects"
    if not projects_root.exists():
        return []
    roots: list[Path] = []
    for domain in DIRECT_PROJECT_DOMAINS:
        domain_root = projects_root / domain
        if not domain_root.exists():
            continue
        roots.extend(path for path in domain_root.iterdir() if path.is_dir())
    workspace_root = projects_root / "workspace"
    if workspace_root.exists():
        roots.extend(path / "code" for path in workspace_root.iterdir() if path.is_dir() and (path / "code").is_dir())
    return roots


def _single_repo_root(code_asset_root: Path) -> Path | None:
    if (code_asset_root / ".git").exists():
        return code_asset_root
    candidates = [child for child in code_asset_root.iterdir() if child.is_dir() and child.name not in IGNORABLE_NAMES]
    if len(candidates) == 1:
        return candidates[0]
    return None


def _git_tracked_paths(repo_root: Path) -> set[str]:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_root), "ls-files", "-z"],
            check=True,
            capture_output=True,
            text=False,
        )
    except (OSError, subprocess.CalledProcessError):
        return set()
    return {entry.decode("utf-8", errors="ignore") for entry in result.stdout.split(b"\0") if entry}


def _is_git_tracked(candidate: Path, repo_root: Path | None, tracked_paths: set[str]) -> bool:
    if repo_root is None or not tracked_paths:
        return False
    try:
        relative = candidate.relative_to(repo_root)
    except ValueError:
        return False
    return relative.as_posix() in tracked_paths


def _iter_suspicious_code_paths(code_asset_root: Path) -> list[Path]:
    suspicious: list[Path] = []
    for current_root, dirnames, filenames in os.walk(code_asset_root, topdown=True):
        current_path = Path(current_root)
        dirnames[:] = sorted(name for name in dirnames if not _skip_code_audit_name(name))
        for filename in sorted(filenames):
            candidate = current_path / filename
            if _is_suspicious_code_name(candidate.name):
                suspicious.append(candidate)
    return suspicious


def _skip_code_audit_name(name: str) -> bool:
    if name in CODE_AUDIT_SKIP_NAMES:
        return True
    return name.startswith("cmake-build-")


def _is_suspicious_code_name(name: str) -> bool:
    return bool(SUSPICIOUS_CODE_NAME_PATTERN.search(name))


def _normalize_code_filename(path: Path, config: IndexOrganizerConfig) -> str:
    stem = path.stem
    stem = CODE_NAME_PREFIX_PATTERN.sub("", stem).strip()
    candidate = Path(f"{stem}{path.suffix}")
    normalized = normalize_segment(candidate.stem, config.naming.delimiter, config.naming.max_stem_length)
    return f"{normalized}{path.suffix.lower()}"


def _is_temp_runner_file(path: Path) -> bool:
    return path.name.lower().startswith("tempcoderunnerfile")


def _should_skip_empty_cleanup(path: Path) -> bool:
    for part in path.parts:
        if part in PROTECTED_EMPTY_DIR_NAMES:
            return True
        if part.startswith("cmake-build-"):
            return True
    return False


def _is_metadata_artifact_file(path: Path) -> bool:
    name = path.name.strip().lower()
    if not name:
        return False
    if METADATA_ARTIFACT_PATTERN.search(name) is None and not name.startswith("~$"):
        return False
    return path.name.startswith(".") or "ds store" in name or name == "thumbs.db" or name.startswith("~$")


def _needs_noncode_name_repair(path: Path) -> bool:
    return bool(NONCODE_NAME_REPAIR_PATTERN.search(path.name))
