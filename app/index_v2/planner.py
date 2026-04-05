from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import unicodedata

from app.index_v2.classifier import IndexClassifier
from app.index_v2.db import IndexDatabase
from app.index_v2.naming import normalize_filename
from app.index_v2.types import ActionPlan, ClassificationResult, IndexOrganizerConfig, IndexedNode, PlannedAction

ARCHIVE_SKIP_NAMES = {".git", ".venv", "venv", "node_modules", "__pycache__", ".mypy_cache", ".pytest_cache"}
TEMP_RUNNER_PREFIXES = ("tempcoderunnerfile",)
SYNTHETIC_TEST_HINTS = ("dummy-test", "dummy_test", "classification-smoke-test")
VISIBLE_DUPLICATE_ASSET_TYPES = {"docs", "slides", "notes", "forms", "data", "assets"}
FORCE_CANONICAL_RENAME_PATTERN = re.compile(
    r"(?i)(?:^\d{4}-\d{2}-\d{2}[_\-\s]|^\d{8}[_\-\s]|^\~\$|^\s*untitled(?:[\W_]|$)|\[(?:file|image|code|note|reference|spreadsheet|uncertain)\]|\buncertain\b)"
)


class IndexPlanner:
    def __init__(self, config: IndexOrganizerConfig, database: IndexDatabase, classifier: IndexClassifier) -> None:
        self.config = config
        self.database = database
        self.classifier = classifier

    def build_plan(
        self,
        *,
        command: str,
        nodes: list[IndexedNode],
        now: datetime | None = None,
    ) -> ActionPlan:
        timestamp = (now or datetime.now(timezone.utc)).isoformat()
        actions: list[PlannedAction] = []
        duplicate_groups = self.database.duplicate_hash_groups()
        duplicate_paths = {path for paths in duplicate_groups.values() for path in paths[1:]}
        covered_roots: list[Path] = []
        self.classifier.begin_batch(command)

        for node in nodes:
            if any(node.path != root and _is_relative_to(node.path, root) for root in covered_roots):
                continue
            if self._is_structural_container(node.path):
                actions.append(
                    PlannedAction(
                        action_type="ignore",
                        source_path=node.path,
                        destination_path=None,
                        reason="structural container directory",
                        confidence=1.0,
                    )
                )
                continue
            if self.config.is_protected_project_internal(node.path):
                actions.append(
                    PlannedAction(
                        action_type="ignore",
                        source_path=node.path,
                        destination_path=None,
                        reason="protected project internal path",
                        confidence=1.0,
                    )
                )
                continue
            if self._has_transient_lock_sibling(node.path):
                actions.append(
                    PlannedAction(
                        action_type="ignore",
                        source_path=node.path,
                        destination_path=None,
                        reason="paired Office lock file detected; file is likely still open",
                        confidence=1.0,
                    )
                )
                continue
            classification = self.classifier.classify(node)
            normalized = normalize_filename(node.path, classification, self.config) if node.kind == "file" else None
            if normalized is not None:
                classification.metadata = {**classification.metadata, "normalization": normalized.to_dict()}
            self.database.upsert_classification(node.path, classification, normalized.filename if normalized else None)

            if command == "scan":
                continue
            if classification.metadata.get("deferred_reason"):
                continue

            if node.path.name in self.config.ignore_names:
                actions.append(
                    PlannedAction(
                        action_type="ignore",
                        source_path=node.path,
                        destination_path=None,
                        reason="ignored system file",
                        confidence=1.0,
                    )
                )
                continue

            if command == "archive":
                archive_action = self._archive_action(node, classification, now or datetime.now(timezone.utc))
                if archive_action is not None:
                    actions.append(archive_action)
                    if node.kind == "dir":
                        covered_roots.append(node.path)
                continue

            if node.kind == "file" and str(node.path) in duplicate_paths:
                # Duplicates already inside the managed tree are common in datasets/assets.
                # Keep them in place by default to avoid review queue saturation.
                if self._should_keep_duplicate_visible(node, classification):
                    pass
                elif _is_relative_to(node.path, self.config.spaces_root) and command in {"plan", "report", "apply", "migrate"}:
                    actions.append(
                        PlannedAction(
                            action_type="ignore",
                            source_path=node.path,
                            destination_path=None,
                            reason="duplicate hash detected in managed tree (kept in place)",
                            confidence=0.97,
                            metadata={
                                "classification": classification.to_dict(),
                                "proposal_reason": "duplicate-file",
                            },
                        )
                    )
                    continue
                else:
                    actions.append(
                        PlannedAction(
                            action_type="quarantine",
                            source_path=node.path,
                            destination_path=self.config.quarantine_root / node.path.name,
                            reason="duplicate hash detected outside managed tree",
                            confidence=0.97,
                            metadata={
                                "classification": classification.to_dict(),
                                "proposal_reason": "duplicate-file",
                                "expires_days": self.config.deletion.quarantine_ttl_days,
                            },
                        )
                    )
                    continue

            low_value = self._low_value_quarantine(node, classification, now or datetime.now(timezone.utc))
            if low_value is not None:
                actions.append(low_value)
                if node.kind == "dir":
                    covered_roots.append(node.path)
                continue

            if classification.placement_mode == "keep_here":
                actions.append(
                    PlannedAction(
                        action_type="ignore",
                        source_path=node.path,
                        destination_path=None,
                        reason=classification.rationale or "classification explicitly kept item in place",
                        confidence=classification.confidence,
                        review_required=False,
                        metadata={"classification": classification.to_dict()},
                    )
                )
                continue

            if classification.review_required and not self._has_deterministic_target(node, classification):
                actions.append(
                    PlannedAction(
                        action_type="flag_for_review",
                        source_path=node.path,
                        destination_path=None,
                        reason=classification.rationale,
                        confidence=classification.confidence,
                        review_required=True,
                        metadata={"classification": classification.to_dict()},
                    )
                )
                continue

            if not classification.target_path:
                actions.append(
                    PlannedAction(
                        action_type="flag_for_review",
                        source_path=node.path,
                        destination_path=None,
                        reason=classification.rationale or "classification did not produce a prompt-compliant target path",
                        confidence=classification.confidence,
                        review_required=True,
                        metadata={"classification": classification.to_dict()},
                    )
                )
                continue

            destination_root = self.config.destination_root_for(classification)
            desired_dir = destination_root / self._destination_relative_dir(classification)
            desired_path = desired_dir if node.kind == "dir" else desired_dir / node.path.name
            if node.kind == "file" and classification.placement_mode == "merge_existing" and not desired_dir.exists():
                actions.append(
                    PlannedAction(
                        action_type="flag_for_review",
                        source_path=node.path,
                        destination_path=None,
                        reason="merge_existing requested but no existing target topic/project folder was found",
                        confidence=classification.confidence,
                        review_required=True,
                        metadata={"classification": classification.to_dict()},
                    )
                )
                continue
            if node.kind == "file" and classification.placement_mode == "single_file_folder":
                desired_path = desired_dir / node.path.name
            if len(str(desired_path)) > self.config.naming.max_path_length:
                actions.append(
                    PlannedAction(
                        action_type="flag_for_review",
                        source_path=node.path,
                        destination_path=None,
                        reason=(
                            f"canonical destination would exceed safe path limit "
                            f"({self.config.naming.max_path_length} chars)"
                        ),
                        confidence=classification.confidence,
                        review_required=True,
                        metadata={"classification": classification.to_dict()},
                    )
                )
                continue
            if node.kind == "dir" and _is_relative_to(desired_path, node.path):
                actions.append(
                    PlannedAction(
                        action_type="ignore",
                        source_path=node.path,
                        destination_path=None,
                        reason="self-nesting destination detected (ignored)",
                        confidence=classification.confidence,
                        metadata={"classification": classification.to_dict()},
                    )
                )
                continue
            if node.path != desired_path:
                actions.append(
                    PlannedAction(
                        action_type="move",
                        source_path=node.path,
                        destination_path=desired_path,
                        reason=classification.rationale,
                        confidence=classification.confidence,
                        review_required=classification.review_required,
                        metadata={"classification": classification.to_dict()},
                    )
                )
                if node.kind == "dir":
                    covered_roots.append(node.path)
                if normalized and normalized.filename != node.path.name:
                    actions.append(
                        PlannedAction(
                            action_type="rename",
                            source_path=desired_path,
                            destination_path=desired_dir / normalized.filename,
                            reason="normalized name after canonical move",
                            confidence=classification.confidence,
                            review_required=False,
                            metadata={"normalization": normalized.to_dict()},
                        )
                    )
                continue

            if normalized and normalized.filename != node.path.name and self._should_rename_canonical_file(node.path):
                actions.append(
                    PlannedAction(
                        action_type="rename",
                        source_path=node.path,
                        destination_path=node.path.with_name(normalized.filename),
                        reason="normalize filename within canonical location",
                        confidence=classification.confidence,
                        review_required=False,
                        metadata={"normalization": normalized.to_dict()},
                    )
                )

        return ActionPlan(command=command, created_at=timestamp, scanned_roots=self.config.all_scan_roots(), actions=actions)

    def _destination_relative_dir(self, classification: ClassificationResult) -> Path:
        return self.config.destination_relative_dir_for(classification)

    def _should_rename_canonical_file(self, path: Path) -> bool:
        return bool(FORCE_CANONICAL_RENAME_PATTERN.search(path.stem))

    def _has_transient_lock_sibling(self, path: Path) -> bool:
        if not path.is_file() or path.name.startswith("~$"):
            return False
        try:
            siblings = list(path.parent.iterdir())
        except OSError:
            return False
        stem = unicodedata.normalize("NFKC", path.stem).lower()
        for sibling in siblings:
            if sibling == path or not sibling.is_file():
                continue
            if not sibling.name.startswith("~$") or sibling.suffix.lower() != path.suffix.lower():
                continue
            lock_stem = unicodedata.normalize("NFKC", sibling.stem[2:]).lower()
            if not lock_stem:
                return True
            if lock_stem in stem or stem.endswith(lock_stem):
                return True
        return False

    def _has_deterministic_target(self, node: IndexedNode, classification: ClassificationResult) -> bool:
        if classification.metadata.get("invalid_canonical"):
            return False
        if classification.metadata.get("deferred_reason"):
            return False
        if classification.placement_mode == "review_only" and not classification.target_path:
            return False
        if int(classification.metadata.get("target_depth", 0) or 0) >= 5:
            return False
        if not classification.target_path and classification.placement_mode != "keep_here":
            return False
        return True

    def _low_value_quarantine(
        self,
        node: IndexedNode,
        classification: ClassificationResult,
        now: datetime,
    ) -> PlannedAction | None:
        normalized_path = unicodedata.normalize("NFKC", str(node.path)).lower()
        if node.kind == "file" and any(hint in normalized_path for hint in SYNTHETIC_TEST_HINTS):
            return PlannedAction(
                action_type="quarantine",
                source_path=node.path,
                destination_path=self.config.quarantine_root / node.path.name,
                reason="synthetic classification test artifact detected",
                confidence=0.97,
                metadata={
                    "classification": classification.to_dict(),
                    "proposal_reason": "synthetic-classification-test-artifact",
                    "expires_days": self.config.deletion.quarantine_ttl_days,
                },
            )
        if node.kind == "file" and node.path.name.lower().startswith(TEMP_RUNNER_PREFIXES):
            return PlannedAction(
                action_type="quarantine",
                source_path=node.path,
                destination_path=self.config.quarantine_root / node.path.name,
                reason="temporary runner artifact detected",
                confidence=0.91,
                metadata={
                    "classification": classification.to_dict(),
                    "proposal_reason": "temp-runner-artifact",
                    "expires_days": self.config.deletion.quarantine_ttl_days,
                },
            )
        if (
            node.kind == "file"
            and classification.asset_type == "installers"
            and not _is_relative_to(node.path, self.config.spaces_root)
        ):
            age_days = (now - datetime.fromtimestamp(node.mtime, tz=timezone.utc)).total_seconds() / 86400
            if age_days >= self.config.deletion.installer_grace_days:
                return PlannedAction(
                    action_type="quarantine",
                    source_path=node.path,
                    destination_path=self.config.quarantine_root / node.path.name,
                    reason="stale installer artifact moved to quarantine pending delete confirmation",
                    confidence=0.94,
                    metadata={
                        "classification": classification.to_dict(),
                        "proposal_reason": "stale-installer-artifact",
                        "expires_days": self.config.deletion.quarantine_ttl_days,
                    },
                    )
        return None

    def _should_keep_duplicate_visible(self, node: IndexedNode, classification: ClassificationResult) -> bool:
        if node.kind != "file":
            return False
        if classification.asset_type not in VISIBLE_DUPLICATE_ASSET_TYPES:
            return False
        return any(_is_relative_to(node.path, root) for root in self.config.watch_roots)

    def _archive_action(
        self,
        node: IndexedNode,
        classification: ClassificationResult,
        now: datetime,
    ) -> PlannedAction | None:
        if not self._archive_candidate(node, classification, now):
            return None
        archive_stamp = now.strftime("%Y%m%d_%H%M%S")
        archive_id = f"{node.path.stem}-{archive_stamp}"
        archive_dir = self.config.history_root / "archive" / now.strftime("%Y") / now.strftime("%m")
        destination = archive_dir / f"{archive_id}.zip"
        metadata = {"archive_id": archive_id, "classification": classification.to_dict()}
        return PlannedAction(
            action_type="archive",
            source_path=node.path,
            destination_path=destination,
            reason="stale item exceeded archive threshold",
            confidence=0.9,
            metadata=metadata,
        )

    def _archive_candidate(self, node: IndexedNode, classification: ClassificationResult, now: datetime) -> bool:
        if node.path.name in ARCHIVE_SKIP_NAMES:
            return False
        age = now - datetime.fromtimestamp(node.mtime, tz=timezone.utc)
        if age < timedelta(days=self.config.archive.stale_days):
            return False
        if node.kind == "dir":
            return True
        return True

    def _is_structural_container(self, path: Path) -> bool:
        for root in self.config.migration_roots:
            if not _is_relative_to(path, root):
                continue
            relative = path.relative_to(root)
            if len(relative.parts) == 1 and any(rule.legacy_root_name == relative.parts[0] for rule in self.config.migration_rules):
                return True
        if _is_relative_to(path, self.config.spaces_root):
            relative = path.relative_to(self.config.spaces_root)
            if self.config.shallow_structure and path.is_dir() and len(relative.parts) <= 1:
                return True
        return False


def _is_relative_to(path: Path, root: Path) -> bool:
    candidates = ((path, root), (path.resolve(strict=False), root.resolve(strict=False)))
    for candidate_path, candidate_root in candidates:
        try:
            candidate_path.relative_to(candidate_root)
            return True
        except ValueError:
            continue
    return False
