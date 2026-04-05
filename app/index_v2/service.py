from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any

from app.errors import AppError
from app.index_v2.classifier import IndexClassifier
from app.index_v2.db import IndexDatabase
from app.index_v2.executor import IndexExecutor
from app.index_v2.output_repair import build_outputs_repair_plan
from app.index_v2.planner import IndexPlanner
from app.index_v2.project_repair import build_project_repair_plan, cleanup_project_repair_source
from app.index_v2.reporting import prune_watch_reports, write_plan_report
from app.index_v2.semantic_policy import SemanticDomainPolicy
from app.index_v2.tree_repair import (
    _is_metadata_artifact_file,
    build_code_name_audit_plan,
    build_tree_repair_plan,
    cleanup_tree_repair_targets,
)
from app.index_v2.types import ActionPlan, IndexOrganizerConfig, IndexedNode

INCOMPLETE_SUFFIXES = (".download", ".crdownload", ".part", ".tmp", ".partial")
SYSTEM_DEPENDENCY_NAMES = {
    ".git",
    ".venv",
    "venv",
    "node_modules",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    "site-packages",
    "dist",
    "build",
}
INSTALLER_EXTENSIONS = {".dmg", ".pkg"}


class IndexOrganizerService:
    def __init__(self, config: IndexOrganizerConfig) -> None:
        self.config = config
        self.database = IndexDatabase(config.database_path)
        self.semantic_policy = SemanticDomainPolicy(config, self.database)
        self.classifier = IndexClassifier(config, self.database, self.semantic_policy)
        self.planner = IndexPlanner(config, self.database, self.classifier)
        self.executor = IndexExecutor(config, self.database)
        self._suppressed_paths: dict[str, float] = {}
        self._suppressed_paths_lock = Lock()
        self._watch_backlog_seeded = False
        self._last_watch_backlog_seed_at = 0.0

    def close(self) -> None:
        self.database.close()

    def scan(self, roots: tuple[Path, ...] | None = None) -> list[IndexedNode]:
        indexed: list[IndexedNode] = []
        excluded_roots = {self.config.state_dir, self.config.history_root}
        for root in roots or self.config.all_scan_roots():
            if not root.exists():
                continue
            if root.is_file():
                node = self._index_path(root)
                indexed.append(node)
                continue
            try:
                indexed.append(self._index_path(root))
            except OSError:
                continue
            for current_root, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
                current_path = Path(current_root)
                dirnames[:] = sorted(
                    [
                        name
                        for name in dirnames
                        if name not in self.config.ignore_names
                        and not (current_path / name).is_symlink()
                        and not self.config.is_protected_project_internal(current_path / name)
                        and not any((current_path / name) == excluded or _is_relative_to(current_path / name, excluded) for excluded in excluded_roots)
                    ]
                )
                filenames = sorted(
                    name
                    for name in filenames
                    if name not in self.config.ignore_names and not (current_path / name).is_symlink()
                )

                if current_path != root:
                    try:
                        indexed.append(self._index_path(current_path))
                    except OSError:
                        continue

                for filename in filenames:
                    try:
                        indexed.append(self._index_path(current_path / filename))
                    except OSError:
                        continue
        return indexed

    def run_command(
        self,
        *,
        command: str,
        apply_requested: bool = False,
        undo_limit: int = 1,
        delete_target: str | None = None,
    ) -> tuple[ActionPlan | None, dict[str, object]]:
        if command == "undo":
            previews = self.executor.undo(limit=undo_limit, apply_requested=apply_requested)
            return None, {"undone": previews}
        if command == "confirm-delete":
            deleted = self.executor.confirm_delete(apply_requested=apply_requested, target=delete_target)
            return None, {"deleted": deleted}
        if command == "stabilize":
            return self._run_stabilize_command(apply_requested=apply_requested)
        if command == "review-drain":
            nodes = self._adaptive_review_backlog_nodes()
            plan = self.planner.build_plan(command=command, nodes=nodes, now=datetime.now(timezone.utc))
            if apply_requested:
                self._suppress_plan_paths(plan)
                plan = self.executor.apply(plan, apply_requested=True)
                cleaned = self._cleanup_empty_adaptive_review_dirs()
            else:
                cleaned = []
            extras = self._report_plan(plan, command=command)
            extras["cleaned_adaptive_review_dirs"] = [str(path) for path in cleaned]
            return plan, extras
        if command == "repair-projects":
            plan = build_project_repair_plan(self.config)
            plan = self.executor.apply(plan, apply_requested=apply_requested)
            cleaned: list[Path] = []
            if apply_requested:
                cleaned = cleanup_project_repair_source(self.config)
            json_path, md_path = write_plan_report(plan, self.config.reports_dir)
            return plan, {
                "report_json": str(json_path),
                "report_md": str(md_path),
                "cleaned_empty_dirs": [str(path) for path in cleaned],
            }
        if command == "repair-tree":
            plan = build_tree_repair_plan(self.config)
            plan = self.executor.apply(plan, apply_requested=apply_requested)
            cleaned: list[Path] = []
            if apply_requested:
                cleaned = cleanup_tree_repair_targets(self.config)
            json_path, md_path = write_plan_report(plan, self.config.reports_dir)
            return plan, {
                "report_json": str(json_path),
                "report_md": str(md_path),
                "cleaned_empty_dirs": [str(path) for path in cleaned],
            }
        if command == "repair-code-names":
            plan = build_code_name_audit_plan(self.config, apply_requested=apply_requested)
            plan = self.executor.apply(plan, apply_requested=apply_requested)
            json_path, md_path = write_plan_report(plan, self.config.reports_dir)
            return plan, {"report_json": str(json_path), "report_md": str(md_path)}
        if command == "repair-outputs":
            outputs_root = Path(__file__).resolve().parents[2] / "outputs"
            plan = build_outputs_repair_plan(outputs_root)
            plan = self.executor.apply(plan, apply_requested=apply_requested)
            json_path, md_path = write_plan_report(plan, self.config.reports_dir)
            return plan, {"report_json": str(json_path), "report_md": str(md_path)}

        if command == "watch":
            self._observe_staging_queue()
            nodes = self._staged_candidates()
        else:
            roots = self._roots_for_command(command)
            nodes = self.scan(roots=roots)

        plan = self.planner.build_plan(command=command, nodes=nodes, now=datetime.now(timezone.utc))
        if command == "watch":
            self._finalize_staging_entries(nodes)
        if command in {"apply", "archive", "migrate", "watch"}:
            if command == "watch" and apply_requested:
                self._suppress_plan_paths(plan)
            plan = self.executor.apply(plan, apply_requested=apply_requested)
            cleaned_empty_dirs: list[Path] = []
            if apply_requested and command in {"apply", "migrate"}:
                cleaned_empty_dirs = self._cleanup_empty_legacy_roots()
            cleaned_watch_dirs: list[Path] = []
            if apply_requested and command in {"apply", "watch"}:
                cleaned_watch_dirs = self._cleanup_empty_watch_roots()
            extras = self._report_plan(plan, command=command)
            extras["cleaned_empty_dirs"] = [str(path) for path in cleaned_empty_dirs]
            extras["cleaned_empty_watch_dirs"] = [str(path) for path in cleaned_watch_dirs]
            return plan, extras
        json_path, md_path = write_plan_report(plan, self.config.reports_dir)
        return plan, {"report_json": str(json_path), "report_md": str(md_path)}

    def watch_forever(self, *, apply_requested: bool) -> None:
        try:
            from watchdog.events import FileSystemEventHandler
            from watchdog.observers import Observer
        except ImportError as exc:  # pragma: no cover - optional runtime dependency
            raise AppError("watchdog 패키지가 필요합니다. `pip install -r requirements.txt`를 실행하세요.") from exc

        service = self

        class Handler(FileSystemEventHandler):
            def on_any_event(self, event) -> None:  # type: ignore[override]
                service._dispatch_watchdog_event(event)

        observer = Observer()
        handler = Handler()
        for root in self.config.watch_roots:
            if root.exists():
                observer.schedule(handler, str(root), recursive=True)
        observer.start()
        try:
            while True:
                try:
                    self.run_watch_cycle(apply_requested=apply_requested)
                except Exception:
                    traceback.print_exc()
                time.sleep(self.config.watch.poll_interval_seconds)
        finally:
            observer.stop()
            observer.join()

    def run_service_forever(self, *, apply_requested: bool) -> None:
        try:
            from watchdog.events import FileSystemEventHandler
            from watchdog.observers import Observer
        except ImportError as exc:  # pragma: no cover - optional runtime dependency
            raise AppError("watchdog 패키지가 필요합니다. `pip install -r requirements.txt`를 실행하세요.") from exc

        service = self

        class Handler(FileSystemEventHandler):
            def on_any_event(self, event) -> None:  # type: ignore[override]
                service._dispatch_watchdog_event(event)

        observer = Observer()
        handler = Handler()
        for root in self.config.watch_roots:
            if root.exists():
                observer.schedule(handler, str(root), recursive=True)
        observer.start()
        next_report_at = time.monotonic() + self.config.service.maintenance_interval_seconds
        next_archive_at = time.monotonic() + self.config.service.archive_interval_seconds
        review_drain_interval = max(0, int(self.config.adaptive_placement.hidden_review_drain_interval_seconds))
        next_review_drain_at = time.monotonic()
        try:
            self.database.clear_staging_entries()
            # Seed existing entries once at service start so backlog files are
            # processed even without fresh filesystem events.
            self._seed_watch_backlog_once()
            if self.config.service.startup_apply:
                self._run_housekeeping(apply_requested=apply_requested)
                next_report_at = time.monotonic() + self.config.service.maintenance_interval_seconds
            if self.config.service.startup_archive:
                self.run_command(command="archive", apply_requested=apply_requested)
                next_archive_at = time.monotonic() + self.config.service.archive_interval_seconds
            while True:
                try:
                    self._seed_watch_backlog_periodic()
                    self.run_watch_cycle(apply_requested=apply_requested)
                    now = time.monotonic()
                    if now >= next_report_at:
                        self._run_housekeeping(apply_requested=apply_requested)
                        state = self._load_service_state()
                        state["last_report"] = datetime.now(timezone.utc).isoformat()
                        self._save_service_state(state)
                        next_report_at = now + self.config.service.maintenance_interval_seconds
                    if (
                        self.config.adaptive_mode_enabled()
                        and self.config.adaptive_placement.auto_drain_hidden_review
                        and now >= next_review_drain_at
                    ):
                        self.run_command(command="review-drain", apply_requested=apply_requested)
                        state = self._load_service_state()
                        state["last_adaptive_review_drain"] = datetime.now(timezone.utc).isoformat()
                        self._save_service_state(state)
                        next_review_drain_at = now + review_drain_interval if review_drain_interval > 0 else now
                    if now >= next_archive_at:
                        self.run_command(command="archive", apply_requested=apply_requested)
                        next_archive_at = now + self.config.service.archive_interval_seconds
                except Exception:
                    traceback.print_exc()
                time.sleep(self.config.watch.poll_interval_seconds)
        finally:
            observer.stop()
            observer.join()

    def run_service_tick(self, *, apply_requested: bool) -> dict[str, object]:
        now = datetime.now(timezone.utc)
        state = self._load_service_state()
        state_updates: dict[str, Any] = {}
        results: dict[str, object] = {}

        # Keep one-shot ticks useful by seeding watch backlog when needed.
        self._seed_watch_backlog_once()
        self._seed_watch_backlog_periodic()

        watch_plan, watch_extras = self.run_command(command="watch", apply_requested=apply_requested)
        results["watch"] = {"summary": watch_plan.summary(), **watch_extras}

        review_drain_due = self._adaptive_review_drain_due(now=now, state=state)
        if review_drain_due:
            review_plan, review_extras = self.run_command(command="review-drain", apply_requested=apply_requested)
            results["adaptive_review"] = {"summary": review_plan.summary(), **review_extras}
            state_updates["last_adaptive_review_drain"] = now.isoformat()

        should_report = False
        last_report = _parse_iso(state.get("last_report"))
        if self.config.service.startup_apply and last_report is None:
            should_report = True
        elif last_report is None or (now - last_report).total_seconds() >= self.config.service.maintenance_interval_seconds:
            should_report = True
        if should_report:
            results["maintenance"] = self._run_housekeeping(apply_requested=apply_requested)
            state_updates["last_report"] = now.isoformat()

        should_archive = False
        last_archive = _parse_iso(state.get("last_archive"))
        if self.config.service.startup_archive and last_archive is None:
            should_archive = True
        elif last_archive is None or (now - last_archive).total_seconds() >= self.config.service.archive_interval_seconds:
            should_archive = True
        if should_archive:
            archive_plan, archive_extras = self.run_command(command="archive", apply_requested=apply_requested)
            results["archive"] = {"summary": archive_plan.summary(), **archive_extras}
            state_updates["last_archive"] = now.isoformat()

        # Merge in-memory updates with latest on-disk state to avoid clobbering
        # watch/housekeeping fields written by nested command handlers.
        merged_state = self._load_service_state()
        if not merged_state:
            merged_state = state
        merged_state.update(state_updates)
        self._save_service_state(merged_state)
        return results

    def _seed_watch_backlog_once(self) -> None:
        if self._watch_backlog_seeded:
            return
        self._seed_watch_backlog()
        self._watch_backlog_seeded = True
        self._last_watch_backlog_seed_at = time.monotonic()

    def _seed_watch_backlog_periodic(self) -> None:
        interval = max(0, int(self.config.watch.backlog_rescan_seconds))
        if interval == 0:
            return
        now = time.monotonic()
        if now - self._last_watch_backlog_seed_at < interval:
            return
        self._seed_watch_backlog()
        self._last_watch_backlog_seed_at = now

    def _seed_watch_backlog(self) -> None:
        for candidate in self._watch_entry_roots():
            self._queue_watch_path(candidate)

    def _adaptive_review_drain_due(self, *, now: datetime, state: dict[str, Any]) -> bool:
        policy = self.config.adaptive_placement
        if not self.config.adaptive_mode_enabled() or not policy.auto_drain_hidden_review:
            return False
        if not self.config.adaptive_review_root.exists():
            return False
        if not self._adaptive_review_backlog_paths(limit=1):
            return False
        interval = max(0, int(policy.hidden_review_drain_interval_seconds))
        if interval == 0:
            return True
        last = _parse_iso(state.get("last_adaptive_review_drain"))
        if last is None:
            return True
        return (now - last).total_seconds() >= interval

    def _index_path(self, path: Path) -> IndexedNode:
        stat = path.lstat()
        kind = "dir" if path.is_dir() else "file"
        sha256 = self._sha256(path) if path.is_file() and not path.is_symlink() else None
        node = IndexedNode(
            path=path.absolute(),
            kind=kind,
            size=stat.st_size,
            ext=path.suffix.lower(),
            mtime=stat.st_mtime,
            ctime=stat.st_ctime,
            parent_path=path.parent.absolute() if path.parent != path else None,
            sha256=sha256,
            is_symlink=path.is_symlink(),
        )
        self.database.upsert_node(node)
        return node

    def _sha256(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    def _refresh_staging(self, nodes: list[IndexedNode]) -> None:
        observed_at = datetime.now(timezone.utc)
        existing = {row["path"]: row for row in self.database.list_staging_entries()}
        for node in nodes:
            root = self._watch_root_for(node.path)
            if root is None:
                continue
            previous = existing.get(str(node.path))
            gate_state = self._gate_state_for(node.path)
            stable_count = 0
            stable_since = None
            if previous is not None and gate_state == "stable_candidate":
                same_snapshot = previous["last_size"] == node.size and float(previous["last_mtime"]) == float(node.mtime)
                if same_snapshot:
                    anchor_raw = previous["stable_since"] or previous["last_observed_at"]
                    try:
                        anchor_at = datetime.fromisoformat(anchor_raw)
                    except Exception:
                        anchor_at = observed_at
                    stable_since = anchor_raw
                    if (observed_at - anchor_at).total_seconds() >= self.config.watch.stable_observation_seconds:
                        stable_count = int(previous["stable_count"]) + 1
                    else:
                        stable_count = int(previous["stable_count"])
            self.database.upsert_staging_entry(
                path=node.path,
                root_path=root,
                size=node.size,
                mtime=node.mtime,
                stable_count=stable_count,
                stable_since=stable_since,
                gate_state=gate_state,
            )

    def _observe_staging_queue(self) -> None:
        observed: list[IndexedNode] = []
        for row in self.database.list_staging_entries():
            path = Path(row["path"])
            if self._is_suppressed_path(path):
                self.database.delete_staging_entry(path)
                continue
            if not path.exists():
                self.database.delete_staging_entry(path)
                continue
            try:
                observed.append(self._index_path(path))
            except OSError:
                self.database.delete_staging_entry(path)
        if observed:
            self._refresh_staging(observed)

    def _staged_candidates(self) -> list[IndexedNode]:
        now = datetime.now(timezone.utc)
        indexed: list[IndexedNode] = []
        for row in self.database.list_staging_entries():
            path = Path(row["path"])
            if not path.exists():
                self.database.delete_staging_entry(path)
                continue
            defer_until = _parse_iso(row["defer_until"])
            if defer_until is not None and defer_until > now:
                continue
            gate_state = str(row["gate_state"] or "stable_candidate")
            if gate_state != "stable_candidate":
                continue
            last_write_age = now.timestamp() - path.stat().st_mtime
            if last_write_age < self.config.watch.staging_age_seconds:
                continue
            if int(row["stable_count"]) < 2:
                continue
            indexed.append(self._index_path(path))
        return indexed

    def _watch_root_for(self, path: Path) -> Path | None:
        for root in self.config.watch_roots:
            if _is_relative_to(path, root):
                return root
        return None

    def _handle_watchdog_event(self, event: object) -> None:
        for candidate in self._event_paths(event):
            if candidate is None:
                continue
            self._queue_watch_path(candidate)

    def _dispatch_watchdog_event(self, event: object) -> None:
        try:
            self._handle_watchdog_event(event)
        except Exception:
            traceback.print_exc()

    def _event_paths(self, event: object) -> tuple[Path, ...]:
        raw_paths = [getattr(event, "src_path", None), getattr(event, "dest_path", None)]
        unique: list[Path] = []
        seen: set[str] = set()
        for raw_path in raw_paths:
            if not raw_path:
                continue
            candidate = Path(raw_path)
            marker = str(candidate)
            if marker in seen:
                continue
            seen.add(marker)
            unique.append(candidate)
        return tuple(unique)

    def _queue_watch_path(self, path: Path) -> None:
        if self._is_runtime_internal(path):
            return
        if self.config.is_protected_project_internal(path):
            return
        self._prune_suppressed_paths()
        candidate = self._watch_candidate_path(path)
        if candidate is None:
            return
        if self._is_suppressed_path(candidate):
            self.database.delete_staging_entry(candidate)
            return
        if not candidate.exists():
            self.database.delete_staging_entry(candidate)
            return
        try:
            node = self._index_path(candidate)
        except OSError:
            self.database.delete_staging_entry(candidate)
            return
        self._refresh_staging([node])

    def _watch_candidate_path(self, path: Path) -> Path | None:
        root = self._watch_root_for(path)
        if root is None:
            return None
        # Never treat watch roots themselves (e.g., ~/Downloads) as move targets.
        if path == root:
            return None
        level = str(self.config.protection_level or "strict").lower()
        if level == "strict" and path.is_symlink():
            return None
        if _is_relative_to(path, self.config.spaces_root):
            relative = path.relative_to(self.config.spaces_root)
            managed_container_names = self._managed_watch_container_names()
            if relative.parts and relative.parts[0].startswith("."):
                return None
            if relative.parts and relative.parts[0] in managed_container_names and level in {"strict", "balanced"}:
                return None
            if self.config.is_protected_project_internal(path):
                return None
            if not relative.parts:
                return None
            if len(relative.parts) == 1:
                return path
            top_level = self.config.spaces_root / relative.parts[0]
            if top_level.is_dir() and self.classifier._is_project_root(top_level):
                return None
            if relative.parts[0].startswith(".") or relative.parts[0] in self.config.ignore_names:
                return None
            if top_level.name in SYSTEM_DEPENDENCY_NAMES:
                return None
            project_root = self._project_root_for(path, top_level)
            if project_root is not None:
                return project_root
            return path
        project_root = self._project_root_for(path, root)
        if project_root is not None:
            return project_root
        return path

    def _project_root_for(self, path: Path, root: Path) -> Path | None:
        current = path if path.is_dir() else path.parent
        while current != root and _is_relative_to(current, root):
            if self.classifier._is_project_root(current):
                return current
            current = current.parent
        return None

    def _is_runtime_internal(self, path: Path) -> bool:
        return any(_is_relative_to(path, excluded) for excluded in (self.config.state_dir, self.config.history_root))

    def _suppress_plan_paths(self, plan: ActionPlan) -> None:
        expire_at = time.monotonic() + max(
            30,
            self.config.watch.stable_observation_seconds + (self.config.watch.poll_interval_seconds * 2),
        )
        with self._suppressed_paths_lock:
            self._prune_suppressed_paths_locked(now=time.monotonic())
            for action in plan.actions:
                for candidate in (action.source_path, action.destination_path):
                    if candidate is None:
                        continue
                    if self._watch_root_for(candidate) is None:
                        continue
                    self._suppressed_paths[str(candidate)] = expire_at

    def _prune_suppressed_paths(self) -> None:
        with self._suppressed_paths_lock:
            self._prune_suppressed_paths_locked(now=time.monotonic())

    def _prune_suppressed_paths_locked(self, *, now: float) -> None:
        expired = [path for path, expire_at in self._suppressed_paths.items() if expire_at <= now]
        for path in expired:
            self._suppressed_paths.pop(path, None)

    def _is_suppressed_path(self, path: Path) -> bool:
        with self._suppressed_paths_lock:
            self._prune_suppressed_paths_locked(now=time.monotonic())
            suppressed_roots = tuple(self._suppressed_paths)
        for raw_root in suppressed_roots:
            root = Path(raw_root)
            if path == root or _is_relative_to(path, root):
                return True
        return False

    def run_watch_cycle(self, *, apply_requested: bool) -> tuple[ActionPlan, dict[str, object]]:
        self._observe_staging_queue()
        nodes = self._staged_candidates()
        plan = self.planner.build_plan(command="watch", nodes=nodes, now=datetime.now(timezone.utc))
        self._finalize_staging_entries(nodes)
        if apply_requested:
            self._suppress_plan_paths(plan)
            plan = self.executor.apply(plan, apply_requested=True)
        return plan, self._report_plan(plan, command="watch")

    def _gate_state_for(self, path: Path) -> str:
        name = path.name.lower()
        if self.config.is_protected_project_internal(path) or any(part in SYSTEM_DEPENDENCY_NAMES for part in path.parts):
            return "system_or_dependency"
        if name.endswith(INCOMPLETE_SUFFIXES):
            return "incomplete_or_transient"
        if name.endswith(".icloud"):
            return "cloud_placeholder"
        if path.suffix.lower() in INSTALLER_EXTENSIONS:
            return "installer_artifact"
        if path.is_dir():
            try:
                entries = [entry for entry in path.iterdir() if entry.name not in self.config.ignore_names]
            except OSError:
                return "stable_candidate"
            if not entries:
                return "empty_container"
        return "stable_candidate"

    def _finalize_staging_entries(self, nodes: list[IndexedNode]) -> None:
        for node in nodes:
            row = self.database.get_classification(node.path)
            if row is None:
                self.database.delete_staging_entry(node.path)
                continue
            try:
                metadata = json.loads(row["metadata_json"]) if row["metadata_json"] else {}
            except Exception:
                metadata = {}
            deferred_reason = metadata.get("deferred_reason")
            if deferred_reason:
                current_entry = next((item for item in self.database.list_staging_entries() if item["path"] == str(node.path)), None)
                previous_attempts = int(current_entry["attempt_count"]) if current_entry is not None else 0
                provider_attempts = metadata.get("provider_attempts") or []
                last_provider = None
                if provider_attempts and isinstance(provider_attempts, list):
                    last = provider_attempts[-1]
                    if isinstance(last, dict):
                        last_provider = last.get("provider")
                self.database.update_staging_entry(
                    node.path,
                    gate_state="stable_candidate",
                    defer_until=metadata.get("defer_until"),
                    attempt_count=previous_attempts + 1,
                    last_error_code=metadata.get("last_error_code"),
                    last_provider=str(last_provider) if last_provider else None,
                )
                continue
            self.database.delete_staging_entry(node.path)

    def _cleanup_empty_legacy_roots(self) -> list[Path]:
        cleaned: list[Path] = []
        for legacy_root in self._legacy_container_roots():
            for current_root, _, _ in os.walk(legacy_root, topdown=False):
                candidate = Path(current_root)
                if not candidate.exists() or not candidate.is_dir():
                    continue
                removable_entries: list[Path] = []
                has_meaningful_entries = False
                try:
                    for entry in candidate.iterdir():
                        if entry.name in self.config.ignore_names:
                            removable_entries.append(entry)
                            continue
                        has_meaningful_entries = True
                        break
                except OSError:
                    continue
                if has_meaningful_entries:
                    continue
                for entry in removable_entries:
                    try:
                        if entry.is_dir():
                            entry.rmdir()
                        else:
                            entry.unlink()
                    except OSError:
                        has_meaningful_entries = True
                        break
                if has_meaningful_entries:
                    continue
                try:
                    candidate.rmdir()
                    cleaned.append(candidate)
                except OSError:
                    continue
        return cleaned

    def _cleanup_empty_watch_roots(self) -> list[Path]:
        if not self.config.cleanup.prune_empty_watch_dirs:
            return []
        cleaned: list[Path] = []
        for watch_root in self.config.watch_roots:
            if not watch_root.exists() or not watch_root.is_dir():
                continue
            if watch_root == self.config.spaces_root or _is_relative_to(watch_root, self.config.spaces_root):
                continue
            for current_root, _, _ in os.walk(watch_root, topdown=False):
                candidate = Path(current_root)
                if candidate == watch_root or not candidate.exists() or not candidate.is_dir():
                    continue
                if self.config.is_protected_project_internal(candidate):
                    continue
                removable_entries: list[Path] = []
                has_meaningful_entries = False
                try:
                    for entry in candidate.iterdir():
                        if entry.name in self.config.ignore_names or _is_metadata_artifact_file(entry):
                            removable_entries.append(entry)
                            continue
                        has_meaningful_entries = True
                        break
                except OSError:
                    continue
                if has_meaningful_entries:
                    continue
                for entry in removable_entries:
                    try:
                        if entry.is_dir():
                            entry.rmdir()
                        else:
                            entry.unlink()
                    except OSError:
                        has_meaningful_entries = True
                        break
                if has_meaningful_entries:
                    continue
                try:
                    candidate.rmdir()
                    cleaned.append(candidate)
                except OSError:
                    continue
        return cleaned

    def _adaptive_review_backlog_nodes(self) -> list[IndexedNode]:
        nodes: list[IndexedNode] = []
        for path in self._adaptive_review_backlog_paths():
            try:
                nodes.append(self._index_path(path))
            except OSError:
                continue
        return nodes

    def _adaptive_review_backlog_paths(self, *, limit: int | None = None) -> list[Path]:
        if not self.config.adaptive_mode_enabled():
            return []
        root = self.config.adaptive_review_root
        if not root.exists() or not root.is_dir():
            return []
        limit_value = max(1, int(limit or self.config.adaptive_placement.hidden_review_max_items_per_tick))
        asset_types = set(self.config.asset_types)
        candidates: list[Path] = []
        seen: set[str] = set()
        for child in sorted(root.iterdir(), key=lambda path: path.name.lower()):
            if child.name in self.config.ignore_names or child.name.startswith("."):
                continue
            if child.is_file():
                continue
            if not child.is_dir():
                continue
            if child.name == "blocked-top-level":
                for bucket in sorted(child.iterdir(), key=lambda path: path.name.lower()):
                    if (
                        bucket.name in self.config.ignore_names
                        or bucket.name.startswith(".")
                        or not bucket.is_dir()
                        or bucket.name not in asset_types
                    ):
                        continue
                    if self._append_adaptive_review_bucket_children(
                        bucket=bucket,
                        candidates=candidates,
                        seen=seen,
                        limit_value=limit_value,
                    ):
                        break
                if len(candidates) >= limit_value:
                    break
                continue
            if child.name in asset_types:
                if self._append_adaptive_review_bucket_children(
                    bucket=child,
                    candidates=candidates,
                    seen=seen,
                    limit_value=limit_value,
                ):
                    break
            else:
                marker = str(child)
                if marker not in seen:
                    candidates.append(child)
                    seen.add(marker)
            if len(candidates) >= limit_value:
                break
        return candidates[:limit_value]

    def _append_adaptive_review_bucket_children(
        self,
        *,
        bucket: Path,
        candidates: list[Path],
        seen: set[str],
        limit_value: int,
    ) -> bool:
        for nested in sorted(bucket.iterdir(), key=lambda path: path.name.lower()):
            if nested.name in self.config.ignore_names or nested.name.startswith("."):
                continue
            if not (nested.is_dir() or nested.is_file()):
                continue
            if nested.is_dir():
                normalized_name = nested.name.strip().lower()
                if normalized_name in self.config.asset_types:
                    continue
                if (
                    normalized_name in self.config.allowed_domains()
                    and bucket.name in {"code", "notes"}
                ):
                    if bucket.parent.name == "blocked-top-level":
                        if self._append_adaptive_review_nested_children(
                            directory=nested,
                            candidates=candidates,
                            seen=seen,
                            limit_value=limit_value,
                        ):
                            return True
                    continue
                if self.semantic_policy.is_banned_generic_name(normalized_name):
                    continue
            marker = str(nested)
            if marker in seen:
                continue
            candidates.append(nested)
            seen.add(marker)
            if len(candidates) >= limit_value:
                return True
        return len(candidates) >= limit_value

    def _append_adaptive_review_nested_children(
        self,
        *,
        directory: Path,
        candidates: list[Path],
        seen: set[str],
        limit_value: int,
    ) -> bool:
        for child in sorted(directory.iterdir(), key=lambda path: path.name.lower()):
            if child.name in self.config.ignore_names or child.name.startswith("."):
                continue
            if not (child.is_dir() or child.is_file()):
                continue
            marker = str(child)
            if marker in seen:
                continue
            candidates.append(child)
            seen.add(marker)
            if len(candidates) >= limit_value:
                return True
        return len(candidates) >= limit_value

    def _cleanup_empty_adaptive_review_dirs(self) -> list[Path]:
        root = self.config.adaptive_review_root
        if not root.exists() or not root.is_dir():
            return []
        cleaned: list[Path] = []
        for current_root, _, _ in os.walk(root, topdown=False):
            candidate = Path(current_root)
            if candidate == root or not candidate.exists() or not candidate.is_dir():
                continue
            try:
                entries = [
                    entry
                    for entry in candidate.iterdir()
                    if entry.name not in self.config.ignore_names and not _is_metadata_artifact_file(entry)
                ]
            except OSError:
                continue
            if entries:
                continue
            for artifact in sorted(candidate.iterdir(), key=lambda entry: entry.name.lower()):
                if artifact.name in self.config.ignore_names or _is_metadata_artifact_file(artifact):
                    try:
                        if artifact.is_dir():
                            artifact.rmdir()
                        else:
                            artifact.unlink()
                    except OSError:
                        pass
            try:
                candidate.rmdir()
                cleaned.append(candidate)
            except OSError:
                continue
        return cleaned

    def _report_plan(self, plan: ActionPlan, *, command: str) -> dict[str, object]:
        plan.metadata["domain_candidates"] = self.semantic_policy.status_payload(limit=10)
        if command == "watch":
            summary = plan.summary()
            total = int(summary.get("total", 0))
            if total == 0 and not self.config.reporting.write_noop_watch_reports:
                pruned = prune_watch_reports(
                    self.config.reports_dir,
                    retention_days=self.config.reporting.watch_retention_days,
                    max_report_pairs=self.config.reporting.watch_max_report_pairs,
                )
                self._record_watch_state(plan, reported=False, pruned_watch_reports=len(pruned))
                extras: dict[str, object] = {"reported": False, "domain_candidates": plan.metadata["domain_candidates"]}
                if pruned:
                    extras["pruned_watch_reports"] = len(pruned)
                return extras

        json_path, md_path = write_plan_report(plan, self.config.reports_dir)
        extras: dict[str, object] = {
            "report_json": str(json_path),
            "report_md": str(md_path),
            "domain_candidates": plan.metadata["domain_candidates"],
        }
        if command == "watch":
            pruned = prune_watch_reports(
                self.config.reports_dir,
                retention_days=self.config.reporting.watch_retention_days,
                max_report_pairs=self.config.reporting.watch_max_report_pairs,
            )
            self._record_watch_state(
                plan,
                reported=True,
                report_json=json_path,
                report_md=md_path,
                pruned_watch_reports=len(pruned),
            )
            extras["reported"] = True
            if pruned:
                extras["pruned_watch_reports"] = len(pruned)
        return extras

    def _record_watch_state(
        self,
        plan: ActionPlan,
        *,
        reported: bool,
        report_json: Path | None = None,
        report_md: Path | None = None,
        pruned_watch_reports: int = 0,
    ) -> None:
        state = self._load_service_state()
        state["last_watch_at"] = plan.created_at
        state["last_watch_total"] = int(plan.summary().get("total", 0))
        state["last_watch_reported"] = reported
        state["last_watch_report_json"] = str(report_json) if report_json is not None else None
        state["last_watch_report_md"] = str(report_md) if report_md is not None else None
        state["last_watch_pruned_reports"] = pruned_watch_reports
        self._save_service_state(state)

    def _run_housekeeping(self, *, apply_requested: bool) -> dict[str, object]:
        removed_metadata = self._cleanup_metadata_artifacts(apply_requested=apply_requested)
        normalized_review_layout = self._normalize_adaptive_review_layout(apply_requested=apply_requested)
        relocated_blocked_top_levels = self._rehome_blocked_adaptive_top_level_dirs(apply_requested=apply_requested)
        pruned_stream_roots = self._prune_empty_stream_roots(apply_requested=apply_requested)
        pruned_watch_roots = self._cleanup_empty_watch_roots() if apply_requested else []
        pruned_watch_reports = prune_watch_reports(
            self.config.reports_dir,
            retention_days=self.config.reporting.watch_retention_days,
            max_report_pairs=self.config.reporting.watch_max_report_pairs,
        )
        state = self._load_service_state()
        state["last_housekeeping_at"] = datetime.now(timezone.utc).isoformat()
        state["last_housekeeping_removed_metadata"] = len(removed_metadata)
        state["last_housekeeping_pruned_stream_roots"] = len(pruned_stream_roots)
        state["last_housekeeping_pruned_watch_roots"] = len(pruned_watch_roots)
        state["last_housekeeping_pruned_watch_reports"] = len(pruned_watch_reports)
        self._save_service_state(state)
        return {
            "removed_metadata_artifacts": [str(path) for path in removed_metadata],
            "normalized_adaptive_review_layout": [str(path) for path in normalized_review_layout],
            "relocated_blocked_top_level_dirs": [str(path) for path in relocated_blocked_top_levels],
            "pruned_empty_stream_roots": [str(path) for path in pruned_stream_roots],
            "pruned_empty_watch_roots": [str(path) for path in pruned_watch_roots],
            "pruned_watch_reports": len(pruned_watch_reports),
        }

    def _run_stabilize_command(self, *, apply_requested: bool) -> tuple[ActionPlan, dict[str, object]]:
        timestamp = datetime.now(timezone.utc).isoformat()
        combined_actions = []
        scanned_roots: list[Path] = []
        cleaned: list[Path] = []
        component_summaries: dict[str, dict[str, int]] = {}
        component_builders = (
            ("repair-projects", build_project_repair_plan, cleanup_project_repair_source),
            ("repair-tree", build_tree_repair_plan, cleanup_tree_repair_targets),
            ("repair-code-names", lambda config: build_code_name_audit_plan(config, apply_requested=apply_requested), None),
        )

        for component_name, builder, cleanup in component_builders:
            plan = builder(self.config)
            plan = self.executor.apply(plan, apply_requested=apply_requested)
            combined_actions.extend(plan.actions)
            scanned_roots.extend(plan.scanned_roots)
            component_summaries[component_name] = {key: int(value) for key, value in plan.summary().items()}
            if apply_requested and cleanup is not None:
                cleaned.extend(cleanup(self.config))

        aggregate_plan = ActionPlan(
            command="stabilize",
            created_at=timestamp,
            scanned_roots=tuple(dict.fromkeys(scanned_roots)),
            actions=combined_actions,
        )
        json_path, md_path = write_plan_report(aggregate_plan, self.config.reports_dir)
        extras: dict[str, object] = {
            "report_json": str(json_path),
            "report_md": str(md_path),
            "cleaned_empty_dirs": [str(path) for path in cleaned],
            "component_summaries": component_summaries,
        }
        if apply_requested:
            maintenance = self._run_housekeeping(apply_requested=True)
            extras["maintenance_removed_metadata_artifacts"] = maintenance["removed_metadata_artifacts"]
            extras["maintenance_pruned_empty_stream_roots"] = maintenance["pruned_empty_stream_roots"]
            extras["maintenance_pruned_watch_reports"] = maintenance["pruned_watch_reports"]
        return aggregate_plan, extras

    def _cleanup_metadata_artifacts(self, *, apply_requested: bool) -> list[Path]:
        if not self.config.cleanup.remove_metadata_artifacts:
            return []
        removed: list[Path] = []
        roots: list[Path] = [self.config.spaces_root]
        if self.config.adaptive_review_root not in roots:
            roots.append(self.config.adaptive_review_root)
        for watch_root in self.config.watch_roots:
            if watch_root not in roots:
                roots.append(watch_root)
        for root in roots:
            if not root.exists():
                continue
            for current_root, dirnames, filenames in os.walk(root, topdown=True):
                current_path = Path(current_root)
                dirnames[:] = sorted(
                    name
                    for name in dirnames
                    if not self.config.is_protected_project_internal(current_path / name)
                )
                for filename in sorted(filenames):
                    candidate = current_path / filename
                    if candidate.is_symlink() or self.config.is_protected_project_internal(candidate):
                        continue
                    if candidate.name in self.config.ignore_names or _is_metadata_artifact_file(candidate):
                        removed.append(candidate)
                        if apply_requested:
                            try:
                                candidate.unlink()
                            except OSError:
                                removed.pop()
        return removed

    def _normalize_adaptive_review_layout(self, *, apply_requested: bool) -> list[Path]:
        if not apply_requested or not self.config.adaptive_mode_enabled():
            return []
        review_root = self.config.adaptive_review_root
        if not review_root.exists() or not review_root.is_dir():
            return []
        moved: list[Path] = []
        asset_types = set(self.config.asset_types)
        for focus_root in sorted(review_root.iterdir(), key=lambda path: path.name.lower()):
            if (
                not focus_root.is_dir()
                or focus_root.name in self.config.ignore_names
                or focus_root.name.startswith(".")
                or focus_root.name in asset_types
                or focus_root.name == "blocked-top-level"
            ):
                continue
            for child in sorted(focus_root.iterdir(), key=lambda path: path.name.lower()):
                if child.name in self.config.ignore_names or _is_metadata_artifact_file(child):
                    continue
                if child.name in asset_types:
                    destination = review_root / child.name / focus_root.name
                else:
                    destination = review_root / self.config.naming.misc_asset_type / focus_root.name / child.name
                moved.append(self._merge_move_path(child, destination))
            self._prune_empty_directory_branch(focus_root, stop_at=review_root)
        return moved

    def _rehome_blocked_adaptive_top_level_dirs(self, *, apply_requested: bool) -> list[Path]:
        if not apply_requested or not self.config.adaptive_mode_enabled():
            return []
        moved: list[Path] = []
        spaces_root = self.config.spaces_root
        if not spaces_root.exists() or not spaces_root.is_dir():
            return moved
        managed_names = set(self.config.managed_root_names())
        for child in sorted(spaces_root.iterdir(), key=lambda path: path.name.lower()):
            if (
                not child.is_dir()
                or child.name in self.config.ignore_names
                or child.name.startswith(".")
                or child.name in managed_names
            ):
                continue
            if not self.classifier._adaptive_name_is_blocked_for_top_level(child.name):
                continue
            destination = self.config.adaptive_review_root / "blocked-top-level" / child.name
            moved.append(self._merge_move_path(child, destination))
        return moved

    def _merge_move_path(self, source: Path, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            if not destination.exists():
                shutil.move(str(source), str(destination))
                return destination
            if destination.is_dir():
                for child in sorted(source.iterdir(), key=lambda path: path.name.lower()):
                    self._merge_move_path(child, destination / child.name)
                self._prune_empty_directory_branch(source, stop_at=source.parent)
                return destination
            destination = self._available_destination_path(destination)
        else:
            destination = self._available_destination_path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(destination))
        return destination

    def _available_destination_path(self, destination: Path) -> Path:
        if not destination.exists():
            return destination
        stem = destination.stem
        suffix = destination.suffix
        index = 2
        while True:
            candidate = destination.with_name(f"{stem}-{index}{suffix}")
            if not candidate.exists():
                return candidate
            index += 1

    def _prune_empty_directory_branch(self, path: Path, *, stop_at: Path) -> None:
        current = path
        while current.exists() and current.is_dir():
            if current == stop_at:
                break
            try:
                entries = [
                    entry
                    for entry in current.iterdir()
                    if entry.name not in self.config.ignore_names and not _is_metadata_artifact_file(entry)
                ]
            except OSError:
                break
            if entries:
                break
            try:
                metadata_entries = list(current.iterdir())
            except OSError:
                metadata_entries = []
            for artifact in metadata_entries:
                if artifact.name in self.config.ignore_names or _is_metadata_artifact_file(artifact):
                    try:
                        if artifact.is_dir():
                            artifact.rmdir()
                        else:
                            artifact.unlink()
                    except OSError:
                        pass
            try:
                parent = current.parent
                current.rmdir()
                current = parent
            except OSError:
                break

    def _prune_empty_stream_roots(self, *, apply_requested: bool) -> list[Path]:
        if not self.config.cleanup.prune_empty_stream_roots or self.config.adaptive_mode_enabled():
            return []
        removed: list[Path] = []
        for stream in self.config.streams:
            candidate = self.config.spaces_root / stream
            if not candidate.exists() or not candidate.is_dir():
                continue
            try:
                entries = [entry for entry in candidate.iterdir() if entry.name not in self.config.ignore_names]
            except OSError:
                continue
            if entries:
                continue
            removed.append(candidate)
            if apply_requested:
                try:
                    candidate.rmdir()
                except OSError:
                    removed.pop()
        return removed

    def _load_service_state(self) -> dict[str, Any]:
        path = self.config.service_state_path
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if not isinstance(payload, dict):
            return {}
        return {str(key): value for key, value in payload.items()}

    def _save_service_state(self, payload: dict[str, Any]) -> None:
        self.config.service_state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _roots_for_command(self, command: str) -> tuple[Path, ...]:
        if command == "archive":
            return (self.config.spaces_root,)
        if command == "migrate":
            return self._legacy_container_roots()
        if command == "watch":
            return self._watch_entry_roots()
        if command in {"apply", "plan", "report"}:
            roots: list[Path] = [self.config.spaces_root]
            for path in self._legacy_container_roots():
                if path not in roots:
                    roots.append(path)
            for path in self._watch_entry_roots():
                if path not in roots:
                    roots.append(path)
            return tuple(roots)
        return self.config.all_scan_roots()

    def _legacy_container_roots(self) -> tuple[Path, ...]:
        roots: list[Path] = []
        for root in self.config.migration_roots:
            for rule in self.config.migration_rules:
                candidate = root / rule.legacy_root_name
                if candidate.exists():
                    roots.append(candidate)
        return tuple(roots)

    def _watch_entry_roots(self) -> tuple[Path, ...]:
        protect_names = {"node_modules", ".venv", "venv", "__pycache__", ".git", ".gradle", ".idea", ".pio"}
        org_container_names = self._managed_watch_container_names()

        roots: list[Path] = []

        for root in self.config.watch_roots:
            if not root.exists() or not root.is_dir():
                continue
            is_spaces = root == self.config.spaces_root or _is_relative_to(root, self.config.spaces_root)

            if is_spaces:
                # Documents 내부: managed library roots are never reclassified in place.
                # Only loose top-level items or ad hoc drop folders are considered.
                try:
                    for child in sorted(root.iterdir(), key=lambda p: p.name.lower()):
                        if child.name in self.config.ignore_names or child.is_symlink():
                            continue
                        if child.name.startswith("."):
                            continue
                        if child.name in protect_names and child.is_dir():
                            continue
                        if child.name in org_container_names and child.is_dir():
                            continue
                        if child.is_file():
                            roots.append(child)
                            continue
                        if not child.is_dir():
                            continue
                        if self.config.adaptive_mode_enabled():
                            if self.classifier._adaptive_name_is_blocked_for_top_level(child.name):
                                self._collect_leaf_units(
                                    base=child,
                                    org_container_names=set(),
                                    protect_names=protect_names,
                                    out=roots,
                                    depth=0,
                                )
                            continue
                        if self.classifier._is_project_root(child):
                            continue
                        self._collect_leaf_units(
                            base=child,
                            org_container_names=set(),
                            protect_names=protect_names,
                            out=roots,
                            depth=0,
                        )
                except OSError:
                    pass
            else:
                # 외부 watch_root (Downloads, Desktop 등):
                # 재귀적으로 실제 분류 단위(파일/프로젝트)를 찾아 반환
                self._collect_leaf_units(
                    base=root,
                    org_container_names=org_container_names,
                    protect_names=protect_names,
                    out=roots,
                    depth=0,
                )

        return tuple(roots)

    def _managed_watch_container_names(self) -> set[str]:
        names = set(self.config.managed_root_names())
        names.update(rule.legacy_root_name for rule in self.config.migration_rules)
        return names

    def _collect_leaf_units(
        self,
        base: Path,
        org_container_names: set[str],
        protect_names: set[str],
        out: list[Path],
        depth: int = 0,
    ) -> None:
        """외부 watch_root에서 실제 AI 분류 단위를 재귀적으로 수집합니다.

        - 파일          → 분류 단위로 추가
        - 프로젝트 루트  → 폴더 전체를 하나의 단위로 추가
        - 순수 조직 폴더 → 재귀해서 내부 처리
        - 그 외 폴더    → depth < 2 이면 재귀, 그 이상이면 단위로 추가
        """
        if depth > 8:
            out.append(base)
            return
        try:
            children = sorted(base.iterdir(), key=lambda p: p.name.lower())
        except OSError:
            return

        for child in children:
            if child.name in self.config.ignore_names:
                continue
            if child.is_symlink():
                continue
            if child.name.startswith("."):
                continue

            if child.is_file():
                out.append(child)
                continue

            if not child.is_dir():
                continue

            if child.name in protect_names:
                continue

            # 프로젝트 루트 (.git, package.json+src/ 등) → 폴더 전체를 단위로
            if self.classifier._is_project_root(child):
                out.append(child)
                continue

            # 순수 조직 컨테이너 → 재귀
            if child.name in org_container_names:
                self._collect_leaf_units(
                    base=child,
                    org_container_names=org_container_names,
                    protect_names=protect_names,
                    out=out,
                    depth=depth + 1,
                )
                continue

            # 일반 일반 폴더: 무조건 최하위 파일까지 재귀해서 들어가 개별 단위로 판단 (폴더째 이동 금지)
            self._collect_leaf_units(
                base=child,
                org_container_names=org_container_names,
                protect_names=protect_names,
                out=out,
                depth=depth + 1,
            )


def _is_relative_to(path: Path, root: Path) -> bool:
    candidates = ((path, root), (path.resolve(strict=False), root.resolve(strict=False)))
    for candidate_path, candidate_root in candidates:
        try:
            candidate_path.relative_to(candidate_root)
            return True
        except ValueError:
            continue
    return False


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None
