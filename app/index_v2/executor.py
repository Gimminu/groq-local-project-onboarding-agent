from __future__ import annotations

import hashlib
import json
import shutil
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.index_v2.db import IndexDatabase
from app.index_v2.history import append_operation_history, write_archive_markdown
from app.index_v2.types import ActionPlan, IndexOrganizerConfig, PlannedAction


class IndexExecutor:
    def __init__(self, config: IndexOrganizerConfig, database: IndexDatabase) -> None:
        self.config = config
        self.database = database

    def apply(self, plan: ActionPlan, *, apply_requested: bool) -> ActionPlan:
        relocated_paths: dict[str, Path] = {}
        for action in plan.actions:
            if action.action_type in {"flag_for_review", "ignore"}:
                action.status = "skipped"
                continue
            if not apply_requested:
                action.status = "planned"
                continue
            try:
                if action.source_path is not None:
                    mapped_source = relocated_paths.get(str(action.source_path))
                    if mapped_source is not None:
                        action.source_path = mapped_source
                if action.action_type == "move":
                    planned_destination = str(action.destination_path) if action.destination_path else None
                    final_path = self._apply_move(action)
                    undo_payload = {"restore_src": str(action.source_path), "restore_dst": str(final_path)}
                    if planned_destination is not None:
                        relocated_paths[planned_destination] = final_path
                elif action.action_type == "rename":
                    final_path = self._apply_move(action)
                    undo_payload = {"restore_src": str(action.source_path), "restore_dst": str(final_path)}
                elif action.action_type == "quarantine":
                    final_path = self._apply_quarantine(action)
                    undo_payload = {"restore_src": str(action.source_path), "restore_dst": str(final_path)}
                elif action.action_type == "archive":
                    archive_payload = self._apply_archive(action)
                    undo_payload = archive_payload
                else:
                    action.status = "skipped"
                    continue
                action.status = "applied"
                self.database.record_operation(action, "applied", undo_payload)
                append_operation_history(self.config, action=action, status="applied")
            except Exception as exc:  # pragma: no cover - filesystem dependent
                action.status = "error"
                action.error = str(exc)
                self.database.record_operation(action, "error", {"error": str(exc)}, {"error": str(exc)})
        return plan

    def undo(self, *, limit: int, apply_requested: bool) -> list[dict[str, str]]:
        operations = self.database.list_operations(limit)
        previews: list[dict[str, str]] = []
        for row in operations:
            payload = json.loads(row["undo_payload_json"])
            op_type = row["op_type"]
            previews.append({"op_type": op_type, "src_path": row["src_path"], "dst_path": row["dst_path"]})
            if not apply_requested:
                continue
            if op_type in {"move", "rename", "quarantine"}:
                dst = Path(payload["restore_dst"])
                src = Path(payload["restore_src"])
                if dst.exists():
                    src.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(dst), str(src))
                    append_operation_history(
                        self.config,
                        action=PlannedAction(
                            action_type=op_type,
                            source_path=dst,
                            destination_path=src,
                            reason="undo applied operation",
                            confidence=1.0,
                        ),
                        status="undone",
                        event_type=f"undo-{op_type}",
                    )
            elif op_type == "archive":
                zip_path = Path(payload["zip_path"])
                manifest_path = Path(payload["manifest_path"])
                archive_md_path = Path(payload["archive_md_path"]) if payload.get("archive_md_path") else None
                for entry in payload["entries"]:
                    original_path = Path(entry["path"])
                    original_path.parent.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(zip_path, "r") as archive:
                    for entry in payload["entries"]:
                        original_path = Path(entry["path"])
                        archive.extract(entry["arcname"], path=original_path.parent)
                        extracted = original_path.parent / entry["arcname"]
                        if extracted != original_path:
                            extracted.replace(original_path)
                zip_path.unlink(missing_ok=True)
                manifest_path.unlink(missing_ok=True)
                if archive_md_path is not None:
                    archive_md_path.unlink(missing_ok=True)
                append_operation_history(
                    self.config,
                    action=PlannedAction(
                        action_type="archive",
                        source_path=zip_path,
                        destination_path=None,
                        reason="undo archive and restore original files",
                        confidence=1.0,
                    ),
                    status="undone",
                    event_type="undo-archive",
                    extra_lines=[
                        f"manifest: `{manifest_path}`",
                        f"archive_md: `{archive_md_path}`" if archive_md_path is not None else "archive_md: `-`",
                    ],
                )
        return previews

    def confirm_delete(self, *, apply_requested: bool, target: str | None = None) -> list[dict[str, str]]:
        proposals = self.database.list_deletion_proposals("pending")
        results: list[dict[str, str]] = []
        for row in proposals:
            quarantine_path = row["quarantine_path"]
            if quarantine_path is None:
                continue
            if target and target not in {row["path"], quarantine_path, str(row["id"])}:
                continue
            results.append({"path": quarantine_path, "reason": row["reason"]})
            if not apply_requested:
                continue
            path = Path(quarantine_path)
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink(missing_ok=True)
            self.database.update_deletion_proposal_status(path, "deleted")
            append_operation_history(
                self.config,
                action=PlannedAction(
                    action_type="confirm-delete",
                    source_path=path,
                    destination_path=None,
                    reason=row["reason"],
                    confidence=1.0,
                ),
                status="deleted",
            )
        return results

    def _apply_move(self, action: PlannedAction) -> Path:
        assert action.source_path is not None
        assert action.destination_path is not None
        original_source = action.source_path
        destination = self._unique_destination(action.destination_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(original_source), str(destination))
        action.source_path = original_source
        action.destination_path = destination
        return destination

    def _apply_quarantine(self, action: PlannedAction) -> Path:
        destination = self._apply_move(action)
        expires_at = (datetime.now(timezone.utc) + timedelta(days=action.metadata.get("expires_days", self.config.deletion.quarantine_ttl_days))).isoformat()
        self.database.upsert_deletion_proposal(
            path=action.source_path or destination,
            reason=action.reason,
            expires_at=expires_at,
            status="pending",
            quarantine_path=destination,
            metadata=action.metadata,
        )
        return destination

    def _apply_archive(self, action: PlannedAction) -> dict[str, object]:
        assert action.source_path is not None
        assert action.destination_path is not None
        archive_id = action.metadata["archive_id"]
        archive_path = action.destination_path
        manifest_path = archive_path.with_suffix(".manifest.json")
        archive_path.parent.mkdir(parents=True, exist_ok=True)

        entries: list[dict[str, str | int]] = []
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            if action.source_path.is_dir():
                for child in sorted(action.source_path.rglob("*")):
                    if child.is_dir() or child.is_symlink():
                        continue
                    arcname = str(Path(action.source_path.name) / child.relative_to(action.source_path))
                    archive.write(child, arcname=arcname)
                    entries.append(self._archive_entry(child, arcname))
            else:
                arcname = action.source_path.name
                archive.write(action.source_path, arcname=arcname)
                entries.append(self._archive_entry(action.source_path, arcname))

        manifest_payload = {
            "archive_id": archive_id,
            "reason": action.reason,
            "archived_at": datetime.now(timezone.utc).isoformat(),
            "classification": action.metadata.get("classification", {}),
            "original_paths": [entry["path"] for entry in entries],
            "entries": entries,
        }
        manifest_path.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        archive_md_path, history_path = write_archive_markdown(
            self.config,
            action=action,
            archive_path=archive_path,
            manifest_path=manifest_path,
            manifest_payload=manifest_payload,
        )
        self.database.record_archive_event(
            archive_id=archive_id,
            original_paths=[entry["path"] for entry in entries],
            zip_path=archive_path,
            manifest_path=manifest_path,
            reason=action.reason,
            stats={"entry_count": len(entries), "history_path": str(history_path), "archive_md_path": str(archive_md_path)},
        )

        if action.source_path.is_dir():
            shutil.rmtree(action.source_path)
        else:
            action.source_path.unlink(missing_ok=True)

        return {
            "zip_path": str(archive_path),
            "manifest_path": str(manifest_path),
            "archive_md_path": str(archive_md_path),
            "entries": entries,
        }

    def _unique_destination(self, destination: Path) -> Path:
        if not destination.exists():
            return destination
        stem = destination.stem if destination.is_file() else destination.name
        suffix = destination.suffix if destination.is_file() else ""
        counter = 2
        while True:
            candidate = destination.with_name(f"{stem}-{counter}{suffix}")
            if not candidate.exists():
                return candidate
            counter += 1

    def _archive_entry(self, path: Path, arcname: str) -> dict[str, str | int]:
        payload: dict[str, str | int] = {
            "path": str(path),
            "arcname": arcname,
            "size": path.stat().st_size,
        }
        if self.config.archive.manifest_hashes:
            payload["sha256"] = self._sha256(path)
        return payload

    def _sha256(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()
