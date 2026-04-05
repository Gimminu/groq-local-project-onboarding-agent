from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from app.index_v2.types import ClassificationResult, IndexedNode, PlannedAction


class IndexDatabase:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self._configure()
        self._create_tables()
        self._migrate_schema()

    def close(self) -> None:
        with self._lock:
            self.connection.close()

    def _configure(self) -> None:
        with self._lock:
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA synchronous=NORMAL")
            self.connection.execute("PRAGMA foreign_keys=ON")

    def _create_tables(self) -> None:
        with self._lock:
            self.connection.executescript(
                """
            CREATE TABLE IF NOT EXISTS nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL UNIQUE,
                type TEXT NOT NULL,
                parent_id INTEGER,
                size INTEGER NOT NULL,
                ext TEXT NOT NULL,
                mtime REAL NOT NULL,
                ctime REAL NOT NULL,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                sha256 TEXT,
                is_symlink INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(parent_id) REFERENCES nodes(id)
            );

            CREATE TABLE IF NOT EXISTS classification (
                node_id INTEGER PRIMARY KEY,
                placement_mode TEXT NOT NULL,
                target_path TEXT,
                confidence REAL NOT NULL,
                rationale TEXT NOT NULL,
                source TEXT NOT NULL,
                create_folders_json TEXT NOT NULL,
                alternatives_json TEXT NOT NULL,
                normalized_name TEXT,
                metadata_json TEXT NOT NULL,
                classified_at TEXT NOT NULL,
                FOREIGN KEY(node_id) REFERENCES nodes(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                op_type TEXT NOT NULL,
                src_path TEXT,
                dst_path TEXT,
                prev_name TEXT,
                new_name TEXT,
                status TEXT NOT NULL,
                undo_payload_json TEXT NOT NULL,
                details_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS archive_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                archive_id TEXT NOT NULL,
                original_paths_json TEXT NOT NULL,
                zip_path TEXT NOT NULL,
                manifest_path TEXT NOT NULL,
                reason TEXT NOT NULL,
                stats_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS staging_queue (
                path TEXT PRIMARY KEY,
                root_path TEXT NOT NULL,
                first_seen TEXT NOT NULL,
                last_observed_at TEXT NOT NULL,
                last_size INTEGER NOT NULL,
                last_mtime REAL NOT NULL,
                stable_count INTEGER NOT NULL DEFAULT 0,
                stable_since TEXT,
                gate_state TEXT NOT NULL DEFAULT 'stable_candidate',
                defer_until TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error_code TEXT,
                last_provider TEXT,
                queued_for_action INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS deletion_proposals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL UNIQUE,
                reason TEXT NOT NULL,
                proposed_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                status TEXT NOT NULL,
                quarantine_path TEXT,
                metadata_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS llm_cache (
                signature TEXT PRIMARY KEY,
                response_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS provider_state (
                provider TEXT PRIMARY KEY,
                cooldown_until TEXT,
                last_error_code TEXT,
                consecutive_rate_limits INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS domain_registry (
                domain TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                approved_at TEXT,
                rejected_at TEXT,
                metadata_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS domain_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL,
                node_path TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                confidence REAL NOT NULL,
                signals_json TEXT NOT NULL,
                source TEXT NOT NULL,
                UNIQUE(domain, node_path)
            );
            """
            )
            self.connection.commit()

    def _migrate_schema(self) -> None:
        self._ensure_column("staging_queue", "gate_state", "TEXT NOT NULL DEFAULT 'stable_candidate'")
        self._ensure_column("staging_queue", "defer_until", "TEXT")
        self._ensure_column("staging_queue", "attempt_count", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("staging_queue", "last_error_code", "TEXT")
        self._ensure_column("staging_queue", "last_provider", "TEXT")
        with self._lock:
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS provider_state (
                    provider TEXT PRIMARY KEY,
                    cooldown_until TEXT,
                    last_error_code TEXT,
                    consecutive_rate_limits INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self.connection.commit()

    def _ensure_column(self, table: str, column: str, ddl: str) -> None:
        with self._lock:
            rows = self.connection.execute(f"PRAGMA table_info({table})").fetchall()
            known = {row["name"] for row in rows}
            if column in known:
                return
            self.connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
            self.connection.commit()

    def upsert_node(self, node: IndexedNode) -> int:
        now = _utc_now()
        with self._lock:
            parent_id = self._node_id(node.parent_path) if node.parent_path else None
            self.connection.execute(
                """
            INSERT INTO nodes(path, type, parent_id, size, ext, mtime, ctime, first_seen, last_seen, sha256, is_symlink)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                type=excluded.type,
                parent_id=excluded.parent_id,
                size=excluded.size,
                ext=excluded.ext,
                mtime=excluded.mtime,
                ctime=excluded.ctime,
                last_seen=excluded.last_seen,
                sha256=excluded.sha256,
                is_symlink=excluded.is_symlink
            """,
            (
                str(node.path),
                node.kind,
                parent_id,
                node.size,
                node.ext,
                node.mtime,
                node.ctime,
                now,
                now,
                node.sha256,
                1 if node.is_symlink else 0,
            ),
            )
            self.connection.commit()
            return self._node_id(node.path) or 0

    def upsert_classification(
        self,
        path: Path,
        classification: ClassificationResult,
        normalized_name: str | None = None,
    ) -> None:
        with self._lock:
            node_id = self._node_id(path)
            if node_id is None:
                return
            self.connection.execute(
                """
            INSERT INTO classification(
                node_id, placement_mode, target_path, confidence, rationale, source, 
                create_folders_json, alternatives_json, normalized_name, metadata_json, classified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                placement_mode=excluded.placement_mode,
                target_path=excluded.target_path,
                confidence=excluded.confidence,
                rationale=excluded.rationale,
                source=excluded.source,
                create_folders_json=excluded.create_folders_json,
                alternatives_json=excluded.alternatives_json,
                normalized_name=excluded.normalized_name,
                metadata_json=excluded.metadata_json,
                classified_at=excluded.classified_at
            """,
            (
                node_id,
                classification.placement_mode,
                classification.target_path,
                classification.confidence,
                classification.rationale,
                classification.source,
                json.dumps(list(classification.create_folders), ensure_ascii=False),
                json.dumps(list(classification.alternatives), ensure_ascii=False),
                normalized_name,
                json.dumps(classification.metadata, ensure_ascii=False),
                _utc_now(),
            ),
            )
            self.connection.commit()

    def get_classification(self, path: Path) -> sqlite3.Row | None:
        with self._lock:
            node_id = self._node_id(path)
            if node_id is None:
                return None
            return self.connection.execute("SELECT * FROM classification WHERE node_id = ?", (node_id,)).fetchone()

    def list_nodes(self, roots: Iterable[Path] | None = None) -> list[sqlite3.Row]:
        with self._lock:
            rows = self.connection.execute("SELECT * FROM nodes ORDER BY LENGTH(path), path").fetchall()
        if not roots:
            return rows
        filtered = []
        for row in rows:
            row_path = Path(row["path"])
            if any(_is_relative_to(row_path, root) for root in roots):
                filtered.append(row)
        return filtered

    def record_operation(
        self,
        action: PlannedAction,
        status: str,
        undo_payload: dict[str, Any],
        details: dict[str, Any] | None = None,
    ) -> int:
        with self._lock:
            cursor = self.connection.execute(
                """
            INSERT INTO operations(ts, op_type, src_path, dst_path, prev_name, new_name, status, undo_payload_json, details_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _utc_now(),
                action.action_type,
                str(action.source_path) if action.source_path else None,
                str(action.destination_path) if action.destination_path else None,
                action.source_path.name if action.source_path else None,
                action.destination_path.name if action.destination_path else None,
                status,
                json.dumps(undo_payload, ensure_ascii=False),
                json.dumps(details or action.metadata, ensure_ascii=False),
            ),
            )
            self.connection.commit()
            return int(cursor.lastrowid)

    def list_operations(self, limit: int) -> list[sqlite3.Row]:
        with self._lock:
            return self.connection.execute(
                "SELECT * FROM operations WHERE status = 'applied' ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()

    def record_archive_event(
        self,
        *,
        archive_id: str,
        original_paths: list[str],
        zip_path: Path,
        manifest_path: Path,
        reason: str,
        stats: dict[str, Any],
    ) -> None:
        with self._lock:
            self.connection.execute(
                """
            INSERT INTO archive_events(ts, archive_id, original_paths_json, zip_path, manifest_path, reason, stats_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _utc_now(),
                archive_id,
                json.dumps(original_paths, ensure_ascii=False),
                str(zip_path),
                str(manifest_path),
                reason,
                json.dumps(stats, ensure_ascii=False),
            ),
            )
            self.connection.commit()

    def upsert_staging_entry(
        self,
        *,
        path: Path,
        root_path: Path,
        size: int,
        mtime: float,
        stable_count: int,
        stable_since: str | None,
        gate_state: str = "stable_candidate",
        defer_until: str | None = None,
        attempt_count: int | None = None,
        last_error_code: str | None = None,
        last_provider: str | None = None,
    ) -> None:
        now = _utc_now()
        stored_attempt_count = 0 if attempt_count is None else attempt_count
        with self._lock:
            self.connection.execute(
                """
            INSERT INTO staging_queue(
                path, root_path, first_seen, last_observed_at, last_size, last_mtime,
                stable_count, stable_since, gate_state, defer_until, attempt_count,
                last_error_code, last_provider, queued_for_action
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            ON CONFLICT(path) DO UPDATE SET
                root_path=excluded.root_path,
                last_observed_at=excluded.last_observed_at,
                last_size=excluded.last_size,
                last_mtime=excluded.last_mtime,
                stable_count=excluded.stable_count,
                stable_since=excluded.stable_since,
                gate_state=excluded.gate_state,
                defer_until=COALESCE(excluded.defer_until, staging_queue.defer_until),
                attempt_count=COALESCE(excluded.attempt_count, staging_queue.attempt_count),
                last_error_code=COALESCE(excluded.last_error_code, staging_queue.last_error_code),
                last_provider=COALESCE(excluded.last_provider, staging_queue.last_provider)
            """,
            (
                str(path),
                str(root_path),
                now,
                now,
                size,
                mtime,
                stable_count,
                stable_since,
                gate_state,
                defer_until,
                stored_attempt_count,
                last_error_code,
                last_provider,
            ),
            )
            self.connection.commit()

    def list_staging_entries(self) -> list[sqlite3.Row]:
        with self._lock:
            return self.connection.execute("SELECT * FROM staging_queue ORDER BY last_observed_at").fetchall()

    def delete_staging_entry(self, path: Path) -> None:
        with self._lock:
            self.connection.execute("DELETE FROM staging_queue WHERE path = ?", (str(path),))
            self.connection.commit()

    def clear_staging_entries(self) -> None:
        with self._lock:
            self.connection.execute("DELETE FROM staging_queue")
            self.connection.commit()

    def update_staging_entry(
        self,
        path: Path,
        *,
        gate_state: str | None = None,
        defer_until: str | None = None,
        attempt_count: int | None = None,
        last_error_code: str | None = None,
        last_provider: str | None = None,
        clear_defer: bool = False,
    ) -> None:
        assignments: list[str] = []
        values: list[Any] = []
        if gate_state is not None:
            assignments.append("gate_state = ?")
            values.append(gate_state)
        if defer_until is not None:
            assignments.append("defer_until = ?")
            values.append(defer_until)
        elif clear_defer:
            assignments.append("defer_until = NULL")
        if attempt_count is not None:
            assignments.append("attempt_count = ?")
            values.append(attempt_count)
        if last_error_code is not None:
            assignments.append("last_error_code = ?")
            values.append(last_error_code)
        if last_provider is not None:
            assignments.append("last_provider = ?")
            values.append(last_provider)
        if not assignments:
            return
        values.append(str(path))
        with self._lock:
            self.connection.execute(
                f"UPDATE staging_queue SET {', '.join(assignments)} WHERE path = ?",
                tuple(values),
            )
            self.connection.commit()

    def get_provider_state(self, provider: str) -> sqlite3.Row | None:
        with self._lock:
            return self.connection.execute(
                "SELECT * FROM provider_state WHERE provider = ?",
                (provider,),
            ).fetchone()

    def upsert_provider_state(
        self,
        *,
        provider: str,
        cooldown_until: str | None,
        last_error_code: str | None,
        consecutive_rate_limits: int,
        updated_at: str | None = None,
    ) -> None:
        with self._lock:
            self.connection.execute(
                """
                INSERT INTO provider_state(provider, cooldown_until, last_error_code, consecutive_rate_limits, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(provider) DO UPDATE SET
                    cooldown_until=excluded.cooldown_until,
                    last_error_code=excluded.last_error_code,
                    consecutive_rate_limits=excluded.consecutive_rate_limits,
                    updated_at=excluded.updated_at
                """,
                (
                    provider,
                    cooldown_until,
                    last_error_code,
                    consecutive_rate_limits,
                    updated_at or _utc_now(),
                ),
            )
            self.connection.commit()

    def get_domain_registry(self, domain: str) -> sqlite3.Row | None:
        with self._lock:
            return self.connection.execute(
                "SELECT * FROM domain_registry WHERE domain = ?",
                (domain,),
            ).fetchone()

    def upsert_domain_registry(
        self,
        *,
        domain: str,
        status: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = _utc_now()
        with self._lock:
            self.connection.execute(
                """
                INSERT INTO domain_registry(domain, status, first_seen, last_seen, approved_at, rejected_at, metadata_json)
                VALUES (?, ?, ?, ?, NULL, NULL, ?)
                ON CONFLICT(domain) DO UPDATE SET
                    last_seen=excluded.last_seen,
                    metadata_json=excluded.metadata_json,
                    status=CASE
                        WHEN domain_registry.status IN ('approved', 'rejected') THEN domain_registry.status
                        ELSE excluded.status
                    END
                """,
                (
                    domain,
                    status,
                    now,
                    now,
                    json.dumps(metadata or {}, ensure_ascii=False),
                ),
            )
            self.connection.commit()

    def set_domain_status(self, *, domain: str, status: str, metadata: dict[str, Any] | None = None) -> None:
        now = _utc_now()
        with self._lock:
            self.connection.execute(
                """
                INSERT INTO domain_registry(domain, status, first_seen, last_seen, approved_at, rejected_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(domain) DO UPDATE SET
                    status=excluded.status,
                    last_seen=excluded.last_seen,
                    approved_at=excluded.approved_at,
                    rejected_at=excluded.rejected_at,
                    metadata_json=excluded.metadata_json
                """,
                (
                    domain,
                    status,
                    now,
                    now,
                    now if status == "approved" else None,
                    now if status == "rejected" else None,
                    json.dumps(metadata or {}, ensure_ascii=False),
                ),
            )
            self.connection.commit()

    def list_domain_registry(self, status: str | None = None) -> list[sqlite3.Row]:
        with self._lock:
            if status is None:
                return self.connection.execute(
                    "SELECT * FROM domain_registry ORDER BY last_seen DESC, domain ASC"
                ).fetchall()
            return self.connection.execute(
                "SELECT * FROM domain_registry WHERE status = ? ORDER BY last_seen DESC, domain ASC",
                (status,),
            ).fetchall()

    def list_approved_domains(self) -> list[str]:
        with self._lock:
            rows = self.connection.execute(
                "SELECT domain FROM domain_registry WHERE status = 'approved' ORDER BY domain ASC"
            ).fetchall()
        return [str(row["domain"]) for row in rows]

    def record_domain_observation(
        self,
        *,
        domain: str,
        node_path: Path,
        confidence: float,
        signals: list[str],
        source: str,
    ) -> None:
        with self._lock:
            self.connection.execute(
                """
                INSERT INTO domain_observations(domain, node_path, observed_at, confidence, signals_json, source)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(domain, node_path) DO UPDATE SET
                    observed_at=excluded.observed_at,
                    confidence=excluded.confidence,
                    signals_json=excluded.signals_json,
                    source=excluded.source
                """,
                (
                    domain,
                    str(node_path),
                    _utc_now(),
                    confidence,
                    json.dumps(signals, ensure_ascii=False),
                    source,
                ),
            )
            self.connection.commit()

    def list_domain_observations(
        self,
        *,
        domain: str | None = None,
        since: str | None = None,
    ) -> list[sqlite3.Row]:
        query = "SELECT * FROM domain_observations"
        clauses: list[str] = []
        values: list[Any] = []
        if domain is not None:
            clauses.append("domain = ?")
            values.append(domain)
        if since is not None:
            clauses.append("observed_at >= ?")
            values.append(since)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY observed_at DESC, domain ASC"
        with self._lock:
            return self.connection.execute(query, tuple(values)).fetchall()

    def count_domains_approved_since(self, since: str) -> int:
        with self._lock:
            row = self.connection.execute(
                "SELECT COUNT(*) AS total FROM domain_registry WHERE status = 'approved' AND approved_at >= ?",
                (since,),
            ).fetchone()
        return int(row["total"] if row is not None else 0)

    def get_llm_cache(self, signature: str) -> dict[str, Any] | None:
        with self._lock:
            row = self.connection.execute("SELECT response_json FROM llm_cache WHERE signature = ?", (signature,)).fetchone()
            if row is None:
                return None
            return json.loads(row["response_json"])

    def set_llm_cache(self, signature: str, payload: dict[str, Any]) -> None:
        with self._lock:
            self.connection.execute(
                """
            INSERT INTO llm_cache(signature, response_json, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(signature) DO UPDATE SET
                response_json=excluded.response_json,
                created_at=excluded.created_at
            """,
            (signature, json.dumps(payload, ensure_ascii=False), _utc_now()),
            )
            self.connection.commit()

    def upsert_deletion_proposal(
        self,
        *,
        path: Path,
        reason: str,
        expires_at: str,
        status: str,
        quarantine_path: Path | None,
        metadata: dict[str, Any],
    ) -> None:
        with self._lock:
            self.connection.execute(
                """
            INSERT INTO deletion_proposals(path, reason, proposed_at, expires_at, status, quarantine_path, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                reason=excluded.reason,
                expires_at=excluded.expires_at,
                status=excluded.status,
                quarantine_path=excluded.quarantine_path,
                metadata_json=excluded.metadata_json
            """,
            (
                str(path),
                reason,
                _utc_now(),
                expires_at,
                status,
                str(quarantine_path) if quarantine_path else None,
                json.dumps(metadata, ensure_ascii=False),
            ),
            )
            self.connection.commit()

    def list_deletion_proposals(self, status: str | None = None) -> list[sqlite3.Row]:
        with self._lock:
            if status is None:
                return self.connection.execute("SELECT * FROM deletion_proposals ORDER BY proposed_at DESC").fetchall()
            return self.connection.execute(
                "SELECT * FROM deletion_proposals WHERE status = ? ORDER BY proposed_at DESC",
                (status,),
            ).fetchall()

    def update_deletion_proposal_status(self, path: Path, status: str) -> None:
        with self._lock:
            self.connection.execute(
                "UPDATE deletion_proposals SET status = ? WHERE path = ? OR quarantine_path = ?",
                (status, str(path), str(path)),
            )
            self.connection.commit()

    def duplicate_hash_groups(self) -> dict[str, list[str]]:
        with self._lock:
            rows = self.connection.execute(
                """
            SELECT sha256, group_concat(path, '\n') AS paths
            FROM nodes
            WHERE sha256 IS NOT NULL AND sha256 != ''
            GROUP BY sha256
            HAVING COUNT(*) > 1
            """
            ).fetchall()
        groups: dict[str, list[str]] = {}
        for row in rows:
            existing_paths = [path for path in row["paths"].splitlines() if Path(path).exists()]
            if len(existing_paths) > 1:
                groups[row["sha256"]] = existing_paths
        return groups

    def _node_id(self, path: Path | None) -> int | None:
        if path is None:
            return None
        row = self.connection.execute("SELECT id FROM nodes WHERE path = ?", (str(path),)).fetchone()
        return int(row["id"]) if row else None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_relative_to(path: Path, root: Path) -> bool:
    candidates = ((path, root), (path.resolve(strict=False), root.resolve(strict=False)))
    for candidate_path, candidate_root in candidates:
        try:
            candidate_path.relative_to(candidate_root)
            return True
        except ValueError:
            continue
    return False
