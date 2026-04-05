from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.index_v2.db import IndexDatabase
from app.index_v2.naming import normalize_segment
from app.index_v2.types import IndexOrganizerConfig


@dataclass(frozen=True)
class DomainGateResult:
    domain: str
    status: str
    allowed: bool
    candidate_ready: bool
    observation_count: int
    max_confidence: float
    max_independent_signals: int
    blocked_reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "status": self.status,
            "allowed": self.allowed,
            "candidate_ready": self.candidate_ready,
            "observation_count": self.observation_count,
            "max_confidence": round(self.max_confidence, 4),
            "max_independent_signals": self.max_independent_signals,
            "blocked_reasons": list(self.blocked_reasons),
        }


class SemanticDomainPolicy:
    def __init__(self, config: IndexOrganizerConfig, database: IndexDatabase) -> None:
        self.config = config
        self.database = database
        self.sync_runtime_domains()

    def sync_runtime_domains(self) -> tuple[str, ...]:
        approved = tuple(self.database.list_approved_domains())
        self.config.runtime_approved_domains = approved
        return approved

    def approved_domains(self) -> tuple[str, ...]:
        return self.sync_runtime_domains()

    def is_allowed_domain(self, domain: str | None) -> bool:
        normalized = self.normalize_domain(domain)
        if not normalized:
            return False
        return normalized in set(self.config.allowed_domains())

    def is_banned_generic_name(self, value: str | None) -> bool:
        normalized = self.normalize_domain(value)
        if not normalized:
            return True
        return normalized in set(self.config.banned_domain_names())

    def normalize_domain(self, value: str | None) -> str:
        normalized = normalize_segment(
            str(value or ""),
            self.config.naming.delimiter,
            self.config.naming.max_segment_length,
        )
        return normalized.strip().lower()

    def observe_candidate(
        self,
        *,
        domain: str | None,
        node_path: Path,
        confidence: float,
        signals: list[str],
        source: str,
    ) -> DomainGateResult | None:
        normalized = self.normalize_domain(domain)
        if not normalized:
            return None
        if self.is_banned_generic_name(normalized):
            return DomainGateResult(
                domain=normalized,
                status="rejected",
                allowed=False,
                candidate_ready=False,
                observation_count=0,
                max_confidence=confidence,
                max_independent_signals=len(set(signals)),
                blocked_reasons=("banned_name",),
            )
        if self.is_allowed_domain(normalized):
            return DomainGateResult(
                domain=normalized,
                status="approved" if normalized in set(self.config.runtime_approved_domains) else "pinned",
                allowed=True,
                candidate_ready=False,
                observation_count=0,
                max_confidence=confidence,
                max_independent_signals=len(set(signals)),
                blocked_reasons=(),
            )
        if not self.config.domain_policy.allow_dynamic_domains:
            return DomainGateResult(
                domain=normalized,
                status="blocked",
                allowed=False,
                candidate_ready=False,
                observation_count=0,
                max_confidence=confidence,
                max_independent_signals=len(set(signals)),
                blocked_reasons=("dynamic_disabled",),
            )

        gate = self.config.domain_policy.new_domain_gate
        signal_set = sorted({str(signal).strip().lower() for signal in signals if str(signal).strip()})
        self.database.upsert_domain_registry(
            domain=normalized,
            status="candidate",
            metadata={"last_source": source, "last_signals": signal_set},
        )
        self.database.record_domain_observation(
            domain=normalized,
            node_path=node_path,
            confidence=confidence,
            signals=signal_set,
            source=source,
        )
        result = self.get_domain_gate_result(normalized)
        self.database.upsert_domain_registry(
            domain=normalized,
            status=result.status,
            metadata=result.to_dict(),
        )
        return result

    def get_domain_gate_result(self, domain: str | None) -> DomainGateResult | None:
        normalized = self.normalize_domain(domain)
        if not normalized:
            return None
        if self.is_allowed_domain(normalized):
            return DomainGateResult(
                domain=normalized,
                status="approved" if normalized in set(self.config.runtime_approved_domains) else "pinned",
                allowed=True,
                candidate_ready=False,
                observation_count=0,
                max_confidence=1.0,
                max_independent_signals=0,
                blocked_reasons=(),
            )

        row = self.database.get_domain_registry(normalized)
        status = str(row["status"]) if row is not None else "candidate"
        if status == "rejected":
            return DomainGateResult(
                domain=normalized,
                status="rejected",
                allowed=False,
                candidate_ready=False,
                observation_count=0,
                max_confidence=0.0,
                max_independent_signals=0,
                blocked_reasons=("rejected",),
            )

        gate = self.config.domain_policy.new_domain_gate
        since = (datetime.now(timezone.utc) - timedelta(days=gate.window_days)).isoformat()
        recent = self.database.list_domain_observations(domain=normalized, since=since)
        observation_count = len({str(item["node_path"]) for item in recent})
        max_confidence = max((float(item["confidence"]) for item in recent), default=0.0)
        max_independent_signals = max((len(set(_signals_from_row(item))) for item in recent), default=0)
        weekly_since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        approved_this_week = self.database.count_domains_approved_since(weekly_since)

        blocked: list[str] = []
        if observation_count < gate.min_items_in_window:
            blocked.append("insufficient_items")
        if max_confidence < gate.min_confidence:
            blocked.append("insufficient_confidence")
        if max_independent_signals < gate.min_independent_signals:
            blocked.append("insufficient_signals")
        if approved_this_week >= gate.max_new_domains_per_week:
            blocked.append("approval_rate_limited")

        return DomainGateResult(
            domain=normalized,
            status=status,
            allowed=False,
            candidate_ready=not blocked,
            observation_count=observation_count,
            max_confidence=max_confidence,
            max_independent_signals=max_independent_signals,
            blocked_reasons=tuple(blocked),
        )

    def approve_domain(self, domain: str) -> DomainGateResult:
        normalized = self.normalize_domain(domain)
        if not normalized:
            raise ValueError("domain is required")
        if self.is_banned_generic_name(normalized):
            raise ValueError(f"cannot approve banned domain: {normalized}")
        result = self.get_domain_gate_result(normalized)
        if result is None:
            raise ValueError("domain is required")
        metadata = result.to_dict()
        metadata["approved_via"] = "cli"
        self.database.set_domain_status(domain=normalized, status="approved", metadata=metadata)
        self.sync_runtime_domains()
        return self.get_domain_gate_result(normalized) or result

    def reject_domain(self, domain: str) -> DomainGateResult:
        normalized = self.normalize_domain(domain)
        if not normalized:
            raise ValueError("domain is required")
        existing = self.get_domain_gate_result(normalized)
        metadata = existing.to_dict() if existing is not None else {"domain": normalized}
        metadata["rejected_via"] = "cli"
        self.database.set_domain_status(domain=normalized, status="rejected", metadata=metadata)
        self.sync_runtime_domains()
        result = self.get_domain_gate_result(normalized)
        if result is None:
            raise ValueError("domain is required")
        return result

    def status_payload(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        rows = self.database.list_domain_registry()
        payload: list[dict[str, Any]] = []
        for row in rows:
            result = self.get_domain_gate_result(str(row["domain"]))
            if result is None:
                continue
            item = result.to_dict()
            item["first_seen"] = row["first_seen"]
            item["last_seen"] = row["last_seen"]
            item["approved_at"] = row["approved_at"]
            item["rejected_at"] = row["rejected_at"]
            payload.append(item)
        if limit is not None:
            return payload[: max(0, limit)]
        return payload


def _signals_from_row(row: Any) -> list[str]:
    raw = row["signals_json"] if row is not None else "[]"
    try:
        payload = json.loads(raw) if raw else []
    except Exception:
        payload = []
    if not isinstance(payload, list):
        return []
    return [str(item) for item in payload]
