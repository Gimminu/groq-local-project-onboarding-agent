from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import sys
from typing import Any

_DATACLASS_SLOTS = {"slots": True} if sys.version_info >= (3, 10) else {}

PROVIDER_VALUES = ("heuristic", "groq", "ollama")
RENAME_MODE_VALUES = ("keep", "normalize", "semantic")
PROJECT_MODE_VALUES = ("skip", "wrap")
PARA_ROOTS = ("00_Inbox", "01_Projects", "02_Areas", "03_Resources", "04_Archive")
AREA_CATEGORIES = ("Finance", "Health", "Career", "Education", "Admin", "Personal")
RESOURCE_TYPES = (
    "Documents",
    "Datasets",
    "Media",
    "Templates",
    "Reading",
    "Research",
    "Installers",
    "Misc",
)


@dataclass(**_DATACLASS_SLOTS)
class OrganizerConfig:
    source_root: Path
    target_root: Path
    output_dir: Path
    provider: str = "heuristic"
    model: str | None = None
    ollama_url: str = "http://127.0.0.1:11434"
    rename_mode: str = "normalize"
    project_mode: str = "wrap"
    max_depth: int = 5
    min_age_seconds: int = 20
    watch_interval_seconds: int = 15
    enable_llm_for_known_types: bool = False
    sample_limit: int = 24
    allow_project_root: bool = False
    active_window_days: int = 30
    stale_project_days: int = 90
    event_stage_only: bool = True
    para_area_categories: tuple[str, ...] = AREA_CATEGORIES
    para_resource_types: tuple[str, ...] = RESOURCE_TYPES


@dataclass(**_DATACLASS_SLOTS)
class OrganizerDecision:
    source_path: Path
    destination_path: Path | None
    action: str
    status: str
    reason: str
    confidence: float
    risk_level: str
    para_root: str
    bucket_name: str
    review_required: bool = False
    blocked_reason: str | None = None
    rename_applied: bool = False
    provider_used: str = "heuristic"
    error: str | None = None

    @property
    def category_label(self) -> str:
        if self.bucket_name:
            return f"{self.para_root}/{self.bucket_name}"
        return self.para_root

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_path": str(self.source_path),
            "destination_path": str(self.destination_path) if self.destination_path else None,
            "action": self.action,
            "status": self.status,
            "reason": self.reason,
            "confidence": round(self.confidence, 4),
            "risk_level": self.risk_level,
            "para_root": self.para_root,
            "bucket_name": self.bucket_name,
            "review_required": self.review_required,
            "blocked_reason": self.blocked_reason,
            "rename_applied": self.rename_applied,
            "provider_used": self.provider_used,
            "error": self.error,
        }


@dataclass(**_DATACLASS_SLOTS)
class OrganizerPlan:
    command: str
    source_root: Path
    target_root: Path
    requested_provider: str
    provider_used: str
    decisions: list[OrganizerDecision] = field(default_factory=list)
    llm_fallback_reason: str | None = None

    def summary(self) -> dict[str, Any]:
        total = len(self.decisions)
        planned_moves = sum(1 for item in self.decisions if item.action == "move")
        manual_review = sum(1 for item in self.decisions if item.status == "manual_review")
        skipped = sum(1 for item in self.decisions if item.status == "skipped")
        applied = sum(1 for item in self.decisions if item.status == "applied")
        errors = sum(1 for item in self.decisions if item.status == "error")
        low_confidence = sum(
            1
            for item in self.decisions
            if item.review_required or item.risk_level != "low" or item.confidence < 0.85
        )
        category_counts: dict[str, int] = {}
        for item in self.decisions:
            key = item.para_root if item.status != "manual_review" else "manual_review"
            category_counts[key] = category_counts.get(key, 0) + 1
        return {
            "total": total,
            "planned_moves": planned_moves,
            "manual_review": manual_review,
            "skipped": skipped,
            "applied": applied,
            "errors": errors,
            "low_confidence": low_confidence,
            "category_counts": category_counts,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "command": self.command,
            "source_root": str(self.source_root),
            "target_root": str(self.target_root),
            "requested_provider": self.requested_provider,
            "provider_used": self.provider_used,
            "llm_fallback_reason": self.llm_fallback_reason,
            "summary": self.summary(),
            "decisions": [item.to_dict() for item in self.decisions],
        }
