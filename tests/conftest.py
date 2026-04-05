from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
import yaml

import app.index_v2.classifier as classifier_module
import index_organizer
from app.index_v2.config import load_index_config
from app.index_v2.service import IndexOrganizerService


def _deep_merge(target: dict, updates: dict) -> dict:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_merge(target[key], value)
        else:
            target[key] = value
    return target


@pytest.fixture(autouse=True)
def _disable_live_llm_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    # Keep tests deterministic regardless of local API keys/Ollama daemon state.
    monkeypatch.setattr(index_organizer, "load_local_env", lambda: None)
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.setattr(classifier_module, "_OLLAMA_AVAILABLE", False)


@pytest.fixture
def make_v2_service(tmp_path: Path):
    services: list[IndexOrganizerService] = []

    def _factory(overrides: dict | None = None):
        legacy_root = tmp_path / "legacy"
        watch_root = tmp_path / "Downloads"
        spaces_root = tmp_path / "spaces"
        history_root = tmp_path / "History"
        state_dir = tmp_path / "state"

        payload = {
            "spaces_root": str(spaces_root),
            "history_root": str(history_root),
            "state_dir": str(state_dir),
            "watch_roots": [str(watch_root)],
            "migration_roots": [str(legacy_root)],
            "scan_roots": [],
            "root_spaces": {str(watch_root): "personal", str(legacy_root): "learning"},
            "spaces": ["work", "personal", "learning", "shared", "ops"],
            "streams": ["inbox", "projects", "areas", "resources", "archive", "review", "system"],
            "domains": ["unknown", "apps", "embedded", "workspace", "experiments", "legacy-review", "education", "admin", "coding", "finance", "research", "legal", "templates"],
            "asset_types": ["misc", "docs", "slides", "notes", "forms", "code", "data", "output", "assets", "exports", "installers", "archives"],
            "default_space": "personal",
            "include_space_level": False,
            "move_uncertain_items": False,
            "review_mode": "flat",
            "protected_project_internal_roots": [
                "projects/*/*/code",
                "projects/*/*/code/*",
                "projects/*/*/code/**",
            ],
            "repair_defaults": {
                "general_focus": "general",
            },
            "naming": {
                "delimiter": "kebab-case",
                "max_stem_length": 80,
                "unknown_domain": "unknown",
                "unsorted_focus": "unsorted",
                "misc_asset_type": "misc",
            },
            "groq": {
                "enabled": False,
                "confidence_threshold": 0.75,
                "max_siblings": 20,
            },
            "watch": {
                "poll_interval_seconds": 1,
                "stable_observation_seconds": 0,
                "staging_age_seconds": 0,
            },
            "archive": {
                "stale_days": 30,
                "manifest_hashes": True,
            },
            "deletion": {
                "installer_grace_days": 7,
                "export_stale_days": 7,
                "quarantine_ttl_days": 30,
            },
            "reporting": {
                "write_noop_watch_reports": False,
                "watch_retention_days": 14,
                "watch_max_report_pairs": 200,
            },
            "cleanup": {
                "remove_metadata_artifacts": True,
                "prune_empty_stream_roots": True,
                "noncode_name_repair_streams": ["areas", "resources", "archive", "review"],
            },
            "repair_code_names": {
                "auto_apply_scopes": ["projects/legacy-review"],
            },
            "migration_rules": [
                {"legacy_root_name": "00_Inbox", "stream": "inbox"},
                {"legacy_root_name": "01_Projects", "stream": "projects", "domain": "coding", "asset_type": "code"},
                {"legacy_root_name": "02_Areas", "stream": "areas"},
                {"legacy_root_name": "03_Resources", "stream": "resources"},
                {"legacy_root_name": "04_Archive", "stream": "archive", "asset_type": "archives"},
            ],
            "pattern_overrides": [
                {"pattern": "(?i)education|course|lecture|class|수업|강의", "domain": "education"},
            ],
        }
        if overrides:
            _deep_merge(payload, deepcopy(overrides))

        config_path = tmp_path / "config.yml"
        config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
        config = load_index_config(config_path)
        service = IndexOrganizerService(config)
        services.append(service)
        return service, config, {
            "legacy_root": legacy_root,
            "watch_root": watch_root,
            "spaces_root": spaces_root,
            "history_root": history_root,
            "state_dir": state_dir,
        }

    yield _factory

    for service in services:
        service.close()
