from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import index_organizer


def test_operation_state_reports_config_warning_when_watch_roots_invalid() -> None:
    summary = {"move": 0, "rename": 0, "archive": 0, "quarantine": 0, "flag_for_review": 0}

    state = index_organizer._operation_state(summary, watch_root_issues=1)

    assert state == "CONFIG_WARNING"


def test_watch_root_health_detects_missing_and_non_directory(tmp_path: Path) -> None:
    existing = tmp_path / "existing"
    existing.mkdir()
    missing = tmp_path / "missing"
    file_path = tmp_path / "not-dir"
    file_path.write_text("x", encoding="utf-8")

    config = SimpleNamespace(watch_roots=(existing, missing, file_path))

    health = index_organizer._watch_root_health(config)

    assert health["total"] == 3
    assert existing in health["existing"]
    assert missing in health["missing"]
    assert file_path in health["not_directories"]
