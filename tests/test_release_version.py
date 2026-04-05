from __future__ import annotations

from pathlib import Path

from scripts.release_version import (
    bump_version,
    ensure_changelog_entry,
    parse_version,
    replace_version,
)


def test_parse_version_reads_semver() -> None:
    payload = """
[project]
name = "demo"
version = "1.2.3"
"""
    assert parse_version(payload) == (1, 2, 3)


def test_bump_version_parts() -> None:
    assert bump_version((1, 2, 3), "patch") == (1, 2, 4)
    assert bump_version((1, 2, 3), "minor") == (1, 3, 0)
    assert bump_version((1, 2, 3), "major") == (2, 0, 0)


def test_replace_version_updates_payload() -> None:
    payload = """
[project]
name = "demo"
version = "0.1.0"
"""
    updated = replace_version(payload, "0.1.1")
    assert 'version = "0.1.1"' in updated
    assert 'version = "0.1.0"' not in updated


def test_ensure_changelog_entry_is_idempotent(tmp_path: Path) -> None:
    changelog = tmp_path / "CHANGELOG.md"

    first = ensure_changelog_entry(changelog, "0.2.0", entry_date="2026-04-05")
    second = ensure_changelog_entry(changelog, "0.2.0", entry_date="2026-04-05")

    text = changelog.read_text(encoding="utf-8")
    assert first is True
    assert second is False
    assert text.count("## v0.2.0 - 2026-04-05") == 1
