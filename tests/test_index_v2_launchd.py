from __future__ import annotations

from pathlib import Path

import app.index_v2.launchd as launchd_module
from app.index_v2.config import load_index_config
from app.index_v2.launchd import build_launch_agent_plist, ensure_user_config, resolve_launchd_python_executable
from app.index_v2.types import IndexOrganizerConfig


def _build_config(tmp_path: Path) -> IndexOrganizerConfig:
    return IndexOrganizerConfig(
        config_path=tmp_path / "config.yml",
        spaces_root=tmp_path / "spaces",
        history_root=tmp_path / "History",
        state_dir=tmp_path / "state",
    )


def test_service_plist_uses_long_running_service_run(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    config.ensure_directories()

    plist = build_launch_agent_plist(
        label_prefix="com.example.index-organizer.v2",
        mode="service",
        repo_root=Path("/repo"),
        python_executable="/usr/bin/python3",
        config_path=Path("/tmp/folder-organizer-v2.yml"),
        config=config,
    )

    assert plist["Label"] == "com.example.index-organizer.v2.service"
    assert plist["ProgramArguments"][2:] == ["service-run", "--config", "/tmp/folder-organizer-v2.yml", "--apply"]
    assert plist["RunAtLoad"] is True
    assert plist["KeepAlive"] is True
    assert "StartInterval" not in plist


def test_load_index_config_defaults_history_root_inside_state_dir(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        f"spaces_root: {tmp_path / 'Documents'}\nstate_dir: {tmp_path / 'state'}\n",
        encoding="utf-8",
    )

    config = load_index_config(config_path)

    assert config.history_root == config.state_dir / "history"


def test_unsupported_launchd_mode_raises(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    config.ensure_directories()

    try:
        build_launch_agent_plist(
            label_prefix="com.example.index-organizer.v2",
            mode="archive",
            repo_root=Path("/repo"),
            python_executable="/usr/bin/python3",
            config_path=Path("/tmp/folder-organizer-v2.yml"),
            config=config,
        )
    except ValueError as exc:
        assert "unsupported" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected ValueError")


def test_ensure_user_config_copies_sample_when_missing(tmp_path: Path) -> None:
    sample = tmp_path / "sample.yml"
    destination = tmp_path / "folder-organizer-v2.yml"
    sample.write_text("spaces_root: ~/Documents\n", encoding="utf-8")

    written = ensure_user_config(destination, sample)

    assert written == destination
    assert destination.exists()
    assert destination.read_text(encoding="utf-8") == sample.read_text(encoding="utf-8")


def test_resolve_launchd_python_executable_prefers_existing_stable_candidate(tmp_path: Path) -> None:
    missing = tmp_path / "missing-python"
    stable = tmp_path / "python3.13"
    stable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    stable.chmod(0o755)

    selected = resolve_launchd_python_executable(
        current_executable=str(missing),
        candidates=(str(missing), str(stable)),
    )

    assert selected == str(stable)


def test_install_launch_agents_reloads_running_service_even_when_plist_is_unchanged(tmp_path: Path, monkeypatch) -> None:
    config = _build_config(tmp_path)
    config.ensure_directories()
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(launchd_module, "uninstall_legacy_launch_agents", lambda: [])
    monkeypatch.setattr(launchd_module, "uninstall_previous_v2_launch_agents", lambda _label_prefix: [])
    monkeypatch.setattr(launchd_module, "_plist_matches_existing", lambda _plist_path, _payload: True)

    load_calls: list[tuple[str, Path]] = []
    monkeypatch.setattr(launchd_module, "_load_agent", lambda label, plist_path: load_calls.append((label, plist_path)))

    installed = launchd_module.install_launch_agents(
        label_prefix="com.example.index-organizer.v2",
        repo_root=Path("/repo"),
        python_executable="/usr/bin/python3",
        config_path=Path("/tmp/folder-organizer-v2.yml"),
        config=config,
    )

    plist_path = tmp_path / "Library" / "LaunchAgents" / "com.example.index-organizer.v2.service.plist"
    assert installed == {"service": plist_path}
    assert load_calls == [("com.example.index-organizer.v2.service", plist_path)]
