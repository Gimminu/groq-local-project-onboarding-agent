from __future__ import annotations

import os
import plistlib
import shutil
import subprocess
from pathlib import Path

from app.errors import AppError
from app.index_v2.types import IndexOrganizerConfig

DEFAULT_V2_LAUNCHD_LABEL = "com.groqmcp.index-organizer.v2"
LEGACY_LAUNCHD_PREFIXES = (
    "com.groqmcp.folder-organizer.watch",
    "com.groqmcp.folder-organizer.watch.standard",
)
PREVIOUS_V2_MODES = ("watch", "audit", "archive")
CURRENT_V2_MODE = "service"


def ensure_user_config(config_path: Path, sample_config_path: Path) -> Path:
    destination = config_path.expanduser().absolute()
    if destination.exists():
        return destination
    if not sample_config_path.exists():
        raise AppError(f"샘플 설정 파일을 찾을 수 없습니다: {sample_config_path}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(sample_config_path, destination)
    return destination


def build_launch_agent_plist(
    *,
    label_prefix: str,
    mode: str,
    repo_root: Path,
    python_executable: str,
    config_path: Path,
    config: IndexOrganizerConfig,
) -> dict:
    label = f"{label_prefix}.{mode}"
    organizer_script = repo_root / "organizer.py"
    log_dir = config.service_logs_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    if mode == CURRENT_V2_MODE:
        program_arguments = [
            python_executable,
            str(organizer_script),
            "service-run",
            "--config",
            str(config_path),
            "--apply",
        ]
        return {
            "Label": label,
            "ProgramArguments": program_arguments,
            "WorkingDirectory": str(repo_root),
            "StandardOutPath": str(log_dir / f"{label}.out.log"),
            "StandardErrorPath": str(log_dir / f"{label}.err.log"),
            "ProcessType": "Background",
            "RunAtLoad": True,
            "KeepAlive": True,
        }
    raise ValueError(f"unsupported V2 launchd mode: {mode}")


def resolve_launchd_python_executable(
    *,
    current_executable: str | None = None,
    candidates: tuple[str, ...] | None = None,
) -> str:
    preferred = candidates or (
        "/opt/homebrew/bin/python3.13",
        "/opt/homebrew/bin/python3",
        "/usr/local/bin/python3.13",
        "/usr/local/bin/python3",
        "/usr/bin/python3",
        current_executable or "",
        shutil.which("python3") or "",
    )
    for raw_candidate in preferred:
        if not raw_candidate:
            continue
        candidate = Path(raw_candidate).expanduser()
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    raise AppError("launchd 서비스에 사용할 Python 실행 파일을 찾을 수 없습니다.")


def install_launch_agents(
    *,
    label_prefix: str,
    repo_root: Path,
    python_executable: str,
    config_path: Path,
    config: IndexOrganizerConfig,
) -> dict[str, Path]:
    uninstall_legacy_launch_agents()
    uninstall_previous_v2_launch_agents(label_prefix)
    launch_agents_dir = Path.home() / "Library" / "LaunchAgents"
    launch_agents_dir.mkdir(parents=True, exist_ok=True)

    plist_path = launch_agents_dir / f"{label_prefix}.{CURRENT_V2_MODE}.plist"
    payload = build_launch_agent_plist(
        label_prefix=label_prefix,
        mode=CURRENT_V2_MODE,
        repo_root=repo_root,
        python_executable=python_executable,
        config_path=config_path,
        config=config,
    )
    if not _plist_matches_existing(plist_path, payload):
        plist_path.write_bytes(plistlib.dumps(payload))
    _load_agent(payload["Label"], plist_path)
    return {CURRENT_V2_MODE: plist_path}


def uninstall_launch_agents(label_prefix: str = DEFAULT_V2_LAUNCHD_LABEL) -> list[Path]:
    removed = uninstall_previous_v2_launch_agents(label_prefix)
    removed.extend(uninstall_legacy_launch_agents())
    return removed


def uninstall_previous_v2_launch_agents(label_prefix: str = DEFAULT_V2_LAUNCHD_LABEL) -> list[Path]:
    launch_agents_dir = Path.home() / "Library" / "LaunchAgents"
    removed: list[Path] = []
    for mode in (*PREVIOUS_V2_MODES, CURRENT_V2_MODE):
        plist_path = launch_agents_dir / f"{label_prefix}.{mode}.plist"
        label = f"{label_prefix}.{mode}"
        if plist_path.exists():
            _unload_agent(label, plist_path)
            plist_path.unlink(missing_ok=True)
            removed.append(plist_path)
    return removed


def uninstall_legacy_launch_agents() -> list[Path]:
    launch_agents_dir = Path.home() / "Library" / "LaunchAgents"
    removed: list[Path] = []
    for prefix in LEGACY_LAUNCHD_PREFIXES:
        for suffix in ("watch", "daily", "reconcile"):
            label = f"{prefix}.{suffix}"
            plist_path = launch_agents_dir / f"{label}.plist"
            if not plist_path.exists():
                continue
            _unload_agent(label, plist_path)
            plist_path.unlink(missing_ok=True)
            removed.append(plist_path)
    return removed


def service_status(label_prefix: str = DEFAULT_V2_LAUNCHD_LABEL) -> dict[str, dict[str, object]]:
    launch_agents_dir = Path.home() / "Library" / "LaunchAgents"
    status: dict[str, dict[str, object]] = {}
    for mode in (CURRENT_V2_MODE,):
        label = f"{label_prefix}.{mode}"
        plist_path = launch_agents_dir / f"{label}.plist"
        status[mode] = {
            "label": label,
            "plist_path": str(plist_path),
            "exists": plist_path.exists(),
            "loaded": _label_loaded(label),
        }
    status["previous_v2"] = {
        "labels": [f"{label_prefix}.{mode}" for mode in PREVIOUS_V2_MODES],
        "loaded": [f"{label_prefix}.{mode}" for mode in PREVIOUS_V2_MODES if _label_loaded(f"{label_prefix}.{mode}")],
    }
    status["legacy"] = {
        "labels": [f"{prefix}.{suffix}" for prefix in LEGACY_LAUNCHD_PREFIXES for suffix in ("watch", "daily")],
        "loaded": [label for label in [f"{prefix}.{suffix}" for prefix in LEGACY_LAUNCHD_PREFIXES for suffix in ("watch", "daily")] if _label_loaded(label)],
    }
    return status


def _load_agent(label: str, plist_path: Path) -> None:
    domain = f"gui/{os.getuid()}"
    _launchctl(["enable", f"{domain}/{label}"], allow_failure=True)
    _launchctl(["bootout", domain, str(plist_path)], allow_failure=True)
    _launchctl(["bootstrap", domain, str(plist_path)])


def _unload_agent(label: str, plist_path: Path) -> None:
    domain = f"gui/{os.getuid()}"
    _launchctl(["bootout", domain, str(plist_path)], allow_failure=True)


def _launchctl(args: list[str], allow_failure: bool = False) -> None:
    completed = subprocess.run(
        ["launchctl", *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if completed.returncode != 0 and not allow_failure:
        raise AppError(completed.stderr.strip() or completed.stdout.strip() or "launchctl failed")


def _label_loaded(label: str) -> bool:
    domain = f"gui/{os.getuid()}"
    completed = subprocess.run(
        ["launchctl", "print", f"{domain}/{label}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return completed.returncode == 0


def _plist_matches_existing(plist_path: Path, payload: dict) -> bool:
    if not plist_path.exists():
        return False
    try:
        existing = plistlib.loads(plist_path.read_bytes())
    except Exception:
        return False
    return existing == payload
