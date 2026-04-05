from __future__ import annotations

import os
import plistlib
import subprocess
from pathlib import Path

from app.organizer_types import OrganizerConfig

DEFAULT_LAUNCHD_LABEL = "com.groqmcp.folder-organizer.watch"
STANDARD_SCOPE = "standard"


def build_launch_agent_plist(
    *,
    label_prefix: str,
    mode: str,
    config: OrganizerConfig,
    repo_root: Path,
    python_executable: str,
    source_argument: str | None,
    profile: str | None,
    mcp_config_path: Path,
    daily_minute: int = 0,
    command_name: str | None = None,
    watch_paths: list[str] | None = None,
    run_at_load: bool = False,
) -> dict:
    label = f"{label_prefix}.{mode}"
    organizer_script = repo_root / "organizer.py"
    output_dir = _resolve_output_dir(repo_root, config.output_dir)

    program_arguments = [
        python_executable,
        str(organizer_script),
        command_name or ("watch-once" if mode == "watch" else "daily"),
    ]
    if source_argument:
        program_arguments.append(source_argument)
    elif profile:
        program_arguments.extend(["--profile", profile])

    common_args = [
        "--provider",
        config.provider,
        "--rename-mode",
        config.rename_mode,
        "--project-mode",
        config.project_mode,
        "--max-depth",
        str(config.max_depth),
        "--sample-limit",
        str(config.sample_limit),
        "--active-window-days",
        str(config.active_window_days),
        "--stale-project-days",
        str(config.stale_project_days),
        "--watch-interval",
        str(config.watch_interval_seconds),
        "--output-dir",
        str(config.output_dir),
        "--mcp-config",
        str(mcp_config_path),
        "--mcp-server",
        "local-fs",
    ]
    if config.target_root != config.source_root:
        common_args.extend(["--target-root", str(config.target_root)])
    if config.model:
        common_args.extend(["--model", config.model])
    if config.ollama_url:
        common_args.extend(["--ollama-url", config.ollama_url])
    if config.allow_project_root:
        common_args.append("--allow-project-root")
    if config.enable_llm_for_known_types:
        common_args.append("--enable-llm-for-known-types")
    common_args.extend(["--min-age-seconds", str(config.min_age_seconds)])
    program_arguments.extend(common_args)

    plist = {
        "Label": label,
        "ProgramArguments": program_arguments,
        "WorkingDirectory": str(repo_root),
        "StandardOutPath": str(output_dir / f"{label}.out.log"),
        "StandardErrorPath": str(output_dir / f"{label}.err.log"),
        "ProcessType": "Background",
        "RunAtLoad": run_at_load,
    }

    if mode == "watch":
        plist["WatchPaths"] = watch_paths or [str(config.source_root)]
        plist["ThrottleInterval"] = max(5, min(30, config.watch_interval_seconds))
    elif mode == "daily":
        plist["StartCalendarInterval"] = {"Hour": 9, "Minute": daily_minute}
    else:  # pragma: no cover - defensive guard
        raise ValueError(f"unsupported launchd mode: {mode}")
    return plist


def install_launch_agents(
    *,
    label_prefix: str,
    config: OrganizerConfig,
    repo_root: Path,
    python_executable: str,
    source_argument: str | None,
    profile: str,
    mcp_config_path: Path,
    daily_minute: int = 0,
) -> dict[str, Path]:
    launch_agents_dir = Path.home() / "Library" / "LaunchAgents"
    launch_agents_dir.mkdir(parents=True, exist_ok=True)
    installed: dict[str, Path] = {}

    _remove_legacy_reconcile_agent(label_prefix)
    for mode in ("watch", "daily"):
        plist_path = launch_agents_dir / f"{label_prefix}.{mode}.plist"
        payload = build_launch_agent_plist(
            label_prefix=label_prefix,
            mode=mode,
            config=config,
            repo_root=repo_root,
            python_executable=python_executable,
            source_argument=source_argument,
            profile=profile,
            mcp_config_path=mcp_config_path,
            daily_minute=daily_minute,
            run_at_load=False,
        )
        _resolve_output_dir(repo_root, config.output_dir).mkdir(parents=True, exist_ok=True)
        if _plist_matches_existing(plist_path, payload) and _label_loaded(payload["Label"]):
            installed[mode] = plist_path
            continue
        plist_path.write_bytes(plistlib.dumps(payload))
        _load_agent(payload["Label"], plist_path)
        installed[mode] = plist_path
    return installed


def uninstall_launch_agents(label_prefix: str) -> list[Path]:
    launch_agents_dir = Path.home() / "Library" / "LaunchAgents"
    removed: list[Path] = []
    for mode in ("watch", "daily"):
        plist_path = launch_agents_dir / f"{label_prefix}.{mode}.plist"
        label = f"{label_prefix}.{mode}"
        if plist_path.exists():
            _unload_agent(label, plist_path)
            plist_path.unlink(missing_ok=True)
            removed.append(plist_path)
    removed.extend(_remove_legacy_reconcile_agent(label_prefix))
    return removed


def install_standard_launch_agents(
    *,
    label_prefix: str,
    configs_by_profile: dict[str, OrganizerConfig],
    watch_profiles: tuple[str, ...] | list[str],
    repo_root: Path,
    python_executable: str,
    mcp_config_path: Path,
    daily_minute: int = 0,
) -> dict[str, Path]:
    launch_agents_dir = Path.home() / "Library" / "LaunchAgents"
    launch_agents_dir.mkdir(parents=True, exist_ok=True)
    installed: dict[str, Path] = {}

    standard_label_prefix = f"{label_prefix}.{STANDARD_SCOPE}"
    reference_config = next(iter(configs_by_profile.values()))
    watch_paths = [str(configs_by_profile[profile].source_root) for profile in watch_profiles]

    for mode, command_name in (("watch", "watch-standard-once"), ("daily", "daily-standard")):
        plist_path = launch_agents_dir / f"{standard_label_prefix}.{mode}.plist"
        payload = build_launch_agent_plist(
            label_prefix=standard_label_prefix,
            mode=mode,
            config=reference_config,
            repo_root=repo_root,
            python_executable=python_executable,
            source_argument=None,
            profile=None,
            mcp_config_path=mcp_config_path,
            daily_minute=daily_minute,
            command_name=command_name,
            watch_paths=watch_paths if mode == "watch" else None,
            run_at_load=False,
        )
        _resolve_output_dir(repo_root, reference_config.output_dir).mkdir(parents=True, exist_ok=True)
        if _plist_matches_existing(plist_path, payload) and _label_loaded(payload["Label"]):
            installed[mode] = plist_path
            continue
        plist_path.write_bytes(plistlib.dumps(payload))
        _load_agent(payload["Label"], plist_path)
        installed[mode] = plist_path
    return installed


def _remove_legacy_reconcile_agent(label_prefix: str) -> list[Path]:
    launch_agents_dir = Path.home() / "Library" / "LaunchAgents"
    removed: list[Path] = []
    label = f"{label_prefix}.reconcile"
    plist_path = launch_agents_dir / f"{label}.plist"
    if plist_path.exists():
        _unload_agent(label, plist_path)
        plist_path.unlink(missing_ok=True)
        removed.append(plist_path)
    return removed


def _load_agent(label: str, plist_path: Path) -> None:
    domain = f"gui/{os.getuid()}"
    _launchctl(["bootout", domain, str(plist_path)], allow_failure=True)
    _launchctl(["bootstrap", domain, str(plist_path)])
    _launchctl(["enable", f"{domain}/{label}"], allow_failure=True)


def _unload_agent(label: str, plist_path: Path) -> None:
    domain = f"gui/{os.getuid()}"
    _launchctl(["bootout", domain, str(plist_path)], allow_failure=True)
    _launchctl(["disable", f"{domain}/{label}"], allow_failure=True)


def _launchctl(args: list[str], allow_failure: bool = False) -> None:
    completed = subprocess.run(
        ["launchctl", *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if completed.returncode != 0 and not allow_failure:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or "launchctl failed")


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


def _resolve_output_dir(repo_root: Path, output_dir: Path) -> Path:
    candidate = output_dir.expanduser()
    if candidate.is_absolute():
        return candidate
    return (repo_root / candidate).resolve()
