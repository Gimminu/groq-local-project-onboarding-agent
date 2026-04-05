from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from app.errors import AppError


def get_default_mcp_config_path() -> Path:
    env_override = os.getenv("MCP_CONFIG_PATH")
    candidates: list[Path] = []
    if env_override:
        candidates.append(Path(env_override).expanduser())

    repo_root = Path(__file__).resolve().parents[2]
    candidates.extend(
        [
            repo_root / "mcp" / ".vscode" / "mcp.json",
            repo_root.parent / "mcp" / ".vscode" / "mcp.json",
            Path.home()
            / "Documents"
            / "projects"
            / "apps"
            / "mcp"
            / ".vscode"
            / "mcp.json",
            Path.home()
            / "Documents"
            / "projects"
            / "legacy-review"
            / "mcp-legacy-organizer"
            / "code"
            / "mcp"
            / ".vscode"
            / "mcp.json",
        ]
    )

    for candidate in candidates:
        expanded = candidate.expanduser()
        if expanded.exists():
            return expanded.resolve()

    # Return primary expected location if none exist, so caller gets a clear error message.
    return (repo_root / "mcp" / ".vscode" / "mcp.json").resolve()


DEFAULT_MCP_CONFIG_PATH = get_default_mcp_config_path()


@dataclass(frozen=True)
class ServerConfig:
    name: str
    command: str
    args: list[str]
    env: dict[str, str]


def load_mcp_servers(config_path: Path) -> dict[str, ServerConfig]:
    if not config_path.exists():
        raise AppError(f"MCP 설정 파일을 찾을 수 없습니다: {config_path}")

    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise AppError(f"MCP 설정 파일 JSON 파싱에 실패했습니다: {config_path}") from exc

    servers = payload.get("servers")
    if not isinstance(servers, dict) or not servers:
        raise AppError("MCP 설정 파일에 사용할 수 있는 servers 항목이 없습니다.")

    workspace_root = _workspace_root(config_path)
    config_dir = config_path.parent
    resolved: dict[str, ServerConfig] = {}
    default_env = {
        "GRPC_VERBOSITY": os.getenv("GRPC_VERBOSITY", "NONE"),
        "GLOG_minloglevel": os.getenv("GLOG_minloglevel", "3"),
        "MCP_QUIET_LOGS": os.getenv("MCP_QUIET_LOGS", "1"),
    }

    for name, raw_server in servers.items():
        if not isinstance(raw_server, dict):
            raise AppError(f"서버 설정 형식이 올바르지 않습니다: {name}")

        command = raw_server.get("command")
        if not isinstance(command, str) or not command.strip():
            raise AppError(f"서버 `{name}`에 command가 없습니다.")

        raw_args = raw_server.get("args", [])
        if raw_args is None:
            raw_args = []
        if not isinstance(raw_args, list) or not all(isinstance(arg, str) for arg in raw_args):
            raise AppError(f"서버 `{name}`의 args는 문자열 배열이어야 합니다.")

        raw_env = raw_server.get("env", {})
        if raw_env is None:
            raw_env = {}
        if not isinstance(raw_env, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in raw_env.items()
        ):
            raise AppError(f"서버 `{name}`의 env는 문자열 딕셔너리여야 합니다.")

        resolved[name] = ServerConfig(
            name=name,
            command=_resolve_executable_path(command, workspace_root, config_dir),
            args=[
                _resolve_arg_path(arg, workspace_root, config_dir)
                for arg in raw_args
            ],
            env={**os.environ, **default_env, **raw_env},
        )

    return resolved


def _workspace_root(config_path: Path) -> Path:
    if config_path.parent.name == ".vscode":
        return config_path.parent.parent
    return config_path.parent


def _resolve_executable_path(command: str, workspace_root: Path, config_dir: Path) -> str:
    if "/" not in command and "\\" not in command:
        return command

    candidate = Path(command).expanduser()
    if candidate.is_absolute():
        return str(candidate)

    for base in (workspace_root, config_dir):
        path_candidate = (base / candidate).resolve()
        if path_candidate.exists():
            return str(path_candidate)
    return command


def _resolve_arg_path(arg: str, workspace_root: Path, config_dir: Path) -> str:
    candidate = Path(arg).expanduser()
    if candidate.is_absolute():
        return str(candidate)
    for base in (workspace_root, config_dir):
        path_candidate = (base / candidate).resolve()
        if path_candidate.exists():
            return str(path_candidate)
    return arg
