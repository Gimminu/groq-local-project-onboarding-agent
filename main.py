#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional during local inspection
    def load_dotenv(dotenv_path=None, override: bool = False) -> bool:
        candidate = Path(dotenv_path) if dotenv_path else Path(".env")
        if not candidate.exists():
            return False

        loaded = False
        for raw_line in candidate.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"'")
            if not key:
                continue
            if override or key not in os.environ:
                os.environ[key] = value
            loaded = True
        return loaded

from app.agent import AutomationAgent
from app.config import DEFAULT_MCP_CONFIG_PATH, ServerConfig, load_mcp_servers
from app.errors import AppError
from app.groq_planner import GroqPlanner
from app.policy import MODE_VALUES
from app.presets import build_preset_request, maybe_expand_directory_request
from app.rendering import write_trace_files
from app.schema import DEFAULT_MODEL

HELP_TEXT = """
명령어:
  /help               사용 가능한 명령어 보기
  /status             현재 서버, 모드, 모델 보기
  /servers            설정된 MCP 서버 목록 보기
  /tools              현재 서버에서 허용된 MCP 툴 보기
  /server <name>      사용할 MCP 서버 변경
  /mode <safe|write|full>
                      툴 실행 모드 변경
  /model <model_id>   Groq 모델 변경
  /onboard <path>     프로젝트 온보딩 보고서 생성
  /stack <path>       기술 스택과 의존성 파일 요약
  /runbook <path>     실행/테스트 명령과 환경 변수 힌트 정리
  /files <path>       먼저 읽어야 할 핵심 파일 추천
  /risks <path>       설정 위험 요소와 막힐 수 있는 지점 정리
  /sample             현재 프로젝트를 샘플로 분석
  /quit, /exit        CLI 종료

빠른 사용:
  - `python main.py`로 인터랙티브 CLI를 시작합니다.
  - `python main.py .` 처럼 폴더 경로만 주면 온보딩 요청으로 자동 변환됩니다.
  - `python main.py "/Users/.../my-project"`도 같은 방식으로 동작합니다.
  - `python main.py "requirements.txt를 읽고 실행법을 정리해줘"`처럼 자유 요청도 가능합니다.
  - `pbpaste | python main.py --stdin`으로 파이프 입력도 받을 수 있습니다.
  - 기본 MCP 설정 파일은 Desktop/mcp/.vscode/mcp.json 입니다.
""".strip()

SAMPLE_REQUEST = build_preset_request(
    "onboard",
    str(Path(__file__).resolve().parent),
)


def load_local_env() -> None:
    env_path = Path(__file__).with_name(".env")
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Groq와 MCP를 이용해 로컬 프로젝트 온보딩 보고서를 생성하는 CLI 에이전트"
    )
    parser.add_argument("request", nargs="?", help="실행할 자연어 요청")
    parser.add_argument(
        "--config",
        default=str(DEFAULT_MCP_CONFIG_PATH),
        help=f"MCP 설정 파일 경로 (기본값: {DEFAULT_MCP_CONFIG_PATH})",
    )
    parser.add_argument(
        "--server",
        default="local-fs",
        help="사용할 MCP 서버 이름 (기본값: local-fs)",
    )
    parser.add_argument(
        "--mode",
        default="safe",
        choices=MODE_VALUES,
        help="허용할 툴 실행 범위 (기본값: safe)",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Groq 모델 ID (기본값: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="실행 로그를 저장할 디렉터리 (기본값: outputs)",
    )
    parser.add_argument(
        "--stdin",
        action="store_true",
        help="표준 입력에서 자연어 요청을 읽기",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="인터랙티브 CLI 셸 시작",
    )
    parser.add_argument(
        "--list-servers",
        action="store_true",
        help="설정된 MCP 서버 목록만 출력하고 종료",
    )
    parser.add_argument(
        "--list-tools",
        action="store_true",
        help="현재 서버에서 허용된 툴 목록만 출력하고 종료",
    )
    return parser.parse_args(argv)


def get_servers(config_path: str) -> dict[str, ServerConfig]:
    return load_mcp_servers(Path(config_path))


def get_server(config_path: str, server_name: str) -> ServerConfig:
    servers = get_servers(config_path)
    try:
        return servers[server_name]
    except KeyError as exc:
        raise AppError(
            f"MCP 서버 `{server_name}`를 찾을 수 없습니다. "
            f"사용 가능한 서버: {', '.join(sorted(servers))}"
        ) from exc


def build_agent(model: str, require_planner: bool) -> AutomationAgent:
    planner = None
    if require_planner:
        planner = GroqPlanner(
            api_key=os.getenv("GROQ_API_KEY"),
            model=model,
        )
    return AutomationAgent(planner=planner)


def print_server_list(config_path: str) -> None:
    servers = get_servers(config_path)
    for name, server in sorted(servers.items()):
        args_preview = " ".join(server.args)
        print(f"{name}: {server.command} {args_preview}".strip())


def print_tool_list(config_path: str, server_name: str, mode: str) -> None:
    server_config = get_server(config_path, server_name)
    agent = build_agent(model=DEFAULT_MODEL, require_planner=False)
    tools = asyncio.run(agent.list_available_tools(server_config, mode))
    if not tools:
        raise AppError(f"{server_name} 서버에서 mode={mode}로 사용할 수 있는 tool이 없습니다.")
    for tool in tools:
        description = tool.description or "(설명 없음)"
        print(f"{tool.name}: {description}")


def run_request_once(
    request: str,
    *,
    config_path: str,
    server_name: str,
    mode: str,
    model: str,
    output_dir: Path,
) -> tuple[str, Path, Path]:
    server_config = get_server(config_path, server_name)
    agent = build_agent(model=model, require_planner=True)
    trace = asyncio.run(
        agent.execute_request(
            request=request,
            server_config=server_config,
            mode=mode,
            model=model,
        )
    )
    json_path, markdown_path = write_trace_files(trace=trace, output_dir=output_dir)
    return trace.final_answer, json_path, markdown_path


def resolve_request(args: argparse.Namespace) -> str | None:
    requested_sources = 0
    if args.request:
        requested_sources += 1
    if args.stdin:
        requested_sources += 1
    if args.interactive:
        if requested_sources:
            raise AppError("--interactive는 다른 요청 입력과 함께 사용할 수 없습니다.")
        return None
    if requested_sources > 1:
        raise AppError("요청 입력은 하나만 사용하세요.")
    if args.stdin:
        return sys.stdin.read().strip()
    return args.request.strip() if args.request else None


def run_interactive_shell(
    *,
    config_path: str,
    server_name: str,
    mode: str,
    model: str,
    output_dir: Path,
) -> int:
    current_config_path = config_path
    current_server_name = server_name
    current_mode = mode
    current_model = model
    current_output_dir = output_dir

    print("Groq Local Project Onboarding Agent")
    print(f"config={current_config_path}")
    print(f"server={current_server_name} mode={current_mode} model={current_model}")
    print("`/help`로 명령어를 확인하세요.")

    while True:
        try:
            raw = input("agent> ").strip()
        except EOFError:
            print()
            return 0
        except KeyboardInterrupt:
            print("\n종료합니다.")
            return 0

        if not raw:
            continue

        if raw.startswith("/"):
            should_exit, current_server_name, current_mode, current_model, current_output_dir = (
                handle_shell_command(
                    raw=raw,
                    config_path=current_config_path,
                    server_name=current_server_name,
                    mode=current_mode,
                    model=current_model,
                    output_dir=current_output_dir,
                )
            )
            if should_exit:
                return 0
            continue

        execute_and_print(
            request=raw,
            config_path=current_config_path,
            server_name=current_server_name,
            mode=current_mode,
            model=current_model,
            output_dir=current_output_dir,
        )


def handle_shell_command(
    *,
    raw: str,
    config_path: str,
    server_name: str,
    mode: str,
    model: str,
    output_dir: Path,
) -> tuple[bool, str, str, str, Path]:
    parts = raw.split(maxsplit=1)
    command = parts[0].lower()
    argument = parts[1].strip() if len(parts) > 1 else ""

    if command in {"/quit", "/exit"}:
        return True, server_name, mode, model, output_dir
    if command == "/help":
        print(HELP_TEXT)
        return False, server_name, mode, model, output_dir
    if command == "/status":
        print(f"config={config_path}")
        print(f"server={server_name}")
        print(f"mode={mode}")
        print(f"model={model}")
        print(f"output={output_dir}")
        return False, server_name, mode, model, output_dir
    if command == "/servers":
        try:
            print_server_list(config_path)
        except AppError as exc:
            print(f"오류: {exc}")
        return False, server_name, mode, model, output_dir
    if command == "/tools":
        try:
            print_tool_list(config_path, server_name, mode)
        except AppError as exc:
            print(f"오류: {exc}")
        return False, server_name, mode, model, output_dir
    if command == "/server":
        if not argument:
            print("사용법: /server <name>")
            return False, server_name, mode, model, output_dir
        try:
            get_server(config_path, argument)
        except AppError as exc:
            print(f"오류: {exc}")
            return False, server_name, mode, model, output_dir
        print(f"server 변경: {server_name} -> {argument}")
        return False, argument, mode, model, output_dir
    if command == "/mode":
        if argument not in MODE_VALUES:
            print(f"사용법: /mode <{'|'.join(MODE_VALUES)}>")
            return False, server_name, mode, model, output_dir
        print(f"mode 변경: {mode} -> {argument}")
        return False, server_name, argument, model, output_dir
    if command == "/model":
        if not argument:
            print("사용법: /model <model_id>")
            return False, server_name, mode, model, output_dir
        print(f"model 변경: {model} -> {argument}")
        return False, server_name, mode, argument, output_dir
    if command == "/sample":
        execute_and_print(
            request=SAMPLE_REQUEST,
            config_path=config_path,
            server_name=server_name,
            mode=mode,
            model=model,
            output_dir=output_dir,
        )
        return False, server_name, mode, model, output_dir
    if command in {"/onboard", "/stack", "/runbook", "/files", "/risks"}:
        if not argument:
            preset_name = command.removeprefix("/")
            print(f"사용법: {command} <project_path>")
            print(f"예시: {command} ~/Desktop/{preset_name}-demo")
            return False, server_name, mode, model, output_dir
        try:
            request = build_preset_request(command.removeprefix("/"), argument)
            execute_and_print(
                request=request,
                config_path=config_path,
                server_name=server_name,
                mode=mode,
                model=model,
                output_dir=output_dir,
            )
        except AppError as exc:
            print(f"오류: {exc}")
        return False, server_name, mode, model, output_dir

    print("알 수 없는 명령어입니다. `/help`를 확인하세요.")
    return False, server_name, mode, model, output_dir


def execute_and_print(
    *,
    request: str,
    config_path: str,
    server_name: str,
    mode: str,
    model: str,
    output_dir: Path,
) -> int:
    try:
        final_answer, json_path, markdown_path = run_request_once(
            request=request,
            config_path=config_path,
            server_name=server_name,
            mode=mode,
            model=model,
            output_dir=output_dir,
        )
    except AppError as exc:
        print(f"오류: {exc}")
        return 1

    print(final_answer)
    print(f"saved json: {json_path}")
    print(f"saved md:   {markdown_path}")
    return 0


def run(argv: list[str] | None = None) -> int:
    load_local_env()
    args = parse_args(argv)

    try:
        if args.list_servers:
            print_server_list(args.config)
            return 0

        if args.list_tools:
            print_tool_list(args.config, args.server, args.mode)
            return 0

        request = resolve_request(args)
        if request is None:
            if args.interactive or sys.stdin.isatty():
                return run_interactive_shell(
                    config_path=args.config,
                    server_name=args.server,
                    mode=args.mode,
                    model=args.model,
                    output_dir=Path(args.output_dir),
                )

            piped_request = sys.stdin.read().strip()
            if not piped_request:
                raise AppError("표준 입력으로 전달된 요청이 비어 있습니다.")
            request = piped_request

        if not request:
            raise AppError("실행할 요청이 비어 있습니다.")

        request = maybe_expand_directory_request(request)

        return execute_and_print(
            request=request,
            config_path=args.config,
            server_name=args.server,
            mode=args.mode,
            model=args.model,
            output_dir=Path(args.output_dir),
        )
    except AppError as exc:
        print(f"오류: {exc}", file=sys.stderr)
        return 1


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
