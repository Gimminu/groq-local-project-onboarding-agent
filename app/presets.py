from __future__ import annotations

from pathlib import Path

from app.errors import AppError


def normalize_project_path(raw_path: str) -> Path:
    candidate = Path(raw_path.strip()).expanduser()
    try:
        resolved = candidate.resolve()
    except FileNotFoundError:
        resolved = candidate

    if not str(raw_path).strip():
        raise AppError("프로젝트 경로가 비어 있습니다.")
    if not resolved.exists():
        raise AppError(f"프로젝트 경로를 찾을 수 없습니다: {raw_path}")
    if not resolved.is_dir():
        raise AppError(f"프로젝트 경로는 폴더여야 합니다: {resolved}")
    return resolved


def maybe_expand_directory_request(request: str) -> str:
    stripped = request.strip()
    if not stripped:
        return stripped

    try:
        path = normalize_project_path(stripped)
    except AppError:
        return stripped

    return build_preset_request("onboard", str(path))


def build_preset_request(preset_name: str, project_path: str) -> str:
    resolved_path = normalize_project_path(project_path)
    path_text = str(resolved_path)

    if preset_name == "onboard":
        return "\n".join(
            [
                f"프로젝트 경로: {path_text}",
                "이 프로젝트를 처음 받는 팀원을 위한 온보딩 보고서를 작성해줘.",
                "반드시 다음 항목을 포함해줘:",
                "1. 프로젝트 한 줄 소개",
                "2. 기술 스택과 근거가 되는 파일",
                "3. 실행/테스트 방법",
                "4. 먼저 읽어야 할 핵심 파일 5개와 이유",
                "5. 설정 시 막힐 수 있는 지점이나 미확인 사항",
                "README, requirements.txt, pyproject.toml, package.json, Makefile, Dockerfile, .env.example 같은 표준 파일을 우선 탐색하고, 없는 내용은 추측하지 말아줘.",
            ]
        )

    if preset_name == "stack":
        return "\n".join(
            [
                f"프로젝트 경로: {path_text}",
                "이 프로젝트의 기술 스택을 정리해줘.",
                "언어, 프레임워크, 패키지 관리자, 배포/실행 관련 파일을 파일 근거와 함께 정리하고 추측은 하지 말아줘.",
            ]
        )

    if preset_name == "runbook":
        return "\n".join(
            [
                f"프로젝트 경로: {path_text}",
                "이 프로젝트를 처음 실행하는 사람을 위한 runbook을 작성해줘.",
                "설치 명령, 실행 명령, 테스트 명령, 필요한 환경 변수 파일 여부를 정리하고, 문서나 설정 파일에 근거가 없는 명령은 만들지 말아줘.",
            ]
        )

    if preset_name == "files":
        return "\n".join(
            [
                f"프로젝트 경로: {path_text}",
                "이 프로젝트에서 먼저 읽어야 할 핵심 파일 5개를 추천해줘.",
                "각 파일마다 왜 중요한지 한 줄씩 설명하고, 가능하면 README와 엔트리포인트를 우선 포함해줘.",
            ]
        )

    if preset_name == "risks":
        return "\n".join(
            [
                f"프로젝트 경로: {path_text}",
                "이 프로젝트를 셋업하거나 이해할 때 막힐 수 있는 위험 요소를 정리해줘.",
                "누락된 환경 변수, 문서 부족, 실행 경로 불명확성, 테스트 부재 같은 항목을 파일 근거와 함께 정리하고 과장하지 말아줘.",
            ]
        )

    raise AppError(f"지원하지 않는 preset입니다: {preset_name}")
