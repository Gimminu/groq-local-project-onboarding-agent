# Groq Local Project Onboarding Agent

Groq API와 MCP를 결합해서 로컬 프로젝트를 빠르게 읽고, 새 팀원이 바로 이해할 수 있는 온보딩 보고서를 만들어주는 Python CLI 에이전트입니다.

이 프로젝트는 "단일 목적 에이전트" 과제에 맞춰 범위를 `로컬 프로젝트 온보딩`으로 고정했습니다.  
즉, 아무 자동화나 하는 범용 에이전트가 아니라 다음 한 가지 목적에 집중합니다.

- 프로젝트 폴더를 읽고 구조, 기술 스택, 실행법, 핵심 파일, 리스크를 정리한다.

## 과제 적합성

- 단일 목적이 명확합니다: `프로젝트 온보딩 보고서 생성`
- LLM 추론과 도구 호출이 함께 들어갑니다: `Groq + MCP`
- 실제 입력과 출력이 분명합니다
  - 입력: 프로젝트 폴더 경로 또는 프로젝트 분석 요청
  - 출력: Markdown/JSON 보고서

## 주요 기능

- 프로젝트 폴더 경로만 넣어도 온보딩 요청으로 자동 변환
- README, `requirements.txt`, `pyproject.toml`, `package.json`, `Makefile`, `Dockerfile`, `.env.example` 등 표준 파일 우선 탐색
- 기술 스택 요약
- 실행/테스트 명령 정리
- 먼저 읽어야 할 핵심 파일 추천
- 설정 위험 요소 및 미확인 지점 정리
- 인터랙티브 CLI
- 분석 로그를 Markdown/JSON으로 저장

## 폴더 구조

```text
groq-mcp-mac-agent/
├── app/
├── examples/
├── outputs/
├── samples/
├── tests/
├── .env.example
├── main.py
├── README.md
└── requirements.txt
```

## 설치

```bash
cd /Users/giminu0930/Desktop/groq-mcp-mac-agent
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
chmod +x main.py
```

`.env`에 Groq API 키를 넣습니다.

```env
GROQ_API_KEY=your_groq_api_key_here
```

기본 MCP 설정 파일은 `/Users/giminu0930/Desktop/mcp/.vscode/mcp.json` 입니다.

## 실행 방법

인터랙티브 CLI:

```bash
python3 main.py
```

현재 폴더를 바로 온보딩:

```bash
python3 main.py .
```

특정 프로젝트 경로를 바로 온보딩:

```bash
python3 main.py /Users/giminu0930/Desktop/groq-meeting-minutes-agent
```

자유 요청 실행:

```bash
python3 main.py "requirements.txt와 README를 읽고 실행 방법을 정리해줘"
```

표준 입력:

```bash
pbpaste | python3 main.py --stdin
```

## CLI 명령

- `/help`
- `/status`
- `/servers`
- `/tools`
- `/onboard <path>`
- `/stack <path>`
- `/runbook <path>`
- `/files <path>`
- `/risks <path>`
- `/sample`
- `/quit`

예시:

```text
/onboard ~/Desktop/groq-meeting-minutes-agent
/stack ~/Desktop/openai-realtime-transcribe
/runbook ~/Desktop/groq-debugging-helper-agent
/files ~/Desktop/mcp
/risks ~/Desktop/groq-mcp-mac-agent
```

## 출력물

각 실행 결과는 `outputs/` 아래에 저장됩니다.

- `project_onboarding_YYYYMMDD_HHMMSS.json`
- `project_onboarding_YYYYMMDD_HHMMSS.md`

Markdown은 사람이 읽는 보고서이고, JSON은 제출 자료나 후속 자동화에 쓰기 쉬운 실행 로그입니다.

## 샘플 자료

- 요청 예시: [samples/example_requests.md](/Users/giminu0930/Desktop/groq-mcp-mac-agent/samples/example_requests.md)
- 출력 예시 Markdown: [examples/sample_onboarding_report.md](/Users/giminu0930/Desktop/groq-mcp-mac-agent/examples/sample_onboarding_report.md)
- 출력 예시 JSON: [examples/sample_onboarding_report.json](/Users/giminu0930/Desktop/groq-mcp-mac-agent/examples/sample_onboarding_report.json)

## 테스트

```bash
python3 -m unittest discover -s tests -v
```

## 한계

- 현재는 `local-fs` 같은 로컬 파일 중심 MCP 서버에 최적화되어 있습니다.
- 잘못된 실행 명령을 만들지 않도록 파일 근거가 없는 내용은 일부러 비워둘 수 있습니다.
- 기본 모드는 `safe`라서 읽기 위주 분석에 맞춰져 있습니다.

## 제출 시 설명 예시

이 프로젝트는 로컬 프로젝트 폴더를 읽고, 새로운 팀원이 빠르게 적응할 수 있도록 기술 스택, 실행 방법, 핵심 파일, 리스크를 자동으로 정리하는 단일 목적 에이전트입니다. Groq API가 분석과 의사결정을 담당하고, MCP가 실제 파일 시스템 접근 도구를 제공해 단순 텍스트 생성이 아니라 도구 기반 분석 흐름을 보여줍니다.
