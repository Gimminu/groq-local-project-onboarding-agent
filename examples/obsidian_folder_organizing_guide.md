# Obsidian Documents Folder Organizing Guide

이 가이드는 Obsidian 볼트 루트는 유지하면서, 점진적으로 Documents 기반 자동 정리를 적용하는 방법을 설명합니다.

## 1) 현재 운영 원칙

- 기존 볼트를 `/Users/giminu0930/Documents/Obsidian` 경로로 통합합니다.
- 자동 정리 대상 저장소는 `/Users/giminu0930/Documents/Obsidian/Organizer`입니다.
- staging 투입 경로는 `/Users/giminu0930/Documents/Obsidian/Organizer/inbox`입니다.
- 기존 노트 폴더(예: Calendar, Dashboard, Hubs, Tags, Templates)는 외부 터미널 이동 대신 Obsidian 앱 내부 이동을 권장합니다.
- 자동 이관은 우선 안전 파일(압축/문서/미디어 등)부터 시작합니다.

## 2) 권장 최상위 구조

`/Users/giminu0930/Documents/Obsidian/Organizer`

- `inbox` : 수동 분류 대기
- `projects` : 프로젝트 코드/산출물
- `areas` : 생활/행정/금융 등 운영 문서
- `resources` : 참고자료, 학습자료
- `archive` : 장기 보관
- `review` : 불확실 항목
- `system` : 시스템성/예외성 항목

## 3) 점진 이관 순서

1. 루트의 안전 파일만 staging(`/Users/giminu0930/Documents/Obsidian/Organizer/inbox`)으로 이동
2. organizer를 `plan -> apply -> status` 순서로 실행
3. `review`에 남은 항목만 수동 확인
4. 노트 폴더 대량 이동은 나중에 Obsidian 앱에서 천천히 수행

## 4) 자주 쓰는 명령

환경: `/Users/giminu0930/projects/workspace/groq-mcp-mac-agent/code`

```bash
./.venv/bin/python scripts/obsidian_stage_move.py
./.venv/bin/python scripts/obsidian_stage_move.py --apply

./.venv/bin/python index_organizer.py plan --config samples/index_organizer_v2.obsidian_documents.yml
./.venv/bin/python index_organizer.py apply --config samples/index_organizer_v2.obsidian_documents.yml --apply
./.venv/bin/python index_organizer.py status --config samples/index_organizer_v2.obsidian_documents.yml --review-limit 20

./.venv/bin/python index_organizer.py stabilize --config samples/index_organizer_v2.obsidian_documents.yml --apply

./.venv/bin/python index_organizer.py service-install --config samples/index_organizer_v2.obsidian_documents.yml
./.venv/bin/python index_organizer.py service-status

./.venv/bin/python scripts/obsidian_link_audit.py
./.venv/bin/python scripts/obsidian_link_audit.py --strict
```

## 5) 운영 팁

- review가 늘어나면 먼저 `stabilize --apply`를 1회 실행합니다.
- 이 프로필은 LLM-first(`llm.enable_llm_first=true`)와 watch LLM(`llm.enable_for_watch=true`)이 켜져 있으므로 provider 쿨다운/요청량을 함께 모니터링합니다.
- 서비스 전환 전에 수동 실행으로 3~5회 정상 수렴을 확인합니다.
- `status` 출력의 `watch_root_missing`, `watch_root_not_directory`가 0이 아니면 자동 정리 신뢰도가 떨어지므로 경로 복구 후 다시 확인합니다.

## 6) 대량 이동 전 링크 감사 권장 순서

1. `scripts/obsidian_link_audit.py`를 실행해 현재 깨진 링크 기준선을 저장합니다.
2. 소량 배치 이동 후 같은 명령을 다시 실행합니다.
3. 새로 증가한 `not_found`/`ambiguous_stem` 이슈가 있으면 해당 배치를 되돌리거나 경로를 수정합니다.

## 7) Documents 루트 모호성 정리 (저위험)

이 단계는 Obsidian 볼트 자체는 유지하고, Documents 루트의 애매한 분산 구조만 정리합니다.

```bash
# 1) 현재 구조 모호성 감사
./.venv/bin/python scripts/documents_structure_audit.py

# 2) 저위험 이동 매니페스트 생성 (dry-run)
./.venv/bin/python scripts/documents_rehome_manifest.py

# 3) 매니페스트 적용
./.venv/bin/python scripts/documents_rehome_manifest.py --apply

# 4) 적용 후 재감사
./.venv/bin/python scripts/documents_structure_audit.py

# 5) Obsidian 안전 게이트 재검증
./.venv/bin/python scripts/obsidian_link_audit.py --strict
./.venv/bin/python index_organizer.py status --config samples/index_organizer_v2.obsidian_documents.yml --review-limit 20
```

기본 저위험 이동 정책:

- `education-docs`, `admin-docs`, `legal` → `Documents/Collections/*`
- `루키-제안서-양식`, `리눅스2급1차족보new`, `산학-캡스톤-디자인`, `코로나-데이터-분석`, `통신사고객데이터분석`, `영상` → `Documents/Archive/Legacy-KR/*`
- `Obsidian` 및 활성 코드 프로젝트 폴더는 holdout으로 유지

## 8) 타입 우선 재구성 (처음부터 재배치)

같은 타입 파일이 여러 폴더에 흩어져 모호할 때, 타입 우선으로 재배치합니다.

```bash
# dry-run: 타입 기준 이동 계획만 생성
./.venv/bin/python scripts/documents_type_rehome.py

# 필요 시 소스 범위를 좁혀 실행
./.venv/bin/python scripts/documents_type_rehome.py --type-source Collections --type-source Archive/Legacy-KR

# 적용
./.venv/bin/python scripts/documents_type_rehome.py --apply
```

기본 동작:

- 소스: `Collections`, `Archive/Legacy-KR`, `프로젝트`
- 타깃: `Documents/Collections/ByType`
- 분류: `documents/*`, `media/*`, `archives/*`, `data/*`, `code/*`, `notes/*`, `other/*`
- 매니페스트: `outputs/documents_type_rehome_manifest_*.{json,md}`

## 9) holdout 2차 정리 (프로젝트성 루트 폴더)

루트에 남은 프로젝트성 holdout을 `Projects` 계층으로 정리합니다.

```bash
# dry-run: 계획만 생성
./.venv/bin/python scripts/documents_holdout_rehome.py

# 적용
./.venv/bin/python scripts/documents_holdout_rehome.py --apply
```

기본 정책:

- `agent-workflows`, `mobile-manipulator-robot`, `pdf-quiz-grader`, `presentation-deck-builder` → `Documents/Projects/Active/*`
- `libraries` → `Documents/Projects/Shared/libraries`
- `프로젝트` → `Documents/Projects/Legacy-KR/프로젝트`
- 루트 유지: `Obsidian`, `photos`, `Collections`, `Archive`

## 10) depth 복구 (ByType 과중첩 정규화)

`Collections/ByType/*/*/{Collections|Archive__Legacy-KR|프로젝트}/...` 형태로 깊이가 과도해졌다면 semantic-first로 복구합니다.

```bash
# dry-run
./.venv/bin/python scripts/documents_depth_rebalance.py

# 적용 + 빈 폴더 정리
./.venv/bin/python scripts/documents_depth_rebalance.py --apply --prune-empty
```

정규화 규칙:

- `.../Collections/<domain>/...` → `Documents/Collections/<domain>/<lane>/<subtype>/...`
- `.../Archive__Legacy-KR/<topic>/...` → `Documents/Archive/Legacy-KR/<topic>/<lane>/<subtype>/...`
- `.../프로젝트/...` → `Documents/Projects/Legacy-KR/프로젝트/<lane>/<subtype>/...`
