# Findability-First Folder Organizer (V2)

이 프로젝트는 `Desktop`, `Documents`, `Downloads` 폴더를 대상으로, 거대한 분류표(Taxonomy)를 짜맞추는 것이 아니라 **사용자가 파일을 다시 찾을 때 걸리는 클릭/검색/망설임 비용을 최소화(Findability-First)**하는 것을 목표로 합니다.

## Core Philosophy: File-Level Precision & Dynamic Routing

이전 방식처럼 고정된 카테고리(Projects/Research 등 Anchor)를 강제하지 않습니다. 매 파일마다 **'어떤 방식으로 배치할지(placement mode)'**를 먼저 고르고, 최적의 깊이(Depth) 내에서 가장 직관적인 경로를 동적으로 제안합니다.

1. **Project Root Protection**: `.git`, `package.json` + `src/` 등이 있는 활성 프로젝트 폴더는 절대 쪼개지 않고 통째로 보호합니다.
2. **Recursive Leaf Evaluation (최하위 리프 재귀 스캔)**: 무의미한 중간 폴더(`resources/education/...`)를 통째로 옮기지 않고, 무조건 최하위 **개별 파일 단위**로 내려가 LLM이 하나씩 최적의 위치를 판단합니다.
3. **Dynamic Root Gate**: 미리 정해진 최상위 폴더에 억지로 넣지 않습니다. 필요하다면 새 최상위 폴더를 만들 수 있지만, 무분별한 폴더 생성을 막기 위한 강력한 제명(Gate) 규칙을 통과해야 합니다.

## Decision Sequence (Strict Order)

## Production System Prompt (English)

```text
You are a file placement engine. Do NOT build a fixed taxonomy.
Goal: minimize future re-find cost (clicks + uncertainty).

Do NOT require fixed top-level anchors. Top-level folders may be created dynamically,
but only when clearly beneficial and not generic.

INPUT YOU GET:
- base_dir (e.g., ~/Documents)
- item_path, filename, extension, size, timestamps
- optional: short content hints (first lines / metadata)
- existing_tree_summary: list of existing folders near base_dir (at least depth 1-3)
- protected_paths: paths that must not be renamed/moved (project roots, repos, etc.)

STEP 0 — Hard protection:
If item is inside or is a dependency/system folder, DO NOT MOVE.
Examples: node_modules, .venv, site-packages, __pycache__, dist, build.
Return placement_mode = review_only or keep_here. confidence = 1.0.

STEP 1 — Choose PLACEMENT MODE (only one):
- direct
- single_file_folder
- merge_existing
- review_only

Definitions:
- direct: filename is specific enough; placing it directly under a nearby folder keeps it easy to find.
- single_file_folder: filename is specific AND likely to spawn related files (versions, assets, notes); creating a folder named after the file/topic improves refinding.
- merge_existing: an existing folder clearly matches the same topic/project (high similarity, low ambiguity).
- review_only: confidence < 0.75 OR any rule conflict OR multiple strong candidates OR depth budget fail.

STEP 2 — Generate 3 candidate target paths (at most 1 may create a NEW top-level folder):
A) direct candidate (no new folder)
B) single_file_folder candidate (creates only the leaf folder, named from filename/topic)
C) merge_existing candidate (pick best existing folder)
Optional D) create_new_root candidate:
- Allowed only if A/B/C cannot satisfy depth budget AND the new root name is highly specific.
- New root name MUST be a short, human-understandable noun phrase.
- Never create generic roots (general/misc/temp/other/etc).

STEP 3 — Score candidates and pick best.
Minimize: depth + generic_names + redundancy + ambiguity.
Maximize: predictability from path names (strong information scent).

Scoring rules (hard constraints):
- Depth budget: target depth 2-3. Depth 4 => warning. Depth >= 5 => FAIL.
  If best candidate depth >= 5, return review_only.
- Banned names (case-insensitive) MUST NOT appear in any NEW folder segment:
  general, misc, temp, other, stuff, category, etc, unnamed, new folder
- Redundancy penalty: avoid repeated meaning tokens in path (e.g., workspace/mcp-workspace).
- Never create meaningless intermediate containers like "code" or "docs" unless they add real meaning.

STEP 4 — Output STRICT JSON ONLY (no extra text).
Return:
{
  "placement_mode": "direct|single_file_folder|merge_existing|review_only|keep_here",
  "target_path": "relative/path/from/base_dir",
  "create_folders": ["list of folders to create"],
  "confidence": 0.0-1.0,
  "reason": "1-2 short sentences",
  "alternatives": [
    {"placement_mode": "...", "target_path": "...", "why_not": "..."}
  ]
}
```

## Validation Rules

1. JSON 파싱 실패 시 `review_only` 처리.
2. `placement_mode`는 허용된 5개 중 하나여야 함.
3. 생성될 최종 경로의 Depth가 5 이상이면 무조건 튕겨내어 `review_only` 처리.
4. `create_folders`의 폴더명에 `general`, `misc` 등 금지어가 포함되어 있으면 `review_only` 처리.
5. 새로 생성되는 최상위 루트 폴더가 2개 이상일 경우 FAIL.
6. `reason` 길이가 2문장을 초과하면 길이를 자르거나 과도한 변명으로 간주하여 신뢰도를 낮춤.
7. 이때 하위 폴더명과 상위 폴더명이 중복되어 불필요한 깊이가 생기는 경우 FAIL. AI에게 재요청을 실행한다.

## Runtime Enforcement Notes

- review_only: planner always routes to review queue (no auto move).
- merge_existing: planner requires an existing destination topic/project folder; otherwise review queue.
- single_file_folder: planner creates one folder from the file stem and moves file under it.
  If the folder stem is generic/low-scent, planner routes to review queue.

## Source of Truth

Runtime implementation lives in app/index_v2/classifier.py and planner-level review enforcement in app/index_v2/planner.py.

## Operational Checks

Use these commands when the organizer feels inactive.

```bash
/Users/giminu0930/Documents/.venv/bin/python index_organizer.py status --config ~/folder-organizer-v2.yml
```

Expected key lines:

- service_loaded=true: launchd service is loaded and running.
- operation_state=ACTION_REQUIRED: automatic moves exist.
- operation_state=MANUAL_REVIEW_PENDING: no automatic action, but review queue exists.
- operation_state=CONVERGED_OR_IDLE: no pending automatic action and no review queue.
- llm_fallback_active=true: ambiguous watch-root items can use AI classification.
- llm_fallback_active=false: V2 is currently running in deterministic-only mode.

To print more review candidates:

```bash
/Users/giminu0930/Documents/.venv/bin/python index_organizer.py status --config ~/folder-organizer-v2.yml --review-limit 20
```

Run one explicit service cycle:

```bash
/Users/giminu0930/Documents/.venv/bin/python index_organizer.py service-tick --config ~/folder-organizer-v2.yml --apply
```

Shortcut wrapper:

```bash
/Users/giminu0930/Documents/.venv/bin/python quick_organizer.py status --config ~/folder-organizer-v2.yml
```

## Keep the organizer running automatically

Install the `service` launchd job with the exact config you just created so it boots at login and keeps rerunning the watcher/opportunistic cycles without manual restarts:

```bash
cd /Users/giminu0930/projects/workspace/groq-mcp-mac-agent/code
/Users/giminu0930/Documents/.venv/bin/python index_organizer.py service-install \
  --config /Users/giminu0930/projects/workspace/groq-mcp-mac-agent/folder-organizer-v2.live.yml
```

The job now references the provided file and will run at every login or reboot. When you edit the config again, rerun the same install command (or `service-uninstall` first if you want to reset the job) to refresh the launchd payload. Use `service-status` to confirm the agent is loaded:

```bash
/Users/giminu0930/Documents/.venv/bin/python index_organizer.py service-status
```
