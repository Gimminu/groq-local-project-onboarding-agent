#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


CATEGORY_PATTERN = re.compile(r"^\d{3}_.+")
PERIOD_PATTERN = re.compile(r"^20\d{2}(?:_Q[1-4])?$")
DATE_PATTERN = re.compile(r"\b(20\d{2}[01]\d[0-3]\d)\b")
VERSION_PATTERN = re.compile(r"(?:^|_)(?:v|버전)(\d{1,3})(?:_|$)", re.IGNORECASE)

NOISE_TOKENS = {
    "giminu0930",
    "final",
    "copy",
}

ENVELOPE_PARTS = {
    "documents",
    "document",
    "archives",
    "archive",
    "media",
    "code",
    "notebooks",
    "compressed",
    "pdf",
    "word",
    "hwp",
    "video",
    "audio",
}

PERIODIC_TOP_PREFIXES = (
    "003_법무_양식",
    "099_임시_30일삭제",
)

WORKTYPE_OVERRIDES = {
    "001_제안_산학_루키": "제안산학",
    "002_데이터_분석": "데이터분석",
    "003_법무_양식": "법무양식",
    "004_압축_원본": "압축원본",
    "005_교육_학습": "교육학습",
    "006_서비스_정의": "서비스정의",
    "007_오디오_발표": "오디오발표",
    "099_임시_30일삭제": "임시정리",
}

PROJECT_TOP_CATEGORY = "000_프로젝트"
PROJECT_CATEGORY_KEYWORDS = (
    "프로젝트",
    "project",
)

DOCUMENT_FILE_EXTENSIONS = {
    ".txt",
    ".md",
    ".rtf",
    ".pdf",
    ".doc",
    ".docx",
    ".hwp",
    ".hwpx",
    ".odt",
    ".ppt",
    ".pptx",
    ".xls",
    ".xlsx",
    ".csv",
    ".tsv",
    ".odg",
    ".ods",
    ".odp",
}

PRESERVE_ORIGINAL_NAME_KEYWORDS = (
    "제안서",
    "proposal",
    "설문",
    "survey",
    "동의서",
    "consent",
    "서약서",
    "agreement",
)

PROJECT_CODE_EXTENSIONS = {
    ".js",
    ".mjs",
    ".cjs",
    ".ts",
    ".tsx",
    ".jsx",
    ".py",
    ".java",
    ".c",
    ".cc",
    ".cpp",
    ".h",
    ".hpp",
    ".rs",
    ".go",
    ".rb",
    ".php",
    ".swift",
    ".kt",
    ".sh",
    ".bash",
    ".zsh",
    ".ps1",
    ".toml",
    ".lock",
}

PROJECT_CODE_FILENAMES = {
    "package.json",
    "package-lock.json",
    "yarn.lock",
    "pnpm-lock.yaml",
    "tsconfig.json",
    "webpack.config.js",
    "vite.config.js",
    "next.config.js",
    "pyproject.toml",
    "poetry.lock",
    "pipfile",
    "pipfile.lock",
    "requirements.txt",
    "dockerfile",
    "makefile",
}

PROJECT_CODE_DIR_PARTS = {
    "node_modules",
    "src",
    "dist",
    "build",
    "lib",
    "libs",
    "module",
    "modules",
    "vendor",
    "venv",
    ".venv",
    "__pycache__",
}


@dataclass(slots=True)
class RepairPlan:
    source: Path
    destination: Path
    top_category: str
    reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Repair Documents taxonomy: promote Korean numbered categories to root and normalize naming/depth.",
    )
    parser.add_argument(
        "--documents-root",
        default="/Users/giminu0930/Documents",
        help="Documents root path.",
    )
    parser.add_argument(
        "--source-root",
        default="Collections",
        help="Source container holding numbered Korean categories.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory where repair manifests are written.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply planned moves. Without this flag, only dry-run manifest is generated.",
    )
    parser.add_argument(
        "--prune-empty",
        action="store_true",
        help="After apply, prune empty directories under the source container.",
    )
    parser.add_argument(
        "--min-files-for-new-top",
        type=int,
        default=2,
        help="Minimum file count required to create a new top category when it does not already exist.",
    )
    parser.add_argument(
        "--fallback-top-category",
        default="090_보류_통합",
        help="Fallback top category used when a sparse new top category should be merged instead of created.",
    )
    parser.add_argument(
        "--documents-only",
        action="store_true",
        help="Organize only document-like files (pdf/doc/hwp/xlsx/csv/markdown, etc.).",
    )
    parser.add_argument(
        "--project-documents-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="When true, project categories only process document-like files and skip non-document assets.",
    )
    return parser.parse_args()


def _is_periodic_top(top_category: str) -> bool:
    return any(top_category.startswith(prefix) for prefix in PERIODIC_TOP_PREFIXES)


def _normalize_token(text: str) -> str:
    value = text.strip()
    value = value.replace("-", "_").replace(" ", "_")
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


def _extract_worktype(top_category: str) -> str:
    if top_category == PROJECT_TOP_CATEGORY:
        return "프로젝트"
    if top_category in WORKTYPE_OVERRIDES:
        return WORKTYPE_OVERRIDES[top_category]
    suffix = top_category.split("_", 1)[1] if "_" in top_category else top_category
    return _normalize_token(suffix).replace("_", "") or "업무"


def _is_project_category_name(value: str) -> bool:
    lowered = value.casefold()
    return any(keyword in value or keyword in lowered for keyword in PROJECT_CATEGORY_KEYWORDS)


def _is_document_file(path: Path) -> bool:
    return path.suffix.lower() in DOCUMENT_FILE_EXTENSIONS


def _should_preserve_original_name(path: Path) -> bool:
    lowered = _normalize_token(path.stem).casefold()
    return any(keyword in lowered for keyword in PRESERVE_ORIGINAL_NAME_KEYWORDS)


def _extract_date(stem: str, file_path: Path) -> str:
    match = DATE_PATTERN.search(stem)
    if match:
        return match.group(1)
    return datetime.fromtimestamp(file_path.stat().st_mtime).strftime("%Y%m%d")


def _title_from_stem(stem: str, date_value: str, *, worktype: str, top_category: str) -> str:
    normalized = _normalize_token(stem)
    if not normalized:
        return "문서"

    tokens = [token for token in normalized.split("_") if token]
    filtered: list[str] = []
    banned_tokens: set[str] = {worktype.casefold()}
    top_suffix = top_category.split("_", 1)[1] if "_" in top_category else top_category
    for token in _normalize_token(top_suffix).split("_"):
        if token:
            banned_tokens.add(token.casefold())

    for token in tokens:
        lower = token.casefold()
        if lower in NOISE_TOKENS:
            continue
        if lower in banned_tokens:
            continue
        if lower.replace("_", "") in banned_tokens:
            continue
        if token == date_value:
            continue
        if VERSION_PATTERN.fullmatch(token):
            continue
        if token.startswith("v") and token[1:].isdigit():
            continue
        if token.startswith("버전") and token[2:].isdigit():
            continue
        filtered.append(token)

    if not filtered:
        return "문서"

    return _normalize_token("_".join(filtered)) or "문서"


def _normalize_file_name(file_path: Path, *, top_category: str) -> str:
    if _should_preserve_original_name(file_path):
        return file_path.name

    stem = file_path.stem
    date_value = _extract_date(stem, file_path)
    worktype = _extract_worktype(top_category)
    title = _title_from_stem(stem, date_value, worktype=worktype, top_category=top_category)
    return f"{title}_{date_value}_{worktype}{file_path.suffix.lower()}"


def _normalize_parent_parts(parts: tuple[str, ...], *, periodic_top: bool) -> tuple[str, ...]:
    normalized: list[str] = []
    for part in parts:
        cleaned = _normalize_token(part)
        if not cleaned:
            continue
        lower = cleaned.casefold()
        if lower.startswith("."):
            continue
        if lower in ENVELOPE_PARTS:
            continue
        if not periodic_top and PERIOD_PATTERN.match(cleaned):
            continue
        normalized.append(cleaned)
    return tuple(normalized)


def _resolve_collision(destination: Path) -> Path:
    if not destination.exists():
        return destination

    stem = destination.stem
    suffix = destination.suffix
    base_stem = stem
    version = 1

    version_match = re.search(r"_버전(\d{2,3})$", stem)
    if version_match:
        base_stem = stem[: version_match.start()]
        version = int(version_match.group(1))

    while True:
        version += 1
        candidate = destination.with_name(f"{base_stem}_버전{version:02d}{suffix}")
        if not candidate.exists():
            return candidate


def _is_project_code_file(relative: Path, *, top_category: str) -> bool:
    parts = tuple(part.casefold() for part in relative.parts)
    file_name = relative.name.casefold()
    suffix = relative.suffix.lower()

    project_context = (
        "프로젝트" in top_category
        or "project" in top_category.casefold()
        or any("project" in part or "프로젝트" in part for part in parts)
    )

    if any(part in PROJECT_CODE_DIR_PARTS for part in parts):
        return True
    if file_name in PROJECT_CODE_FILENAMES:
        return True
    if project_context and suffix in PROJECT_CODE_EXTENSIONS:
        return True
    return False


def _discover_category_dirs(source_root: Path) -> list[Path]:
    if not source_root.exists() or not source_root.is_dir():
        return []
    result: list[Path] = []
    for child in sorted(source_root.iterdir(), key=lambda path: path.name.casefold()):
        if not child.is_dir():
            continue
        if not CATEGORY_PATTERN.match(child.name):
            continue
        result.append(child)
    return result


def _existing_top_categories(documents_root: Path) -> set[str]:
    categories: set[str] = set()
    if not documents_root.exists() or not documents_root.is_dir():
        return categories
    for child in documents_root.iterdir():
        if not child.is_dir():
            continue
        if CATEGORY_PATTERN.match(child.name):
            categories.add(_normalize_token(child.name))
    return categories


def build_repair_plans(
    *,
    documents_root: Path,
    source_root: Path,
    min_files_for_new_top: int,
    fallback_top_category: str,
    documents_only: bool = False,
    project_documents_only: bool = True,
) -> list[RepairPlan]:
    plans: list[RepairPlan] = []
    existing_top_categories = _existing_top_categories(documents_root)
    fallback_top = _normalize_token(fallback_top_category)

    for category_dir in _discover_category_dirs(source_root):
        top_category = _normalize_token(category_dir.name)
        project_category = _is_project_category_name(top_category)
        periodic_top = _is_periodic_top(top_category)
        candidate_files = [
            path
            for path in sorted(category_dir.rglob("*"), key=lambda path: path.as_posix().casefold())
            if path.is_file() and not path.name.startswith(".")
        ]

        promote_to_root = top_category in existing_top_categories or len(candidate_files) >= max(1, min_files_for_new_top)
        destination_top = top_category if promote_to_root else fallback_top
        if project_category:
            destination_top = PROJECT_TOP_CATEGORY

        for file_path in candidate_files:
            if documents_only and not _is_document_file(file_path):
                continue
            if project_category and project_documents_only and not _is_document_file(file_path):
                continue

            relative = file_path.relative_to(category_dir)
            if _is_project_code_file(relative, top_category=top_category):
                continue
            parent_parts = _normalize_parent_parts(relative.parts[:-1], periodic_top=periodic_top)
            normalized_name = _normalize_file_name(file_path, top_category=destination_top)

            destination = documents_root / destination_top
            if destination_top != top_category:
                destination = destination / top_category
            for part in parent_parts:
                destination = destination / part
            destination = destination / normalized_name

            plans.append(
                RepairPlan(
                    source=file_path,
                    destination=destination,
                    top_category=top_category,
                    reason=(
                        "project_fixed_top"
                        if project_category and destination_top == PROJECT_TOP_CATEGORY
                        else ("promote_and_normalize" if destination_top == top_category else "merge_sparse_top")
                    ),
                )
            )
    return plans


def _apply_plan(plan: RepairPlan) -> tuple[bool, str, Path | None]:
    source = plan.source
    destination = plan.destination

    if not source.exists():
        return False, "skip_missing_source", None
    if source.is_symlink():
        return False, "skip_symlink_source", None

    if source.resolve(strict=False) == destination.resolve(strict=False):
        return False, "skip_same_path", source

    final_destination = _resolve_collision(destination)
    final_destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(final_destination))
    return True, "moved", final_destination


def _prune_empty_dirs(root: Path) -> int:
    if not root.exists() or not root.is_dir():
        return 0

    removed = 0
    for candidate in sorted(root.rglob("*"), key=lambda path: len(path.parts), reverse=True):
        if not candidate.is_dir():
            continue
        try:
            next(candidate.iterdir())
            continue
        except StopIteration:
            candidate.rmdir()
            removed += 1

    if root.exists() and root.is_dir():
        try:
            next(root.iterdir())
        except StopIteration:
            root.rmdir()
            removed += 1

    return removed


def write_manifest(
    output_dir: Path,
    *,
    documents_root: Path,
    source_root: Path,
    applied: bool,
    pruned_empty_dirs: int,
    results: list[dict],
) -> tuple[Path, Path]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    json_path = output_dir / f"documents_korean_taxonomy_repair_{timestamp}.json"
    md_path = output_dir / f"documents_korean_taxonomy_repair_{timestamp}.md"

    status_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}
    for item in results:
        status = str(item.get("status", "-"))
        category = str(item.get("top_category", "-"))
        status_counts[status] = int(status_counts.get(status, 0)) + 1
        category_counts[category] = int(category_counts.get(category, 0)) + 1

    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "documents_root": str(documents_root),
        "source_root": str(source_root),
        "applied": applied,
        "pruned_empty_dirs": pruned_empty_dirs,
        "planned_moves": len(results),
        "status_counts": status_counts,
        "category_counts": category_counts,
        "results": results,
    }

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines: list[str] = []
    lines.append("# Documents Korean Taxonomy Repair Manifest")
    lines.append("")
    lines.append(f"- Timestamp: {payload['timestamp']}")
    lines.append(f"- Root: {documents_root}")
    lines.append(f"- Source root: {source_root}")
    lines.append(f"- Applied: {str(applied).lower()}")
    lines.append(f"- Planned moves: {len(results)}")
    lines.append(f"- Pruned empty dirs: {pruned_empty_dirs}")
    lines.append("")

    lines.append("## Status Counts")
    for key in sorted(status_counts):
        lines.append(f"- {key}: {status_counts[key]}")
    if not status_counts:
        lines.append("- none")
    lines.append("")

    lines.append("## Category Counts")
    for key in sorted(category_counts):
        lines.append(f"- {key}: {category_counts[key]}")
    if not category_counts:
        lines.append("- none")
    lines.append("")

    lines.append("## Move Results")
    if results:
        lines.append("| Source | Destination | Top Category | Status | Result |")
        lines.append("|---|---|---|---|---|")
        for item in results:
            lines.append(
                f"| {item['source']} | {item['destination']} | {item['top_category']} | {item['status']} | {item['result']} |"
            )
    else:
        lines.append("- none")

    lines.append("")
    lines.append("## Rollback Hints")
    lines.append("For rows with status=success, move destination back to source.")

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def main() -> int:
    args = parse_args()
    documents_root = Path(args.documents_root).expanduser().resolve()
    source_root = (documents_root / args.source_root).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()

    if not documents_root.exists() or not documents_root.is_dir():
        print(f"Documents root does not exist or is not a directory: {documents_root}")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)

    plans = build_repair_plans(
        documents_root=documents_root,
        source_root=source_root,
        min_files_for_new_top=int(args.min_files_for_new_top),
        fallback_top_category=str(args.fallback_top_category),
        documents_only=bool(args.documents_only),
        project_documents_only=bool(args.project_documents_only),
    )
    results: list[dict] = []

    for plan in plans:
        if args.apply:
            success, result, final_destination = _apply_plan(plan)
            status = "success" if success else "skipped"
            destination_text = str(final_destination or plan.destination)
        else:
            status = "planned"
            result = "planned"
            destination_text = str(plan.destination)

        results.append(
            {
                "source": str(plan.source),
                "destination": destination_text,
                "top_category": plan.top_category,
                "status": status,
                "result": result,
            }
        )

    pruned_empty_dirs = 0
    if args.apply and args.prune_empty:
        pruned_empty_dirs = _prune_empty_dirs(source_root)

    json_path, md_path = write_manifest(
        output_dir,
        documents_root=documents_root,
        source_root=source_root,
        applied=bool(args.apply),
        pruned_empty_dirs=pruned_empty_dirs,
        results=results,
    )

    print(f"Documents root: {documents_root}")
    print(f"Source root: {source_root}")
    print(f"Planned moves: {len(results)}")
    print(f"Applied: {str(bool(args.apply)).lower()}")
    print(f"Pruned empty dirs: {pruned_empty_dirs}")
    print(f"JSON manifest: {json_path}")
    print(f"MD manifest: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
