#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

DEFAULT_TYPE_SOURCES = (
    "Collections",
    "Archive/Legacy-KR",
    "프로젝트",
)

TYPE_BUCKET_BY_EXTENSION = {
    ".pdf": "documents/pdf",
    ".doc": "documents/word",
    ".docx": "documents/word",
    ".hwp": "documents/hwp",
    ".hwpx": "documents/hwp",
    ".ppt": "documents/slides",
    ".pptx": "documents/slides",
    ".xls": "data/spreadsheets",
    ".xlsx": "data/spreadsheets",
    ".csv": "data/tabular",
    ".tsv": "data/tabular",
    ".json": "data/json",
    ".zip": "archives/compressed",
    ".rar": "archives/compressed",
    ".7z": "archives/compressed",
    ".tar": "archives/compressed",
    ".gz": "archives/compressed",
    ".bz2": "archives/compressed",
    ".xz": "archives/compressed",
    ".mp4": "media/video",
    ".mov": "media/video",
    ".avi": "media/video",
    ".mkv": "media/video",
    ".mp3": "media/audio",
    ".wav": "media/audio",
    ".m4a": "media/audio",
    ".aac": "media/audio",
    ".flac": "media/audio",
    ".ipynb": "code/notebooks",
    ".py": "code/source",
    ".js": "code/source",
    ".ts": "code/source",
    ".sh": "code/source",
    ".java": "code/source",
    ".c": "code/source",
    ".cpp": "code/source",
    ".rs": "code/source",
    ".md": "notes/markdown",
}

SKIP_FILE_NAMES = {
    ".DS_Store",
}


@dataclass(slots=True)
class MovePlan:
    source: Path
    destination: Path
    source_scope: str
    bucket: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rehome Documents files by file type with manifest and rollback trace.",
    )
    parser.add_argument(
        "--documents-root",
        default="/Users/giminu0930/Documents",
        help="Documents root path.",
    )
    parser.add_argument(
        "--target-root",
        default="Collections/ByType",
        help="Relative target root (inside Documents) for type-first layout.",
    )
    parser.add_argument(
        "--type-source",
        action="append",
        default=[],
        help="Relative source root to include. Repeatable. Defaults include Collections, Archive/Legacy-KR, 프로젝트.",
    )
    parser.add_argument(
        "--include-hidden",
        action="store_true",
        help="Include hidden entries while scanning source roots.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory where manifest reports are written.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply moves. Without this flag, only dry-run manifest is generated.",
    )
    return parser.parse_args()


def _bucket_for_extension(ext: str) -> str:
    if not ext:
        return "other/noext"
    return TYPE_BUCKET_BY_EXTENSION.get(ext.lower(), "other/files")


def _scope_label(path: Path, documents_root: Path) -> str:
    rel = path.relative_to(documents_root).as_posix()
    return rel.replace("/", "__")


def _resolve_destination_collision(destination: Path) -> Path:
    if not destination.exists():
        return destination
    stem = destination.stem
    suffix = destination.suffix
    index = 1
    while True:
        candidate = destination.with_name(f"{stem}__dup{index}{suffix}")
        if not candidate.exists():
            return candidate
        index += 1


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _iter_source_files(root: Path, include_hidden: bool) -> list[Path]:
    files: list[Path] = []
    for candidate in sorted(root.rglob("*"), key=lambda path: path.as_posix().casefold()):
        if not candidate.is_file():
            continue
        if candidate.name in SKIP_FILE_NAMES:
            continue
        rel_parts = candidate.relative_to(root).parts
        if not include_hidden and any(part.startswith(".") for part in rel_parts):
            continue
        files.append(candidate)
    return files


def build_type_plans(
    documents_root: Path,
    *,
    target_root: Path,
    source_roots: tuple[Path, ...],
    include_hidden: bool,
) -> list[MovePlan]:
    plans: list[MovePlan] = []
    target_abs = (documents_root / target_root).resolve()

    for source_root in source_roots:
        if not source_root.exists() or not source_root.is_dir():
            continue

        try:
            resolved_source = source_root.resolve()
        except OSError:
            continue

        # Avoid recursive self-ingestion if target root is included as source.
        if str(resolved_source).startswith(str(target_abs)):
            continue

        scope = _scope_label(source_root, documents_root)
        for source_file in _iter_source_files(source_root, include_hidden):
            try:
                resolved_file = source_file.resolve()
            except OSError:
                continue
            if _is_relative_to(resolved_file, target_abs):
                continue

            bucket = _bucket_for_extension(source_file.suffix.lower())
            rel_parent = source_file.parent.relative_to(source_root)

            destination = documents_root / target_root / bucket / scope
            if rel_parent.parts:
                destination = destination / rel_parent
            destination = destination / source_file.name

            plans.append(
                MovePlan(
                    source=source_file,
                    destination=destination,
                    source_scope=scope,
                    bucket=bucket,
                )
            )
    return plans


def _apply_move(plan: MovePlan) -> tuple[bool, str, Path | None]:
    source = plan.source
    destination = plan.destination

    if source.is_symlink():
        return False, "skip_symlink_source", None
    if not source.exists():
        return False, "skip_missing_source", None

    final_destination = _resolve_destination_collision(destination)
    final_destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(final_destination))
    return True, "moved", final_destination


def write_manifest(
    output_dir: Path,
    *,
    documents_root: Path,
    target_root: Path,
    applied: bool,
    sources: tuple[Path, ...],
    results: list[dict],
) -> tuple[Path, Path]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    json_path = output_dir / f"documents_type_rehome_manifest_{timestamp}.json"
    md_path = output_dir / f"documents_type_rehome_manifest_{timestamp}.md"

    bucket_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    for item in results:
        bucket = str(item.get("bucket", "-"))
        status = str(item.get("status", "-"))
        bucket_counts[bucket] = int(bucket_counts.get(bucket, 0)) + 1
        status_counts[status] = int(status_counts.get(status, 0)) + 1

    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "documents_root": str(documents_root),
        "target_root": str(target_root),
        "applied": applied,
        "sources": [str(source.relative_to(documents_root)) for source in sources if source.exists()],
        "planned_moves": len(results),
        "status_counts": status_counts,
        "bucket_counts": bucket_counts,
        "results": results,
    }

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines: list[str] = []
    lines.append("# Documents Type Rehome Manifest")
    lines.append("")
    lines.append(f"- Timestamp: {payload['timestamp']}")
    lines.append(f"- Root: {documents_root}")
    lines.append(f"- Target root: {target_root}")
    lines.append(f"- Applied: {str(applied).lower()}")
    lines.append(f"- Planned moves: {len(results)}")
    lines.append("")

    lines.append("## Status Counts")
    for key in sorted(status_counts):
        lines.append(f"- {key}: {status_counts[key]}")
    lines.append("")

    lines.append("## Bucket Counts")
    for key in sorted(bucket_counts):
        lines.append(f"- {key}: {bucket_counts[key]}")
    lines.append("")

    lines.append("## Move Results")
    if results:
        lines.append("| Source | Destination | Bucket | Scope | Status | Result |")
        lines.append("|---|---|---|---|---|---|")
        for item in results:
            lines.append(
                f"| {item['source']} | {item['destination']} | {item['bucket']} | {item['source_scope']} | {item['status']} | {item['result']} |"
            )
    else:
        lines.append("- none")

    lines.append("")
    lines.append("## Rollback Hints")
    lines.append("For rows with status=success, reverse move from destination to source.")

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def main() -> int:
    args = parse_args()
    documents_root = Path(args.documents_root).expanduser().resolve()
    target_root = Path(args.target_root)
    output_dir = Path(args.output_dir).expanduser().resolve()

    if not documents_root.exists() or not documents_root.is_dir():
        print(f"Documents root does not exist or is not a directory: {documents_root}")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)

    sources_input = tuple(args.type_source) if args.type_source else DEFAULT_TYPE_SOURCES
    source_roots: tuple[Path, ...] = tuple((documents_root / value).expanduser().resolve() for value in sources_input)

    plans = build_type_plans(
        documents_root,
        target_root=target_root,
        source_roots=source_roots,
        include_hidden=bool(args.include_hidden),
    )

    results: list[dict] = []
    for plan in plans:
        if args.apply:
            success, result, final_destination = _apply_move(plan)
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
                "bucket": plan.bucket,
                "source_scope": plan.source_scope,
                "status": status,
                "result": result,
            }
        )

    json_path, md_path = write_manifest(
        output_dir,
        documents_root=documents_root,
        target_root=target_root,
        applied=bool(args.apply),
        sources=source_roots,
        results=results,
    )

    print(f"Documents root: {documents_root}")
    print(f"Target root: {documents_root / target_root}")
    print(f"Planned moves: {len(results)}")
    print(f"Applied: {str(bool(args.apply)).lower()}")
    print(f"JSON manifest: {json_path}")
    print(f"MD manifest: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
