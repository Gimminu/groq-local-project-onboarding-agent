#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

GENERIC_NAMES = {
    "misc",
    "other",
    "temp",
    "tmp",
    "untitled",
    "new folder",
    "folder",
    "test",
    "sample",
    "프로젝트",
}

COLLECTION_CANDIDATES = {
    "education-docs",
    "admin-docs",
    "legal",
}

ARCHIVE_CANDIDATES = {
    "루키-제안서-양식",
    "리눅스2급1차족보new",
    "산학-캡스톤-디자인",
    "코로나-데이터-분석",
    "통신사고객데이터분석",
    "영상",
}

ROOT_KEEP = {"Obsidian"}


@dataclass(slots=True)
class EntrySummary:
    name: str
    kind: str
    children: int
    direct_file_count: int
    direct_dir_count: int
    suggestion: str


def has_korean(text: str) -> bool:
    return any("\uac00" <= ch <= "\ud7a3" for ch in text)


def has_latin(text: str) -> bool:
    return any(("a" <= ch.lower() <= "z") for ch in text)


def suggest_bucket(name: str) -> str:
    if name in ROOT_KEEP:
        return "keep_root"
    if name in COLLECTION_CANDIDATES:
        return "collections"
    if name in ARCHIVE_CANDIDATES:
        return "archive_legacy"
    if name.startswith(".tmp."):
        return "temp_holdout"
    if name.startswith("."):
        return "hidden_holdout"
    return "holdout"


def collect_extension_counts(documents_root: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    probe_roots = (
        documents_root / "Collections",
        documents_root / "Archive" / "Legacy-KR",
    )
    for root in probe_roots:
        if not root.exists() or not root.is_dir():
            continue
        for candidate in root.rglob("*"):
            if not candidate.is_file():
                continue
            if candidate.name.startswith("."):
                continue
            ext = candidate.suffix.lower() or "<noext>"
            counts[ext] = int(counts.get(ext, 0)) + 1
    return counts


def summarize_entry(path: Path) -> EntrySummary:
    if path.is_dir():
        children = list(path.iterdir())
        direct_file_count = sum(1 for child in children if child.is_file())
        direct_dir_count = sum(1 for child in children if child.is_dir())
        return EntrySummary(
            name=path.name,
            kind="dir",
            children=len(children),
            direct_file_count=direct_file_count,
            direct_dir_count=direct_dir_count,
            suggestion=suggest_bucket(path.name),
        )
    return EntrySummary(
        name=path.name,
        kind="file",
        children=0,
        direct_file_count=0,
        direct_dir_count=0,
        suggestion="root_file_holdout",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit top-level ambiguity in Documents folder and suggest low-risk buckets.",
    )
    parser.add_argument(
        "--documents-root",
        default="/Users/giminu0930/Documents",
        help="Documents root path to audit.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory to write audit reports.",
    )
    parser.add_argument(
        "--include-hidden",
        action="store_true",
        help="Include hidden top-level entries (dot-prefixed).",
    )
    return parser.parse_args()


def write_reports(output_dir: Path, summaries: Iterable[EntrySummary], documents_root: Path) -> tuple[Path, Path]:
    rows = list(summaries)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    json_path = output_dir / f"documents_structure_audit_{timestamp}.json"
    md_path = output_dir / f"documents_structure_audit_{timestamp}.md"

    generic_names = [row.name for row in rows if row.name.casefold() in GENERIC_NAMES]
    mixed_language = [row.name for row in rows if has_korean(row.name) and has_latin(row.name)]
    single_item_dirs = [row.name for row in rows if row.kind == "dir" and row.children == 1]
    extension_counts = collect_extension_counts(documents_root)

    by_suggestion: dict[str, int] = {}
    for row in rows:
        by_suggestion[row.suggestion] = int(by_suggestion.get(row.suggestion, 0)) + 1

    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "documents_root": str(documents_root),
        "total_entries": len(rows),
        "directories": sum(1 for row in rows if row.kind == "dir"),
        "files": sum(1 for row in rows if row.kind == "file"),
        "generic_name_count": len(generic_names),
        "generic_names": generic_names,
        "mixed_language_name_count": len(mixed_language),
        "mixed_language_names": mixed_language,
        "single_item_dir_count": len(single_item_dirs),
        "single_item_dirs": single_item_dirs,
        "extension_counts": extension_counts,
        "suggestion_counts": by_suggestion,
        "entries": [
            {
                "name": row.name,
                "kind": row.kind,
                "children": row.children,
                "direct_file_count": row.direct_file_count,
                "direct_dir_count": row.direct_dir_count,
                "suggestion": row.suggestion,
            }
            for row in rows
        ],
    }

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines: list[str] = []
    lines.append("# Documents Structure Audit")
    lines.append("")
    lines.append(f"- Timestamp: {payload['timestamp']}")
    lines.append(f"- Root: {documents_root}")
    lines.append(f"- Total entries: {payload['total_entries']}")
    lines.append(f"- Directories: {payload['directories']}")
    lines.append(f"- Files: {payload['files']}")
    lines.append(f"- Generic names: {payload['generic_name_count']}")
    lines.append(f"- Mixed-language names: {payload['mixed_language_name_count']}")
    lines.append(f"- Single-item dirs: {payload['single_item_dir_count']}")
    lines.append("")

    lines.append("## Suggested Buckets")
    for key in sorted(by_suggestion):
        lines.append(f"- {key}: {by_suggestion[key]}")
    lines.append("")

    lines.append("## Extension Summary (Collections + Archive/Legacy-KR)")
    if extension_counts:
        for ext, count in sorted(extension_counts.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"- {ext}: {count}")
    else:
        lines.append("- none")
    lines.append("")

    lines.append("## Top-Level Entries")
    lines.append("| Name | Kind | Children | Files | Dirs | Suggestion |")
    lines.append("|---|---|---:|---:|---:|---|")
    for row in sorted(rows, key=lambda item: item.name.casefold()):
        lines.append(
            f"| {row.name} | {row.kind} | {row.children} | {row.direct_file_count} | {row.direct_dir_count} | {row.suggestion} |"
        )

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def main() -> int:
    args = parse_args()
    documents_root = Path(args.documents_root).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()

    if not documents_root.exists() or not documents_root.is_dir():
        print(f"Documents root does not exist or is not a directory: {documents_root}")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)

    summaries: list[EntrySummary] = []
    for child in sorted(documents_root.iterdir(), key=lambda path: path.name.casefold()):
        if not args.include_hidden and child.name.startswith("."):
            continue
        summaries.append(summarize_entry(child))

    json_path, md_path = write_reports(output_dir=output_dir, summaries=summaries, documents_root=documents_root)

    print(f"Documents root: {documents_root}")
    print(f"Entries scanned: {len(summaries)}")
    print(f"JSON report: {json_path}")
    print(f"MD report: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
