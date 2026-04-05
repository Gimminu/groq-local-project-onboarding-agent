#!/usr/bin/env python3

from __future__ import annotations

import argparse
import shutil
from datetime import datetime
from pathlib import Path

SAFE_FILE_EXTENSIONS = {
    ".zip",
    ".rar",
    ".7z",
    ".tar",
    ".gz",
    ".bz2",
    ".xz",
    ".dmg",
    ".pkg",
    ".exe",
    ".msi",
    ".pdf",
    ".doc",
    ".docx",
    ".hwp",
    ".hwpx",
    ".ppt",
    ".pptx",
    ".xls",
    ".xlsx",
    ".csv",
    ".tsv",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
    ".mp4",
    ".mov",
    ".avi",
    ".mp3",
    ".wav",
    ".m4a",
}

NOTE_FILE_EXTENSIONS = {
    ".md",
    ".canvas",
    ".excalidraw",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage safe top-level files from an Obsidian vault root into the organizer dropbox.",
    )
    parser.add_argument(
        "--source-root",
        default="/Users/giminu0930/Documents/Obsidian",
        help="Vault root where existing files currently live.",
    )
    parser.add_argument(
        "--dropbox-root",
        default="/Users/giminu0930/Documents/Obsidian/Organizer/inbox",
        help="Organizer inbox root for gradual migration.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually move files. Without this flag, only preview is shown.",
    )
    parser.add_argument(
        "--include-notes",
        action="store_true",
        help="Also include note-like files (.md/.canvas/.excalidraw). Default is skip.",
    )
    parser.add_argument(
        "--name",
        action="append",
        default=[],
        help="Move only files with this exact basename. Repeatable.",
    )
    return parser.parse_args()


def _destination_for(source_name: str, destination_dir: Path) -> Path:
    candidate = destination_dir / source_name
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    index = 1
    while True:
        fallback = destination_dir / f"{stem}__dup{index}{suffix}"
        if not fallback.exists():
            return fallback
        index += 1


def _iter_move_candidates(source_root: Path, include_notes: bool, names: set[str]) -> list[Path]:
    candidates: list[Path] = []
    if not source_root.exists() or not source_root.is_dir():
        return candidates

    for child in sorted(source_root.iterdir(), key=lambda path: path.name.lower()):
        if child.name.startswith("."):
            continue
        if not child.is_file():
            continue

        if names and child.name not in names:
            continue

        suffix = child.suffix.lower()
        if suffix in NOTE_FILE_EXTENSIONS:
            if include_notes:
                candidates.append(child)
            continue
        if suffix in SAFE_FILE_EXTENSIONS:
            candidates.append(child)
    return candidates


def main() -> int:
    args = parse_args()
    source_root = Path(args.source_root).expanduser().resolve()
    dropbox_root = Path(args.dropbox_root).expanduser().resolve()
    names = {value for value in args.name if value}

    candidates = _iter_move_candidates(source_root, args.include_notes, names)
    if not candidates:
        print("No safe top-level candidates found.")
        return 0

    batch_name = f"root-import-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    destination_dir = dropbox_root / batch_name

    print(f"Source root: {source_root}")
    print(f"Dropbox root: {dropbox_root}")
    print(f"Batch dir: {destination_dir}")
    print(f"Candidates: {len(candidates)}")

    for item in candidates:
        print(f"  - {item.name}")

    if not args.apply:
        print("Preview only. Re-run with --apply to move files.")
        return 0

    destination_dir.mkdir(parents=True, exist_ok=True)

    moved = 0
    for item in candidates:
        destination = _destination_for(item.name, destination_dir)
        shutil.move(str(item), str(destination))
        moved += 1
        print(f"Moved: {item} -> {destination}")

    print(f"Completed. Moved {moved} files into {destination_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
