#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


SKIP_FILE_NAMES = {
    ".DS_Store",
}


@dataclass(slots=True)
class MovePlan:
    source: Path
    destination: Path
    scope: str
    lane: str
    reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebalance over-nested ByType layouts into semantic-first depth-aware paths.",
    )
    parser.add_argument(
        "--documents-root",
        default="/Users/giminu0930/Documents",
        help="Documents root path.",
    )
    parser.add_argument(
        "--source-root",
        default="Collections/ByType",
        help="Relative source root inside Documents.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory where manifest reports are written.",
    )
    parser.add_argument(
        "--include-hidden",
        action="store_true",
        help="Include hidden files while scanning source root.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply moves. Without this flag, only dry-run manifest is generated.",
    )
    parser.add_argument(
        "--prune-empty",
        action="store_true",
        help="After apply, remove empty directories under source root.",
    )
    return parser.parse_args()


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


def _iter_source_files(source_root: Path, include_hidden: bool) -> list[Path]:
    files: list[Path] = []
    for candidate in sorted(source_root.rglob("*"), key=lambda path: path.as_posix().casefold()):
        if not candidate.is_file():
            continue
        if candidate.name in SKIP_FILE_NAMES:
            continue
        rel_parts = candidate.relative_to(source_root).parts
        if not include_hidden and any(part.startswith(".") for part in rel_parts):
            continue
        files.append(candidate)
    return files


def _destination_for_rebalanced(
    *,
    documents_root: Path,
    source_root: Path,
    source_file: Path,
) -> tuple[Path, str, str, str]:
    rel = source_file.relative_to(source_root)
    parts = rel.parts

    if len(parts) < 4:
        lane = parts[0] if parts else "unknown"
        subtype = parts[1] if len(parts) > 1 else "unknown"
        scope = parts[2] if len(parts) > 2 else "unknown"
        destination = documents_root / "Collections" / "recovered-scopes" / scope / lane / subtype / source_file.name
        return destination, scope, f"{lane}/{subtype}", "recover_short_path"

    lane = parts[0]
    subtype = parts[1]
    scope = parts[2]
    context_parts = list(parts[3:-1])
    filename = parts[-1]

    if scope == "Collections":
        domain = context_parts[0] if context_parts else "misc"
        remainder = context_parts[1:] if len(context_parts) > 1 else []
        destination = documents_root / "Collections" / domain / lane / subtype
        for value in remainder:
            destination = destination / value
        destination = destination / filename
        return destination, scope, f"{lane}/{subtype}", "collections_semantic_depth"

    if scope == "Archive__Legacy-KR":
        topic = context_parts[0] if context_parts else "unsorted"
        remainder = context_parts[1:] if len(context_parts) > 1 else []
        destination = documents_root / "Archive" / "Legacy-KR" / topic / lane / subtype
        for value in remainder:
            destination = destination / value
        destination = destination / filename
        return destination, scope, f"{lane}/{subtype}", "archive_semantic_depth"

    if scope == "프로젝트":
        topic = context_parts[0] if context_parts else ""
        remainder = context_parts[1:] if len(context_parts) > 1 else []
        destination = documents_root / "Projects" / "Legacy-KR" / "프로젝트"
        if topic:
            destination = destination / topic
        destination = destination / lane / subtype
        for value in remainder:
            destination = destination / value
        destination = destination / filename
        return destination, scope, f"{lane}/{subtype}", "project_semantic_depth"

    destination = documents_root / "Collections" / "recovered-scopes" / scope / lane / subtype
    for value in context_parts:
        destination = destination / value
    destination = destination / filename
    return destination, scope, f"{lane}/{subtype}", "recover_unknown_scope"


def build_rebalance_plans(
    *,
    documents_root: Path,
    source_root: Path,
    include_hidden: bool,
) -> list[MovePlan]:
    plans: list[MovePlan] = []
    if not source_root.exists() or not source_root.is_dir():
        return plans

    for source_file in _iter_source_files(source_root, include_hidden):
        destination, scope, lane, reason = _destination_for_rebalanced(
            documents_root=documents_root,
            source_root=source_root,
            source_file=source_file,
        )
        plans.append(
            MovePlan(
                source=source_file,
                destination=destination,
                scope=scope,
                lane=lane,
                reason=reason,
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


def _prune_empty_dirs(root: Path) -> int:
    removed = 0
    if not root.exists() or not root.is_dir():
        return removed
    for candidate in sorted(root.rglob("*"), key=lambda path: len(path.parts), reverse=True):
        if not candidate.is_dir():
            continue
        try:
            next(candidate.iterdir())
            continue
        except StopIteration:
            candidate.rmdir()
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
    json_path = output_dir / f"documents_depth_rebalance_manifest_{timestamp}.json"
    md_path = output_dir / f"documents_depth_rebalance_manifest_{timestamp}.md"

    status_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    lane_counts: dict[str, int] = {}
    for item in results:
        status = str(item.get("status", "-"))
        reason = str(item.get("reason", "-"))
        lane = str(item.get("lane", "-"))
        status_counts[status] = int(status_counts.get(status, 0)) + 1
        reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1
        lane_counts[lane] = int(lane_counts.get(lane, 0)) + 1

    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "documents_root": str(documents_root),
        "source_root": str(source_root),
        "applied": applied,
        "pruned_empty_dirs": pruned_empty_dirs,
        "planned_moves": len(results),
        "status_counts": status_counts,
        "reason_counts": reason_counts,
        "lane_counts": lane_counts,
        "results": results,
    }

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines: list[str] = []
    lines.append("# Documents Depth Rebalance Manifest")
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

    lines.append("## Lane Counts")
    for key in sorted(lane_counts):
        lines.append(f"- {key}: {lane_counts[key]}")
    if not lane_counts:
        lines.append("- none")
    lines.append("")

    lines.append("## Reason Counts")
    for key in sorted(reason_counts):
        lines.append(f"- {key}: {reason_counts[key]}")
    if not reason_counts:
        lines.append("- none")
    lines.append("")

    lines.append("## Move Results")
    if results:
        lines.append("| Source | Destination | Scope | Lane | Reason | Status | Result |")
        lines.append("|---|---|---|---|---|---|---|")
        for item in results:
            lines.append(
                f"| {item['source']} | {item['destination']} | {item['scope']} | {item['lane']} | {item['reason']} | {item['status']} | {item['result']} |"
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
    source_root = (documents_root / args.source_root).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()

    if not documents_root.exists() or not documents_root.is_dir():
        print(f"Documents root does not exist or is not a directory: {documents_root}")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)

    plans = build_rebalance_plans(
        documents_root=documents_root,
        source_root=source_root,
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
                "scope": plan.scope,
                "lane": plan.lane,
                "reason": plan.reason,
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
