#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(slots=True)
class MovePlan:
    source: Path
    destination: Path
    reason: str


COLLECTION_MOVE_NAMES = (
    "education-docs",
    "admin-docs",
    "legal",
)

ARCHIVE_LEGACY_MOVE_NAMES = (
    "루키-제안서-양식",
    "리눅스2급1차족보new",
    "산학-캡스톤-디자인",
    "코로나-데이터-분석",
    "통신사고객데이터분석",
    "영상",
)

HOLDOUT_NAMES = (
    "Obsidian",
    "agent-workflows",
    "pdf-quiz-grader",
    "presentation-deck-builder",
    "mobile-manipulator-robot",
    "libraries",
    "photos",
    "프로젝트",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate and optionally apply low-risk Documents rehome plan with rollback manifest.",
    )
    parser.add_argument(
        "--documents-root",
        default="/Users/giminu0930/Documents",
        help="Documents root path.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory where manifest reports are written.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply planned moves. Without this flag, only manifest is generated.",
    )
    return parser.parse_args()


def _build_plans(documents_root: Path) -> list[MovePlan]:
    plans: list[MovePlan] = []

    for name in COLLECTION_MOVE_NAMES:
        source = documents_root / name
        if source.exists():
            destination = documents_root / "Collections" / name
            plans.append(MovePlan(source=source, destination=destination, reason="collection_docs"))

    for name in ARCHIVE_LEGACY_MOVE_NAMES:
        source = documents_root / name
        if source.exists():
            destination = documents_root / "Archive" / "Legacy-KR" / name
            plans.append(MovePlan(source=source, destination=destination, reason="legacy_archive"))

    return plans


def _apply_move(plan: MovePlan) -> tuple[bool, str]:
    source = plan.source
    destination = plan.destination

    if source.is_symlink():
        return False, "skip_symlink_source"
    if not source.exists():
        return False, "skip_missing_source"
    if destination.exists():
        return False, "skip_destination_exists"

    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(destination))
    return True, "moved"


def _write_manifest(
    output_dir: Path,
    documents_root: Path,
    plans: list[MovePlan],
    applied: bool,
    results: list[dict],
) -> tuple[Path, Path]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    json_path = output_dir / f"documents_rehome_manifest_{timestamp}.json"
    md_path = output_dir / f"documents_rehome_manifest_{timestamp}.md"

    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "documents_root": str(documents_root),
        "applied": applied,
        "holdout_names": list(HOLDOUT_NAMES),
        "planned_moves": len(plans),
        "results": results,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines: list[str] = []
    lines.append("# Documents Rehome Manifest")
    lines.append("")
    lines.append(f"- Timestamp: {payload['timestamp']}")
    lines.append(f"- Root: {documents_root}")
    lines.append(f"- Applied: {str(applied).lower()}")
    lines.append(f"- Planned moves: {len(plans)}")
    lines.append("")

    lines.append("## Move Results")
    if results:
        lines.append("| Source | Destination | Reason | Status | Result |")
        lines.append("|---|---|---|---|---|")
        for item in results:
            lines.append(
                f"| {item['source']} | {item['destination']} | {item['reason']} | {item['status']} | {item['result']} |"
            )
    else:
        lines.append("- none")

    lines.append("")
    lines.append("## Holdout Names")
    for name in HOLDOUT_NAMES:
        lines.append(f"- {name}")

    lines.append("")
    lines.append("## Rollback Hints")
    lines.append("Use the JSON result list to reverse each successful move: destination -> source")

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

    plans = _build_plans(documents_root)
    results: list[dict] = []
    for plan in plans:
        if args.apply:
            success, result = _apply_move(plan)
            status = "success" if success else "skipped"
        else:
            success, result = False, "planned"
            status = "planned"
        results.append(
            {
                "source": str(plan.source),
                "destination": str(plan.destination),
                "reason": plan.reason,
                "status": status,
                "result": result,
            }
        )

    json_path, md_path = _write_manifest(
        output_dir=output_dir,
        documents_root=documents_root,
        plans=plans,
        applied=bool(args.apply),
        results=results,
    )

    print(f"Documents root: {documents_root}")
    print(f"Planned moves: {len(plans)}")
    print(f"Applied: {str(bool(args.apply)).lower()}")
    print(f"JSON manifest: {json_path}")
    print(f"MD manifest: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
