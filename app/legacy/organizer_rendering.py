from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from app.organizer_summary import render_compact_summary
from app.organizer_types import OrganizerPlan


def write_organizer_files(plan: OrganizerPlan, output_dir: Path) -> tuple[Path, Path, Path]:
    resolved_output_dir = _resolve_output_dir(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"folder_organizer_{plan.command}_{stamp}"
    json_path = resolved_output_dir / f"{base_name}.json"
    markdown_path = resolved_output_dir / f"{base_name}.md"
    actions_path = resolved_output_dir / f"{base_name}.actions.jsonl"

    json_path.write_text(
        json.dumps(plan.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    markdown_lines = [
        "# Folder Organizer",
        "",
        "```text",
        render_compact_summary(plan),
        "```",
        "",
        "## Actions",
        "",
    ]
    for item in plan.decisions:
        destination = str(item.destination_path) if item.destination_path else "-"
        markdown_lines.append(
            f"- `{item.source_path}` -> `{destination}` "
            f"({item.status}, {item.risk_level}, {item.confidence:.2f})"
        )
        markdown_lines.append(f"  reason: {item.reason}")
    markdown_path.write_text("\n".join(markdown_lines) + "\n", encoding="utf-8")

    with actions_path.open("w", encoding="utf-8") as handle:
        for item in plan.decisions:
            handle.write(json.dumps(item.to_dict(), ensure_ascii=False) + "\n")

    return json_path, markdown_path, actions_path


def _resolve_output_dir(output_dir: Path) -> Path:
    candidate = output_dir.expanduser()
    if candidate.is_absolute():
        return candidate
    repo_root = Path(__file__).resolve().parents[1]
    return (repo_root / candidate).resolve()
