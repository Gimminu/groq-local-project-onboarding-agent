from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from app.index_v2.types import ActionPlan


def write_plan_report(plan: ActionPlan, reports_dir: Path) -> tuple[Path, Path]:
    reports_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"index_organizer_v2_{plan.command}_{stamp}"
    json_path = reports_dir / f"{base_name}.json"
    md_path = reports_dir / f"{base_name}.md"

    json_path.write_text(json.dumps(plan.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Index-Friendly Folder Manager V2",
        "",
        f"- command: `{plan.command}`",
        f"- created_at: `{plan.created_at}`",
        f"- total_actions: `{plan.summary().get('total', 0)}`",
        "",
        "## Summary",
        "",
    ]
    for key, value in sorted(plan.summary().items()):
        lines.append(f"- {key}: {value}")
    domain_candidates = plan.metadata.get("domain_candidates")
    if isinstance(domain_candidates, list) and domain_candidates:
        lines.extend(["", "## Candidate Domains", ""])
        for item in domain_candidates:
            if not isinstance(item, dict):
                continue
            blocked = ",".join(str(value) for value in item.get("blocked_reasons", [])) or "-"
            lines.append(
                f"- `{item.get('domain', '-')}` status={item.get('status', '-')} "
                f"ready={str(item.get('candidate_ready', False)).lower()} "
                f"count={item.get('observation_count', 0)} blocked={blocked}"
            )
    lines.extend(["", "## Actions", ""])
    for action in plan.actions:
        destination = str(action.destination_path) if action.destination_path else "-"
        lines.append(
            f"- `{action.action_type}` `{action.source_path}` -> `{destination}` "
            f"({action.status}, confidence={action.confidence:.2f})"
        )
        lines.append(f"  reason: {action.reason}")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def prune_watch_reports(reports_dir: Path, *, retention_days: int, max_report_pairs: int) -> list[Path]:
    if not reports_dir.exists():
        return []

    grouped: dict[str, list[Path]] = {}
    for path in reports_dir.glob("index_organizer_v2_watch_*"):
        if path.suffix not in {".json", ".md"}:
            continue
        grouped.setdefault(path.stem, []).append(path)

    cutoff = datetime.now() - timedelta(days=max(0, retention_days))
    ranked = sorted(
        grouped.items(),
        key=lambda item: max(member.stat().st_mtime for member in item[1]),
        reverse=True,
    )

    keep_limit = max(0, max_report_pairs)
    keep: set[str] = set()
    for index, (stem, members) in enumerate(ranked):
        newest = datetime.fromtimestamp(max(member.stat().st_mtime for member in members))
        within_age_limit = newest >= cutoff
        within_count_limit = keep_limit <= 0 or index < keep_limit
        if within_age_limit and within_count_limit:
            keep.add(stem)

    removed: list[Path] = []
    for stem, members in ranked:
        if stem in keep:
            continue
        for member in members:
            try:
                member.unlink()
                removed.append(member)
            except OSError:
                continue
    return removed
