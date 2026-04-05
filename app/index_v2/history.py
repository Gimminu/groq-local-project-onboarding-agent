from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.index_v2.types import IndexOrganizerConfig, PlannedAction


def append_operation_history(
    config: IndexOrganizerConfig,
    *,
    action: PlannedAction,
    status: str,
    event_type: str | None = None,
    extra_lines: list[str] | None = None,
) -> Path:
    space = _space_for_action(config, action)
    history_path = config.history_root / space / "HISTORY.md"
    history_path.parent.mkdir(parents=True, exist_ok=True)

    if not history_path.exists():
        history_path.write_text(f"# {space} History\n\n", encoding="utf-8")

    timestamp = datetime.now(timezone.utc).isoformat()
    destination = str(action.destination_path) if action.destination_path else "-"
    lines = [
        f"- {timestamp} | {event_type or action.action_type} | {status} | `{action.source_path}` -> `{destination}`",
        f"  reason: {action.reason}",
    ]
    if extra_lines:
        for entry in extra_lines:
            lines.append(f"  {entry}")

    with history_path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
    return history_path


def write_archive_markdown(
    config: IndexOrganizerConfig,
    *,
    action: PlannedAction,
    archive_path: Path,
    manifest_path: Path,
    manifest_payload: dict[str, Any],
) -> tuple[Path, Path]:
    archive_md_path = archive_path.with_suffix(".md")
    classification = _classification_for_action(config, action)
    archive_md_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        f"# Archive {manifest_payload['archive_id']}",
        "",
        f"- archived_at: `{manifest_payload['archived_at']}`",
        f"- reason: {manifest_payload['reason']}",
        f"- zip_path: `{archive_path}`",
        f"- manifest_path: `{manifest_path}`",
        f"- space: `{classification.get('space', config.default_space)}`",
        f"- stream: `{classification.get('stream', 'resources')}`",
        f"- domain: `{classification.get('domain', config.naming.unknown_domain)}`",
        f"- focus: `{classification.get('focus', config.naming.unsorted_focus)}`",
        f"- asset_type: `{classification.get('asset_type', config.naming.misc_asset_type)}`",
        "",
        "## Files",
        "",
    ]
    for entry in manifest_payload["entries"]:
        digest = entry.get("sha256")
        digest_suffix = f", sha256={digest}" if digest else ""
        lines.append(f"- `{entry['path']}` ({entry['size']} bytes{digest_suffix})")
    archive_md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    history_path = append_operation_history(
        config,
        action=action,
        status="applied",
        event_type="archive-record",
        extra_lines=[
            f"archive_id: `{manifest_payload['archive_id']}`",
            f"archive_md: `{archive_md_path}`",
            f"manifest: `{manifest_path}`",
        ],
    )
    return archive_md_path, history_path


def _space_for_action(config: IndexOrganizerConfig, action: PlannedAction) -> str:
    classification = _classification_for_action(config, action)
    if "space" in classification:
        return str(classification["space"])

    for candidate in (action.destination_path, action.source_path):
        if candidate is None:
            continue
        try:
            return candidate.relative_to(config.spaces_root).parts[0]
        except ValueError:
            pass
        try:
            return candidate.relative_to(config.history_root).parts[0]
        except ValueError:
            pass
    return config.default_space


def _classification_for_action(config: IndexOrganizerConfig, action: PlannedAction) -> dict[str, Any]:
    classification = action.metadata.get("classification")
    if isinstance(classification, dict):
        return classification

    for candidate in (action.destination_path, action.source_path):
        if candidate is None:
            continue
        try:
            relative = candidate.relative_to(config.spaces_root)
        except ValueError:
            continue
        parsed = config.parse_canonical_relative(relative)
        if parsed is not None:
            space, stream, domain, focus, asset_type = parsed
            return {
                "space": space,
                "stream": stream,
                "domain": domain,
                "focus": focus,
                "asset_type": asset_type,
            }
    return {}
