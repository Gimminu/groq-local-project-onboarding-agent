from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from app.index_v2.types import ActionPlan, PlannedAction

LEGACY_REPORT_COMMANDS = {"apply", "archive", "daily", "plan", "watch"}
STABLE_OUTPUT_DIRS = {"legacy-v1", "onboarding-agent"}
STABLE_OUTPUT_FILES = {".gitkeep", "README.md"}


def build_outputs_repair_plan(outputs_root: Path) -> ActionPlan:
    actions: list[PlannedAction] = []
    if outputs_root.exists():
        for child in sorted(outputs_root.iterdir(), key=lambda path: path.name.lower()):
            if child.name in STABLE_OUTPUT_FILES:
                continue
            if child.is_dir() and child.name in STABLE_OUTPUT_DIRS:
                continue
            destination, reason = _output_destination(outputs_root, child)
            if destination is None or destination == child:
                continue
            actions.append(
                PlannedAction(
                    action_type="move",
                    source_path=child,
                    destination_path=destination,
                    reason=reason,
                    confidence=1.0,
                )
            )
    return ActionPlan(
        command="repair-outputs",
        created_at=datetime.now(timezone.utc).isoformat(),
        scanned_roots=(outputs_root,),
        actions=actions,
    )


def _output_destination(outputs_root: Path, path: Path) -> tuple[Path | None, str]:
    name = path.name
    if name.startswith("folder_organizer_"):
        command = name.removeprefix("folder_organizer_").split("_", 1)[0]
        bucket = command if command in LEGACY_REPORT_COMMANDS else "misc"
        return outputs_root / "legacy-v1" / "reports" / bucket / name, "archive legacy V1 organizer report"
    if name.startswith("com.groqmcp.") or name.startswith("launchd."):
        return outputs_root / "legacy-v1" / "logs" / name, "archive legacy launchd log"
    if name == ".organizer.lock":
        return outputs_root / "legacy-v1" / "runtime" / name, "archive legacy organizer runtime file"
    if name.startswith("automation_") or name.startswith("project_onboarding_") or name in {"run.json", "run.md"}:
        return outputs_root / "onboarding-agent" / "reports" / name, "group onboarding agent traces"
    if path.is_dir():
        return outputs_root / "legacy-v1" / "misc" / name, "archive unclassified legacy output directory"
    return outputs_root / "legacy-v1" / "misc" / name, "archive unclassified legacy output file"
