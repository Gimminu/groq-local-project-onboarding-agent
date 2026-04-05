from __future__ import annotations

from pathlib import Path

from app.index_v2.output_repair import build_outputs_repair_plan


def test_build_outputs_repair_plan_groups_legacy_outputs(tmp_path: Path) -> None:
    outputs_root = tmp_path / "outputs"
    outputs_root.mkdir()
    (outputs_root / "folder_organizer_watch_20260331_154703.json").write_text("{}", encoding="utf-8")
    (outputs_root / "folder_organizer_plan_20260331_153516.md").write_text("# test", encoding="utf-8")
    (outputs_root / "com.groqmcp.folder-organizer.watch.standard.watch.out.log").write_text("", encoding="utf-8")
    (outputs_root / ".organizer.lock").write_text("", encoding="utf-8")
    (outputs_root / "automation_20260328_124341.json").write_text("{}", encoding="utf-8")
    (outputs_root / "weird.txt").write_text("?", encoding="utf-8")

    plan = build_outputs_repair_plan(outputs_root)
    destinations = {
        action.source_path.name: action.destination_path.relative_to(outputs_root).as_posix()
        for action in plan.actions
        if action.source_path is not None and action.destination_path is not None
    }

    assert destinations["folder_organizer_watch_20260331_154703.json"] == "legacy-v1/reports/watch/folder_organizer_watch_20260331_154703.json"
    assert destinations["folder_organizer_plan_20260331_153516.md"] == "legacy-v1/reports/plan/folder_organizer_plan_20260331_153516.md"
    assert destinations["com.groqmcp.folder-organizer.watch.standard.watch.out.log"] == "legacy-v1/logs/com.groqmcp.folder-organizer.watch.standard.watch.out.log"
    assert destinations[".organizer.lock"] == "legacy-v1/runtime/.organizer.lock"
    assert destinations["automation_20260328_124341.json"] == "onboarding-agent/reports/automation_20260328_124341.json"
    assert destinations["weird.txt"] == "legacy-v1/misc/weird.txt"


def test_build_outputs_repair_plan_leaves_stable_dirs_and_readme(tmp_path: Path) -> None:
    outputs_root = tmp_path / "outputs"
    (outputs_root / "legacy-v1").mkdir(parents=True)
    (outputs_root / "onboarding-agent").mkdir()
    (outputs_root / ".gitkeep").write_text("", encoding="utf-8")
    (outputs_root / "README.md").write_text("docs", encoding="utf-8")

    plan = build_outputs_repair_plan(outputs_root)

    assert plan.actions == []
