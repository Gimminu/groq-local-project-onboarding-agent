from __future__ import annotations

import json
import os
import subprocess
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import index_organizer
import app.index_v2.db as db_module
import app.index_v2.service as service_module
from app.index_v2.llm_controller import LLMRateLimitError
from app.index_v2.types import ClassificationResult


def test_incomplete_download_waits_for_stable_snapshot(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    download = roots["watch_root"]
    download.mkdir(parents=True, exist_ok=True)
    target = download / "report.pdf"
    target.write_text("a", encoding="utf-8")

    def refresh() -> None:
        nodes = [node for node in service.scan() if node.path == target.absolute()]
        service._refresh_staging(nodes)  # type: ignore[attr-defined]

    refresh()
    target.write_text("ab", encoding="utf-8")
    refresh()
    assert service._staged_candidates() == []  # type: ignore[attr-defined]

    refresh()
    assert service._staged_candidates() == []  # type: ignore[attr-defined]

    refresh()
    candidates = service._staged_candidates()  # type: ignore[attr-defined]
    assert [item.path for item in candidates] == [target.absolute()]


def test_watch_command_rechecks_staging_queue_without_root_rescan(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    download = roots["watch_root"]
    download.mkdir(parents=True, exist_ok=True)
    target = download / "report.pdf"
    target.write_text("payload", encoding="utf-8")

    service._queue_watch_path(target)  # type: ignore[attr-defined]

    original_scan = service.scan

    def fail_scan(*args, **kwargs):
        raise AssertionError("watch command should not rescan roots")

    service.scan = fail_scan  # type: ignore[method-assign]
    try:
        first_plan, _ = service.run_command(command="watch", apply_requested=False)
        second_plan, _ = service.run_command(command="watch", apply_requested=False)
    finally:
        service.scan = original_scan  # type: ignore[method-assign]

    assert first_plan is not None
    assert second_plan is not None
    assert second_plan.actions
    assert second_plan.actions[0].source_path == target.absolute()


def test_watch_noop_does_not_write_report_and_records_service_state(make_v2_service) -> None:
    service, _, roots = make_v2_service()

    plan, extras = service.run_watch_cycle(apply_requested=False)

    assert plan.summary()["total"] == 0
    assert "report_json" not in extras
    payload = json.loads(roots["state_dir"].joinpath("service-state.json").read_text(encoding="utf-8"))
    assert payload["last_watch_total"] == 0
    assert payload["last_watch_reported"] is False


def test_watch_with_actions_writes_report(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    target = roots["watch_root"] / "report.pdf"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("payload", encoding="utf-8")

    service._queue_watch_path(target)  # type: ignore[attr-defined]
    service.run_watch_cycle(apply_requested=False)
    plan, extras = service.run_watch_cycle(apply_requested=False)

    assert plan.summary()["total"] > 0
    assert Path(extras["report_json"]).exists()
    assert Path(extras["report_md"]).exists()


def test_watch_stability_anchor_survives_multiple_poll_intervals(make_v2_service, monkeypatch) -> None:
    service, _, roots = make_v2_service(
        {
            "watch": {
                "poll_interval_seconds": 10,
                "stable_observation_seconds": 15,
                "staging_age_seconds": 0,
            }
        }
    )
    target = roots["watch_root"] / "report.pdf"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("payload", encoding="utf-8")

    class FakeDateTime:
        current = datetime.fromtimestamp(target.stat().st_mtime, tz=timezone.utc)

        @classmethod
        def now(cls, tz=None):
            value = cls.current
            if tz is not None:
                return value.astimezone(tz)
            return value

        @classmethod
        def fromisoformat(cls, value: str):
            return datetime.fromisoformat(value)

    monkeypatch.setattr(service_module, "datetime", FakeDateTime)
    monkeypatch.setattr(db_module, "_utc_now", lambda: FakeDateTime.current.isoformat())

    service._queue_watch_path(target)  # type: ignore[attr-defined]

    FakeDateTime.current += timedelta(seconds=10)
    service._observe_staging_queue()  # type: ignore[attr-defined]

    FakeDateTime.current += timedelta(seconds=10)
    service._observe_staging_queue()  # type: ignore[attr-defined]

    FakeDateTime.current += timedelta(seconds=10)
    service._observe_staging_queue()  # type: ignore[attr-defined]
    candidates = service._staged_candidates()  # type: ignore[attr-defined]

    row = next(item for item in service.database.list_staging_entries() if item["path"] == str(target.absolute()))
    assert int(row["stable_count"]) >= 2
    assert [item.path for item in candidates] == [target.absolute()]


def test_apply_prunes_empty_external_watch_wrappers_after_move(make_v2_service) -> None:
    service, config, roots = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
            },
            "pattern_overrides": [],
        }
    )
    wrapper = roots["watch_root"] / "다운로드"
    wrapper.mkdir(parents=True, exist_ok=True)
    target = wrapper / "report.md"
    target.write_text("meeting notes", encoding="utf-8")

    plan, extras = service.run_command(command="apply", apply_requested=True)

    assert plan is not None
    assert not wrapper.exists()
    assert str(wrapper) in extras["cleaned_empty_watch_dirs"]
    moved = [action for action in plan.actions if action.action_type == "move" and action.source_path == target.absolute()]
    assert len(moved) == 1
    assert moved[0].destination_path is not None
    assert moved[0].destination_path.is_relative_to(config.adaptive_review_root)


def test_watchdog_dispatch_does_not_kill_service_on_handler_error(make_v2_service, capsys) -> None:
    service, _, _ = make_v2_service()

    def boom(_event: object) -> None:
        raise RuntimeError("watchdog handler failure")

    service._handle_watchdog_event = boom  # type: ignore[method-assign]
    service._dispatch_watchdog_event(SimpleNamespace(src_path="/tmp/demo.txt", dest_path=None))  # type: ignore[attr-defined]

    captured = capsys.readouterr()
    assert "RuntimeError: watchdog handler failure" in captured.err


def test_canonical_documents_change_is_checked_without_being_relocated(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    service.config.watch_roots = (roots["watch_root"], roots["spaces_root"])
    canonical = roots["spaces_root"] / "resources" / "research" / "robotics" / "docs"
    canonical.mkdir(parents=True, exist_ok=True)
    target = canonical / "paper.pdf"
    target.write_text("content", encoding="utf-8")

    service._queue_watch_path(target)  # type: ignore[attr-defined]
    service.run_watch_cycle(apply_requested=True)
    plan, _ = service.run_watch_cycle(apply_requested=True)

    assert plan.summary()["total"] == 0
    assert target.exists()


def test_protected_top_level_media_root_is_not_restaged(make_v2_service) -> None:
    service, _, roots = make_v2_service({"protected_stream_roots": ["projects", "photos"]})
    service.config.watch_roots = (roots["watch_root"], roots["spaces_root"])
    photos_root = roots["spaces_root"] / "photos" / "가현이"
    photos_root.mkdir(parents=True, exist_ok=True)
    target = photos_root / "1-ott-선물.jpeg"
    target.write_text("binary-placeholder", encoding="utf-8")

    service._queue_watch_path(target)  # type: ignore[attr-defined]

    assert service.database.list_staging_entries() == []


def test_project_root_moves_as_single_directory(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    project_root = roots["legacy_root"] / "01_Projects" / "pdf-quiz-app"
    (project_root / "src").mkdir(parents=True, exist_ok=True)
    (project_root / "package.json").write_text("{}", encoding="utf-8")
    (project_root / "src" / "index.js").write_text("console.log('hi')", encoding="utf-8")

    plan, _ = service.run_command(command="migrate", apply_requested=False)
    assert plan is not None
    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == project_root.absolute()]
    assert len(moves) == 1
    assert moves[0].destination_path == (
        config.spaces_root / "projects" / "apps" / "pdf-quiz-app"
    )
    assert not any(
        action.source_path is not None
        and action.source_path != project_root.absolute()
        and action.source_path.is_relative_to(project_root.absolute())
        for action in plan.actions
    )


def test_apply_cleans_empty_legacy_container_after_move(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    project_root = roots["legacy_root"] / "01_Projects" / "demo-app"
    (project_root / "src").mkdir(parents=True, exist_ok=True)
    (project_root / "package.json").write_text("{}", encoding="utf-8")
    (project_root / "src" / "index.js").write_text("console.log('hi')", encoding="utf-8")

    plan, extras = service.run_command(command="apply", apply_requested=True)
    assert plan is not None
    assert not (roots["legacy_root"] / "01_Projects").exists()
    assert str(roots["legacy_root"] / "01_Projects") in extras["cleaned_empty_dirs"]
    assert (config.spaces_root / "projects" / "apps" / "demo-app").exists()


def test_apply_cleans_ds_store_only_legacy_directories(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    empty_resource = roots["legacy_root"] / "03_Resources" / "Documents"
    empty_resource.mkdir(parents=True, exist_ok=True)
    (empty_resource / ".DS_Store").write_text("metadata", encoding="utf-8")

    plan, extras = service.run_command(command="apply", apply_requested=True)
    assert plan is not None
    assert not any(action.action_type == "move" and action.source_path == empty_resource.absolute() for action in plan.actions)
    assert not empty_resource.exists()
    assert str(empty_resource) in extras["cleaned_empty_dirs"]
    assert not (roots["legacy_root"] / "03_Resources").exists()


def test_duplicate_file_is_quarantined_and_proposal_recorded(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    watch_root = roots["watch_root"]
    watch_root.mkdir(parents=True, exist_ok=True)
    first = watch_root / "alpha.zip"
    second = watch_root / "beta.zip"
    first.write_text("same", encoding="utf-8")
    second.write_text("same", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=True)
    assert plan is not None

    quarantines = [action for action in plan.actions if action.action_type == "quarantine" and action.status == "applied"]
    assert len(quarantines) == 1
    quarantine_path = quarantines[0].destination_path
    assert quarantine_path is not None and quarantine_path.exists()

    proposals = service.database.list_deletion_proposals("pending")
    assert len(proposals) == 1
    assert proposals[0]["quarantine_path"] == str(quarantine_path)


def test_stale_duplicate_db_entry_does_not_quarantine_new_archive(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    watch_root = roots["watch_root"]
    watch_root.mkdir(parents=True, exist_ok=True)

    stale = watch_root / "old-bundle.zip"
    stale.write_text("same", encoding="utf-8")
    service.database.upsert_node(service._index_path(stale))  # type: ignore[attr-defined]
    stale.unlink()

    fresh = watch_root / "new-bundle.zip"
    fresh.write_text("same", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=False)
    assert plan is not None
    assert not any(action.action_type == "quarantine" and action.source_path == fresh.absolute() for action in plan.actions)
    assert any(action.action_type == "flag_for_review" and action.source_path == fresh.absolute() for action in plan.actions)


def test_duplicate_hwp_from_watch_root_moves_to_review_instead_of_quarantine(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    watch_root = roots["watch_root"]
    watch_root.mkdir(parents=True, exist_ok=True)
    first = watch_root / "제출서류-서식1-참가신청서류-루키.hwp"
    second = watch_root / "제출서류-서식2-참가신청서류-루키.hwp"
    first.write_text("same", encoding="utf-8")
    second.write_text("same", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=False)
    assert plan is not None

    assert not any(action.action_type == "quarantine" for action in plan.actions)
    moves = {action.source_path: action.destination_path for action in plan.actions if action.action_type == "move"}
    assert moves[first.absolute()] == config.spaces_root / "review" / "루키-제출서류" / "forms" / first.name
    assert moves[second.absolute()] == config.spaces_root / "review" / "루키-제출서류" / "forms" / second.name


def test_duplicate_misc_hwp_from_watch_root_is_not_quarantined(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    watch_root = roots["watch_root"]
    watch_root.mkdir(parents=True, exist_ok=True)
    first = watch_root / "붙임-a.hwp"
    second = watch_root / "붙임-b.hwp"
    payload = "duplicate-watch-root-hwp"
    first.write_text(payload, encoding="utf-8")
    second.write_text(payload, encoding="utf-8")

    original_classify = service.classifier.classify

    def classify_as_misc(_node):
        return ClassificationResult(
            placement_mode="direct",
            target_path="review/misc",
            confidence=0.9,
            rationale="mocked llm misc classification",
            source="llm",
            review_required=False,
            metadata={"destination_root": "spaces"},
            space="personal",
            stream="review",
            domain="review",
            focus="unsorted",
            asset_type="misc",
        )

    service.classifier.classify = classify_as_misc  # type: ignore[method-assign]
    try:
        plan, _ = service.run_command(command="apply", apply_requested=False)
    finally:
        service.classifier.classify = original_classify  # type: ignore[method-assign]

    assert plan is not None
    quarantined_sources = {action.source_path for action in plan.actions if action.action_type == "quarantine"}
    assert first.absolute() not in quarantined_sources
    assert second.absolute() not in quarantined_sources
    moved_sources = {action.source_path for action in plan.actions if action.action_type == "move"}
    assert first.absolute() in moved_sources or second.absolute() in moved_sources


def test_review_drain_merge_existing_directory_merges_into_existing_target(make_v2_service) -> None:
    service, config, roots = make_v2_service({"adaptive_placement": {"enabled": True}})
    target_dir = roots["spaces_root"] / "005_교육_학습"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "already-there.txt").write_text("old", encoding="utf-8")

    incoming_dir = config.adaptive_review_root / "005-교육-학습"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    incoming_file = incoming_dir / "리눅스2급2차압축원본.zip"
    incoming_file.write_text("payload", encoding="utf-8")

    original_classify = service.classifier.classify

    def classify_as_merge_existing(_node):
        return ClassificationResult(
            placement_mode="merge_existing",
            target_path="005_교육_학습",
            confidence=0.9,
            rationale="merged into an existing similar folder using filename and content similarity",
            source="heuristic",
            review_required=False,
            metadata={"adaptive_match": True},
            space="personal",
            stream="adaptive",
            domain="005_교육_학습",
            focus="005_교육_학습",
            asset_type="misc",
        )

    service.classifier.classify = classify_as_merge_existing  # type: ignore[method-assign]
    try:
        plan, _ = service.run_command(command="review-drain", apply_requested=True)
    finally:
        service.classifier.classify = original_classify  # type: ignore[method-assign]

    assert plan is not None
    moves = [action for action in plan.actions if action.action_type == "move"]
    assert len(moves) == 1
    assert moves[0].destination_path == target_dir
    assert not (roots["spaces_root"] / "005_교육_학습-2").exists()
    assert (target_dir / incoming_file.name).exists()
    assert not incoming_dir.exists()


def test_archive_manifest_is_created_and_undo_restores_files(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    canonical_dir = roots["spaces_root"] / "personal" / "resources" / "research" / "robotics" / "docs"
    canonical_dir.mkdir(parents=True, exist_ok=True)
    old_file = canonical_dir / "old-paper.pdf"
    old_file.write_text("archive me", encoding="utf-8")
    stale_ts = time.time() - 40 * 86400
    os.utime(old_file, (stale_ts, stale_ts))

    plan, _ = service.run_command(command="archive", apply_requested=True)
    assert plan is not None
    archive_actions = [action for action in plan.actions if action.action_type == "archive" and action.status == "applied"]
    assert len(archive_actions) == 1

    zip_path = archive_actions[0].destination_path
    assert zip_path is not None and zip_path.exists()
    assert zip_path.with_suffix(".manifest.json").exists()
    assert zip_path.with_suffix(".md").exists()
    history_path = roots["history_root"] / "personal" / "HISTORY.md"
    assert history_path.exists()
    assert "archive-record" in history_path.read_text(encoding="utf-8")
    assert not old_file.exists()

    _, undo_result = service.run_command(command="undo", apply_requested=True, undo_limit=1)
    assert undo_result["undone"]
    assert old_file.exists()
    assert not zip_path.exists()
    assert not zip_path.with_suffix(".md").exists()


def test_confirm_delete_requires_explicit_apply(make_v2_service) -> None:
    service, _, roots = make_v2_service({"deletion": {"installer_grace_days": 0}})
    watch_root = roots["watch_root"]
    watch_root.mkdir(parents=True, exist_ok=True)
    installer = watch_root / "tool.dmg"
    installer.write_text("installer", encoding="utf-8")
    stale_ts = time.time() - 2 * 86400
    os.utime(installer, (stale_ts, stale_ts))

    plan, _ = service.run_command(command="apply", apply_requested=True)
    assert plan is not None
    quarantine = next(action.destination_path for action in plan.actions if action.action_type == "quarantine")
    assert quarantine is not None and quarantine.exists()

    _, preview = service.run_command(command="confirm-delete", apply_requested=False, delete_target=str(quarantine))
    assert preview["deleted"]
    assert quarantine.exists()

    _, final = service.run_command(command="confirm-delete", apply_requested=True, delete_target=str(quarantine))
    assert final["deleted"]
    assert not quarantine.exists()


def test_migration_routes_unresolved_items_into_review_state_path(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    candidate = roots["legacy_root"] / "03_Resources" / "mystery.bin"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_bytes(b"\x00\x01")

    plan, _ = service.run_command(command="migrate", apply_requested=False)
    assert plan is not None
    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == candidate.absolute()]
    assert moves
    assert moves[0].destination_path == config.spaces_root / "review" / "misc" / candidate.name
    assert moves[0].review_required is True


def test_legacy_archive_file_uses_unsorted_focus_instead_of_filename_folder(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    candidate = roots["legacy_root"] / "04_Archive" / "3._lms_summarize_[file]_(uncertain).code-workspace"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("{}", encoding="utf-8")

    plan, _ = service.run_command(command="migrate", apply_requested=False)
    assert plan is not None
    flagged = [action for action in plan.actions if action.action_type == "flag_for_review"]
    assert flagged
    assert flagged[0].source_path == candidate.absolute()


def test_normalization_preserves_filename_when_redundancy_would_empty_it(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    current = roots["spaces_root"] / "archive" / "apps" / "lms-summarize" / "code"
    current.mkdir(parents=True, exist_ok=True)
    candidate = current / "lms_summarize.code-workspace"
    candidate.write_text("{}", encoding="utf-8")

    nodes = [node for node in service.scan() if node.path == candidate.absolute()]
    classification = service.classifier.classify(nodes[0])
    normalized = service.classifier.normalized_name(nodes[0], classification)

    assert classification.focus == "lms-summarize"
    assert normalized == "lms-summarize.code-workspace"


def test_classifier_uses_content_hints_for_domain_detection(make_v2_service) -> None:
    service, _, roots = make_v2_service({"pattern_overrides": []})
    candidate = roots["watch_root"] / "document.md"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("invoice receipt payment budget ledger summary", encoding="utf-8")

    node = next(node for node in service.scan() if node.path == candidate.absolute())
    classification = service.classifier.classify(node)

    assert classification.review_required is True
    assert classification.target_path == "review/finance/notes"
    assert classification.metadata["hinted_domain"] == "finance"


def test_classifier_merges_review_items_into_existing_focus_using_content_similarity(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    existing = roots["spaces_root"] / "review" / "project-alpha" / "notes"
    existing.mkdir(parents=True, exist_ok=True)
    (existing / "alpha-summary.md").write_text("summary", encoding="utf-8")

    candidate = roots["watch_root"] / "meeting-notes.md"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("Project Alpha architecture summary and next sprint notes.", encoding="utf-8")

    node = next(node for node in service.scan() if node.path == candidate.absolute())
    classification = service.classifier.classify(node)

    assert classification.review_required is True
    assert classification.target_path == "review/project-alpha/notes"


def test_adaptive_mode_routes_unmatched_files_to_hidden_review_root(make_v2_service) -> None:
    service, config, roots = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    candidate = roots["watch_root"] / "novel-meeting-notes.md"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("completely new topic with no matching destination", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == candidate.absolute()]
    assert len(moves) == 1
    assert moves[0].destination_path == config.adaptive_review_root / "notes" / candidate.name


def test_adaptive_mode_merges_into_existing_dynamic_folder(make_v2_service) -> None:
    service, config, roots = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
            },
            "pattern_overrides": [],
        }
    )
    existing = roots["spaces_root"] / "project-alpha"
    existing.mkdir(parents=True, exist_ok=True)
    (existing / "alpha-overview.md").write_text("project alpha architecture roadmap", encoding="utf-8")

    candidate = roots["watch_root"] / "sprint-notes.md"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("project alpha architecture sprint notes and roadmap", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == candidate.absolute()]
    assert len(moves) == 1
    assert moves[0].destination_path == existing / candidate.name


def test_adaptive_mode_preserves_meaningful_source_folder_name_for_new_top_level(make_v2_service) -> None:
    service, config, roots = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
            },
            "pattern_overrides": [],
        }
    )
    bundle = roots["watch_root"] / "presentation-deck-builder"
    bundle.mkdir(parents=True, exist_ok=True)
    (bundle / "src").mkdir(parents=True, exist_ok=True)
    (bundle / "package.json").write_text("{}", encoding="utf-8")
    (bundle / "src" / "index.ts").write_text("export const title = 'capstone bw text';", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == bundle.absolute()]
    assert len(moves) == 1
    assert moves[0].destination_path == config.spaces_root / "presentation-deck-builder"


def test_adaptive_mode_blocks_cache_like_top_level_dirs_from_documents(make_v2_service) -> None:
    service, config, roots = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
            },
            "pattern_overrides": [],
        }
    )
    bundle = roots["watch_root"] / "platformio-build-cache"
    bundle.mkdir(parents=True, exist_ok=True)
    (bundle / "state.json").write_text("{}", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    moves = [
        action
        for action in plan.actions
        if action.action_type == "move" and action.source_path is not None and action.source_path.is_relative_to(bundle.absolute())
    ]
    assert moves
    assert all(action.destination_path is not None and action.destination_path.is_relative_to(config.adaptive_review_root) for action in moves)
    assert not any(action.destination_path == config.spaces_root / "platformio-build-cache" for action in moves)


def test_adaptive_mode_keeps_existing_user_folders_stable(make_v2_service) -> None:
    service, _, roots = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
            },
        }
    )
    existing = roots["spaces_root"] / "research-notes"
    existing.mkdir(parents=True, exist_ok=True)
    target = existing / "paper-summary.md"
    target.write_text("summary", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == target.absolute()]
    assert moves == []


def test_adaptive_mode_does_not_reseed_existing_top_level_spaces_dirs(make_v2_service) -> None:
    service, _, roots = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
            },
        }
    )
    service.config.watch_roots = (roots["watch_root"], roots["spaces_root"])
    existing = roots["spaces_root"] / "research-notes"
    existing.mkdir(parents=True, exist_ok=True)
    target = existing / "paper-summary.md"
    target.write_text("summary", encoding="utf-8")

    service._seed_watch_backlog()  # type: ignore[attr-defined]

    assert service.database.list_staging_entries() == []


def test_adaptive_review_destination_uses_asset_first_layout(make_v2_service) -> None:
    _, config, _ = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "review_layout": "asset-first",
            },
        }
    )
    classification = ClassificationResult(
        placement_mode="review_only",
        target_path="review/admin/docs",
        confidence=0.4,
        rationale="review",
        source="heuristic",
        review_required=True,
        metadata={"destination_root": "review_staging"},
        asset_type="docs",
    )

    assert config.destination_relative_dir_for(classification) == Path("docs") / "admin"


def test_adaptive_mode_moves_unmatched_archives_and_code_into_hidden_review(make_v2_service) -> None:
    service, config, roots = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    archive_wrapper = roots["watch_root"] / "null"
    archive_wrapper.mkdir(parents=True, exist_ok=True)
    archive_file = archive_wrapper / "half.zip"
    archive_file.write_text("zip payload", encoding="utf-8")

    notebook_wrapper = roots["watch_root"] / "다운로드-4"
    notebook_wrapper.mkdir(parents=True, exist_ok=True)
    notebook_file = notebook_wrapper / "분석예제01.ipynb"
    notebook_file.write_text('{"cells":[],"metadata":{},"nbformat":4,"nbformat_minor":5}', encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=True)

    assert plan is not None
    archive_destinations = list((config.adaptive_review_root / "archives").rglob("half.zip"))
    notebook_destinations = list((config.adaptive_review_root / "code").rglob("분석예제01.ipynb"))
    assert archive_destinations
    assert notebook_destinations
    assert not archive_wrapper.exists()
    assert not notebook_wrapper.exists()


def test_adaptive_mode_routes_watch_archives_into_existing_archive_bucket(make_v2_service) -> None:
    service, _, roots = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    archive_root = roots["spaces_root"] / "004_압축_원본"
    linux_bucket = archive_root / "01_리눅스_압축원본"
    linux_bucket.mkdir(parents=True, exist_ok=True)

    archive_file = roots["watch_root"] / "리눅스2급2차압축원본.zip"
    archive_file.parent.mkdir(parents=True, exist_ok=True)
    archive_file.write_text("zip payload", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == archive_file.absolute()]
    assert len(moves) == 1
    assert moves[0].destination_path == linux_bucket / archive_file.name


def test_adaptive_mode_numbered_taxonomy_merges_forms_into_existing_subtopic(make_v2_service) -> None:
    service, _, roots = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    top = roots["spaces_root"] / "001_제안_산학_루키"
    sub = top / "01_루키_제안서"
    sub.mkdir(parents=True, exist_ok=True)

    candidate = roots["watch_root"] / "루키_도전제안서_양식.hwpx"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("proposal form", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == candidate.absolute()]
    assert len(moves) == 1
    assert moves[0].destination_path == sub / candidate.name


def test_adaptive_mode_numbered_taxonomy_creates_two_digit_subtopic_for_periodic_docs(make_v2_service) -> None:
    service, _, roots = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    top = roots["spaces_root"] / "001_제안_산학_루키"
    (top / "01_무관_폴더").mkdir(parents=True, exist_ok=True)

    candidate = roots["watch_root"] / "루키_도전제안서_2026-04.hwpx"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("periodic proposal", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == candidate.absolute()]
    assert len(moves) == 1
    destination = moves[0].destination_path
    assert destination is not None
    assert destination.parent.parent == top
    assert destination.parent.name.startswith("02_")
    assert "2026-04" in destination.parent.name


def test_adaptive_mode_numbered_taxonomy_prefers_watch_parent_context_over_image_noise(make_v2_service, monkeypatch) -> None:
    service, _, roots = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    service_subtopic = roots["spaces_root"] / "006_서비스_정의" / "01_서비스_정의서"
    service_subtopic.mkdir(parents=True, exist_ok=True)

    photo_subtopic = roots["spaces_root"] / "008_사진" / "01_스크린샷"
    photo_subtopic.mkdir(parents=True, exist_ok=True)
    for index in range(3):
        (photo_subtopic / f"shot-{index}.png").write_text("img", encoding="utf-8")

    candidate = (
        roots["watch_root"]
        / "organizer_live_test_20260406_023453"
        / "006_서비스_정의"
        / "01_서비스_정의서"
        / "서비스_기획_요약_20260403.docx"
    )
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("spec payload", encoding="utf-8")

    monkeypatch.setattr(service.classifier, "_content_hint", lambda _path: "image screenshot")

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == candidate.absolute()]
    assert len(moves) == 1
    assert moves[0].destination_path == service_subtopic / candidate.name


def test_adaptive_mode_numbered_top_level_creation_uses_three_digits(make_v2_service) -> None:
    service, _, roots = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    (roots["spaces_root"] / "001_제안_산학_루키").mkdir(parents=True, exist_ok=True)
    (roots["spaces_root"] / "002_데이터_분석").mkdir(parents=True, exist_ok=True)

    bundle = roots["watch_root"] / "신규_회의_번들"
    bundle.mkdir(parents=True, exist_ok=True)
    (bundle / "회의록.txt").write_text("minutes", encoding="utf-8")

    node = service._index_path(bundle)  # type: ignore[attr-defined]
    generated = service.classifier._adaptive_new_top_level_name(node)  # type: ignore[attr-defined]

    assert generated is not None
    assert generated.startswith("003_")


def test_watch_llm_keeps_adaptive_archive_in_review_staging(make_v2_service, monkeypatch) -> None:
    service, _, roots = make_v2_service(
        {
            "llm": {
                "enable_for_watch": True,
            },
        }
    )
    archive_file = roots["watch_root"] / "리눅스2급2차압축원본.zip"
    archive_file.parent.mkdir(parents=True, exist_ok=True)
    archive_file.write_text("zip payload", encoding="utf-8")
    node = service._index_path(archive_file)  # type: ignore[attr-defined]
    current = ClassificationResult(
        placement_mode="review_only",
        target_path="review/리눅스2급1차족보new/archives",
        confidence=0.52,
        rationale="ambiguous adaptive placement is staged in hidden review",
        source="heuristic",
        review_required=True,
        metadata={"adaptive_review": True, "destination_root": "review_staging"},
        stream="review",
        domain="review",
        focus="리눅스2급1차족보new",
        asset_type="archives",
    )

    monkeypatch.setattr(
        service.classifier._llm_controller,
        "invoke",
        lambda **_kwargs: SimpleNamespace(
            payload={
                "placement_mode": "direct",
                "target_path": "obsidian/004-organizer/eb-a3-8c-ec-9e-ac-ec-9e-98-eb-a5-4945cd",
                "create_folders": [],
                "confidence": 0.98,
                "reason": "route zip into organizer root",
                "alternatives": [],
            },
            provider_used="groq",
            provider_attempts=[{"provider": "groq", "status": "ok", "attempt": 1}],
            cloud_provider_used=True,
        ),
    )

    service.classifier.begin_batch("watch")
    service.classifier._pending_node = node  # type: ignore[attr-defined]
    result = service.classifier._llm_fallback(node, current)

    assert result.placement_mode == "review_only"
    assert result.target_path == "review/리눅스2급1차족보new/archives"
    assert result.metadata.get("destination_root") == "review_staging"


def test_review_drain_moves_hidden_review_bundle_into_documents(make_v2_service) -> None:
    service, config, _ = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    hidden_bundle = config.adaptive_review_root / "assets" / "photos"
    hidden_bundle.mkdir(parents=True, exist_ok=True)
    (hidden_bundle / "shot.png").write_text("png", encoding="utf-8")

    plan, extras = service.run_command(command="review-drain", apply_requested=True)

    assert plan is not None
    assert (config.spaces_root / "photos" / "shot.png").exists()
    assert not hidden_bundle.exists()
    assert any(path.endswith("adaptive-review/assets") for path in extras["cleaned_adaptive_review_dirs"])


def test_hidden_review_domain_hint_stays_in_review_until_destination_is_proven(make_v2_service) -> None:
    service, config, _ = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [
                {"pattern": "(?i)proposal|template|양식|루키", "domain": "templates"},
            ],
        }
    )
    hidden_file = config.adaptive_review_root / "docs" / "도전제안서-양식-루키.hwpx"
    hidden_file.parent.mkdir(parents=True, exist_ok=True)
    hidden_file.write_text("proposal", encoding="utf-8")

    result = service.classifier.classify(service._index_path(hidden_file))  # type: ignore[attr-defined]

    assert result.review_required is True
    assert result.placement_mode == "review_only"
    assert result.metadata["destination_root"] == "review_staging"
    assert result.target_path == "review/루키-제안서-양식/forms"


def test_review_drain_groups_hidden_review_loose_files_under_meaningful_focus(make_v2_service) -> None:
    service, config, _ = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    hidden_file = config.adaptive_review_root / "docs" / "서비스-정의서.docx"
    hidden_file.parent.mkdir(parents=True, exist_ok=True)
    hidden_file.write_text("spec", encoding="utf-8")

    plan, _ = service.run_command(command="review-drain", apply_requested=True)

    assert plan is not None
    assert not hidden_file.exists()
    assert (config.adaptive_review_root / "docs" / "admin-docs" / "서비스-정의서.docx").exists()
    assert not (config.spaces_root / "서비스-정의서.docx").exists()


def test_review_drain_skips_domain_named_hidden_review_group_dirs(make_v2_service) -> None:
    service, config, _ = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    grouped = config.adaptive_review_root / "code" / "legal"
    grouped.mkdir(parents=True, exist_ok=True)
    (grouped / "분석예제01.ipynb").write_text("{}", encoding="utf-8")

    plan, _ = service.run_command(command="review-drain", apply_requested=True)

    assert plan is not None
    assert (grouped / "분석예제01.ipynb").exists()
    assert not (config.spaces_root / "legal").exists()


def test_review_drain_requeues_blocked_top_level_asset_buckets_but_keeps_cache_dirs(make_v2_service) -> None:
    service, config, _ = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    blocked_doc = config.adaptive_review_root / "blocked-top-level" / "docs" / "서비스-정의서.docx"
    blocked_doc.parent.mkdir(parents=True, exist_ok=True)
    blocked_doc.write_text("spec", encoding="utf-8")
    blocked_cache = config.adaptive_review_root / "blocked-top-level" / "platformio-build-cache"
    blocked_cache.mkdir(parents=True, exist_ok=True)
    (blocked_cache / "state.json").write_text("{}", encoding="utf-8")

    plan, _ = service.run_command(command="review-drain", apply_requested=True)

    assert plan is not None
    assert not blocked_doc.exists()
    assert (config.adaptive_review_root / "docs" / "admin-docs" / "서비스-정의서.docx").exists()
    assert (blocked_cache / "state.json").exists()


def test_review_drain_promotes_domain_named_doc_groups_but_not_code_groups(make_v2_service) -> None:
    service, config, _ = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    legal_docs = config.adaptive_review_root / "docs" / "legal"
    legal_docs.mkdir(parents=True, exist_ok=True)
    (legal_docs / "동의서.hwpx").write_text("form", encoding="utf-8")
    legal_code = config.adaptive_review_root / "code" / "legal"
    legal_code.mkdir(parents=True, exist_ok=True)
    (legal_code / "분석예제01.ipynb").write_text("{}", encoding="utf-8")

    plan, _ = service.run_command(command="review-drain", apply_requested=True)

    assert plan is not None
    assert (config.spaces_root / "legal" / "동의서.hwpx").exists()
    assert not legal_docs.exists()
    assert (legal_code / "분석예제01.ipynb").exists()


def test_review_drain_groups_hidden_review_capstone_docs_under_focus(make_v2_service) -> None:
    service, config, _ = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    hidden_file = config.adaptive_review_root / "docs" / "붙임-sw중심대학사업-2026학년도-1학기-산학캡스톤디자인-지원.hwp"
    hidden_file.parent.mkdir(parents=True, exist_ok=True)
    hidden_file.write_text("doc", encoding="utf-8")

    plan, _ = service.run_command(command="review-drain", apply_requested=True)

    assert plan is not None
    assert not hidden_file.exists()
    assert (
        config.adaptive_review_root
        / "docs"
        / "산학-캡스톤-디자인"
        / "붙임-sw중심대학사업-2026학년도-1학기-산학캡스톤디자인-지원.hwp"
    ).exists()


def test_review_drain_groups_hidden_review_code_under_focus_using_content_hints(make_v2_service) -> None:
    service, config, _ = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    hidden_file = config.adaptive_review_root / "code" / "분석예제01.ipynb"
    hidden_file.parent.mkdir(parents=True, exist_ok=True)
    hidden_file.write_text(
        json.dumps(
            {
                "nbformat": 4,
                "nbformat_minor": 5,
                "cells": [
                    {
                        "cell_type": "markdown",
                        "metadata": {},
                        "source": ["대한민국 코로나 바이러스 감염 현황을 분석하는 프로젝트"],
                    }
                ],
                "metadata": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    plan, _ = service.run_command(command="review-drain", apply_requested=True)

    assert plan is not None
    assert not hidden_file.exists()
    assert (config.adaptive_review_root / "code" / "코로나-데이터-분석" / "분석예제01.ipynb").exists()


def test_review_drain_groups_hidden_review_archives_using_zip_manifest(make_v2_service) -> None:
    service, config, _ = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    hidden_zip = config.adaptive_review_root / "archives" / "revenue-통신사고객데이터분석.zip"
    hidden_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(hidden_zip, "w") as archive:
        archive.writestr("[분석예제][Revenue]통신사고객데이터분석.ipynb", "{}")
        archive.writestr("WA_Fn-UseC_-Telco-Customer-Churn.csv", "a,b\n1,2\n")

    plan, _ = service.run_command(command="review-drain", apply_requested=True)

    assert plan is not None
    assert not hidden_zip.exists()
    archive_group = config.adaptive_review_root / "archives" / "통신사고객데이터분석"
    assert archive_group.exists()
    assert any(child.suffix == ".zip" for child in archive_group.iterdir())


def test_review_drain_keeps_archive_focus_grouping_instead_of_merging_into_unrelated_project(make_v2_service) -> None:
    service, config, _ = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    unrelated_project = config.spaces_root / "mobile-manipulator-robot"
    unrelated_project.mkdir(parents=True, exist_ok=True)
    (
        unrelated_project / "분석예제01-코로나데이터분석-half.ipynb"
    ).write_text("{}", encoding="utf-8")

    hidden_zip = config.adaptive_review_root / "archives" / "half.zip"
    hidden_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(hidden_zip, "w") as archive:
        archive.writestr("분석예제01.코로나데이터분석(half).ipynb", "{}")

    plan, _ = service.run_command(command="review-drain", apply_requested=True)

    assert plan is not None
    assert not hidden_zip.exists()
    assert (config.adaptive_review_root / "archives" / "코로나-데이터-분석" / "half.zip").exists()
    assert not (unrelated_project / "half.zip").exists()


def test_review_drain_quarantines_synthetic_dummy_test_artifact(make_v2_service) -> None:
    service, config, _ = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
            "pattern_overrides": [],
        }
    )
    hidden_note = config.adaptive_review_root / "blocked-top-level" / "notes" / "education" / "dummy-test-ai.txt"
    hidden_note.parent.mkdir(parents=True, exist_ok=True)
    hidden_note.write_text("This is a dummy test file for classification.", encoding="utf-8")

    plan, _ = service.run_command(command="review-drain", apply_requested=True)

    assert plan is not None
    assert not hidden_note.exists()
    assert (config.quarantine_root / "dummy-test-ai.txt").exists()


def test_review_drain_apply_suppresses_followup_watch_staging(make_v2_service) -> None:
    service, config, _ = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
        }
    )
    service.config.watch_roots = (config.adaptive_review_root, config.spaces_root)
    hidden_bundle = config.adaptive_review_root / "assets" / "photos"
    hidden_bundle.mkdir(parents=True, exist_ok=True)
    moved_file = hidden_bundle / "shot.png"
    moved_file.write_text("png", encoding="utf-8")

    plan, _ = service.run_command(command="review-drain", apply_requested=True)

    assert plan is not None
    destination = config.spaces_root / "photos" / "shot.png"
    assert destination.exists()
    service._queue_watch_path(destination)  # type: ignore[attr-defined]
    assert service.database.list_staging_entries() == []


def test_para_tree_dry_run_generates_report_without_mutation(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    legacy = roots["legacy_root"]
    (legacy / "00_Inbox").mkdir(parents=True, exist_ok=True)
    (legacy / "01_Projects" / "pdf-quiz-app" / "src").mkdir(parents=True, exist_ok=True)
    (legacy / "02_Areas" / "Education").mkdir(parents=True, exist_ok=True)
    (legacy / "03_Resources" / "Documents").mkdir(parents=True, exist_ok=True)
    (legacy / "04_Archive").mkdir(parents=True, exist_ok=True)

    (legacy / "00_Inbox" / "2026-03-31_Linear Algebra Summary (2).pdf").write_text("notes", encoding="utf-8")
    (legacy / "01_Projects" / "pdf-quiz-app" / "package.json").write_text("{}", encoding="utf-8")
    (legacy / "01_Projects" / "pdf-quiz-app" / "src" / "index.js").write_text("console.log('x')", encoding="utf-8")
    (legacy / "02_Areas" / "Education" / "lecture-notes.pdf").write_text("class", encoding="utf-8")
    (legacy / "03_Resources" / "Documents" / "reference-guide.pdf").write_text("ref", encoding="utf-8")
    (legacy / "04_Archive" / "old.zip").write_text("zip", encoding="utf-8")

    plan, extras = service.run_command(command="migrate", apply_requested=False)
    assert plan is not None
    assert plan.summary()["total"] > 0
    assert Path(extras["report_json"]).exists()
    assert Path(extras["report_md"]).exists()
    assert (legacy / "01_Projects" / "pdf-quiz-app").exists()
    assert (legacy / "00_Inbox" / "2026-03-31_Linear Algebra Summary (2).pdf").exists()


def test_repair_projects_dry_run_maps_folders_into_new_domains(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    coding_root = roots["spaces_root"] / "projects" / "coding"
    (coding_root / "pdf-quiz-app" / "code" / "pdf-quiz-app").mkdir(parents=True, exist_ok=True)
    (coding_root / "mcp-workspace" / "code" / "groq-mcp-mac-agent").mkdir(parents=True, exist_ok=True)
    (coding_root / "mcp-workspace" / "docs").mkdir(parents=True, exist_ok=True)
    (coding_root / "mcp-workspace" / "assets").mkdir(parents=True, exist_ok=True)
    (coding_root / "실습파일-사전배포" / "misc" / "실습파일(사전배포)" / "data").mkdir(parents=True, exist_ok=True)
    (coding_root / "output" / "output" / "output" / "Docs").mkdir(parents=True, exist_ok=True)
    (coding_root / "vscode" / "misc" / ".vscode").mkdir(parents=True, exist_ok=True)

    plan, _ = service.run_command(command="repair-projects", apply_requested=False)

    assert plan is not None
    destinations = {(action.source_path, action.destination_path) for action in plan.actions if action.action_type == "move"}
    assert (
        (coding_root / "pdf-quiz-app").absolute(),
        config.spaces_root / "projects" / "apps" / "pdf-quiz-app",
    ) in destinations
    assert (
        (coding_root / "mcp-workspace").absolute(),
        config.spaces_root / "projects" / "workspace" / "mcp-workspace",
    ) in destinations
    assert (
        (coding_root / "실습파일-사전배포" / "misc" / "실습파일(사전배포)" / "data").absolute(),
        config.spaces_root / "resources" / "education" / "실습파일-사전배포" / "misc" / "data",
    ) in destinations
    assert (
        (coding_root / "output" / "output" / "output" / "Docs").absolute(),
        config.spaces_root / "projects" / "legacy-review" / "output-dumps" / "output" / "Docs",
    ) in destinations
    assert (
        (coding_root / "vscode").absolute(),
        config.spaces_root / "resources" / "templates" / "vscode-settings",
    ) in destinations


def test_repair_projects_apply_prunes_old_tree_and_preserves_protected_empty_dirs(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    coding_root = roots["spaces_root"] / "projects" / "coding"
    protected = coding_root / "c-project" / "code" / "c project" / "cmake-build-debug" / "CMakeFiles" / "pkgRedirects"
    protected.mkdir(parents=True, exist_ok=True)
    coursework_docs = coding_root / "26-1-coding" / "misc" / "26_1_coding" / "Docs"
    coursework_docs.mkdir(parents=True, exist_ok=True)
    (coursework_docs / "26-1-coding_v0-0.pdf").write_text("notes", encoding="utf-8")

    plan, extras = service.run_command(command="repair-projects", apply_requested=True)

    assert plan is not None
    assert not coding_root.exists()
    assert str(roots["spaces_root"] / "projects" / "coding") in extras["cleaned_empty_dirs"]
    assert (config.spaces_root / "projects" / "experiments" / "c-project" / "code" / "c project" / "cmake-build-debug" / "CMakeFiles" / "pkgRedirects").exists()
    assert (config.spaces_root / "areas" / "education" / "26-1-coding" / "misc" / "Docs" / "26-1-coding_v0-0.pdf").exists()


def test_new_project_root_from_watch_root_moves_into_typed_projects_tree(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    watch_root = roots["watch_root"]
    project_root = watch_root / "demo-app"
    (project_root / "src").mkdir(parents=True, exist_ok=True)
    (project_root / "package.json").write_text("{}", encoding="utf-8")
    (project_root / "src" / "index.js").write_text("console.log('hi')", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == project_root.absolute()]
    assert len(moves) == 1
    assert moves[0].destination_path == config.spaces_root / "projects" / "apps" / "demo-app"


def test_ambiguous_watch_root_note_moves_into_review_instead_of_manual_review(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    candidate = roots["watch_root"] / "Pasted markdown.md"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("# pasted", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == candidate.absolute()]
    assert len(moves) == 1
    assert moves[0].destination_path == config.spaces_root / "review" / "notes" / "Pasted markdown.md"
    renames = [action for action in plan.actions if action.action_type == "rename" and action.source_path == moves[0].destination_path]
    assert len(renames) == 1
    assert renames[0].destination_path == config.spaces_root / "review" / "notes" / "pasted-markdown.md"
    assert not any(action.action_type == "flag_for_review" and action.source_path == candidate.absolute() for action in plan.actions)


def test_ambiguous_watch_root_file_uses_llm_fallback_when_runtime_ready(make_v2_service, monkeypatch) -> None:
    service, _, roots = make_v2_service({"groq": {"enabled": True, "confidence_threshold": 0.9}})
    candidate = roots["watch_root"] / "서비스 정의서.docx"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("payload", encoding="utf-8")

    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    called = {"value": False}

    def fake_llm(node, current):
        called["value"] = True
        return current

    monkeypatch.setattr(service.classifier, "_llm_fallback", fake_llm)
    result = service.classifier.classify(service._index_path(candidate))  # type: ignore[attr-defined]

    assert called["value"] is True
    assert result.stream == "review"
    assert result.asset_type == "docs"


def test_llm_first_invokes_llm_even_for_high_confidence_rule_result(make_v2_service, monkeypatch) -> None:
    service, config, roots = make_v2_service(
        {
            "groq": {"enabled": True, "confidence_threshold": 0.75},
            "llm": {"enable_llm_first": True, "enable_for_watch": True, "fallback_to_other_cloud": False, "fallback_to_ollama": False},
            "pattern_overrides": [
                {"pattern": "(?i)education|course|lecture|class|수업|강의", "domain": "education", "stream": "resources"},
            ],
        }
    )
    existing_target = roots["spaces_root"] / "resources" / "education" / "docs"
    existing_target.mkdir(parents=True, exist_ok=True)
    candidate = roots["watch_root"] / "course-overview.pdf"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("payload", encoding="utf-8")

    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    called = {"value": False}

    def fake_llm(node, current):
        called["value"] = True
        assert current.confidence >= config.groq.confidence_threshold
        return current

    monkeypatch.setattr(service.classifier, "_llm_fallback", fake_llm)
    service.classifier.classify(service._index_path(candidate))  # type: ignore[attr-defined]

    assert called["value"] is True


def test_llm_first_preserves_system_dependency_protection(make_v2_service, monkeypatch) -> None:
    service, _, roots = make_v2_service(
        {
            "groq": {"enabled": True},
            "llm": {"enable_llm_first": True, "enable_for_watch": True},
        }
    )
    candidate = roots["watch_root"] / "node_modules" / "react" / "index.js"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("console.log('x')", encoding="utf-8")

    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    called = {"value": False}

    def fake_llm(_node, current):
        called["value"] = True
        return current

    monkeypatch.setattr(service.classifier, "_llm_fallback", fake_llm)
    result = service.classifier.classify(service._index_path(candidate))  # type: ignore[attr-defined]

    assert called["value"] is False
    assert result.placement_mode == "keep_here"
    assert result.metadata.get("system_dependency") is True


def test_llm_prompt_payload_uses_relative_item_path_hint(make_v2_service) -> None:
    service, _, roots = make_v2_service({"llm": {"enable_llm_first": True, "enable_for_watch": True}})
    candidate = roots["watch_root"] / "서비스 정의서.docx"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("payload", encoding="utf-8")

    node = service._index_path(candidate)  # type: ignore[attr-defined]
    current = service.classifier._rule_based(node)  # type: ignore[attr-defined]
    payload = service.classifier._llm_prompt_payload(node=node, current=current)  # type: ignore[attr-defined]

    assert payload["base_dir"] == "spaces_root"
    assert payload["item_path"].startswith("watch_root_1/")
    assert "/Users/" not in payload["item_path"]


def test_watch_cycle_skips_llm_fallback_for_ambiguous_items(make_v2_service, monkeypatch) -> None:
    service, _, roots = make_v2_service(
        {
            "groq": {"enabled": True, "confidence_threshold": 0.9},
            "pattern_overrides": [],
        }
    )
    candidate = roots["watch_root"] / "서비스 정의서.docx"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("payload", encoding="utf-8")

    monkeypatch.setenv("GROQ_API_KEY", "test-key")

    def fail_llm(_node, _current):
        raise AssertionError("watch batch should not invoke llm fallback")

    monkeypatch.setattr(service.classifier, "_llm_fallback", fail_llm)

    service._queue_watch_path(candidate)  # type: ignore[attr-defined]
    service.run_watch_cycle(apply_requested=False)
    plan, _ = service.run_watch_cycle(apply_requested=False)

    assert plan.summary()["total"] > 0
    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == candidate.absolute()]
    assert len(moves) == 1


def test_watch_cycle_uses_llm_fallback_for_ambiguous_items_when_enabled(make_v2_service, monkeypatch) -> None:
    service, _, roots = make_v2_service(
        {
            "groq": {"enabled": True, "confidence_threshold": 0.9},
            "llm": {"enable_for_watch": True, "fallback_to_other_cloud": False, "fallback_to_ollama": False},
            "pattern_overrides": [],
        }
    )
    candidate = roots["watch_root"] / "서비스 정의서.docx"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("payload", encoding="utf-8")

    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    called = {"value": False}

    def fake_llm(node, current):
        called["value"] = True
        return current

    monkeypatch.setattr(service.classifier, "_llm_fallback", fake_llm)

    service._queue_watch_path(candidate)  # type: ignore[attr-defined]
    service.run_watch_cycle(apply_requested=False)
    plan, _ = service.run_watch_cycle(apply_requested=False)

    assert plan.summary()["total"] > 0
    assert called["value"] is True


def test_llm_keep_here_preserves_current_hwp_asset_type(make_v2_service, monkeypatch) -> None:
    service, _, roots = make_v2_service()
    candidate = roots["watch_root"] / "도전제안서-양식-루키.hwpx"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("payload", encoding="utf-8")
    node = service._index_path(candidate)  # type: ignore[attr-defined]
    current = ClassificationResult(
        placement_mode="review_only",
        target_path="review/forms",
        confidence=0.55,
        rationale="hwpx template should remain classified as a form",
        source="heuristic",
        review_required=True,
        stream="review",
        domain="review",
        focus="review",
        asset_type="forms",
    )

    monkeypatch.setattr(
        service.classifier._llm_controller,
        "invoke",
        lambda **_kwargs: SimpleNamespace(
            payload={
                "placement_mode": "keep_here",
                "target_path": None,
                "create_folders": [],
                "confidence": 0.9,
                "reason": "keep the file where it is for now",
                "alternatives": [],
            },
            provider_used="ollama",
            provider_attempts=[],
            cloud_provider_used=False,
        ),
    )

    result = service.classifier._llm_fallback(node, current)

    assert result.placement_mode == "keep_here"
    assert result.asset_type == "forms"
    assert result.stream == "review"


def test_llm_rejects_banned_generated_target_segments_and_falls_back_to_review(make_v2_service, monkeypatch) -> None:
    service, config, roots = make_v2_service({"groq": {"enabled": True, "confidence_threshold": 0.9}})
    candidate = roots["watch_root"] / "서비스 정의서.docx"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("payload", encoding="utf-8")

    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    monkeypatch.setattr(
        service.classifier._llm_controller,
        "_groq_call",
        lambda: {
            "placement_mode": "direct",
            "target_path": "areas/admin/misc/Downloads",
            "create_folders": ["areas/admin/misc/Downloads"],
            "confidence": 0.98,
            "reason": "bad fixed taxonomy path",
            "alternatives": [],
        },
    )

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == candidate.absolute()]
    assert len(moves) == 1
    assert moves[0].destination_path == config.spaces_root / "review" / "admin-docs" / "docs" / "서비스 정의서.docx"
    row = service.database.get_classification(candidate.absolute())
    metadata = json.loads(row["metadata_json"])
    assert metadata["banned_target_segment"] is True
    assert metadata["llm_target_path"] == "review/admin-docs/docs"


def test_watch_llm_rejects_shallow_top_level_target_for_ambiguous_note(make_v2_service, monkeypatch) -> None:
    service, _, roots = make_v2_service({"llm": {"enable_for_watch": True}})
    candidate = roots["watch_root"] / "random-scratchpad.txt"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("scratch", encoding="utf-8")
    node = service._index_path(candidate)  # type: ignore[attr-defined]
    current = ClassificationResult(
        placement_mode="review_only",
        target_path="review/notes",
        confidence=0.52,
        rationale="ambiguous note stays in review by default",
        source="heuristic",
        review_required=True,
        stream="review",
        domain="review",
        focus="unsorted",
        asset_type="notes",
        metadata={"adaptive_review": True, "destination_root": "review_staging"},
    )

    monkeypatch.setattr(
        service.classifier._llm_controller,
        "invoke",
        lambda **_kwargs: SimpleNamespace(
            payload={
                "placement_mode": "direct",
                "target_path": "scratchpad",
                "create_folders": ["scratchpad"],
                "confidence": 0.99,
                "reason": "create a new top-level topic folder",
                "alternatives": [],
            },
            provider_used="groq",
            provider_attempts=[{"provider": "groq", "status": "ok", "attempt": 1}],
            cloud_provider_used=True,
        ),
    )

    service.classifier.begin_batch("watch")
    service.classifier._pending_node = node  # type: ignore[attr-defined]
    result = service.classifier._llm_fallback(node, current)

    assert result.placement_mode == "review_only"
    assert result.target_path == "review/notes"


def test_watch_skips_cloud_rate_limit_path_and_uses_heuristic_review_target(make_v2_service, monkeypatch) -> None:
    service, config, roots = make_v2_service(
        {
            "groq": {"enabled": True, "confidence_threshold": 0.9},
            "llm": {"fallback_to_other_cloud": False, "fallback_to_ollama": False, "retry_attempts": 0},
        }
    )
    candidate = roots["watch_root"] / "서비스 정의서.docx"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("payload", encoding="utf-8")

    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    monkeypatch.setattr(service.classifier._llm_controller, "_sleep", lambda _: None)
    monkeypatch.setattr(service.classifier._llm_controller, "_jitter", lambda _a, _b: 0.0)

    calls = {"groq": 0}

    def fail_groq() -> dict[str, object]:
        calls["groq"] += 1
        raise LLMRateLimitError("429")

    monkeypatch.setattr(service.classifier._llm_controller, "_groq_call", fail_groq)

    service._queue_watch_path(candidate)  # type: ignore[attr-defined]
    service.run_watch_cycle(apply_requested=False)
    plan, _ = service.run_watch_cycle(apply_requested=False)

    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == candidate.absolute()]
    assert len(moves) == 1
    assert moves[0].destination_path == config.spaces_root / "review" / "admin-docs" / "docs" / candidate.name
    assert calls["groq"] == 0
    assert service.database.list_staging_entries() == []

    row = service.database.get_classification(candidate.absolute())
    payload = json.loads(row["metadata_json"])
    assert "provider_used" not in payload


def test_project_root_detection_recognizes_modern_repo_layout_without_manifest(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    project_root = roots["watch_root"] / "campus-dashboard"
    (project_root / "frontend").mkdir(parents=True, exist_ok=True)
    (project_root / "backend").mkdir(parents=True, exist_ok=True)
    (project_root / "vite.config.ts").write_text("export default {}", encoding="utf-8")
    (project_root / "frontend" / "main.ts").write_text("console.log('hi')", encoding="utf-8")
    (project_root / "backend" / "server.ts").write_text("console.log('hi')", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == project_root.absolute()]
    assert len(moves) == 1
    assert moves[0].destination_path == config.spaces_root / "projects" / "apps" / "campus-dashboard"


def test_watch_skips_ollama_fallback_when_llm_is_disabled_for_watch(make_v2_service, monkeypatch) -> None:
    service, config, roots = make_v2_service(
        {
            "groq": {"enabled": True, "confidence_threshold": 0.9},
            "llm": {"fallback_to_other_cloud": False, "fallback_to_ollama": True, "retry_attempts": 0},
        }
    )
    candidate = roots["watch_root"] / "서비스 정의서.docx"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("payload", encoding="utf-8")

    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    monkeypatch.setattr(service.classifier._llm_controller, "_sleep", lambda _: None)
    monkeypatch.setattr(service.classifier._llm_controller, "_jitter", lambda _a, _b: 0.0)
    monkeypatch.setattr(service.classifier._llm_controller, "_ollama_healthcheck", lambda: True)

    calls = {"groq": 0, "ollama": 0}

    def fail_groq() -> dict[str, object]:
        calls["groq"] += 1
        raise LLMRateLimitError("429")

    def ollama_success() -> dict[str, object]:
        calls["ollama"] += 1
        return {
            "placement_mode": "review_only",
            "target_path": "review/docs",
            "create_folders": ["review/docs"],
            "confidence": 0.88,
            "reason": "ollama fallback selected the shallow review path",
            "alternatives": [],
        }

    monkeypatch.setattr(service.classifier._llm_controller, "_groq_call", fail_groq)
    monkeypatch.setattr(service.classifier._llm_controller, "_ollama_call", ollama_success)

    service._queue_watch_path(candidate)  # type: ignore[attr-defined]
    service.run_watch_cycle(apply_requested=False)
    plan, _ = service.run_watch_cycle(apply_requested=False)

    moves = [action for action in plan.actions if action.action_type == "move" and action.source_path == candidate.absolute()]
    assert len(moves) == 1
    assert moves[0].destination_path == config.spaces_root / "review" / "admin-docs" / "docs" / "서비스 정의서.docx"
    assert service.database.list_staging_entries() == []
    assert calls == {"groq": 0, "ollama": 0}

    row = service.database.get_classification(candidate.absolute())
    payload = json.loads(row["metadata_json"])
    assert "provider_used" not in payload
    assert "provider_attempts" not in payload


def test_watch_cloud_budget_settings_do_not_gate_heuristic_review_moves(make_v2_service, monkeypatch) -> None:
    service, _, roots = make_v2_service(
        {
            "groq": {"enabled": True, "confidence_threshold": 0.9},
            "llm": {
                "fallback_to_other_cloud": False,
                "fallback_to_ollama": False,
                "retry_attempts": 0,
                "max_items_per_watch_tick": 1,
            },
        }
    )
    first = roots["watch_root"] / "alpha.docx"
    second = roots["watch_root"] / "beta.docx"
    first.parent.mkdir(parents=True, exist_ok=True)
    first.write_text("alpha", encoding="utf-8")
    second.write_text("beta", encoding="utf-8")

    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    monkeypatch.setattr(service.classifier._llm_controller, "_sleep", lambda _: None)
    monkeypatch.setattr(service.classifier._llm_controller, "_jitter", lambda _a, _b: 0.0)
    calls = {"groq": 0}
    monkeypatch.setattr(
        service.classifier._llm_controller,
        "_groq_call",
        lambda: calls.__setitem__("groq", calls["groq"] + 1) or {
            "placement_mode": "review_only",
            "target_path": "review/docs",
            "create_folders": ["review/docs"],
            "confidence": 0.86,
            "reason": "budgeted cloud call selected review docs",
            "alternatives": [],
        },
    )

    service._queue_watch_path(first)  # type: ignore[attr-defined]
    service._queue_watch_path(second)  # type: ignore[attr-defined]
    service.run_watch_cycle(apply_requested=False)
    plan, _ = service.run_watch_cycle(apply_requested=False)

    moves = [action for action in plan.actions if action.action_type == "move"]
    assert len(moves) == 2
    assert calls["groq"] == 0
    assert service.database.list_staging_entries() == []


def test_transient_office_lock_file_is_ignored(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    candidate = roots["watch_root"] / "~$서비스 정의서.docx"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("lock", encoding="utf-8")

    result = service.classifier.classify(service._index_path(candidate))  # type: ignore[attr-defined]

    assert result.stream == "system"
    assert result.metadata["transient_system_file"] is True


def test_open_office_document_with_lock_sibling_is_not_moved(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    candidate = roots["watch_root"] / "서비스 정의서.docx"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("payload", encoding="utf-8")
    (roots["watch_root"] / "~$비스 정의서.docx").write_text("lock", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    ignored = [action for action in plan.actions if action.action_type == "ignore" and action.source_path == candidate.absolute()]
    assert len(ignored) == 1
    assert "lock file" in ignored[0].reason


def test_planner_honors_keep_here_without_synthesizing_destination(make_v2_service, monkeypatch) -> None:
    service, _, roots = make_v2_service()
    candidate = roots["watch_root"] / "keep.txt"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("payload", encoding="utf-8")

    monkeypatch.setattr(
        service.classifier,
        "classify",
        lambda _node: ClassificationResult(
            placement_mode="keep_here",
            target_path=None,
            confidence=1.0,
            rationale="keep this item in place",
            source="rule",
            review_required=False,
        ),
    )

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    ignored = [action for action in plan.actions if action.action_type == "ignore" and action.source_path == candidate.absolute()]
    assert len(ignored) == 1
    assert not any(action.action_type == "move" and action.source_path == candidate.absolute() for action in plan.actions)


def test_planner_flags_missing_target_path_instead_of_synthesizing_taxonomy(make_v2_service, monkeypatch) -> None:
    service, _, roots = make_v2_service()
    candidate = roots["watch_root"] / "ambiguous.txt"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("payload", encoding="utf-8")

    monkeypatch.setattr(
        service.classifier,
        "classify",
        lambda _node: ClassificationResult(
            placement_mode="direct",
            target_path=None,
            confidence=0.91,
            rationale="missing target path should never synthesize a taxonomy destination",
            source="heuristic",
            review_required=False,
        ),
    )

    plan, _ = service.run_command(command="apply", apply_requested=False)

    assert plan is not None
    flagged = [action for action in plan.actions if action.action_type == "flag_for_review" and action.source_path == candidate.absolute()]
    assert len(flagged) == 1
    assert not any(action.action_type == "move" and action.source_path == candidate.absolute() for action in plan.actions)


def test_plan_skips_watch_root_symlink_shortcuts(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    target = roots["spaces_root"] / "projects" / "apps" / "demo-app"
    target.mkdir(parents=True, exist_ok=True)
    shortcut = roots["watch_root"] / "demo-app-shortcut"
    shortcut.parent.mkdir(parents=True, exist_ok=True)
    shortcut.symlink_to(target, target_is_directory=True)

    plan, _ = service.run_command(command="plan", apply_requested=False)

    assert plan is not None
    assert not any(action.source_path == shortcut.absolute() for action in plan.actions)


def test_repair_tree_flattens_project_code_wrapper_into_focus_root(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    project_code_root = roots["spaces_root"] / "projects" / "experiments" / "c-project" / "code" / "c project"
    project_code_root.mkdir(parents=True, exist_ok=True)
    (project_code_root / "main.c").write_text("int main(){return 0;}", encoding="utf-8")

    plan, extras = service.run_command(command="repair-tree", apply_requested=True)

    assert plan is not None
    assert (config.spaces_root / "projects" / "experiments" / "c-project" / "main.c").exists()
    assert str(roots["spaces_root"] / "projects" / "experiments" / "c-project" / "code" / "c project") in extras["cleaned_empty_dirs"]


def test_invalid_review_imports_path_is_not_treated_as_canonical(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    candidate = roots["spaces_root"] / "review" / "imports" / "resources" / "misc" / "media" / "assets" / "13-발-image-uncertain.png"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("payload", encoding="utf-8")

    node = next(node for node in service.scan() if node.path == candidate.absolute())
    classification = service.classifier.classify(node)

    assert classification.metadata["invalid_canonical"] is True
    assert classification.domain == "unknown"
    assert classification.review_required is True


def test_repair_tree_dry_run_flattens_review_imports_and_preserves_mixed_bundles(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    review_misc = roots["spaces_root"] / "review" / "imports" / "resources" / "misc"
    dated_docs = review_misc / "2026-03-31-documents-새-한글-v1-pdf" / "docs"
    dated_docs.mkdir(parents=True, exist_ok=True)
    (dated_docs / "untitled__v01.pdf").write_text("pdf", encoding="utf-8")

    archives = roots["spaces_root"] / "review" / "imports" / "archive" / "misc" / "pdf-리눅스2급1차족보new-zip" / "archives"
    archives.mkdir(parents=True, exist_ok=True)
    (archives / "untitled.zip").write_text("zip", encoding="utf-8")

    media_assets = review_misc / "media" / "assets"
    media_assets.mkdir(parents=True, exist_ok=True)
    (media_assets / "13-발-image-uncertain.png").write_text("img", encoding="utf-8")

    datasets = review_misc / "datasets"
    (datasets / "data").mkdir(parents=True, exist_ok=True)
    (datasets / "archives").mkdir(parents=True, exist_ok=True)
    (datasets / "data" / "manage.json").write_text("{}", encoding="utf-8")
    (datasets / "archives" / "bundle.zip").write_text("zip", encoding="utf-8")

    plan, _ = service.run_command(command="repair-tree", apply_requested=False)

    assert plan is not None
    destinations = {(action.source_path, action.destination_path) for action in plan.actions if action.action_type == "move"}
    assert (
        (dated_docs / "untitled__v01.pdf").absolute(),
        config.spaces_root / "review" / "docs" / "새-한글__v01.pdf",
    ) in destinations
    assert (
        (archives / "untitled.zip").absolute(),
        config.spaces_root / "review" / "리눅스2급1차족보new" / "archives" / "리눅스2급1차족보new.zip",
    ) in destinations
    assert (
        (media_assets / "13-발-image-uncertain.png").absolute(),
        config.spaces_root / "review" / "photos" / "assets" / "13-발.png",
    ) in destinations
    assert (
        (datasets / "data" / "manage.json").absolute(),
        config.spaces_root / "review" / "misc" / "datasets" / "data" / "manage.json",
    ) in destinations
    assert (
        (datasets / "archives" / "bundle.zip").absolute(),
        config.spaces_root / "review" / "misc" / "datasets" / "archives" / "bundle.zip",
    ) in destinations


def test_repair_tree_groups_flat_documents_into_focus_folders(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    education_docs = roots["spaces_root"] / "areas" / "education" / "docs"
    education_assets = roots["spaces_root"] / "areas" / "education" / "assets"
    admin_forms = roots["spaces_root"] / "areas" / "admin" / "forms"
    template_docs = roots["spaces_root"] / "resources" / "templates" / "docs"
    research_data = roots["spaces_root"] / "resources" / "research" / "data"
    for directory in (education_docs, education_assets, admin_forms, template_docs, research_data):
        directory.mkdir(parents=True, exist_ok=True)

    info_doc = education_docs / "2026-정보처리기사-실기-기출문제집-핵심요약__v02.pdf"
    info_doc.write_text("pdf", encoding="utf-8")
    lecture_video = education_assets / "고급프로그래밍설계-02분반-pandas활용하기1-빨강펜__v01.mp4"
    lecture_video.write_text("video", encoding="utf-8")
    rookie_form = admin_forms / "제출서류-서식1-참가신청서류-루키__v01.hwp"
    rookie_form.write_text("form", encoding="utf-8")
    template_doc = template_docs / "도전제안서-양식-루키.hwpx"
    template_doc.write_text("template", encoding="utf-8")
    market_data = research_data / "2003-국제시장__v02.csv"
    market_data.write_text("data", encoding="utf-8")

    plan, _ = service.run_command(command="repair-tree", apply_requested=False)

    assert plan is not None
    moves = {action.source_path: action.destination_path for action in plan.actions if action.action_type == "move"}
    assert moves[info_doc.absolute()] == config.spaces_root / "areas" / "education" / "정보처리기사" / "docs" / info_doc.name
    assert moves[lecture_video.absolute()] == config.spaces_root / "areas" / "education" / "고급-프로그래밍-설계" / "assets" / lecture_video.name
    assert moves[rookie_form.absolute()] == config.spaces_root / "areas" / "admin" / "루키-제출서류" / "forms" / rookie_form.name
    assert moves[template_doc.absolute()] == config.spaces_root / "resources" / "templates" / "루키-제안서-양식" / "forms" / template_doc.name
    assert moves[market_data.absolute()] == config.spaces_root / "resources" / "research" / "유가-데이터" / "data" / market_data.name


def test_repair_tree_groups_review_focuses_and_rehomes_workspace_files(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    review_assets = roots["spaces_root"] / "review" / "assets"
    review_data = roots["spaces_root"] / "review" / "data"
    review_docs = roots["spaces_root"] / "review" / "docs"
    review_code = roots["spaces_root"] / "review" / "code"
    project_root = roots["spaces_root"] / "projects" / "apps" / "lms-summarize"
    for directory in (review_assets, review_data, review_docs, review_code, project_root):
        directory.mkdir(parents=True, exist_ok=True)

    screenshot = review_assets / "scr-20260130-svly-2.png"
    screenshot.write_text("png", encoding="utf-8")
    log_csv = review_data / "groq-logs-default-project-1d-2026-04-02.csv"
    log_csv.write_text("csv", encoding="utf-8")
    spec_doc = review_docs / "서비스-정의서.docx"
    spec_doc.write_text("doc", encoding="utf-8")
    workspace_file = review_code / "lms-summarize.code-workspace"
    workspace_file.write_text("{}", encoding="utf-8")

    plan, _ = service.run_command(command="repair-tree", apply_requested=False)

    assert plan is not None
    moves = {action.source_path: action.destination_path for action in plan.actions if action.action_type == "move"}
    assert moves[screenshot.absolute()] == config.spaces_root / "review" / "screenshots" / "assets" / screenshot.name
    assert moves[log_csv.absolute()] == config.spaces_root / "review" / "groq-logs" / "data" / "default-project-1d-2026-04-02.csv"
    assert moves[spec_doc.absolute()] == config.spaces_root / "review" / "admin-docs" / "docs" / spec_doc.name
    assert moves[workspace_file.absolute()] == project_root / workspace_file.name


def test_housekeeping_removes_watch_root_metadata_artifacts(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    desktop_root = roots["state_dir"].parent / "Desktop"
    desktop_root.mkdir(parents=True, exist_ok=True)
    service.config.watch_roots = (roots["watch_root"], desktop_root)
    desktop_ds_store = desktop_root / ".DS_Store"
    desktop_ds_store.write_text("metadata", encoding="utf-8")

    result = service._run_housekeeping(apply_requested=True)  # type: ignore[attr-defined]

    assert not desktop_ds_store.exists()
    assert str(desktop_ds_store) in result["removed_metadata_artifacts"]


def test_repair_tree_apply_flattens_legacy_area_wrappers(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    admin_root = roots["spaces_root"] / "areas" / "admin" / "02-areas" / "misc" / "Admin"
    admin_root.mkdir(parents=True, exist_ok=True)
    (admin_root / "제출서류-서식1-참가신청서류-루키__v01.hwp").write_text("form", encoding="utf-8")

    education_root = roots["spaces_root"] / "areas" / "education" / "02-areas" / "misc" / "Education"
    education_root.mkdir(parents=True, exist_ok=True)
    (education_root / "과제-제안서__v01.pdf").write_text("pdf", encoding="utf-8")

    focus_docs = roots["spaces_root"] / "areas" / "education" / "26-1-coding" / "misc" / "Docs"
    focus_docs.mkdir(parents=True, exist_ok=True)
    (focus_docs / "lecture-notes.pdf").write_text("notes", encoding="utf-8")

    plan, extras = service.run_command(command="repair-tree", apply_requested=True)

    assert plan is not None
    assert (config.spaces_root / "areas" / "admin" / "forms" / "제출서류-서식1-참가신청서류-루키__v01.hwp").exists()
    assert (config.spaces_root / "areas" / "education" / "docs" / "과제-제안서__v01.pdf").exists()
    assert (config.spaces_root / "areas" / "education" / "26-1-coding" / "docs" / "lecture-notes.pdf").exists()
    assert str(roots["spaces_root"] / "areas" / "admin" / "02-areas") in extras["cleaned_empty_dirs"]
    assert str(roots["spaces_root"] / "areas" / "education" / "02-areas") in extras["cleaned_empty_dirs"]


def test_repair_tree_collapses_legacy_output_dump_wrappers(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    wrapper_root = roots["spaces_root"] / "projects" / "legacy-review" / "output-dumps" / "output" / "Docs" / "output"
    wrapper_root.mkdir(parents=True, exist_ok=True)
    (wrapper_root / "summary.md").write_text("summary", encoding="utf-8")
    (wrapper_root / "Transcripts").mkdir(parents=True, exist_ok=True)
    (wrapper_root / "Transcripts" / "file.txt").write_text("transcript", encoding="utf-8")
    protected_empty = roots["spaces_root"] / "projects" / "apps" / "demo-app" / "build" / "uploads"
    protected_empty.mkdir(parents=True, exist_ok=True)

    plan, extras = service.run_command(command="repair-tree", apply_requested=True)

    assert plan is not None
    assert (config.spaces_root / "projects" / "legacy-review" / "output-dumps" / "output" / "summary.md").exists()
    assert (config.spaces_root / "projects" / "legacy-review" / "output-dumps" / "output" / "Transcripts" / "file.txt").exists()
    assert protected_empty.exists()
    assert str(roots["spaces_root"] / "projects" / "legacy-review" / "output-dumps" / "output" / "Docs" / "output") in extras["cleaned_empty_dirs"]


def test_repair_tree_merges_suffixed_asset_dir_back_into_canonical_folder(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    suffixed = roots["spaces_root"] / "archive" / "experiments" / "misc-2"
    (suffixed / ".idea").mkdir(parents=True, exist_ok=True)
    (suffixed / ".idea" / "workspace.xml").write_text("<xml/>", encoding="utf-8")
    canonical = roots["spaces_root"] / "archive" / "experiments" / "misc"
    canonical.mkdir(parents=True, exist_ok=True)
    (canonical / "keep.txt").write_text("keep", encoding="utf-8")

    plan, extras = service.run_command(command="repair-tree", apply_requested=True)

    assert plan is not None
    assert (config.spaces_root / "archive" / "experiments" / "misc" / ".idea" / "workspace.xml").exists()
    assert str(roots["spaces_root"] / "archive" / "experiments" / "misc-2") in extras["cleaned_empty_dirs"]


def test_repair_tree_merges_workspace_suffixed_code_wrapper_into_canonical_code_folder(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    focus_root = roots["spaces_root"] / "projects" / "workspace" / "code"
    canonical_code = focus_root / "code" / "groq-mcp-mac-agent"
    suffixed_code = focus_root / "code-2" / "shared-lib"
    canonical_code.mkdir(parents=True, exist_ok=True)
    suffixed_code.mkdir(parents=True, exist_ok=True)
    (canonical_code / "README.md").write_text("# app", encoding="utf-8")
    (suffixed_code / "package.json").write_text("{}", encoding="utf-8")

    plan, extras = service.run_command(command="repair-tree", apply_requested=True)

    assert plan is not None
    assert (config.spaces_root / "projects" / "workspace" / "code" / "shared-lib" / "package.json").exists()
    assert not (focus_root / "code-2").exists()
    assert str(focus_root / "code-2") in extras["cleaned_empty_dirs"]


def test_repair_tree_flattens_nested_embedded_project_tree(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    nested_root = roots["spaces_root"] / "projects" / "embedded" / "projects" / "embedded" / "projects"
    agents = nested_root / "apps" / "agents"
    robot = nested_root / "robot"
    agents.mkdir(parents=True, exist_ok=True)
    robot.mkdir(parents=True, exist_ok=True)
    (agents / "README.md").write_text("# agents", encoding="utf-8")
    (robot / "platformio.ini").write_text("[env:test]", encoding="utf-8")

    plan, extras = service.run_command(command="repair-tree", apply_requested=True)

    assert plan is not None
    assert (config.spaces_root / "projects" / "apps" / "agents" / "README.md").exists()
    assert (config.spaces_root / "projects" / "embedded" / "robot" / "platformio.ini").exists()
    assert str(roots["spaces_root"] / "projects" / "embedded" / "projects" / "embedded" / "projects") in extras["cleaned_empty_dirs"]


def test_repair_tree_renames_existing_review_files(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    review_docs = roots["spaces_root"] / "review" / "docs"
    review_docs.mkdir(parents=True, exist_ok=True)
    candidate = review_docs / "documents-guide-reference.pdf"
    candidate.write_text("guide", encoding="utf-8")

    plan, _ = service.run_command(command="repair-tree", apply_requested=False)

    assert plan is not None
    destinations = {(action.source_path, action.destination_path) for action in plan.actions if action.action_type == "move"}
    assert (
        candidate.absolute(),
        config.spaces_root / "review" / "docs" / "guide.pdf",
    ) in destinations


def test_repair_tree_apply_removes_ds_store_style_metadata_artifact(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    bundle = roots["spaces_root"] / "review" / "admin" / "documents-project" / "misc"
    bundle.mkdir(parents=True, exist_ok=True)
    artifact = bundle / "1. ds store [file] (uncertain)"
    artifact.write_text("metadata", encoding="utf-8")

    plan, extras = service.run_command(command="repair-tree", apply_requested=True)

    assert plan is not None
    assert not artifact.exists()
    assert str(roots["spaces_root"] / "review" / "admin") in extras["cleaned_empty_dirs"]


def test_repair_tree_repairs_noncode_names_and_review_documents_project(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    untitled = roots["spaces_root"] / "resources" / "templates" / "도전제안서-양식-루키-hwpx" / "docs" / "untitled.hwpx"
    untitled.parent.mkdir(parents=True, exist_ok=True)
    untitled.write_text("hwpx", encoding="utf-8")

    bundle = roots["spaces_root"] / "review" / "misc" / "documents-project"
    bundle.mkdir(parents=True, exist_ok=True)
    (bundle / "4-레포트-표지.pages").write_text("pages", encoding="utf-8")
    (bundle / "editor.xml").write_text("<xml/>", encoding="utf-8")
    (bundle / "1. ds store [file] (uncertain)").write_text("metadata", encoding="utf-8")

    plan, _ = service.run_command(command="repair-tree", apply_requested=True)

    assert plan is not None
    assert (config.spaces_root / "resources" / "templates" / "루키-제안서-양식" / "docs" / "도전제안서.hwpx").exists()
    assert (config.spaces_root / "review" / "docs" / "4-레포트-표지.pages").exists()
    assert (config.spaces_root / "review" / "misc" / "documents-project" / "editor.xml").exists()
    assert not (bundle / "1. ds store [file] (uncertain)").exists()


def test_repair_tree_restores_documents_project_metadata_filenames(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    bundle = roots["spaces_root"] / "review" / "misc" / "documents-project"
    bundle.mkdir(parents=True, exist_ok=True)
    (bundle / "idea-config.xml").write_text("<workspace/>", encoding="utf-8")
    (bundle / "idea-config.iml").write_text("<module/>", encoding="utf-8")

    plan, _ = service.run_command(command="repair-tree", apply_requested=True)

    assert plan is not None
    assert (bundle / "workspace.xml").exists()
    assert (bundle / "Documents.iml").exists()
    assert not (bundle / "idea-config.xml").exists()
    assert not (bundle / "idea-config.iml").exists()


def test_repair_tree_moves_ipynb_to_code_and_quarantines_temp_runner(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    misc_root = roots["spaces_root"] / "resources" / "education" / "실습파일-사전배포" / "misc"
    misc_root.mkdir(parents=True, exist_ok=True)
    notebook = misc_root / "분석예제01.코로나데이터분석(solved).ipynb"
    notebook.write_text("{}", encoding="utf-8")
    temp_notebook = misc_root / "tempCodeRunnerFile.ipynb"
    temp_notebook.write_text("{}", encoding="utf-8")

    plan, _ = service.run_command(command="repair-tree", apply_requested=True)

    assert plan is not None
    assert (config.spaces_root / "resources" / "education" / "코로나-데이터-분석" / "code" / "분석예제01.코로나데이터분석(solved).ipynb").exists()
    proposals = service.database.list_deletion_proposals("pending")
    assert any(row["reason"] == "temporary runner artifact detected during tree repair" for row in proposals)


def test_repair_tree_flattens_template_dot_config_bundles_into_data(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    vscode_root = roots["spaces_root"] / "resources" / "templates" / "vscode-settings" / ".vscode"
    vscode_root.mkdir(parents=True, exist_ok=True)
    launch_json = vscode_root / "launch.json"
    launch_json.write_text("{}", encoding="utf-8")

    plan, _ = service.run_command(command="repair-tree", apply_requested=False)

    assert plan is not None
    destinations = {(action.source_path, action.destination_path) for action in plan.actions if action.action_type == "move"}
    assert (
        launch_json.absolute(),
        roots["spaces_root"] / "resources" / "templates" / "vscode-settings" / "data" / "launch.json",
    ) in destinations


def test_repair_tree_removes_empty_project_code_wrapper_with_only_ds_store(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    wrapper = roots["spaces_root"] / "projects" / "apps" / "demo-app" / "code"
    wrapper.mkdir(parents=True, exist_ok=True)
    (wrapper / ".DS_Store").write_text("meta", encoding="utf-8")

    plan, extras = service.run_command(command="repair-tree", apply_requested=True)

    assert plan is not None
    assert not wrapper.exists()
    assert str(wrapper) in extras["cleaned_empty_dirs"]


def test_watch_ignores_project_internal_file_changes(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    service.config.watch_roots = (roots["watch_root"], roots["spaces_root"])
    target = roots["spaces_root"] / "projects" / "apps" / "demo-app" / "src" / "index.js"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("console.log('hi')", encoding="utf-8")

    service._queue_watch_path(target)  # type: ignore[attr-defined]
    plan, _ = service.run_watch_cycle(apply_requested=False)

    assert plan.summary()["total"] == 0


def test_watch_still_ignores_top_level_project_internal_file_changes(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    service.config.watch_roots = (roots["watch_root"], roots["spaces_root"])
    project_root = roots["spaces_root"] / "demo-app"
    (project_root / "src").mkdir(parents=True, exist_ok=True)
    (project_root / "package.json").write_text("{}", encoding="utf-8")
    target = project_root / "src" / "index.js"
    target.write_text("console.log('hi')", encoding="utf-8")

    service._queue_watch_path(target)  # type: ignore[attr-defined]
    plan, _ = service.run_watch_cycle(apply_requested=False)

    assert plan.summary()["total"] == 0


def test_repair_tree_moves_loose_top_level_roots_into_canonical_locations(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    project_root = roots["spaces_root"] / "deck-builder"
    (project_root / "src").mkdir(parents=True, exist_ok=True)
    (project_root / "package.json").write_text("{}", encoding="utf-8")
    loose_wrapper = roots["spaces_root"] / "다운로드-2"
    (loose_wrapper / "nested").mkdir(parents=True, exist_ok=True)
    (loose_wrapper / "nested" / "project-alpha-plan.txt").write_text("project alpha planning payload", encoding="utf-8")
    (loose_wrapper / "nested" / "project-alpha-notes.txt").write_text("project alpha notes payload", encoding="utf-8")

    plan, _ = service.run_command(command="repair-tree", apply_requested=False)

    assert plan is not None
    moves = {action.source_path: action.destination_path for action in plan.actions if action.action_type == "move"}
    assert moves[project_root.absolute()] == config.spaces_root / "projects" / "apps" / "deck-builder"
    assert moves[loose_wrapper.absolute()] == config.spaces_root / "review" / "misc" / "project-alpha"


def test_watch_ignores_deep_managed_documents_paths_but_accepts_loose_drop_folders(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    service.config.watch_roots = (roots["watch_root"], roots["spaces_root"])

    managed = roots["spaces_root"] / "areas" / "admin" / "misc" / "Downloads" / "guide.pdf"
    managed.parent.mkdir(parents=True, exist_ok=True)
    managed.write_text("pdf", encoding="utf-8")

    loose = roots["spaces_root"] / "Loose Drop" / "nested" / "manual-spec.docx"
    loose.parent.mkdir(parents=True, exist_ok=True)
    loose.write_text("docx", encoding="utf-8")

    service._queue_watch_path(managed)  # type: ignore[attr-defined]
    service._queue_watch_path(loose)  # type: ignore[attr-defined]
    service.run_watch_cycle(apply_requested=False)
    plan, _ = service.run_watch_cycle(apply_requested=False)

    moves = [action for action in plan.actions if action.action_type == "move"]
    assert len(moves) == 1
    assert moves[0].source_path == loose.absolute()
    assert moves[0].destination_path == config.spaces_root / "review" / "docs" / "manual-spec.docx"
    assert all(action.source_path != managed.absolute() for action in plan.actions)


def test_watch_ignores_hidden_cloud_temp_roots_in_documents(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    service.config.watch_roots = (roots["watch_root"], roots["spaces_root"])

    hidden_temp = roots["spaces_root"] / ".tmp.drivedownload"
    hidden_temp.mkdir(parents=True, exist_ok=True)
    hidden_file = hidden_temp / "partial.docx"
    hidden_file.write_text("payload", encoding="utf-8")

    service._queue_watch_path(hidden_temp)  # type: ignore[attr-defined]
    service._queue_watch_path(hidden_file)  # type: ignore[attr-defined]
    plan, _ = service.run_watch_cycle(apply_requested=False)

    assert plan.summary()["total"] == 0
    assert service.database.list_staging_entries() == []


def test_watch_ignores_canonical_projects_tree_paths(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    service.config.watch_roots = (roots["watch_root"], roots["spaces_root"])
    project_unit = roots["spaces_root"] / "projects" / "experiments" / "c-project"
    project_unit.mkdir(parents=True, exist_ok=True)

    service._queue_watch_path(project_unit)  # type: ignore[attr-defined]
    plan, _ = service.run_watch_cycle(apply_requested=False)

    assert plan.summary()["total"] == 0


def test_plan_does_not_duplicate_canonical_stream_actions_when_documents_is_watched(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    service.config.watch_roots = (roots["watch_root"], roots["spaces_root"])
    target_dir = roots["spaces_root"] / "areas" / "education" / "docs"
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / "26-1-coding_v0-0.pdf"
    target.write_text("pdf", encoding="utf-8")

    plan, _ = service.run_command(command="plan", apply_requested=False)

    assert plan is not None
    renames = [action for action in plan.actions if action.action_type == "rename" and action.source_path == target.absolute()]
    assert len(renames) <= 1


def test_plan_skips_pure_cosmetic_renames_in_canonical_locations(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    target_dir = roots["spaces_root"] / "resources" / "templates" / "vscode-settings" / "data"
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / "c_cpp_properties.json"
    target.write_text("{}", encoding="utf-8")

    plan, _ = service.run_command(command="plan", apply_requested=False)

    assert plan is not None
    renames = [action for action in plan.actions if action.action_type == "rename" and action.source_path == target.absolute()]
    assert renames == []


def test_repair_code_names_reports_suspicious_non_git_code_tree_without_moves(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    code_root = roots["spaces_root"] / "projects" / "experiments" / "c-project"
    code_root.mkdir(parents=True, exist_ok=True)
    suspicious = code_root / "3. Main [code] (uncertain).c"
    suspicious.write_text("int main() { return 0; }", encoding="utf-8")

    plan, _ = service.run_command(command="repair-code-names", apply_requested=False)

    assert plan is not None
    flagged = [action for action in plan.actions if action.action_type == "flag_for_review"]
    assert any(action.source_path == suspicious.absolute() for action in flagged)
    assert not any(action.action_type in {"move", "rename"} for action in plan.actions)


def test_repair_code_names_apply_renames_only_legacy_review_files(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    legacy_file = roots["spaces_root"] / "projects" / "legacy-review" / "web-file-bundle" / "1. Server [code] (uncertain).js"
    legacy_file.parent.mkdir(parents=True, exist_ok=True)
    legacy_file.write_text("console.log('x')", encoding="utf-8")

    active_file = roots["spaces_root"] / "projects" / "apps" / "demo-app" / "3. Main [code] (uncertain).c"
    active_file.parent.mkdir(parents=True, exist_ok=True)
    active_file.write_text("int main() { return 0; }", encoding="utf-8")

    plan, _ = service.run_command(command="repair-code-names", apply_requested=True)

    assert plan is not None
    assert legacy_file.with_name("server.js").exists()
    assert active_file.exists()


def test_repair_code_names_apply_leaves_generic_untitled_collisions_for_review(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    legacy_dir = roots["spaces_root"] / "projects" / "legacy-review" / "web-file-bundle"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    first = legacy_dir / "1. { [file].json"
    second = legacy_dir / "2. { [file].json"
    first.write_text("{}", encoding="utf-8")
    second.write_text("{}", encoding="utf-8")

    plan, _ = service.run_command(command="repair-code-names", apply_requested=True)

    assert plan is not None
    assert first.exists()
    assert second.exists()
    flagged = [action for action in plan.actions if action.action_type == "flag_for_review"]
    assert any(action.source_path == first.absolute() for action in flagged)
    assert any(action.source_path == second.absolute() for action in flagged)


def test_repair_code_names_skips_tracked_git_files(make_v2_service) -> None:
    service, _, roots = make_v2_service()
    repo_root = roots["spaces_root"] / "projects" / "apps" / "demo-app"
    repo_root.mkdir(parents=True, exist_ok=True)
    suspicious = repo_root / "3. Main [code] (uncertain).c"
    suspicious.write_text("int main() { return 0; }", encoding="utf-8")

    subprocess.run(["git", "init", "-q", str(repo_root)], check=True)
    subprocess.run(["git", "-C", str(repo_root), "add", suspicious.name], check=True)

    plan, _ = service.run_command(command="repair-code-names", apply_requested=False)

    assert plan is not None
    flagged = [action for action in plan.actions if action.action_type == "flag_for_review"]
    assert not any(action.source_path == suspicious.absolute() for action in flagged)


def test_housekeeping_removes_metadata_prunes_empty_stream_and_watch_reports(make_v2_service) -> None:
    service, _, roots = make_v2_service({"reporting": {"watch_retention_days": 1, "watch_max_report_pairs": 0}})
    (roots["spaces_root"] / ".DS_Store").write_text("meta", encoding="utf-8")
    archive_root = roots["spaces_root"] / "archive"
    archive_root.mkdir(parents=True, exist_ok=True)
    (archive_root / ".DS_Store").write_text("meta", encoding="utf-8")
    empty_watch_wrapper = roots["watch_root"] / "docs"
    empty_watch_wrapper.mkdir(parents=True, exist_ok=True)
    reports_dir = roots["state_dir"] / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    old_json = reports_dir / "index_organizer_v2_watch_20240101_010101.json"
    old_md = reports_dir / "index_organizer_v2_watch_20240101_010101.md"
    old_json.write_text("{}", encoding="utf-8")
    old_md.write_text("# old", encoding="utf-8")
    stale = time.time() - (20 * 86400)
    os.utime(old_json, (stale, stale))
    os.utime(old_md, (stale, stale))

    result = service._run_housekeeping(apply_requested=True)  # type: ignore[attr-defined]

    assert not (roots["spaces_root"] / ".DS_Store").exists()
    assert not archive_root.exists()
    assert not empty_watch_wrapper.exists()
    assert result["pruned_watch_reports"] == 2
    assert any(path.endswith("/Downloads/docs") for path in result["pruned_empty_watch_roots"])


def test_housekeeping_prunes_watch_reports_by_count_even_when_recent(make_v2_service) -> None:
    service, _, roots = make_v2_service({"reporting": {"watch_retention_days": 365, "watch_max_report_pairs": 2}})
    reports_dir = roots["state_dir"] / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    for index in range(4):
        stamp = f"20260401_01010{index}"
        json_path = reports_dir / f"index_organizer_v2_watch_{stamp}.json"
        md_path = reports_dir / f"index_organizer_v2_watch_{stamp}.md"
        json_path.write_text("{}", encoding="utf-8")
        md_path.write_text("# report", encoding="utf-8")
        current = time.time() - (3 - index)
        os.utime(json_path, (current, current))
        os.utime(md_path, (current, current))

    result = service._run_housekeeping(apply_requested=True)  # type: ignore[attr-defined]

    assert result["pruned_watch_reports"] == 4
    assert len(list(reports_dir.glob("index_organizer_v2_watch_*.json"))) == 2
    assert len(list(reports_dir.glob("index_organizer_v2_watch_*.md"))) == 2


def test_housekeeping_normalizes_hidden_review_layout_and_rehomes_blocked_top_level_dirs(make_v2_service) -> None:
    service, config, roots = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
            },
        }
    )
    legacy_review_doc = config.adaptive_review_root / "admin-docs" / "docs" / "서비스-정의서.docx"
    legacy_review_doc.parent.mkdir(parents=True, exist_ok=True)
    legacy_review_doc.write_text("doc", encoding="utf-8")
    legacy_review_asset = config.adaptive_review_root / "photos" / "assets" / "캡처.png"
    legacy_review_asset.parent.mkdir(parents=True, exist_ok=True)
    legacy_review_asset.write_text("png", encoding="utf-8")

    blocked_top_level = roots["spaces_root"] / "platformio-build-cache"
    blocked_top_level.mkdir(parents=True, exist_ok=True)
    (blocked_top_level / "state.json").write_text("{}", encoding="utf-8")

    result = service._run_housekeeping(apply_requested=True)  # type: ignore[attr-defined]

    assert (config.adaptive_review_root / "docs" / "admin-docs" / "서비스-정의서.docx").exists()
    assert (config.adaptive_review_root / "assets" / "photos" / "캡처.png").exists()
    assert (config.adaptive_review_root / "blocked-top-level" / "platformio-build-cache" / "state.json").exists()
    assert not blocked_top_level.exists()
    assert any("blocked-top-level/platformio-build-cache" in path for path in result["relocated_blocked_top_level_dirs"])


def test_status_plan_uses_ephemeral_state_dir(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    target = roots["watch_root"] / "report.pdf"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("payload", encoding="utf-8")

    state_dir = roots["state_dir"]
    original_mode = state_dir.stat().st_mode
    state_dir.chmod(0o555)
    try:
        plan, extras = index_organizer._build_status_plan(config)
    finally:
        state_dir.chmod(original_mode)

    assert plan.summary()["total"] > 0
    assert extras["report_json"] == "-"
    assert extras["report_md"] == "-"


def test_status_helpers_report_provider_and_staging_state(make_v2_service, monkeypatch) -> None:
    service, config, roots = make_v2_service(
        {
            "groq": {"enabled": True},
            "llm": {"fallback_to_ollama": True},
        }
    )
    target = roots["watch_root"] / "deferred.docx"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("payload", encoding="utf-8")

    future = (datetime.now(timezone.utc) + timedelta(minutes=10)).replace(microsecond=0).isoformat()
    service.database.upsert_provider_state(
        provider="groq",
        cooldown_until=future,
        last_error_code="rate_limit",
        consecutive_rate_limits=2,
    )
    service.database.upsert_staging_entry(
        path=target.absolute(),
        root_path=roots["watch_root"],
        size=target.stat().st_size,
        mtime=target.stat().st_mtime,
        stable_count=2,
        stable_since=future,
        gate_state="stable_candidate",
        defer_until=future,
        attempt_count=1,
        last_error_code="rate_limit",
        last_provider="groq",
    )

    partial = roots["watch_root"] / "partial.crdownload"
    partial.write_text("partial", encoding="utf-8")
    service.database.upsert_staging_entry(
        path=partial.absolute(),
        root_path=roots["watch_root"],
        size=partial.stat().st_size,
        mtime=partial.stat().st_mtime,
        stable_count=0,
        stable_since=None,
        gate_state="incomplete_or_transient",
    )

    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setattr(index_organizer, "_ollama_ready", lambda _config: True)

    llm_status = index_organizer._llm_runtime_status(config)
    staging = index_organizer._staging_snapshot(config)

    assert llm_status["preferred_provider"] == "groq"
    assert llm_status["providers"]["groq"]["configured"] is True
    assert llm_status["providers"]["groq"]["cooling_down"] is True
    assert llm_status["providers"]["groq"]["last_error_code"] == "rate_limit"
    assert "ollama" in llm_status["active_providers"]

    assert staging["total"] == 2
    assert staging["deferred"] == 1
    assert staging["ready"] == 0
    assert staging["by_gate_state"]["stable_candidate"] == 1
    assert staging["by_gate_state"]["incomplete_or_transient"] == 1
    assert staging["next_defer_until"] == future


def test_service_tick_startup_apply_skips_full_report_plan(make_v2_service) -> None:
    service, _, _ = make_v2_service(
        {
            "service": {
                "startup_apply": True,
                "startup_archive": False,
            }
        }
    )

    original_run_command = service.run_command

    def wrapped_run_command(*, command: str, **kwargs):
        if command == "report":
            raise AssertionError("service tick should not run full report plans in the background loop")
        return original_run_command(command=command, **kwargs)

    service.run_command = wrapped_run_command  # type: ignore[method-assign]

    payload = service.run_service_tick(apply_requested=False)

    assert "watch" in payload
    assert "maintenance" in payload
    assert "report" not in payload


def test_service_tick_runs_adaptive_review_drain_when_backlog_exists(make_v2_service) -> None:
    service, config, _ = make_v2_service(
        {
            "adaptive_placement": {
                "enabled": True,
                "hidden_review_relative": "adaptive-review",
                "auto_drain_hidden_review": True,
                "hidden_review_drain_interval_seconds": 0,
            },
            "service": {
                "startup_apply": False,
                "startup_archive": False,
            },
            "pattern_overrides": [],
        }
    )
    hidden_bundle = config.adaptive_review_root / "assets" / "photos"
    hidden_bundle.mkdir(parents=True, exist_ok=True)
    (hidden_bundle / "shot.png").write_text("png", encoding="utf-8")

    payload = service.run_service_tick(apply_requested=True)

    assert "adaptive_review" in payload
    assert payload["adaptive_review"]["summary"]["move"] >= 1
    assert (config.spaces_root / "photos" / "shot.png").exists()
    assert not hidden_bundle.exists()


def test_stabilize_apply_runs_repair_stack_and_records_component_summaries(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    coding_root = roots["spaces_root"] / "projects" / "coding"
    (coding_root / "pdf-quiz-app" / "src").mkdir(parents=True, exist_ok=True)
    (coding_root / "pdf-quiz-app" / "package.json").write_text("{}", encoding="utf-8")
    (coding_root / "pdf-quiz-app" / "src" / "index.js").write_text("console.log('x')", encoding="utf-8")

    suffixed = roots["spaces_root"] / "archive" / "experiments" / "misc-2"
    (suffixed / ".idea").mkdir(parents=True, exist_ok=True)
    (suffixed / ".idea" / "workspace.xml").write_text("<xml/>", encoding="utf-8")

    legacy_file = roots["spaces_root"] / "projects" / "legacy-review" / "web-file-bundle" / "1. Server [code] (uncertain).js"
    legacy_file.parent.mkdir(parents=True, exist_ok=True)
    legacy_file.write_text("console.log('x')", encoding="utf-8")

    plan, extras = service.run_command(command="stabilize", apply_requested=True)

    assert plan is not None
    assert plan.command == "stabilize"
    assert (config.spaces_root / "projects" / "apps" / "pdf-quiz-app").exists()
    assert (config.spaces_root / "archive" / "experiments" / "misc" / ".idea" / "workspace.xml").exists()
    assert legacy_file.with_name("server.js").exists()
    assert Path(extras["report_json"]).exists()
    assert Path(extras["report_md"]).exists()
    component_summaries = extras["component_summaries"]
    assert component_summaries["repair-projects"]["total"] > 0
    assert component_summaries["repair-tree"]["total"] > 0
    assert component_summaries["repair-code-names"]["total"] > 0


def test_repair_tree_rehomes_residual_unknown_and_download_wrappers(make_v2_service) -> None:
    service, config, roots = make_v2_service()
    working_rules = roots["spaces_root"] / "resources" / "unknown" / "general" / "notes" / "2-desktop-working-rules.md"
    working_rules.parent.mkdir(parents=True, exist_ok=True)
    working_rules.write_text("# rules", encoding="utf-8")

    slide = roots["spaces_root"] / "resources" / "user_data" / "Downloads" / "Outline SWOT Analysis.pptx"
    slide.parent.mkdir(parents=True, exist_ok=True)
    slide.write_text("slides", encoding="utf-8")

    unsorted_test = roots["spaces_root"] / "resources" / "unsorted" / "usability-depth-test-2.txt"
    unsorted_test.parent.mkdir(parents=True, exist_ok=True)
    unsorted_test.write_text("test", encoding="utf-8")

    system_test = roots["spaces_root"] / "system" / "giminu0930" / "Downloads" / "usability_depth_test.txt"
    system_test.parent.mkdir(parents=True, exist_ok=True)
    system_test.write_text("test", encoding="utf-8")

    plan, _ = service.run_command(command="repair-tree", apply_requested=True)

    assert plan is not None
    assert (config.spaces_root / "resources" / "coding" / "notes" / "desktop-working-rules.md").exists()
    assert (config.spaces_root / "review" / "slides" / "outline-swot-analysis.pptx").exists()

    proposals = service.database.list_deletion_proposals("pending")
    reasons = [row["reason"] for row in proposals]
    assert reasons.count("low-value organizer depth/usability test artifact detected") == 2
    assert not (config.spaces_root / "resources" / "unsorted").exists()
    assert not (config.spaces_root / "system").exists()
