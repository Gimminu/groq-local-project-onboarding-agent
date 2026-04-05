from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_module(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise AssertionError(f"Failed to load spec for {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_documents_structure_audit_suggest_bucket_and_summary(tmp_path: Path) -> None:
    module = _load_module(
        "documents_structure_audit_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_structure_audit.py",
    )

    keep_dir = tmp_path / "Obsidian"
    keep_dir.mkdir()
    (keep_dir / "A.md").write_text("x", encoding="utf-8")

    hidden_dir = tmp_path / ".tmp.driveupload"
    hidden_dir.mkdir()

    assert module.suggest_bucket("Obsidian") == "keep_root"
    assert module.suggest_bucket("education-docs") == "collections"
    assert module.suggest_bucket("영상") == "archive_legacy"
    assert module.suggest_bucket(".tmp.driveupload") == "temp_holdout"
    assert module.suggest_bucket("anything-else") == "holdout"

    keep_summary = module.summarize_entry(keep_dir)
    hidden_summary = module.summarize_entry(hidden_dir)

    assert keep_summary.kind == "dir"
    assert keep_summary.children == 1
    assert keep_summary.suggestion == "keep_root"

    assert hidden_summary.kind == "dir"
    assert hidden_summary.suggestion == "temp_holdout"


def test_documents_rehome_manifest_builds_expected_low_risk_plans(tmp_path: Path) -> None:
    module = _load_module(
        "documents_rehome_manifest_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_rehome_manifest.py",
    )

    (tmp_path / "education-docs").mkdir()
    (tmp_path / "admin-docs").mkdir()
    (tmp_path / "영상").mkdir()
    (tmp_path / "mobile-manipulator-robot").mkdir()  # holdout, should not be planned

    plans = module._build_plans(tmp_path)
    planned_pairs = {(plan.source.name, plan.destination.relative_to(tmp_path).as_posix()) for plan in plans}

    assert ("education-docs", "Collections/education-docs") in planned_pairs
    assert ("admin-docs", "Collections/admin-docs") in planned_pairs
    assert ("영상", "Archive/Legacy-KR/영상") in planned_pairs
    assert all(source != "mobile-manipulator-robot" for source, _dest in planned_pairs)


def test_documents_rehome_manifest_apply_move_moves_directory(tmp_path: Path) -> None:
    module = _load_module(
        "documents_rehome_manifest_apply_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_rehome_manifest.py",
    )

    source = tmp_path / "legal"
    source.mkdir()
    (source / "form.hwpx").write_text("data", encoding="utf-8")

    destination = tmp_path / "Collections" / "legal"
    plan = module.MovePlan(source=source, destination=destination, reason="collection_docs")

    success, result = module._apply_move(plan)

    assert success is True
    assert result == "moved"
    assert not source.exists()
    assert (destination / "form.hwpx").exists()


def test_documents_type_rehome_bucket_mapping() -> None:
    module = _load_module(
        "documents_type_rehome_bucket_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_type_rehome.py",
    )

    assert module._bucket_for_extension(".pdf") == "documents/pdf"
    assert module._bucket_for_extension(".hwpx") == "documents/hwp"
    assert module._bucket_for_extension(".mp4") == "media/video"
    assert module._bucket_for_extension(".mp3") == "media/audio"
    assert module._bucket_for_extension(".zip") == "archives/compressed"
    assert module._bucket_for_extension("") == "other/noext"
    assert module._bucket_for_extension(".unknown") == "other/files"


def test_documents_type_rehome_build_plans_and_apply(tmp_path: Path) -> None:
    module = _load_module(
        "documents_type_rehome_apply_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_type_rehome.py",
    )

    documents_root = tmp_path
    source_collections = documents_root / "Collections"
    source_archive = documents_root / "Archive" / "Legacy-KR"
    source_collections.mkdir(parents=True)
    source_archive.mkdir(parents=True)

    pdf_file = source_collections / "admin-docs" / "service.docx"
    pdf_file.parent.mkdir(parents=True)
    pdf_file.write_text("doc", encoding="utf-8")

    media_file = source_archive / "영상" / "lesson.mp4"
    media_file.parent.mkdir(parents=True)
    media_file.write_text("video", encoding="utf-8")

    plans = module.build_type_plans(
        documents_root=documents_root,
        target_root=Path("Collections/ByType"),
        source_roots=(source_collections, source_archive),
        include_hidden=False,
    )

    plan_sources = {plan.source.name for plan in plans}
    assert "service.docx" in plan_sources
    assert "lesson.mp4" in plan_sources

    service_plan = next(plan for plan in plans if plan.source.name == "service.docx")
    ok, result, final_dest = module._apply_move(service_plan)
    assert ok is True
    assert result == "moved"
    assert final_dest is not None
    assert final_dest.exists()
    assert final_dest.as_posix().endswith("Collections/ByType/documents/word/Collections/admin-docs/service.docx")


def test_documents_structure_audit_collect_extension_counts(tmp_path: Path) -> None:
    module = _load_module(
        "documents_structure_audit_ext_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_structure_audit.py",
    )

    (tmp_path / "Collections" / "education-docs").mkdir(parents=True)
    (tmp_path / "Archive" / "Legacy-KR" / "영상").mkdir(parents=True)

    (tmp_path / "Collections" / "education-docs" / "a.pdf").write_text("pdf", encoding="utf-8")
    (tmp_path / "Collections" / "education-docs" / "b.pdf").write_text("pdf", encoding="utf-8")
    (tmp_path / "Archive" / "Legacy-KR" / "영상" / "c.mp4").write_text("mp4", encoding="utf-8")

    counts = module.collect_extension_counts(tmp_path)

    assert counts[".pdf"] == 2
    assert counts[".mp4"] == 1


def test_documents_type_rehome_skips_existing_target_subtree(tmp_path: Path) -> None:
    module = _load_module(
        "documents_type_rehome_skip_target_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_type_rehome.py",
    )

    documents_root = tmp_path
    source_collections = documents_root / "Collections"
    source_collections.mkdir(parents=True)

    source_file = source_collections / "education-docs" / "lecture.pdf"
    source_file.parent.mkdir(parents=True)
    source_file.write_text("pdf", encoding="utf-8")

    already_typed = documents_root / "Collections" / "ByType" / "documents" / "pdf" / "Collections" / "education-docs" / "lecture.pdf"
    already_typed.parent.mkdir(parents=True)
    already_typed.write_text("typed", encoding="utf-8")

    plans = module.build_type_plans(
        documents_root=documents_root,
        target_root=Path("Collections/ByType"),
        source_roots=(source_collections,),
        include_hidden=False,
    )

    sources = [plan.source for plan in plans]
    assert source_file in sources
    assert already_typed not in sources


def test_documents_holdout_rehome_builds_expected_plans(tmp_path: Path) -> None:
    module = _load_module(
        "documents_holdout_rehome_plan_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_holdout_rehome.py",
    )

    (tmp_path / "agent-workflows").mkdir()
    (tmp_path / "libraries").mkdir()
    (tmp_path / "Obsidian").mkdir()  # keep-root, not part of move policy

    plans = module.build_plans(tmp_path)
    pairs = {(plan.source.name, plan.destination.relative_to(tmp_path).as_posix()) for plan in plans}

    assert ("agent-workflows", "Projects/Active/agent-workflows") in pairs
    assert ("libraries", "Projects/Shared/libraries") in pairs
    assert all(source != "Obsidian" for source, _ in pairs)


def test_documents_holdout_rehome_apply_move_and_skip_existing_destination(tmp_path: Path) -> None:
    module = _load_module(
        "documents_holdout_rehome_apply_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_holdout_rehome.py",
    )

    source = tmp_path / "pdf-quiz-grader"
    source.mkdir()
    (source / "README.md").write_text("x", encoding="utf-8")

    destination = tmp_path / "Projects" / "Active" / "pdf-quiz-grader"
    plan = module.MovePlan(source=source, destination=destination, reason="project_active")

    ok, result = module._apply_move(plan)
    assert ok is True
    assert result == "moved"
    assert (destination / "README.md").exists()

    second_source = tmp_path / "presentation-deck-builder"
    second_source.mkdir()
    (second_source / "notes.txt").write_text("y", encoding="utf-8")
    existing_destination = tmp_path / "Projects" / "Active" / "presentation-deck-builder"
    existing_destination.mkdir(parents=True)

    second_plan = module.MovePlan(source=second_source, destination=existing_destination, reason="project_active")
    ok2, result2 = module._apply_move(second_plan)
    assert ok2 is False
    assert result2 == "skip_destination_exists"
    assert second_source.exists()


def test_documents_depth_rebalance_builds_semantic_destinations(tmp_path: Path) -> None:
    module = _load_module(
        "documents_depth_rebalance_plan_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_depth_rebalance.py",
    )

    documents_root = tmp_path
    source_root = documents_root / "Collections" / "ByType"
    a = source_root / "documents" / "pdf" / "Collections" / "education-docs"
    b = source_root / "archives" / "compressed" / "Archive__Legacy-KR" / "코로나-데이터-분석"
    a.mkdir(parents=True)
    b.mkdir(parents=True)
    (a / "lecture.pdf").write_text("x", encoding="utf-8")
    (b / "half.zip").write_text("y", encoding="utf-8")

    plans = module.build_rebalance_plans(
        documents_root=documents_root,
        source_root=source_root,
        include_hidden=False,
    )

    by_name = {plan.source.name: plan for plan in plans}
    assert by_name["lecture.pdf"].destination.as_posix().endswith(
        "Collections/education-docs/documents/pdf/lecture.pdf"
    )
    assert by_name["half.zip"].destination.as_posix().endswith(
        "Archive/Legacy-KR/코로나-데이터-분석/archives/compressed/half.zip"
    )


def test_documents_depth_rebalance_apply_moves_file(tmp_path: Path) -> None:
    module = _load_module(
        "documents_depth_rebalance_apply_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_depth_rebalance.py",
    )

    documents_root = tmp_path
    source_root = documents_root / "Collections" / "ByType"
    source_dir = source_root / "media" / "audio" / "프로젝트"
    source_dir.mkdir(parents=True)
    source_file = source_dir / "sample.mp3"
    source_file.write_text("audio", encoding="utf-8")

    plans = module.build_rebalance_plans(
        documents_root=documents_root,
        source_root=source_root,
        include_hidden=False,
    )
    plan = next(item for item in plans if item.source.name == "sample.mp3")

    ok, result, final_destination = module._apply_move(plan)
    assert ok is True
    assert result == "moved"
    assert final_destination is not None
    assert final_destination.exists()
    assert final_destination.as_posix().endswith("Projects/Legacy-KR/프로젝트/media/audio/sample.mp3")


def test_documents_korean_taxonomy_repair_promotes_to_root_and_flattens_non_periodic_year(tmp_path: Path) -> None:
    module = _load_module(
        "documents_korean_taxonomy_repair_plan_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_korean_taxonomy_repair.py",
    )

    documents_root = tmp_path
    (documents_root / "006_서비스_정의").mkdir()
    source_root = documents_root / "Collections"
    source_dir = source_root / "006_서비스_정의" / "01_서비스_정의서" / "2026"
    source_dir.mkdir(parents=True)
    source_file = source_dir / "프로젝트-서비스-정의서_v01_giminu0930.docx"
    source_file.write_text("x", encoding="utf-8")

    plans = module.build_repair_plans(
        documents_root=documents_root,
        source_root=source_root,
        min_files_for_new_top=2,
        fallback_top_category="090_보류_통합",
    )
    assert len(plans) == 1
    plan = plans[0]

    assert plan.destination.as_posix().startswith((documents_root / "006_서비스_정의").as_posix())
    assert "/2026/" not in plan.destination.as_posix()
    assert plan.destination.name.endswith("_서비스정의.docx")


def test_documents_korean_taxonomy_repair_keeps_period_for_periodic_top(tmp_path: Path) -> None:
    module = _load_module(
        "documents_korean_taxonomy_repair_period_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_korean_taxonomy_repair.py",
    )

    documents_root = tmp_path
    source_root = documents_root / "Collections"
    source_dir = source_root / "003_법무_양식" / "01_법무_동의_양식" / "2026"
    source_dir.mkdir(parents=True)
    source_file = source_dir / "서식2-개인정보-동의서_20260403_v1.docx"
    source_file.write_text("y", encoding="utf-8")

    plans = module.build_repair_plans(
        documents_root=documents_root,
        source_root=source_root,
        min_files_for_new_top=2,
        fallback_top_category="090_보류_통합",
    )
    assert len(plans) == 1
    plan = plans[0]

    assert "/2026/" in plan.destination.as_posix()
    assert plan.destination.name == source_file.name


def test_documents_korean_taxonomy_repair_adds_version_only_on_collision(tmp_path: Path) -> None:
    module = _load_module(
        "documents_korean_taxonomy_repair_collision_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_korean_taxonomy_repair.py",
    )

    destination = tmp_path / "001_제안_산학_루키" / "01_루키_제안서" / "도전제안서_양식_20260403_제안산학.hwpx"
    destination.parent.mkdir(parents=True)
    destination.write_text("existing", encoding="utf-8")

    source = tmp_path / "incoming.hwpx"
    source.write_text("incoming", encoding="utf-8")

    plan = module.RepairPlan(
        source=source,
        destination=destination,
        top_category="001_제안_산학_루키",
        reason="promote_and_normalize",
    )

    ok, result, final_destination = module._apply_plan(plan)
    assert ok is True
    assert result == "moved"
    assert final_destination is not None
    assert final_destination.name == "도전제안서_양식_20260403_제안산학_버전02.hwpx"


def test_documents_korean_taxonomy_repair_skips_project_code_files(tmp_path: Path) -> None:
    module = _load_module(
        "documents_korean_taxonomy_repair_project_code_skip_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_korean_taxonomy_repair.py",
    )

    documents_root = tmp_path
    source_root = documents_root / "Collections"
    code_dir = source_root / "008_프로젝트_문서" / "demo_project" / "src"
    doc_dir = source_root / "008_프로젝트_문서" / "demo_project" / "문서"
    code_dir.mkdir(parents=True)
    doc_dir.mkdir(parents=True)

    (code_dir / "index.js").write_text("console.log('x')", encoding="utf-8")
    (doc_dir / "요구사항정리_20260405_프로젝트문서.docx").write_text("doc", encoding="utf-8")

    plans = module.build_repair_plans(
        documents_root=documents_root,
        source_root=source_root,
        min_files_for_new_top=2,
        fallback_top_category="090_보류_통합",
    )
    names = {plan.source.name for plan in plans}
    project_doc_plan = next(plan for plan in plans if plan.source.name == "요구사항정리_20260405_프로젝트문서.docx")

    assert "index.js" not in names
    assert "요구사항정리_20260405_프로젝트문서.docx" in names
    assert "/000_프로젝트/" in project_doc_plan.destination.as_posix()


def test_documents_korean_taxonomy_repair_merges_sparse_new_top_category(tmp_path: Path) -> None:
    module = _load_module(
        "documents_korean_taxonomy_repair_sparse_merge_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_korean_taxonomy_repair.py",
    )

    documents_root = tmp_path
    source_root = documents_root / "Collections"
    sparse_dir = source_root / "008_단발성_자료" / "01_메모"
    sparse_dir.mkdir(parents=True)
    (sparse_dir / "임시메모_20260405.docx").write_text("memo", encoding="utf-8")

    plans = module.build_repair_plans(
        documents_root=documents_root,
        source_root=source_root,
        min_files_for_new_top=2,
        fallback_top_category="090_보류_통합",
    )

    assert len(plans) == 1
    assert "/090_보류_통합/008_단발성_자료/" in plans[0].destination.as_posix()
    assert plans[0].reason == "merge_sparse_top"


def test_documents_korean_taxonomy_repair_preserves_proposal_name(tmp_path: Path) -> None:
    module = _load_module(
        "documents_korean_taxonomy_repair_preserve_proposal_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_korean_taxonomy_repair.py",
    )

    documents_root = tmp_path
    source_root = documents_root / "Collections"
    source_dir = source_root / "001_제안_산학_루키" / "01_루키_제안서"
    source_dir.mkdir(parents=True)
    source_file = source_dir / "도전제안서-양식-final_v1.hwpx"
    source_file.write_text("proposal", encoding="utf-8")

    plans = module.build_repair_plans(
        documents_root=documents_root,
        source_root=source_root,
        min_files_for_new_top=2,
        fallback_top_category="090_보류_통합",
    )

    assert len(plans) == 1
    assert plans[0].destination.name == source_file.name


def test_documents_korean_taxonomy_repair_documents_only_flag_filters_binary(tmp_path: Path) -> None:
    module = _load_module(
        "documents_korean_taxonomy_repair_documents_only_test",
        Path(__file__).resolve().parents[1] / "scripts" / "documents_korean_taxonomy_repair.py",
    )

    documents_root = tmp_path
    source_root = documents_root / "Collections"
    source_dir = source_root / "005_교육_학습" / "01_교육_학습_자료"
    source_dir.mkdir(parents=True)
    (source_dir / "학습요약_20260405.pdf").write_text("doc", encoding="utf-8")
    (source_dir / "샘플영상_20260405.mp4").write_text("video", encoding="utf-8")

    plans = module.build_repair_plans(
        documents_root=documents_root,
        source_root=source_root,
        min_files_for_new_top=2,
        fallback_top_category="090_보류_통합",
        documents_only=True,
    )

    names = {plan.source.name for plan in plans}
    assert "학습요약_20260405.pdf" in names
    assert "샘플영상_20260405.mp4" not in names
