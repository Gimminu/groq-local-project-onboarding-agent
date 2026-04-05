from __future__ import annotations

from pathlib import Path

from app.index_v2.naming import normalize_filename
from app.index_v2.types import ClassificationResult


def test_leading_date_and_explicit_version_are_normalized(make_v2_service) -> None:
    service, config, _ = make_v2_service()
    classification = ClassificationResult(
        space="learning",
        stream="resources",
        domain="education",
        focus="linear-algebra",
        asset_type="docs",
        confidence=0.9,
        rationale="fixture",
        source="rule",
    )

    result = normalize_filename(Path("/tmp/2026-03-31_Education_과제_제안서_v1.pdf"), classification, config)

    assert result.filename == "과제-제안서__v01.pdf"
    assert result.removed_date_token == "2026-03-31"


def test_copy_counter_is_promoted_to_version(make_v2_service) -> None:
    service, config, _ = make_v2_service()
    classification = ClassificationResult(
        space="learning",
        stream="resources",
        domain="education",
        focus="statistics",
        asset_type="docs",
        confidence=0.9,
        rationale="fixture",
        source="rule",
    )

    result = normalize_filename(Path("/tmp/핵심요약 (2).pdf"), classification, config)

    assert result.filename == "핵심요약__v02.pdf"
    assert result.version_token == "v02"
    assert "copy counter" in result.assumptions[0]


def test_redundant_domain_prefix_and_reserved_name_are_sanitized(make_v2_service) -> None:
    _, config, _ = make_v2_service()
    classification = ClassificationResult(
        space="learning",
        stream="areas",
        domain="education",
        focus="linear-algebra",
        asset_type="docs",
        confidence=0.9,
        rationale="fixture",
        source="rule",
    )

    result = normalize_filename(Path("/tmp/20260331_Education_CON.pdf"), classification, config)

    assert result.removed_date_token == "20260331"
    assert result.redundant_tokens_removed == ("Education",)
    assert result.filename == "con-item.pdf"


def test_generic_filetype_tokens_are_dropped_before_falling_back_to_original_terms(make_v2_service) -> None:
    _, config, _ = make_v2_service()
    classification = ClassificationResult(
        space="main",
        stream="areas",
        domain="education",
        focus="선형대수-01분반-3",
        asset_type="assets",
        confidence=0.9,
        rationale="fixture",
        source="rule",
    )

    result = normalize_filename(
        Path("/tmp/2026-03-19_Assets_선형대수_01분반_3_v1.mp4"),
        classification,
        config,
    )

    assert result.removed_date_token == "2026-03-19"
    assert result.filename == "선형대수-01분반-3__v01.mp4"


def test_redundancy_stripping_does_not_collapse_name_to_numeric_token(make_v2_service) -> None:
    _, config, _ = make_v2_service()
    classification = ClassificationResult(
        space="main",
        stream="areas",
        domain="education",
        focus="26-1-coding",
        asset_type="docs",
        confidence=0.9,
        rationale="fixture",
        source="rule",
    )

    result = normalize_filename(Path("/tmp/26-1-coding_v0-0.pdf"), classification, config)

    assert result.filename == "26-1-coding-0__v00.pdf"
