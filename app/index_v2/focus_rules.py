from __future__ import annotations

import unicodedata
from pathlib import Path

from app.index_v2.naming import normalize_segment
from app.index_v2.types import IndexOrganizerConfig

EDUCATION_FOCUS_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("정보처리기사", ("정보처리기사", "기출문제집", "핵심요약")),
    ("26-1-coding", ("26-1-coding", "26_1_coding")),
    ("고급-프로그래밍-설계", ("고급프로그래밍설계", "고프설", "apd", "pandas활용하기")),
    ("선형대수", ("선형대수",)),
    ("산학-캡스톤-디자인", ("산학캡스톤디자인", "캡스톤디자인", "산학-캡스톤", "sw중심대학사업")),
    ("지정학적-충격-금융시장-반응", ("지정학적 충격과 금융시장 반응 분석", "금융시장 반응 분석")),
    ("코로나-데이터-분석", ("코로나데이터분석", "코로나 데이터 분석", "코로나 바이러스 감염 현황", "owid-covid-data", "covid-19")),
    ("과제-제안서", ("과제-제안서", "과제 제안서", "연구과제 제안서")),
    ("시험-안내", ("시험-시작-5분전", "시험 시작 5분전")),
)

ADMIN_FOCUS_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("global-ict", ("글로벌-ict", "글로벌 ict", "global-ict", "global ict")),
    ("루키-제출서류", ("루키", "참가신청서류", "참가서약서", "개인정보 수집 이용 제공 동의서", "사진 영상 촬영과 활용 동의서")),
    ("연구과제-제안서", ("연구과제 제안서", "연구과제-제안서", "제안서")),
)

TEMPLATE_FOCUS_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("vscode-settings", ("vscode", "launch.json", "extensions.json", "c_cpp_properties.json")),
    ("루키-제안서-양식", ("도전제안서", "양식", "루키")),
)

RESEARCH_FOCUS_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("유가-데이터", ("유가", "oil", "국제시장", "유럽")),
)

CODING_RESOURCE_FOCUS_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("organizer-workflow", ("working-rules", "desktop-working-rules", "publish-repo", "publish-workflow")),
)

REVIEW_DOC_FOCUS_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("admin-docs", ("서비스 정의서", "서비스-정의서", "정의서")),
    ("education-docs", ("리눅스마스터", "파이널", "topic", "전체합", "특강")),
    ("산학-캡스톤-디자인", ("산학캡스톤디자인", "캡스톤디자인", "sw중심대학사업")),
)

REVIEW_CODE_FOCUS_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("코로나-데이터-분석", ("코로나데이터분석", "코로나 데이터 분석", "코로나 바이러스 감염 현황", "owid-covid-data", "covid-19")),
    ("organizer-workflow", ("working-rules", "desktop-working-rules", "publish-repo", "publish-workflow")),
)

REVIEW_ARCHIVE_FOCUS_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("코로나-데이터-분석", ("코로나데이터분석", "코로나 데이터 분석", "owid-covid-data", "covid-19")),
    ("통신사고객데이터분석", ("통신사고객데이터분석", "telco-customer-churn", "customer churn", "wa_fn-usec")),
    ("리눅스2급1차족보new", ("리눅스2급1차족보", "리눅스2급", "족보new")),
)


def infer_focus_from_path(
    path: Path,
    *,
    stream: str,
    domain: str,
    asset_type: str,
    config: IndexOrganizerConfig,
    hint_text: str | None = None,
) -> str | None:
    if stream == "review":
        focus = _infer_review_focus(path, asset_type=asset_type, config=config, hint_text=hint_text)
        if focus:
            return focus

    if domain == "education":
        return _focus_from_rules(path, EDUCATION_FOCUS_RULES, config, hint_text=hint_text)
    if domain == "admin":
        return _focus_from_rules(path, ADMIN_FOCUS_RULES, config, hint_text=hint_text)
    if domain == "templates":
        return _focus_from_rules(path, TEMPLATE_FOCUS_RULES, config, hint_text=hint_text)
    if domain == "research":
        return _focus_from_rules(path, RESEARCH_FOCUS_RULES, config, hint_text=hint_text)
    if domain == "coding":
        return _focus_from_rules(path, CODING_RESOURCE_FOCUS_RULES, config, hint_text=hint_text)
    return None


def _infer_review_focus(path: Path, *, asset_type: str, config: IndexOrganizerConfig, hint_text: str | None = None) -> str | None:
    text = _normalized_text(path, hint_text=hint_text)
    stem = _normalized_text(Path(path.stem), hint_text=hint_text)
    if asset_type == "data" and "groq-logs" in text:
        return _focus_value("groq-logs", config)
    if asset_type == "assets":
        if stem.startswith("scr-") or "screenshot" in text:
            return _focus_value("screenshots", config)
        return _focus_value("photos", config)
    if asset_type == "archives":
        focus = _focus_from_rules(path, REVIEW_ARCHIVE_FOCUS_RULES, config, hint_text=hint_text)
        if focus:
            return focus
        if any(token in text for token in ("리눅스", "분석예제", "코로나데이터분석", "arduino")):
            return _focus_value("imported-archives", config)
    if asset_type == "docs":
        return _focus_from_rules(path, REVIEW_DOC_FOCUS_RULES, config)
    if asset_type == "code":
        focus = _focus_from_rules(path, REVIEW_CODE_FOCUS_RULES, config, hint_text=hint_text)
        if focus:
            return focus
    if asset_type == "code" and path.suffix.lower() == ".code-workspace":
        return _focus_value(path.stem, config)
    return None


def _focus_from_rules(
    path: Path,
    rules: tuple[tuple[str, tuple[str, ...]], ...],
    config: IndexOrganizerConfig,
    hint_text: str | None = None,
) -> str | None:
    text = _normalized_text(path, hint_text=hint_text)
    for focus, patterns in rules:
        if any(pattern.lower() in text for pattern in patterns):
            return _focus_value(focus, config)
    return None


def _focus_value(value: str, config: IndexOrganizerConfig) -> str | None:
    normalized = normalize_segment(
        value,
        config.naming.delimiter,
        config.naming.max_segment_length,
    )
    if normalized in {
        "",
        config.naming.unsorted_focus,
        config.repair_defaults.general_focus,
        "review",
        "misc",
    }:
        return None
    return normalized


def _normalized_text(path: Path, *, hint_text: str | None = None) -> str:
    base = unicodedata.normalize("NFKC", str(path)).lower()
    if not hint_text:
        return base
    return f"{base}\n{unicodedata.normalize('NFKC', hint_text).lower()}"
