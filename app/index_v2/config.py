from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from app.errors import AppError
from app.index_v2.types import (
    AdaptivePlacementPolicy,
    ArchivePolicy,
    DeletionPolicy,
    DomainGatePolicy,
    DomainPolicy,
    DEFAULT_ASSET_ALIASES,
    DEFAULT_ASSET_TYPES,
    DEFAULT_DOMAIN_ALIASES,
    DEFAULT_DOMAINS,
    DEFAULT_GENERIC_TOKENS,
    DEFAULT_IGNORE_NAMES,
    DEFAULT_OUTPUT_TOKENS,
    DEFAULT_PROJECT_MARKERS,
    DEFAULT_SPACES,
    DEFAULT_STREAMS,
    GroqPolicy,
    IndexOrganizerConfig,
    CleanupPolicy,
    LLMPolicy,
    MigrationRule,
    NamingPolicy,
    OllamaPolicy,
    PatternOverride,
    RepairCodeNamesPolicy,
    RepairDefaults,
    ReportingPolicy,
    ServicePolicy,
    WatchPolicy,
)


def load_index_config(config_path: Path) -> IndexOrganizerConfig:
    raw_path = config_path.expanduser()
    if not raw_path.exists():
        raise AppError(f"V2 설정 파일을 찾을 수 없습니다: {raw_path}")

    payload = yaml.safe_load(raw_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise AppError("V2 설정 파일은 YAML 객체여야 합니다.")

    base_dir = raw_path.parent
    state_dir = _expand_path(payload.get("state_dir"), base_dir, "~/Library/Application Support/FolderOrganizerV2")
    history_root = _expand_path(payload.get("history_root"), base_dir, str(state_dir / "history"))
    naming = NamingPolicy(**_mapping(payload.get("naming", {})))
    groq = GroqPolicy(**_mapping(payload.get("groq", {})))
    llm = _llm_policy(payload.get("llm"), groq)
    watch = WatchPolicy(**_mapping(payload.get("watch", {})))
    archive = ArchivePolicy(**_mapping(payload.get("archive", {})))
    deletion = DeletionPolicy(**_mapping(payload.get("deletion", {})))
    service = ServicePolicy(**_mapping(payload.get("service", {})))
    reporting = ReportingPolicy(**_mapping(payload.get("reporting", {})))
    cleanup = CleanupPolicy(**_mapping(payload.get("cleanup", {})))
    repair_code_names = RepairCodeNamesPolicy(**_mapping(payload.get("repair_code_names", {})))
    repair_defaults = _repair_defaults(payload.get("repair_defaults"))
    domains = _string_tuple(payload.get("domains")) or DEFAULT_DOMAINS
    domain_policy = _domain_policy(payload.get("domain_policy"), domains)
    adaptive_placement = _adaptive_placement_policy(payload.get("adaptive_placement"))

    config = IndexOrganizerConfig(
        config_path=raw_path.absolute(),
        spaces_root=_expand_path(payload.get("spaces_root"), base_dir, "~/Documents"),
        history_root=history_root,
        state_dir=state_dir,
        watch_roots=_path_tuple(payload.get("watch_roots", []), base_dir),
        migration_roots=_path_tuple(payload.get("migration_roots", []), base_dir),
        scan_roots=_path_tuple(payload.get("scan_roots", []), base_dir),
        root_spaces=_path_mapping(payload.get("root_spaces", {}), base_dir),
        spaces=_string_tuple(payload.get("spaces")) or DEFAULT_SPACES,
        streams=_string_tuple(payload.get("streams")) or DEFAULT_STREAMS,
        domains=domains,
        asset_types=_string_tuple(payload.get("asset_types")) or DEFAULT_ASSET_TYPES,
        domain_policy=domain_policy,
        adaptive_placement=adaptive_placement,
        default_space=str(payload.get("default_space") or "personal"),
        include_space_level=bool(payload.get("include_space_level", False)),
        move_uncertain_items=bool(payload.get("move_uncertain_items", False)),
        shallow_structure=bool(payload.get("shallow_structure", True)),
        preferred_depth=int(payload.get("preferred_depth", 3)),
        max_depth_limit=int(payload.get("max_depth_limit", 5)),
        avoid_middle_folders=_string_tuple(payload.get("avoid_middle_folders"))
        or ("code", "misc", "temp", "category", "categories"),
        protection_level=str(payload.get("protection_level") or "strict"),
        naming=naming,
        groq=groq,
        llm=llm,
        watch=watch,
        archive=archive,
        deletion=deletion,
        service=service,
        reporting=reporting,
        cleanup=cleanup,
        review_mode=str(payload.get("review_mode") or "single-inbox"),
        protected_project_internal_roots=_string_tuple(payload.get("protected_project_internal_roots"))
        or (
            "projects/*/*/code",
            "projects/*/*/code/*",
            "projects/*/*/code/**",
        ),
        protected_stream_roots=_string_tuple(payload.get("protected_stream_roots")) or ("projects",),
        repair_code_names=repair_code_names,
        repair_defaults=repair_defaults,
        migration_rules=_migration_rules(payload.get("migration_rules")) or _default_migration_rules(),
        pattern_overrides=_pattern_overrides(payload.get("pattern_overrides")),
        domain_aliases=_alias_mapping(payload.get("domain_aliases")) or dict(DEFAULT_DOMAIN_ALIASES),
        asset_aliases=_alias_mapping(payload.get("asset_aliases")) or dict(DEFAULT_ASSET_ALIASES),
        project_markers=_string_tuple(payload.get("project_markers")) or DEFAULT_PROJECT_MARKERS,
        output_tokens=_string_tuple(payload.get("output_tokens")) or DEFAULT_OUTPUT_TOKENS,
        generic_tokens=_string_tuple(payload.get("generic_tokens")) or DEFAULT_GENERIC_TOKENS,
        ignore_names=_string_tuple(payload.get("ignore_names")) or DEFAULT_IGNORE_NAMES,
    )
    config.ensure_directories()
    return config


def _mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise AppError("설정 항목은 YAML mapping 이어야 합니다.")
    return value


def _expand_path(raw_value: Any, base_dir: Path, default_value: str) -> Path:
    value = str(raw_value or default_value)
    expanded = Path(os.path.expandvars(value)).expanduser()
    if not expanded.is_absolute():
        expanded = (base_dir / expanded).absolute()
    else:
        expanded = expanded.absolute()
    return expanded


def _path_tuple(raw_value: Any, base_dir: Path) -> tuple[Path, ...]:
    if raw_value is None:
        return ()
    if not isinstance(raw_value, list):
        raise AppError("경로 목록 설정은 YAML 배열이어야 합니다.")
    return tuple(_expand_path(item, base_dir, ".") for item in raw_value)


def _path_mapping(raw_value: Any, base_dir: Path) -> dict[Path, str]:
    payload = _mapping(raw_value)
    return {_expand_path(key, base_dir, "."): str(value) for key, value in payload.items()}


def _llm_policy(raw_value: Any, groq: GroqPolicy) -> LLMPolicy:
    payload = _mapping(raw_value)
    ollama_payload = _mapping(payload.get("ollama"))
    intervals = payload.get("min_request_interval_seconds")
    if intervals is None:
        min_intervals = {"groq": 3, "gemini": 5, "ollama": 0}
    else:
        if not isinstance(intervals, dict):
            raise AppError("llm.min_request_interval_seconds 는 YAML mapping 이어야 합니다.")
        min_intervals = {
            str(key): int(value)
            for key, value in intervals.items()
        }
    backoff = payload.get("backoff_seconds")
    if backoff is None:
        backoff_seconds = (15, 45)
    else:
        if not isinstance(backoff, list):
            raise AppError("llm.backoff_seconds 는 YAML 배열이어야 합니다.")
        backoff_seconds = tuple(int(item) for item in backoff)

    preferred_provider = str(payload.get("preferred_provider") or ("groq" if groq.enabled else "ollama"))
    return LLMPolicy(
        preferred_provider=preferred_provider,
        fallback_to_other_cloud=bool(payload.get("fallback_to_other_cloud", True)),
        fallback_to_ollama=bool(payload.get("fallback_to_ollama", True)),
        enable_for_watch=bool(payload.get("enable_for_watch", False)),
        enable_llm_first=bool(payload.get("enable_llm_first", False)),
        max_items_per_watch_tick=int(payload.get("max_items_per_watch_tick", 2)),
        retry_attempts=int(payload.get("retry_attempts", 2)),
        min_request_interval_seconds=min_intervals,
        backoff_seconds=backoff_seconds,
        provider_cooldown_seconds=int(payload.get("provider_cooldown_seconds", 600)),
        ollama=OllamaPolicy(
            base_url=str(ollama_payload.get("base_url") or "http://127.0.0.1:11434"),
            model=str(ollama_payload.get("model") or "qwen2.5:3b-instruct"),
            healthcheck_timeout_seconds=int(ollama_payload.get("healthcheck_timeout_seconds", 2)),
        ),
    )


def _repair_defaults(raw_value: Any) -> RepairDefaults:
    payload = _mapping(raw_value)
    if "fallback_focus" not in payload and "general_focus" in payload:
        payload = dict(payload)
        payload["fallback_focus"] = payload["general_focus"]
    payload.pop("general_focus", None)
    return RepairDefaults(**payload)


def _domain_policy(raw_value: Any, domains: tuple[str, ...]) -> DomainPolicy:
    payload = _mapping(raw_value)
    gate_payload = _mapping(payload.get("new_domain_gate"))
    pinned = _string_tuple(payload.get("pinned_domains")) or domains
    return DomainPolicy(
        pinned_domains=pinned,
        allow_dynamic_domains=bool(payload.get("allow_dynamic_domains", True)),
        new_domain_gate=DomainGatePolicy(
            min_items_in_window=int(gate_payload.get("min_items_in_window", 3)),
            window_days=int(gate_payload.get("window_days", 14)),
            min_confidence=float(gate_payload.get("min_confidence", 0.85)),
            min_independent_signals=int(gate_payload.get("min_independent_signals", 2)),
            max_new_domains_per_week=int(gate_payload.get("max_new_domains_per_week", 2)),
            banned_domain_names=_string_tuple(gate_payload.get("banned_domain_names"))
            or DomainGatePolicy().banned_domain_names,
        ),
    )


def _adaptive_placement_policy(raw_value: Any) -> AdaptivePlacementPolicy:
    payload = _mapping(raw_value)
    return AdaptivePlacementPolicy(
        enabled=bool(payload.get("enabled", False)),
        hidden_review_relative=str(payload.get("hidden_review_relative") or "adaptive-review"),
        max_existing_folder_depth=int(payload.get("max_existing_folder_depth", 3)),
        min_existing_folder_score=int(payload.get("min_existing_folder_score", 2)),
        reserved_top_level_names=_string_tuple(payload.get("reserved_top_level_names"))
        or AdaptivePlacementPolicy().reserved_top_level_names,
        blocked_top_level_names=_string_tuple(payload.get("blocked_top_level_names"))
        or AdaptivePlacementPolicy().blocked_top_level_names,
        blocked_top_level_fragments=_string_tuple(payload.get("blocked_top_level_fragments"))
        or AdaptivePlacementPolicy().blocked_top_level_fragments,
        review_layout=str(payload.get("review_layout") or "asset-first"),
        stabilize_existing_spaces_items=bool(payload.get("stabilize_existing_spaces_items", True)),
        auto_drain_hidden_review=bool(payload.get("auto_drain_hidden_review", True)),
        hidden_review_drain_interval_seconds=int(payload.get("hidden_review_drain_interval_seconds", 900)),
        hidden_review_max_items_per_tick=int(payload.get("hidden_review_max_items_per_tick", 12)),
    )


def _string_tuple(raw_value: Any) -> tuple[str, ...]:
    if raw_value is None:
        return ()
    if not isinstance(raw_value, list):
        raise AppError("문자열 목록 설정은 YAML 배열이어야 합니다.")
    return tuple(str(item) for item in raw_value)


def _migration_rules(raw_value: Any) -> tuple[MigrationRule, ...]:
    if raw_value is None:
        return ()
    if not isinstance(raw_value, list):
        raise AppError("migration_rules 는 YAML 배열이어야 합니다.")
    rules: list[MigrationRule] = []
    for item in raw_value:
        if not isinstance(item, dict):
            raise AppError("migration_rules 항목은 YAML 객체여야 합니다.")
        rules.append(MigrationRule(**item))
    return tuple(rules)


def _pattern_overrides(raw_value: Any) -> tuple[PatternOverride, ...]:
    if raw_value is None:
        return ()
    if not isinstance(raw_value, list):
        raise AppError("pattern_overrides 는 YAML 배열이어야 합니다.")
    rules: list[PatternOverride] = []
    for item in raw_value:
        if not isinstance(item, dict):
            raise AppError("pattern_overrides 항목은 YAML 객체여야 합니다.")
        rules.append(PatternOverride(**item))
    return tuple(rules)


def _alias_mapping(raw_value: Any) -> dict[str, tuple[str, ...]]:
    payload = _mapping(raw_value)
    return {str(key): tuple(str(item) for item in value) for key, value in payload.items()}


def _default_migration_rules() -> tuple[MigrationRule, ...]:
    return (
        MigrationRule(legacy_root_name="00_Inbox", stream="inbox"),
        MigrationRule(legacy_root_name="01_Projects", stream="projects", domain="coding", asset_type="code"),
        MigrationRule(legacy_root_name="02_Areas", stream="areas"),
        MigrationRule(legacy_root_name="03_Resources", stream="resources"),
        MigrationRule(legacy_root_name="04_Archive", stream="archive", asset_type="archives"),
    )
