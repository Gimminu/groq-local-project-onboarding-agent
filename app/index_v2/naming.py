from __future__ import annotations

import hashlib
import re
from pathlib import Path

from app.index_v2.types import ClassificationResult, IndexOrganizerConfig, NormalizationResult

LEADING_DATE_PATTERN = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2}|\d{8})[_\-\s]+")
VERSION_PATTERN = re.compile(r"(?:^|[_\-\s])v(?P<version>\d{1,3})(?:$|[_\-\s])", re.IGNORECASE)
COPY_PATTERN = re.compile(r"\((?P<copy>\d{1,3})\)$")
SYSTEM_TAG_PATTERN = re.compile(r"[\[\(](?:file|note|image|spreadsheet|reference|code|uncertain)[\]\)]", re.IGNORECASE)
SYSTEM_WORD_TOKENS = {
    "code",
    "file",
    "image",
    "note",
    "reference",
    "spreadsheet",
    "uncertain",
}
WINDOWS_RESERVED_NAMES = {
    "con",
    "prn",
    "aux",
    "nul",
    "com1",
    "com2",
    "com3",
    "com4",
    "com5",
    "com6",
    "com7",
    "com8",
    "com9",
    "lpt1",
    "lpt2",
    "lpt3",
    "lpt4",
    "lpt5",
    "lpt6",
    "lpt7",
    "lpt8",
    "lpt9",
}
GENERIC_FILETYPE_TOKENS = {
    "archive",
    "archives",
    "asset",
    "assets",
    "code",
    "data",
    "doc",
    "docs",
    "document",
    "documents",
    "file",
    "hwp",
    "hwpx",
    "image",
    "ipynb",
    "media",
    "note",
    "notes",
    "pages",
    "pdf",
    "png",
    "jpg",
    "jpeg",
    "zip",
}


def normalize_filename(path: Path, classification: ClassificationResult, config: IndexOrganizerConfig) -> NormalizationResult:
    original_filename = path.name
    ext = path.suffix.lower()
    stem = path.stem
    removed_date = None
    assumptions: list[str] = []
    removed_tokens: list[str] = []

    date_match = LEADING_DATE_PATTERN.match(stem)
    if date_match:
        removed_date = date_match.group("date")
        stem = stem[date_match.end() :]

    version_value = None
    version_match = VERSION_PATTERN.search(stem)
    if version_match:
        version_value = int(version_match.group("version"))
        stem = VERSION_PATTERN.sub(" ", stem)

    copy_match = COPY_PATTERN.search(stem.strip())
    if copy_match:
        copy_value = int(copy_match.group("copy"))
        version_value = max(version_value or 0, copy_value)
        stem = COPY_PATTERN.sub("", stem).strip()
        assumptions.append(f"copy counter ({copy_value}) interpreted as version bump")

    original_tokens = _tokenize(stem)
    tokens = list(original_tokens)
    tokens, redundant = _remove_redundancy(tokens, classification, config)
    removed_tokens.extend(redundant)
    if not tokens and original_tokens:
        preserved_tokens = [
            token
            for token in original_tokens
            if _normalize_compare_token(token) not in GENERIC_FILETYPE_TOKENS
        ]
        if preserved_tokens:
            tokens = preserved_tokens
            removed_tokens = [token for token in original_tokens if token not in preserved_tokens]
            assumptions.append("preserved original non-generic tokens after redundancy stripping removed all terms")
        else:
            tokens = original_tokens
            removed_tokens = []
            assumptions.append("redundancy stripping skipped to preserve filename meaning")

    if not tokens or _tokens_are_generic(tokens, config) or _tokens_are_low_signal(tokens):
        tokens = _fallback_parent_tokens(path, classification, config)
        if tokens:
            assumptions.append("used nearest meaningful wrapper tokens to replace generic basename")

    if not tokens:
        return NormalizationResult(
            filename=original_filename,
            removed_date_token=removed_date,
            version_token=f"v{version_value:02d}" if version_value is not None else None,
            assumptions=tuple(assumptions + ["kept original basename because no safe replacement tokens were found"]),
            redundant_tokens_removed=tuple(removed_tokens),
        )

    delimiter = "-" if config.naming.delimiter == "kebab-case" else "_"
    normalized_stem = delimiter.join(token.lower() for token in tokens)
    normalized_stem = _safe_name(normalized_stem, delimiter)
    normalized_stem = _truncate(normalized_stem, config.naming.max_stem_length, path.name, delimiter)

    suffix = ""
    if version_value is not None:
        suffix = f"__v{version_value:02d}"

    filename = f"{normalized_stem}{suffix}{ext}"
    return NormalizationResult(
        filename=filename,
        removed_date_token=removed_date,
        version_token=f"v{version_value:02d}" if version_value is not None else None,
        assumptions=tuple(assumptions),
        redundant_tokens_removed=tuple(removed_tokens),
    )


def normalize_segment(value: str, delimiter: str = "kebab-case", max_length: int | None = None) -> str:
    tokens = _tokenize(value)
    if not tokens:
        return "untitled"
    joiner = "-" if delimiter == "kebab-case" else "_"
    normalized = joiner.join(token.lower() for token in tokens)
    normalized = _safe_name(normalized, joiner)
    if max_length is not None and len(normalized) > max_length:
        normalized = _truncate(normalized, max_length, value, joiner)
    return normalized


def _tokenize(value: str) -> list[str]:
    cleaned = SYSTEM_TAG_PATTERN.sub(" ", value)
    cleaned = re.sub(r"\buncertain\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[^\w\s-]+", " ", cleaned, flags=re.UNICODE)
    cleaned = re.sub(r"[_\-\s]+", " ", cleaned).strip()
    tokens = [token for token in cleaned.split(" ") if token]
    filtered = [token for token in tokens if _normalize_compare_token(token) not in SYSTEM_WORD_TOKENS]
    if filtered:
        tokens = filtered
    return tokens


def _remove_redundancy(
    tokens: list[str],
    classification: ClassificationResult,
    config: IndexOrganizerConfig,
) -> tuple[list[str], list[str]]:
    domain_aliases = _conservative_aliases(classification.domain, config.domain_aliases.get(classification.domain, ()))
    asset_aliases = _conservative_aliases(classification.asset_type, config.asset_aliases.get(classification.asset_type, ()))
    focus_tokens = tuple(item for item in classification.focus.split("-") if item)
    focus_contains_numeric = any(any(char.isdigit() for char in item) for item in focus_tokens)
    redundant_sets = [
        {
            _normalize_compare_token(item)
            for item in (*classification.domain.split("-"), *domain_aliases)
            if item
        },
        set() if focus_contains_numeric else {_normalize_compare_token(item) for item in focus_tokens},
        {
            _normalize_compare_token(item)
            for item in (*classification.asset_type.split("-"), *asset_aliases)
            if item
        },
    ]
    remaining = list(tokens)
    removed: list[str] = []
    while remaining:
        head = _normalize_compare_token(remaining[0])
        if any(head and head in candidates for candidates in redundant_sets) or head in GENERIC_FILETYPE_TOKENS:
            removed.append(remaining.pop(0))
            continue
        break
    while remaining:
        tail = _normalize_compare_token(remaining[-1])
        if any(tail and tail in candidates for candidates in redundant_sets) or tail in GENERIC_FILETYPE_TOKENS:
            removed.append(remaining.pop())
            continue
        break
    return remaining, removed


def _normalize_compare_token(value: str) -> str:
    return re.sub(r"[_\-\s]+", "", value).lower()


def _conservative_aliases(value: str, aliases: tuple[str, ...]) -> tuple[str, ...]:
    normalized_value = _normalize_compare_token(value)
    retained: list[str] = []
    for alias in aliases:
        normalized_alias = _normalize_compare_token(alias)
        if not normalized_alias:
            continue
        if normalized_alias == normalized_value:
            retained.append(alias)
            continue
        if normalized_alias.isascii() and (
            normalized_value.startswith(normalized_alias) or normalized_alias.startswith(normalized_value)
        ):
            retained.append(alias)
    return tuple(retained)


def _safe_name(value: str, delimiter: str) -> str:
    cleaned = value.strip(" .")
    if not cleaned:
        cleaned = "untitled"
    if cleaned.lower() in WINDOWS_RESERVED_NAMES:
        cleaned = f"{cleaned}{delimiter}item"
    return cleaned


def _fallback_parent_tokens(
    path: Path,
    classification: ClassificationResult,
    config: IndexOrganizerConfig,
) -> list[str]:
    generic_values = {
        *config.generic_tokens,
        *config.streams,
        *config.spaces,
        *config.allowed_domains(),
        *config.asset_types,
        *GENERIC_FILETYPE_TOKENS,
        "review",
        "inbox",
    }
    for aliases in config.asset_aliases.values():
        generic_values.update(aliases)
    generic_values_normalized = {_normalize_compare_token(value) for value in generic_values}
    for parent in path.parents:
        if parent == path or parent == config.spaces_root.parent:
            continue
        candidate = parent.name
        if not candidate:
            continue
        date_match = LEADING_DATE_PATTERN.match(candidate)
        if date_match:
            candidate = candidate[date_match.end() :]
        candidate = VERSION_PATTERN.sub(" ", candidate)
        candidate = COPY_PATTERN.sub("", candidate).strip()
        tokens = _tokenize(candidate)
        if not tokens:
            continue
        tokens, _ = _remove_redundancy(tokens, classification, config)
        filtered = [token for token in tokens if _normalize_compare_token(token) not in generic_values_normalized]
        if filtered:
            return filtered
        raw_tokens = [token for token in _tokenize(candidate) if _normalize_compare_token(token) not in generic_values_normalized]
        if raw_tokens:
            return raw_tokens
        if parent == config.spaces_root:
            break
    return []


def _tokens_are_generic(tokens: list[str], config: IndexOrganizerConfig) -> bool:
    generic_values = {
        *config.generic_tokens,
        *config.streams,
        *config.spaces,
        *config.allowed_domains(),
        *config.asset_types,
        *GENERIC_FILETYPE_TOKENS,
        "inbox",
        "review",
        "untitled",
    }
    for aliases in config.asset_aliases.values():
        generic_values.update(aliases)
    generic_tokens = {_normalize_compare_token(value) for value in generic_values}
    return all(_normalize_compare_token(token) in generic_tokens for token in tokens)


def _tokens_are_low_signal(tokens: list[str]) -> bool:
    if not tokens:
        return True
    if len(tokens) == 1:
        compact = _normalize_compare_token(tokens[0])
        return compact.isdigit() or len(compact) <= 1
    return False


def _truncate(stem: str, max_length: int, original_name: str, delimiter: str) -> str:
    if len(stem) <= max_length:
        return stem
    digest = hashlib.sha1(original_name.encode("utf-8")).hexdigest()[:6]
    budget = max(8, max_length - 7)
    return f"{stem[:budget].rstrip(delimiter)}{delimiter}{digest}"
