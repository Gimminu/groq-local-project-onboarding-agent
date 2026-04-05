from __future__ import annotations

import json
import re
import unicodedata
import zipfile
from pathlib import Path
from typing import Iterable, Sequence

from app.index_v2.naming import normalize_segment

try:  # pragma: no cover - optional runtime dependency
    from pypdf import PdfReader
except ImportError:  # pragma: no cover - optional runtime dependency
    PdfReader = None

TEXT_PREVIEW_EXTENSIONS = {
    ".c",
    ".cc",
    ".cpp",
    ".cs",
    ".css",
    ".go",
    ".h",
    ".hpp",
    ".html",
    ".ini",
    ".ino",
    ".ipynb",
    ".java",
    ".js",
    ".json",
    ".kt",
    ".md",
    ".mjs",
    ".py",
    ".rb",
    ".rs",
    ".scala",
    ".sh",
    ".sql",
    ".svg",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".xml",
    ".yaml",
    ".yml",
    ".zsh",
    ".csv",
    ".tsv",
}
ZIP_PREVIEW_EXTENSIONS = {".docx", ".hwpx", ".pptx"}
ARCHIVE_MANIFEST_EXTENSIONS = {".zip"}
PREVIEW_MAX_BYTES = 512 * 1024
TOKEN_STOPWORDS = {
    "and",
    "archive",
    "archives",
    "asset",
    "assets",
    "bundle",
    "copy",
    "data",
    "desktop",
    "doc",
    "docs",
    "document",
    "documents",
    "download",
    "downloads",
    "draft",
    "export",
    "exports",
    "file",
    "files",
    "final",
    "folder",
    "form",
    "forms",
    "image",
    "images",
    "json",
    "latest",
    "md",
    "misc",
    "note",
    "notes",
    "other",
    "output",
    "outputs",
    "part",
    "pdf",
    "png",
    "ppt",
    "pptx",
    "sample",
    "slides",
    "solved",
    "temp",
    "template",
    "templates",
    "tmp",
    "txt",
    "untitled",
    "vscode",
    "zip",
}
_XML_TAG_PATTERN = re.compile(r"<[^>]+>")
_TOKEN_PATTERN = re.compile(r"[0-9a-z가-힣]+")
_ZIP_ENTRY_CANDIDATES = {
    ".docx": ("docProps/core.xml", "word/document.xml"),
    ".hwpx": ("Contents/content.hpf", "Contents/section0.xml", "Contents/header.xml"),
    ".pptx": ("docProps/core.xml", "ppt/slides/slide1.xml", "ppt/presentation.xml"),
}


def extract_content_hint(path: Path, *, max_chars: int = 1200) -> str:
    if not path.is_file():
        return ""
    try:
        if path.stat().st_size > PREVIEW_MAX_BYTES:
            return ""
    except OSError:
        return ""

    suffix = path.suffix.lower()
    try:
        if suffix == ".ipynb":
            preview = _ipynb_preview(path)
        elif suffix in TEXT_PREVIEW_EXTENSIONS:
            preview = path.read_text(encoding="utf-8", errors="ignore")
        elif suffix in ZIP_PREVIEW_EXTENSIONS:
            preview = _zip_xml_preview(path, suffix)
        elif suffix in ARCHIVE_MANIFEST_EXTENSIONS:
            preview = _zip_manifest_preview(path)
        elif suffix == ".pdf":
            preview = _pdf_preview(path)
        else:
            preview = ""
    except OSError:
        return ""
    return _compact_text(preview, max_chars=max_chars)


def semantic_tokens_for_path(path: Path, *, extra_stopwords: Iterable[str] = ()) -> set[str]:
    values = [str(path.stem), extract_content_hint(path)]
    return semantic_tokens_from_sources(values, extra_stopwords=extra_stopwords)


def semantic_tokens_from_sources(values: Sequence[str], *, extra_stopwords: Iterable[str] = ()) -> set[str]:
    stopwords = {_normalize_token(value) for value in TOKEN_STOPWORDS}
    stopwords.update(_normalize_token(value) for value in extra_stopwords)
    tokens: set[str] = set()
    for value in values:
        normalized = unicodedata.normalize("NFKC", str(value or "")).lower().replace("-", " ")
        for token in _TOKEN_PATTERN.findall(normalized):
            normalized_token = _normalize_token(token)
            if len(normalized_token) < 2:
                continue
            if normalized_token in stopwords:
                continue
            tokens.add(normalized_token)
    return tokens


def infer_bundle_focus_name(
    paths: Sequence[Path],
    *,
    fallback_name: str | None,
    delimiter: str,
    max_segment_length: int,
    generic_tokens: Iterable[str],
) -> str | None:
    labels = [_meaningful_label(path, generic_tokens=generic_tokens) for path in paths]
    label_lists = [label for label in labels if label]
    if not label_lists and fallback_name:
        return _normalized_label(fallback_name, delimiter=delimiter, max_segment_length=max_segment_length, generic_tokens=generic_tokens)
    if not label_lists:
        return None

    common_prefix = _common_prefix_tokens(label_lists) if len(label_lists) >= 2 else []
    if common_prefix:
        joined = normalize_segment("-".join(common_prefix[:4]), delimiter, max_segment_length)
        if joined:
            return joined

    token_frequency: dict[str, int] = {}
    for path in paths:
        tokens = semantic_tokens_for_path(path, extra_stopwords=generic_tokens)
        for token in tokens:
            token_frequency[token] = token_frequency.get(token, 0) + 1
    threshold = max(1, min(2, len(paths)))
    ranked = sorted(
        (token for token, count in token_frequency.items() if count >= threshold),
        key=lambda token: (-token_frequency[token], len(token), token),
    )
    if ranked:
        joined = normalize_segment("-".join(ranked[:3]), delimiter, max_segment_length)
        if joined:
            return joined

    if fallback_name:
        fallback = _normalized_label(
            fallback_name,
            delimiter=delimiter,
            max_segment_length=max_segment_length,
            generic_tokens=generic_tokens,
        )
        if fallback:
            return fallback

    for label in label_lists:
        joined = normalize_segment("-".join(label[:4]), delimiter, max_segment_length)
        if joined:
            return joined
    return None


def _meaningful_label(path: Path, *, generic_tokens: Iterable[str]) -> list[str]:
    stopwords = {_normalize_token(value) for value in generic_tokens}
    stopwords.update(_normalize_token(value) for value in TOKEN_STOPWORDS)
    raw = unicodedata.normalize("NFKC", path.stem).lower().replace("-", " ")
    tokens = [_normalize_token(token) for token in _TOKEN_PATTERN.findall(raw)]
    cleaned = [token for token in tokens if len(token) >= 2 and token not in stopwords]
    return cleaned


def _common_prefix_tokens(labels: Sequence[Sequence[str]]) -> list[str]:
    if not labels:
        return []
    prefix = list(labels[0])
    for label in labels[1:]:
        limit = min(len(prefix), len(label))
        index = 0
        while index < limit and prefix[index] == label[index]:
            index += 1
        prefix = prefix[:index]
        if not prefix:
            return []
    return prefix


def _normalized_label(
    value: str,
    *,
    delimiter: str,
    max_segment_length: int,
    generic_tokens: Iterable[str],
) -> str | None:
    tokens = _meaningful_label(Path(value), generic_tokens=generic_tokens)
    if not tokens:
        return None
    return normalize_segment("-".join(tokens[:4]), delimiter, max_segment_length)


def _normalize_token(value: str) -> str:
    return unicodedata.normalize("NFKC", str(value)).strip().lower().replace("_", "").replace("-", "")


def _compact_text(value: str, *, max_chars: int) -> str:
    collapsed = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value or "")).strip()
    if len(collapsed) <= max_chars:
        return collapsed
    return collapsed[:max_chars].rstrip()


def _ipynb_preview(path: Path) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except json.JSONDecodeError:
        return path.read_text(encoding="utf-8", errors="ignore")
    if not isinstance(payload, dict):
        return ""
    cells = payload.get("cells")
    if not isinstance(cells, list):
        return ""
    snippets: list[str] = []
    for cell in cells[:3]:
        if not isinstance(cell, dict):
            continue
        source = cell.get("source")
        if isinstance(source, list):
            snippets.append("".join(str(item) for item in source))
        elif isinstance(source, str):
            snippets.append(source)
    return "\n".join(snippets)


def _zip_xml_preview(path: Path, suffix: str) -> str:
    entries = _ZIP_ENTRY_CANDIDATES.get(suffix, ())
    parts: list[str] = []
    try:
        with zipfile.ZipFile(path) as archive:
            for entry in entries:
                try:
                    payload = archive.read(entry).decode("utf-8", errors="ignore")
                except KeyError:
                    continue
                parts.append(_XML_TAG_PATTERN.sub(" ", payload))
    except (OSError, zipfile.BadZipFile):
        return ""
    return "\n".join(parts)


def _zip_manifest_preview(path: Path) -> str:
    entries: list[str] = []
    try:
        with zipfile.ZipFile(path) as archive:
            for index, name in enumerate(archive.namelist()):
                if index >= 20:
                    break
                entries.append(name)
    except (OSError, zipfile.BadZipFile):
        return ""
    return "\n".join(entries)


def _pdf_preview(path: Path) -> str:
    if PdfReader is None:  # pragma: no cover - dependency optional
        return ""
    try:  # pragma: no cover - depends on runtime package and document content
        reader = PdfReader(str(path))
        snippets: list[str] = []
        metadata = getattr(reader, "metadata", None)
        if metadata is not None:
            title = getattr(metadata, "title", None) or metadata.get("/Title")
            if title:
                snippets.append(str(title))
        if reader.pages:
            snippets.append(reader.pages[0].extract_text() or "")
        return "\n".join(snippets)
    except Exception:
        return ""
