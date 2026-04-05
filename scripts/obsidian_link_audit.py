#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import unquote

WIKILINK_PATTERN = re.compile(r"(?<!\!)\[\[([^\]]+)\]\]")
MARKDOWN_LINK_PATTERN = re.compile(r"(?<!!)(?<!\!)\[[^\]]*\]\(([^)]+)\)")

EXTERNAL_SCHEMES = (
    "http://",
    "https://",
    "mailto:",
    "obsidian:",
    "data:",
    "tel:",
    "ftp://",
    "file://",
)


@dataclass(slots=True)
class BrokenLink:
    source: str
    line: int
    kind: str
    target: str
    reason: str


class VaultIndex:
    def __init__(
        self,
        vault_root: Path,
        *,
        include_hidden: bool,
        excluded_dirs: set[str],
    ) -> None:
        self.vault_root = vault_root
        self.markdown_files: list[Path] = []
        self.file_keys: set[str] = set()
        self.dir_keys: set[str] = set()
        self.notes_by_stem: dict[str, list[Path]] = defaultdict(list)
        self.include_hidden = include_hidden
        self.excluded_dirs = excluded_dirs

        for path in sorted(vault_root.rglob("*")):
            rel = path.relative_to(vault_root).as_posix()
            if path.is_dir():
                self.dir_keys.add(self._norm_key(rel))
                continue
            if not path.is_file():
                continue
            self.file_keys.add(self._norm_key(rel))
            if path.suffix.lower() == ".md":
                if self._is_scannable_markdown(path):
                    self.markdown_files.append(path)
                self.notes_by_stem[self._norm_stem(path.stem)].append(path)

    def _is_scannable_markdown(self, path: Path) -> bool:
        rel_parts = path.relative_to(self.vault_root).parts
        parent_parts = rel_parts[:-1]
        file_name = rel_parts[-1] if rel_parts else ""

        if not self.include_hidden:
            if file_name.startswith("."):
                return False
            if any(part.startswith(".") for part in parent_parts):
                return False

        if any(part in self.excluded_dirs for part in parent_parts):
            return False

        return True

    def exists_from_root(self, rel_path: str) -> bool:
        return self._norm_key(rel_path) in self.file_keys

    def dir_exists_from_root(self, rel_path: str) -> bool:
        return self._norm_key(rel_path.rstrip("/")) in self.dir_keys

    @staticmethod
    def _norm_key(path: str) -> str:
        normalized = unicodedata.normalize("NFC", path.replace("\\", "/").strip("/"))
        return normalized.casefold()

    @staticmethod
    def _norm_stem(stem: str) -> str:
        return unicodedata.normalize("NFC", stem).casefold()


class LinkAuditor:
    def __init__(self, vault_root: Path, *, include_hidden: bool, excluded_dirs: set[str]) -> None:
        self.vault_root = vault_root
        self.index = VaultIndex(
            vault_root,
            include_hidden=include_hidden,
            excluded_dirs=excluded_dirs,
        )

    def audit(self) -> tuple[list[BrokenLink], int]:
        broken: list[BrokenLink] = []
        total_links = 0

        for note_path in self.index.markdown_files:
            rel_source = note_path.relative_to(self.vault_root).as_posix()
            try:
                text = note_path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                # Skip binary-looking markdown files and continue auditing others.
                continue

            in_fenced_code = False
            for line_no, line in enumerate(text.splitlines(), start=1):
                stripped = line.strip()
                if stripped.startswith("```"):
                    in_fenced_code = not in_fenced_code
                    continue
                if in_fenced_code:
                    continue

                for match in WIKILINK_PATTERN.finditer(line):
                    total_links += 1
                    raw_target = match.group(1)
                    normalized = self._normalize_target(raw_target)
                    if not normalized:
                        continue
                    ok, reason = self._resolve_wikilink(normalized, note_path)
                    if not ok:
                        broken.append(
                            BrokenLink(
                                source=rel_source,
                                line=line_no,
                                kind="wikilink",
                                target=raw_target,
                                reason=reason,
                            )
                        )

                for match in MARKDOWN_LINK_PATTERN.finditer(line):
                    total_links += 1
                    raw_target = match.group(1)
                    normalized = self._normalize_markdown_target(raw_target)
                    if not normalized:
                        continue
                    ok, reason = self._resolve_markdown_link(normalized, note_path)
                    if not ok:
                        broken.append(
                            BrokenLink(
                                source=rel_source,
                                line=line_no,
                                kind="markdown",
                                target=raw_target,
                                reason=reason,
                            )
                        )

        return broken, total_links

    def _normalize_target(self, raw_target: str) -> str:
        target = raw_target.strip()
        if not target:
            return ""
        target = target.replace("\\|", "|").replace("\\#", "#")
        target = target.split("|", 1)[0].strip()
        target = target.split("#", 1)[0].strip()
        target = unquote(target)
        if not target:
            return ""
        lowered = target.lower()
        if lowered.startswith(EXTERNAL_SCHEMES):
            return ""
        if target.startswith("#"):
            return ""
        return target.replace("\\", "/")

    def _normalize_markdown_target(self, raw_target: str) -> str:
        target = raw_target.strip()
        if not target:
            return ""

        # Basic support for links with optional title: (path "title")
        if target.startswith("<") and target.endswith(">"):
            target = target[1:-1].strip()
        else:
            parts = target.split()
            if parts:
                target = parts[0].strip()

        target = unquote(target)
        target = target.split("#", 1)[0].strip()
        target = target.strip('"').strip("'")

        if not target:
            return ""
        lowered = target.lower()
        if lowered.startswith(EXTERNAL_SCHEMES):
            return ""
        if target.startswith("#"):
            return ""
        return target.replace("\\", "/")

    def _resolve_wikilink(self, target: str, source_file: Path) -> tuple[bool, str]:
        normalized = target.lstrip("/")
        source_dir = source_file.parent

        has_ext = Path(normalized).suffix != ""
        candidates: list[str] = []

        if has_ext:
            candidates.extend(
                [
                    self._to_rel(source_dir / normalized),
                    normalized,
                ]
            )
        else:
            candidates.extend(
                [
                    self._to_rel(source_dir / f"{normalized}.md"),
                    f"{normalized}.md",
                ]
            )

        for rel_candidate in candidates:
            if not rel_candidate:
                continue
            if self.index.exists_from_root(rel_candidate):
                return True, "resolved"

        dir_candidates = [
            self._to_rel(source_dir / normalized),
            normalized,
        ]
        for rel_candidate in dir_candidates:
            if not rel_candidate:
                continue
            if self.index.dir_exists_from_root(rel_candidate):
                return True, "resolved_as_directory"

        if "/" not in normalized:
            matches = self.index.notes_by_stem.get(self.index._norm_stem(normalized), [])
            if len(matches) == 1:
                return True, "resolved_by_stem"
            if len(matches) > 1:
                return False, "ambiguous_stem"

        return False, "not_found"

    def _resolve_markdown_link(self, target: str, source_file: Path) -> tuple[bool, str]:
        normalized = target.lstrip("/")
        source_dir = source_file.parent

        direct_candidates = [
            self._to_rel(source_dir / normalized),
            normalized,
        ]

        for rel_candidate in direct_candidates:
            if not rel_candidate:
                continue
            if self.index.exists_from_root(rel_candidate):
                return True, "resolved"

        # Markdown links that omit extension are usually notes.
        if Path(normalized).suffix == "":
            note_candidates = [
                self._to_rel(source_dir / f"{normalized}.md"),
                f"{normalized}.md",
            ]
            for rel_candidate in note_candidates:
                if not rel_candidate:
                    continue
                if self.index.exists_from_root(rel_candidate):
                    return True, "resolved_as_note"

            if "/" not in normalized:
                matches = self.index.notes_by_stem.get(self.index._norm_stem(normalized), [])
                if len(matches) == 1:
                    return True, "resolved_by_stem"
                if len(matches) > 1:
                    return False, "ambiguous_stem"

        return False, "not_found"

    def _to_rel(self, path: Path) -> str:
        try:
            return path.resolve().relative_to(self.vault_root.resolve()).as_posix()
        except Exception:
            return ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit an Obsidian vault for potentially broken wiki/markdown links.",
    )
    parser.add_argument(
        "--vault-root",
        default="/Users/giminu0930/Documents/Obsidian",
        help="Obsidian vault root path.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory where audit reports are written.",
    )
    parser.add_argument(
        "--max-md-items",
        type=int,
        default=200,
        help="Maximum number of broken-link entries to print in markdown report.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return exit code 2 when broken links are found.",
    )
    parser.add_argument(
        "--include-hidden",
        action="store_true",
        help="Include hidden folders/files (dot-prefixed) in markdown scan targets.",
    )
    parser.add_argument(
        "--exclude-dir",
        action="append",
        default=[".archive", ".trash"],
        help="Directory name to exclude from scan targets. Repeatable.",
    )
    return parser.parse_args()


def write_reports(
    output_dir: Path,
    vault_root: Path,
    broken_links: Iterable[BrokenLink],
    total_links: int,
    files_scanned: int,
    max_md_items: int,
) -> tuple[Path, Path]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    broken_list = list(broken_links)

    json_path = output_dir / f"obsidian_link_audit_{timestamp}.json"
    md_path = output_dir / f"obsidian_link_audit_{timestamp}.md"

    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "vault_root": str(vault_root),
        "files_scanned": files_scanned,
        "total_links": total_links,
        "broken_links": len(broken_list),
        "issues": [
            {
                "source": issue.source,
                "line": issue.line,
                "kind": issue.kind,
                "target": issue.target,
                "reason": issue.reason,
            }
            for issue in broken_list
        ],
    }

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines: list[str] = []
    lines.append("# Obsidian Link Audit")
    lines.append("")
    lines.append(f"- Timestamp: {payload['timestamp']}")
    lines.append(f"- Vault root: {vault_root}")
    lines.append(f"- Files scanned: {files_scanned}")
    lines.append(f"- Total links scanned: {total_links}")
    lines.append(f"- Broken links: {len(broken_list)}")
    lines.append("")

    reason_counts: dict[str, int] = defaultdict(int)
    for issue in broken_list:
        reason_counts[issue.reason] += 1

    lines.append("## Reason Summary")
    if reason_counts:
        for reason, count in sorted(reason_counts.items(), key=lambda item: item[0]):
            lines.append(f"- {reason}: {count}")
    else:
        lines.append("- none")
    lines.append("")

    lines.append("## Broken Link Samples")
    if broken_list:
        lines.append("| Source | Line | Kind | Target | Reason |")
        lines.append("|---|---:|---|---|---|")
        for issue in broken_list[: max_md_items if max_md_items > 0 else len(broken_list)]:
            safe_target = issue.target.replace("|", "\\|")
            lines.append(
                f"| {issue.source} | {issue.line} | {issue.kind} | {safe_target} | {issue.reason} |"
            )
        if len(broken_list) > max_md_items > 0:
            lines.append("")
            lines.append(f"- truncated: showing {max_md_items} of {len(broken_list)} issues")
    else:
        lines.append("- none")

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def main() -> int:
    args = parse_args()
    vault_root = Path(args.vault_root).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()

    if not vault_root.exists() or not vault_root.is_dir():
        print(f"Vault root does not exist or is not a directory: {vault_root}")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)

    excluded_dirs = {value for value in args.exclude_dir if value}
    auditor = LinkAuditor(
        vault_root=vault_root,
        include_hidden=args.include_hidden,
        excluded_dirs=excluded_dirs,
    )
    broken_links, total_links = auditor.audit()

    json_path, md_path = write_reports(
        output_dir=output_dir,
        vault_root=vault_root,
        broken_links=broken_links,
        total_links=total_links,
        files_scanned=len(auditor.index.markdown_files),
        max_md_items=args.max_md_items,
    )

    print(f"Vault root: {vault_root}")
    print(f"Files scanned: {len(auditor.index.markdown_files)}")
    print(f"Total links scanned: {total_links}")
    print(f"Broken links: {len(broken_links)}")
    print(f"JSON report: {json_path}")
    print(f"MD report: {md_path}")

    if args.strict and broken_links:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
