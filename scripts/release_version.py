#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path
import re

VERSION_RE = re.compile(r'(?m)^version\s*=\s*"(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)"\s*$')


def default_pyproject() -> Path:
    return Path(__file__).resolve().parents[1] / "pyproject.toml"


def parse_version(text: str) -> tuple[int, int, int]:
    match = VERSION_RE.search(text)
    if not match:
        raise ValueError("Could not find [project].version in pyproject.toml")
    return int(match.group("major")), int(match.group("minor")), int(match.group("patch"))


def format_version(version: tuple[int, int, int]) -> str:
    return f"{version[0]}.{version[1]}.{version[2]}"


def bump_version(version: tuple[int, int, int], part: str) -> tuple[int, int, int]:
    major, minor, patch = version
    if part == "major":
        return major + 1, 0, 0
    if part == "minor":
        return major, minor + 1, 0
    if part == "patch":
        return major, minor, patch + 1
    raise ValueError(f"Unsupported bump part: {part}")


def replace_version(text: str, new_version: str) -> str:
    if VERSION_RE.search(text) is None:
        raise ValueError("Could not find [project].version in pyproject.toml")
    return VERSION_RE.sub(f'version = "{new_version}"', text, count=1)


def ensure_changelog_entry(changelog_path: Path, version: str, *, entry_date: str | None = None) -> bool:
    heading = f"## v{version} - {entry_date or date.today().isoformat()}"
    entry_block = f"{heading}\n- TBD\n"

    if not changelog_path.exists():
        changelog_path.write_text(f"# Changelog\n\n{entry_block}", encoding="utf-8")
        return True

    content = changelog_path.read_text(encoding="utf-8")
    if heading in content:
        return False

    if content.startswith("# Changelog"):
        remaining = content[len("# Changelog") :].lstrip("\n")
        new_content = f"# Changelog\n\n{entry_block}\n{remaining}" if remaining else f"# Changelog\n\n{entry_block}"
    else:
        new_content = f"# Changelog\n\n{entry_block}\n{content}"

    changelog_path.write_text(new_content, encoding="utf-8")
    return True


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Release version helper")
    sub = parser.add_subparsers(dest="command", required=True)

    show_parser = sub.add_parser("show", help="Show current version from pyproject.toml")
    show_parser.add_argument("--file", default=str(default_pyproject()), help="Path to pyproject.toml")

    bump_parser = sub.add_parser("bump", help="Bump semantic version")
    bump_parser.add_argument("--part", choices=("patch", "minor", "major"), default="patch")
    bump_parser.add_argument("--file", default=str(default_pyproject()), help="Path to pyproject.toml")
    bump_parser.add_argument("--apply", action="store_true", help="Write bumped version to pyproject.toml")
    bump_parser.add_argument("--update-changelog", action="store_true", help="Add release heading to CHANGELOG.md")
    bump_parser.add_argument("--changelog", default="CHANGELOG.md", help="Changelog path")
    bump_parser.add_argument("--tag", action="store_true", help="Create git tag (requires --apply)")

    args = parser.parse_args(argv)

    pyproject_path = Path(args.file).expanduser().resolve()
    if not pyproject_path.exists():
        print(f"error: pyproject file not found: {pyproject_path}", file=sys.stderr)
        return 1

    try:
        payload = pyproject_path.read_text(encoding="utf-8")
        current = parse_version(payload)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.command == "show":
        print(format_version(current))
        return 0

    if args.tag and not args.apply:
        print("error: --tag requires --apply", file=sys.stderr)
        return 1

    next_version = bump_version(current, args.part)
    current_s = format_version(current)
    next_s = format_version(next_version)

    print(f"current={current_s}")
    print(f"next={next_s}")

    if not args.apply:
        print("preview-only=true")
        print("hint=Run again with --apply to write pyproject.toml")
        return 0

    updated_payload = replace_version(payload, next_s)
    pyproject_path.write_text(updated_payload, encoding="utf-8")
    print(f"updated_pyproject={pyproject_path}")

    if args.update_changelog:
        changelog_path = Path(args.changelog).expanduser().resolve()
        inserted = ensure_changelog_entry(changelog_path, next_s)
        print(f"updated_changelog={changelog_path}")
        print(f"changelog_entry_added={str(inserted).lower()}")

    if args.tag:
        tag_name = f"v{next_s}"
        try:
            subprocess.run(["git", "tag", tag_name], check=True)
        except subprocess.CalledProcessError as exc:
            print(f"error: failed to create tag {tag_name}: {exc}", file=sys.stderr)
            return 1
        print(f"created_tag={tag_name}")

    return 0


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
