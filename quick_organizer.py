#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

PROFILE_CHOICES = ("downloads", "desktop", "documents")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Index-Friendly Folder Manager V2 실행기")
    parser.add_argument(
        "command",
        choices=(
            "preview",
            "organize-now",
            "migrate-preview",
            "migrate-apply",
            "archive-preview",
            "archive-now",
            "status",
            "repair-outputs",
            "watch",
            "service-on",
            "service-off",
            "service-status",
        ),
    )
    parser.add_argument("--config", default=default_config_path(), help="V2 YAML config path")
    parser.add_argument("--profile", choices=PROFILE_CHOICES, default="downloads", help="호환성용 표시값")
    return parser.parse_args(argv)


def default_config_path() -> str:
    if os.getenv("INDEX_ORGANIZER_CONFIG"):
        return os.getenv("INDEX_ORGANIZER_CONFIG", "")
    home_config = Path.home() / "folder-organizer-v2.yml"
    if home_config.exists():
        return str(home_config)
    return str(Path(__file__).resolve().with_name("samples").joinpath("index_organizer_v2.example.yml"))


def organizer_path() -> Path:
    return Path(__file__).resolve().with_name("organizer.py")


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    command_map = {
        "preview": ("plan", False),
        "organize-now": ("apply", True),
        "migrate-preview": ("migrate", False),
        "migrate-apply": ("migrate", True),
        "archive-preview": ("archive", False),
        "archive-now": ("archive", True),
        "status": ("status", False),
        "repair-outputs": ("repair-outputs", True),
        "watch": ("watch", True),
        "service-on": ("service-install", False),
        "service-off": ("service-uninstall", False),
        "service-status": ("service-status", False),
    }
    command, needs_apply = command_map[args.command]

    payload = [sys.executable, str(organizer_path()), command, "--config", args.config]
    if needs_apply:
        payload.append("--apply")

    completed = subprocess.run(payload, check=False)
    return completed.returncode


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
