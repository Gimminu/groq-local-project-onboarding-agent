from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.organizer_launchd import build_launch_agent_plist
from app.organizer_types import OrganizerConfig


class OrganizerLaunchdTests(unittest.TestCase):
    def build_config(self, source_root: Path) -> OrganizerConfig:
        return OrganizerConfig(
            source_root=source_root,
            target_root=source_root,
            output_dir=Path("outputs"),
            provider="heuristic",
            min_age_seconds=20,
            watch_interval_seconds=12,
        )

    def test_watch_plist_uses_watch_once_and_watchpaths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source_root = Path(temp_dir)
            config = self.build_config(source_root)

            plist = build_launch_agent_plist(
                label_prefix="com.example.organizer.downloads",
                mode="watch",
                config=config,
                repo_root=Path("/repo"),
                python_executable="/usr/bin/python3",
                source_argument=None,
                profile="downloads",
                mcp_config_path=Path("/repo/mcp/.vscode/mcp.json"),
            )

            self.assertEqual("watch-once", plist["ProgramArguments"][2])
            self.assertIn(str(source_root), plist["WatchPaths"])
            self.assertIn("--min-age-seconds", plist["ProgramArguments"])
            min_age_index = plist["ProgramArguments"].index("--min-age-seconds")
            self.assertEqual("20", plist["ProgramArguments"][min_age_index + 1])
            self.assertEqual(12, plist["ThrottleInterval"])

    def test_daily_plist_uses_calendar_interval(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source_root = Path(temp_dir)
            config = self.build_config(source_root)

            plist = build_launch_agent_plist(
                label_prefix="com.example.organizer.documents",
                mode="daily",
                config=config,
                repo_root=Path("/repo"),
                python_executable="/usr/bin/python3",
                source_argument=None,
                profile="documents",
                mcp_config_path=Path("/repo/mcp/.vscode/mcp.json"),
                daily_minute=20,
            )

            self.assertEqual("daily", plist["ProgramArguments"][2])
            self.assertEqual({"Hour": 9, "Minute": 20}, plist["StartCalendarInterval"])

    def test_standard_watch_plist_uses_single_agent_for_multiple_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source_root = Path(temp_dir)
            config = self.build_config(source_root)
            watch_paths = ["/Users/test/Downloads", "/Users/test/Desktop", "/Users/test/Documents"]

            plist = build_launch_agent_plist(
                label_prefix="com.example.organizer.standard",
                mode="watch",
                config=config,
                repo_root=Path("/repo"),
                python_executable="/usr/bin/python3",
                source_argument=None,
                profile=None,
                mcp_config_path=Path("/repo/mcp/.vscode/mcp.json"),
                command_name="watch-standard-once",
                watch_paths=watch_paths,
            )

            self.assertEqual("watch-standard-once", plist["ProgramArguments"][2])
            self.assertEqual(watch_paths, plist["WatchPaths"])


if __name__ == "__main__":
    unittest.main()
