from __future__ import annotations

import unittest

from app.policy import filter_tools_for_mode
from app.schema import ToolDescriptor


class PolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tools = [
            ToolDescriptor("fs_read", None, "read", {}),
            ToolDescriptor("fs_write", None, "write", {}),
            ToolDescriptor("ssh_exec", None, "ssh", {}),
            ToolDescriptor("custom_unknown", None, "unknown", {}),
        ]

    def test_safe_mode_filters_read_only_tools(self) -> None:
        filtered = filter_tools_for_mode(self.tools, "safe")
        self.assertEqual([tool.name for tool in filtered], ["fs_read"])

    def test_write_mode_includes_write_tools(self) -> None:
        filtered = filter_tools_for_mode(self.tools, "write")
        self.assertEqual([tool.name for tool in filtered], ["fs_read", "fs_write"])

    def test_full_mode_keeps_all_discovered_tools(self) -> None:
        filtered = filter_tools_for_mode(self.tools, "full")
        self.assertEqual(
            [tool.name for tool in filtered],
            ["custom_unknown", "fs_read", "fs_write", "ssh_exec"],
        )


if __name__ == "__main__":
    unittest.main()
