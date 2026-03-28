from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from app.config import load_mcp_servers


class ConfigTests(unittest.TestCase):
    def test_load_mcp_servers_resolves_relative_script_argument(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir) / "workspace"
            vscode_dir = workspace / ".vscode"
            vscode_dir.mkdir(parents=True)
            server_script = workspace / "server.js"
            server_script.write_text("console.log('ok')\n", encoding="utf-8")

            config_path = vscode_dir / "mcp.json"
            config_path.write_text(
                json.dumps(
                    {
                        "servers": {
                            "local-fs": {
                                "type": "stdio",
                                "command": "node",
                                "args": ["server.js"],
                                "env": {"MCP_FS_ROOT": "/tmp/demo"},
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            servers = load_mcp_servers(config_path)

        server = servers["local-fs"]
        self.assertEqual(server.command, "node")
        self.assertEqual(server.args[0], str(server_script.resolve()))
        self.assertEqual(server.env["MCP_FS_ROOT"], "/tmp/demo")


if __name__ == "__main__":
    unittest.main()
