from __future__ import annotations

from typing import Any

try:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
except ImportError:  # pragma: no cover - exercised when dependency is absent
    ClientSession = None
    StdioServerParameters = None
    stdio_client = None

from app.config import ServerConfig
from app.errors import AppError
from app.schema import ToolCallResult, ToolDescriptor


class StdioMCPToolClient:
    def __init__(self, server_config: ServerConfig) -> None:
        self.server_config = server_config
        self._stdio_cm = None
        self._session_cm = None
        self._session = None

    async def __aenter__(self) -> "StdioMCPToolClient":
        if ClientSession is None or StdioServerParameters is None or stdio_client is None:
            raise AppError(
                "`mcp` 패키지가 설치되지 않았습니다. "
                "`pip install -r requirements.txt`를 먼저 실행하세요."
            )

        params = StdioServerParameters(
            command=self.server_config.command,
            args=self.server_config.args,
            env=self.server_config.env,
        )

        self._stdio_cm = stdio_client(params)
        read_stream, write_stream = await self._stdio_cm.__aenter__()
        self._session_cm = ClientSession(read_stream, write_stream)
        self._session = await self._session_cm.__aenter__()
        await self._session.initialize()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session_cm is not None:
            await self._session_cm.__aexit__(exc_type, exc, tb)
        if self._stdio_cm is not None:
            await self._stdio_cm.__aexit__(exc_type, exc, tb)

    async def list_tools(self) -> list[ToolDescriptor]:
        try:
            result = await self._session.list_tools()
        except Exception as exc:  # pragma: no cover - depends on MCP server
            raise AppError(f"MCP tool 목록 조회에 실패했습니다: {exc}") from exc

        tools: list[ToolDescriptor] = []
        for tool in getattr(result, "tools", []):
            tools.append(
                ToolDescriptor(
                    name=getattr(tool, "name", ""),
                    title=getattr(tool, "title", None),
                    description=getattr(tool, "description", None),
                    input_schema=_to_serializable(getattr(tool, "inputSchema", None)),
                )
            )
        return tools

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> ToolCallResult:
        try:
            result = await self._session.call_tool(name, arguments=arguments)
        except Exception as exc:  # pragma: no cover - depends on MCP server
            raise AppError(f"MCP tool `{name}` 호출에 실패했습니다: {exc}") from exc

        text_chunks: list[str] = []
        for block in getattr(result, "content", []):
            text = getattr(block, "text", None)
            if isinstance(text, str):
                text_chunks.append(text)
            elif isinstance(block, dict) and isinstance(block.get("text"), str):
                text_chunks.append(block["text"])

        structured = getattr(result, "structuredContent", None)
        text = "\n".join(chunk for chunk in text_chunks if chunk).strip() or "[empty tool result]"
        return ToolCallResult(
            is_error=bool(getattr(result, "isError", False)),
            text=text,
            structured_content=_to_serializable(structured),
        )


def _to_serializable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_to_serializable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _to_serializable(item) for key, item in value.items()}
    if hasattr(value, "model_dump"):
        return _to_serializable(value.model_dump())
    if hasattr(value, "dict"):
        return _to_serializable(value.dict())
    return str(value)
