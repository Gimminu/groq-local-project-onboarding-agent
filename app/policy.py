from __future__ import annotations

from app.schema import ToolDescriptor

MODE_VALUES = ("safe", "write", "full")

SAFE_TOOL_NAMES = {
    "fs_list",
    "fs_read",
    "system_health_state",
    "system_health_metrics",
    "hybrid_llm_route",
    "scheduler_list_tasks",
}

WRITE_TOOL_NAMES = SAFE_TOOL_NAMES | {
    "fs_write",
    "fs_mkdir",
    "fs_copy",
    "fs_move",
    "fs_archive",
    "fs_organize",
    "scheduler_add_task",
    "scheduler_tick",
}


def filter_tools_for_mode(tools: list[ToolDescriptor], mode: str) -> list[ToolDescriptor]:
    if mode == "full":
        return sorted(tools, key=lambda tool: tool.name)
    if mode == "write":
        allowed = WRITE_TOOL_NAMES
    else:
        allowed = SAFE_TOOL_NAMES
    return sorted(
        [tool for tool in tools if tool.name in allowed],
        key=lambda tool: tool.name,
    )


def is_tool_allowed(tool_name: str, mode: str, available_tools: list[ToolDescriptor]) -> bool:
    return any(tool.name == tool_name for tool in filter_tools_for_mode(available_tools, mode))
