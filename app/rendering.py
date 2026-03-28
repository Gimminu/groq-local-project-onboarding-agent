from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from app.schema import AgentRunTrace


def write_trace_files(trace: AgentRunTrace, output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    base_name = "project_onboarding_" + datetime.now().strftime("%Y%m%d_%H%M%S")

    json_path = output_dir / f"{base_name}.json"
    markdown_path = output_dir / f"{base_name}.md"

    json_path.write_text(
        json.dumps(trace.to_dict(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_markdown(trace), encoding="utf-8")
    return json_path, markdown_path


def render_markdown(trace: AgentRunTrace) -> str:
    lines: list[str] = [
        "# Groq Project Onboarding Run",
        "",
        "## Request",
        trace.request,
        "",
        "## Context",
        f"- created_at: {trace.created_at}",
        f"- server: {trace.server_name}",
        f"- mode: {trace.mode}",
        f"- model: {trace.model}",
        f"- available_tools: {', '.join(trace.available_tools)}",
        "",
        "## Tool Steps",
        *_render_steps(trace),
        "",
        "## Final Report",
        trace.final_answer,
        "",
    ]
    return "\n".join(lines)


def _render_steps(trace: AgentRunTrace) -> list[str]:
    if not trace.steps:
        return ["- tool 호출 없음"]

    rendered: list[str] = []
    for step in trace.steps:
        rendered.extend(
            [
                f"{step.step_number}. {step.tool_name}",
                f"   - reasoning: {step.reasoning}",
                f"   - is_error: {step.is_error}",
                f"   - arguments: {json.dumps(step.arguments, ensure_ascii=False)}",
                f"   - result: {step.text}",
            ]
        )
    return rendered
