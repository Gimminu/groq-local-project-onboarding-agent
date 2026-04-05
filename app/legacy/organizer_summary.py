from __future__ import annotations

from app.organizer_types import OrganizerPlan


def render_compact_summary(plan: OrganizerPlan) -> str:
    summary = plan.summary()
    lines = [
        f"폴더: {plan.source_root}",
        f"분류기: {plan.provider_used}",
        f"이동 예정: {summary['planned_moves']}개",
        f"수동 검토: {summary['manual_review']}개",
        f"건너뜀: {summary['skipped']}개",
        f"낮은 확신/중위험: {summary['low_confidence']}개",
        "",
        "PARA 요약:",
    ]

    for category, count in sorted(
        summary["category_counts"].items(),
        key=lambda item: (-item[1], item[0]),
    ):
        lines.append(f"- {category}: {count}개")

    examples = plan.decisions[:8]
    if examples:
        lines.extend(("", "예시 항목:"))
        for item in examples:
            target = item.category_label if item.destination_path else item.status
            lines.append(
                f"- {item.source_path.name} -> {target} [{item.action} {item.confidence:.2f}]"
            )

    needs_review = [
        item for item in plan.decisions if item.review_required or item.risk_level != "low" or item.confidence < 0.85
    ][:6]
    if needs_review:
        lines.extend(("", "한 번 확인할 항목:"))
        for item in needs_review:
            lines.append(
                f"- {item.source_path.name} [{item.category_label} {item.risk_level} {item.confidence:.2f}]"
            )
    return "\n".join(lines)
