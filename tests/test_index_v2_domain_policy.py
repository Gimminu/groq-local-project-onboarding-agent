from __future__ import annotations

import json
from pathlib import Path

import pytest

import app.index_v2.classifier as classifier_module
import index_organizer


@pytest.fixture(autouse=True)
def _disable_llm_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(index_organizer, "load_local_env", lambda: None)
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.setattr(classifier_module, "_OLLAMA_AVAILABLE", False)


def test_legacy_general_focus_alias_maps_to_fallback_focus(make_v2_service) -> None:
    _, config, _ = make_v2_service({"repair_defaults": {"general_focus": "triage"}})

    assert config.repair_defaults.fallback_focus == "triage"
    assert config.repair_defaults.general_focus == "triage"


def test_dynamic_domain_candidate_is_review_gated_until_approved(make_v2_service) -> None:
    service, config, roots = make_v2_service(
        {
            "pattern_overrides": [
                {"pattern": "(?i)travel|booking|itinerary", "domain": "travel"},
            ],
            "domain_policy": {
                "pinned_domains": [],
            },
        }
    )
    watch_root = roots["watch_root"]
    watch_root.mkdir(parents=True, exist_ok=True)
    for name in ("travel-booking.pdf", "travel-itinerary.pdf", "travel-notes.pdf"):
        (watch_root / name).write_text("payload", encoding="utf-8")

    plan, _ = service.run_command(command="apply", apply_requested=False)
    assert plan is not None
    move_actions = [action for action in plan.actions if action.action_type == "move"]
    assert len(move_actions) == 3
    assert all("/review/" in str(action.destination_path) for action in move_actions if action.destination_path is not None)

    status_rows = service.semantic_policy.status_payload()
    travel = next(item for item in status_rows if item["domain"] == "travel")
    assert travel["status"] == "candidate"
    assert travel["candidate_ready"] is True
    assert travel["blocked_reasons"] == []
    assert config.parse_canonical_relative(Path("resources/travel/docs")) is None

    approved = service.semantic_policy.approve_domain("travel")
    assert approved.allowed is True
    parsed = config.parse_canonical_relative(Path("resources/travel/docs"))
    assert parsed is not None
    assert parsed[2] == "travel"


def test_domain_cli_commands_report_and_mutate_domain_registry(make_v2_service, capsys) -> None:
    service, config, roots = make_v2_service(
        {
            "pattern_overrides": [
                {"pattern": "(?i)travel|booking|itinerary", "domain": "travel"},
            ],
        }
    )
    watch_root = roots["watch_root"]
    watch_root.mkdir(parents=True, exist_ok=True)
    for name in ("travel-booking.pdf", "travel-itinerary.pdf", "travel-summary.pdf"):
        (watch_root / name).write_text("payload", encoding="utf-8")
    service.run_command(command="apply", apply_requested=False)

    assert index_organizer.run(["domain-status", "--config", str(config.config_path)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert any(item["domain"] == "travel" and item["status"] == "candidate" for item in payload)

    assert index_organizer.run(["domain-approve", "--config", str(config.config_path), "--domain", "travel"]) == 0
    approved = json.loads(capsys.readouterr().out)
    assert approved["domain"] == "travel"
    assert approved["allowed"] is True

    assert index_organizer.run(["domain-reject", "--config", str(config.config_path), "--domain", "travel"]) == 0
    rejected = json.loads(capsys.readouterr().out)
    assert rejected["domain"] == "travel"
    assert rejected["status"] == "rejected"


def test_banned_dynamic_domain_name_is_not_registered(make_v2_service) -> None:
    service, _, roots = make_v2_service(
        {
            "pattern_overrides": [
                {"pattern": "(?i)misc", "domain": "misc"},
            ],
        }
    )
    target = roots["watch_root"] / "misc-reference.pdf"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("payload", encoding="utf-8")

    service.run_command(command="apply", apply_requested=False)

    assert service.semantic_policy.status_payload() == []
