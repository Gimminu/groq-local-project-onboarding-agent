#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from dataclasses import replace
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional runtime dependency
    load_dotenv = None

from app.errors import AppError
from app.organizer import IndexOrganizerService, load_index_config
from app.index_v2.launchd import (
    DEFAULT_V2_LAUNCHD_LABEL,
    ensure_user_config,
    install_launch_agents,
    resolve_launchd_python_executable,
    service_status,
    uninstall_launch_agents,
)

CLI_COMMANDS = (
    "status",
    "scan",
    "plan",
    "apply",
    "undo",
    "watch",
    "service-run",
    "service-tick",
    "review-drain",
    "archive",
    "report",
    "migrate",
    "stabilize",
    "repair-projects",
    "repair-tree",
    "repair-code-names",
    "repair-outputs",
    "confirm-delete",
    "domain-status",
    "domain-approve",
    "domain-reject",
    "service-install",
    "service-uninstall",
    "service-status",
)
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "samples" / "index_organizer_v2.example.yml"
DEFAULT_USER_CONFIG_PATH = Path.home() / "folder-organizer-v2.yml"
ACTIONABLE_TYPES = ("move", "rename", "archive", "quarantine")
KNOWN_GATE_STATES = (
    "stable_candidate",
    "incomplete_or_transient",
    "installer_artifact",
    "system_or_dependency",
    "cloud_placeholder",
    "empty_container",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Index-Friendly Folder Manager V2")
    parser.add_argument("command", choices=CLI_COMMANDS)
    parser.add_argument(
        "--config",
        default=(str(Path.home() / "folder-organizer-v2.yml") if (Path.home() / "folder-organizer-v2.yml").exists() else str(DEFAULT_CONFIG_PATH)),
        help="YAML config path for V2",
    )
    parser.add_argument("--apply", action="store_true", help="Execute filesystem mutations instead of dry-run output")
    parser.add_argument("--undo-limit", type=int, default=1, help="How many applied operations to reverse for undo")
    parser.add_argument("--delete-target", help="Proposal id/path/quarantine path for confirm-delete")
    parser.add_argument("--domain", help="Domain name for domain-approve/domain-reject")
    parser.add_argument("--review-limit", type=int, default=5, help="How many review candidates to print in status output")
    return parser.parse_args(argv)


def load_local_env() -> None:
    env_path = Path(__file__).with_name(".env")
    if load_dotenv is None:
        return
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()


def _actionable_count(summary: dict[str, Any]) -> int:
    return sum(int(summary.get(action_type, 0)) for action_type in ACTIONABLE_TYPES)


def _operation_state(summary: dict[str, Any], *, watch_root_issues: int = 0) -> str:
    if watch_root_issues > 0:
        return "CONFIG_WARNING"
    actionable = _actionable_count(summary)
    review = int(summary.get("flag_for_review", 0))
    if actionable > 0:
        return "ACTION_REQUIRED"
    if review > 0:
        return "MANUAL_REVIEW_PENDING"
    return "CONVERGED_OR_IDLE"


def _watch_root_health(config: Any) -> dict[str, Any]:
    roots = tuple(Path(path).expanduser() for path in getattr(config, "watch_roots", ()) or ())
    missing: list[Path] = []
    not_directories: list[Path] = []
    existing: list[Path] = []
    for root in roots:
        if not root.exists():
            missing.append(root)
            continue
        if not root.is_dir():
            not_directories.append(root)
            continue
        existing.append(root)
    return {
        "total": len(roots),
        "existing": existing,
        "missing": missing,
        "not_directories": not_directories,
    }


def _read_service_state(config_path: Path) -> dict[str, Any]:
    try:
        config = load_index_config(config_path)
        state_path = config.service_state_path
    except Exception:
        return {}
    if not state_path.exists():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _llm_runtime_status(config: Any) -> dict[str, Any]:
    provider_env = os.getenv("LLM_PROVIDER")
    preferred_provider = provider_env.strip().lower() if provider_env else str(config.llm.preferred_provider)
    db_states = _provider_state_snapshot(config)

    providers: dict[str, dict[str, Any]] = {}
    active_providers: list[str] = []
    for provider in ("groq", "gemini", "ollama"):
        configured = _provider_is_configured(config, provider)
        ready = _provider_is_ready(config, provider)
        state = db_states.get(provider, {})
        cooldown_until = state.get("cooldown_until")
        cooling_down = bool(cooldown_until) and _parse_iso(cooldown_until) and _parse_iso(cooldown_until) > datetime.now(timezone.utc)
        provider_payload = {
            "configured": configured,
            "ready": ready,
            "cooldown_until": cooldown_until,
            "cooling_down": cooling_down,
            "last_error_code": state.get("last_error_code"),
            "consecutive_rate_limits": int(state.get("consecutive_rate_limits") or 0),
        }
        providers[provider] = provider_payload
        if configured and ready and not cooling_down:
            active_providers.append(provider)

    active = bool(active_providers)
    if not active_providers:
        reason = "no_provider_ready"
    elif preferred_provider not in active_providers:
        reason = "preferred_provider_unavailable"
    else:
        reason = "active"
    return {
        "provider": preferred_provider,
        "preferred_provider": preferred_provider,
        "providers": providers,
        "active_providers": active_providers,
        "config_enabled": True,
        "api_ready": active,
        "active": active,
        "reason": reason,
        "watch_fallback_enabled": bool(config.llm.enable_for_watch),
        "fallback_to_other_cloud": bool(config.llm.fallback_to_other_cloud),
        "fallback_to_ollama": bool(config.llm.fallback_to_ollama),
    }


def _provider_state_snapshot(config: Any) -> dict[str, dict[str, Any]]:
    path = config.database_path
    if not path.exists():
        return {}
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
    except sqlite3.Error:
        return {}
    try:
        rows = connection.execute(
            "SELECT provider, cooldown_until, last_error_code, consecutive_rate_limits FROM provider_state"
        ).fetchall()
    except sqlite3.Error:
        return {}
    finally:
        connection.close()
    return {
        str(row["provider"]): {
            "cooldown_until": row["cooldown_until"],
            "last_error_code": row["last_error_code"],
            "consecutive_rate_limits": row["consecutive_rate_limits"],
        }
        for row in rows
    }


def _staging_snapshot(config: Any) -> dict[str, Any]:
    path = config.database_path
    summary: dict[str, Any] = {
        "total": 0,
        "deferred": 0,
        "ready": 0,
        "by_gate_state": {state: 0 for state in KNOWN_GATE_STATES},
        "next_defer_until": None,
    }
    if not path.exists():
        return summary
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
    except sqlite3.Error:
        return summary
    try:
        rows = connection.execute(
            "SELECT gate_state, defer_until FROM staging_queue"
        ).fetchall()
    except sqlite3.Error:
        return summary
    finally:
        connection.close()

    summary["total"] = len(rows)
    next_defer: str | None = None
    for row in rows:
        gate_state = str(row["gate_state"] or "stable_candidate")
        summary["by_gate_state"][gate_state] = int(summary["by_gate_state"].get(gate_state, 0)) + 1
        defer_until = row["defer_until"]
        if defer_until:
            summary["deferred"] += 1
            if next_defer is None or (parsed := _parse_iso(defer_until)) and parsed < _parse_iso(next_defer):
                next_defer = defer_until
        elif gate_state == "stable_candidate":
            summary["ready"] += 1
    summary["next_defer_until"] = next_defer
    return summary


def _adaptive_review_snapshot(config: Any) -> dict[str, Any]:
    root = config.adaptive_review_root
    summary: dict[str, Any] = {
        "total": 0,
        "by_section": {},
        "items": [],
    }
    if not root.exists() or not root.is_dir():
        return summary
    for candidate in sorted(root.rglob("*"), key=lambda path: str(path).lower()):
        if candidate.name == ".DS_Store":
            continue
        if not candidate.is_file():
            continue
        try:
            relative = candidate.relative_to(root)
        except ValueError:
            continue
        top = relative.parts[0] if relative.parts else "__root__"
        summary["total"] += 1
        summary["by_section"][top] = int(summary["by_section"].get(top, 0)) + 1
        summary["items"].append(str(relative))
    return summary


def _provider_is_configured(config: Any, provider: str) -> bool:
    if provider == "groq":
        return bool(config.groq.enabled and os.getenv("GROQ_API_KEY"))
    if provider == "gemini":
        return bool(os.getenv("GEMINI_API_KEY"))
    if provider == "ollama":
        return bool(config.llm.fallback_to_ollama)
    return False


def _provider_is_ready(config: Any, provider: str) -> bool:
    if provider == "groq":
        return bool(config.groq.enabled and os.getenv("GROQ_API_KEY") and importlib.util.find_spec("groq"))
    if provider == "gemini":
        return bool(os.getenv("GEMINI_API_KEY") and importlib.util.find_spec("google.generativeai"))
    if provider == "ollama":
        return _ollama_ready(config)
    return False


def _ollama_ready(config: Any) -> bool:
    try:
        import urllib.request
    except ImportError:
        return False
    base_url = str(config.llm.ollama.base_url).rstrip("/")
    try:
        req = urllib.request.Request(f"{base_url}/api/tags", method="GET")
        with urllib.request.urlopen(req, timeout=int(config.llm.ollama.healthcheck_timeout_seconds)) as resp:
            return resp.status == 200
    except Exception:
        return False


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _top_review_items(plan: Any, *, limit: int = 5) -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []
    for action in getattr(plan, "actions", []):
        if getattr(action, "action_type", "") != "flag_for_review":
            continue
        source_path = str(getattr(action, "source_path", "-"))
        reason = str(getattr(action, "reason", "manual review required"))
        items.append((source_path, reason))
        if len(items) >= limit:
            break
    return items


def _build_status_plan(config):
    # `status` should be observable even when the real state DB is busy or
    # unwritable. Use an ephemeral state dir so the command remains read-only
    # with respect to the live organizer runtime.
    with tempfile.TemporaryDirectory(prefix="index-organizer-status-") as tmpdir:
        status_config = replace(config, state_dir=Path(tmpdir))
        status_config.ensure_directories()
        service = IndexOrganizerService(status_config)
        try:
            nodes = service.scan(roots=service._roots_for_command("plan"))  # type: ignore[attr-defined]
            plan = service.planner.build_plan(command="plan", nodes=nodes)
        finally:
            service.close()
    return plan, {"report_json": "-", "report_md": "-"}


def run(argv: list[str] | None = None) -> int:
    load_local_env()
    args = parse_args(argv)
    config_path = Path(args.config).expanduser()
    try:
        if args.command == "status":
            config = load_index_config(config_path)
            domain_candidates_payload: list[dict[str, Any]] = []
            try:
                service = IndexOrganizerService(config)
                try:
                    plan, extras = service.run_command(command="plan", apply_requested=False)
                    domain_candidates_payload = service.semantic_policy.status_payload()
                finally:
                    service.close()
            except (OSError, PermissionError, sqlite3.Error):
                plan, extras = _build_status_plan(config)
            if plan is None:
                print("status=UNKNOWN")
                return 1

            summary = plan.summary()
            actionable = _actionable_count(summary)
            review = int(summary.get("flag_for_review", 0))
            svc = service_status(DEFAULT_V2_LAUNCHD_LABEL)
            loaded = bool(svc.get("service", {}).get("loaded", False))
            service_state = _read_service_state(config_path)
            llm_status = _llm_runtime_status(config)
            staging_status = _staging_snapshot(config)
            adaptive_review_status = _adaptive_review_snapshot(config)
            watch_root_health = _watch_root_health(config)
            watch_root_issue_count = len(watch_root_health["missing"]) + len(watch_root_health["not_directories"])

            print("command=status")
            print(f"service_loaded={str(loaded).lower()}")
            print(f"spaces_root={config.spaces_root}")
            print(f"watch_roots={','.join(str(path) for path in config.watch_roots) or '-'}")
            print(f"watch_root_total={watch_root_health['total']}")
            print(f"watch_root_existing={len(watch_root_health['existing'])}")
            print(f"watch_root_missing={len(watch_root_health['missing'])}")
            print(f"watch_root_not_directory={len(watch_root_health['not_directories'])}")
            for index, path in enumerate(watch_root_health["missing"], start=1):
                print(f"watch_root_missing_{index}={path}")
            for index, path in enumerate(watch_root_health["not_directories"], start=1):
                print(f"watch_root_not_directory_{index}={path}")
            print(f"operation_state={_operation_state(summary, watch_root_issues=watch_root_issue_count)}")
            print(f"pending_actionable={actionable}")
            print(f"pending_review={review}")
            print(f"pending_move={int(summary.get('move', 0))}")
            print(f"pending_rename={int(summary.get('rename', 0))}")
            print(f"pending_archive={int(summary.get('archive', 0))}")
            print(f"pending_quarantine={int(summary.get('quarantine', 0))}")
            print(f"llm_provider={llm_status['provider']}")
            print(f"llm_preferred_provider={llm_status['preferred_provider']}")
            print(f"llm_enabled_in_config={str(llm_status['config_enabled']).lower()}")
            print(f"llm_api_ready={str(llm_status['api_ready']).lower()}")
            print(f"llm_fallback_active={str(llm_status['active']).lower()}")
            print(f"llm_active_providers={','.join(llm_status['active_providers']) or '-'}")
            print(f"llm_watch_fallback_enabled={str(llm_status['watch_fallback_enabled']).lower()}")
            print(f"llm_fallback_to_other_cloud={str(llm_status['fallback_to_other_cloud']).lower()}")
            print(f"llm_fallback_to_ollama={str(llm_status['fallback_to_ollama']).lower()}")
            for provider, provider_state in llm_status["providers"].items():
                print(f"llm_provider_{provider}_configured={str(provider_state['configured']).lower()}")
                print(f"llm_provider_{provider}_ready={str(provider_state['ready']).lower()}")
                print(f"llm_provider_{provider}_cooling_down={str(provider_state['cooling_down']).lower()}")
                print(f"llm_provider_{provider}_cooldown_until={provider_state['cooldown_until'] or '-'}")
                print(f"llm_provider_{provider}_last_error={provider_state['last_error_code'] or '-'}")
                print(f"llm_provider_{provider}_rate_limits={provider_state['consecutive_rate_limits']}")
            print(f"staging_total={staging_status['total']}")
            print(f"staging_ready={staging_status['ready']}")
            print(f"staging_deferred={staging_status['deferred']}")
            print(f"staging_next_defer_until={staging_status['next_defer_until'] or '-'}")
            for gate_state, count in sorted(staging_status["by_gate_state"].items()):
                print(f"staging_gate_{gate_state}={count}")
            print(f"adaptive_review_total={adaptive_review_status['total']}")
            for section, count in sorted(adaptive_review_status["by_section"].items()):
                print(f"adaptive_review_section_{section}={count}")
            print(f"plan_report_json={extras.get('report_json', '-')}")
            print(f"plan_report_md={extras.get('report_md', '-')}")
            if service_state:
                print(f"last_watch_at={service_state.get('last_watch_at', '-')}")
                print(f"last_watch_total={service_state.get('last_watch_total', '-')}")
                print(f"last_watch_reported={service_state.get('last_watch_reported', '-')}")
                print(f"last_housekeeping_at={service_state.get('last_housekeeping_at', '-')}")
                print(f"last_adaptive_review_drain_at={service_state.get('last_adaptive_review_drain', '-')}")
                print(f"last_report_at={service_state.get('last_report', '-')}")
                print(f"last_archive_at={service_state.get('last_archive', '-')}")
            domain_limit = max(1, int(args.review_limit))
            domain_candidates = domain_candidates_payload[:domain_limit]
            print(f"candidate_domain_total={len(domain_candidates_payload)}")
            for index, item in enumerate(domain_candidates, start=1):
                blocked = ",".join(str(value) for value in item.get("blocked_reasons", [])) or "-"
                print(f"candidate_domain_{index}_name={item.get('domain', '-')}")
                print(f"candidate_domain_{index}_status={item.get('status', '-')}")
                print(f"candidate_domain_{index}_ready={str(item.get('candidate_ready', False)).lower()}")
                print(f"candidate_domain_{index}_count={int(item.get('observation_count', 0))}")
                print(f"candidate_domain_{index}_blocked={blocked}")
            if review > 0:
                review_limit = max(1, int(args.review_limit))
                for index, (path, reason) in enumerate(_top_review_items(plan, limit=review_limit), start=1):
                    print(f"review_item_{index}_path={path}")
                    print(f"review_item_{index}_reason={reason}")
            if adaptive_review_status["total"] > 0:
                review_limit = max(1, int(args.review_limit))
                for index, item in enumerate(adaptive_review_status["items"][:review_limit], start=1):
                    print(f"adaptive_review_item_{index}_path={item}")
            if actionable == 0:
                print("status_hint=No automatic move/rename/archive/quarantine is currently pending.")
            if review > 0:
                print("status_hint_review=Manual review items remain. Resolve review items to enable further automatic progress.")
            if adaptive_review_status["total"] > 0:
                print("status_hint_adaptive_review=Hidden adaptive-review backlog exists. The service will retry it automatically, or run `index_organizer.py review-drain --apply` to force an immediate pass.")
            if not llm_status["active"]:
                print(f"status_hint_llm=LLM fallback is currently {llm_status['reason']}. Set the API key and keep groq.enabled=true to let ambiguous Desktop/Documents/Downloads items use AI classification.")
            if int(staging_status["deferred"]) > 0:
                print("status_hint_deferred=Some staged items are deferred and will retry after provider cooldown or the next watch tick.")
            if actionable > 0 or review > 0:
                print("status_hint_stabilize=Run `index_organizer.py stabilize --apply` once to absorb deterministic repair leftovers before relying on watch mode.")
            if watch_root_issue_count > 0:
                print("status_hint_watch_roots=One or more configured watch roots are missing or invalid. Recreate the paths or update watch_roots in config before relying on service status.")
            return 0

        if args.command == "service-install":
            if not config_path.exists():
                if config_path == DEFAULT_CONFIG_PATH:
                    config_path = ensure_user_config(DEFAULT_USER_CONFIG_PATH, DEFAULT_CONFIG_PATH)
                else:
                    raise AppError(f"설정 파일을 찾을 수 없습니다: {config_path}")
            config = load_index_config(config_path)
            resolved_config_path = config.config_path
            installed = install_launch_agents(
                label_prefix=DEFAULT_V2_LAUNCHD_LABEL,
                repo_root=Path(__file__).resolve().parent,
                python_executable=resolve_launchd_python_executable(current_executable=sys.executable),
                config_path=resolved_config_path,
                config=config,
            )
            print(f"config={resolved_config_path}")
            for mode, plist_path in installed.items():
                print(f"{mode}={plist_path}")
            return 0
        if args.command == "service-uninstall":
            removed = uninstall_launch_agents(DEFAULT_V2_LAUNCHD_LABEL)
            for plist_path in removed:
                print(f"removed={plist_path}")
            return 0
        if args.command == "service-status":
            print(json.dumps(service_status(DEFAULT_V2_LAUNCHD_LABEL), ensure_ascii=False, indent=2))
            return 0

        config = load_index_config(config_path)
        service = IndexOrganizerService(config)
        try:
            if args.command == "domain-status":
                print(json.dumps(service.semantic_policy.status_payload(), ensure_ascii=False, indent=2))
                return 0
            if args.command == "domain-approve":
                if not args.domain:
                    raise AppError("--domain is required for domain-approve")
                try:
                    payload = service.semantic_policy.approve_domain(args.domain)
                except ValueError as exc:
                    raise AppError(str(exc)) from exc
                print(json.dumps(payload.to_dict(), ensure_ascii=False, indent=2))
                return 0
            if args.command == "domain-reject":
                if not args.domain:
                    raise AppError("--domain is required for domain-reject")
                try:
                    payload = service.semantic_policy.reject_domain(args.domain)
                except ValueError as exc:
                    raise AppError(str(exc)) from exc
                print(json.dumps(payload.to_dict(), ensure_ascii=False, indent=2))
                return 0
            if args.command == "watch":
                service.watch_forever(apply_requested=args.apply)
                return 0
            if args.command == "service-run":
                service.run_service_forever(apply_requested=args.apply)
                return 0
            if args.command == "service-tick":
                payload = service.run_service_tick(apply_requested=args.apply)
                print(json.dumps(payload, ensure_ascii=False, indent=2))
                return 0
            plan, extras = service.run_command(
                command=args.command,
                apply_requested=args.apply,
                undo_limit=args.undo_limit,
                delete_target=args.delete_target,
            )
        finally:
            service.close()
    except AppError as exc:
        print(f"오류: {exc}", file=sys.stderr)
        return 1

    if plan is not None:
        summary = plan.summary()
        print(f"command={plan.command}")
        for key, value in sorted(summary.items()):
            print(f"{key}={value}")
        component_summaries = extras.get("component_summaries")
        if isinstance(component_summaries, dict):
            for component, component_summary in component_summaries.items():
                if not isinstance(component_summary, dict):
                    continue
                print(f"component_{component}_total={int(component_summary.get('total', 0))}")
                print(f"component_{component}_pending_actionable={_actionable_count(component_summary)}")
        actionable = _actionable_count(summary)
        print(f"pending_actionable={actionable}")
        print(f"operation_state={_operation_state(summary)}")
        if actionable == 0:
            print("status_hint=No automatic move/rename/archive/quarantine is currently pending.")
        if int(summary.get("flag_for_review", 0)) > 0:
            print("status_hint_review=Manual review items remain. Check status output for review candidates.")
        for action in plan.actions[:20]:
            destination = str(action.destination_path) if action.destination_path else "-"
            print(f"{action.action_type}: {action.source_path} -> {destination} [{action.status}]")
    for key, value in extras.items():
        print(f"{key}={value}")
    return 0


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
