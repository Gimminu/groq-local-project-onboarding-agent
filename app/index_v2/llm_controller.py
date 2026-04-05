from __future__ import annotations

import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from app.errors import AppError
from app.index_v2.db import IndexDatabase
from app.index_v2.types import IndexOrganizerConfig

CLOUD_PROVIDERS = ("groq", "gemini")
ALL_PROVIDERS = ("groq", "gemini", "ollama")


class LLMProviderError(AppError):
    def __init__(self, message: str, *, error_code: str = "provider_error") -> None:
        super().__init__(message)
        self.error_code = error_code


class LLMRateLimitError(LLMProviderError):
    def __init__(self, message: str, *, error_code: str = "rate_limit") -> None:
        super().__init__(message, error_code=error_code)


class LLMDeferredDecision(AppError):
    def __init__(
        self,
        *,
        reason: str,
        defer_until: str | None,
        provider_attempts: list[dict[str, Any]],
        last_error_code: str | None,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.defer_until = defer_until
        self.provider_attempts = provider_attempts
        self.last_error_code = last_error_code


@dataclass(frozen=True)
class LLMInvocationResult:
    payload: dict[str, Any]
    provider_used: str
    provider_attempts: tuple[dict[str, Any], ...]
    cloud_provider_used: bool


class LLMProviderController:
    def __init__(
        self,
        config: IndexOrganizerConfig,
        database: IndexDatabase,
        *,
        groq_call: Callable[[], dict[str, Any]],
        gemini_call: Callable[[], dict[str, Any]],
        ollama_call: Callable[[], dict[str, Any]],
        ollama_healthcheck: Callable[[], bool],
        sleep_fn: Callable[[float], None] = time.sleep,
        jitter_fn: Callable[[float, float], float] | None = None,
    ) -> None:
        self.config = config
        self.database = database
        self._groq_call = groq_call
        self._gemini_call = gemini_call
        self._ollama_call = ollama_call
        self._ollama_healthcheck = ollama_healthcheck
        self._sleep = sleep_fn
        self._jitter = jitter_fn or random.uniform

    def invoke(
        self,
        *,
        preferred_provider: str,
        allow_cloud: bool,
    ) -> LLMInvocationResult:
        attempts: list[dict[str, Any]] = []
        providers = self._provider_order(preferred_provider=preferred_provider, allow_cloud=allow_cloud)
        last_error_code: str | None = None
        soonest_defer_until: datetime | None = None

        for provider in providers:
            if not self._provider_configured(provider):
                attempts.append({"provider": provider, "status": "unavailable", "error_code": "not_configured"})
                continue

            cooldown_until = self._cooldown_until(provider)
            if cooldown_until is not None and cooldown_until > _utc_now_dt():
                attempts.append(
                    {
                        "provider": provider,
                        "status": "cooldown",
                        "error_code": "cooldown_active",
                        "cooldown_until": cooldown_until.isoformat(),
                    }
                )
                soonest_defer_until = _min_dt(soonest_defer_until, cooldown_until)
                continue

            self._respect_min_interval(provider)
            attempt_limit = max(1, int(self.config.llm.retry_attempts) + 1)
            for attempt_index in range(attempt_limit):
                try:
                    print(f"🧠 [LLM Request] Attempting classification with provider: '{provider}' (attempt {attempt_index + 1}/{attempt_limit})...")
                    payload = self._call_provider(provider)
                except LLMRateLimitError as exc:
                    last_error_code = exc.error_code
                    attempts.append(
                        {
                            "provider": provider,
                            "status": "rate_limited",
                            "error_code": exc.error_code,
                            "attempt": attempt_index + 1,
                        }
                    )
                    self._mark_provider_attempt(provider, error_code=exc.error_code, rate_limited=True, cooldown_until=None)
                    print(f"⚠️ [LLM Warning] '{provider}' is rate limited (429/quota resource exhausted).")
                    if attempt_index + 1 < attempt_limit:
                        self._sleep(self._retry_delay(attempt_index))
                        continue
                    cooldown = _utc_now_dt() + timedelta(seconds=max(0, int(self.config.llm.provider_cooldown_seconds)))
                    self._mark_provider_attempt(
                        provider,
                        error_code=exc.error_code,
                        rate_limited=True,
                        cooldown_until=cooldown.isoformat(),
                    )
                    soonest_defer_until = _min_dt(soonest_defer_until, cooldown)
                    break
                except LLMProviderError as exc:
                    last_error_code = exc.error_code
                    attempts.append(
                        {
                            "provider": provider,
                            "status": "failed",
                            "error_code": exc.error_code,
                            "attempt": attempt_index + 1,
                        }
                    )
                    self._mark_provider_attempt(provider, error_code=exc.error_code, rate_limited=False, cooldown_until=None)
                    break
                except Exception as exc:  # pragma: no cover - defensive
                    error_code = _guess_error_code(str(exc))
                    last_error_code = error_code
                    attempts.append(
                        {
                            "provider": provider,
                            "status": "failed",
                            "error_code": error_code,
                            "attempt": attempt_index + 1,
                        }
                    )
                    self._mark_provider_attempt(provider, error_code=error_code, rate_limited=False, cooldown_until=None)
                    break

                attempts.append({"provider": provider, "status": "ok", "attempt": attempt_index + 1})
                self._mark_provider_attempt(provider, error_code=None, rate_limited=False, cooldown_until=None)
                print(f"✅ [LLM Success] Successfully generated classification using '{provider}'.")
                return LLMInvocationResult(
                    payload=payload,
                    provider_used=provider,
                    provider_attempts=tuple(attempts),
                    cloud_provider_used=provider in CLOUD_PROVIDERS,
                )

        if soonest_defer_until is None:
            soonest_defer_until = _utc_now_dt() + timedelta(seconds=60)
        raise LLMDeferredDecision(
            reason="all configured llm providers are unavailable, cooling down, or rate limited",
            defer_until=soonest_defer_until.isoformat(),
            provider_attempts=attempts,
            last_error_code=last_error_code,
        )

    def _provider_order(self, *, preferred_provider: str, allow_cloud: bool) -> list[str]:
        preferred = (preferred_provider or self.config.llm.preferred_provider or "groq").strip().lower()
        ordered: list[str] = []

        def add(provider: str) -> None:
            if provider not in ALL_PROVIDERS or provider in ordered:
                return
            if provider in CLOUD_PROVIDERS and not allow_cloud:
                return
            ordered.append(provider)

        add(preferred)
        if self.config.llm.fallback_to_other_cloud:
            for provider in CLOUD_PROVIDERS:
                add(provider)
        if self.config.llm.fallback_to_ollama:
            add("ollama")
        if not ordered and self.config.llm.fallback_to_ollama:
            ordered.append("ollama")
        return ordered

    def _provider_configured(self, provider: str) -> bool:
        if provider == "groq":
            return self.config.groq.enabled and bool(os.getenv("GROQ_API_KEY"))
        if provider == "gemini":
            return bool(os.getenv("GEMINI_API_KEY"))
        if provider == "ollama":
            return self._ollama_healthcheck()
        return False

    def _cooldown_until(self, provider: str) -> datetime | None:
        row = self.database.get_provider_state(provider)
        if row is None:
            return None
        return _parse_iso(row["cooldown_until"])

    def _respect_min_interval(self, provider: str) -> None:
        gap = int(self.config.llm.min_request_interval_seconds.get(provider, 0))
        if gap <= 0:
            return
        row = self.database.get_provider_state(provider)
        if row is None:
            return
        updated_at = _parse_iso(row["updated_at"])
        if updated_at is None:
            return
        remaining = gap - (_utc_now_dt() - updated_at).total_seconds()
        if remaining > 0:
            self._sleep(remaining)

    def _retry_delay(self, attempt_index: int) -> float:
        if not self.config.llm.backoff_seconds:
            return 0.0
        base = self.config.llm.backoff_seconds[min(attempt_index, len(self.config.llm.backoff_seconds) - 1)]
        return float(base) + float(self._jitter(0.0, 1.0))

    def _call_provider(self, provider: str) -> dict[str, Any]:
        if provider == "groq":
            return self._groq_call()
        if provider == "gemini":
            return self._gemini_call()
        if provider == "ollama":
            return self._ollama_call()
        raise LLMProviderError(f"unsupported provider: {provider}", error_code="unsupported_provider")

    def _mark_provider_attempt(
        self,
        provider: str,
        *,
        error_code: str | None,
        rate_limited: bool,
        cooldown_until: str | None,
    ) -> None:
        row = self.database.get_provider_state(provider)
        previous = int(row["consecutive_rate_limits"]) if row is not None else 0
        self.database.upsert_provider_state(
            provider=provider,
            cooldown_until=cooldown_until,
            last_error_code=error_code,
            consecutive_rate_limits=(previous + 1) if rate_limited else 0,
            updated_at=_utc_now_dt().isoformat(),
        )


def _guess_error_code(message: str) -> str:
    normalized = message.lower()
    if any(token in normalized for token in ("429", "rate limit", "rate_limit", "too many requests")):
        return "rate_limit"
    if "quota" in normalized or "resource_exhausted" in normalized:
        return "quota"
    return "provider_error"


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _min_dt(current: datetime | None, candidate: datetime | None) -> datetime | None:
    if candidate is None:
        return current
    if current is None:
        return candidate
    return candidate if candidate < current else current
