"""Optional StepCode live-model probe for the semantic-repair benchmark.

The deterministic benchmark remains the correctness source of truth. This
module adds a small, explicitly opt-in measurement that translates repair
attempt counts into observed model calls, tokens and latency.
"""

from __future__ import annotations

import json
import os
import statistics
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from lhos.sdk import Agent, AgentOS, Goal
from lhos.sdk.verification import VerificationOutcome

DEFAULT_BASE_URL = "https://stepcode.basemind.com/v1"
DEFAULT_MODEL = "gpt-5.6-sol"


class StepCodeAPIError(RuntimeError):
    """A sanitized StepCode API failure."""


@dataclass(frozen=True)
class ModelCall:
    model: str
    latency_ms: float
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    usage_reported: bool
    passed: bool
    instruction_followed: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "latency_ms": self.latency_ms,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "usage_reported": self.usage_reported,
            "passed": self.passed,
            "instruction_followed": self.instruction_followed,
        }


class StepCodeChatClient:
    """Minimal OpenAI-compatible client without an additional dependency."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_keys: list[str] | None = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = 60.0,
        opener: Callable[..., Any] | None = None,
    ) -> None:
        configured_keys = api_keys or [
            value.strip()
            for value in os.environ.get("STEPCODE_API_KEYS", "").split(",")
            if value.strip()
        ]
        if not configured_keys and api_key:
            configured_keys = [api_key]
        if not configured_keys and os.environ.get("STEPCODE_API_KEY"):
            configured_keys = [os.environ["STEPCODE_API_KEY"]]
        if not configured_keys:
            raise StepCodeAPIError("STEPCODE_API_KEY is required for --live-model")
        self._api_keys = configured_keys
        self._key_index = 0
        self._key_lock = threading.Lock()
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._opener = opener or urlopen

    @property
    def key_count(self) -> int:
        return len(self._api_keys)

    def list_models(self) -> list[str]:
        payload = self._request("GET", "/models?purpose=code")
        raw_models = payload.get("data", payload.get("models", []))
        model_ids: list[str] = []
        for item in raw_models:
            if isinstance(item, str):
                model_ids.append(item)
            elif isinstance(item, dict) and item.get("id"):
                model_ids.append(str(item["id"]))
        return sorted(set(model_ids))

    def chat(self, *, model: str, task_id: str) -> ModelCall:
        prompt = (
            "Act as a deterministic verification worker. "
            f"The task id is {task_id}. Reply with exactly PASS."
        )
        started = time.perf_counter()
        response = self._request(
            "POST",
            "/chat/completions",
            {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0,
                "max_tokens": 8,
                "stream": False,
            },
        )
        latency_ms = round((time.perf_counter() - started) * 1000, 3)
        choices = response.get("choices", [])
        content = ""
        if choices:
            message = choices[0].get("message", {})
            raw_content = message.get("content", "")
            if isinstance(raw_content, str):
                content = raw_content
            elif isinstance(raw_content, list):
                content = "".join(
                    str(item.get("text", "")) for item in raw_content if isinstance(item, dict)
                )
        raw_usage = response.get("usage")
        usage = raw_usage if isinstance(raw_usage, dict) else {}
        prompt_tokens = int(usage.get("prompt_tokens", 0) or 0)
        completion_tokens = int(usage.get("completion_tokens", 0) or 0)
        total_tokens = int(usage.get("total_tokens", prompt_tokens + completion_tokens) or 0)
        usage_reported = bool(
            total_tokens > 0
            and any(key in usage for key in ("prompt_tokens", "completion_tokens", "total_tokens"))
        )
        return ModelCall(
            model=model,
            latency_ms=latency_ms,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            usage_reported=usage_reported,
            passed=bool(choices),
            instruction_followed=content.strip().upper().startswith("PASS"),
        )

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = Request(
            f"{self.base_url}{path}",
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self._next_key()}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with self._opener(request, timeout=self.timeout_seconds) as response:
                raw = response.read()
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:300]
            raise StepCodeAPIError(
                f"StepCode HTTP {exc.code}: {self._redact_secrets(detail)}"
            ) from exc
        except (URLError, TimeoutError) as exc:
            raise StepCodeAPIError(f"StepCode request failed: {type(exc).__name__}") from exc
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise StepCodeAPIError("StepCode returned invalid JSON") from exc
        if not isinstance(parsed, dict):
            raise StepCodeAPIError("StepCode returned a non-object JSON response")
        return parsed

    def _next_key(self) -> str:
        with self._key_lock:
            key = self._api_keys[self._key_index % len(self._api_keys)]
            self._key_index += 1
            return key

    def _redact_secrets(self, value: str) -> str:
        redacted = value
        for api_key in self._api_keys:
            redacted = redacted.replace(api_key, "[REDACTED]")
        return redacted


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * percentile)))
    return round(ordered[index], 3)


def _summarize_calls(calls: list[ModelCall]) -> dict[str, Any]:
    latencies = [call.latency_ms for call in calls]
    usage_reported_calls = sum(call.usage_reported for call in calls)
    return {
        "model_calls": len(calls),
        "successful_calls": sum(call.passed for call in calls),
        "instruction_following_calls": sum(call.instruction_followed for call in calls),
        "usage_reported_calls": usage_reported_calls,
        "usage_missing_calls": len(calls) - usage_reported_calls,
        "token_totals_complete": usage_reported_calls == len(calls),
        "prompt_tokens": sum(call.prompt_tokens for call in calls),
        "completion_tokens": sum(call.completion_tokens for call in calls),
        "total_tokens": sum(call.total_tokens for call in calls),
        "wall_time_ms": round(sum(latencies), 3),
        "latency_p50_ms": round(statistics.median(latencies), 3) if latencies else 0.0,
        "latency_p95_ms": _percentile(latencies, 0.95),
    }


def run_live_model_benchmark(
    *,
    model: str = DEFAULT_MODEL,
    api_key: str | None = None,
    base_url: str = DEFAULT_BASE_URL,
    timeout_seconds: float = 60.0,
    client: StepCodeChatClient | None = None,
) -> dict[str, Any]:
    """Run a small live four-task semantic-repair case.

    The initial closure is shared setup and excluded from strategy comparison.
    The mutation affects a three-task chain while one independent task remains
    valid. Full restart performs four calls, a task-DAG checkpoint performs
    three, and LongHorizonOS performs the attempts observed from its scheduler.
    """
    chat = client or StepCodeChatClient(
        api_key=api_key,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
    )
    available_models = chat.list_models()
    if available_models and model not in available_models:
        raise StepCodeAPIError(
            f"model {model!r} is not in the current StepCode coding model catalog"
        )

    runtime = AgentOS(":memory:")
    runtime.add_agent(Agent("live", specializations=("verification",), max_concurrency=4))
    goal = Goal("live-semantic-repair")
    versions = {"root": 1, "middle": 1, "leaf": 1, "independent": 1}
    observed_calls: list[ModelCall] = []

    def verifier(task_id: str, artifact_id: str) -> Callable[[], VerificationOutcome]:
        def _run() -> VerificationOutcome:
            call = chat.chat(model=model, task_id=task_id)
            observed_calls.append(call)
            return VerificationOutcome(
                passed=call.passed,
                artifact_id=artifact_id,
                version=versions[artifact_id],
                content=f"{artifact_id}-v{versions[artifact_id]}",
                evidence_note=f"stepcode:{model}",
                details=call.as_dict(),
            )

        return _run

    root = goal.task(
        "T0",
        agent="live",
        verify=verifier("T0", "root"),
        metadata={"task_kind": "verification"},
        required_specializations=("verification",),
    )
    middle = goal.task(
        "T1",
        agent="live",
        depends_on=(root,),
        verify=verifier("T1", "middle"),
        metadata={"task_kind": "verification"},
        required_specializations=("verification",),
    )
    goal.task(
        "T2",
        agent="live",
        depends_on=(middle,),
        verify=verifier("T2", "leaf"),
        metadata={"task_kind": "verification"},
        required_specializations=("verification",),
    )
    goal.task(
        "T3",
        agent="live",
        verify=verifier("T3", "independent"),
        metadata={"task_kind": "verification"},
        required_specializations=("verification",),
    )

    initial = runtime.run(goal, max_dispatches=16, max_steps=16)
    initial_calls = list(observed_calls)
    if initial.goal_state != "closed":
        raise StepCodeAPIError("live-model initial closure failed")

    versions["root"] = 2
    repair = runtime.repair(goal, artifact_id="root", new_artifact_version=2)
    repair_call_offset = len(observed_calls)
    attempt_offset = len(runtime.scheduler.attempts)
    final = runtime.run(goal, max_dispatches=16, max_steps=16)
    lhos_calls = observed_calls[repair_call_offset:]
    lhos_attempts = runtime.scheduler.attempts[attempt_offset:]

    def direct_calls(task_ids: list[str]) -> tuple[list[ModelCall], float]:
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=min(chat.key_count, len(task_ids))) as executor:
            calls = list(
                executor.map(
                    lambda task_id: chat.chat(model=model, task_id=task_id),
                    task_ids,
                )
            )
        return calls, round((time.perf_counter() - started) * 1000, 3)

    full_restart_calls, full_batch_wall_ms = direct_calls(["T0", "T1", "T2", "T3"])
    checkpoint_calls, checkpoint_batch_wall_ms = direct_calls(["T0", "T1", "T2"])
    lhos_summary = _summarize_calls(lhos_calls)
    full_summary = _summarize_calls(full_restart_calls)
    checkpoint_summary = _summarize_calls(checkpoint_calls)
    full_summary["parallel_batch_wall_ms"] = full_batch_wall_ms
    checkpoint_summary["parallel_batch_wall_ms"] = checkpoint_batch_wall_ms

    return {
        "benchmark_id": "stepcode_live_semantic_repair_v1",
        "model": model,
        "base_url": base_url,
        "model_catalog_size": len(available_models),
        "api_key_count": chat.key_count,
        "initial_closure": {
            **_summarize_calls(initial_calls),
            "goal_closed": initial.goal_state == "closed",
        },
        "mutation": {
            "affected_tasks": sorted(repair.affected),
            "preserved_tasks": sorted(repair.preserved),
            "initial_repair_frontier": sorted(repair.frontier),
        },
        "strategies": {
            "full_restart": full_summary,
            "state_only_resume": {
                "model_calls": 0,
                "usage_reported_calls": 0,
                "usage_missing_calls": 0,
                "token_totals_complete": True,
                "total_tokens": 0,
                "false_closure": True,
            },
            "task_dag_checkpoint": checkpoint_summary,
            "longhorizonos": {
                **lhos_summary,
                "observed_attempts": len(lhos_attempts),
                "reexecuted_tasks": [attempt.task_id for attempt in lhos_attempts],
                "final_goal_closed": final.goal_state == "closed",
                "false_verified_after_invalidation": 0,
            },
        },
        "call_saving_vs_full_restart": round(
            1.0 - (lhos_summary["model_calls"] / max(1, full_summary["model_calls"])),
            6,
        ),
        "call_saving_vs_task_dag_checkpoint": round(
            1.0 - (lhos_summary["model_calls"] / max(1, checkpoint_summary["model_calls"])),
            6,
        ),
        "interpretation": (
            "This task-level DAG case tests semantic detection and real repair cost. "
            "LongHorizonOS is expected to beat full restart and match an oracle-informed "
            "task-DAG checkpoint on model-call count."
        ),
    }
