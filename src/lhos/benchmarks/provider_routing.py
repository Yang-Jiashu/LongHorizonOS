"""Deterministic benchmark for the bounded provider-routing adapter.

The benchmark deliberately compares two *public* ``AgentOS.run_async``
executions of the same four-task workload:

``callback``
    The normal ``Agent.executor``/``Task.verify`` callback path with
    ``adaptive=False``.

``provider_route``
    ``adaptive=True`` plus explicit
    ``Task.metadata["compute_routing"]["provider_routing"]``.  The model and
    verifier are deterministic in-process providers registered through
    :class:`~lhos.sdk.ComputeProviderRegistry`.

No network, model SDK, GPU, or wall-clock service is involved.  The fake
providers expose counters and a fixed token/cost profile so the benchmark can
assert that routing really occurred while keeping the result reproducible.
Wall-clock values are reported for orientation only; correctness and token/
call counts are the stable comparison dimensions.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from functools import partial
from typing import Any

from lhos.sdk import (
    Agent,
    AgentOS,
    ComputeProviderRegistry,
    ConflictGraph,
    Goal,
    TaskAccessSet,
    VerificationOutcome,
)

DEFAULT_TASK_COUNT = 4
DEFAULT_DELAY_SECONDS = 0.002
DEFAULT_MAX_CONCURRENCY = 2
DEFAULT_MAX_DISPATCHES = DEFAULT_TASK_COUNT
DEFAULT_MAX_STEPS = 8
BENCHMARK_VERSION = 1


@dataclass(frozen=True, slots=True)
class _TokenProfile:
    input_tokens: int
    output_tokens: int
    verification_tokens: int


CALLBACK_PROFILE = _TokenProfile(
    input_tokens=96,
    output_tokens=48,
    verification_tokens=24,
)

# The routed profile represents an explicitly selected low-cost provider.  It
# is not a claim about any real model; it exists to make the adapter's metrics
# observable without network/API variability.
ROUTED_PROFILE = _TokenProfile(
    input_tokens=64,
    output_tokens=32,
    verification_tokens=16,
)


@dataclass
class _Counters:
    model_calls: int = 0
    verifier_calls: int = 0
    callback_model_calls: int = 0
    callback_verifier_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    verification_tokens: int = 0
    provider_model_keys: list[str] = field(default_factory=list)
    provider_verifier_keys: list[str] = field(default_factory=list)

    @property
    def provider_calls(self) -> int:
        return self.model_calls + self.verifier_calls

    @property
    def callback_calls(self) -> int:
        return self.callback_model_calls + self.callback_verifier_calls

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens + self.verification_tokens

    def as_dict(self) -> dict[str, Any]:
        return {
            "model_calls": self.model_calls,
            "verifier_calls": self.verifier_calls,
            "provider_calls": self.provider_calls,
            "callback_model_calls": self.callback_model_calls,
            "callback_verifier_calls": self.callback_verifier_calls,
            "callback_calls": self.callback_calls,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "verification_tokens": self.verification_tokens,
            "total_tokens": self.total_tokens,
            "provider_model_keys": list(self.provider_model_keys),
            "provider_verifier_keys": list(self.provider_verifier_keys),
        }


class _CallbackWorkload:
    def __init__(self, counters: _Counters, delay_seconds: float) -> None:
        self._counters = counters
        self._delay_seconds = delay_seconds

    async def execute(self, task_id: str) -> VerificationOutcome:
        self._counters.callback_model_calls += 1
        self._counters.input_tokens += CALLBACK_PROFILE.input_tokens
        self._counters.output_tokens += CALLBACK_PROFILE.output_tokens
        await asyncio.sleep(self._delay_seconds)
        return VerificationOutcome(
            passed=True,
            artifact_id=f"provider-routing-{task_id}",
            version=1,
            content=f"{task_id}:callback",
            details={
                "input_tokens": CALLBACK_PROFILE.input_tokens,
                "output_tokens": CALLBACK_PROFILE.output_tokens,
                "execution_mode": "callback",
            },
        )

    async def verify(self, task_id: str) -> VerificationOutcome:
        self._counters.callback_verifier_calls += 1
        self._counters.verification_tokens += CALLBACK_PROFILE.verification_tokens
        await asyncio.sleep(self._delay_seconds)
        return VerificationOutcome(
            passed=True,
            artifact_id=f"provider-routing-{task_id}",
            version=1,
            content=f"{task_id}:callback-verified",
            details={
                "verification_tokens": CALLBACK_PROFILE.verification_tokens,
                "execution_mode": "callback",
            },
        )


class _RoutedModelProvider:
    def __init__(self, counters: _Counters, delay_seconds: float) -> None:
        self._counters = counters
        self._delay_seconds = delay_seconds

    async def execute(
        self,
        task_id: str,
        _context: Any,
        _base_executor: Any,
    ) -> VerificationOutcome:
        self._counters.model_calls += 1
        self._counters.provider_model_keys.append("cheap")
        self._counters.input_tokens += ROUTED_PROFILE.input_tokens
        self._counters.output_tokens += ROUTED_PROFILE.output_tokens
        await asyncio.sleep(self._delay_seconds)
        return VerificationOutcome(
            passed=True,
            artifact_id=f"provider-routing-{task_id}",
            version=1,
            content=f"{task_id}:provider",
            details={
                "input_tokens": ROUTED_PROFILE.input_tokens,
                "output_tokens": ROUTED_PROFILE.output_tokens,
                "execution_mode": "provider_route",
            },
        )


class _RoutedVerifierProvider:
    def __init__(self, counters: _Counters, delay_seconds: float) -> None:
        self._counters = counters
        self._delay_seconds = delay_seconds

    async def verify(
        self,
        task_id: str,
        _context: Any,
        executor_outcome: Any,
        _base_verifier: Any,
    ) -> VerificationOutcome:
        self._counters.verifier_calls += 1
        self._counters.provider_verifier_keys.append("light")
        self._counters.verification_tokens += ROUTED_PROFILE.verification_tokens
        await asyncio.sleep(self._delay_seconds)
        if not isinstance(executor_outcome, VerificationOutcome):
            raise TypeError("routed benchmark received an invalid model outcome")
        # Preserve the model artifact while adding a verifier marker.  The
        # normal SDK evidence guardian still validates/commits this outcome.
        return VerificationOutcome(
            passed=executor_outcome.passed,
            artifact_id=executor_outcome.artifact_id,
            version=executor_outcome.version,
            content=executor_outcome.content,
            details={
                **executor_outcome.details,
                "verification_tokens": ROUTED_PROFILE.verification_tokens,
                "execution_mode": "provider_route",
            },
        )


def _provider_metadata() -> dict[str, Any]:
    return {
        "compute_routing": {
            "criticality": 0,
            "downstream_fanout": 0,
            "failure_blast_radius": 0,
            "input_stability": 1.0,
            "provider_routing": {
                "enabled": True,
                "model": "cheap",
                "verifier": "light",
            },
        }
    }


def _make_goal(
    *,
    goal_id: str,
    task_count: int,
    executor: Any,
    verifier_factory: Any,
    metadata: dict[str, Any] | None = None,
) -> Goal:
    goal = Goal(goal_id)
    for index in range(task_count):
        task_id = f"task-{index + 1}"
        goal.task(
            task_id,
            agent="benchmark-worker",
            required_specializations=("benchmark",),
            inputs=(f"workspace://provider-routing/{task_id}",),
            outputs=(f"workspace://provider-routing/{task_id}",),
            metadata={} if metadata is None else dict(metadata),
            verify=verifier_factory(task_id),
        )
    # ``executor`` is attached to the Agent rather than Task; retaining the
    # parameter here documents that both modes use the same task graph.
    del executor
    return goal


def _independent_conflict_graph(task_count: int) -> ConflictGraph:
    """Declare disjoint logical resources so adaptive can batch safely."""

    return ConflictGraph.from_access_sets(
        [
            TaskAccessSet(
                task_id=f"task-{index + 1}",
                read_set=(f"workspace://provider-routing/task-{index + 1}",),
                write_set=(f"workspace://provider-routing/task-{index + 1}",),
            )
            for index in range(task_count)
        ]
    )


async def _run_case(
    *,
    mode: str,
    task_count: int,
    delay_seconds: float,
    max_concurrency: int,
) -> dict[str, Any]:
    counters = _Counters()
    runtime: AgentOS | None = None
    try:
        if mode == "callback":
            workload = _CallbackWorkload(counters, delay_seconds)
            registry = None
            executor = workload.execute

            def verifier_factory(task_id: str) -> Any:
                return partial(workload.verify, task_id)

            metadata: dict[str, Any] = {}
            adaptive = False
        elif mode == "provider_route":
            model = _RoutedModelProvider(counters, delay_seconds)
            verifier = _RoutedVerifierProvider(counters, delay_seconds)
            registry = (
                ComputeProviderRegistry()
                .register_model("cheap", model)
                .register_verifier("light", verifier)
            )
            # The base callback is intentionally instrumented too: a routed
            # attempt must not invoke it.
            base = _CallbackWorkload(counters, delay_seconds)
            executor = base.execute

            def verifier_factory(task_id: str) -> Any:
                return partial(base.verify, task_id)

            metadata = _provider_metadata()
            adaptive = True
        else:
            raise ValueError(f"unknown benchmark mode: {mode!r}")

        runtime = AgentOS(":memory:", provider_registry=registry)
        runtime.add_agent(
            Agent(
                "benchmark-worker",
                executor=executor,
                specializations=("benchmark",),
                max_concurrency=max_concurrency,
            )
        )
        goal = _make_goal(
            goal_id=f"provider-routing-{mode}",
            task_count=task_count,
            executor=executor,
            verifier_factory=verifier_factory,
            metadata=metadata,
        )

        started = time.perf_counter()
        result = await runtime.run_async(
            goal,
            max_dispatches=task_count,
            max_steps=DEFAULT_MAX_STEPS,
            max_concurrency=max_concurrency,
            adaptive=adaptive,
            conflict_graph=(_independent_conflict_graph(task_count) if adaptive else None),
            persist_adaptive_epochs=False,
        )
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        metrics = counters.as_dict()
        verified_progress = len(result.verified) / task_count if task_count else 1.0
        correctness = {
            "goal_closed": result.goal_state == "closed",
            "all_tasks_verified": len(result.verified) == task_count,
            "provider_route_invoked": (
                mode != "provider_route"
                or (counters.model_calls == task_count and counters.verifier_calls == task_count)
            ),
            "base_callbacks_not_used_by_route": (
                mode != "provider_route" or counters.callback_calls == 0
            ),
        }
        return {
            "mode": mode,
            "elapsed_ms": round(elapsed_ms, 3),
            "task_count": task_count,
            "verified_progress": round(verified_progress, 6),
            "goal_state": result.goal_state,
            "verified": list(result.verified),
            "failures": list(result.failures),
            "adaptive_epochs": (len(result.meta.get("adaptive_epochs", ())) if adaptive else 0),
            **metrics,
            "correctness": correctness,
        }
    finally:
        if runtime is not None:
            runtime.close()


async def run_provider_routing_benchmark_async(
    *,
    task_count: int = DEFAULT_TASK_COUNT,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
) -> dict[str, Any]:
    """Run the deterministic callback-vs-provider comparison."""

    if isinstance(task_count, bool) or not isinstance(task_count, int) or task_count < 1:
        raise ValueError("task_count must be a positive integer")
    if isinstance(delay_seconds, bool) or not isinstance(delay_seconds, (int, float)):
        raise ValueError("delay_seconds must be a positive number")
    if delay_seconds <= 0:
        raise ValueError("delay_seconds must be positive")
    if (
        isinstance(max_concurrency, bool)
        or not isinstance(max_concurrency, int)
        or max_concurrency < 1
    ):
        raise ValueError("max_concurrency must be a positive integer")

    callback = await _run_case(
        mode="callback",
        task_count=task_count,
        delay_seconds=float(delay_seconds),
        max_concurrency=max_concurrency,
    )
    routed = await _run_case(
        mode="provider_route",
        task_count=task_count,
        delay_seconds=float(delay_seconds),
        max_concurrency=max_concurrency,
    )
    report: dict[str, Any] = {
        "benchmark": "provider_routing",
        "benchmark_version": BENCHMARK_VERSION,
        "workload": {
            "task_count": task_count,
            "delay_seconds": float(delay_seconds),
            "max_concurrency": max_concurrency,
            "offline": True,
            "provider_registry": {
                "model": "cheap",
                "verifier": "light",
            },
        },
        "callback": callback,
        "provider_route": routed,
        "comparison": {
            "verified_progress_delta": (
                routed["verified_progress"] - callback["verified_progress"]
            ),
            "token_reduction": callback["total_tokens"] - routed["total_tokens"],
            "provider_calls": routed["provider_calls"],
            "callback_calls": callback["callback_calls"],
            "wall_time_ratio": (
                routed["elapsed_ms"] / callback["elapsed_ms"] if callback["elapsed_ms"] > 0 else 0.0
            ),
        },
        "scope": {
            "measures": (
                "bounded adapter invocation, deterministic token counters, "
                "provider/callback calls, verified progress, and orientation "
                "wall time"
            ),
            "does_not_measure": (
                "real model quality, network/provider economics, physical "
                "CPU/GPU/RAM/VRAM scheduling, or hidden provenance discovery"
            ),
        },
    }
    violations = []
    for label in ("callback", "provider_route"):
        if not all(report[label]["correctness"].values()):
            violations.append(f"{label} correctness contract failed")
    if callback["verified_progress"] != 1.0 or routed["verified_progress"] != 1.0:
        violations.append("benchmark did not reach a fully verified goal")
    if routed["provider_calls"] != task_count * 2:
        violations.append("provider route did not invoke one model and verifier per task")
    if routed["callback_calls"] != 0:
        violations.append("provider route unexpectedly invoked base callbacks")
    report["violations"] = violations
    report["valid"] = not violations
    return report


def run_provider_routing_benchmark(**kwargs: Any) -> dict[str, Any]:
    """Synchronous wrapper for scripts and notebooks."""

    return asyncio.run(run_provider_routing_benchmark_async(**kwargs))


__all__ = [
    "DEFAULT_DELAY_SECONDS",
    "DEFAULT_MAX_CONCURRENCY",
    "DEFAULT_TASK_COUNT",
    "run_provider_routing_benchmark",
    "run_provider_routing_benchmark_async",
]
