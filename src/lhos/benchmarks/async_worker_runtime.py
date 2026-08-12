"""Offline end-to-end benchmark for ``AgentOS.run_async``.

The benchmark compares the same public SDK workload with global concurrency
set to one and to a bounded worker pool. It exercises Goal compilation,
Scheduler claims and resource admission, Kernel leases, executor overlap,
independent verification, Evidence attachment, and VPG Goal closure.

The resource vector is logical Scheduler accounting. This benchmark does not
measure or enforce physical CPU, RAM, GPU, VRAM, or model-provider usage.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from lhos.runtimes.multi_agent import AttemptState, ClaimState, ResourceVector
from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome

DEFAULT_TASKS = 24
DEFAULT_DELAY_SECONDS = 0.025
DEFAULT_MAX_CONCURRENCY = 4
DEFAULT_AGENT_CONCURRENCY = 2
DEFAULT_AGENT_COUNT = 2
DEFAULT_MIN_SPEEDUP = 1.50

_TASK_RESOURCES = ResourceVector(
    cpu_millis=500,
    ram_bytes=16_000_000,
    gpu_count=1,
    vram_bytes=256_000_000,
    model_slots={"benchmark-model": 1},
)


def _scaled_resources(value: ResourceVector, factor: int) -> ResourceVector:
    return ResourceVector(
        cpu_millis=value.cpu_millis * factor,
        ram_bytes=value.ram_bytes * factor,
        gpu_count=value.gpu_count * factor,
        vram_bytes=value.vram_bytes * factor,
        model_slots={name: count * factor for name, count in value.model_slots.items()},
    )


@dataclass(frozen=True, slots=True)
class _AdmissionProbe:
    ownership_ready: bool
    reservation_ready: bool
    active_reservations: int


class _ControlledIOExecutor:
    def __init__(
        self,
        *,
        delay_seconds: float,
        global_limit: int,
        agent_limits: dict[str, int],
    ) -> None:
        self.delay_seconds = delay_seconds
        self.global_limit = global_limit
        self.agent_limits = dict(agent_limits)
        self.active = 0
        self.peak = 0
        self.active_by_agent: dict[str, int] = {}
        self.peak_by_agent: dict[str, int] = {}
        self.global_capacity_violations = 0
        self.agent_capacity_violations = 0
        self.ownership_admission_violations = 0
        self.resource_admission_violations = 0
        self.peak_active_reservations = 0
        self.completed: list[str] = []
        self._probe: Callable[[str, str], _AdmissionProbe] | None = None

    def bind_probe(self, probe: Callable[[str, str], _AdmissionProbe]) -> None:
        self._probe = probe

    async def execute(self, agent_id: str, task_id: str) -> None:
        if self._probe is None:
            raise RuntimeError("benchmark admission probe is not bound")
        probe = self._probe(agent_id, task_id)
        if not probe.ownership_ready:
            self.ownership_admission_violations += 1
        if not probe.reservation_ready:
            self.resource_admission_violations += 1
        self.peak_active_reservations = max(
            self.peak_active_reservations,
            probe.active_reservations,
        )

        self.active += 1
        self.peak = max(self.peak, self.active)
        if self.active > self.global_limit:
            self.global_capacity_violations += 1

        agent_active = self.active_by_agent.get(agent_id, 0) + 1
        self.active_by_agent[agent_id] = agent_active
        self.peak_by_agent[agent_id] = max(
            agent_active,
            self.peak_by_agent.get(agent_id, 0),
        )
        if agent_active > self.agent_limits[agent_id]:
            self.agent_capacity_violations += 1

        try:
            await asyncio.sleep(self.delay_seconds)
            self.completed.append(task_id)
        finally:
            self.active -= 1
            remaining = self.active_by_agent[agent_id] - 1
            if remaining:
                self.active_by_agent[agent_id] = remaining
            else:
                self.active_by_agent.pop(agent_id, None)


def _validate_inputs(
    *,
    task_count: int,
    delay_seconds: float,
    max_concurrency: int,
    agent_concurrency: int,
    agent_count: int,
) -> None:
    if isinstance(task_count, bool) or not isinstance(task_count, int) or task_count < 1:
        raise ValueError("task_count must be a positive integer")
    if not isinstance(delay_seconds, (int, float)) or delay_seconds <= 0:
        raise ValueError("delay_seconds must be positive")
    for name, value in (
        ("max_concurrency", max_concurrency),
        ("agent_concurrency", agent_concurrency),
        ("agent_count", agent_count),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name} must be a positive integer")


def _verification_outcome(task_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=f"artifact-{task_id}",
        version=1,
        content=f"verified:{task_id}",
        evidence_note="controlled AgentOS.run_async benchmark",
    )


async def _run_agentos(
    *,
    mode: str,
    task_count: int,
    delay_seconds: float,
    run_concurrency: int,
    agent_concurrency: int,
    agent_count: int,
) -> dict[str, Any]:
    agent_ids = [f"agent-{index}" for index in range(agent_count)]
    agent_limits = {agent_id: agent_concurrency for agent_id in agent_ids}
    executor = _ControlledIOExecutor(
        delay_seconds=delay_seconds,
        global_limit=run_concurrency,
        agent_limits=agent_limits,
    )
    runtime = AgentOS(":memory:")
    goal_id = f"benchmark-{mode}"
    task_ids = [f"task-{index:03d}" for index in range(task_count)]
    try:
        for agent_id in agent_ids:

            async def execute(task_id: str, *, _agent_id: str = agent_id) -> None:
                await executor.execute(_agent_id, task_id)

            runtime.add_agent(
                Agent(
                    agent_id,
                    executor=execute,
                    specializations=("benchmark",),
                    max_concurrency=agent_concurrency,
                    resource_capacity=_scaled_resources(
                        _TASK_RESOURCES,
                        agent_concurrency,
                    ),
                )
            )

        goal = Goal(goal_id)
        for index, task_id in enumerate(task_ids):
            goal.task(
                task_id,
                agent=agent_ids[index % agent_count],
                required_specializations=("benchmark",),
                resources=_TASK_RESOURCES,
                verify=lambda _task_id=task_id: _verification_outcome(_task_id),
            )

        def probe(agent_id: str, task_id: str) -> _AdmissionProbe:
            claim = runtime.scheduler.active_claim_for_task(task_id)
            attempt = (
                runtime.scheduler.attempt_for_claim(claim.claim_id) if claim is not None else None
            )
            reservation = (
                runtime.scheduler._s.resource_manager.for_owner(claim.claim_id)
                if claim is not None
                else None
            )
            ownership_ready = bool(
                claim is not None
                and claim.agent_id == agent_id
                and claim.state == ClaimState.ACTIVE
                and claim.lease_id
                and attempt is not None
                and attempt.state == AttemptState.RUNNING
            )
            reservation_ready = bool(
                reservation is not None
                and reservation.pool_id == agent_id
                and reservation.resources == _TASK_RESOURCES
            )
            return _AdmissionProbe(
                ownership_ready=ownership_ready,
                reservation_ready=reservation_ready,
                active_reservations=len(runtime.scheduler._s.resource_manager.list_active()),
            )

        executor.bind_probe(probe)
        started = time.perf_counter()
        result = await runtime.run_async(
            goal,
            max_dispatches=task_count,
            max_steps=task_count,
            max_concurrency=run_concurrency,
        )
        elapsed = time.perf_counter() - started

        claims = runtime.scheduler.claims
        attempts = runtime.scheduler.attempts
        active_reservations_after = runtime.scheduler._s.resource_manager.list_active()
        verified = set(result.verified)
        completed = set(executor.completed)
        correctness = {
            "goal_closed": result.goal_state == "closed",
            "all_tasks_verified": verified == set(task_ids),
            "all_executors_completed": completed == set(task_ids),
            "all_tasks_dispatched_once": (
                len(executor.completed) == task_count
                and len(claims) == task_count
                and len(attempts) == task_count
            ),
            "no_run_failures": result.failures == [],
            "all_claims_completed": all(claim.state == ClaimState.COMPLETED for claim in claims),
            "all_attempts_semantically_verified": all(
                attempt.state == AttemptState.VERIFIED_SEMANTICALLY for attempt in attempts
            ),
            "all_claims_had_full_resource_vector": all(
                claim.resource_reservation_id and claim.reserved_resources == _TASK_RESOURCES
                for claim in claims
            ),
            "no_active_reservations_after_run": not active_reservations_after,
            "no_active_executors_after_run": executor.active == 0,
        }
        return {
            "mode": mode,
            "elapsed_seconds": elapsed,
            "completed_tasks": len(executor.completed),
            "peak_concurrency": executor.peak,
            "peak_by_agent": dict(sorted(executor.peak_by_agent.items())),
            "configured_max_concurrency": run_concurrency,
            "configured_agent_concurrency": dict(sorted(agent_limits.items())),
            "global_capacity_violations": executor.global_capacity_violations,
            "agent_capacity_violations": executor.agent_capacity_violations,
            "capacity_violations": (
                executor.global_capacity_violations + executor.agent_capacity_violations
            ),
            "ownership_admission_violations": executor.ownership_admission_violations,
            "resource_admission_violations": executor.resource_admission_violations,
            "peak_active_resource_reservations": executor.peak_active_reservations,
            "active_resource_reservations_after_run": len(active_reservations_after),
            "claim_count": len(claims),
            "attempt_count": len(attempts),
            "correctness": correctness,
        }
    finally:
        runtime.close()


def _violations(
    report: dict[str, Any],
    *,
    min_speedup: float,
) -> list[str]:
    violations: list[str] = []
    serial = report["baseline"]
    parallel = report["async_runtime"]
    for label, measurement in (("serial baseline", serial), ("async runtime", parallel)):
        if measurement["capacity_violations"] != 0:
            violations.append(f"{label} exceeded configured execution capacity")
        if measurement["ownership_admission_violations"] != 0:
            violations.append(f"{label} executed without a live Claim/Lease/Attempt")
        if measurement["resource_admission_violations"] != 0:
            violations.append(f"{label} executed without its full resource reservation")
        if not all(measurement["correctness"].values()):
            violations.append(f"{label} correctness contract failed")
    if parallel["peak_concurrency"] > parallel["configured_max_concurrency"]:
        violations.append("observed global peak exceeded configured max_concurrency")
    for agent_id, peak in parallel["peak_by_agent"].items():
        if peak > parallel["configured_agent_concurrency"][agent_id]:
            violations.append(f"{agent_id} exceeded configured max_concurrency")
    speedup = report["comparison"]["speedup"]
    if speedup < min_speedup:
        violations.append(
            f"speedup {speedup:.3f}x is below the conservative {min_speedup:.2f}x threshold"
        )
    return violations


async def run_benchmark_async(
    *,
    task_count: int = DEFAULT_TASKS,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    agent_concurrency: int = DEFAULT_AGENT_CONCURRENCY,
    agent_count: int = DEFAULT_AGENT_COUNT,
    min_speedup: float = DEFAULT_MIN_SPEEDUP,
) -> dict[str, Any]:
    """Run the controlled public-SDK benchmark."""

    _validate_inputs(
        task_count=task_count,
        delay_seconds=delay_seconds,
        max_concurrency=max_concurrency,
        agent_concurrency=agent_concurrency,
        agent_count=agent_count,
    )
    if (
        isinstance(min_speedup, bool)
        or not isinstance(min_speedup, (int, float))
        or min_speedup <= 0
    ):
        raise ValueError("min_speedup must be positive")

    serial = await _run_agentos(
        mode="agentos_run_async_serial",
        task_count=task_count,
        delay_seconds=delay_seconds,
        run_concurrency=1,
        agent_concurrency=agent_concurrency,
        agent_count=agent_count,
    )
    parallel = await _run_agentos(
        mode="agentos_run_async_parallel",
        task_count=task_count,
        delay_seconds=delay_seconds,
        run_concurrency=max_concurrency,
        agent_concurrency=agent_concurrency,
        agent_count=agent_count,
    )
    serial_elapsed = float(serial["elapsed_seconds"])
    parallel_elapsed = float(parallel["elapsed_seconds"])
    speedup = serial_elapsed / parallel_elapsed if parallel_elapsed > 0 else float("inf")
    report: dict[str, Any] = {
        "benchmark": "agentos_run_async_end_to_end",
        "benchmark_version": 2,
        "workload": {
            "task_count": task_count,
            "delay_seconds": delay_seconds,
            "agent_count": agent_count,
            "workload_kind": "independent asyncio.sleep I/O-shaped Agent tasks",
            "task_resource_vector": _TASK_RESOURCES.model_dump(mode="json"),
        },
        "baseline": serial,
        "async_runtime": parallel,
        "comparison": {
            "speedup": speedup,
            "parallel_over_serial_ratio": (
                parallel_elapsed / serial_elapsed if serial_elapsed > 0 else 0.0
            ),
            "min_speedup_threshold": min_speedup,
        },
        "scope": {
            "offline": True,
            "external_api_calls": False,
            "public_agentos_run_async": True,
            "scheduler_claims": True,
            "kernel_leases": True,
            "scheduler_resource_admission": True,
            "independent_verification": True,
            "evidence_and_vpg_goal_closure": True,
            "measures": (
                "controlled public-SDK latency and I/O-shaped executor overlap "
                "through semantic Goal closure"
            ),
            "does_not_measure": (
                "physical CPU/GPU/RAM/VRAM utilization, model throughput, "
                "external API latency, or distributed scheduling"
            ),
        },
    }
    violations = _violations(report, min_speedup=min_speedup)
    report["violations"] = violations
    report["valid"] = not violations
    return report


def run_benchmark(**kwargs: Any) -> dict[str, Any]:
    """Synchronous wrapper used by the CLI, tests, and benchmark script."""

    return asyncio.run(run_benchmark_async(**kwargs))


__all__ = [
    "DEFAULT_AGENT_CONCURRENCY",
    "DEFAULT_AGENT_COUNT",
    "DEFAULT_DELAY_SECONDS",
    "DEFAULT_MAX_CONCURRENCY",
    "DEFAULT_MIN_SPEEDUP",
    "DEFAULT_TASKS",
    "run_benchmark",
    "run_benchmark_async",
]
