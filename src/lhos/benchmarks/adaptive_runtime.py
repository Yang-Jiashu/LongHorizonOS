"""Controlled benchmark for bounded online adaptive scheduling.

This benchmark compares two executions of the same small workload:

* ``static``: the public ``AgentOS.run_async`` path with a fixed
  ``max_concurrency`` and no conflict policy;
* ``adaptive``: ``adaptive=True`` with an explicit ``ConflictGraph``.

Two tasks intentionally write the same logical workspace resource.  When the
static baseline overlaps those writers, one controlled attempt is marked
stale and must be retried.  The adaptive policy should serialize the
conflicting pair while still running independent tasks in parallel.

The workload is deterministic/offline and uses only ``asyncio.sleep`` and an
in-memory counter.  It is a systems regression benchmark, not a model,
provider, GPU, or production-throughput benchmark.
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter, defaultdict, deque
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

from lhos.runtimes.multi_agent import AttemptState, ClaimState
from lhos.runtimes.verified_progress.models import EvidenceNode, EvidenceResult
from lhos.sdk import (
    Agent,
    AgentOS,
    ConflictGraph,
    Goal,
    TaskAccessSet,
    VerificationOutcome,
)

DEFAULT_DELAY_SECONDS = 0.01
DEFAULT_MAX_CONCURRENCY = 2
DEFAULT_MAX_DISPATCHES = 8
DEFAULT_MAX_STEPS = 8

TASK_RESOURCES: dict[str, str] = {
    "a-conflict": "shared",
    "b-conflict": "shared",
    "c-independent": "c",
    "d-independent": "d",
}
TASK_IDS: tuple[str, ...] = tuple(TASK_RESOURCES)


@dataclass(frozen=True, slots=True)
class _AttemptRecord:
    task_id: str
    attempt_number: int
    resource: str
    passed: bool
    conflict: bool


@dataclass
class _ConflictWorkload:
    """Deterministic in-process workload instrumented for benchmark metrics."""

    delay_seconds: float
    attempts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    pending_verifier_results: dict[str, deque[bool]] = field(
        default_factory=lambda: defaultdict(deque)
    )
    active_by_resource: dict[str, dict[tuple[str, int], str]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    failed_attempts: set[tuple[str, int]] = field(default_factory=set)
    records: list[_AttemptRecord] = field(default_factory=list)
    peak_parallelism: int = 0
    conflict_overlap_events: int = 0
    _shared_arrivals: set[str] = field(default_factory=set)
    _shared_arrival_event: asyncio.Event | None = None
    _active_count: int = 0

    def _event(self) -> asyncio.Event:
        if self._shared_arrival_event is None:
            self._shared_arrival_event = asyncio.Event()
        return self._shared_arrival_event

    async def execute(self, task_id: str) -> None:
        resource = TASK_RESOURCES[task_id]
        attempt_number = self.attempts[task_id] + 1
        self.attempts[task_id] = attempt_number
        identity = (task_id, attempt_number)

        active = self.active_by_resource[resource]
        conflict = bool(active)
        if conflict:
            self.conflict_overlap_events += 1
            # Keep the lexical first writer as the deterministic winner.
            loser = max((task_id, *active.values()))
            if loser != min((task_id, *active.values())):
                self.failed_attempts.add((loser, 1))
            # The active map stores task ids; mark the concrete attempt for a
            # previously registered loser as well.
            for active_identity, active_task in active.items():
                if active_task == loser:
                    self.failed_attempts.add(active_identity)
        active[identity] = task_id
        self._active_count += 1
        self.peak_parallelism = max(self.peak_parallelism, self._active_count)

        # A short rendezvous makes the static baseline reliably overlap both
        # shared writers.  Adaptive execution may run only one shared writer
        # in an epoch; it then proceeds after the bounded timeout.
        if resource == "shared" and attempt_number == 1:
            self._shared_arrivals.add(task_id)
            arrival_event = self._event()
            if {"a-conflict", "b-conflict"} <= self._shared_arrivals:
                arrival_event.set()
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    arrival_event.wait(),
                    timeout=max(self.delay_seconds * 4.0, 0.002),
                )

        await asyncio.sleep(self.delay_seconds)
        passed = identity not in self.failed_attempts
        self.pending_verifier_results[task_id].append(passed)
        self.records.append(
            _AttemptRecord(
                task_id=task_id,
                attempt_number=attempt_number,
                resource=resource,
                passed=passed,
                conflict=conflict,
            )
        )
        active.pop(identity, None)
        self._active_count -= 1

    def verify(self, task_id: str) -> VerificationOutcome:
        queue = self.pending_verifier_results[task_id]
        if not queue:
            raise RuntimeError(f"no executor result is available for {task_id!r}")
        passed = queue.popleft()
        attempt_number = self.attempts[task_id]
        return VerificationOutcome(
            passed=passed,
            artifact_id=f"adaptive-artifact-{task_id}",
            version=1,
            content=f"{task_id}:attempt-{attempt_number}",
            evidence_note="controlled adaptive-runtime benchmark",
        )

    def metrics(self) -> dict[str, Any]:
        executed = len(self.records)
        stale_work = sum(not record.passed for record in self.records)
        rework = sum(max(count - 1, 0) for count in self.attempts.values())
        return {
            "executed_attempts": executed,
            "unique_tasks": len(self.attempts),
            "attempts_by_task": dict(sorted(self.attempts.items())),
            "stale_work": stale_work,
            "rework_attempts": rework,
            "conflict_overlap_events": self.conflict_overlap_events,
            "peak_parallelism": self.peak_parallelism,
            "records": [
                {
                    "task_id": record.task_id,
                    "attempt_number": record.attempt_number,
                    "resource": record.resource,
                    "passed": record.passed,
                    "conflict": record.conflict,
                }
                for record in self.records
            ],
        }


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value))


def _runtime_audit(runtime: AgentOS, graph_id: str) -> dict[str, Any]:
    """Project durable Scheduler/Kernel/VPG facts for one benchmark run.

    The workload counters are collected in the controlled fake executor, but
    this independent projection proves that every invocation crossed the real
    AgentOS -> Scheduler -> Claim -> Kernel Lease -> WorkerPool -> VPG path.
    """

    claims = [claim for claim in runtime.scheduler.claims if claim.graph_id == graph_id]
    attempts = [attempt for attempt in runtime.scheduler.attempts if attempt.graph_id == graph_id]
    events = [event for event in runtime.scheduler.events if event.graph_id == graph_id]
    event_counts = Counter(_enum_value(event.event_type) for event in events)
    active_states = {
        ClaimState.PROPOSED.value,
        ClaimState.ACQUIRING.value,
        ClaimState.ACTIVE.value,
    }
    active_claims = [claim for claim in claims if _enum_value(claim.state) in active_states]
    nodes, _edges = runtime.vpg.snapshot_projection(graph_id)
    pass_evidence = [
        node
        for node in nodes.values()
        if isinstance(node, EvidenceNode) and node.result is EvidenceResult.PASS
    ]
    evidence_by_task = {
        task_id: len(runtime._vpg_surface.task_evidence_bindings(graph_id, task_id))
        for task_id in TASK_IDS
    }
    attempt_state_counts = Counter(_enum_value(attempt.state) for attempt in attempts)
    claim_state_counts = Counter(_enum_value(claim.state) for claim in claims)
    active_reservations = runtime.scheduler.resource_manager.list_active()
    live_kernel_leases = (
        [] if runtime.kernel is None else runtime.kernel._lease_service.list_all_leases()
    )
    return {
        "graph_id": graph_id,
        "graph_version": runtime.vpg.get_graph(graph_id).current_version,
        "scheduler_attempts": len(attempts),
        "scheduler_attempts_by_task": dict(
            sorted(Counter(attempt.task_id for attempt in attempts).items())
        ),
        "attempt_state_counts": dict(sorted(attempt_state_counts.items())),
        "claims_created": len(claims),
        "claims_with_kernel_lease": sum(bool(claim.lease_id) for claim in claims),
        "claims_with_positive_fence": sum(
            isinstance(claim.lease_fencing_token, int)
            and not isinstance(claim.lease_fencing_token, bool)
            and claim.lease_fencing_token > 0
            for claim in claims
        ),
        "claim_state_counts": dict(sorted(claim_state_counts.items())),
        "active_claims_after_run": len(active_claims),
        "active_reservations_after_run": len(active_reservations),
        "live_kernel_leases_after_run": len(live_kernel_leases),
        "scheduler_event_counts": dict(sorted(event_counts.items())),
        "pass_evidence_nodes": len(pass_evidence),
        "valid_evidence_bindings_by_task": evidence_by_task,
    }


def _conflict_graph() -> ConflictGraph:
    return ConflictGraph.from_access_sets(
        [
            TaskAccessSet(
                task_id=task_id,
                read_set=(f"workspace://{resource}",),
                write_set=(f"workspace://{resource}",),
            )
            for task_id, resource in TASK_RESOURCES.items()
        ]
    )


async def _run_case(
    *,
    mode: str,
    adaptive: bool,
    delay_seconds: float,
    max_concurrency: int,
) -> dict[str, Any]:
    workload = _ConflictWorkload(delay_seconds=delay_seconds)
    runtime = AgentOS(":memory:")
    try:
        runtime.add_agent(
            Agent(
                "benchmark-worker",
                executor=workload.execute,
                specializations=("benchmark",),
                max_concurrency=max_concurrency,
            )
        )
        goal = Goal(f"adaptive-runtime-{mode}")
        for task_id, resource in TASK_RESOURCES.items():
            goal.task(
                task_id,
                agent="benchmark-worker",
                required_specializations=("benchmark",),
                inputs=(f"workspace://{resource}",),
                outputs=(f"workspace://{resource}",),
                max_attempts=3,
                verify=lambda task_id=task_id: workload.verify(task_id),
            )

        started = time.perf_counter()
        result = await runtime.run_async(
            goal,
            max_dispatches=DEFAULT_MAX_DISPATCHES,
            max_steps=DEFAULT_MAX_STEPS,
            max_concurrency=max_concurrency,
            adaptive=adaptive,
            conflict_graph=_conflict_graph() if adaptive else None,
        )
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        metrics = workload.metrics()
        epochs = result.meta.get("adaptive_epochs", []) if adaptive else []
        selected_trace = [len(tuple(epoch.get("selected_task_ids", ()))) for epoch in epochs]
        graph_id = runtime._goal_gid[goal.goal_id]
        audit = _runtime_audit(runtime, graph_id)
        correctness = {
            "goal_closed": result.goal_state == "closed",
            "all_tasks_verified": set(result.verified) == set(TASK_IDS),
            "executor_attempts_match_scheduler": (
                metrics["executed_attempts"] == audit["scheduler_attempts"]
            ),
            "executor_attempts_by_task_match_scheduler": (
                metrics["attempts_by_task"] == audit["scheduler_attempts_by_task"]
            ),
            "all_attempts_crossed_claim_and_lease": (
                audit["claims_created"] == audit["scheduler_attempts"]
                and audit["claims_with_kernel_lease"] == audit["claims_created"]
                and audit["claims_with_positive_fence"] == audit["claims_created"]
            ),
            "all_verified_tasks_have_valid_evidence": all(
                audit["valid_evidence_bindings_by_task"].get(task_id, 0) == 1
                for task_id in TASK_IDS
            ),
            "semantic_attempt_count_matches_verified_tasks": (
                audit["attempt_state_counts"].get(AttemptState.VERIFIED_SEMANTICALLY.value, 0)
                == len(TASK_IDS)
            ),
            "no_active_claims": audit["active_claims_after_run"] == 0,
            "no_active_reservations": audit["active_reservations_after_run"] == 0,
            "no_live_kernel_leases": audit["live_kernel_leases_after_run"] == 0,
        }
        if adaptive:
            correctness["adaptive_metadata_present"] = bool(result.meta.get("adaptive"))
            correctness["no_adaptive_conflict_overlap"] = metrics["conflict_overlap_events"] == 0
            correctness["no_adaptive_stale_work"] = metrics["stale_work"] == 0

        return {
            "mode": mode,
            "elapsed_ms": round(elapsed_ms, 3),
            **metrics,
            "selected_parallelism_trace": selected_trace,
            "selected_parallelism_avg": (
                round(sum(selected_trace) / len(selected_trace), 3) if selected_trace else 0.0
            ),
            "selected_parallelism_peak": max(selected_trace, default=0),
            "configured_parallelism": max_concurrency,
            "result_failures": list(result.failures),
            "correctness": correctness,
            "goal_state": result.goal_state,
            "verified": list(result.verified),
            "adaptive_epochs": epochs,
            "runtime_audit": audit,
        }
    finally:
        runtime.close()


async def run_benchmark_async(
    *,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
) -> dict[str, Any]:
    """Run the static-vs-adaptive controlled benchmark."""

    if isinstance(delay_seconds, bool) or not isinstance(delay_seconds, (int, float)):
        raise ValueError("delay_seconds must be a positive number")
    if delay_seconds <= 0:
        raise ValueError("delay_seconds must be positive")
    if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
        raise ValueError("max_concurrency must be a positive integer")
    if max_concurrency < 2:
        raise ValueError("max_concurrency must be >= 2 for this conflict workload")

    static = await _run_case(
        mode="static",
        adaptive=False,
        delay_seconds=float(delay_seconds),
        max_concurrency=max_concurrency,
    )
    adaptive = await _run_case(
        mode="adaptive",
        adaptive=True,
        delay_seconds=float(delay_seconds),
        max_concurrency=max_concurrency,
    )
    static_elapsed = float(static["elapsed_ms"])
    adaptive_elapsed = float(adaptive["elapsed_ms"])
    report: dict[str, Any] = {
        "benchmark": "adaptive_runtime",
        "benchmark_version": 1,
        "workload": {
            "task_ids": list(TASK_IDS),
            "conflicting_tasks": ["a-conflict", "b-conflict"],
            "independent_tasks": ["c-independent", "d-independent"],
            "delay_seconds": float(delay_seconds),
            "max_concurrency": max_concurrency,
            "resource_declarations": dict(TASK_RESOURCES),
        },
        "static": static,
        "adaptive": adaptive,
        "comparison": {
            "adaptive_wall_time_ratio": (
                adaptive_elapsed / static_elapsed if static_elapsed > 0 else 0.0
            ),
            "rework_attempt_reduction": (static["rework_attempts"] - adaptive["rework_attempts"]),
            "stale_work_reduction": static["stale_work"] - adaptive["stale_work"],
            "executed_attempt_reduction": (
                static["executed_attempts"] - adaptive["executed_attempts"]
            ),
        },
        "scope": {
            "offline": True,
            "public_agentos_run_async": True,
            "authoritative_path": (
                "AgentOS.run_async -> Scheduler eligibility/resource admission -> "
                "TaskClaim -> Kernel Lease -> AsyncWorkerPool -> verifier -> VPG Evidence"
            ),
            "explicit_conflict_graph": True,
            "controlled_conflict_injection": True,
            "fake_executor": True,
            "wall_clock_measured": True,
            "does_not_measure": (
                "model quality, provider economics, physical CPU/GPU/RAM/VRAM "
                "telemetry, distributed scheduling, or hidden provenance discovery"
            ),
        },
    }
    violations: list[str] = []
    for label in ("static", "adaptive"):
        if not all(report[label]["correctness"].values()):
            violations.append(f"{label} correctness contract failed")
    if adaptive["stale_work"] != 0 or adaptive["rework_attempts"] != 0:
        violations.append("adaptive policy performed stale/rework work")
    if static["stale_work"] < 1 or static["rework_attempts"] < 1:
        violations.append("static baseline did not exercise the intended conflict")
    report["violations"] = violations
    report["valid"] = not violations
    return report


def run_benchmark(**kwargs: Any) -> dict[str, Any]:
    """Synchronous wrapper for scripts, tests, and notebooks."""

    return asyncio.run(run_benchmark_async(**kwargs))


__all__ = [
    "DEFAULT_DELAY_SECONDS",
    "DEFAULT_MAX_CONCURRENCY",
    "run_benchmark",
    "run_benchmark_async",
]
