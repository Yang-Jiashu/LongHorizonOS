"""Static-vs-adaptive benchmark through the real AgentOS/Harness boundary.

This controlled workload is deliberately small, but every attempt follows the
authoritative runtime path:

    Scheduler -> Claim/Attempt -> Kernel Lease -> Agent executor
    -> exact-identity Harness START -> verifier -> VPG Evidence commit

The default provider is deterministic and local.  Its token/cost values are
synthetic accounting signals, not measurements from a model API.  Callers may
explicitly load a provider factory for network-backed experiments; those
results remain provider-reported observations rather than a quality claim.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import time
from collections import Counter, defaultdict, deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator

from lhos.runtimes.multi_agent import AttemptState, ClaimState
from lhos.runtimes.verified_progress.models import EvidenceNode, EvidenceResult
from lhos.sdk import (
    Agent,
    AgentOS,
    CallableHarnessAdapter,
    ConflictGraph,
    Goal,
    HarnessHookOutcome,
    HarnessOperation,
    HarnessResultStatus,
    HarnessSessionIdentity,
    TaskAccessSet,
    VerificationOutcome,
)

DEFAULT_DELAY_SECONDS = 0.01
DEFAULT_MAX_CONCURRENCY = 2
DEFAULT_MAX_DISPATCHES = 8
DEFAULT_MAX_STEPS = 8
BENCHMARK_VERSION = 1

TASK_RESOURCES: dict[str, str] = {
    "a-conflict": "shared",
    "b-conflict": "shared",
    "c-independent": "c",
    "d-independent": "d",
}
TASK_IDS: tuple[str, ...] = tuple(TASK_RESOURCES)


class HarnessProviderRequest(BaseModel):
    """One provider invocation made from inside a Harness START hook."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    task_id: str = Field(min_length=1)
    attempt_number: int = Field(ge=1)
    resource_id: str = Field(min_length=1)
    prompt: str = Field(min_length=1)
    declared_delay_seconds: float = Field(gt=0.0)


class HarnessProviderResult(BaseModel):
    """Normalized provider output and usage accounting for one attempt."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    content: str = Field(min_length=1)
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    model_cost_usd: float = Field(ge=0.0)
    usage_kind: Literal["synthetic", "provider_reported"] = "provider_reported"

    @field_validator("model_cost_usd")
    @classmethod
    def _finite_cost(cls, value: float) -> float:
        number = float(value)
        if number != number or number in (float("inf"), float("-inf")):
            raise ValueError("model_cost_usd must be finite")
        return number


class HarnessBenchmarkProvider(Protocol):
    """Minimal optional provider contract for this benchmark."""

    provider_id: str

    def execute(
        self,
        request: HarnessProviderRequest,
    ) -> HarnessProviderResult | Mapping[str, Any] | Any: ...


class DeterministicLocalHarnessProvider:
    """Reproducible local execution provider with declared synthetic usage."""

    provider_id = "deterministic-local"

    def __init__(
        self,
        *,
        input_tokens: int = 128,
        output_tokens: int = 64,
        input_cost_per_token_usd: float = 0.000001,
        output_cost_per_token_usd: float = 0.000002,
    ) -> None:
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens
        self.input_cost_per_token_usd = input_cost_per_token_usd
        self.output_cost_per_token_usd = output_cost_per_token_usd

    async def execute(self, request: HarnessProviderRequest) -> HarnessProviderResult:
        await asyncio.sleep(request.declared_delay_seconds)
        cost = (
            self.input_tokens * self.input_cost_per_token_usd
            + self.output_tokens * self.output_cost_per_token_usd
        )
        return HarnessProviderResult(
            content=f"{request.task_id}:attempt-{request.attempt_number}",
            input_tokens=self.input_tokens,
            output_tokens=self.output_tokens,
            model_cost_usd=round(cost, 12),
            usage_kind="synthetic",
        )


def make_local_provider() -> HarnessBenchmarkProvider:
    """Public no-argument factory used by CLI/plugin smoke tests."""

    return DeterministicLocalHarnessProvider()


def load_provider_factory(spec: str) -> Callable[[], HarnessBenchmarkProvider]:
    """Load an explicit ``module:factory`` provider plugin.

    Importing a provider executes caller-selected Python code.  The CLI only
    does this when ``--provider-factory`` is explicitly supplied.
    """

    normalized = str(spec).strip()
    module_name, separator, attribute = normalized.partition(":")
    if not separator or not module_name.strip() or not attribute.strip():
        raise ValueError("provider factory must use the form module:callable")
    module = importlib.import_module(module_name.strip())
    factory = getattr(module, attribute.strip(), None)
    if not callable(factory):
        raise ValueError(f"provider factory {normalized!r} is not callable")

    def create() -> HarnessBenchmarkProvider:
        provider = factory()
        _validate_provider(provider)
        return cast(HarnessBenchmarkProvider, provider)

    return create


@dataclass(frozen=True, slots=True)
class _AttemptRecord:
    task_id: str
    attempt_number: int
    resource: str
    passed: bool
    conflict: bool
    claim_id: str
    attempt_id: str
    lease_id: str
    session_id: str
    input_tokens: int
    output_tokens: int
    verification_tokens: int
    model_cost_usd: float
    verification_cost_usd: float
    usage_kind: str

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens + self.verification_tokens

    @property
    def total_cost_usd(self) -> float:
        return self.model_cost_usd + self.verification_cost_usd


@dataclass
class _HarnessWorkload:
    runtime: AgentOS
    goal_id: str
    provider: HarnessBenchmarkProvider
    delay_seconds: float
    verification_tokens_per_attempt: int
    verification_cost_usd_per_attempt: float
    attempts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    pending_records: dict[str, deque[_AttemptRecord]] = field(
        default_factory=lambda: defaultdict(deque)
    )
    active_by_resource: dict[str, dict[tuple[str, int], str]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    failed_attempts: set[tuple[str, int]] = field(default_factory=set)
    records: list[_AttemptRecord] = field(default_factory=list)
    harness_sessions: int = 0
    harness_start_controls: int = 0
    harness_control_failures: int = 0
    conflict_overlap_events: int = 0
    active_count: int = 0
    peak_parallelism: int = 0
    parallelism_trace: list[int] = field(default_factory=list)

    async def execute(self, context: Any, task_id: str) -> Any:
        """Register and START a Harness bound to the live SDK attempt."""

        claim = next(
            (item for item in self.runtime.scheduler.claims if item.claim_id == context.claim_id),
            None,
        )
        attempt = self.runtime.scheduler.attempt_for_claim(context.claim_id)
        if claim is None or attempt is None:
            raise RuntimeError("Harness benchmark could not resolve its live Claim/Attempt")
        if attempt.attempt_id != context.attempt_id:
            raise RuntimeError("Harness benchmark execution context lost its Attempt fence")
        if not claim.lease_id:
            raise RuntimeError("Harness benchmark Claim has no Kernel Lease")

        identity = HarnessSessionIdentity(
            session_id=f"harness-{attempt.attempt_id}",
            graph_id=claim.graph_id,
            graph_version=claim.graph_version,
            semantic_epoch=attempt.semantic_epoch,
            task_id=task_id,
            agent_id=claim.agent_id,
            claim_id=claim.claim_id,
            attempt_id=attempt.attempt_id,
        )

        async def start(_request: Any, _snapshot: Any) -> HarnessHookOutcome:
            record = await self._execute_provider_attempt(
                identity=identity,
                lease_id=claim.lease_id,
            )
            self.pending_records[task_id].append(record)
            return HarnessHookOutcome(
                completed=True,
                progress=1.0,
                output={
                    "task_id": task_id,
                    "attempt_number": record.attempt_number,
                    "content": f"{task_id}:harness-complete",
                },
                details={
                    "usage_kind": record.usage_kind,
                    "input_tokens": record.input_tokens,
                    "output_tokens": record.output_tokens,
                    "model_cost_usd": record.model_cost_usd,
                },
            )

        harness = CallableHarnessAdapter(identity, start=start)
        self.runtime.register_harness(harness)
        self.harness_sessions += 1
        try:
            result = await self.runtime.control_harness(
                claim.claim_id,
                HarnessOperation.START,
                request_id=f"harness-start-{attempt.attempt_id}",
                reason="execute controlled benchmark attempt",
            )
            if result.status is not HarnessResultStatus.APPLIED:
                self.harness_control_failures += 1
                raise RuntimeError(f"Harness START was {result.status.value}")
            self.harness_start_controls += 1
            return result.output
        finally:
            self.runtime.unregister_harness(identity.session_id, claim_id=claim.claim_id)

    async def _execute_provider_attempt(
        self,
        *,
        identity: HarnessSessionIdentity,
        lease_id: str,
    ) -> _AttemptRecord:
        task_id = identity.task_id
        resource = TASK_RESOURCES[task_id]
        attempt_number = self.attempts[task_id] + 1
        self.attempts[task_id] = attempt_number
        attempt_key = (task_id, attempt_number)

        active = self.active_by_resource[resource]
        conflict = bool(active)
        if conflict:
            self.conflict_overlap_events += 1
            loser = max((task_id, *active.values()))
            if loser == task_id:
                self.failed_attempts.add(attempt_key)
            for active_key, active_task in active.items():
                if active_task == loser:
                    self.failed_attempts.add(active_key)
        active[attempt_key] = task_id
        self.active_count += 1
        self.peak_parallelism = max(self.peak_parallelism, self.active_count)
        self.parallelism_trace.append(self.active_count)

        try:
            # Hold the logical attempt open for one scheduling slice before
            # entering the provider.  This keeps the controlled static
            # baseline's declared write/write conflict observable even when an
            # optional provider returns synchronously, while applying the same
            # delay to both policies.
            await asyncio.sleep(self.delay_seconds)
            response = await _invoke_provider(
                self.provider,
                HarnessProviderRequest(
                    task_id=task_id,
                    attempt_number=attempt_number,
                    resource_id=resource,
                    prompt=(
                        f"Complete controlled task {task_id} for resource {resource}; "
                        "return a compact completion marker."
                    ),
                    declared_delay_seconds=self.delay_seconds,
                ),
            )
            passed = attempt_key not in self.failed_attempts
            record = _AttemptRecord(
                task_id=task_id,
                attempt_number=attempt_number,
                resource=resource,
                passed=passed,
                conflict=conflict,
                claim_id=identity.claim_id,
                attempt_id=identity.attempt_id,
                lease_id=lease_id,
                session_id=identity.session_id,
                input_tokens=response.input_tokens,
                output_tokens=response.output_tokens,
                verification_tokens=self.verification_tokens_per_attempt,
                model_cost_usd=response.model_cost_usd,
                verification_cost_usd=self.verification_cost_usd_per_attempt,
                usage_kind=response.usage_kind,
            )
            self.records.append(record)
            return record
        finally:
            active.pop(attempt_key, None)
            self.active_count -= 1
            self.parallelism_trace.append(self.active_count)

    def verify(self, task_id: str) -> VerificationOutcome:
        queue = self.pending_records[task_id]
        if not queue:
            raise RuntimeError(f"no Harness result is available for {task_id!r}")
        record = queue.popleft()
        return VerificationOutcome(
            passed=record.passed,
            artifact_id=f"harness-adaptive-{task_id}",
            version=1,
            content=f"{task_id}:attempt-{record.attempt_number}",
            evidence_note="controlled AgentOS/Harness adaptive benchmark",
            details={
                "attempt_number": record.attempt_number,
                "usage_kind": record.usage_kind,
                "verification_tokens": record.verification_tokens,
                "verification_cost_usd": record.verification_cost_usd,
            },
        )

    def metrics(self) -> dict[str, Any]:
        usage = _aggregate_usage(self.records)
        stale_records = [record for record in self.records if not record.passed]
        reexecuted_ids = tuple(
            sorted(task_id for task_id, count in self.attempts.items() if count > 1)
        )
        reexecuted_records = [record for record in self.records if record.attempt_number > 1]
        return {
            "executed_attempts": len(self.records),
            "unique_tasks": len(self.attempts),
            "attempts_by_task": dict(sorted(self.attempts.items())),
            "stale_attempts": len(stale_records),
            "stale_task_ids": sorted({record.task_id for record in stale_records}),
            "stale_work_tokens": sum(record.total_tokens for record in stale_records),
            "stale_work_cost_usd": round(
                sum(record.total_cost_usd for record in stale_records),
                12,
            ),
            "rework_attempts": sum(max(count - 1, 0) for count in self.attempts.values()),
            "reexecuted_task_ids": list(reexecuted_ids),
            "reexecuted_work_tokens": sum(record.total_tokens for record in reexecuted_records),
            "conflict_overlap_events": self.conflict_overlap_events,
            "actual_parallelism_peak": self.peak_parallelism,
            "actual_parallelism_trace": list(self.parallelism_trace),
            "harness_sessions": self.harness_sessions,
            "harness_start_controls": self.harness_start_controls,
            "harness_control_failures": self.harness_control_failures,
            "usage": usage,
            "records": [
                {
                    "task_id": record.task_id,
                    "attempt_number": record.attempt_number,
                    "resource": record.resource,
                    "passed": record.passed,
                    "conflict": record.conflict,
                    "claim_id": record.claim_id,
                    "attempt_id": record.attempt_id,
                    "lease_id": record.lease_id,
                    "session_id": record.session_id,
                    "input_tokens": record.input_tokens,
                    "output_tokens": record.output_tokens,
                    "verification_tokens": record.verification_tokens,
                    "model_cost_usd": record.model_cost_usd,
                    "verification_cost_usd": record.verification_cost_usd,
                    "usage_kind": record.usage_kind,
                }
                for record in self.records
            ],
        }


def _validate_provider(provider: Any) -> None:
    provider_id = str(getattr(provider, "provider_id", "")).strip()
    if not provider_id:
        raise ValueError("Harness benchmark provider must expose a non-empty provider_id")
    if not callable(getattr(provider, "execute", None)):
        raise TypeError("Harness benchmark provider must implement execute(request)")


async def _invoke_provider(
    provider: HarnessBenchmarkProvider,
    request: HarnessProviderRequest,
) -> HarnessProviderResult:
    hook = provider.execute
    if inspect.iscoroutinefunction(hook):
        raw = hook(request)
    else:
        # The protocol deliberately permits either a sync result or an
        # awaitable.  Isolate the broad plugin return type at this boundary
        # instead of leaking it into asyncio.to_thread's callable inference.
        sync_hook = cast(Callable[[HarnessProviderRequest], Any], hook)
        raw = await asyncio.to_thread(sync_hook, request)
    if inspect.isawaitable(raw):
        raw = await raw
    if isinstance(raw, HarnessProviderResult):
        return raw
    if isinstance(raw, Mapping):
        return HarnessProviderResult.model_validate(dict(raw))
    raise TypeError("Harness benchmark provider must return HarnessProviderResult or a mapping")


def _aggregate_usage(records: list[_AttemptRecord]) -> dict[str, Any]:
    input_tokens = sum(record.input_tokens for record in records)
    output_tokens = sum(record.output_tokens for record in records)
    verification_tokens = sum(record.verification_tokens for record in records)
    model_cost = sum(record.model_cost_usd for record in records)
    verification_cost = sum(record.verification_cost_usd for record in records)
    usage_sources = Counter(record.usage_kind for record in records)
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "verification_tokens": verification_tokens,
        "total_tokens": input_tokens + output_tokens + verification_tokens,
        "model_cost_usd": round(model_cost, 12),
        "verification_cost_usd": round(verification_cost, 12),
        "total_cost_usd": round(model_cost + verification_cost, 12),
        "usage_source_counts": dict(sorted(usage_sources.items())),
        "synthetic_usage": bool(records)
        and all(record.usage_kind == "synthetic" for record in records),
        "provider_reported_usage": any(
            record.usage_kind == "provider_reported" for record in records
        ),
    }


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value))


def _runtime_audit(runtime: AgentOS, graph_id: str) -> dict[str, Any]:
    claims = [claim for claim in runtime.scheduler.claims if claim.graph_id == graph_id]
    attempts = [attempt for attempt in runtime.scheduler.attempts if attempt.graph_id == graph_id]
    events = [event for event in runtime.scheduler.events if event.graph_id == graph_id]
    event_counts = Counter(_enum_value(event.event_type) for event in events)
    active_states = {
        ClaimState.PROPOSED.value,
        ClaimState.ACQUIRING.value,
        ClaimState.ACTIVE.value,
    }
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
    active_reservations = runtime.scheduler.resource_manager.list_active()
    live_kernel_leases = (
        [] if runtime.kernel is None else runtime.kernel._lease_service.list_all_leases()
    )
    return {
        "ownership_path": True,
        "graph_id": graph_id,
        "graph_version": runtime.vpg.get_graph(graph_id).current_version,
        "scheduler_attempts": len(attempts),
        "scheduler_attempts_by_task": dict(
            sorted(Counter(attempt.task_id for attempt in attempts).items())
        ),
        "attempt_state_counts": dict(
            sorted(Counter(_enum_value(attempt.state) for attempt in attempts).items())
        ),
        "claims_created": len(claims),
        "claims_with_kernel_lease": sum(bool(claim.lease_id) for claim in claims),
        "claims_with_positive_fence": sum(
            isinstance(claim.lease_fencing_token, int)
            and not isinstance(claim.lease_fencing_token, bool)
            and claim.lease_fencing_token > 0
            for claim in claims
        ),
        "claim_state_counts": dict(
            sorted(Counter(_enum_value(claim.state) for claim in claims).items())
        ),
        "harness_control_events": event_counts.get("harness_control", 0),
        "scheduler_event_counts": dict(sorted(event_counts.items())),
        "pass_evidence_nodes": len(pass_evidence),
        "valid_evidence_bindings_by_task": evidence_by_task,
        "active_claims_after_run": sum(
            _enum_value(claim.state) in active_states for claim in claims
        ),
        "active_reservations_after_run": len(active_reservations),
        "live_kernel_leases_after_run": len(live_kernel_leases),
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
    provider_factory: Callable[[], HarnessBenchmarkProvider],
    verification_tokens_per_attempt: int,
    verification_cost_usd_per_attempt: float,
) -> dict[str, Any]:
    provider = provider_factory()
    _validate_provider(provider)
    runtime = AgentOS(":memory:")
    workload = _HarnessWorkload(
        runtime=runtime,
        goal_id=f"harness-adaptive-{mode}",
        provider=provider,
        delay_seconds=delay_seconds,
        verification_tokens_per_attempt=verification_tokens_per_attempt,
        verification_cost_usd_per_attempt=verification_cost_usd_per_attempt,
    )
    try:
        runtime.add_agent(
            Agent(
                "benchmark-worker",
                executor=workload.execute,
                executor_api="context_v1",
                specializations=("benchmark",),
                max_concurrency=max_concurrency,
            )
        )
        goal = Goal(workload.goal_id, executor_api="context_v1")
        for task_id, resource in TASK_RESOURCES.items():
            goal.task(
                task_id,
                agent="benchmark-worker",
                required_specializations=("benchmark",),
                inputs=(f"workspace://{resource}",),
                outputs=(f"workspace://{resource}",),
                max_attempts=3,
                executor_api="context_v1",
                verify=lambda _context, task_id=task_id: workload.verify(task_id),
            )

        started = time.perf_counter()
        result = await runtime.run_async(
            goal,
            max_dispatches=DEFAULT_MAX_DISPATCHES,
            max_steps=DEFAULT_MAX_STEPS,
            max_concurrency=max_concurrency,
            adaptive=adaptive,
            conflict_graph=_conflict_graph() if adaptive else None,
            persist_adaptive_epochs=False,
        )
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        graph_id = runtime._goal_gid[goal.goal_id]
        metrics = workload.metrics()
        audit = _runtime_audit(runtime, graph_id)
        verified = tuple(sorted(result.verified))
        adaptive_epochs = result.meta.get("adaptive_epochs", ()) if adaptive else ()
        selected_parallelism = [
            len(tuple(epoch.get("selected_task_ids", ()))) for epoch in adaptive_epochs
        ]
        correctness = {
            "goal_closed": result.goal_state == "closed",
            "all_tasks_verified": set(verified) == set(TASK_IDS),
            "ownership_path": audit["ownership_path"] is True,
            "executor_attempts_match_scheduler": (
                metrics["executed_attempts"] == audit["scheduler_attempts"]
            ),
            "attempts_by_task_match_scheduler": (
                metrics["attempts_by_task"] == audit["scheduler_attempts_by_task"]
            ),
            "every_attempt_has_claim_lease_and_harness_start": (
                audit["claims_created"] == metrics["executed_attempts"]
                and audit["claims_with_kernel_lease"] == metrics["executed_attempts"]
                and audit["claims_with_positive_fence"] == metrics["executed_attempts"]
                and audit["harness_control_events"] == metrics["executed_attempts"]
                and metrics["harness_sessions"] == metrics["executed_attempts"]
                and metrics["harness_start_controls"] == metrics["executed_attempts"]
                and metrics["harness_control_failures"] == 0
            ),
            "all_verified_tasks_have_evidence": all(
                audit["valid_evidence_bindings_by_task"].get(task_id, 0) == 1
                for task_id in TASK_IDS
            ),
            "semantic_attempt_count_matches_verified_tasks": (
                audit["attempt_state_counts"].get(
                    AttemptState.VERIFIED_SEMANTICALLY.value,
                    0,
                )
                == len(TASK_IDS)
            ),
            "no_live_ownership_after_run": (
                audit["active_claims_after_run"] == 0
                and audit["active_reservations_after_run"] == 0
                and audit["live_kernel_leases_after_run"] == 0
            ),
        }
        if adaptive:
            correctness["adaptive_epoch_metadata_present"] = bool(adaptive_epochs)
            correctness["adaptive_avoids_declared_conflict"] = (
                metrics["conflict_overlap_events"] == 0 and metrics["stale_attempts"] == 0
            )
        return {
            "mode": mode,
            "provider_id": str(provider.provider_id),
            "elapsed_ms": round(elapsed_ms, 3),
            "wall_clock_measured": True,
            "verified_progress": round(len(verified) / len(TASK_IDS), 6),
            "verified_task_ids": list(verified),
            "goal_state": result.goal_state,
            "failures": list(result.failures),
            "configured_parallelism": max_concurrency,
            "selected_parallelism_trace": selected_parallelism,
            "selected_parallelism_peak": max(selected_parallelism, default=0),
            "adaptive_epoch_count": len(adaptive_epochs),
            **metrics,
            "runtime_audit": audit,
            "correctness": correctness,
        }
    finally:
        runtime.close()


async def run_benchmark_async(
    *,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    provider_factory: Callable[[], HarnessBenchmarkProvider] | None = None,
    provider_factory_spec: str | None = None,
    verification_tokens_per_attempt: int = 32,
    verification_cost_usd_per_attempt: float = 0.000032,
) -> dict[str, Any]:
    """Run the controlled static-vs-adaptive Harness integration benchmark."""

    if isinstance(delay_seconds, bool) or not isinstance(delay_seconds, (int, float)):
        raise ValueError("delay_seconds must be a positive number")
    if delay_seconds <= 0:
        raise ValueError("delay_seconds must be positive")
    if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
        raise ValueError("max_concurrency must be a positive integer")
    if max_concurrency < 2:
        raise ValueError("max_concurrency must be >= 2 for this conflict workload")
    if (
        isinstance(verification_tokens_per_attempt, bool)
        or not isinstance(verification_tokens_per_attempt, int)
        or verification_tokens_per_attempt < 0
    ):
        raise ValueError("verification_tokens_per_attempt must be a non-negative integer")
    if isinstance(verification_cost_usd_per_attempt, bool) or not isinstance(
        verification_cost_usd_per_attempt,
        (int, float),
    ):
        raise ValueError("verification_cost_usd_per_attempt must be non-negative")
    if verification_cost_usd_per_attempt < 0:
        raise ValueError("verification_cost_usd_per_attempt must be non-negative")
    if provider_factory is not None and not callable(provider_factory):
        raise TypeError("provider_factory must be callable")

    factory = provider_factory or make_local_provider
    static = await _run_case(
        mode="static",
        adaptive=False,
        delay_seconds=float(delay_seconds),
        max_concurrency=max_concurrency,
        provider_factory=factory,
        verification_tokens_per_attempt=verification_tokens_per_attempt,
        verification_cost_usd_per_attempt=float(verification_cost_usd_per_attempt),
    )
    adaptive = await _run_case(
        mode="adaptive",
        adaptive=True,
        delay_seconds=float(delay_seconds),
        max_concurrency=max_concurrency,
        provider_factory=factory,
        verification_tokens_per_attempt=verification_tokens_per_attempt,
        verification_cost_usd_per_attempt=float(verification_cost_usd_per_attempt),
    )
    static_usage = static["usage"]
    adaptive_usage = adaptive["usage"]
    report: dict[str, Any] = {
        "benchmark": "harness_adaptive",
        "benchmark_version": BENCHMARK_VERSION,
        "workload": {
            "task_ids": list(TASK_IDS),
            "conflicting_tasks": ["a-conflict", "b-conflict"],
            "independent_tasks": ["c-independent", "d-independent"],
            "resource_declarations": dict(TASK_RESOURCES),
            "delay_seconds": float(delay_seconds),
            "max_concurrency": max_concurrency,
            "provider_factory": provider_factory_spec or "built-in deterministic local",
            "verification_tokens_per_attempt": verification_tokens_per_attempt,
            "verification_cost_usd_per_attempt": float(verification_cost_usd_per_attempt),
        },
        "static": static,
        "adaptive": adaptive,
        "comparison": {
            "same_verified_set": (static["verified_task_ids"] == adaptive["verified_task_ids"]),
            "verified_progress_delta": (
                adaptive["verified_progress"] - static["verified_progress"]
            ),
            "attempt_reduction": (static["executed_attempts"] - adaptive["executed_attempts"]),
            "stale_attempt_reduction": (static["stale_attempts"] - adaptive["stale_attempts"]),
            "stale_work_token_reduction": (
                static["stale_work_tokens"] - adaptive["stale_work_tokens"]
            ),
            "total_token_reduction": (
                static_usage["total_tokens"] - adaptive_usage["total_tokens"]
            ),
            "total_cost_reduction_usd": round(
                static_usage["total_cost_usd"] - adaptive_usage["total_cost_usd"],
                12,
            ),
            "adaptive_wall_time_ratio": (
                adaptive["elapsed_ms"] / static["elapsed_ms"] if static["elapsed_ms"] > 0 else 0.0
            ),
        },
        "scope": {
            "offline_default": provider_factory is None,
            "ownership_path": True,
            "public_agentos_run_async": True,
            "exact_identity_harness_control": True,
            "authoritative_path": (
                "AgentOS.run_async -> Scheduler -> TaskClaim/Attempt -> Kernel Lease "
                "-> context-aware Agent executor -> registered Harness START "
                "-> verifier -> VPG Evidence commit"
            ),
            "policy_difference_only": True,
            "static_policy": "fixed concurrency without ConflictGraph",
            "adaptive_policy": "online epochs with explicit ConflictGraph",
            "wall_clock_measured": True,
            "usage_accounting": (
                "built-in provider values are declared synthetic proxies; an "
                "explicit provider plugin may return provider_reported usage"
            ),
            "does_not_measure": (
                "general model quality, hidden dependency discovery, physical "
                "CPU/GPU/RAM/VRAM placement, distributed scheduling, hard process "
                "preemption, or arbitrary real-world Agent workloads"
            ),
        },
    }
    violations: list[str] = []
    for label in ("static", "adaptive"):
        if not all(report[label]["correctness"].values()):
            violations.append(f"{label} correctness contract failed")
    if not report["comparison"]["same_verified_set"]:
        violations.append("static and adaptive modes reached different verified sets")
    if static["stale_attempts"] < 1 or static["rework_attempts"] < 1:
        violations.append("static baseline did not exercise the declared conflict")
    if adaptive["stale_attempts"] != 0 or adaptive["rework_attempts"] != 0:
        violations.append("adaptive mode performed stale or repeated work")
    if static["provider_id"] != adaptive["provider_id"]:
        violations.append("static and adaptive modes used different providers")
    report["violations"] = violations
    report["valid"] = not violations
    return report


def run_benchmark(**kwargs: Any) -> dict[str, Any]:
    """Synchronous wrapper for CLI, scripts, and tests."""

    return asyncio.run(run_benchmark_async(**kwargs))


__all__ = [
    "BENCHMARK_VERSION",
    "DEFAULT_DELAY_SECONDS",
    "DEFAULT_MAX_CONCURRENCY",
    "DeterministicLocalHarnessProvider",
    "HarnessBenchmarkProvider",
    "HarnessProviderRequest",
    "HarnessProviderResult",
    "load_provider_factory",
    "make_local_provider",
    "run_benchmark",
    "run_benchmark_async",
]
