"""Immutable global runtime-state projection for long-running Agent compute.

This module is an observation surface only. It derives a point-in-time view
from the VPG, Scheduler, Context VM bindings, and logical resource accounting;
it does not compile goals, reconcile state, claim work, or make scheduling
decisions.
"""

from __future__ import annotations

import hashlib
import json
from contextlib import nullcontext
from datetime import datetime
from typing import Any, Final, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field

from lhos.runtimes.verified_progress.readiness import compute_ready_task_ids

from .errors import ConfigurationError
from .graph_analysis import derive_graph_analysis

RUNTIME_STATE_SCHEMA_VERSION: Final[Literal["runtime-state.v1"]] = "runtime-state.v1"
RUNTIME_RECENT_EVENT_LIMIT: Final[int] = 32
_CURRENT_CLAIM_STATES = frozenset({"proposed", "acquiring", "active"})
_MISSING = object()
_GRAPH_SNAPSHOT_RETRIES = 3


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class UnavailableField(_FrozenModel):
    """One field that the current runtime cannot authoritatively project."""

    name: str
    reason: str


class ModelSlotState(_FrozenModel):
    name: str
    quantity: int = Field(ge=0)


class ResourceVectorState(_FrozenModel):
    """Immutable form of the Scheduler's additive logical resource vector."""

    cpu_millis: int = Field(default=0, ge=0)
    ram_bytes: int = Field(default=0, ge=0)
    gpu_count: int = Field(default=0, ge=0)
    vram_bytes: int = Field(default=0, ge=0)
    model_slots: tuple[ModelSlotState, ...] = ()


class TaskUnlockValueState(_FrozenModel):
    """Immediate structural unlock signal for one task."""

    task_id: str
    unlock_value: int = Field(default=0, ge=0)


class ProgressSemanticState(_FrozenModel):
    """VPG-relative semantic progress pinned to one immutable GraphVersion."""

    graph_id: str
    graph_version: int = Field(ge=0)
    projection_hash: str
    graph_closed: bool
    goal_closed: bool
    ready_frontier: tuple[str, ...]
    repair_ready_frontier: tuple[str, ...]
    verified_task_ids: tuple[str, ...]
    stale_task_ids: tuple[str, ...]
    invalid_task_ids: tuple[str, ...]
    unverified_task_ids: tuple[str, ...]
    # Graph-derived control signals.  These are relative to the declared VPG
    # dependency edges; they do not imply automatic provenance discovery.
    critical_path: tuple[str, ...] = ()
    downstream_unlock_values: tuple[TaskUnlockValueState, ...] = ()
    parallel_frontier: tuple[str, ...] = ()


class RecentRuntimeEventState(_FrozenModel):
    """Bounded, payload-redacted summary of one durable VPG event."""

    event_id: str
    event_type: str
    graph_version: int | None = Field(default=None, ge=0)
    causation_patch_id: str | None = None
    subject_id: str | None = None
    node_id: str | None = None
    payload_hash: str = Field(min_length=64, max_length=64)
    payload_size_bytes: int = Field(default=0, ge=0)
    recorded_at: datetime


class ResourceBindingState(_FrozenModel):
    """One normalized resource observation from a durable AgentSnapshot."""

    operation: str
    resource_uri: str
    artifact_id: str | None = None
    version: int | None = None
    content_hash: str | None = None
    action_id: str | None = None
    idempotency_key: str | None = None
    source_event_id: str | None = None
    source: str = ""
    known: bool = True
    observed_at: datetime | None = None


class ComputationCostState(_FrozenModel):
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    cached_input_tokens: int = Field(default=0, ge=0)
    model_calls: int = Field(default=0, ge=0)
    tool_calls: int = Field(default=0, ge=0)
    elapsed_ms: int = Field(default=0, ge=0)
    monetary_micros: int = Field(default=0, ge=0)


class CognitionAttemptState(_FrozenModel):
    """Current claim/attempt identity and the cognition state actually captured."""

    claim_id: str
    claim_state: str
    task_id: str
    agent_id: str
    process_id: str
    graph_version: int | None = Field(default=None, ge=0)
    attempt_id: str | None = None
    attempt_state: str | None = None
    semantic_epoch: int | None = None
    provenance_digest: str | None = None
    context_snapshot_id: str | None = None
    read_set: tuple[ResourceBindingState, ...] = ()
    write_set: tuple[ResourceBindingState, ...] = ()
    progress: float | None = Field(default=None, ge=0.0, le=1.0)
    cost: ComputationCostState | None = None
    unavailable: tuple[UnavailableField, ...] = ()


class AgentCognitionState(_FrozenModel):
    """Scheduler-backed view of all current, non-terminal execution claims."""

    available: bool
    reason: str | None = None
    current_attempts: tuple[CognitionAttemptState, ...] = ()


class ContextSnapshotSummary(_FrozenModel):
    """Materialized snapshot details, when a public Context VM lookup exposes them."""

    page_binding_count: int = Field(ge=0)
    tokens_used: int = Field(ge=0)
    bytes_used: int = Field(ge=0)


class ContextBindingState(_FrozenModel):
    """Exact Context VM identity sealed onto one Scheduler attempt."""

    attempt_id: str
    claim_id: str
    task_id: str
    agent_id: str
    snapshot_id: str
    manifest_id: str
    manifest_hash: str
    working_set_hash: str
    materialized_hash: str
    summary: ContextSnapshotSummary | None = None
    unavailable: tuple[UnavailableField, ...] = ()


class ContextRuntimeState(_FrozenModel):
    """Current context bindings; absence is explicit rather than inferred."""

    available: bool
    reason: str | None = None
    bindings: tuple[ContextBindingState, ...] = ()
    unavailable: tuple[UnavailableField, ...] = ()


class ActiveClaimResourceState(_FrozenModel):
    claim_id: str
    task_id: str
    agent_id: str
    reservation_id: str | None = None
    reserved: ResourceVectorState


class ResourcePoolState(_FrozenModel):
    pool_id: str
    capacity: ResourceVectorState | None
    reserved: ResourceVectorState
    available: ResourceVectorState | None
    reservation_ids: tuple[str, ...] = ()
    pending_reservation_ids: tuple[str, ...] = ()
    active_claim_ids: tuple[str, ...] = ()
    unavailable: tuple[UnavailableField, ...] = ()


class ResourceRuntimeState(_FrozenModel):
    """Scheduler logical admission state, not physical machine telemetry."""

    available: bool
    reason: str | None = None
    scope: Literal["scheduler_logical_resources"] = "scheduler_logical_resources"
    # Reservations are currently projected from the Scheduler's global
    # logical allocator.  Keep that scope explicit so callers do not mistake
    # this for graph-local physical telemetry.
    scope_id: Literal["global"] = "global"
    pools: tuple[ResourcePoolState, ...] = ()
    active_claims: tuple[ActiveClaimResourceState, ...] = ()


class GlobalRuntimeState(_FrozenModel):
    """The four-part state consumed by future online scheduling policies."""

    schema_version: Literal["runtime-state.v1"] = RUNTIME_STATE_SCHEMA_VERSION
    goal_id: str
    graph_id: str
    progress: ProgressSemanticState
    agent_cognition: AgentCognitionState
    context: ContextRuntimeState
    resources: ResourceRuntimeState
    recent_events: tuple[RecentRuntimeEventState, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


RuntimeStateView: TypeAlias = GlobalRuntimeState


def build_runtime_state_view(os_: Any, goal: Any) -> GlobalRuntimeState:
    """Build an immutable, deterministic, read-only global state projection.

    ``goal`` may be a compiled SDK Goal or its goal id. Missing goals fail
    instead of being compiled because observation must never mutate runtime
    state.
    """

    goal_id = str(getattr(goal, "goal_id", goal)).strip()
    if not goal_id:
        raise ConfigurationError("runtime state requires a non-empty goal id")
    graph_id = os_._gid_for(goal_id)
    if graph_id is None:
        raise ConfigurationError(
            f"goal {goal_id!r} is not compiled; runtime state observation is read-only"
        )

    graph, _graph_version, nodes, edges, projection_hash = _load_consistent_graph_projection(
        os_, graph_id
    )
    goal_node = nodes.get(goal_id)
    if _enum_value(getattr(goal_node, "node_type", "")) != "goal":
        raise ConfigurationError(
            f"goal {goal_id!r} is not owned by graph {graph_id!r}; "
            "runtime state observation is read-only"
        )

    progress = _build_progress_state(
        goal_id=goal_id,
        graph=graph,
        projection_hash=projection_hash,
        nodes=nodes,
        edges=edges,
    )
    scheduler_snapshot = _capture_scheduler_state(os_, graph_id)
    cognition = _build_cognition_state(
        scheduler_snapshot.claims,
        scheduler_snapshot.attempts,
        read_only=bool(getattr(os_, "_read_only", False)),
    )
    context = _build_context_state(
        cognition.current_attempts,
        scheduler_snapshot.attempts,
        context_service=getattr(os_, "_context_service", None),
    )
    resources = _build_resource_state(
        scheduler_snapshot,
        read_only=bool(getattr(os_, "_read_only", False)),
    )
    # ``_load_consistent_graph_projection`` already validates graph ownership
    # and pins the version.  Read the store tail directly so this observation
    # does not perform a second graph-record lookup or widen the race window.
    recent_events_reader = getattr(getattr(os_.vpg, "store", None), "get_recent_events", None)
    # ``get_recent_events`` is discovered dynamically for compatibility with
    # older GraphStore implementations, so its return type is intentionally
    # unknown to the type checker.  Normalize the bounded tail at this
    # boundary instead of widening ``_build_recent_event_state``'s contract.
    recent_event_tail: list[Any] = []
    if callable(recent_events_reader):
        recent_event_tail = list(
            recent_events_reader(
                graph_id,
                through_version=int(graph.current_version),
                limit=RUNTIME_RECENT_EVENT_LIMIT,
            )
        )
    recent_events = _build_recent_event_state(recent_event_tail)
    return GlobalRuntimeState(
        goal_id=goal_id,
        graph_id=graph_id,
        progress=progress,
        agent_cognition=cognition,
        context=context,
        resources=resources,
        recent_events=recent_events,
    )


def _load_consistent_graph_projection(
    os_: Any,
    graph_id: str,
    *,
    retries: int = _GRAPH_SNAPSHOT_RETRIES,
) -> tuple[Any, int, dict[str, Any], list[Any], str]:
    """Read one graph/version pair without publishing a mixed-version view.

    GraphStore exposes immutable snapshots but no public transaction spanning
    ``GraphRecord`` and the snapshot lookup.  An optimistic double-read is
    therefore used: if a concurrent commit advances the graph during the
    read, the candidate is discarded and retried.
    """

    for _ in range(max(1, retries)):
        graph_before = os_.vpg.get_graph(graph_id)
        graph_version = int(graph_before.current_version)
        nodes, edges = os_.vpg.store.load_projection_snapshot(graph_id, graph_version)
        version = os_.vpg.store.get_version(graph_id, graph_version)
        if version is None:
            raise ConfigurationError(
                f"graph {graph_id!r} version {graph_version} has no immutable version record"
            )
        graph_after = os_.vpg.get_graph(graph_id)
        if int(graph_after.current_version) != graph_version:
            continue
        return (
            graph_after,
            graph_version,
            nodes,
            edges,
            str(version.projection_hash),
        )
    raise ConfigurationError(
        f"graph {graph_id!r} changed while runtime state was being observed; retry"
    )


def _build_progress_state(
    *,
    goal_id: str,
    graph: Any,
    projection_hash: str,
    nodes: dict[str, Any],
    edges: list[Any],
) -> ProgressSemanticState:
    tasks = {
        node_id: node
        for node_id, node in nodes.items()
        if _enum_value(getattr(node, "node_type", "")) == "task"
    }
    ready = tuple(compute_ready_task_ids(nodes, edges))
    verified = tuple(
        sorted(
            node_id
            for node_id, node in tasks.items()
            if _enum_value(getattr(node, "validity", "")) == "verified"
        )
    )
    stale = tuple(
        sorted(
            node_id
            for node_id, node in tasks.items()
            if _enum_value(getattr(node, "validity", "")) == "stale"
        )
    )
    invalid = tuple(
        sorted(
            node_id
            for node_id, node in tasks.items()
            if _enum_value(getattr(node, "validity", "")) == "invalid"
        )
    )
    unverified = tuple(
        sorted(
            node_id
            for node_id, node in tasks.items()
            if _enum_value(getattr(node, "validity", "")) == "unverified"
        )
    )
    goal_node = nodes.get(goal_id)
    if _enum_value(getattr(goal_node, "node_type", "")) != "goal":
        goal_nodes = [
            node for node in nodes.values() if _enum_value(getattr(node, "node_type", "")) == "goal"
        ]
        goal_node = goal_nodes[0] if len(goal_nodes) == 1 else None
    goal_closed = (
        goal_node is not None and _enum_value(getattr(goal_node, "lifecycle", "")) == "closed"
    )
    stale_set = set(stale)
    repair_ready = tuple(task_id for task_id in ready if task_id in stale_set)
    analysis = derive_graph_analysis(
        goal_id=goal_id,
        nodes=nodes,
        edges=edges,
        ready_frontier=ready,
        repair_ready_frontier=repair_ready,
    )
    return ProgressSemanticState(
        graph_id=str(graph.graph_id),
        graph_version=int(graph.current_version),
        projection_hash=str(projection_hash),
        graph_closed=bool(graph.closed),
        goal_closed=goal_closed,
        ready_frontier=ready,
        repair_ready_frontier=repair_ready,
        verified_task_ids=verified,
        stale_task_ids=stale,
        invalid_task_ids=invalid,
        unverified_task_ids=unverified,
        critical_path=analysis.critical_path,
        downstream_unlock_values=tuple(
            TaskUnlockValueState(
                task_id=item.task_id,
                unlock_value=item.value,
            )
            for item in analysis.downstream_unlock_values
        ),
        parallel_frontier=analysis.parallel_frontier,
    )


class _SchedulerSnapshot(_FrozenModel):
    claims: tuple[Any, ...]
    attempts: tuple[Any, ...]
    resource_claims: tuple[Any, ...]
    capacity_by_pool: tuple[tuple[str, Any], ...]
    reservations: tuple[Any, ...]
    pending_reservations: tuple[Any, ...] = ()
    resource_available: bool = True
    resource_reason: str | None = None


def _capture_scheduler_state(os_: Any, graph_id: str) -> _SchedulerSnapshot:
    scheduler = os_.scheduler
    core = getattr(scheduler, "_s", scheduler)
    schedule_lock = getattr(core, "_schedule_lock", None)
    with schedule_lock if schedule_lock is not None else nullcontext():
        raw_claims_value = getattr(core, "claims", None)
        if raw_claims_value is None:
            raw_claims_value = getattr(scheduler, "claims", None)
        raw_claims: tuple[Any, ...] = tuple(raw_claims_value or ())
        raw_attempts_value = getattr(core, "attempts", None)
        if raw_attempts_value is None:
            raw_attempts_value = getattr(scheduler, "attempts", None)
        raw_attempts: tuple[Any, ...] = tuple(raw_attempts_value or ())
        claims = tuple(
            _model_copy(claim)
            for claim in raw_claims
            if str(getattr(claim, "graph_id", "")) == graph_id
        )
        attempts = tuple(
            _model_copy(attempt)
            for attempt in raw_attempts
            if str(getattr(attempt, "graph_id", "")) == graph_id
        )
        resource_manager = getattr(core, "resource_manager", None)
        if resource_manager is None:
            resource_manager = getattr(scheduler, "resource_manager", None)
        if resource_manager is None:
            return _SchedulerSnapshot(
                claims=tuple(sorted(claims, key=_claim_sort_key)),
                attempts=tuple(sorted(attempts, key=_attempt_sort_key)),
                resource_claims=tuple(
                    sorted(
                        (_model_copy(claim) for claim in raw_claims),
                        key=_claim_sort_key,
                    )
                ),
                capacity_by_pool=(),
                reservations=(),
                pending_reservations=(),
                resource_available=False,
                resource_reason="Scheduler exposes no logical resource manager",
            )
        resource_lock = getattr(resource_manager, "_lock", None)
        with resource_lock if resource_lock is not None else nullcontext():
            raw_capacities = dict(getattr(resource_manager, "_capacities", {}))
            reservations = tuple(
                _model_copy(reservation) for reservation in resource_manager.list_active()
            )
        pending_reservations = tuple(
            _model_copy(reservation)
            for reservation in (getattr(core, "_pending_durable_reservations", None) or ())
        )
        registry = getattr(os_, "_registry", None)
        registry_snapshot = registry.snapshot() if registry is not None else {}
        for pool_id, descriptor in registry_snapshot.items():
            raw_capacities.setdefault(pool_id, getattr(descriptor, "resource_capacity", None))
    capacities = tuple(
        (str(pool_id), _model_copy(vector))
        for pool_id, vector in sorted(raw_capacities.items())
        if vector is not None
    )
    return _SchedulerSnapshot(
        claims=tuple(sorted(claims, key=_claim_sort_key)),
        attempts=tuple(sorted(attempts, key=_attempt_sort_key)),
        resource_claims=tuple(
            sorted(
                (_model_copy(claim) for claim in raw_claims),
                key=_claim_sort_key,
            )
        ),
        capacity_by_pool=capacities,
        reservations=tuple(
            sorted(reservations, key=lambda item: str(getattr(item, "reservation_id", "")))
        ),
        pending_reservations=tuple(
            sorted(
                pending_reservations,
                key=lambda item: str(getattr(item, "reservation_id", "")),
            )
        ),
        resource_available=True,
    )


def _build_cognition_state(
    claims: tuple[Any, ...],
    attempts: tuple[Any, ...],
    *,
    read_only: bool,
) -> AgentCognitionState:
    if read_only:
        return AgentCognitionState(
            available=False,
            reason="scheduler durable state is not loaded by read-only AgentOS",
        )
    latest_by_claim: dict[str, Any] = {}
    for attempt in attempts:
        claim_id = str(getattr(attempt, "claim_id", ""))
        previous = latest_by_claim.get(claim_id)
        if previous is None or _attempt_sort_key(previous) < _attempt_sort_key(attempt):
            latest_by_claim[claim_id] = attempt

    current: list[CognitionAttemptState] = []
    for claim in claims:
        claim_state = _enum_value(getattr(claim, "state", ""))
        if claim_state not in _CURRENT_CLAIM_STATES:
            continue
        claim_id = str(getattr(claim, "claim_id", ""))
        attempt = latest_by_claim.get(claim_id)
        current.append(_cognition_attempt(claim, attempt))
    return AgentCognitionState(
        available=True,
        current_attempts=tuple(sorted(current, key=lambda item: (item.task_id, item.claim_id))),
    )


def _cognition_attempt(claim: Any, attempt: Any | None) -> CognitionAttemptState:
    unavailable: list[UnavailableField] = []
    if attempt is None:
        claim_graph_version = _optional_non_negative_int(getattr(claim, "graph_version", None))
        for name in (
            "attempt_id",
            "attempt_state",
            "semantic_epoch",
            "provenance_digest",
            "context_snapshot_id",
            "read_set",
            "write_set",
            "progress",
            "cost",
        ):
            unavailable.append(
                UnavailableField(name=name, reason="no Scheduler attempt exists for this claim")
            )
        if claim_graph_version is None:
            unavailable.append(
                UnavailableField(
                    name="graph_version",
                    reason="Scheduler claim has no graph version",
                )
            )
        return CognitionAttemptState(
            claim_id=str(getattr(claim, "claim_id", "")),
            claim_state=_enum_value(getattr(claim, "state", "")),
            task_id=str(getattr(claim, "task_id", "")),
            agent_id=str(getattr(claim, "agent_id", "")),
            process_id=str(getattr(claim, "process_id", "")),
            graph_version=claim_graph_version,
            unavailable=tuple(sorted(unavailable, key=lambda item: item.name)),
        )

    provenance_digest = getattr(attempt, "provenance_digest", None)
    if provenance_digest is None:
        unavailable.append(
            UnavailableField(name="provenance_digest", reason="provenance is not bound")
        )
    context_identity = getattr(getattr(attempt, "agent_snapshot", None), "context_identity", None)
    context_snapshot_id = getattr(attempt, "context_snapshot_id", None) or getattr(
        context_identity, "snapshot_id", None
    )
    if context_snapshot_id is None:
        unavailable.append(
            UnavailableField(name="context_snapshot_id", reason="Context VM snapshot is not bound")
        )

    snapshot = getattr(attempt, "agent_snapshot", None)
    read_set: tuple[ResourceBindingState, ...] = ()
    write_set: tuple[ResourceBindingState, ...] = ()
    progress: float | None = None
    cost: ComputationCostState | None = None
    if snapshot is None:
        for name in ("read_set", "write_set", "progress", "cost"):
            unavailable.append(UnavailableField(name=name, reason="AgentSnapshot is not captured"))
    else:
        raw_read_set: Any = getattr(snapshot, "read_set", _MISSING)
        if raw_read_set is _MISSING:
            unavailable.append(
                UnavailableField(
                    name="read_set",
                    reason="AgentSnapshot does not expose a read-set field",
                )
            )
        else:
            read_set = tuple(
                sorted(
                    (_binding_state(item) for item in (raw_read_set or ())),
                    key=_binding_sort_key,
                )
            )
        raw_write_set: Any = getattr(snapshot, "write_set", _MISSING)
        if raw_write_set is _MISSING:
            unavailable.append(
                UnavailableField(
                    name="write_set",
                    reason="AgentSnapshot does not expose a write-set field",
                )
            )
        else:
            write_set = tuple(
                sorted(
                    (_binding_state(item) for item in (raw_write_set or ())),
                    key=_binding_sort_key,
                )
            )
        raw_progress: Any = getattr(snapshot, "progress", _MISSING)
        if raw_progress is _MISSING or raw_progress is None:
            unavailable.append(
                UnavailableField(name="progress", reason="AgentSnapshot has no progress estimate")
            )
        else:
            progress = float(raw_progress)
        raw_cost: Any = getattr(snapshot, "cost", _MISSING)
        if raw_cost is _MISSING or raw_cost is None:
            unavailable.append(
                UnavailableField(name="cost", reason="AgentSnapshot has no cost observation")
            )
        else:
            cost = _cost_state(raw_cost)

    raw_graph_version = getattr(attempt, "graph_version", _MISSING)
    graph_version = _optional_non_negative_int(raw_graph_version)
    if graph_version is None:
        unavailable.append(
            UnavailableField(
                name="graph_version",
                reason="Scheduler claim/attempt has no graph version",
            )
        )
    raw_semantic_epoch = getattr(attempt, "semantic_epoch", _MISSING)
    semantic_epoch = _optional_non_negative_int(raw_semantic_epoch)
    if semantic_epoch is None:
        unavailable.append(
            UnavailableField(
                name="semantic_epoch",
                reason="Scheduler attempt has no semantic epoch",
            )
        )

    return CognitionAttemptState(
        claim_id=str(getattr(claim, "claim_id", "")),
        claim_state=_enum_value(getattr(claim, "state", "")),
        task_id=str(getattr(claim, "task_id", "")),
        agent_id=str(getattr(claim, "agent_id", "")),
        process_id=str(getattr(claim, "process_id", "")),
        graph_version=graph_version,
        attempt_id=str(getattr(attempt, "attempt_id", "")) or None,
        attempt_state=_enum_value(getattr(attempt, "state", "")) or None,
        semantic_epoch=semantic_epoch,
        provenance_digest=None if provenance_digest is None else str(provenance_digest),
        context_snapshot_id=(None if context_snapshot_id is None else str(context_snapshot_id)),
        read_set=read_set,
        write_set=write_set,
        progress=progress,
        cost=cost,
        unavailable=tuple(sorted(unavailable, key=lambda item: item.name)),
    )


def _build_context_state(
    current_attempts: tuple[CognitionAttemptState, ...],
    raw_attempts: tuple[Any, ...],
    *,
    context_service: Any | None,
) -> ContextRuntimeState:
    raw_by_id = {
        str(getattr(attempt, "attempt_id", "")): attempt
        for attempt in raw_attempts
        if getattr(attempt, "attempt_id", None)
    }
    bindings: list[ContextBindingState] = []
    incomplete: list[UnavailableField] = []
    for cognition in current_attempts:
        if cognition.attempt_id is None:
            continue
        attempt = raw_by_id.get(cognition.attempt_id)
        if attempt is None:
            continue
        # A cognition record without a sealed snapshot is a normal
        # pre-Context-VM attempt.  Do not reinterpret unrelated legacy
        # manifest fields as a malformed identity; the cognition layer already
        # reports ``context_snapshot_id`` as unavailable.
        if cognition.context_snapshot_id is None:
            continue
        identity = getattr(getattr(attempt, "agent_snapshot", None), "context_identity", None)
        manifest_id = getattr(attempt, "context_manifest_id", None) or getattr(
            identity, "manifest_id", None
        )
        manifest_hash = getattr(attempt, "context_manifest_hash", None) or getattr(
            identity, "manifest_hash", None
        )
        working_set_hash = getattr(attempt, "context_working_set_hash", None) or getattr(
            identity, "working_set_hash", None
        )
        materialized_hash = getattr(attempt, "context_materialized_hash", None) or getattr(
            identity, "materialized_hash", None
        )
        required = {
            "snapshot_id": cognition.context_snapshot_id,
            "manifest_id": manifest_id,
            "manifest_hash": manifest_hash,
            "working_set_hash": working_set_hash,
            "materialized_hash": materialized_hash,
        }
        if not any(value is not None for value in required.values()):
            # No context identity was bound at all.  This is a normal
            # pre-Context-VM attempt, not a malformed partial identity.
            continue
        if not all(required.values()):
            missing = tuple(name for name, value in required.items() if not value)
            incomplete.append(
                UnavailableField(
                    name=f"{cognition.attempt_id}.context_identity",
                    reason=(
                        "ContextSnapshot identity is incomplete; missing " + ", ".join(missing)
                    ),
                )
            )
            continue
        snapshot_id = cognition.context_snapshot_id
        if snapshot_id is None:
            # ``required`` was checked above; retain an explicit guard for
            # static type narrowing and fail-closed behavior if a legacy
            # object changes between reads.
            incomplete.append(
                UnavailableField(
                    name=f"{cognition.attempt_id}.context_identity",
                    reason="ContextSnapshot identity disappeared during projection",
                )
            )
            continue
        summary, summary_reason = _context_snapshot_summary(
            context_service,
            snapshot_id,
        )
        unavailable: tuple[UnavailableField, ...] = ()
        if summary is None:
            unavailable = (
                UnavailableField(
                    name="summary",
                    reason=summary_reason
                    or "Context VM exposes no public snapshot lookup for this binding",
                ),
            )
        bindings.append(
            ContextBindingState(
                attempt_id=cognition.attempt_id,
                claim_id=cognition.claim_id,
                task_id=cognition.task_id,
                agent_id=cognition.agent_id,
                snapshot_id=snapshot_id,
                manifest_id=str(manifest_id),
                manifest_hash=str(manifest_hash),
                working_set_hash=str(working_set_hash),
                materialized_hash=str(materialized_hash),
                summary=summary,
                unavailable=unavailable,
            )
        )
    if bindings:
        return ContextRuntimeState(
            available=not incomplete,
            reason=(
                None
                if not incomplete
                else "one or more current ContextSnapshot bindings are incomplete"
            ),
            bindings=tuple(sorted(bindings, key=lambda item: (item.task_id, item.attempt_id))),
            unavailable=tuple(incomplete),
        )
    if incomplete:
        reason = "one or more current ContextSnapshot bindings are incomplete"
        return ContextRuntimeState(
            available=False,
            reason=reason,
            unavailable=tuple(incomplete),
        )
    reason = (
        "Context VM is not wired"
        if context_service is None
        else "no current Scheduler attempt has a bound ContextSnapshot"
    )
    return ContextRuntimeState(
        available=False,
        reason=reason,
        unavailable=(UnavailableField(name="bindings", reason=reason),),
    )


def _context_snapshot_summary(
    context_service: Any | None,
    snapshot_id: str,
) -> tuple[ContextSnapshotSummary | None, str | None]:
    if context_service is None:
        return None, "Context VM is not wired"
    lookup = getattr(context_service, "get_snapshot", None)
    if not callable(lookup):
        return None, "Context VM exposes no public snapshot lookup for this binding"
    try:
        snapshot = lookup(snapshot_id)
    except Exception as exc:
        return None, f"Context VM snapshot lookup failed: {type(exc).__name__}"
    if snapshot is None:
        return None, f"Context VM snapshot {snapshot_id!r} is unavailable"
    try:
        return (
            ContextSnapshotSummary(
                page_binding_count=len(getattr(snapshot, "page_bindings", ())),
                tokens_used=int(getattr(snapshot, "tokens_used", 0)),
                bytes_used=int(getattr(snapshot, "bytes_used", 0)),
            ),
            None,
        )
    except Exception as exc:
        return None, f"Context VM snapshot summary is malformed: {type(exc).__name__}"


def _build_resource_state(
    snapshot: _SchedulerSnapshot,
    *,
    read_only: bool,
) -> ResourceRuntimeState:
    if read_only:
        return ResourceRuntimeState(
            available=False,
            reason="scheduler durable state is not loaded by read-only AgentOS",
        )
    if not snapshot.resource_available:
        return ResourceRuntimeState(
            available=False,
            reason=snapshot.resource_reason or "Scheduler logical resources are unavailable",
        )
    capacities = dict(snapshot.capacity_by_pool)
    reservations_by_pool: dict[str, list[Any]] = {}
    for reservation in snapshot.reservations:
        reservations_by_pool.setdefault(str(getattr(reservation, "pool_id", "")), []).append(
            reservation
        )
    pending_by_pool: dict[str, list[Any]] = {}
    for reservation in snapshot.pending_reservations:
        pending_by_pool.setdefault(str(getattr(reservation, "pool_id", "")), []).append(reservation)
    active_claims_raw = [
        claim
        for claim in snapshot.resource_claims
        if _enum_value(getattr(claim, "state", "")) == "active"
    ]
    claims_by_id = {
        str(getattr(claim, "claim_id", "")): claim
        for claim in snapshot.resource_claims
        if getattr(claim, "claim_id", None)
    }
    pool_ids = set(capacities) | set(reservations_by_pool) | set(pending_by_pool)
    pool_ids.update(str(getattr(claim, "agent_id", "")) for claim in active_claims_raw)
    pool_ids.discard("")

    pools: list[ResourcePoolState] = []
    for pool_id in sorted(pool_ids):
        pool_reservations = reservations_by_pool.get(pool_id, [])
        pool_pending = pending_by_pool.get(pool_id, [])
        pending_ids = {
            str(getattr(reservation, "reservation_id", "")) for reservation in pool_pending
        }
        pending_ids.update(
            str(getattr(reservation, "reservation_id", ""))
            for reservation in pool_reservations
            if _enum_value(
                getattr(
                    claims_by_id.get(str(getattr(reservation, "owner_id", ""))),
                    "state",
                    "pending",
                )
            )
            != "active"
        )
        reserved = _sum_vectors(
            getattr(reservation, "resources", None) for reservation in pool_reservations
        )
        capacity_raw = capacities.get(pool_id)
        capacity = _vector_state(capacity_raw) if capacity_raw is not None else None
        unavailable: tuple[UnavailableField, ...] = ()
        available: ResourceVectorState | None = None
        if capacity is None:
            unavailable = (
                UnavailableField(
                    name="capacity",
                    reason="Scheduler has no declared capacity for this resource pool",
                ),
                UnavailableField(
                    name="available",
                    reason="available resources require a declared capacity",
                ),
            )
        elif shortages := _vector_shortages(reserved, capacity):
            unavailable = (
                UnavailableField(
                    name="available",
                    reason=(
                        "active reservations exceed declared capacity: "
                        + ", ".join(
                            f"{name}={quantity}" for name, quantity in sorted(shortages.items())
                        )
                    ),
                ),
            )
        else:
            available = _subtract_vectors(capacity, reserved)
        pools.append(
            ResourcePoolState(
                pool_id=pool_id,
                capacity=capacity,
                reserved=reserved,
                available=available,
                reservation_ids=tuple(
                    sorted(
                        str(getattr(reservation, "reservation_id", ""))
                        for reservation in pool_reservations
                    )
                ),
                pending_reservation_ids=tuple(sorted(pending_ids)),
                active_claim_ids=tuple(
                    sorted(
                        str(getattr(claim, "claim_id", ""))
                        for claim in active_claims_raw
                        if str(getattr(claim, "agent_id", "")) == pool_id
                    )
                ),
                unavailable=unavailable,
            )
        )
    active_claims = tuple(
        ActiveClaimResourceState(
            claim_id=str(getattr(claim, "claim_id", "")),
            task_id=str(getattr(claim, "task_id", "")),
            agent_id=str(getattr(claim, "agent_id", "")),
            reservation_id=(
                None
                if getattr(claim, "resource_reservation_id", None) is None
                else str(claim.resource_reservation_id)
            ),
            reserved=_vector_state(getattr(claim, "reserved_resources", None)),
        )
        for claim in sorted(active_claims_raw, key=_claim_sort_key)
    )
    projection_available = (
        not any(pool.unavailable for pool in pools) and not snapshot.pending_reservations
    )
    projection_reason = (
        None
        if projection_available
        else (
            "durable resource reservations are pending capacity restoration"
            if snapshot.pending_reservations
            else "one or more Scheduler resource pools are inconsistent or unavailable"
        )
    )
    return ResourceRuntimeState(
        available=projection_available,
        reason=projection_reason,
        pools=tuple(pools),
        active_claims=active_claims,
    )


def _build_recent_event_state(events: list[Any]) -> tuple[RecentRuntimeEventState, ...]:
    """Redact and normalize a bounded event tail deterministically."""

    normalized: list[RecentRuntimeEventState] = []
    for event in events[-RUNTIME_RECENT_EVENT_LIMIT:]:
        payload = getattr(event, "payload", {}) or {}
        canonical = json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        normalized.append(
            RecentRuntimeEventState(
                event_id=str(getattr(event, "event_id", "")),
                event_type=_enum_value(getattr(event, "event_type", "")),
                graph_version=_optional_non_negative_int(getattr(event, "graph_version", None)),
                causation_patch_id=_optional_str(getattr(event, "causation_patch_id", None)),
                subject_id=_optional_str(getattr(event, "subject_id", None)),
                node_id=_optional_str(getattr(event, "node_id", None)),
                payload_hash=hashlib.sha256(canonical).hexdigest(),
                payload_size_bytes=len(canonical),
                recorded_at=event.recorded_at,
            )
        )
    return tuple(normalized)


def _vector_state(vector: Any | None) -> ResourceVectorState:
    if vector is None:
        return ResourceVectorState()
    raw_slots = getattr(vector, "model_slots", {}) or {}
    return ResourceVectorState(
        cpu_millis=int(getattr(vector, "cpu_millis", 0)),
        ram_bytes=int(getattr(vector, "ram_bytes", 0)),
        gpu_count=int(getattr(vector, "gpu_count", 0)),
        vram_bytes=int(getattr(vector, "vram_bytes", 0)),
        model_slots=tuple(
            ModelSlotState(name=str(name), quantity=int(quantity))
            for name, quantity in sorted(raw_slots.items())
            if int(quantity) != 0
        ),
    )


def _sum_vectors(vectors: Any) -> ResourceVectorState:
    cpu_millis = 0
    ram_bytes = 0
    gpu_count = 0
    vram_bytes = 0
    slots: dict[str, int] = {}
    for vector in vectors:
        if vector is None:
            continue
        cpu_millis += int(getattr(vector, "cpu_millis", 0))
        ram_bytes += int(getattr(vector, "ram_bytes", 0))
        gpu_count += int(getattr(vector, "gpu_count", 0))
        vram_bytes += int(getattr(vector, "vram_bytes", 0))
        for name, quantity in (getattr(vector, "model_slots", {}) or {}).items():
            slots[str(name)] = slots.get(str(name), 0) + int(quantity)
    return ResourceVectorState(
        cpu_millis=cpu_millis,
        ram_bytes=ram_bytes,
        gpu_count=gpu_count,
        vram_bytes=vram_bytes,
        model_slots=tuple(
            ModelSlotState(name=name, quantity=quantity)
            for name, quantity in sorted(slots.items())
            if quantity
        ),
    )


def _subtract_vectors(
    capacity: ResourceVectorState,
    reserved: ResourceVectorState,
) -> ResourceVectorState:
    capacity_slots = {item.name: item.quantity for item in capacity.model_slots}
    reserved_slots = {item.name: item.quantity for item in reserved.model_slots}
    return ResourceVectorState(
        cpu_millis=capacity.cpu_millis - reserved.cpu_millis,
        ram_bytes=capacity.ram_bytes - reserved.ram_bytes,
        gpu_count=capacity.gpu_count - reserved.gpu_count,
        vram_bytes=capacity.vram_bytes - reserved.vram_bytes,
        model_slots=tuple(
            ModelSlotState(
                name=name,
                quantity=capacity_slots.get(name, 0) - reserved_slots.get(name, 0),
            )
            for name in sorted(set(capacity_slots) | set(reserved_slots))
            if capacity_slots.get(name, 0) - reserved_slots.get(name, 0) > 0
        ),
    )


def _vector_shortages(
    reserved: ResourceVectorState,
    capacity: ResourceVectorState,
) -> dict[str, int]:
    shortages: dict[str, int] = {}
    for name in ("cpu_millis", "ram_bytes", "gpu_count", "vram_bytes"):
        missing = getattr(reserved, name) - getattr(capacity, name)
        if missing > 0:
            shortages[name] = missing
    capacity_slots = {item.name: item.quantity for item in capacity.model_slots}
    for item in reserved.model_slots:
        missing = item.quantity - capacity_slots.get(item.name, 0)
        if missing > 0:
            shortages[f"model_slots.{item.name}"] = missing
    return shortages


def _binding_state(binding: Any) -> ResourceBindingState:
    return ResourceBindingState(
        operation=str(getattr(binding, "operation", "")),
        resource_uri=str(getattr(binding, "resource_uri", "")),
        artifact_id=_optional_str(getattr(binding, "artifact_id", None)),
        version=(None if getattr(binding, "version", None) is None else int(binding.version)),
        content_hash=_optional_str(getattr(binding, "content_hash", None)),
        action_id=_optional_str(getattr(binding, "action_id", None)),
        idempotency_key=_optional_str(getattr(binding, "idempotency_key", None)),
        source_event_id=_optional_str(getattr(binding, "source_event_id", None)),
        source=str(getattr(binding, "source", "")),
        known=bool(getattr(binding, "known", True)),
        observed_at=getattr(binding, "observed_at", None),
    )


def _cost_state(cost: Any) -> ComputationCostState:
    return ComputationCostState(
        input_tokens=int(getattr(cost, "input_tokens", 0)),
        output_tokens=int(getattr(cost, "output_tokens", 0)),
        cached_input_tokens=int(getattr(cost, "cached_input_tokens", 0)),
        model_calls=int(getattr(cost, "model_calls", 0)),
        tool_calls=int(getattr(cost, "tool_calls", 0)),
        elapsed_ms=int(getattr(cost, "elapsed_ms", 0)),
        monetary_micros=int(getattr(cost, "monetary_micros", 0)),
    )


def _claim_sort_key(claim: Any) -> tuple[str, str, str]:
    return (
        str(getattr(claim, "task_id", "")),
        str(getattr(claim, "claim_id", "")),
        _enum_value(getattr(claim, "state", "")),
    )


def _attempt_sort_key(attempt: Any) -> tuple[str, int, str]:
    return (
        str(getattr(attempt, "claim_id", "")),
        int(getattr(attempt, "attempt_number", 0)),
        str(getattr(attempt, "attempt_id", "")),
    )


def _binding_sort_key(binding: ResourceBindingState) -> tuple[str, str, str, int]:
    return (
        binding.operation,
        binding.resource_uri,
        binding.artifact_id or "",
        binding.version or 0,
    )


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value))


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _optional_non_negative_int(value: Any) -> int | None:
    if value is _MISSING or value is None:
        return None
    parsed = int(value)
    return parsed if parsed >= 0 else None


def _model_copy(value: Any) -> Any:
    copy = getattr(value, "model_copy", None)
    return copy(deep=True) if callable(copy) else value


__all__ = [
    "RUNTIME_RECENT_EVENT_LIMIT",
    "RUNTIME_STATE_SCHEMA_VERSION",
    "ActiveClaimResourceState",
    "AgentCognitionState",
    "CognitionAttemptState",
    "ComputationCostState",
    "ContextBindingState",
    "ContextRuntimeState",
    "ContextSnapshotSummary",
    "GlobalRuntimeState",
    "ModelSlotState",
    "ProgressSemanticState",
    "RecentRuntimeEventState",
    "ResourceBindingState",
    "ResourcePoolState",
    "ResourceRuntimeState",
    "ResourceVectorState",
    "RuntimeStateView",
    "TaskUnlockValueState",
    "UnavailableField",
    "build_runtime_state_view",
]
