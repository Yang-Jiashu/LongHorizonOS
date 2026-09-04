"""Resource-aware, conflict-aware parallelism suggestions.

This module is an opt-in planning primitive.  It combines three *explicit*
inputs:

* the immutable :class:`~lhos.sdk.runtime_state.GlobalRuntimeState` resource
  projection;
* task resource requests supplied by the caller; and
* an explicit :class:`~lhos.sdk.conflict_graph.ConflictGraph`.

The policy emits a deterministic greedy batch.  It never claims work,
acquires a lease, mutates the Scheduler, or treats host telemetry as a
resource-admission result.  Missing/unknown capacity and requests are
fail-closed: the affected task is deferred and an ``UnavailableField`` is
included in the result.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from contextlib import suppress
from typing import Any, Final, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    field_validator,
    model_validator,
)

from lhos.runtimes.multi_agent.models import ResourceVector

from .conflict_graph import ConflictGraph, TaskAccessSet
from .frontier_policy import FrontierAction
from .runtime_state import (
    GlobalRuntimeState,
    ModelSlotState,
    ResourceVectorState,
    UnavailableField,
)

RESOURCE_AWARE_PARALLELISM_SCHEMA_VERSION: Final[Literal["resource-aware-parallelism.v1"]] = (
    "resource-aware-parallelism.v1"
)
RESOURCE_AWARE_PARALLELISM_POLICY_ID: Final[str] = "resource-aware-conflict-greedy.v1"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ResourceTaskRequest(_FrozenModel):
    """Explicit resource request for one candidate task.

    ``pool_id`` is optional.  When omitted, the policy deterministically
    chooses the first known pool (lexical order) whose remaining vector fits.
    A request marked ``known=False`` is never selected.
    """

    task_id: str = Field(min_length=1)
    resources: ResourceVectorState
    pool_id: str | None = None
    known: StrictBool = True

    @model_validator(mode="before")
    @classmethod
    def _coerce_resource_vector(cls, value: Any) -> Any:
        """Accept the same vector shorthands as :meth:`from_value`.

        The public SDK frequently passes the core ``ResourceVector`` model
        directly.  Pydantic would otherwise reject it before our planning
        policy can apply its fail-closed normalization.
        """

        if not isinstance(value, Mapping):
            return value
        raw = dict(value)
        resources = raw.get("resources")
        if resources is not None:
            with suppress(Exception):
                raw["resources"] = _coerce_vector_state(resources)
            # A malformed value remains untouched so normal validation reports
            # an error for direct construction; ``from_value`` uses the
            # fail-closed ``known=False`` path for untrusted inputs.
        return raw

    @field_validator("task_id")
    @classmethod
    def _normalize_task_id(cls, value: str) -> str:
        value = str(value).strip()
        if not value:
            raise ValueError("task_id must be non-empty")
        return value

    @field_validator("pool_id")
    @classmethod
    def _normalize_pool_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @classmethod
    def from_value(cls, task_id: str, value: Any) -> ResourceTaskRequest:
        """Normalize common SDK forms without guessing unknown values.

        Accepted values are another ``ResourceTaskRequest``, a
        ``ResourceVector``/``ResourceVectorState``, or a mapping containing
        ``resources`` (or ``request``), optional ``pool_id`` and ``known``.
        A malformed value is represented as ``known=False`` rather than
        silently coerced into a zero request.
        """

        normalized_id = str(task_id).strip()
        if isinstance(value, cls):
            if value.task_id == normalized_id:
                return value
            return cls(
                task_id=normalized_id,
                resources=value.resources,
                pool_id=value.pool_id,
                known=value.known,
            )
        if isinstance(value, Mapping):
            raw = dict(value)
            # A plain vector dictionary is also a supported shorthand.
            if "resources" in raw or "request" in raw:
                raw_resources = raw.pop("resources", raw.pop("request", None))
                pool_id = raw.pop("pool_id", None)
                known = raw.pop("known", True)
                if raw:
                    return cls(
                        task_id=normalized_id,
                        resources=ResourceVectorState(),
                        pool_id=pool_id,
                        known=False,
                    )
            else:
                raw_resources = raw
                pool_id = None
                known = True
            try:
                vector = _coerce_vector_state(raw_resources)
                return cls(
                    task_id=normalized_id,
                    resources=vector,
                    pool_id=pool_id,
                    known=known,
                )
            except Exception:
                return cls(
                    task_id=normalized_id,
                    resources=ResourceVectorState(),
                    pool_id=pool_id,
                    known=False,
                )
        try:
            return cls(
                task_id=normalized_id,
                resources=_coerce_vector_state(value),
                known=True,
            )
        except Exception:
            return cls(
                task_id=normalized_id,
                resources=ResourceVectorState(),
                known=False,
            )


# Friendly aliases used by callers that prefer the shorter names.
TaskResourceRequest = ResourceTaskRequest
ResourceRequest = ResourceTaskRequest


class ResourceTaskAssignment(_FrozenModel):
    """The pool and vector selected for one task in a proposed batch."""

    task_id: str = Field(min_length=1)
    pool_id: str = Field(min_length=1)
    resources: ResourceVectorState


class ResourceTaskDecision(_FrozenModel):
    """One deterministic task decision and its resource/conflict blockers."""

    task_id: str = Field(min_length=1)
    action: FrontierAction
    reason: str = Field(min_length=1)
    blockers: tuple[str, ...] = ()
    requested: ResourceVectorState | None = None
    pool_id: str | None = None


class ResourceAwareBatchSuggestion(_FrozenModel):
    """Immutable output of one resource-aware planning pass."""

    schema_version: Literal["resource-aware-parallelism.v1"] = (
        RESOURCE_AWARE_PARALLELISM_SCHEMA_VERSION
    )
    epoch_id: StrictInt = Field(ge=0)
    policy_id: str = Field(min_length=1)
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    projection_hash: str = Field(min_length=1)
    conflict_graph_hash: str = Field(min_length=64, max_length=64)
    candidate_task_ids: tuple[str, ...] = ()
    selected_task_ids: tuple[str, ...] = ()
    deferred_task_ids: tuple[str, ...] = ()
    assignments: tuple[ResourceTaskAssignment, ...] = ()
    decisions: tuple[ResourceTaskDecision, ...] = ()
    parallelism_hint: StrictInt = Field(ge=0)
    safe_under_declared_resources: StrictBool = False
    safe_under_constraints: StrictBool = False
    unavailable: tuple[UnavailableField, ...] = ()
    decision_hash: str = Field(min_length=64, max_length=64)

    @property
    def resource_assignments(self) -> tuple[ResourceTaskAssignment, ...]:
        """Compatibility/readability alias for ``assignments``."""

        return self.assignments

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


# A second descriptive alias keeps the public surface discoverable.
ResourceBatchSuggestion = ResourceAwareBatchSuggestion


class ResourceAwareParallelismPolicy(_FrozenModel):
    """Deterministic greedy batch policy with logical resource fitting.

    ``max_parallelism`` is a strict upper bound.  Resource requests are
    supplied to :meth:`suggest` because they belong to the current graph
    projection, not to the policy itself.  The existing Scheduler remains the
    authority and must revalidate all conditions before dispatch.
    """

    max_parallelism: StrictInt = Field(default=1, ge=1)
    policy_id: str = RESOURCE_AWARE_PARALLELISM_POLICY_ID

    @field_validator("max_parallelism")
    @classmethod
    def _require_parallelism_int(cls, value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("max_parallelism must be an integer")
        return value

    @field_validator("policy_id")
    @classmethod
    def _non_empty_policy_id(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("policy_id must be non-empty")
        return str(value).strip()

    def suggest(
        self,
        state: GlobalRuntimeState,
        conflict_graph: ConflictGraph,
        task_resources: Mapping[str, Any] | Iterable[ResourceTaskRequest] | None = None,
        *,
        requests: Mapping[str, Any] | Iterable[ResourceTaskRequest] | None = None,
        epoch_id: int = 0,
    ) -> ResourceAwareBatchSuggestion:
        """Return a deterministic resource/conflict-aware batch.

        ``requests`` is an alias for ``task_resources``.  Supplying both is
        rejected to avoid silently choosing one declaration over another.
        """

        if not isinstance(state, GlobalRuntimeState):
            raise TypeError("state must be a GlobalRuntimeState/RuntimeStateView")
        if not isinstance(conflict_graph, ConflictGraph):
            raise TypeError("conflict_graph must be a ConflictGraph")
        if task_resources is not None and requests is not None:
            raise ValueError("provide only one of task_resources or requests")
        raw_requests = task_resources if task_resources is not None else requests
        normalized_requests, request_errors = _normalize_requests(raw_requests)
        if isinstance(epoch_id, bool) or not isinstance(epoch_id, int) or epoch_id < 0:
            raise ValueError("epoch_id must be a non-negative integer")

        repair = _normalize_ids(state.progress.repair_ready_frontier)
        repair_set = set(repair)
        ready = _normalize_ids(state.progress.ready_frontier)
        candidates = tuple(
            sorted(
                set(ready) | repair_set,
                key=lambda task_id: (
                    0 if task_id in repair_set else 1,
                    0 if conflict_graph.access_for(task_id) is not None else 1,
                    task_id,
                ),
            )
        )

        unavailable: list[UnavailableField] = list(request_errors)

        # Validate every frontier candidate up front.  A missing declaration
        # must not become "safe" merely because an earlier lexical candidate
        # consumed the parallelism bound and short-circuited the per-task
        # admission path.
        unknown_request_ids = {
            task_id
            for task_id in candidates
            if task_id not in normalized_requests or not normalized_requests[task_id].known
        }
        for task_id in sorted(unknown_request_ids):
            unavailable.append(
                UnavailableField(
                    name=f"task_resources.{task_id}",
                    reason=(
                        "task has no explicit resource request"
                        if task_id not in normalized_requests
                        else "task resource request is unknown or malformed"
                    ),
                )
            )

        resources_available = bool(state.resources.available)
        cognition_available = bool(state.agent_cognition.available)
        closed = bool(state.progress.graph_closed or state.progress.goal_closed)
        pools, pool_errors = _available_pools(state)
        unavailable.extend(pool_errors)
        if not resources_available:
            unavailable.append(
                UnavailableField(
                    name="resources",
                    reason=state.resources.reason or "logical resources unavailable",
                )
            )
        if not cognition_available:
            unavailable.append(
                UnavailableField(
                    name="agent_cognition",
                    reason=state.agent_cognition.reason or "agent cognition unavailable",
                )
            )

        active_accesses, active_errors = _active_access_views(
            state.agent_cognition.current_attempts
        )
        unavailable.extend(active_errors)
        active_tasks = {
            str(attempt.task_id)
            for attempt in state.agent_cognition.current_attempts
            if str(getattr(attempt, "task_id", "") or "").strip()
        }

        selected: list[str] = []
        deferred: list[str] = []
        assignments: list[ResourceTaskAssignment] = []
        decisions: list[ResourceTaskDecision] = []
        remaining = {pool_id: vector for pool_id, vector in pools.items()}
        unsafe_access = False

        for task_id in candidates:
            request = normalized_requests.get(task_id)
            access = conflict_graph.access_for(task_id)
            action = FrontierAction.DEFER
            reason = "ready"
            blockers: tuple[str, ...] = ()
            chosen_pool: str | None = None
            requested_state = request.resources if request is not None else None

            if closed:
                reason = "closed"
            elif not resources_available:
                reason = "resources_unavailable"
            elif not cognition_available:
                reason = "cognition_unavailable"
            elif task_id in active_tasks:
                reason = "active_attempt"
            elif task_id in set(_normalize_ids(state.progress.verified_task_ids)) or task_id in set(
                _normalize_ids(state.progress.invalid_task_ids)
            ):
                reason = "terminal_validity"
            elif (
                task_id in set(_normalize_ids(state.progress.stale_task_ids))
                and task_id not in repair_set
            ):
                reason = "stale_not_repair_ready"
            elif request is None:
                reason = "resource_request_unknown"
                unavailable.append(
                    UnavailableField(
                        name=f"task_resources.{task_id}",
                        reason="task has no explicit resource request",
                    )
                )
            elif not request.known:
                reason = "resource_request_unknown"
                unavailable.append(
                    UnavailableField(
                        name=f"task_resources.{task_id}",
                        reason="task resource request is unknown or malformed",
                    )
                )
            elif active_errors:
                reason = "active_access_unknown"
                blockers = tuple(
                    f"active:{attempt_id}"
                    for attempt_id, _reads, _writes, _known in active_accesses
                    if not _known
                )
            elif access is None or not access.known:
                # Unknown access is serial-only, matching DynamicParallelismPolicy.
                if selected:
                    blockers = tuple(selected)
                    reason = "unknown_access_serial_only"
                elif active_accesses:
                    # An unknown candidate cannot be proven independent of an
                    # already-running attempt.  Even when every active
                    # read/write set is known, the candidate's hidden access
                    # set may overlap it.  Keep the proposal fail-closed and
                    # wait for the active occupancy to drain instead of
                    # accidentally running it concurrently.
                    blockers = tuple(
                        sorted(f"active:{attempt_id}" for attempt_id, *_ in active_accesses)
                    )
                    reason = "unknown_access_active_occupancy"
                else:
                    chosen_pool = _choose_pool(request, remaining, pools)
                    if chosen_pool is None:
                        reason, blockers = _resource_fit_reason(request, remaining, pools)
                    else:
                        selected.append(task_id)
                        action = FrontierAction.RUN
                        reason = "unknown_access_serial_only"
                        unsafe_access = True
            else:
                selected_blockers = tuple(
                    chosen for chosen in selected if conflict_graph.conflicts_with(task_id, chosen)
                )
                active_blockers = _active_conflict_blockers(access, active_accesses)
                blockers = tuple(sorted(set(selected_blockers + active_blockers)))
                if blockers:
                    reason = "active_conflict" if active_blockers else "conflict"
                elif len(selected) >= self.max_parallelism:
                    reason = "max_parallelism"
                else:
                    chosen_pool = _choose_pool(request, remaining, pools)
                    if chosen_pool is None:
                        reason, blockers = _resource_fit_reason(request, remaining, pools)
                    else:
                        selected.append(task_id)
                        action = FrontierAction.RUN
                        reason = "independent"

            if action is FrontierAction.RUN and chosen_pool is not None and request is not None:
                remaining[chosen_pool] = _vector_minus(remaining[chosen_pool], request.resources)
                assignments.append(
                    ResourceTaskAssignment(
                        task_id=task_id,
                        pool_id=chosen_pool,
                        resources=request.resources,
                    )
                )
            else:
                deferred.append(task_id)
            decisions.append(
                ResourceTaskDecision(
                    task_id=task_id,
                    action=action,
                    reason=reason,
                    blockers=tuple(blockers),
                    requested=requested_state,
                    pool_id=chosen_pool,
                )
            )

        # A candidate can only be considered safe if all selected requests and
        # their selected pools were authoritative.  Unknown access remains an
        # explicit warning even when resource fitting itself was safe.
        # A known request that is deferred because the current capacity is
        # insufficient is still a safe *observation*.  Unknown/malformed
        # requests, by contrast, make the whole proposal unsafe to advertise:
        # the caller cannot assume that omitted work has a valid contract.
        unknown_request_present = any(
            decision.reason == "resource_request_unknown" for decision in decisions
        )
        resource_safe = bool(
            resources_available
            and bool(pools)
            and not pool_errors
            and all(
                task_id in normalized_requests and normalized_requests[task_id].known
                for task_id in selected
            )
            and not unknown_request_present
        )
        # Conflict/max-parallelism/insufficient-capacity deferrals do not make
        # an otherwise authoritative selected batch unsafe.  Unknown access
        # and incomplete active snapshots still do.
        intrinsically_unschedulable = any(
            decision.reason == "insufficient_resources"
            and (
                (request := normalized_requests.get(decision.task_id)) is not None
                and not any(_fits(request.resources, pool_vector) for pool_vector in pools.values())
            )
            for decision in decisions
        )
        unknown_pool_or_capacity = any(
            decision.reason in {"resource_capacity_unknown", "resource_pool_unknown"}
            for decision in decisions
        )
        safe = bool(
            resource_safe
            and not unsafe_access
            and not active_errors
            and not pool_errors
            and resources_available
            and cognition_available
            and not intrinsically_unschedulable
            and not unknown_request_ids
            and not unknown_pool_or_capacity
        )
        parallelism_hint = (
            len(selected)
            if resources_available and cognition_available and bool(pools) and not pool_errors
            else 0
        )
        unavailable_tuple = _dedupe_unavailable(unavailable)
        payload = {
            "schema_version": RESOURCE_AWARE_PARALLELISM_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "policy_id": self.policy_id,
            "graph_id": state.graph_id,
            "graph_version": state.progress.graph_version,
            "projection_hash": state.progress.projection_hash,
            "conflict_graph_hash": conflict_graph.graph_hash,
            "candidate_task_ids": candidates,
            "selected_task_ids": tuple(selected),
            "deferred_task_ids": tuple(deferred),
            "assignments": tuple(assignments),
            "decisions": tuple(decisions),
            "parallelism_hint": parallelism_hint,
            "safe_under_declared_resources": resource_safe,
            "safe_under_constraints": safe,
            "unavailable": unavailable_tuple,
        }
        return ResourceAwareBatchSuggestion(
            epoch_id=epoch_id,
            policy_id=self.policy_id,
            graph_id=state.graph_id,
            graph_version=state.progress.graph_version,
            projection_hash=state.progress.projection_hash,
            conflict_graph_hash=conflict_graph.graph_hash,
            candidate_task_ids=candidates,
            selected_task_ids=tuple(selected),
            deferred_task_ids=tuple(deferred),
            assignments=tuple(assignments),
            decisions=tuple(decisions),
            parallelism_hint=parallelism_hint,
            safe_under_declared_resources=resource_safe,
            safe_under_constraints=safe,
            unavailable=unavailable_tuple,
            decision_hash=_hash_payload(payload),
        )

    plan = suggest


def suggest_resource_aware_batch(
    state: GlobalRuntimeState,
    conflict_graph: ConflictGraph,
    task_resources: Mapping[str, Any] | Iterable[ResourceTaskRequest] | None = None,
    *,
    requests: Mapping[str, Any] | Iterable[ResourceTaskRequest] | None = None,
    epoch_id: int = 0,
    max_parallelism: int = 1,
) -> ResourceAwareBatchSuggestion:
    """Convenience wrapper for one resource-aware planning pass."""

    return ResourceAwareParallelismPolicy(max_parallelism=max_parallelism).suggest(
        state,
        conflict_graph,
        task_resources,
        requests=requests,
        epoch_id=epoch_id,
    )


plan_resource_aware_batch = suggest_resource_aware_batch


def _normalize_requests(
    values: Mapping[str, Any] | Iterable[ResourceTaskRequest] | None,
) -> tuple[dict[str, ResourceTaskRequest], tuple[UnavailableField, ...]]:
    if values is None:
        return {}, ()
    items: list[tuple[str, Any]] = []
    if isinstance(values, Mapping):
        items = [(str(key).strip(), value) for key, value in values.items()]
    else:
        try:
            for value in values:
                if isinstance(value, ResourceTaskRequest):
                    items.append((value.task_id, value))
                elif isinstance(value, Mapping) and "task_id" in value:
                    item = dict(value)
                    items.append((str(item.pop("task_id")).strip(), item))
                else:
                    raise TypeError("iterable requests must contain ResourceTaskRequest values")
        except TypeError:
            return {}, (
                UnavailableField(
                    name="task_resources",
                    reason="task resource requests are not an iterable mapping",
                ),
            )
    result: dict[str, ResourceTaskRequest] = {}
    errors: list[UnavailableField] = []
    for task_id, value in items:
        if not task_id:
            errors.append(
                UnavailableField(name="task_resources", reason="empty task id in request map")
            )
            continue
        if task_id in result:
            errors.append(
                UnavailableField(
                    name=f"task_resources.{task_id}",
                    reason="duplicate task resource request",
                )
            )
            continue
        result[task_id] = ResourceTaskRequest.from_value(task_id, value)
    return result, tuple(errors)


def _available_pools(
    state: GlobalRuntimeState,
) -> tuple[dict[str, ResourceVectorState], tuple[UnavailableField, ...]]:
    pools: dict[str, ResourceVectorState] = {}
    errors: list[UnavailableField] = []
    for pool in sorted(state.resources.pools, key=lambda item: item.pool_id):
        pool_id = str(pool.pool_id).strip()
        if not pool_id:
            errors.append(UnavailableField(name="resources.pools", reason="pool id is empty"))
            continue
        if pool.available is None or pool.unavailable:
            errors.append(
                UnavailableField(
                    name=f"resources.pools.{pool_id}.available",
                    reason=(
                        "logical available vector is unknown"
                        if pool.available is None
                        else "; ".join(item.reason for item in pool.unavailable)
                    ),
                )
            )
            continue
        pools[pool_id] = pool.available
    if state.resources.available and not pools:
        errors.append(
            UnavailableField(
                name="resources.available_vector",
                reason="no authoritative logical resource pool is available",
            )
        )
    return pools, _dedupe_unavailable(errors)


def _choose_pool(
    request: ResourceTaskRequest,
    remaining: Mapping[str, ResourceVectorState],
    all_pools: Mapping[str, ResourceVectorState],
) -> str | None:
    candidates = (request.pool_id,) if request.pool_id is not None else tuple(sorted(remaining))
    for pool_id in candidates:
        if pool_id not in remaining:
            continue
        if _fits(request.resources, remaining[pool_id]):
            return pool_id
    return None


def _resource_fit_reason(
    request: ResourceTaskRequest,
    remaining: Mapping[str, ResourceVectorState],
    all_pools: Mapping[str, ResourceVectorState],
) -> tuple[str, tuple[str, ...]]:
    if request.pool_id is not None and request.pool_id not in all_pools:
        return "resource_pool_unknown", (f"pool:{request.pool_id}",)
    if not remaining:
        return "resource_capacity_unknown", ()
    blockers: list[str] = []
    pools = (request.pool_id,) if request.pool_id else tuple(sorted(remaining))
    for pool_id in pools:
        if pool_id in remaining:
            shortages = _shortages(request.resources, remaining[pool_id])
            blockers.extend(f"{pool_id}:{name}={amount}" for name, amount in shortages.items())
    return "insufficient_resources", tuple(sorted(set(blockers)))


def _fits(request: ResourceVectorState, capacity: ResourceVectorState) -> bool:
    return not _shortages(request, capacity)


def _shortages(
    request: ResourceVectorState,
    capacity: ResourceVectorState,
) -> dict[str, int]:
    shortages: dict[str, int] = {}
    for name in ("cpu_millis", "ram_bytes", "gpu_count", "vram_bytes"):
        missing = int(getattr(request, name)) - int(getattr(capacity, name))
        if missing > 0:
            shortages[name] = missing
    available_slots = {item.name: item.quantity for item in capacity.model_slots}
    for item in request.model_slots:
        missing = int(item.quantity) - int(available_slots.get(item.name, 0))
        if missing > 0:
            shortages[f"model_slots.{item.name}"] = missing
    return shortages


def _vector_minus(
    left: ResourceVectorState,
    right: ResourceVectorState,
) -> ResourceVectorState:
    left_slots = {item.name: item.quantity for item in left.model_slots}
    right_slots = {item.name: item.quantity for item in right.model_slots}
    return ResourceVectorState(
        cpu_millis=left.cpu_millis - right.cpu_millis,
        ram_bytes=left.ram_bytes - right.ram_bytes,
        gpu_count=left.gpu_count - right.gpu_count,
        vram_bytes=left.vram_bytes - right.vram_bytes,
        model_slots=tuple(
            ModelSlotState(name=name, quantity=quantity)
            for name, quantity in sorted(
                (name, left_slots.get(name, 0) - right_slots.get(name, 0))
                for name in set(left_slots) | set(right_slots)
            )
            if quantity
        ),
    )


def _coerce_vector_state(value: Any) -> ResourceVectorState:
    if isinstance(value, ResourceVectorState):
        return value
    if isinstance(value, ResourceVector):
        slots = tuple(
            ModelSlotState(name=str(name), quantity=int(quantity))
            for name, quantity in sorted(value.model_slots.items())
            if int(quantity)
        )
        return ResourceVectorState(
            cpu_millis=int(value.cpu_millis),
            ram_bytes=int(value.ram_bytes),
            gpu_count=int(value.gpu_count),
            vram_bytes=int(value.vram_bytes),
            model_slots=slots,
        )
    if isinstance(value, Mapping):
        raw = dict(value)
        allowed = {"cpu_millis", "ram_bytes", "gpu_count", "vram_bytes", "model_slots"}
        unknown = set(raw) - allowed
        if unknown:
            raise ValueError(
                "unknown resource vector fields: " + ", ".join(sorted(map(str, unknown)))
            )
        slots_raw = raw.get("model_slots", ())
        if isinstance(slots_raw, Mapping):
            slots_raw = tuple(
                ModelSlotState(name=str(name), quantity=int(quantity))
                for name, quantity in sorted(slots_raw.items())
                if int(quantity)
            )
        return ResourceVectorState(
            cpu_millis=raw.get("cpu_millis", 0),
            ram_bytes=raw.get("ram_bytes", 0),
            gpu_count=raw.get("gpu_count", 0),
            vram_bytes=raw.get("vram_bytes", 0),
            model_slots=slots_raw,
        )
    raise TypeError("resource request must be a ResourceVector or mapping")


def _active_access_views(
    attempts: Iterable[Any],
) -> tuple[
    tuple[tuple[str, tuple[str, ...], tuple[str, ...], bool], ...],
    tuple[UnavailableField, ...],
]:
    views: list[tuple[str, tuple[str, ...], tuple[str, ...], bool]] = []
    unavailable: list[UnavailableField] = []
    for attempt in attempts:
        attempt_id = str(
            getattr(attempt, "attempt_id", None)
            or getattr(attempt, "claim_id", None)
            or getattr(attempt, "task_id", "")
        )
        read_identities = {
            identity
            for binding in getattr(attempt, "read_set", ())
            if (identity := _binding_identity(binding)) is not None
        }
        write_identities = {
            identity
            for binding in getattr(attempt, "write_set", ())
            if (identity := _binding_identity(binding)) is not None
        }
        reads = tuple(sorted(read_identities))
        writes = tuple(sorted(write_identities))
        bindings = (
            *getattr(attempt, "read_set", ()),
            *getattr(attempt, "write_set", ()),
        )
        unavailable_names = {
            str(item.name)
            for item in getattr(attempt, "unavailable", ())
            if getattr(item, "name", None)
        }
        known = not ({"read_set", "write_set"} & unavailable_names) and all(
            bool(getattr(binding, "known", True)) and _binding_identity(binding) is not None
            for binding in bindings
        )
        if not known:
            unavailable.append(
                UnavailableField(
                    name=f"active_access.{attempt_id}",
                    reason="active attempt has no complete read/write set",
                )
            )
        views.append((attempt_id, reads, writes, known))
    return tuple(sorted(views, key=lambda item: item[0])), tuple(unavailable)


def _binding_identity(binding: Any) -> str | None:
    for field in ("resource_uri", "artifact_id", "action_id", "source_event_id"):
        value = str(getattr(binding, field, "") or "").strip()
        if value:
            return value
    return None


def _active_conflict_blockers(
    candidate: TaskAccessSet,
    active_accesses: tuple[tuple[str, tuple[str, ...], tuple[str, ...], bool], ...],
) -> tuple[str, ...]:
    candidate_reads = set(candidate.read_set)
    candidate_writes = set(candidate.write_set)
    blockers: list[str] = []
    for attempt_id, reads, writes, known in active_accesses:
        if (
            not known
            or candidate_writes & (set(reads) | set(writes))
            or candidate_reads & set(writes)
        ):
            blockers.append(f"active:{attempt_id}")
    return tuple(sorted(set(blockers)))


def _normalize_ids(values: Any) -> tuple[str, ...]:
    return tuple(sorted({str(value).strip() for value in (values or ()) if str(value).strip()}))


def _dedupe_unavailable(values: Iterable[UnavailableField]) -> tuple[UnavailableField, ...]:
    seen: set[tuple[str, str]] = set()
    result: list[UnavailableField] = []
    for item in values:
        key = (item.name, item.reason)
        if key not in seen:
            seen.add(key)
            result.append(item)
    return tuple(sorted(result, key=lambda item: (item.name, item.reason)))


def _hash_payload(payload: Any) -> str:
    canonical = json.dumps(
        _json_compatible(payload),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _json_compatible(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, tuple):
        return [_json_compatible(item) for item in value]
    if isinstance(value, list):
        return [_json_compatible(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_compatible(item) for key, item in value.items()}
    if isinstance(value, FrontierAction):
        return value.value
    return value


__all__ = [
    "RESOURCE_AWARE_PARALLELISM_POLICY_ID",
    "RESOURCE_AWARE_PARALLELISM_SCHEMA_VERSION",
    "ResourceAwareBatchSuggestion",
    "ResourceAwareParallelismPolicy",
    "ResourceBatchSuggestion",
    "ResourceRequest",
    "ResourceTaskAssignment",
    "ResourceTaskDecision",
    "ResourceTaskRequest",
    "TaskResourceRequest",
    "plan_resource_aware_batch",
    "suggest_resource_aware_batch",
]
