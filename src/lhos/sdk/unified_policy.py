"""Unified graph-relative adaptive admission policy.

This module composes the existing verified-progress and resource/conflict
policies into one pure planning pass.  It deliberately remains advisory:
planning never claims work, acquires a lease, or mutates Scheduler state.

The important ordering rule is that only a task which survives *all* guards is
allowed to consume either compute budget or logical pool capacity.  A high
utility task that conflicts, has an insufficient request, or exceeds the
current budget therefore cannot prevent a later candidate from being
backfilled.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from enum import StrEnum
from typing import Any, Final, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, field_validator

from .compute_budget import (
    ComputeBudgetLimits,
    ComputeBudgetRemaining,
    ComputeBudgetTaskDecision,
    ComputeBudgetUsage,
    TaskComputeEstimate,
    VerifiedProgressBudgetPolicy,
    _budget_blockers,
    _normalize_estimates,
    _usage_limit_violations,
)
from .conflict_graph import ConflictGraph
from .frontier_policy import FrontierAction
from .resource_policy import (
    ResourceTaskRequest,
    _active_access_views,
    _active_conflict_blockers,
    _available_pools,
    _choose_pool,
    _dedupe_unavailable,
    _fits,
    _normalize_ids,
    _normalize_requests,
    _resource_fit_reason,
    _vector_minus,
)
from .runtime_state import (
    GlobalRuntimeState,
    ResourceVectorState,
    UnavailableField,
)

UNIFIED_ADAPTIVE_SCHEMA_VERSION: Final[Literal["unified-adaptive-plan.v1"]] = (
    "unified-adaptive-plan.v1"
)
UNIFIED_ADAPTIVE_POLICY_ID: Final[str] = "unified-adaptive.v1"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class UnifiedTaskAssignment(_FrozenModel):
    """Logical resource assignment emitted for one selected task."""

    task_id: str = Field(min_length=1)
    pool_id: str = Field(min_length=1)
    resources: ResourceVectorState


class UnifiedTaskDecision(_FrozenModel):
    """Complete audit record for one candidate in the unified pass."""

    task_id: str = Field(min_length=1)
    action: FrontierAction
    reason: str = Field(min_length=1)
    tier: Literal["repair", "ready"]
    estimate_known: StrictBool
    expected_verified_progress_numerator: StrictInt | None = None
    expected_verified_progress_denominator: StrictInt | None = None
    utility_numerator: StrictInt | None = None
    utility_denominator: StrictInt | None = None
    budget_delta: ComputeBudgetUsage | None = None
    budget_blockers: tuple[str, ...] = ()
    budget_usage_before: ComputeBudgetUsage | None = None
    budget_usage_after: ComputeBudgetUsage | None = None
    requested: ResourceVectorState | None = None
    resource_known: StrictBool = False
    resource_blockers: tuple[str, ...] = ()
    assigned_pool_id: str | None = None
    access_known: StrictBool = False
    conflict_blockers: tuple[str, ...] = ()
    active_conflict_blockers: tuple[str, ...] = ()
    selected_conflict_blockers: tuple[str, ...] = ()
    canonical_budget_decision: ComputeBudgetTaskDecision | None = None

    @property
    def pool_id(self) -> str | None:
        """Compatibility alias for callers using resource-policy terminology."""

        return self.assigned_pool_id


class UnifiedAdaptivePlan(_FrozenModel):
    """Immutable result of one unified adaptive planning epoch."""

    schema_version: Literal["unified-adaptive-plan.v1"] = UNIFIED_ADAPTIVE_SCHEMA_VERSION
    epoch_id: StrictInt = Field(ge=0)
    policy_id: str = Field(min_length=1)
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    projection_hash: str = Field(min_length=1)
    conflict_graph_hash: str = Field(min_length=64, max_length=64)
    candidate_task_ids: tuple[str, ...] = ()
    selected_task_ids: tuple[str, ...] = ()
    deferred_task_ids: tuple[str, ...] = ()
    assignments: tuple[UnifiedTaskAssignment, ...] = ()
    decisions: tuple[UnifiedTaskDecision, ...] = ()
    parallelism_hint: StrictInt = Field(ge=0)
    limits: ComputeBudgetLimits
    usage_before: ComputeBudgetUsage
    usage_after: ComputeBudgetUsage
    remaining_before: ComputeBudgetRemaining
    remaining_after: ComputeBudgetRemaining
    safe_under_declared_budget: StrictBool = False
    safe_under_declared_resources: StrictBool = False
    safe_under_constraints: StrictBool = False
    unavailable: tuple[UnavailableField, ...] = ()
    decision_hash: str = Field(min_length=64, max_length=64)

    @property
    def selected(self) -> tuple[str, ...]:
        return self.selected_task_ids

    @property
    def deferred(self) -> tuple[str, ...]:
        return self.deferred_task_ids

    @property
    def resource_assignments(self) -> tuple[UnifiedTaskAssignment, ...]:
        return self.assignments

    @property
    def safe(self) -> bool:
        """Whether all declared constraints are authoritative for this plan."""

        return self.safe_under_constraints

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class UnifiedAdaptivePolicy(_FrozenModel):
    """Single-pass deterministic composition of semantic, budget, and resource guards."""

    max_parallelism: StrictInt = Field(default=1, ge=1)
    policy_id: str = UNIFIED_ADAPTIVE_POLICY_ID

    @field_validator("max_parallelism")
    @classmethod
    def _require_parallelism_int(cls, value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("max_parallelism must be an integer")
        return value

    @field_validator("policy_id")
    @classmethod
    def _require_policy_id(cls, value: str) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("policy_id must be non-empty")
        return normalized

    def plan(
        self,
        state: GlobalRuntimeState,
        conflict_graph: ConflictGraph,
        task_resources: Mapping[str, Any] | Iterable[ResourceTaskRequest] | None,
        estimates: Mapping[str, TaskComputeEstimate] | Iterable[TaskComputeEstimate],
        limits: ComputeBudgetLimits,
        usage: ComputeBudgetUsage,
        epoch_id: int = 0,
    ) -> UnifiedAdaptivePlan:
        """Return one immutable, side-effect-free adaptive admission plan.

        Candidate order comes from :class:`VerifiedProgressBudgetPolicy`.
        That policy is used only as the canonical normalization/ranking source;
        its budget and parallelism decisions are intentionally recomputed here
        against the *actual selected* usage and logical pool capacity.
        """

        if not isinstance(state, GlobalRuntimeState):
            raise TypeError("state must be a GlobalRuntimeState/RuntimeStateView")
        if not isinstance(conflict_graph, ConflictGraph):
            raise TypeError("conflict_graph must be a ConflictGraph")
        if not isinstance(limits, ComputeBudgetLimits):
            raise TypeError("limits must be ComputeBudgetLimits")
        if not isinstance(usage, ComputeBudgetUsage):
            raise TypeError("usage must be ComputeBudgetUsage")
        if isinstance(epoch_id, bool) or not isinstance(epoch_id, int) or epoch_id < 0:
            raise ValueError("epoch_id must be a non-negative integer")

        # Normalize once before invoking the budget policy so a one-shot
        # iterable is not consumed twice.  The budget policy remains the
        # single source of truth for candidate ordering and estimate DTOs.
        normalized_estimates, estimate_errors = _normalize_estimates(estimates)
        # Give the canonical policy enough room that its max_parallelism guard
        # cannot truncate the candidate order.
        ready = _normalize_ids(state.progress.ready_frontier)
        repair_ready = _normalize_ids(state.progress.repair_ready_frontier)
        candidate_count = max(1, len(set(ready) | set(repair_ready)))
        canonical_budget = VerifiedProgressBudgetPolicy().plan(
            state,
            cast(Mapping[str, TaskComputeEstimate], normalized_estimates),
            limits,
            usage,
            epoch_id=epoch_id,
            max_parallelism=candidate_count,
        )
        canonical_decisions = {item.task_id: item for item in canonical_budget.decisions}
        candidate_ids = canonical_budget.candidate_task_ids

        normalized_requests, request_errors = _normalize_requests(task_resources)
        pools, pool_errors = _available_pools(state)
        active_accesses, active_access_errors = _active_access_views(
            state.agent_cognition.current_attempts
        )

        unavailable: list[UnavailableField] = [
            *canonical_budget.unavailable,
            *estimate_errors,
            *request_errors,
            *pool_errors,
            *active_access_errors,
        ]

        graph_fence_ok = bool(
            state.graph_id.strip()
            and state.progress.graph_id.strip()
            and state.graph_id == state.progress.graph_id
        )
        projection_hash_ok = bool(str(state.progress.projection_hash).strip())
        if not graph_fence_ok:
            unavailable.append(
                UnavailableField(
                    name="graph_fence",
                    reason="state graph_id and progress.graph_id do not match",
                )
            )
        if not projection_hash_ok:
            unavailable.append(
                UnavailableField(
                    name="projection_hash",
                    reason="progress projection hash is missing or empty",
                )
            )

        unknown_estimate_ids = {
            task_id
            for task_id in candidate_ids
            if ((estimate := normalized_estimates.get(task_id)) is None or not estimate.known)
        }
        for task_id in sorted(unknown_estimate_ids):
            unavailable.append(
                UnavailableField(
                    name=f"estimates.{task_id}",
                    reason="task compute estimate is missing or marked unknown",
                )
            )
        unknown_request_ids = {
            task_id
            for task_id in candidate_ids
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

        verified = set(_normalize_ids(state.progress.verified_task_ids))
        invalid = set(_normalize_ids(state.progress.invalid_task_ids))
        stale = set(_normalize_ids(state.progress.stale_task_ids))
        repair_set = set(repair_ready)
        active_tasks = {
            str(getattr(attempt, "task_id", "") or "").strip()
            for attempt in state.agent_cognition.current_attempts
            if str(getattr(attempt, "task_id", "") or "").strip()
        }
        closed = bool(state.progress.graph_closed or state.progress.goal_closed)
        cognition_available = bool(state.agent_cognition.available)
        resources_available = bool(state.resources.available)
        if not cognition_available:
            unavailable.append(
                UnavailableField(
                    name="agent_cognition",
                    reason=state.agent_cognition.reason or "Agent cognition state unavailable",
                )
            )
        if not resources_available:
            unavailable.append(
                UnavailableField(
                    name="resources",
                    reason=state.resources.reason or "logical resources unavailable",
                )
            )

        remaining_pools = dict(pools)
        usage_after = usage
        selected: list[str] = []
        selected_unknown_access_task_ids: list[str] = []
        deferred: list[str] = []
        assignments: list[UnifiedTaskAssignment] = []
        decisions: list[UnifiedTaskDecision] = []
        unsafe_access = False

        for task_id in candidate_ids:
            canonical = canonical_decisions.get(task_id)
            estimate = normalized_estimates.get(task_id)
            known_estimate = estimate is not None and estimate.known
            request = normalized_requests.get(task_id)
            known_request = request is not None and request.known
            access = conflict_graph.access_for(task_id)
            access_known = access is not None and access.known
            is_repair = task_id in repair_set
            tier: Literal["repair", "ready"] = "repair" if is_repair else "ready"

            budget_delta = estimate.total_budget_delta if known_estimate and estimate else None
            action = FrontierAction.DEFER
            reason = "ready"
            budget_blockers: tuple[str, ...] = ()
            conflict_blockers: tuple[str, ...] = ()
            active_conflict_blockers: tuple[str, ...] = ()
            selected_conflict_blockers: tuple[str, ...] = ()
            resource_blockers: tuple[str, ...] = ()
            assigned_pool_id: str | None = None
            usage_before = usage_after

            # Semantic guards are intentionally evaluated before any
            # admission/accounting operation.
            if not graph_fence_ok:
                reason = "graph_fence_invalid"
            elif not projection_hash_ok:
                reason = "projection_hash_unavailable"
            elif closed:
                reason = "closed"
            elif task_id in verified or task_id in invalid:
                reason = "terminal_validity"
            elif task_id in stale and not is_repair:
                reason = "stale_not_repair_ready"
            elif task_id in active_tasks:
                reason = "active_attempt"
            elif not cognition_available:
                reason = "cognition_unavailable"
            elif not resources_available:
                reason = "resources_unavailable"
            elif not known_estimate:
                reason = "estimate_unknown"
            elif not known_request:
                reason = "resource_request_unknown"
            else:
                assert request is not None
                if known_estimate:
                    assert estimate is not None
                # Unknown candidate accesses are serial-only, matching the
                # resource-aware policy: they may run alone when no active or
                # selected occupancy exists, but make the proposal unsafe.
                if selected_unknown_access_task_ids:
                    selected_conflict_blockers = tuple(selected_unknown_access_task_ids)
                    conflict_blockers = selected_conflict_blockers
                    reason = "unknown_access_serial_only"
                elif not access_known:
                    if active_accesses:
                        active_conflict_blockers = tuple(
                            sorted(f"active:{attempt_id}" for attempt_id, *_ in active_accesses)
                        )
                        conflict_blockers = active_conflict_blockers
                        reason = "unknown_access_active_occupancy"
                    elif selected:
                        selected_conflict_blockers = tuple(selected)
                        conflict_blockers = selected_conflict_blockers
                        reason = "unknown_access_serial_only"
                    else:
                        unsafe_access = True
                else:
                    assert access is not None
                    selected_conflict_blockers = tuple(
                        chosen
                        for chosen in selected
                        if conflict_graph.conflicts_with(task_id, chosen)
                    )
                    active_conflict_blockers = _active_conflict_blockers(
                        access,
                        active_accesses,
                    )
                    conflict_blockers = tuple(
                        sorted(set(selected_conflict_blockers + active_conflict_blockers))
                    )
                    if conflict_blockers:
                        reason = "active_conflict" if active_conflict_blockers else "conflict"

                if not conflict_blockers:
                    if len(selected) >= self.max_parallelism:
                        reason = "max_parallelism"
                    else:
                        assert budget_delta is not None
                        budget_blockers = _budget_blockers(
                            usage_after,
                            budget_delta,
                            limits,
                        )
                        if budget_blockers:
                            reason = "budget_exceeded"
                        else:
                            assigned_pool_id = _choose_pool(
                                request,
                                remaining_pools,
                                pools,
                            )
                            if assigned_pool_id is None:
                                reason, resource_blockers = _resource_fit_reason(
                                    request,
                                    remaining_pools,
                                    pools,
                                )
                            else:
                                action = FrontierAction.RUN
                                reason = (
                                    "selected_repair"
                                    if is_repair
                                    else (
                                        "unknown_access_serial_only"
                                        if not access_known
                                        else "selected"
                                    )
                                )
                                selected.append(task_id)
                                if not access_known:
                                    selected_unknown_access_task_ids.append(task_id)
                                usage_after = usage_after.plus(budget_delta)
                                remaining_pools[assigned_pool_id] = _vector_minus(
                                    remaining_pools[assigned_pool_id],
                                    request.resources,
                                )
                                assignments.append(
                                    UnifiedTaskAssignment(
                                        task_id=task_id,
                                        pool_id=assigned_pool_id,
                                        resources=request.resources,
                                    )
                                )

            if action is FrontierAction.DEFER:
                deferred.append(task_id)

            decisions.append(
                UnifiedTaskDecision(
                    task_id=task_id,
                    action=action,
                    reason=reason,
                    tier=tier,
                    estimate_known=known_estimate,
                    expected_verified_progress_numerator=(
                        canonical.expected_verified_progress_numerator
                        if canonical is not None
                        else None
                    ),
                    expected_verified_progress_denominator=(
                        canonical.expected_verified_progress_denominator
                        if canonical is not None
                        else None
                    ),
                    utility_numerator=(
                        canonical.utility_numerator if canonical is not None else None
                    ),
                    utility_denominator=(
                        canonical.utility_denominator if canonical is not None else None
                    ),
                    budget_delta=budget_delta,
                    budget_blockers=budget_blockers,
                    budget_usage_before=usage_before,
                    budget_usage_after=usage_after,
                    requested=request.resources if request is not None else None,
                    resource_known=known_request,
                    resource_blockers=resource_blockers,
                    assigned_pool_id=assigned_pool_id,
                    access_known=access_known,
                    conflict_blockers=conflict_blockers,
                    active_conflict_blockers=active_conflict_blockers,
                    selected_conflict_blockers=selected_conflict_blockers,
                    canonical_budget_decision=canonical,
                )
            )

        usage_limit_violations = _usage_limit_violations(usage, limits)
        remaining_before = ComputeBudgetRemaining.from_limits(limits, usage)
        remaining_after = ComputeBudgetRemaining.from_limits(limits, usage_after)
        unknown_access_present = any(not decision.access_known for decision in decisions)
        unknown_pool_or_capacity = any(
            decision.reason in {"resource_capacity_unknown", "resource_pool_unknown"}
            for decision in decisions
        )
        intrinsically_unschedulable = any(
            decision.reason == "insufficient_resources"
            and (
                (request := normalized_requests.get(decision.task_id)) is not None
                and not any(_fits(request.resources, vector) for vector in pools.values())
            )
            for decision in decisions
        )
        budget_safe = bool(
            graph_fence_ok
            and projection_hash_ok
            and not usage_limit_violations
            and not unknown_estimate_ids
        )
        resource_safe = bool(
            resources_available
            and bool(pools)
            and not pool_errors
            and not unknown_request_ids
            and not unknown_pool_or_capacity
            and not intrinsically_unschedulable
        )
        constraints_safe = bool(
            budget_safe
            and resource_safe
            and cognition_available
            and not active_access_errors
            and not unknown_access_present
            and not unsafe_access
        )
        unavailable_tuple = _dedupe_unavailable(unavailable)
        parallelism_hint = len(selected) if resources_available and cognition_available else 0
        projection_hash = (
            str(state.progress.projection_hash) if projection_hash_ok else "unavailable"
        )
        payload = {
            "schema_version": UNIFIED_ADAPTIVE_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "policy_id": self.policy_id,
            "graph_id": state.graph_id,
            "graph_version": state.progress.graph_version,
            "projection_hash": projection_hash,
            "conflict_graph_hash": conflict_graph.graph_hash,
            "candidate_task_ids": candidate_ids,
            "selected_task_ids": tuple(selected),
            "deferred_task_ids": tuple(deferred),
            "assignments": tuple(assignments),
            "decisions": tuple(decisions),
            "parallelism_hint": parallelism_hint,
            "limits": limits,
            "usage_before": usage,
            "usage_after": usage_after,
            "remaining_before": remaining_before,
            "remaining_after": remaining_after,
            "safe_under_declared_budget": budget_safe,
            "safe_under_declared_resources": resource_safe,
            "safe_under_constraints": constraints_safe,
            "unavailable": unavailable_tuple,
        }
        return UnifiedAdaptivePlan(
            epoch_id=epoch_id,
            policy_id=self.policy_id,
            graph_id=state.graph_id,
            graph_version=state.progress.graph_version,
            projection_hash=projection_hash,
            conflict_graph_hash=conflict_graph.graph_hash,
            candidate_task_ids=candidate_ids,
            selected_task_ids=tuple(selected),
            deferred_task_ids=tuple(deferred),
            assignments=tuple(assignments),
            decisions=tuple(decisions),
            parallelism_hint=parallelism_hint,
            limits=limits,
            usage_before=usage,
            usage_after=usage_after,
            remaining_before=remaining_before,
            remaining_after=remaining_after,
            safe_under_declared_budget=budget_safe,
            safe_under_declared_resources=resource_safe,
            safe_under_constraints=constraints_safe,
            unavailable=unavailable_tuple,
            decision_hash=_decision_hash(payload),
        )


def plan_unified_adaptive(
    state: GlobalRuntimeState,
    conflict_graph: ConflictGraph,
    task_resources: Mapping[str, Any] | Iterable[ResourceTaskRequest] | None,
    estimates: Mapping[str, TaskComputeEstimate] | Iterable[TaskComputeEstimate],
    limits: ComputeBudgetLimits,
    usage: ComputeBudgetUsage,
    *,
    epoch_id: int = 0,
    max_parallelism: int = 1,
) -> UnifiedAdaptivePlan:
    """Convenience wrapper around :class:`UnifiedAdaptivePolicy`."""

    return UnifiedAdaptivePolicy(max_parallelism=max_parallelism).plan(
        state,
        conflict_graph,
        task_resources,
        estimates,
        limits,
        usage,
        epoch_id=epoch_id,
    )


def _decision_hash(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        _json_compatible(dict(payload)),
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
    if isinstance(value, StrEnum):
        return value.value
    return value


__all__ = [
    "UNIFIED_ADAPTIVE_POLICY_ID",
    "UNIFIED_ADAPTIVE_SCHEMA_VERSION",
    "UnifiedAdaptivePlan",
    "UnifiedAdaptivePolicy",
    "UnifiedTaskAssignment",
    "UnifiedTaskDecision",
    "plan_unified_adaptive",
]
