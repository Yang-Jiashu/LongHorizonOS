"""Deterministic WHAT/WHEN selection for an observed runtime state.

``FrontierPolicy`` is intentionally a small, opt-in primitive.  It ranks the
already-derived VPG ready frontier and emits an immutable ``SchedulingEpoch``;
it does not claim work, acquire leases, inspect physical telemetry, or mutate
the scheduler.  The existing scheduler remains the authority for eligibility,
resource admission, and execution ownership.

The default ranking remains the historical repair-first lexical order.  An
explicit ``graph_utility`` strategy can instead consume the declared-VPG
critical path and immediate downstream-unlock signals already projected by
``GlobalRuntimeState``.  These structural hints are not a cost/success
prediction and never relax the policy's safety filters.
"""

from __future__ import annotations

import hashlib
import json
from enum import StrEnum
from typing import Any, Final, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    field_validator,
    model_validator,
)

from .runtime_state import (
    GlobalRuntimeState,
    UnavailableField,
)

SCHEDULING_EPOCH_SCHEMA_VERSION: Final[Literal["scheduling-epoch.v1"]] = "scheduling-epoch.v1"
FRONTIER_POLICY_ID: Final[str] = "deterministic-frontier.v1"
GRAPH_UTILITY_FRONTIER_POLICY_ID: Final[str] = "graph-utility-frontier.v1"


class FrontierAction(StrEnum):
    """Action emitted by the bounded frontier policy."""

    RUN = "run"
    DEFER = "defer"


class FrontierRankingStrategy(StrEnum):
    """Deterministic candidate ordering used by :class:`FrontierPolicy`."""

    REPAIR_FIRST_LEXICAL = "repair_first_lexical"
    GRAPH_UTILITY = "graph_utility"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class FrontierTaskDecision(_FrozenModel):
    """One deterministic decision for a candidate frontier task."""

    task_id: str = Field(min_length=1)
    action: FrontierAction
    score: float = Field(ge=0.0)
    reason: str = Field(min_length=1)


class SchedulingEpoch(_FrozenModel):
    """Immutable output of one frontier-planning pass.

    ``parallelism_hint`` is only the size of the proposed batch.  It is not a
    resource-admission result and does not reserve any capacity.
    """

    schema_version: Literal["scheduling-epoch.v1"] = SCHEDULING_EPOCH_SCHEMA_VERSION
    epoch_id: StrictInt = Field(ge=0)
    policy_id: str = Field(min_length=1)
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    projection_hash: str = Field(min_length=1)
    candidate_task_ids: tuple[str, ...] = ()
    selected_task_ids: tuple[str, ...] = ()
    deferred_task_ids: tuple[str, ...] = ()
    decisions: tuple[FrontierTaskDecision, ...] = ()
    parallelism_hint: StrictInt = Field(ge=0)
    unavailable: tuple[UnavailableField, ...] = ()
    decision_hash: str = Field(min_length=64, max_length=64)

    @field_validator("epoch_id", "graph_version", "parallelism_hint")
    @classmethod
    def _require_real_int(cls, value: int) -> int:
        # Pydantic's normal coercion is useful for most SDK DTOs, but epoch and
        # version identities must not silently turn booleans/floats into ids.
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("must be an integer")
        return value

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible immutable-state projection."""

        return self.model_dump(mode="json")


class FrontierPolicy(_FrozenModel):
    """Conservative deterministic policy over ``GlobalRuntimeState``.

    The policy only answers *what could be attempted next* and *which
    candidates should wait in this epoch*.  It deliberately leaves
    agent/worker matching, claims, leases, and resource admission to the
    existing Scheduler.  ``graph_utility`` is explicit opt-in so callers that
    depend on the historical repair-first lexical ordering remain unchanged.
    """

    max_parallelism: StrictInt = Field(default=1, ge=1)
    ranking_strategy: FrontierRankingStrategy = FrontierRankingStrategy.REPAIR_FIRST_LEXICAL
    policy_id: str = FRONTIER_POLICY_ID

    @model_validator(mode="before")
    @classmethod
    def _default_policy_id_for_strategy(cls, value: Any) -> Any:
        """Give the opt-in strategy a distinct, auditable policy identity."""

        if not isinstance(value, dict):
            return value
        raw_strategy = value.get(
            "ranking_strategy",
            FrontierRankingStrategy.REPAIR_FIRST_LEXICAL,
        )
        try:
            strategy = FrontierRankingStrategy(raw_strategy)
        except (TypeError, ValueError):
            # Let normal Pydantic field validation report the invalid value.
            return value
        if strategy is FrontierRankingStrategy.GRAPH_UTILITY and "policy_id" not in value:
            value = dict(value)
            value["policy_id"] = GRAPH_UTILITY_FRONTIER_POLICY_ID
        return value

    @field_validator("max_parallelism")
    @classmethod
    def _require_parallelism_int(cls, value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("max_parallelism must be an integer")
        return value

    @field_validator("policy_id")
    @classmethod
    def _non_empty_policy_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("policy_id must be non-empty")
        return value

    def plan(self, state: GlobalRuntimeState, epoch_id: int = 0) -> SchedulingEpoch:
        """Plan one immutable scheduling epoch without side effects.

        Candidates are the union of the observed ready and repair-ready
        frontiers.  Repair-ready candidates always rank before ordinary ready
        candidates.  The default strategy breaks ties lexically.  The explicit
        graph-utility strategy next prefers the declared critical path, then
        earlier nodes on that path, then larger immediate downstream-unlock
        values, and finally lexical order.  A task with an active cognition
        attempt is never selected a second time.
        """

        if not isinstance(state, GlobalRuntimeState):
            raise TypeError("state must be a GlobalRuntimeState/RuntimeStateView")
        if isinstance(epoch_id, bool) or not isinstance(epoch_id, int) or epoch_id < 0:
            raise ValueError("epoch_id must be a non-negative integer")

        ready = _normalize_ids(state.progress.ready_frontier)
        repair_ready = _normalize_ids(state.progress.repair_ready_frontier)
        repair_set = set(repair_ready)
        # Keep the repair frontier even if a malformed/legacy projection does
        # not repeat it in ready_frontier; this makes the policy fail closed
        # only when the candidate itself is otherwise unsafe.
        candidate_set = set(ready) | repair_set
        candidates, scores = _rank_candidates(
            candidate_set=candidate_set,
            repair_set=repair_set,
            state=state,
            strategy=self.ranking_strategy,
        )

        verified = set(_normalize_ids(state.progress.verified_task_ids))
        invalid = set(_normalize_ids(state.progress.invalid_task_ids))
        stale = set(_normalize_ids(state.progress.stale_task_ids))
        active_tasks = {
            attempt.task_id for attempt in state.agent_cognition.current_attempts if attempt.task_id
        }
        closed = bool(state.progress.graph_closed or state.progress.goal_closed)
        cognition_available = bool(state.agent_cognition.available)
        resources_available = bool(state.resources.available)
        unavailable = _state_unavailability(state)

        selected: list[str] = []
        deferred: list[str] = []
        decisions: list[FrontierTaskDecision] = []

        for task_id in candidates:
            is_repair = task_id in repair_set
            score = scores[task_id]
            action = FrontierAction.DEFER
            reason = "ready"
            if closed:
                reason = "closed"
            elif task_id in verified or task_id in invalid:
                reason = "terminal_validity"
            elif task_id in stale and not is_repair:
                # A stale task is only safely runnable when the VPG has put it
                # on its repair-ready frontier.
                reason = "stale_not_repair_ready"
            elif task_id in active_tasks:
                reason = "active_attempt"
            elif not cognition_available:
                reason = "cognition_unavailable"
            elif not resources_available:
                reason = "resources_unavailable"
            elif len(selected) < self.max_parallelism:
                action = FrontierAction.RUN
                reason = "selected"
                selected.append(task_id)
            else:
                reason = "max_parallelism"

            if action is FrontierAction.DEFER:
                deferred.append(task_id)
            decisions.append(
                FrontierTaskDecision(
                    task_id=task_id,
                    action=action,
                    score=score,
                    reason=reason,
                )
            )

        # No resource admission is performed here.  A false/unavailable
        # projection therefore yields no proposed batch rather than pretending
        # that a single slot is safe.
        parallelism_hint = 0 if not (cognition_available and resources_available) else len(selected)
        payload = {
            "schema_version": SCHEDULING_EPOCH_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "policy_id": self.policy_id,
            "graph_id": state.graph_id,
            "graph_version": state.progress.graph_version,
            "projection_hash": state.progress.projection_hash,
            "candidate_task_ids": candidates,
            "selected_task_ids": tuple(selected),
            "deferred_task_ids": tuple(deferred),
            "decisions": tuple(decisions),
            "parallelism_hint": parallelism_hint,
            "unavailable": tuple(unavailable),
        }
        decision_hash = _decision_hash(payload)
        return SchedulingEpoch(
            schema_version=SCHEDULING_EPOCH_SCHEMA_VERSION,
            epoch_id=epoch_id,
            policy_id=self.policy_id,
            graph_id=state.graph_id,
            graph_version=state.progress.graph_version,
            projection_hash=state.progress.projection_hash,
            candidate_task_ids=candidates,
            selected_task_ids=tuple(selected),
            deferred_task_ids=tuple(deferred),
            decisions=tuple(decisions),
            parallelism_hint=parallelism_hint,
            unavailable=tuple(unavailable),
            decision_hash=decision_hash,
        )


def plan_frontier(
    state: GlobalRuntimeState,
    *,
    epoch_id: int = 0,
    max_parallelism: int = 1,
    ranking_strategy: FrontierRankingStrategy | str = (
        FrontierRankingStrategy.REPAIR_FIRST_LEXICAL
    ),
) -> SchedulingEpoch:
    """Convenience wrapper for one deterministic frontier-planning pass."""

    return FrontierPolicy(
        max_parallelism=max_parallelism,
        ranking_strategy=FrontierRankingStrategy(ranking_strategy),
    ).plan(state, epoch_id=epoch_id)


def _rank_candidates(
    *,
    candidate_set: set[str],
    repair_set: set[str],
    state: GlobalRuntimeState,
    strategy: FrontierRankingStrategy,
) -> tuple[tuple[str, ...], dict[str, float]]:
    """Return a deterministic candidate order and transparent ordinal score.

    Repair priority is invariant across strategies.  Graph utility only
    reorders candidates *within* that boundary and cannot make a non-frontier
    task runnable.
    """

    if strategy is FrontierRankingStrategy.REPAIR_FIRST_LEXICAL:
        candidates = tuple(
            sorted(
                candidate_set,
                key=lambda task_id: (
                    0 if task_id in repair_set else 1,
                    task_id,
                ),
            )
        )
        return candidates, {
            task_id: 1.0 if task_id in repair_set else 0.0 for task_id in candidates
        }

    critical_positions = _critical_path_positions(state.progress.critical_path)
    unlock_values = _unlock_value_index(state.progress.downstream_unlock_values)
    path_length = len(critical_positions)

    def sort_key(task_id: str) -> tuple[int, int, int, int, str]:
        critical_position = critical_positions.get(task_id)
        return (
            0 if task_id in repair_set else 1,
            0 if critical_position is not None else 1,
            critical_position if critical_position is not None else path_length,
            -unlock_values.get(task_id, 0),
            task_id,
        )

    candidates = tuple(sorted(candidate_set, key=sort_key))
    max_unlock = max((unlock_values.get(task_id, 0) for task_id in candidates), default=0)
    unlock_span = max_unlock + 1
    # Each higher-priority tier has enough space that no downstream-unlock
    # value from a lower tier can overtake it.  This makes ``score`` agree with
    # the lexicographic policy without relying on arbitrary magic weights.
    path_span = (path_length + 1) * unlock_span
    scores: dict[str, float] = {}
    for task_id in candidates:
        critical_position = critical_positions.get(task_id)
        critical = critical_position is not None
        tier = (2 if task_id in repair_set else 0) + (1 if critical else 0)
        path_priority = path_length - critical_position if critical_position is not None else 0
        scores[task_id] = float(
            tier * path_span + path_priority * unlock_span + unlock_values.get(task_id, 0)
        )
    return candidates, scores


def _critical_path_positions(values: Any) -> dict[str, int]:
    """Normalize the projected path while preserving its first-seen order."""

    result: dict[str, int] = {}
    for value in values or ():
        task_id = str(value).strip()
        if task_id and task_id not in result:
            result[task_id] = len(result)
    return result


def _unlock_value_index(values: Any) -> dict[str, int]:
    """Normalize duplicate legacy rows deterministically and conservatively."""

    result: dict[str, int] = {}
    for item in values or ():
        task_id = str(getattr(item, "task_id", "")).strip()
        if not task_id:
            continue
        unlock_value = max(0, int(getattr(item, "unlock_value", 0)))
        result[task_id] = max(result.get(task_id, 0), unlock_value)
    return result


def _normalize_ids(values: Any) -> tuple[str, ...]:
    """Normalize projection ids without mutating the source model."""

    result = {str(value).strip() for value in (values or ()) if str(value).strip()}
    return tuple(sorted(result))


def _state_unavailability(state: GlobalRuntimeState) -> tuple[UnavailableField, ...]:
    unavailable: list[UnavailableField] = []
    if not state.agent_cognition.available:
        unavailable.append(
            UnavailableField(
                name="agent_cognition",
                reason=state.agent_cognition.reason or "Agent cognition state unavailable",
            )
        )
    if not state.resources.available:
        unavailable.append(
            UnavailableField(
                name="resources",
                reason=state.resources.reason or "logical resource state is unavailable",
            )
        )
    return tuple(unavailable)


def _decision_hash(payload: dict[str, Any]) -> str:
    """Hash the canonical epoch payload (excluding the derived hash itself)."""

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
    if isinstance(value, StrEnum):
        return value.value
    return value


__all__ = [
    "FRONTIER_POLICY_ID",
    "GRAPH_UTILITY_FRONTIER_POLICY_ID",
    "SCHEDULING_EPOCH_SCHEMA_VERSION",
    "FrontierAction",
    "FrontierPolicy",
    "FrontierRankingStrategy",
    "FrontierTaskDecision",
    "SchedulingEpoch",
    "plan_frontier",
]
