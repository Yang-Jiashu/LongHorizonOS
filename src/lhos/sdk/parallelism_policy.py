"""Deterministic parallelism-degree and upstream-stability decisions.

This module answers two questions the rest of the online-compute design leaves
to a caller-supplied constant today:

* **How many agents should run in parallel this epoch?**  The degree is
  *derived* from observable structure rather than accepted as a fixed
  ``max_parallelism``.  The caller ceiling is only an upper bound; the policy
  chooses a value at or below it and explains which observation was binding.
* **Which frontier tasks should wait for their upstream to settle?**  A task
  whose upstream artifacts have been re-derived stale / reopened / invalidated
  *repeatedly* across recent graph versions is deferred before it is ever
  dispatched, with the churn evidence attached.

Both decisions are pure functions over an immutable
:class:`~lhos.sdk.runtime_state.GlobalRuntimeState`.  The module composes the
existing conflict and resource policies instead of re-deriving their concepts:

* the conflict-free antichain comes from
  :class:`~lhos.sdk.conflict_graph.DynamicParallelismPolicy`;
* logical resource headroom comes from
  :class:`~lhos.sdk.resource_policy.ResourceAwareParallelismPolicy` when the
  caller supplies per-task requests;
* churn/contention evidence comes from the durable VPG event tail already
  projected into ``GlobalRuntimeState.recent_events``.

Determinism is a hard invariant: every threshold is expressed in graph
*versions* and *event counts*, never wall-clock, and every collection is
normalized through sorted sets so insertion order cannot change the result.

The policy is fail-closed.  If the conflict or resource information required to
prove a task safe to parallelise is unavailable, that task is serial-only or
deferred and an :class:`~lhos.sdk.runtime_state.UnavailableField` is attached;
the policy never reports a fabricated zero for something it could not observe.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from enum import StrEnum
from typing import Any, Final, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    field_validator,
)

from lhos.runtimes.verified_progress.events import GraphEventType

from .conflict_graph import ConflictGraph, DynamicParallelismPolicy
from .frontier_policy import FrontierAction
from .resource_policy import ResourceAwareParallelismPolicy, ResourceTaskRequest
from .runtime_state import GlobalRuntimeState, RecentRuntimeEventState, UnavailableField

ADAPTIVE_PARALLELISM_SCHEMA_VERSION: Final[Literal["adaptive-parallelism.v1"]] = (
    "adaptive-parallelism.v1"
)
ADAPTIVE_PARALLELISM_POLICY_ID: Final[str] = "adaptive-parallelism.v1"

DEFAULT_CHURN_VERSION_THRESHOLD: Final[int] = 2
DEFAULT_CHURN_EVENT_THRESHOLD: Final[int] = 2

# A task's *own* re-derivation is, by VPG semantics, evidence that something it
# depends on changed: a verified task only becomes stale/invalid/reopened when
# an input it consumed was replaced.  ``ARTIFACT_ATTACHED`` names an artifact
# node rather than a task node, so it only contributes when the caller supplies
# an explicit upstream node map that lists those artifact nodes.
_REWORK_EVENT_TYPES: Final[frozenset[str]] = frozenset(
    {
        GraphEventType.TASK_STALE_DERIVED.value,
        GraphEventType.TASK_REOPENED_DERIVED.value,
        GraphEventType.NODE_INVALID.value,
    }
)
_UPSTREAM_CHANGE_EVENT_TYPES: Final[frozenset[str]] = _REWORK_EVENT_TYPES | frozenset(
    {GraphEventType.ARTIFACT_ATTACHED.value}
)


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class UpstreamChurnEvent(_FrozenModel):
    """One recent invalidation/re-derivation event held against a task."""

    event_id: str
    event_type: str
    graph_version: int | None = Field(default=None, ge=0)
    node_id: str | None = None


class UpstreamStabilityVerdict(_FrozenModel):
    """Per-task decision on whether upstream inputs have settled.

    ``action`` is :attr:`FrontierAction.DEFER` when the task's upstream has
    changed *repeatedly* (across at least ``version_threshold`` distinct graph
    versions) or *often* (at least ``event_threshold`` invalidation events) in
    the observed event window.  A single stale transition — the normal event
    that places a task on the repair-ready frontier — never defers it.
    """

    task_id: str = Field(min_length=1)
    action: FrontierAction
    stable: StrictBool
    reason: str = Field(min_length=1)
    churn_event_count: StrictInt = Field(ge=0)
    churn_version_count: StrictInt = Field(ge=0)
    evidence: tuple[UpstreamChurnEvent, ...] = ()


class ParallelismBound(_FrozenModel):
    """One integer bound that constrained the chosen degree, with its reason."""

    name: str = Field(min_length=1)
    value: StrictInt = Field(ge=0)
    observed: StrictBool = True
    binding: StrictBool = False


class AdaptiveParallelismDecision(_FrozenModel):
    """Immutable, auditable degree + stability decision for one epoch."""

    schema_version: Literal["adaptive-parallelism.v1"] = ADAPTIVE_PARALLELISM_SCHEMA_VERSION
    epoch_id: StrictInt = Field(ge=0)
    policy_id: str = Field(min_length=1)
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    projection_hash: str = Field(min_length=1)
    conflict_graph_hash: str = Field(min_length=64, max_length=64)
    ceiling: StrictInt = Field(ge=1)
    chosen_degree: StrictInt = Field(ge=0)
    degree_reason: str = Field(min_length=1)
    contention_backoff: StrictInt = Field(ge=0)
    bounds: tuple[ParallelismBound, ...] = ()
    candidate_task_ids: tuple[str, ...] = ()
    frontier_antichain: tuple[str, ...] = ()
    dispatch_task_ids: tuple[str, ...] = ()
    deferred_for_churn: tuple[str, ...] = ()
    stability: tuple[UpstreamStabilityVerdict, ...] = ()
    resource_headroom_observed: StrictBool = False
    safe: StrictBool = False
    unavailable: tuple[UnavailableField, ...] = ()
    decision_hash: str = Field(min_length=64, max_length=64)

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class AdaptiveParallelismPolicy(_FrozenModel):
    """Choose a parallelism degree and per-task stability verdicts from state.

    ``max_parallelism`` is a strict ceiling, not the answer: the policy selects
    a degree at or below it from the observed conflict-free antichain, logical
    resource headroom, and recent contention.  A degree of ``1`` is a decision
    the policy can reach and explain (for example, "every candidate conflicts"
    or "no per-task resource request was observed, so hold at serial").
    """

    max_parallelism: StrictInt = Field(default=1, ge=1)
    churn_version_threshold: StrictInt = Field(default=DEFAULT_CHURN_VERSION_THRESHOLD, ge=1)
    churn_event_threshold: StrictInt = Field(default=DEFAULT_CHURN_EVENT_THRESHOLD, ge=1)
    policy_id: str = ADAPTIVE_PARALLELISM_POLICY_ID

    @field_validator("max_parallelism", "churn_version_threshold", "churn_event_threshold")
    @classmethod
    def _require_real_int(cls, value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("degree and threshold parameters must be integers")
        return value

    @field_validator("policy_id")
    @classmethod
    def _non_empty_policy_id(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("policy_id must be non-empty")
        return str(value).strip()

    def assess_upstream_stability(
        self,
        state: GlobalRuntimeState,
        *,
        upstream: Mapping[str, Iterable[str]] | None = None,
    ) -> tuple[UpstreamStabilityVerdict, ...]:
        """Return one stability verdict per frontier candidate.

        ``upstream`` optionally maps a frontier task id to the node ids of its
        upstream tasks and/or their artifact nodes.  When omitted, churn is
        attributed from events that name the task itself, which the VPG emits
        precisely when one of the task's inputs is replaced.
        """

        if not isinstance(state, GlobalRuntimeState):
            raise TypeError("state must be a GlobalRuntimeState/RuntimeStateView")
        candidates = _candidate_ids(state)
        upstream_index = _normalize_upstream(upstream)
        return tuple(
            self._verdict_for(task_id, state.recent_events, upstream_index)
            for task_id in candidates
        )

    def decide(
        self,
        state: GlobalRuntimeState,
        conflict_graph: ConflictGraph,
        *,
        task_resources: Mapping[str, Any] | Iterable[ResourceTaskRequest] | None = None,
        upstream: Mapping[str, Iterable[str]] | None = None,
        epoch_id: int = 0,
    ) -> AdaptiveParallelismDecision:
        """Return a deterministic degree + stability decision without side effects."""

        if not isinstance(state, GlobalRuntimeState):
            raise TypeError("state must be a GlobalRuntimeState/RuntimeStateView")
        if not isinstance(conflict_graph, ConflictGraph):
            raise TypeError("conflict_graph must be a ConflictGraph")
        if isinstance(epoch_id, bool) or not isinstance(epoch_id, int) or epoch_id < 0:
            raise ValueError("epoch_id must be a non-negative integer")

        candidates = _candidate_ids(state)
        upstream_index = _normalize_upstream(upstream)
        stability = tuple(
            self._verdict_for(task_id, state.recent_events, upstream_index)
            for task_id in candidates
        )
        churning = {verdict.task_id for verdict in stability if not verdict.stable}
        deferred_for_churn = tuple(sorted(churning))
        stable_candidates = tuple(task_id for task_id in candidates if task_id not in churning)

        unavailable: list[UnavailableField] = []
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

        probe_cap = max(1, len(candidates))
        conflict_probe = DynamicParallelismPolicy(max_parallelism=probe_cap).suggest(
            state, conflict_graph, epoch_id=epoch_id
        )
        unavailable.extend(conflict_probe.unavailable)
        # The conflict-free antichain over the whole frontier, minus any task
        # whose upstream is still churning.  ``selected_task_ids`` is already
        # validity/active/occupancy-safe and conflict-free.
        conflict_antichain = tuple(
            task_id for task_id in conflict_probe.selected_task_ids if task_id not in churning
        )
        conflict_headroom = len(conflict_antichain)

        resource_headroom_observed = task_resources is not None
        if resource_headroom_observed:
            resource_probe = ResourceAwareParallelismPolicy(max_parallelism=probe_cap).suggest(
                state, conflict_graph, task_resources, epoch_id=epoch_id
            )
            unavailable.extend(resource_probe.unavailable)
            resource_selected = {
                task_id for task_id in resource_probe.selected_task_ids if task_id not in churning
            }
            # Preserve the conflict-antichain ordering; resource fitting only
            # filters, it does not re-rank.
            runnable = tuple(
                task_id for task_id in conflict_antichain if task_id in resource_selected
            )
            resource_headroom = len(resource_selected)
        else:
            runnable = conflict_antichain
            resource_headroom = 1
            unavailable.append(
                UnavailableField(
                    name="resource_headroom",
                    reason=(
                        "per-task resource requests were not provided; logical headroom is "
                        "unobservable, so the degree is held at serial (fail-closed)"
                    ),
                )
            )

        contention_backoff = _contention_backoff(state.recent_events, exclude=churning)

        ceiling = int(self.max_parallelism)
        # Each hard cap is an independent upper bound on how many tasks it is
        # safe to run.  ``raw`` is their minimum; contention only shrinks it.
        raw = min(ceiling, conflict_headroom, resource_headroom)
        fail_closed = (
            closed
            or not cognition_available
            or not resources_available
            or conflict_probe.parallelism_hint == 0
        )
        if fail_closed or not stable_candidates or raw <= 0:
            chosen_degree = 0
        else:
            chosen_degree = max(1, raw - contention_backoff)

        degree_reason = _degree_reason(
            chosen_degree=chosen_degree,
            raw=raw,
            ceiling=ceiling,
            conflict_headroom=conflict_headroom,
            resource_headroom=resource_headroom,
            resource_headroom_observed=resource_headroom_observed,
            closed=closed,
            cognition_available=cognition_available,
            resources_available=resources_available,
            conflict_probe_hint=conflict_probe.parallelism_hint,
            has_candidates=bool(candidates),
            has_stable_candidates=bool(stable_candidates),
        )

        dispatch = runnable[:chosen_degree]
        bounds = _bounds(
            ceiling=ceiling,
            conflict_headroom=conflict_headroom,
            resource_headroom=resource_headroom,
            resource_headroom_observed=resource_headroom_observed,
            contention_backoff=contention_backoff,
            stable_candidate_count=len(stable_candidates),
            degree_reason=degree_reason,
        )

        safe = bool(
            not fail_closed
            and resource_headroom_observed
            and conflict_probe.safe_under_declared_accesses
        )
        unavailable_tuple = _dedupe_unavailable(unavailable)
        payload = {
            "schema_version": ADAPTIVE_PARALLELISM_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "policy_id": self.policy_id,
            "graph_id": state.graph_id,
            "graph_version": state.progress.graph_version,
            "projection_hash": state.progress.projection_hash,
            "conflict_graph_hash": conflict_graph.graph_hash,
            "ceiling": ceiling,
            "chosen_degree": chosen_degree,
            "degree_reason": degree_reason,
            "contention_backoff": contention_backoff,
            "bounds": bounds,
            "candidate_task_ids": candidates,
            "frontier_antichain": runnable,
            "dispatch_task_ids": dispatch,
            "deferred_for_churn": deferred_for_churn,
            "stability": stability,
            "resource_headroom_observed": resource_headroom_observed,
            "safe": safe,
            "unavailable": unavailable_tuple,
        }
        return AdaptiveParallelismDecision(
            epoch_id=epoch_id,
            policy_id=self.policy_id,
            graph_id=state.graph_id,
            graph_version=state.progress.graph_version,
            projection_hash=state.progress.projection_hash,
            conflict_graph_hash=conflict_graph.graph_hash,
            ceiling=ceiling,
            chosen_degree=chosen_degree,
            degree_reason=degree_reason,
            contention_backoff=contention_backoff,
            bounds=bounds,
            candidate_task_ids=candidates,
            frontier_antichain=runnable,
            dispatch_task_ids=dispatch,
            deferred_for_churn=deferred_for_churn,
            stability=stability,
            resource_headroom_observed=resource_headroom_observed,
            safe=safe,
            unavailable=unavailable_tuple,
            decision_hash=_hash_payload(payload),
        )

    plan = decide

    def _verdict_for(
        self,
        task_id: str,
        recent_events: tuple[RecentRuntimeEventState, ...],
        upstream_index: dict[str, frozenset[str]],
    ) -> UpstreamStabilityVerdict:
        relevant = {task_id} | upstream_index.get(task_id, frozenset())
        matched = [
            event
            for event in recent_events
            if event.event_type in _UPSTREAM_CHANGE_EVENT_TYPES
            and ((event.node_id in relevant) or (event.subject_id in relevant))
        ]
        event_count = len(matched)
        versions = {event.graph_version for event in matched if event.graph_version is not None}
        version_count = len(versions)
        by_versions = version_count >= int(self.churn_version_threshold)
        by_events = event_count >= int(self.churn_event_threshold)
        evidence = tuple(
            UpstreamChurnEvent(
                event_id=event.event_id,
                event_type=event.event_type,
                graph_version=event.graph_version,
                node_id=event.node_id,
            )
            for event in sorted(matched, key=_event_sort_key)
        )
        if by_versions:
            return UpstreamStabilityVerdict(
                task_id=task_id,
                action=FrontierAction.DEFER,
                stable=False,
                reason="upstream_churn_versions",
                churn_event_count=event_count,
                churn_version_count=version_count,
                evidence=evidence,
            )
        if by_events:
            return UpstreamStabilityVerdict(
                task_id=task_id,
                action=FrontierAction.DEFER,
                stable=False,
                reason="upstream_churn_events",
                churn_event_count=event_count,
                churn_version_count=version_count,
                evidence=evidence,
            )
        return UpstreamStabilityVerdict(
            task_id=task_id,
            action=FrontierAction.RUN,
            stable=True,
            reason="upstream_stable",
            churn_event_count=event_count,
            churn_version_count=version_count,
            evidence=evidence,
        )


def decide_parallelism(
    state: GlobalRuntimeState,
    conflict_graph: ConflictGraph,
    *,
    task_resources: Mapping[str, Any] | Iterable[ResourceTaskRequest] | None = None,
    upstream: Mapping[str, Iterable[str]] | None = None,
    epoch_id: int = 0,
    max_parallelism: int = 1,
    churn_version_threshold: int = DEFAULT_CHURN_VERSION_THRESHOLD,
    churn_event_threshold: int = DEFAULT_CHURN_EVENT_THRESHOLD,
) -> AdaptiveParallelismDecision:
    """Convenience wrapper for one adaptive parallelism/stability decision."""

    return AdaptiveParallelismPolicy(
        max_parallelism=max_parallelism,
        churn_version_threshold=churn_version_threshold,
        churn_event_threshold=churn_event_threshold,
    ).decide(
        state,
        conflict_graph,
        task_resources=task_resources,
        upstream=upstream,
        epoch_id=epoch_id,
    )


def _candidate_ids(state: GlobalRuntimeState) -> tuple[str, ...]:
    ready = _normalize_ids(state.progress.ready_frontier)
    repair = _normalize_ids(state.progress.repair_ready_frontier)
    return tuple(sorted(set(ready) | set(repair)))


def _normalize_ids(values: Any) -> tuple[str, ...]:
    return tuple(sorted({str(value).strip() for value in (values or ()) if str(value).strip()}))


def _normalize_upstream(
    upstream: Mapping[str, Iterable[str]] | None,
) -> dict[str, frozenset[str]]:
    if upstream is None:
        return {}
    index: dict[str, frozenset[str]] = {}
    for task_id, nodes in upstream.items():
        normalized_task = str(task_id).strip()
        if not normalized_task:
            continue
        node_ids = {str(node).strip() for node in (nodes or ()) if str(node).strip()}
        if node_ids:
            index[normalized_task] = frozenset(node_ids)
    return index


def _contention_backoff(
    recent_events: tuple[RecentRuntimeEventState, ...],
    *,
    exclude: set[str],
) -> int:
    """Count distinct task nodes reworked in the observed window.

    This is the "how often recent dispatches ended in quarantine/rework"
    signal: a verified task that is re-derived stale/invalid/reopened is work
    that was thrown away.  Broader rework lowers the chosen degree.  Frontier
    tasks already deferred for churn are excluded so that per-task deferral and
    the global backoff do not penalise the same instability twice.
    """

    reworked: set[str] = set()
    for event in recent_events:
        if event.event_type in _REWORK_EVENT_TYPES:
            node_id = event.node_id or event.subject_id
            if node_id and str(node_id) not in exclude:
                reworked.add(str(node_id))
    return len(reworked)


def _degree_reason(
    *,
    chosen_degree: int,
    raw: int,
    ceiling: int,
    conflict_headroom: int,
    resource_headroom: int,
    resource_headroom_observed: bool,
    closed: bool,
    cognition_available: bool,
    resources_available: bool,
    conflict_probe_hint: int,
    has_candidates: bool,
    has_stable_candidates: bool,
) -> str:
    if chosen_degree == 0:
        if closed:
            return "closed"
        if not cognition_available:
            return "cognition_unavailable"
        if not resources_available:
            return "resources_unavailable"
        if not has_candidates:
            return "no_candidates"
        if not has_stable_candidates:
            return "all_candidates_churning"
        if conflict_probe_hint == 0:
            return "frontier_fail_closed"
        if resource_headroom_observed and resource_headroom == 0:
            return "resource_exhausted"
        if conflict_headroom == 0:
            return "no_conflict_free_antichain"
        return "deferred"
    if chosen_degree < raw:
        return "contention_backoff"
    # ``chosen_degree == raw``: the binding constraint is the smallest hard cap.
    if (
        not resource_headroom_observed
        and resource_headroom == raw
        and resource_headroom <= conflict_headroom
        and resource_headroom <= ceiling
    ):
        return "resource_requests_unavailable_serial"
    if (
        conflict_headroom == raw
        and conflict_headroom <= resource_headroom
        and conflict_headroom <= ceiling
    ):
        return "conflict_antichain"
    if resource_headroom == raw and resource_headroom <= ceiling:
        return "resource_headroom"
    return "ceiling"


def _bounds(
    *,
    ceiling: int,
    conflict_headroom: int,
    resource_headroom: int,
    resource_headroom_observed: bool,
    contention_backoff: int,
    stable_candidate_count: int,
    degree_reason: str,
) -> tuple[ParallelismBound, ...]:
    return (
        ParallelismBound(
            name="ceiling",
            value=ceiling,
            observed=True,
            binding=degree_reason == "ceiling",
        ),
        ParallelismBound(
            name="conflict_free_antichain",
            value=conflict_headroom,
            observed=True,
            binding=degree_reason in {"conflict_antichain", "no_conflict_free_antichain"},
        ),
        ParallelismBound(
            name="resource_headroom",
            value=resource_headroom,
            observed=resource_headroom_observed,
            binding=degree_reason
            in {
                "resource_headroom",
                "resource_exhausted",
                "resource_requests_unavailable_serial",
            },
        ),
        ParallelismBound(
            name="contention_backoff",
            value=contention_backoff,
            observed=True,
            binding=degree_reason == "contention_backoff",
        ),
        ParallelismBound(
            name="stable_candidates",
            value=stable_candidate_count,
            observed=True,
            binding=degree_reason == "all_candidates_churning",
        ),
    )


def _event_sort_key(event: RecentRuntimeEventState) -> tuple[int, str, str]:
    return (
        event.graph_version if event.graph_version is not None else -1,
        event.event_type,
        event.event_id,
    )


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
    if isinstance(value, StrEnum):
        return value.value
    return value


__all__ = [
    "ADAPTIVE_PARALLELISM_POLICY_ID",
    "ADAPTIVE_PARALLELISM_SCHEMA_VERSION",
    "DEFAULT_CHURN_EVENT_THRESHOLD",
    "DEFAULT_CHURN_VERSION_THRESHOLD",
    "AdaptiveParallelismDecision",
    "AdaptiveParallelismPolicy",
    "ParallelismBound",
    "UpstreamChurnEvent",
    "UpstreamStabilityVerdict",
    "decide_parallelism",
]
