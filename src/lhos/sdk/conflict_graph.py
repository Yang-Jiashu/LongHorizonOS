"""Opt-in conflict-aware parallelism suggestions.

This module deliberately implements a *small* piece of the LongHorizonOS
online-compute-management design:

* ``ConflictGraph`` derives pairwise conflicts from explicitly declared
  task read/write sets;
* ``DynamicParallelismPolicy`` greedily proposes a deterministic independent
  batch from the observed runtime frontier.

It does not claim that the declarations are complete, does not discover
provenance, and does not call the Scheduler.  An absent or unknown access set
is treated conservatively: that task may run alone, but it is never grouped
with another task.  The existing Scheduler remains authoritative for
eligibility, resource admission, claims, leases, fencing, and execution.
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
    model_validator,
)

from .frontier_policy import FrontierAction
from .runtime_state import GlobalRuntimeState, UnavailableField

CONFLICT_GRAPH_SCHEMA_VERSION: Final[Literal["conflict-graph.v1"]] = "conflict-graph.v1"
PARALLELISM_SUGGESTION_SCHEMA_VERSION: Final[Literal["parallelism-suggestion.v1"]] = (
    "parallelism-suggestion.v1"
)
DYNAMIC_PARALLELISM_POLICY_ID: Final[str] = "conflict-aware-parallelism.v1"


class ConflictReason(StrEnum):
    """Why two task accesses cannot be safely grouped."""

    READ_WRITE = "read_write"
    WRITE_WRITE = "write_write"
    UNKNOWN_ACCESS = "unknown_access"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class TaskAccessSet(_FrozenModel):
    """Explicit access declaration for one task.

    Resource names are opaque exact-match identifiers (for example
    ``artifact://api`` or ``workspace://src/payment.py``).  Wildcard and
    semantic path matching are intentionally out of scope for this primitive.
    ``known=False`` means the declaration must not be trusted for parallel
    execution.
    """

    task_id: str = Field(min_length=1)
    read_set: tuple[str, ...] = ()
    write_set: tuple[str, ...] = ()
    known: StrictBool = True

    @field_validator("read_set", "write_set", mode="before")
    @classmethod
    def _normalize_resources(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            value = (value,)
        try:
            values = tuple(value)
        except TypeError as exc:
            raise TypeError("access sets must be iterable strings") from exc
        normalized: set[str] = set()
        for item in values:
            if not isinstance(item, str):
                raise ValueError("access-set resource identifiers must be strings")
            item = item.strip()
            if item:
                normalized.add(item)
        return tuple(sorted(normalized))


# Friendly singular alias for callers that prefer ``TaskAccess``.
TaskAccess = TaskAccessSet


class ConflictPair(_FrozenModel):
    """Canonical undirected conflict between two distinct task ids."""

    left_task_id: str = Field(min_length=1)
    right_task_id: str = Field(min_length=1)
    resources: tuple[str, ...] = ()
    reasons: tuple[ConflictReason, ...] = ()

    @field_validator("resources", mode="before")
    @classmethod
    def _normalize_resources(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return ()
        return tuple(sorted({str(item).strip() for item in value if str(item).strip()}))

    @field_validator("reasons", mode="before")
    @classmethod
    def _normalize_reasons(cls, value: Any) -> tuple[ConflictReason, ...]:
        if value is None:
            return ()
        return tuple(sorted({ConflictReason(item) for item in value}, key=lambda item: item.value))

    @model_validator(mode="after")
    def _canonical_endpoints(self) -> ConflictPair:
        if self.left_task_id >= self.right_task_id:
            raise ValueError("conflict pair endpoints must be distinct and lexically ordered")
        if not self.reasons:
            raise ValueError("conflict pair requires at least one reason")
        return self


class ConflictGraph(_FrozenModel):
    """Immutable conflict graph derived from declared task access sets."""

    schema_version: Literal["conflict-graph.v1"] = CONFLICT_GRAPH_SCHEMA_VERSION
    access_sets: tuple[TaskAccessSet, ...] = ()
    conflicts: tuple[ConflictPair, ...] = ()
    unknown_task_ids: tuple[str, ...] = ()
    graph_hash: str = Field(min_length=64, max_length=64)

    @classmethod
    def from_access_sets(
        cls,
        access_sets: Iterable[TaskAccessSet | Mapping[str, Any]],
    ) -> ConflictGraph:
        """Build a canonical graph from explicit declarations.

        Task ids must be unique.  Missing declarations are handled later by
        the parallelism policy as unknown access; the graph itself only
        contains the declarations supplied here.
        """

        normalized: list[TaskAccessSet] = []
        for item in access_sets:
            normalized.append(
                item if isinstance(item, TaskAccessSet) else TaskAccessSet.model_validate(item)
            )
        by_id: dict[str, TaskAccessSet] = {}
        for item in normalized:
            if item.task_id in by_id:
                raise ValueError(f"duplicate task access declaration: {item.task_id!r}")
            by_id[item.task_id] = item
        ordered = tuple(by_id[task_id] for task_id in sorted(by_id))
        conflicts = _derive_conflicts(ordered)
        unknown = tuple(sorted(item.task_id for item in ordered if not item.known))
        graph_hash = _hash_payload(
            {
                "schema_version": CONFLICT_GRAPH_SCHEMA_VERSION,
                "access_sets": ordered,
                "conflicts": conflicts,
                "unknown_task_ids": unknown,
            }
        )
        return cls(
            access_sets=ordered,
            conflicts=conflicts,
            unknown_task_ids=unknown,
            graph_hash=graph_hash,
        )

    build = from_access_sets

    @property
    def task_ids(self) -> tuple[str, ...]:
        return tuple(item.task_id for item in self.access_sets)

    def access_for(self, task_id: str) -> TaskAccessSet | None:
        """Return the declared access set, or ``None`` when absent."""

        normalized = str(task_id).strip()
        return next((item for item in self.access_sets if item.task_id == normalized), None)

    def conflicts_with(self, left_task_id: str, right_task_id: str) -> bool:
        """Return whether two distinct ids have a declared conflict."""

        left, right = sorted((str(left_task_id), str(right_task_id)))
        if left == right:
            return False
        return any(
            pair.left_task_id == left and pair.right_task_id == right for pair in self.conflicts
        )

    def conflict_pairs_for(self, task_id: str) -> tuple[ConflictPair, ...]:
        normalized = str(task_id).strip()
        return tuple(
            pair
            for pair in self.conflicts
            if pair.left_task_id == normalized or pair.right_task_id == normalized
        )


class ParallelTaskDecision(_FrozenModel):
    """One deterministic batch decision and its selected-task blockers."""

    task_id: str = Field(min_length=1)
    action: FrontierAction
    reason: str = Field(min_length=1)
    blockers: tuple[str, ...] = ()


class ParallelBatchSuggestion(_FrozenModel):
    """Immutable conflict-aware batch suggestion for one planning epoch."""

    schema_version: Literal["parallelism-suggestion.v1"] = PARALLELISM_SUGGESTION_SCHEMA_VERSION
    epoch_id: StrictInt = Field(ge=0)
    policy_id: str = Field(min_length=1)
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    projection_hash: str = Field(min_length=1)
    conflict_graph_hash: str = Field(min_length=64, max_length=64)
    candidate_task_ids: tuple[str, ...] = ()
    selected_task_ids: tuple[str, ...] = ()
    deferred_task_ids: tuple[str, ...] = ()
    decisions: tuple[ParallelTaskDecision, ...] = ()
    parallelism_hint: StrictInt = Field(ge=0)
    safe_under_declared_accesses: StrictBool = True
    unavailable: tuple[UnavailableField, ...] = ()
    decision_hash: str = Field(min_length=64, max_length=64)

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class DynamicParallelismPolicy(_FrozenModel):
    """Greedy independent-set suggestion over an observed VPG frontier.

    The policy is deterministic, conservative, and opt-in.  It intentionally
    does not perform resource fitting: ``max_parallelism`` is only a caller
    supplied upper bound, while the Scheduler still performs admission.
    """

    max_parallelism: StrictInt = Field(default=1, ge=1)
    policy_id: str = DYNAMIC_PARALLELISM_POLICY_ID

    @field_validator("max_parallelism")
    @classmethod
    def _require_parallelism_int(cls, value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("max_parallelism must be an integer")
        return value

    def suggest(
        self,
        state: GlobalRuntimeState,
        conflict_graph: ConflictGraph,
        *,
        epoch_id: int = 0,
    ) -> ParallelBatchSuggestion:
        """Return a deterministic conflict-aware batch without side effects."""

        if not isinstance(state, GlobalRuntimeState):
            raise TypeError("state must be a GlobalRuntimeState/RuntimeStateView")
        if not isinstance(conflict_graph, ConflictGraph):
            raise TypeError("conflict_graph must be a ConflictGraph")
        if isinstance(epoch_id, bool) or not isinstance(epoch_id, int) or epoch_id < 0:
            raise ValueError("epoch_id must be a non-negative integer")

        ready = _normalize_ids(state.progress.ready_frontier)
        repair = _normalize_ids(state.progress.repair_ready_frontier)
        repair_set = set(repair)
        # Every query below is answered by scanning a tuple on the model:
        # ``access_for`` walks the access sets and ``conflicts_with`` walks the
        # conflict pairs.  Both are called once per candidate, and
        # ``conflicts_with`` additionally once per already-selected task, so the
        # pass degraded to O(candidates^2 * conflicts).  Index once per pass
        # instead.  The indexes are deliberately local: ``ConflictGraph`` is a
        # frozen model whose payload is hashed, and caching on the instance would
        # both risk entering that payload and go stale across ``model_copy``.
        access_by_task = {item.task_id: item for item in conflict_graph.access_sets}
        conflict_neighbours: dict[str, set[str]] = {}
        for pair in conflict_graph.conflicts:
            conflict_neighbours.setdefault(pair.left_task_id, set()).add(pair.right_task_id)
            conflict_neighbours.setdefault(pair.right_task_id, set()).add(pair.left_task_id)

        def _access_of(task_id: str) -> TaskAccessSet | None:
            return access_by_task.get(str(task_id).strip())

        def _conflicts(left_task_id: str, right_task_id: str) -> bool:
            if left_task_id == right_task_id:
                return False
            return right_task_id in conflict_neighbours.get(left_task_id, ())

        candidates = tuple(
            sorted(
                set(ready) | repair_set,
                key=lambda task_id: (
                    0 if task_id in repair_set else 1,
                    0 if _access_of(task_id) is not None else 1,
                    task_id,
                ),
            )
        )
        verified = set(_normalize_ids(state.progress.verified_task_ids))
        invalid = set(_normalize_ids(state.progress.invalid_task_ids))
        stale = set(_normalize_ids(state.progress.stale_task_ids))
        active = {
            attempt.task_id for attempt in state.agent_cognition.current_attempts if attempt.task_id
        }
        active_accesses, active_access_unavailable = _active_access_views(
            state.agent_cognition.current_attempts
        )
        cognition_available = bool(state.agent_cognition.available)
        resources_available = bool(state.resources.available)
        closed = bool(state.progress.graph_closed or state.progress.goal_closed)
        unavailable: list[UnavailableField] = []
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
        unavailable.extend(active_access_unavailable)

        selected: list[str] = []
        deferred: list[str] = []
        decisions: list[ParallelTaskDecision] = []
        for task_id in candidates:
            access = _access_of(task_id)
            if access is None or not access.known:
                unavailable.append(
                    UnavailableField(
                        name=f"conflict_graph.{task_id}",
                        reason="task access set is absent or unknown; serial-only",
                    )
                )
            action = FrontierAction.DEFER
            reason = "ready"
            blockers: tuple[str, ...] = ()
            if closed:
                reason = "closed"
            elif not cognition_available:
                reason = "cognition_unavailable"
            elif not resources_available:
                reason = "resources_unavailable"
            elif active_access_unavailable:
                reason = "active_access_unknown"
            elif task_id in verified or task_id in invalid:
                reason = "terminal_validity"
            elif task_id in stale and task_id not in repair_set:
                reason = "stale_not_repair_ready"
            elif task_id in active:
                reason = "active_attempt"
            elif access is None or not access.known:
                if selected:
                    blockers = tuple(selected)
                    reason = "unknown_access_serial_only"
                else:
                    active_blockers = tuple(
                        f"active:{label}" for label, _reads, _writes, _known in active_accesses
                    )
                    if active_blockers:
                        blockers = active_blockers
                        reason = "unknown_access_active_occupancy"
                    else:
                        selected.append(task_id)
                        action = FrontierAction.RUN
                        reason = "unknown_access_serial_only"
            else:
                selected_blockers = tuple(
                    chosen for chosen in selected if _conflicts(task_id, chosen)
                )
                active_blockers = _active_conflict_blockers(access, active_accesses)
                blockers = selected_blockers + active_blockers
                if blockers:
                    reason = "active_conflict" if active_blockers else "conflict"
                elif len(selected) >= self.max_parallelism:
                    reason = "max_parallelism"
                else:
                    selected.append(task_id)
                    action = FrontierAction.RUN
                    reason = "independent"
            if action is FrontierAction.DEFER:
                deferred.append(task_id)
            decisions.append(
                ParallelTaskDecision(
                    task_id=task_id,
                    action=action,
                    reason=reason,
                    blockers=blockers,
                )
            )

        safe = bool(
            cognition_available
            and resources_available
            and not active_access_unavailable
            and all(
                (access := _access_of(task_id)) is not None and access.known for task_id in selected
            )
        )
        unavailable_tuple: tuple[UnavailableField, ...] = _dedupe_unavailable(unavailable)
        parallelism_hint: int = (
            0
            if not (cognition_available and resources_available and not active_access_unavailable)
            else len(selected)
        )
        payload = {
            "schema_version": PARALLELISM_SUGGESTION_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "policy_id": self.policy_id,
            "graph_id": state.graph_id,
            "graph_version": state.progress.graph_version,
            "projection_hash": state.progress.projection_hash,
            "conflict_graph_hash": conflict_graph.graph_hash,
            "candidate_task_ids": candidates,
            "selected_task_ids": tuple(selected),
            "deferred_task_ids": tuple(deferred),
            "decisions": tuple(decisions),
            "parallelism_hint": parallelism_hint,
            "safe_under_declared_accesses": safe,
            "unavailable": unavailable_tuple,
        }
        decision_hash = _hash_payload(payload)
        return ParallelBatchSuggestion(
            schema_version=PARALLELISM_SUGGESTION_SCHEMA_VERSION,
            epoch_id=epoch_id,
            policy_id=self.policy_id,
            graph_id=state.graph_id,
            graph_version=state.progress.graph_version,
            projection_hash=state.progress.projection_hash,
            conflict_graph_hash=conflict_graph.graph_hash,
            candidate_task_ids=candidates,
            selected_task_ids=tuple(selected),
            deferred_task_ids=tuple(deferred),
            decisions=tuple(decisions),
            parallelism_hint=parallelism_hint,
            safe_under_declared_accesses=safe,
            unavailable=unavailable_tuple,
            decision_hash=decision_hash,
        )

    plan = suggest


def suggest_parallel_batch(
    state: GlobalRuntimeState,
    conflict_graph: ConflictGraph,
    *,
    epoch_id: int = 0,
    max_parallelism: int = 1,
) -> ParallelBatchSuggestion:
    """Convenience wrapper for an opt-in conflict-aware suggestion."""

    return DynamicParallelismPolicy(max_parallelism=max_parallelism).suggest(
        state, conflict_graph, epoch_id=epoch_id
    )


def _normalize_ids(values: Any) -> tuple[str, ...]:
    return tuple(sorted({str(value).strip() for value in (values or ()) if str(value).strip()}))


def _active_access_views(
    attempts: Iterable[Any],
) -> tuple[
    tuple[tuple[str, tuple[str, ...], tuple[str, ...], bool], ...],
    tuple[UnavailableField, ...],
]:
    """Project active cognition access sets for conservative batch planning.

    A current attempt with no captured AgentSnapshot is an unknown occupancy
    boundary.  We do not infer that it is conflict-free; instead the policy
    emits an unavailable field and returns no proposed parallel batch.
    """

    views: list[tuple[str, tuple[str, ...], tuple[str, ...], bool]] = []
    unavailable: list[UnavailableField] = []
    for attempt in attempts:
        attempt_id = str(attempt.attempt_id or attempt.claim_id or attempt.task_id)
        reads = tuple(
            sorted(
                {
                    key
                    for binding in attempt.read_set
                    for key in (_binding_identity(binding),)
                    if key is not None
                }
            )
        )
        writes = tuple(
            sorted(
                {
                    key
                    for binding in attempt.write_set
                    for key in (_binding_identity(binding),)
                    if key is not None
                }
            )
        )
        bindings = (*attempt.read_set, *attempt.write_set)
        unavailable_names = {item.name for item in attempt.unavailable}
        # An explicitly captured AgentSnapshot may legitimately have empty
        # read/write sets.  Empty is known-empty unless the projection marked
        # either field unavailable (the pre-Snapshot/legacy case).
        known = not ({"read_set", "write_set"} & unavailable_names) and all(
            bool(binding.known) and _binding_identity(binding) is not None for binding in bindings
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
    """Return the best exact identity available for one runtime binding."""

    for field in ("resource_uri", "artifact_id", "action_id", "source_event_id"):
        value = str(getattr(binding, field, "") or "").strip()
        if value:
            return value
    return None


def _active_conflict_blockers(
    candidate: TaskAccessSet,
    active_accesses: tuple[tuple[str, tuple[str, ...], tuple[str, ...], bool], ...],
) -> tuple[str, ...]:
    """Return active attempt ids whose declared accesses conflict."""

    blockers: list[str] = []
    candidate_reads = set(candidate.read_set)
    candidate_writes = set(candidate.write_set)
    for attempt_id, reads, writes, known in active_accesses:
        if not known:
            blockers.append(f"active:{attempt_id}")
            continue
        if candidate_writes & (set(reads) | set(writes)) or candidate_reads & set(writes):
            blockers.append(f"active:{attempt_id}")
    return tuple(sorted(set(blockers)))


def _pair(left: int, right: int) -> tuple[int, int]:
    return (left, right) if left < right else (right, left)


def _conflict_candidates(access_sets: tuple[TaskAccessSet, ...]) -> set[tuple[int, int]]:
    """Index-pair candidates that could possibly conflict.

    Comparing every pair is O(T^2) even though two tasks can only conflict
    through a resource they *share*, so the access sets are inverted instead:
    ``resource -> writers`` and ``resource -> readers``.  Candidates then come
    only from tasks meeting on the same resource, which is O(sum of per-resource
    degrees) -- near-linear when resources are not shared by everything, and no
    worse than the pairwise scan when they are.

    An unknown access set is the one genuinely quadratic case: it conflicts with
    every other task by definition, so the output itself is O(T^2) and no
    indexing can shrink it.
    """

    total = len(access_sets)
    candidates: set[tuple[int, int]] = set()
    for index, access in enumerate(access_sets):
        if access.known:
            continue
        for other in range(total):
            if other != index:
                candidates.add(_pair(index, other))

    writers: dict[str, list[int]] = {}
    readers: dict[str, list[int]] = {}
    for index, access in enumerate(access_sets):
        if not access.known:
            continue
        for resource in access.write_set:
            writers.setdefault(resource, []).append(index)
        for resource in access.read_set:
            readers.setdefault(resource, []).append(index)

    for resource, writing in writers.items():
        for position, left in enumerate(writing):
            for right in writing[position + 1 :]:
                candidates.add(_pair(left, right))
            for reader in readers.get(resource, ()):
                if reader != left:
                    candidates.add(_pair(left, reader))
    return candidates


def _derive_conflicts(access_sets: tuple[TaskAccessSet, ...]) -> tuple[ConflictPair, ...]:
    """Pairwise conflicts, emitted in ascending (left index, right index) order.

    The emission order is part of the derived graph's identity because the
    payload is hashed, so candidates are sorted back into the same order the
    original pairwise scan produced.
    """

    pairs: list[ConflictPair] = []
    for left_index, right_index in sorted(_conflict_candidates(access_sets)):
        left = access_sets[left_index]
        right = access_sets[right_index]
        reasons: set[ConflictReason] = set()
        resources: set[str] = set()
        if not left.known or not right.known:
            reasons.add(ConflictReason.UNKNOWN_ACCESS)
        else:
            ww = set(left.write_set) & set(right.write_set)
            rw = (set(left.write_set) & set(right.read_set)) | (
                set(right.write_set) & set(left.read_set)
            )
            if ww:
                reasons.add(ConflictReason.WRITE_WRITE)
                resources.update(ww)
            if rw:
                reasons.add(ConflictReason.READ_WRITE)
                resources.update(rw)
        if reasons:
            pairs.append(
                ConflictPair(
                    left_task_id=left.task_id,
                    right_task_id=right.task_id,
                    resources=tuple(sorted(resources)),
                    reasons=tuple(sorted(reasons, key=lambda item: item.value)),
                )
            )
    return tuple(pairs)


def _dedupe_unavailable(
    values: Iterable[UnavailableField],
) -> tuple[UnavailableField, ...]:
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
    "CONFLICT_GRAPH_SCHEMA_VERSION",
    "DYNAMIC_PARALLELISM_POLICY_ID",
    "PARALLELISM_SUGGESTION_SCHEMA_VERSION",
    "ConflictGraph",
    "ConflictPair",
    "ConflictReason",
    "DynamicParallelismPolicy",
    "ParallelBatchSuggestion",
    "ParallelTaskDecision",
    "TaskAccess",
    "TaskAccessSet",
    "suggest_parallel_batch",
]
