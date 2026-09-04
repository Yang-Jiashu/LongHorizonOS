"""Read-only projection of the five scheduler wastes from durable state.

LongHorizonOS's objective is to maximize verified progress per unit of
token / time / cost by reducing five specific wastes:

1. re-reading context that was already resident (重复读 Context)
2. redundant reasoning / repeated attempts on unchanged input (重复 reasoning)
3. premature parallelism -- work dispatched before its inputs were stable
   and therefore thrown away (过早并行)
4. continuing in a wrong direction -- work that kept running against a
   superseded graph version (错误方向继续执行)
5. stale computation / avoidable rework (不必要的返工)

These five quantities are the *denominator* of the whole objective, yet they
were previously only referenced inside offline benchmark scenarios and never
observed from a real run.  This module derives them, deterministically and
read-only, from state the runtime already persists:

* ``AgentOS.scheduler.attempts`` -- ``ScheduledExecutionAttempt`` records
  (state, ``graph_version``, ``semantic_epoch``, ``attempt_number`` and, when
  captured, an ``AgentSnapshot`` with read/write sets).
* ``AgentOS.usage_ledger`` -- ``MEASURED`` per-attempt usage, keyed by
  ``claim_id`` (see :func:`_ledger_lookup`).
* ``AgentOS.runtime_state(goal)`` -- the pinned VPG identity / validity view.

The projection is **pure and deterministic**: it never reads the wall clock
and never uses attempt ``started_at`` / ``ended_at`` timestamps in any hash or
decision.  It is **fail-closed**: a quantity that cannot be observed from
existing state is reported ``unavailable`` (``None`` totals + an
:class:`UnavailableField`) rather than as a fabricated ``0``.  A measured
``0`` (observable, and genuinely no waste) is reported as ``0`` and is always
distinguishable from ``unavailable``.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping
from enum import Enum, StrEnum
from typing import Any, Final, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field

from .runtime_state import UnavailableField

WASTE_PROJECTION_SCHEMA_VERSION: Final[Literal["waste-projection.v1"]] = "waste-projection.v1"

# Bounded id lists so a pathological graph cannot produce an unbounded model.
_MAX_DETAIL_IDS: Final[int] = 64

# Terminal, immutable "good" outcome -- never a waste.
_VERIFIED_STATE: Final[str] = "verified_semantically"
# Explicit quarantine of an attempt whose bound inputs were no longer current.
_STALE_COGNITION_STATE: Final[str] = "stale_cognition"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class WasteDimension(StrEnum):
    """The five wastes that form the denominator of the objective."""

    REREAD_CONTEXT = "reread_context"
    REPEATED_REASONING = "repeated_reasoning"
    PREMATURE_PARALLELISM = "premature_parallelism"
    WRONG_DIRECTION_CONTINUATION = "wrong_direction_continuation"
    STALE_REWORK = "stale_rework"


class WasteAttribution(_FrozenModel):
    """Per-task attribution for one waste dimension.

    ``count`` is always observable from durable attempt state.  The measured
    magnitudes (``tokens`` / ``cost_microusd`` / ``wall_time_ms``) are ``None``
    when they cannot be observed for this task, never a fabricated ``0``.
    """

    task_id: str
    count: int = Field(ge=0)
    tokens: int | None = Field(default=None, ge=0)
    cost_microusd: int | None = Field(default=None, ge=0)
    wall_time_ms: int | None = Field(default=None, ge=0)
    detail_ids: tuple[str, ...] = ()
    unavailable: tuple[UnavailableField, ...] = ()


class WasteDimensionReport(_FrozenModel):
    """Projection of one waste dimension across the goal's graph.

    ``observable`` is ``False`` only when the dimension cannot be computed at
    all from current durable state (for example an unobservable dimension, or a
    read-only ``AgentOS`` whose scheduler attempts are not loaded).  When
    ``observable`` is ``True``, ``total_count`` is a measured integer (``0`` is
    a real "no waste" reading), while a ``None`` magnitude with a matching
    :class:`UnavailableField` marks a partially/entirely unobservable measure.
    """

    dimension: WasteDimension
    observable: bool
    reason: str | None = None
    total_count: int | None = Field(default=None, ge=0)
    total_tokens: int | None = Field(default=None, ge=0)
    total_cost_microusd: int | None = Field(default=None, ge=0)
    total_wall_time_ms: int | None = Field(default=None, ge=0)
    by_task: tuple[WasteAttribution, ...] = ()
    unavailable: tuple[UnavailableField, ...] = ()


class WasteProjection(_FrozenModel):
    """Immutable, deterministic projection of all five wastes for one goal."""

    schema_version: Literal["waste-projection.v1"] = WASTE_PROJECTION_SCHEMA_VERSION
    goal_id: str
    graph_id: str
    graph_version: int = Field(ge=0)
    projection_hash: str
    attempts_observed: int = Field(ge=0)
    attempts_with_snapshot: int = Field(ge=0)
    reread_context: WasteDimensionReport
    repeated_reasoning: WasteDimensionReport
    premature_parallelism: WasteDimensionReport
    wrong_direction_continuation: WasteDimensionReport
    stale_rework: WasteDimensionReport

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def dimension(self, dimension: WasteDimension | str) -> WasteDimensionReport:
        """Return the report for one dimension by enum or its string value."""

        key = WasteDimension(dimension) if not isinstance(dimension, WasteDimension) else dimension
        return {
            WasteDimension.REREAD_CONTEXT: self.reread_context,
            WasteDimension.REPEATED_REASONING: self.repeated_reasoning,
            WasteDimension.PREMATURE_PARALLELISM: self.premature_parallelism,
            WasteDimension.WRONG_DIRECTION_CONTINUATION: self.wrong_direction_continuation,
            WasteDimension.STALE_REWORK: self.stale_rework,
        }[key]


WasteProjectionView: TypeAlias = WasteProjection


def build_waste_projection(os_: Any, goal: Any) -> WasteProjection:
    """Build an immutable, deterministic, read-only five-waste projection.

    ``goal`` may be a compiled SDK Goal or its goal id.  An uncompiled/unknown
    goal fails closed via :meth:`AgentOS.runtime_state` because observation must
    never compile or mutate runtime state.  The projection is derived purely
    from durable Scheduler attempts, the measured-usage ledger, and the pinned
    VPG identity; it never reads the wall clock.
    """

    state = os_.runtime_state(goal)
    goal_id = str(state.goal_id)
    graph_id = str(state.graph_id)
    graph_version = int(state.progress.graph_version)

    read_only = bool(getattr(os_, "_read_only", False))
    scheduler = getattr(os_, "scheduler", None)
    attempts_available = scheduler is not None and not read_only
    unavailable_reason: str | None = None
    attempts: list[Any] = []
    if not attempts_available:
        unavailable_reason = (
            "scheduler durable state is not loaded by read-only AgentOS"
            if read_only
            else "AgentOS exposes no scheduler attempt surface"
        )
    else:
        raw = getattr(scheduler, "attempts", None) or ()
        attempts = [attempt for attempt in raw if str(getattr(attempt, "graph_id", "")) == graph_id]

    ledger_by_id = _ledger_measured_by_id(getattr(os_, "usage_ledger", None))

    attempts_with_snapshot = sum(
        1 for attempt in attempts if getattr(attempt, "agent_snapshot", None) is not None
    )

    if attempts_available:
        buckets = _classify_attempts(attempts)
        reread = _build_reread_report(attempts, attempts_with_snapshot)
        repeated = _build_measured_report(
            WasteDimension.REPEATED_REASONING,
            buckets.repeated,
            ledger_by_id,
        )
        repeated = _annotate_repeated_reasoning(repeated, buckets.repeated)
        premature = _build_measured_report(
            WasteDimension.PREMATURE_PARALLELISM,
            buckets.premature,
            ledger_by_id,
        )
        rework = _build_measured_report(
            WasteDimension.STALE_REWORK,
            buckets.rework,
            ledger_by_id,
        )
    else:
        reread = _unobservable_report(WasteDimension.REREAD_CONTEXT, unavailable_reason or "")
        repeated = _unobservable_report(WasteDimension.REPEATED_REASONING, unavailable_reason or "")
        premature = _unobservable_report(
            WasteDimension.PREMATURE_PARALLELISM, unavailable_reason or ""
        )
        rework = _unobservable_report(WasteDimension.STALE_REWORK, unavailable_reason or "")

    scheduler_events = getattr(scheduler, "events", ()) if attempts_available else ()
    wrong_direction = _wrong_direction_report(attempts, scheduler_events, ledger_by_id)

    identity = {
        "schema_version": WASTE_PROJECTION_SCHEMA_VERSION,
        "goal_id": goal_id,
        "graph_id": graph_id,
        "graph_version": graph_version,
        "attempts_observed": len(attempts),
        "attempts_with_snapshot": attempts_with_snapshot,
        "dimensions": [
            report.model_dump(mode="json")
            for report in (reread, repeated, premature, wrong_direction, rework)
        ],
    }
    projection_hash = _sha256_json(identity)

    return WasteProjection(
        goal_id=goal_id,
        graph_id=graph_id,
        graph_version=graph_version,
        projection_hash=projection_hash,
        attempts_observed=len(attempts),
        attempts_with_snapshot=attempts_with_snapshot,
        reread_context=reread,
        repeated_reasoning=repeated,
        premature_parallelism=premature,
        wrong_direction_continuation=wrong_direction,
        stale_rework=rework,
    )


class _AttemptBuckets:
    """Disjoint partition of waste-bearing attempts.

    An attempt contributes to at most one of these buckets so their measured
    token/cost/time magnitudes never double count.  The partition is priority
    ordered: a quarantined attempt is premature parallelism; an attempt
    superseded by a later semantic epoch of the same task is rework; a further
    attempt at the task's latest epoch is repeated reasoning.  Verified
    attempts and a single live/first attempt at the latest epoch are not waste.
    """

    __slots__ = ("premature", "repeated", "rework")

    def __init__(self) -> None:
        self.premature: list[Any] = []
        self.rework: list[Any] = []
        self.repeated: list[Any] = []


def _classify_attempts(attempts: list[Any]) -> _AttemptBuckets:
    buckets = _AttemptBuckets()
    max_epoch_by_task: dict[str, int] = {}
    groups: dict[tuple[str, int], list[Any]] = defaultdict(list)
    for attempt in attempts:
        task_id = _task_id(attempt)
        epoch = _semantic_epoch(attempt)
        max_epoch_by_task[task_id] = max(max_epoch_by_task.get(task_id, epoch), epoch)
        groups[(task_id, epoch)].append(attempt)
    first_id_by_group: dict[tuple[str, int], str] = {}
    for key, members in groups.items():
        members.sort(key=lambda item: (_attempt_number(item), _attempt_id(item)))
        first_id_by_group[key] = _attempt_id(members[0])

    for attempt in attempts:
        state = _attempt_state(attempt)
        if state == _VERIFIED_STATE:
            continue
        task_id = _task_id(attempt)
        epoch = _semantic_epoch(attempt)
        if state == _STALE_COGNITION_STATE:
            buckets.premature.append(attempt)
            continue
        if epoch < max_epoch_by_task[task_id]:
            buckets.rework.append(attempt)
            continue
        if _attempt_id(attempt) != first_id_by_group[(task_id, epoch)]:
            buckets.repeated.append(attempt)
    return buckets


def _build_measured_report(
    dimension: WasteDimension,
    attempts: list[Any],
    ledger_by_id: dict[str, Any],
) -> WasteDimensionReport:
    """Build a report whose magnitude is the MEASURED cost of ``attempts``."""

    by_task_attempts: dict[str, list[Any]] = defaultdict(list)
    for attempt in attempts:
        by_task_attempts[_task_id(attempt)].append(attempt)

    by_task: list[WasteAttribution] = []
    for task_id in sorted(by_task_attempts):
        task_attempts = by_task_attempts[task_id]
        tokens, cost, wall, unavailable = _sum_measured(task_attempts, ledger_by_id)
        detail_ids, truncated = _bounded_ids(_attempt_id(item) for item in task_attempts)
        task_unavailable = list(unavailable)
        if truncated:
            task_unavailable.append(
                UnavailableField(
                    name="detail_ids",
                    reason=f"attributed attempt ids truncated to {_MAX_DETAIL_IDS}",
                )
            )
        by_task.append(
            WasteAttribution(
                task_id=task_id,
                count=len(task_attempts),
                tokens=tokens,
                cost_microusd=cost,
                wall_time_ms=wall,
                detail_ids=detail_ids,
                unavailable=tuple(task_unavailable),
            )
        )

    total_tokens, total_cost, total_wall, total_unavailable = _sum_measured(attempts, ledger_by_id)
    return WasteDimensionReport(
        dimension=dimension,
        observable=True,
        total_count=len(attempts),
        total_tokens=total_tokens,
        total_cost_microusd=total_cost,
        total_wall_time_ms=total_wall,
        by_task=tuple(by_task),
        unavailable=total_unavailable,
    )


def _annotate_repeated_reasoning(
    report: WasteDimensionReport,
    attempts: list[Any],
) -> WasteDimensionReport:
    """Add a note when the "unchanged input read-set" refinement is unobservable.

    The count of same-epoch attempts beyond the first is observable from
    attempt state alone.  Confirming that their *input read-set was unchanged*
    (the precise definition of repeated reasoning) requires an ``AgentSnapshot``
    on both the repeat and its predecessor; without it we can still report the
    repeat but cannot prove the input was identical.
    """

    if not attempts:
        return report
    without_snapshot = sum(
        1 for attempt in attempts if getattr(attempt, "agent_snapshot", None) is None
    )
    if without_snapshot == 0:
        return report
    note = UnavailableField(
        name="unchanged_read_set",
        reason=(
            f"{without_snapshot} of {len(attempts)} repeated attempts have no AgentSnapshot; "
            "the same-epoch repeat is counted but unchanged-input cannot be confirmed"
        ),
    )
    return report.model_copy(update={"unavailable": (*report.unavailable, note)})


def _build_reread_report(
    attempts: list[Any],
    attempts_with_snapshot: int,
) -> WasteDimensionReport:
    """Detect a resource re-materialized by an agent that already read it.

    A re-read is a ``(resource identity, version)`` that appears in the
    read-set of two or more of the same agent's snapshotted attempts, ordered
    by ``(semantic_epoch, attempt_number, attempt_id)``.  Every appearance
    after the first is one re-read, attributed to the re-reading attempt's
    task.  The *magnitude* (bytes/tokens re-materialized) is not observable:
    ``ComputationCost`` is cumulative per attempt, not per resource, and Context
    VM page-level attribution is not persisted on the durable attempt.
    """

    if attempts_with_snapshot == 0:
        return WasteDimensionReport(
            dimension=WasteDimension.REREAD_CONTEXT,
            observable=False,
            reason="no AgentSnapshot read-set is captured on any attempt for this graph",
        )

    by_agent: dict[str, list[Any]] = defaultdict(list)
    for attempt in attempts:
        if getattr(attempt, "agent_snapshot", None) is not None:
            by_agent[_agent_id(attempt)].append(attempt)

    reread_by_task: dict[str, int] = defaultdict(int)
    resources_by_task: dict[str, set[str]] = defaultdict(set)
    for agent_attempts in by_agent.values():
        agent_attempts.sort(
            key=lambda item: (
                _semantic_epoch(item),
                _attempt_number(item),
                _attempt_id(item),
            )
        )
        seen_keys: set[tuple[str, int | None]] = set()
        for attempt in agent_attempts:
            task_id = _task_id(attempt)
            for key, identity in _read_keys(attempt):
                if key in seen_keys:
                    reread_by_task[task_id] += 1
                    resources_by_task[task_id].add(identity)
                else:
                    seen_keys.add(key)

    by_task: list[WasteAttribution] = []
    for task_id in sorted(reread_by_task):
        detail_ids, truncated = _bounded_ids(sorted(resources_by_task[task_id]))
        task_unavailable = [
            UnavailableField(
                name="tokens",
                reason=(
                    "per-resource re-materialization token/byte cost is not recorded on the "
                    "durable AgentSnapshot; ComputationCost is cumulative per attempt"
                ),
            )
        ]
        if truncated:
            task_unavailable.append(
                UnavailableField(
                    name="detail_ids",
                    reason=f"re-read resource ids truncated to {_MAX_DETAIL_IDS}",
                )
            )
        by_task.append(
            WasteAttribution(
                task_id=task_id,
                count=reread_by_task[task_id],
                tokens=None,
                cost_microusd=None,
                wall_time_ms=None,
                detail_ids=detail_ids,
                unavailable=tuple(task_unavailable),
            )
        )

    total_count = sum(reread_by_task.values())
    return WasteDimensionReport(
        dimension=WasteDimension.REREAD_CONTEXT,
        observable=True,
        total_count=total_count,
        total_tokens=None,
        total_cost_microusd=None,
        total_wall_time_ms=None,
        by_task=tuple(by_task),
        unavailable=(
            UnavailableField(
                name="tokens",
                reason=(
                    "re-read occurrences are observable from AgentSnapshot read-sets, but the "
                    "materialized byte/token magnitude is not persisted per resource"
                ),
            ),
        ),
    )


def _live_version_by_attempt(events: Any) -> dict[str, int]:
    """Map attempt id -> graph version in effect when it reached a terminal state.

    Read from the Scheduler journal's terminal-event metadata.  Only events that
    actually carry the stamp contribute; an unstamped journal yields an empty map
    so the dimension stays unobservable rather than silently reporting that no
    superseded work occurred.
    """

    live: dict[str, int] = {}
    for event in events or ():
        attempt_id = str(getattr(event, "attempt_id", "") or "").strip()
        if not attempt_id:
            continue
        metadata = getattr(event, "metadata", None)
        if not isinstance(metadata, Mapping):
            continue
        stamped = metadata.get("live_graph_version")
        if isinstance(stamped, bool) or not isinstance(stamped, int):
            continue
        live[attempt_id] = max(live.get(attempt_id, stamped), stamped)
    return live


def _wrong_direction_report(
    attempts: list[Any],
    events: Any,
    ledger_by_id: dict[str, Any],
) -> WasteDimensionReport:
    """Attempts that kept computing after the graph had already moved on.

    An attempt records the ``graph_version`` it was dispatched under; terminal
    Scheduler events additionally stamp the version in effect when it finished.
    When the second exceeds the first, part of that attempt ran against
    superseded state -- exactly the work a semantic interrupt at the version bump
    would have saved.

    The reported magnitude is the attempt's *whole* measured cost, which is an
    upper bound on the avoidable part: no per-instant progress is persisted, so
    the split between "useful before the bump" and "wasted after" is not
    observable.  The bound is labelled as a bound rather than presented as the
    exact waste.
    """

    live_by_attempt = _live_version_by_attempt(events)
    if not live_by_attempt:
        return WasteDimensionReport(
            dimension=WasteDimension.WRONG_DIRECTION_CONTINUATION,
            observable=False,
            reason=(
                "no terminal Scheduler event carries a live_graph_version stamp, so "
                "supersession-at-end cannot be derived; the Scheduler and VPG journals "
                "share no total order"
            ),
            unavailable=(
                UnavailableField(
                    name="live_graph_version",
                    reason="terminal attempt events in this journal predate the version stamp",
                ),
            ),
        )

    superseded: list[Any] = []
    for attempt in attempts:
        # A VERIFIED attempt cannot be wrong-direction work by construction:
        # commit-time read-set validation quarantines any attempt whose bound
        # inputs stopped being current, so anything that reached VERIFIED had
        # current inputs at commit.  Excluding it also removes a large false
        # positive -- an attempt's own Evidence commit advances the graph, so
        # every success would otherwise look as though the graph had moved out
        # from under it.
        if _attempt_state(attempt) == _VERIFIED_STATE:
            continue
        live = live_by_attempt.get(_attempt_id(attempt))
        if live is None:
            continue
        dispatched = getattr(attempt, "graph_version", None)
        if isinstance(dispatched, bool) or not isinstance(dispatched, int):
            continue
        if live > dispatched:
            superseded.append(attempt)

    report = _build_measured_report(
        WasteDimension.WRONG_DIRECTION_CONTINUATION,
        superseded,
        ledger_by_id,
    )
    return report.model_copy(
        update={
            "reason": (
                "measured cost of attempts whose terminal live graph version exceeded their "
                "dispatch version; an upper bound on the avoidable portion"
            ),
            "unavailable": (
                *report.unavailable,
                UnavailableField(
                    name="avoidable_fraction",
                    reason=(
                        "no per-instant progress is persisted, so the pre-bump/post-bump "
                        "split within one attempt is not observable"
                    ),
                ),
            ),
        }
    )


def _unobservable_report(dimension: WasteDimension, reason: str) -> WasteDimensionReport:
    return WasteDimensionReport(
        dimension=dimension,
        observable=False,
        reason=reason,
        unavailable=(UnavailableField(name="attempts", reason=reason),),
    )


def _sum_measured(
    attempts: list[Any],
    ledger_by_id: dict[str, Any],
) -> tuple[int | None, int | None, int | None, tuple[UnavailableField, ...]]:
    """Sum MEASURED usage over ``attempts``, fail-closed on unmeasured cost.

    Returns a measured ``0`` triple when there are no attributed attempts (a
    genuine zero).  Returns ``None`` magnitudes when *no* attributed attempt has
    a measured ledger entry.  Returns the measured partial sum plus a note when
    only some attempts are measured -- never a silent zero for the missing ones.
    """

    if not attempts:
        return 0, 0, 0, ()
    measured: list[Any] = []
    missing = 0
    for attempt in attempts:
        vector = _ledger_lookup(attempt, ledger_by_id)
        if vector is None:
            missing += 1
        else:
            measured.append(vector)
    if not measured:
        reason = (
            f"none of the {len(attempts)} attributed attempts have a MEASURED usage entry "
            "(measured usage is only recorded on budget-aware runs)"
        )
        return (
            None,
            None,
            None,
            (
                UnavailableField(name="tokens", reason=reason),
                UnavailableField(name="cost_microusd", reason=reason),
                UnavailableField(name="wall_time_ms", reason=reason),
            ),
        )
    tokens = sum(int(getattr(vector, "tokens", 0)) for vector in measured)
    cost = sum(int(getattr(vector, "cost_microusd", 0)) for vector in measured)
    wall = sum(int(getattr(vector, "wall_time_ms", 0)) for vector in measured)
    unavailable: tuple[UnavailableField, ...] = ()
    if missing:
        unavailable = (
            UnavailableField(
                name="measured_usage",
                reason=(
                    f"{missing} of {len(attempts)} attributed attempts have no MEASURED usage "
                    "entry; totals cover the measured subset only"
                ),
            ),
        )
    return tokens, cost, wall, unavailable


def _ledger_measured_by_id(ledger: Any) -> dict[str, Any]:
    """Index a ledger's MEASURED usage vectors by their attempt-key string."""

    records = getattr(ledger, "records", None)
    if not records:
        return {}
    indexed: dict[str, Any] = {}
    for record in records:
        measured = getattr(record, "measured", None)
        if measured is None:
            continue
        key = str(getattr(record, "attempt_id", "")).strip()
        if key:
            indexed[key] = measured
    return indexed


def _ledger_lookup(attempt: Any, ledger_by_id: dict[str, Any]) -> Any | None:
    """Resolve one attempt's MEASURED usage vector.

    The runtime keys ledger entries by ``claim_id`` (or a synthetic
    ``{task_id}#attempt-{n}`` fallback), not by the attempt's own id, so try
    each candidate key in turn.
    """

    if not ledger_by_id:
        return None
    claim_id = str(getattr(attempt, "claim_id", "")).strip()
    attempt_id = _attempt_id(attempt)
    task_id = _task_id(attempt)
    attempt_number = _attempt_number(attempt)
    candidates = (
        claim_id,
        attempt_id,
        f"{task_id}#attempt-{attempt_number}",
    )
    for candidate in candidates:
        if candidate and candidate in ledger_by_id:
            return ledger_by_id[candidate]
    return None


def _read_keys(attempt: Any) -> list[tuple[tuple[str, int | None], str]]:
    """Return ``((identity, version), identity)`` for each auditable read binding."""

    snapshot = getattr(attempt, "agent_snapshot", None)
    if snapshot is None:
        return []
    keys: list[tuple[tuple[str, int | None], str]] = []
    for binding in getattr(snapshot, "read_set", ()) or ():
        identity = getattr(binding, "identity", None)
        if not identity:
            continue
        version = getattr(binding, "version", None)
        normalized_version = None if version is None else int(version)
        keys.append(((str(identity), normalized_version), str(identity)))
    return keys


def _bounded_ids(values: Any) -> tuple[tuple[str, ...], bool]:
    ordered = sorted({str(value).strip() for value in values if str(value).strip()})
    if len(ordered) > _MAX_DETAIL_IDS:
        return tuple(ordered[:_MAX_DETAIL_IDS]), True
    return tuple(ordered), False


def _task_id(attempt: Any) -> str:
    return str(getattr(attempt, "task_id", ""))


def _agent_id(attempt: Any) -> str:
    return str(getattr(attempt, "agent_id", ""))


def _attempt_id(attempt: Any) -> str:
    return str(getattr(attempt, "attempt_id", ""))


def _attempt_number(attempt: Any) -> int:
    try:
        return int(getattr(attempt, "attempt_number", 0))
    except (TypeError, ValueError):
        return 0


def _semantic_epoch(attempt: Any) -> int:
    try:
        return int(getattr(attempt, "semantic_epoch", 0))
    except (TypeError, ValueError):
        return 0


def _attempt_state(attempt: Any) -> str:
    value = getattr(attempt, "state", "")
    return str(getattr(value, "value", value))


def _canonical(value: Any) -> Any:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    if isinstance(value, dict):
        return {str(key): _canonical(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (tuple, list)):
        return [_canonical(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    return value


def _sha256_json(value: Any) -> str:
    encoded = json.dumps(
        _canonical(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "WASTE_PROJECTION_SCHEMA_VERSION",
    "WasteAttribution",
    "WasteDimension",
    "WasteDimensionReport",
    "WasteProjection",
    "WasteProjectionView",
    "build_waste_projection",
]
