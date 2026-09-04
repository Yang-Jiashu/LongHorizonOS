"""Deterministic verified-progress compute-budget admission policy.

The policy is deliberately advisory and side-effect free.  It consumes an
immutable :class:`GlobalRuntimeState`, explicit per-task estimates, and an
explicit cumulative usage snapshot.  It never claims work or mutates the
Scheduler.  Missing/unknown estimates and inconsistent graph fences fail
closed.

All cost dimensions are integer fixed-point units.  In particular, no
floating-point utility weights are used: candidates are ordered by the exact
ratio

``(progress * success_bp * stability_bp) / (cost + expected_rework)``.

The common ``10_000 * 10_000`` basis-point denominator is omitted from the
comparison because it is identical for every candidate.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from functools import cmp_to_key
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

COMPUTE_BUDGET_SCHEMA_VERSION: Final[Literal["compute-budget.v1"]] = "compute-budget.v1"
VERIFIED_PROGRESS_BUDGET_POLICY_ID: Final[str] = "verified-progress-budget.v1"
EXPECTED_PROGRESS_BASIS_DENOMINATOR: Final[int] = 100_000_000
_UNAVAILABLE_PROJECTION_HASH: Final[str] = "unavailable"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class TaskComputeEstimate(_FrozenModel):
    """One explicit, fixed-point estimate for a frontier task.

    ``known=False`` is an explicit fail-closed marker.  The remaining fields
    are retained for auditability but are not used to admit an unknown task.
    ``normalized_cost_units`` and ``expected_rework_cost_units`` are
    intentionally abstract integer units so a caller can calibrate them to
    tokens, time, or a monetary objective without introducing float weights.
    """

    task_id: str = Field(min_length=1)
    verified_progress_units: StrictInt = Field(default=0, ge=0)
    success_basis_points: StrictInt = Field(default=0, ge=0, le=10_000)
    input_stability_basis_points: StrictInt = Field(default=0, ge=0, le=10_000)
    normalized_cost_units: StrictInt = Field(default=0, ge=0)
    expected_rework_cost_units: StrictInt = Field(default=0, ge=0)
    estimated_tokens: StrictInt = Field(default=0, ge=0)
    estimated_wall_time_ms: StrictInt = Field(default=0, ge=0)
    estimated_cost_microusd: StrictInt = Field(default=0, ge=0)
    estimated_context_tokens: StrictInt = Field(default=0, ge=0)
    estimated_verification_tokens: StrictInt = Field(default=0, ge=0)
    known: StrictBool = False

    @field_validator("task_id")
    @classmethod
    def _require_non_empty_task_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("task_id must be non-empty")
        return normalized

    @model_validator(mode="before")
    @classmethod
    def _normalize_input(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        raw = dict(value)
        aliases = {
            "progress_units": "verified_progress_units",
            "success_bp": "success_basis_points",
            "stability_bp": "input_stability_basis_points",
            "cost_units": "normalized_cost_units",
            "rework_cost_units": "expected_rework_cost_units",
            "cost_micros": "estimated_cost_microusd",
            "verification_tokens": "estimated_verification_tokens",
        }
        for source, target in aliases.items():
            if target not in raw and source in raw:
                raw[target] = raw.pop(source)
        return raw

    @classmethod
    def unknown(cls, task_id: str) -> TaskComputeEstimate:
        """Build an explicit unknown estimate for fail-closed normalization."""

        return cls(task_id=task_id, known=False)

    @property
    def expected_verified_progress_numerator(self) -> int:
        """Expected verified-progress numerator in basis-point units."""

        return (
            self.verified_progress_units
            * self.success_basis_points
            * self.input_stability_basis_points
        )

    @property
    def cost_denominator(self) -> int:
        """The additive cost/rework denominator used for ranking."""

        return self.normalized_cost_units + self.expected_rework_cost_units

    @property
    def total_budget_delta(self) -> ComputeBudgetUsage:
        """Convert this estimate to the five hard-budget dimensions."""

        return ComputeBudgetUsage(
            tokens=self.estimated_tokens,
            wall_time_ms=self.estimated_wall_time_ms,
            cost_microusd=self.estimated_cost_microusd,
            context_tokens=self.estimated_context_tokens,
            verification_tokens=self.estimated_verification_tokens,
        )


class ComputeBudgetLimits(_FrozenModel):
    """Optional hard ceilings for one scheduling epoch/goal budget.

    ``None`` means that dimension is not bounded by this policy.  Aliases
    without the ``max_`` prefix are accepted for ergonomic construction but
    the frozen representation always uses the explicit ``max_*`` names.
    """

    max_tokens: StrictInt | None = Field(default=None, ge=0)
    max_wall_time_ms: StrictInt | None = Field(default=None, ge=0)
    max_cost_microusd: StrictInt | None = Field(default=None, ge=0)
    max_context_tokens: StrictInt | None = Field(default=None, ge=0)
    max_verification_tokens: StrictInt | None = Field(default=None, ge=0)

    @model_validator(mode="before")
    @classmethod
    def _normalize_aliases(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        raw = dict(value)
        aliases = {
            "tokens": "max_tokens",
            "wall_time_ms": "max_wall_time_ms",
            "cost_microusd": "max_cost_microusd",
            "context_tokens": "max_context_tokens",
            "verification_tokens": "max_verification_tokens",
        }
        for source, target in aliases.items():
            if target not in raw and source in raw:
                raw[target] = raw.pop(source)
        return raw

    def as_dict(self) -> dict[str, int | None]:
        return self.model_dump(mode="json")


class ComputeBudgetUsage(_FrozenModel):
    """Integer consumption vector used by the hard budget checks."""

    tokens: StrictInt = Field(default=0, ge=0)
    wall_time_ms: StrictInt = Field(default=0, ge=0)
    cost_microusd: StrictInt = Field(default=0, ge=0)
    context_tokens: StrictInt = Field(default=0, ge=0)
    verification_tokens: StrictInt = Field(default=0, ge=0)

    @model_validator(mode="before")
    @classmethod
    def _normalize_aliases(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        raw = dict(value)
        aliases = {
            "used_tokens": "tokens",
            "consumed_tokens": "tokens",
            "used_wall_time_ms": "wall_time_ms",
            "consumed_wall_time_ms": "wall_time_ms",
            "used_cost_microusd": "cost_microusd",
            "consumed_cost_microusd": "cost_microusd",
            "used_context_tokens": "context_tokens",
            "consumed_context_tokens": "context_tokens",
            "used_verification_tokens": "verification_tokens",
            "consumed_verification_tokens": "verification_tokens",
        }
        for source, target in aliases.items():
            if target not in raw and source in raw:
                raw[target] = raw.pop(source)
        return raw

    @property
    def used_tokens(self) -> int:
        return self.tokens

    @property
    def used_wall_time_ms(self) -> int:
        return self.wall_time_ms

    @property
    def used_cost_microusd(self) -> int:
        return self.cost_microusd

    @property
    def used_context_tokens(self) -> int:
        return self.context_tokens

    @property
    def used_verification_tokens(self) -> int:
        return self.verification_tokens

    def plus(self, other: ComputeBudgetUsage) -> ComputeBudgetUsage:
        if not isinstance(other, ComputeBudgetUsage):
            raise TypeError("other must be a ComputeBudgetUsage")
        return ComputeBudgetUsage(
            tokens=self.tokens + other.tokens,
            wall_time_ms=self.wall_time_ms + other.wall_time_ms,
            cost_microusd=self.cost_microusd + other.cost_microusd,
            context_tokens=self.context_tokens + other.context_tokens,
            verification_tokens=self.verification_tokens + other.verification_tokens,
        )


class ComputeBudgetRemaining(_FrozenModel):
    """Remaining hard-budget capacity.

    ``None`` means the corresponding limit is unbounded.  This is deliberately
    distinct from ``0``, which means a bounded dimension has no capacity left.
    """

    tokens: StrictInt | None = Field(default=None, ge=0)
    wall_time_ms: StrictInt | None = Field(default=None, ge=0)
    cost_microusd: StrictInt | None = Field(default=None, ge=0)
    context_tokens: StrictInt | None = Field(default=None, ge=0)
    verification_tokens: StrictInt | None = Field(default=None, ge=0)

    @classmethod
    def from_limits(
        cls,
        limits: ComputeBudgetLimits,
        usage: ComputeBudgetUsage,
    ) -> ComputeBudgetRemaining:
        """Project bounded remaining capacity without collapsing unbounded limits."""

        if not isinstance(limits, ComputeBudgetLimits):
            raise TypeError("limits must be a ComputeBudgetLimits")
        if not isinstance(usage, ComputeBudgetUsage):
            raise TypeError("usage must be a ComputeBudgetUsage")

        def left(limit: int | None, used: int) -> int | None:
            return None if limit is None else max(0, limit - used)

        return cls(
            tokens=left(limits.max_tokens, usage.tokens),
            wall_time_ms=left(limits.max_wall_time_ms, usage.wall_time_ms),
            cost_microusd=left(limits.max_cost_microusd, usage.cost_microusd),
            context_tokens=left(limits.max_context_tokens, usage.context_tokens),
            verification_tokens=left(
                limits.max_verification_tokens,
                usage.verification_tokens,
            ),
        )

    @property
    def unbounded_dimensions(self) -> tuple[str, ...]:
        """Return dimensions whose declared ceiling is unbounded."""

        return tuple(
            name
            for name, value in (
                ("tokens", self.tokens),
                ("wall_time_ms", self.wall_time_ms),
                ("cost_microusd", self.cost_microusd),
                ("context_tokens", self.context_tokens),
                ("verification_tokens", self.verification_tokens),
            )
            if value is None
        )

    def as_dict(self) -> dict[str, int | None]:
        return self.model_dump(mode="json")


class ComputeBudgetAudit(_FrozenModel):
    """Budget before/after projection attached to every plan."""

    limits: ComputeBudgetLimits
    usage_before: ComputeBudgetUsage
    usage_after: ComputeBudgetUsage
    remaining_before: ComputeBudgetRemaining
    remaining_after: ComputeBudgetRemaining


class ComputeBudgetTaskDecision(_FrozenModel):
    """One auditable decision made by :class:`VerifiedProgressBudgetPolicy`."""

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


class VerifiedProgressBudgetPlan(_FrozenModel):
    """Immutable output of one graph-fenced budget planning pass."""

    schema_version: Literal["compute-budget.v1"] = COMPUTE_BUDGET_SCHEMA_VERSION
    epoch_id: StrictInt = Field(ge=0)
    policy_id: str = Field(min_length=1)
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    projection_hash: str = Field(min_length=1)
    candidate_task_ids: tuple[str, ...] = ()
    selected_task_ids: tuple[str, ...] = ()
    deferred_task_ids: tuple[str, ...] = ()
    decisions: tuple[ComputeBudgetTaskDecision, ...] = ()
    parallelism_hint: StrictInt = Field(ge=0)
    limits: ComputeBudgetLimits
    usage_before: ComputeBudgetUsage
    usage_after: ComputeBudgetUsage
    remaining_before: ComputeBudgetRemaining
    remaining_after: ComputeBudgetRemaining
    safe_under_declared_budget: StrictBool
    unavailable: tuple[UnavailableField, ...] = ()
    decision_hash: str = Field(min_length=64, max_length=64)

    @property
    def budget_audit(self) -> ComputeBudgetAudit:
        return ComputeBudgetAudit(
            limits=self.limits,
            usage_before=self.usage_before,
            usage_after=self.usage_after,
            remaining_before=self.remaining_before,
            remaining_after=self.remaining_after,
        )

    @property
    def selected(self) -> tuple[str, ...]:
        return self.selected_task_ids

    @property
    def deferred(self) -> tuple[str, ...]:
        return self.deferred_task_ids

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class VerifiedProgressBudgetPolicy(_FrozenModel):
    """Exact-ratio, fail-closed, graph-relative budget admission policy."""

    policy_id: str = VERIFIED_PROGRESS_BUDGET_POLICY_ID

    def plan(
        self,
        state: GlobalRuntimeState,
        estimates: Mapping[str, TaskComputeEstimate] | Iterable[TaskComputeEstimate],
        limits: ComputeBudgetLimits,
        usage: ComputeBudgetUsage,
        epoch_id: int = 0,
        max_parallelism: int = 1,
    ) -> VerifiedProgressBudgetPlan:
        if not isinstance(state, GlobalRuntimeState):
            raise TypeError("state must be a GlobalRuntimeState/RuntimeStateView")
        if not isinstance(limits, ComputeBudgetLimits):
            raise TypeError("limits must be ComputeBudgetLimits")
        if not isinstance(usage, ComputeBudgetUsage):
            raise TypeError("usage must be ComputeBudgetUsage")
        _require_nonnegative_int(epoch_id, "epoch_id")
        _require_positive_int(max_parallelism, "max_parallelism")

        normalized, estimate_unavailable = _normalize_estimates(estimates)
        ready = _normalize_ids(state.progress.ready_frontier)
        repair_ready = _normalize_ids(state.progress.repair_ready_frontier)
        repair_set = set(repair_ready)
        candidates_set = set(ready) | repair_set
        candidates = _rank_candidates(candidates_set, repair_set, normalized)

        unavailable = list(estimate_unavailable)
        unknown_estimate_ids: set[str] = set()
        for task_id in candidates:
            candidate_estimate = normalized.get(task_id)
            if candidate_estimate is None or not candidate_estimate.known:
                unknown_estimate_ids.add(task_id)
        for task_id in sorted(unknown_estimate_ids):
            unavailable.append(
                UnavailableField(
                    name=f"estimates.{task_id}",
                    reason="task compute estimate is missing or marked unknown",
                )
            )
        usage_limit_violations = _usage_limit_violations(usage, limits)
        for dimension in usage_limit_violations:
            unavailable.append(
                UnavailableField(
                    name=f"usage.{dimension}",
                    reason="current usage already exceeds the declared hard limit",
                )
            )
        graph_fence_ok = bool(
            state.graph_id.strip()
            and state.progress.graph_id.strip()
            and state.graph_id == state.progress.graph_id
        )
        if not graph_fence_ok:
            unavailable.append(
                UnavailableField(
                    name="graph_fence",
                    reason="state graph_id and progress.graph_id do not match",
                )
            )
        projection_hash_ok = isinstance(state.progress.projection_hash, str) and bool(
            state.progress.projection_hash.strip()
        )
        if not projection_hash_ok:
            unavailable.append(
                UnavailableField(
                    name="projection_hash",
                    reason="progress projection hash is missing or empty",
                )
            )

        verified = set(_normalize_ids(state.progress.verified_task_ids))
        invalid = set(_normalize_ids(state.progress.invalid_task_ids))
        stale = set(_normalize_ids(state.progress.stale_task_ids))
        active_tasks = {
            str(attempt.task_id).strip()
            for attempt in state.agent_cognition.current_attempts
            if str(getattr(attempt, "task_id", "") or "").strip()
        }
        closed = bool(state.progress.graph_closed or state.progress.goal_closed)
        cognition_available = bool(state.agent_cognition.available)
        resources_available = bool(state.resources.available)

        selected: list[str] = []
        deferred: list[str] = []
        decisions: list[ComputeBudgetTaskDecision] = []
        usage_after = usage

        for task_id in candidates:
            is_repair = task_id in repair_set
            tier: Literal["repair", "ready"] = "repair" if is_repair else "ready"
            estimate = normalized.get(task_id)
            known = estimate is not None and estimate.known
            if known:
                assert estimate is not None
                numerator = estimate.expected_verified_progress_numerator
                denominator = EXPECTED_PROGRESS_BASIS_DENOMINATOR
                utility_denominator = (
                    EXPECTED_PROGRESS_BASIS_DENOMINATOR * estimate.cost_denominator
                )
                delta = estimate.total_budget_delta
            else:
                numerator = None
                denominator = None
                utility_denominator = None
                delta = None
            action = FrontierAction.DEFER
            reason = "ready"
            blockers: tuple[str, ...] = ()

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
            elif not known:
                reason = "estimate_unknown"
            elif len(selected) >= max_parallelism:
                reason = "max_parallelism"
            else:
                assert delta is not None
                blockers = _budget_blockers(usage_after, delta, limits)
                if blockers:
                    reason = "budget_exceeded"
                else:
                    action = FrontierAction.RUN
                    reason = "selected_repair" if is_repair else "selected"
                    selected.append(task_id)
                    usage_after = usage_after.plus(delta)

            if action is FrontierAction.DEFER:
                deferred.append(task_id)
            decisions.append(
                ComputeBudgetTaskDecision(
                    task_id=task_id,
                    action=action,
                    reason=reason,
                    tier=tier,
                    estimate_known=known,
                    expected_verified_progress_numerator=numerator,
                    expected_verified_progress_denominator=denominator,
                    utility_numerator=numerator,
                    utility_denominator=utility_denominator,
                    budget_delta=delta,
                    budget_blockers=blockers,
                )
            )

        unavailable_tuple = _dedupe_unavailable(unavailable)
        safe = bool(
            graph_fence_ok
            and projection_hash_ok
            and cognition_available
            and resources_available
            and not usage_limit_violations
            and not unavailable_tuple
            and all(
                item.action is FrontierAction.RUN or item.reason not in {"estimate_unknown"}
                for item in decisions
            )
        )
        remaining_before = _remaining(limits, usage)
        remaining_after = _remaining(limits, usage_after)
        projection_hash = (
            state.progress.projection_hash if projection_hash_ok else _UNAVAILABLE_PROJECTION_HASH
        )
        payload = {
            "schema_version": COMPUTE_BUDGET_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "policy_id": self.policy_id,
            "graph_id": state.graph_id,
            "graph_version": state.progress.graph_version,
            "projection_hash": projection_hash,
            "candidate_task_ids": candidates,
            "selected_task_ids": tuple(selected),
            "deferred_task_ids": tuple(deferred),
            "decisions": tuple(decisions),
            "parallelism_hint": len(selected),
            "limits": limits,
            "usage_before": usage,
            "usage_after": usage_after,
            "remaining_before": remaining_before,
            "remaining_after": remaining_after,
            "safe_under_declared_budget": safe,
            "unavailable": unavailable_tuple,
        }
        return VerifiedProgressBudgetPlan(
            epoch_id=epoch_id,
            policy_id=self.policy_id,
            graph_id=state.graph_id,
            graph_version=state.progress.graph_version,
            projection_hash=projection_hash,
            candidate_task_ids=candidates,
            selected_task_ids=tuple(selected),
            deferred_task_ids=tuple(deferred),
            decisions=tuple(decisions),
            parallelism_hint=len(selected),
            limits=limits,
            usage_before=usage,
            usage_after=usage_after,
            remaining_before=remaining_before,
            remaining_after=remaining_after,
            safe_under_declared_budget=safe,
            unavailable=unavailable_tuple,
            decision_hash=_decision_hash(payload),
        )


# Short alias for callers that use the generic policy name.
ComputeBudgetPolicy = VerifiedProgressBudgetPolicy
ComputeBudgetPlan = VerifiedProgressBudgetPlan
BudgetTaskDecision = ComputeBudgetTaskDecision


def plan_verified_progress_budget(
    state: GlobalRuntimeState,
    estimates: Mapping[str, TaskComputeEstimate] | Iterable[TaskComputeEstimate],
    limits: ComputeBudgetLimits,
    usage: ComputeBudgetUsage,
    *,
    epoch_id: int = 0,
    max_parallelism: int = 1,
) -> VerifiedProgressBudgetPlan:
    """Convenience wrapper around :class:`VerifiedProgressBudgetPolicy`."""

    return VerifiedProgressBudgetPolicy().plan(
        state,
        estimates,
        limits,
        usage,
        epoch_id=epoch_id,
        max_parallelism=max_parallelism,
    )


def _normalize_estimates(
    values: Mapping[str, TaskComputeEstimate] | Iterable[TaskComputeEstimate],
) -> tuple[dict[str, TaskComputeEstimate | None], tuple[UnavailableField, ...]]:
    if values is None:
        return {}, ()
    entries: list[tuple[str, Any]] = []
    if isinstance(values, Mapping):
        entries = [(str(key).strip(), value) for key, value in values.items()]
    else:
        try:
            entries = [(str(getattr(item, "task_id", "")).strip(), item) for item in values]
        except TypeError as exc:
            raise TypeError("estimates must be a mapping or iterable") from exc

    normalized: dict[str, TaskComputeEstimate | None] = {}
    unavailable: list[UnavailableField] = []
    for task_id, raw in entries:
        if not task_id:
            unavailable.append(
                UnavailableField(name="estimates", reason="estimate has an empty task_id")
            )
            continue
        if task_id in normalized:
            normalized[task_id] = None
            unavailable.append(
                UnavailableField(
                    name=f"estimates.{task_id}",
                    reason="duplicate task compute estimate",
                )
            )
            continue
        if isinstance(raw, TaskComputeEstimate):
            if raw.task_id != task_id:
                normalized[task_id] = None
                unavailable.append(
                    UnavailableField(
                        name=f"estimates.{task_id}",
                        reason=("mapping key does not match the estimate's declared task_id"),
                    )
                )
            else:
                normalized[task_id] = raw
            continue
        try:
            if isinstance(raw, Mapping):
                raw_mapping = dict(raw)
                declared_task_id = str(raw_mapping.get("task_id", task_id)).strip()
                if declared_task_id != task_id:
                    raise ValueError("estimate task_id does not match mapping key")
                normalized[task_id] = TaskComputeEstimate.model_validate(
                    {**raw_mapping, "task_id": task_id}
                )
            else:
                raise TypeError
        except Exception:
            normalized[task_id] = None
            unavailable.append(
                UnavailableField(
                    name=f"estimates.{task_id}",
                    reason="estimate is malformed or unavailable",
                )
            )
    return normalized, _dedupe_unavailable(unavailable)


def _rank_candidates(
    candidate_set: set[str],
    repair_set: set[str],
    estimates: Mapping[str, TaskComputeEstimate | None],
) -> tuple[str, ...]:
    def compare(left: str, right: str) -> int:
        left_repair = left in repair_set
        right_repair = right in repair_set
        if left_repair != right_repair:
            return -1 if left_repair else 1
        left_estimate = estimates.get(left)
        right_estimate = estimates.get(right)
        left_known = left_estimate is not None and left_estimate.known
        right_known = right_estimate is not None and right_estimate.known
        if left_known != right_known:
            return -1 if left_known else 1
        if left_known and right_known:
            assert left_estimate is not None and right_estimate is not None
            ratio_order = _compare_ratio(
                left_estimate.expected_verified_progress_numerator,
                left_estimate.cost_denominator,
                right_estimate.expected_verified_progress_numerator,
                right_estimate.cost_denominator,
            )
            if ratio_order:
                return ratio_order
        return -1 if left < right else 1 if left > right else 0

    return tuple(sorted(candidate_set, key=cmp_to_key(compare)))


def _compare_ratio(left_num: int, left_den: int, right_num: int, right_den: int) -> int:
    """Return -1 when left has higher exact ratio than right."""

    if left_den == 0 or right_den == 0:
        if left_den == 0 and right_den == 0:
            if left_num == right_num:
                return 0
            return -1 if left_num > right_num else 1
        if left_den == 0:
            return -1 if left_num > 0 else 1
        return 1 if right_num > 0 else -1
    left_cross = left_num * right_den
    right_cross = right_num * left_den
    if left_cross == right_cross:
        return 0
    return -1 if left_cross > right_cross else 1


def _budget_blockers(
    current: ComputeBudgetUsage,
    delta: ComputeBudgetUsage,
    limits: ComputeBudgetLimits,
) -> tuple[str, ...]:
    blockers: list[str] = []
    checks = (
        ("tokens", current.tokens + delta.tokens, limits.max_tokens),
        ("wall_time_ms", current.wall_time_ms + delta.wall_time_ms, limits.max_wall_time_ms),
        ("cost_microusd", current.cost_microusd + delta.cost_microusd, limits.max_cost_microusd),
        (
            "context_tokens",
            current.context_tokens + delta.context_tokens,
            limits.max_context_tokens,
        ),
        (
            "verification_tokens",
            current.verification_tokens + delta.verification_tokens,
            limits.max_verification_tokens,
        ),
    )
    for name, projected, limit in checks:
        if limit is not None and projected > limit:
            blockers.append(name)
    return tuple(blockers)


def _usage_limit_violations(
    usage: ComputeBudgetUsage,
    limits: ComputeBudgetLimits,
) -> tuple[str, ...]:
    checks = (
        ("tokens", usage.tokens, limits.max_tokens),
        ("wall_time_ms", usage.wall_time_ms, limits.max_wall_time_ms),
        ("cost_microusd", usage.cost_microusd, limits.max_cost_microusd),
        ("context_tokens", usage.context_tokens, limits.max_context_tokens),
        (
            "verification_tokens",
            usage.verification_tokens,
            limits.max_verification_tokens,
        ),
    )
    return tuple(name for name, consumed, limit in checks if limit is not None and consumed > limit)


def _remaining(
    limits: ComputeBudgetLimits,
    usage: ComputeBudgetUsage,
) -> ComputeBudgetRemaining:
    return ComputeBudgetRemaining.from_limits(limits, usage)


def _normalize_ids(values: Any) -> tuple[str, ...]:
    return tuple(sorted({str(value).strip() for value in values or () if str(value).strip()}))


def _dedupe_unavailable(values: Iterable[UnavailableField]) -> tuple[UnavailableField, ...]:
    unique = {(item.name, item.reason): item for item in values}
    return tuple(unique[key] for key in sorted(unique))


def _require_nonnegative_int(value: Any, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def _require_positive_int(value: Any, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _json_compatible(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, tuple):
        return [_json_compatible(item) for item in value]
    if isinstance(value, list):
        return [_json_compatible(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_compatible(item) for key, item in value.items()}
    return value


def _decision_hash(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        _json_compatible(dict(payload)),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


__all__ = [
    "COMPUTE_BUDGET_SCHEMA_VERSION",
    "EXPECTED_PROGRESS_BASIS_DENOMINATOR",
    "VERIFIED_PROGRESS_BUDGET_POLICY_ID",
    "ComputeBudgetAudit",
    "ComputeBudgetLimits",
    "ComputeBudgetPlan",
    "ComputeBudgetPolicy",
    "ComputeBudgetRemaining",
    "ComputeBudgetTaskDecision",
    "ComputeBudgetUsage",
    "TaskComputeEstimate",
    "VerifiedProgressBudgetPlan",
    "VerifiedProgressBudgetPolicy",
    "plan_verified_progress_budget",
]
