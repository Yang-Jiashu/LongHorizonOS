"""Controlled compute-budget policy benchmark.

This benchmark compares a repair-first lexical baseline with the real
``VerifiedProgressBudgetPolicy`` under the same declared token, time, money,
context, verification, and parallelism limits.  Every estimate is synthetic
and fixed in the source so the result is deterministic and auditable.

The benchmark validates policy mechanics only.  It does not call an LLM,
measure physical resources, estimate real task success, or claim production
speed/cost improvements.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Final

from lhos.sdk.compute_budget import (
    EXPECTED_PROGRESS_BASIS_DENOMINATOR,
    ComputeBudgetLimits,
    ComputeBudgetUsage,
    TaskComputeEstimate,
    VerifiedProgressBudgetPlan,
    VerifiedProgressBudgetPolicy,
)
from lhos.sdk.runtime_state import (
    AgentCognitionState,
    ContextRuntimeState,
    GlobalRuntimeState,
    ProgressSemanticState,
    ResourceRuntimeState,
)

BENCHMARK_NAME: Final[str] = "compute_budget_controlled"
BENCHMARK_VERSION: Final[int] = 1
STATIC_POLICY_ID: Final[str] = "repair-first-lexical-budget.v1"
MAX_PARALLELISM: Final[int] = 2

_LIMIT_FIELDS: Final[tuple[tuple[str, str], ...]] = (
    ("tokens", "max_tokens"),
    ("wall_time_ms", "max_wall_time_ms"),
    ("cost_microusd", "max_cost_microusd"),
    ("context_tokens", "max_context_tokens"),
    ("verification_tokens", "max_verification_tokens"),
)


def _state(
    *,
    ready: tuple[str, ...],
    repair_ready: tuple[str, ...] = (),
    stale: tuple[str, ...] = (),
) -> GlobalRuntimeState:
    """Build the immutable synthetic state consumed by the real policy."""

    return GlobalRuntimeState(
        goal_id="compute-budget-benchmark-goal",
        graph_id="compute-budget-benchmark-graph",
        progress=ProgressSemanticState(
            graph_id="compute-budget-benchmark-graph",
            graph_version=1,
            projection_hash="b" * 64,
            graph_closed=False,
            goal_closed=False,
            ready_frontier=ready,
            repair_ready_frontier=repair_ready,
            verified_task_ids=(),
            stale_task_ids=stale,
            invalid_task_ids=(),
            unverified_task_ids=ready,
        ),
        agent_cognition=AgentCognitionState(available=True),
        context=ContextRuntimeState(
            available=False,
            reason="controlled benchmark has no live Context VM binding",
        ),
        resources=ResourceRuntimeState(available=True),
    )


def _estimate(
    task_id: str,
    *,
    progress: int,
    normalized_cost: int,
    tokens: int,
    wall_time_ms: int,
    cost_microusd: int,
    context_tokens: int,
    verification_tokens: int,
    known: bool = True,
) -> TaskComputeEstimate:
    return TaskComputeEstimate(
        task_id=task_id,
        verified_progress_units=progress,
        success_basis_points=10_000,
        input_stability_basis_points=10_000,
        normalized_cost_units=normalized_cost,
        expected_rework_cost_units=0,
        estimated_tokens=tokens,
        estimated_wall_time_ms=wall_time_ms,
        estimated_cost_microusd=cost_microusd,
        estimated_context_tokens=context_tokens,
        estimated_verification_tokens=verification_tokens,
        known=known,
    )


def _limits(value: int = 10) -> ComputeBudgetLimits:
    return ComputeBudgetLimits(
        max_tokens=value,
        max_wall_time_ms=value,
        max_cost_microusd=value,
        max_context_tokens=value,
        max_verification_tokens=value,
    )


def _main_estimates() -> dict[str, TaskComputeEstimate]:
    # The repair has deliberately poor utility.  Both policies must still
    # preserve repair priority before considering ordinary READY work.
    return {
        "repair": _estimate(
            "repair",
            progress=10,
            normalized_cost=100,
            tokens=2,
            wall_time_ms=2,
            cost_microusd=2,
            context_tokens=2,
            verification_tokens=2,
        ),
        "a-low-yield": _estimate(
            "a-low-yield",
            progress=5,
            normalized_cost=8,
            tokens=8,
            wall_time_ms=8,
            cost_microusd=8,
            context_tokens=8,
            verification_tokens=8,
        ),
        "z-high-yield": _estimate(
            "z-high-yield",
            progress=80,
            normalized_cost=8,
            tokens=8,
            wall_time_ms=8,
            cost_microusd=8,
            context_tokens=8,
            verification_tokens=8,
        ),
    }


def _budget_blockers(
    current: ComputeBudgetUsage,
    delta: ComputeBudgetUsage,
    limits: ComputeBudgetLimits,
) -> tuple[str, ...]:
    blockers: list[str] = []
    for usage_field, limit_field in _LIMIT_FIELDS:
        limit = getattr(limits, limit_field)
        projected = getattr(current, usage_field) + getattr(delta, usage_field)
        if limit is not None and projected > limit:
            blockers.append(usage_field)
    return tuple(blockers)


def _expected_progress_numerator(
    selected_task_ids: tuple[str, ...],
    estimates: Mapping[str, TaskComputeEstimate],
) -> int:
    return sum(
        estimates[task_id].expected_verified_progress_numerator for task_id in selected_task_ids
    )


def _lexical_plan(
    state: GlobalRuntimeState,
    estimates: Mapping[str, TaskComputeEstimate],
    limits: ComputeBudgetLimits,
    usage: ComputeBudgetUsage,
    *,
    max_parallelism: int,
) -> dict[str, Any]:
    """Repair-first lexical selection with the same declared hard limits."""

    repair = set(state.progress.repair_ready_frontier)
    candidates = tuple(sorted(repair) + sorted(set(state.progress.ready_frontier) - repair))
    selected: list[str] = []
    deferred: list[str] = []
    decisions: list[dict[str, Any]] = []
    usage_after = usage

    for task_id in candidates:
        estimate = estimates.get(task_id)
        blockers: tuple[str, ...] = ()
        if estimate is None or not estimate.known:
            action = "defer"
            reason = "estimate_unknown"
        elif len(selected) >= max_parallelism:
            action = "defer"
            reason = "max_parallelism"
        else:
            blockers = _budget_blockers(usage_after, estimate.total_budget_delta, limits)
            if blockers:
                action = "defer"
                reason = "budget_exceeded"
            else:
                action = "run"
                reason = "selected_repair" if task_id in repair else "selected_lexical"
                selected.append(task_id)
                usage_after = usage_after.plus(estimate.total_budget_delta)
        if action == "defer":
            deferred.append(task_id)
        decisions.append(
            {
                "task_id": task_id,
                "tier": "repair" if task_id in repair else "ready",
                "action": action,
                "reason": reason,
                "budget_blockers": list(blockers),
            }
        )

    selected_ids = tuple(selected)
    numerator = _expected_progress_numerator(selected_ids, estimates)
    return {
        "policy_id": STATIC_POLICY_ID,
        "candidate_task_ids": list(candidates),
        "selected_task_ids": list(selected_ids),
        "deferred_task_ids": deferred,
        "decisions": decisions,
        "parallelism_hint": len(selected_ids),
        "limits": limits.model_dump(mode="json"),
        "usage_before": usage.model_dump(mode="json"),
        "usage_after": usage_after.model_dump(mode="json"),
        "expected_verified_progress_numerator": numerator,
        "expected_verified_progress_denominator": EXPECTED_PROGRESS_BASIS_DENOMINATOR,
        "expected_verified_progress_units": numerator // EXPECTED_PROGRESS_BASIS_DENOMINATOR,
    }


def _policy_payload(
    plan: VerifiedProgressBudgetPlan,
    estimates: Mapping[str, TaskComputeEstimate],
) -> dict[str, Any]:
    payload = plan.as_dict()
    numerator = _expected_progress_numerator(plan.selected_task_ids, estimates)
    payload.update(
        {
            "expected_verified_progress_numerator": numerator,
            "expected_verified_progress_denominator": EXPECTED_PROGRESS_BASIS_DENOMINATOR,
            "expected_verified_progress_units": (numerator // EXPECTED_PROGRESS_BASIS_DENOMINATOR),
        }
    )
    return payload


def _dimension_cases() -> dict[str, dict[str, Any]]:
    """Exercise every hard-budget dimension in isolation."""

    cases: dict[str, dict[str, Any]] = {}
    for usage_field, _limit_field in _LIMIT_FIELDS:
        values = {field: 1 for field, _ in _LIMIT_FIELDS}
        values[usage_field] = 2
        task_id = f"{usage_field}-over-limit"
        estimate = _estimate(
            task_id,
            progress=1,
            normalized_cost=1,
            tokens=values["tokens"],
            wall_time_ms=values["wall_time_ms"],
            cost_microusd=values["cost_microusd"],
            context_tokens=values["context_tokens"],
            verification_tokens=values["verification_tokens"],
        )
        plan = VerifiedProgressBudgetPolicy().plan(
            _state(ready=(task_id,)),
            {task_id: estimate},
            _limits(1),
            ComputeBudgetUsage(),
            max_parallelism=1,
        )
        decision = plan.decisions[0]
        passed = bool(
            not plan.selected_task_ids
            and plan.deferred_task_ids == (task_id,)
            and decision.reason == "budget_exceeded"
            and decision.budget_blockers == (usage_field,)
        )
        cases[usage_field] = {
            "task_id": task_id,
            "selected_task_ids": list(plan.selected_task_ids),
            "deferred_task_ids": list(plan.deferred_task_ids),
            "reason": decision.reason,
            "budget_blockers": list(decision.budget_blockers),
            "passed": passed,
        }
    return cases


def _unknown_estimate_case() -> dict[str, Any]:
    state = _state(ready=("known", "missing", "unknown"))
    estimates = {
        "known": _estimate(
            "known",
            progress=1,
            normalized_cost=1,
            tokens=1,
            wall_time_ms=1,
            cost_microusd=1,
            context_tokens=1,
            verification_tokens=1,
        ),
        "unknown": _estimate(
            "unknown",
            progress=100,
            normalized_cost=1,
            tokens=1,
            wall_time_ms=1,
            cost_microusd=1,
            context_tokens=1,
            verification_tokens=1,
            known=False,
        ),
    }
    plan = VerifiedProgressBudgetPolicy().plan(
        state,
        estimates,
        _limits(),
        ComputeBudgetUsage(),
        max_parallelism=3,
    )
    decisions = {item.task_id: item for item in plan.decisions}
    passed = bool(
        plan.selected_task_ids == ("known",)
        and set(plan.deferred_task_ids) == {"missing", "unknown"}
        and decisions["missing"].reason == "estimate_unknown"
        and decisions["unknown"].reason == "estimate_unknown"
        and not plan.safe_under_declared_budget
    )
    return {
        "selected_task_ids": list(plan.selected_task_ids),
        "deferred_task_ids": list(plan.deferred_task_ids),
        "reasons": {task_id: decision.reason for task_id, decision in sorted(decisions.items())},
        "safe_under_declared_budget": plan.safe_under_declared_budget,
        "unavailable": [item.model_dump(mode="json") for item in plan.unavailable],
        "passed": passed,
    }


def run_benchmark() -> dict[str, Any]:
    """Run the deterministic static-vs-utility compute-budget comparison."""

    state = _state(
        ready=("a-low-yield", "repair", "z-high-yield"),
        repair_ready=("repair",),
        stale=("repair",),
    )
    estimates = _main_estimates()
    limits = _limits()
    usage = ComputeBudgetUsage()

    static = _lexical_plan(
        state,
        estimates,
        limits,
        usage,
        max_parallelism=MAX_PARALLELISM,
    )
    policy_plan = VerifiedProgressBudgetPolicy().plan(
        state,
        estimates,
        limits,
        usage,
        epoch_id=0,
        max_parallelism=MAX_PARALLELISM,
    )
    budget_policy = _policy_payload(policy_plan, estimates)

    static_progress = int(static["expected_verified_progress_numerator"])
    policy_progress = int(budget_policy["expected_verified_progress_numerator"])
    dimension_cases = _dimension_cases()
    unknown_case = _unknown_estimate_case()
    repair_priority = {
        "repair_task_id": "repair",
        "repair_utility_is_lower_than_high_yield": (
            estimates["repair"].expected_verified_progress_numerator
            * estimates["z-high-yield"].cost_denominator
            < estimates["z-high-yield"].expected_verified_progress_numerator
            * estimates["repair"].cost_denominator
        ),
        "static_selected_repair_first": static["selected_task_ids"][0] == "repair",
        "budget_policy_ranked_repair_first": policy_plan.candidate_task_ids[0] == "repair",
        "budget_policy_selected_repair_first": policy_plan.selected_task_ids[0] == "repair",
    }
    comparison = {
        "same_declared_budget": static["limits"] == budget_policy["limits"],
        "same_usage_before": static["usage_before"] == budget_policy["usage_before"],
        "same_parallelism_limit": True,
        "static_expected_verified_progress_numerator": static_progress,
        "budget_policy_expected_verified_progress_numerator": policy_progress,
        "expected_verified_progress_gain_numerator": policy_progress - static_progress,
        "expected_verified_progress_gain_units": (policy_progress - static_progress)
        // EXPECTED_PROGRESS_BASIS_DENOMINATOR,
        "budget_policy_has_more_expected_verified_progress": policy_progress > static_progress,
    }

    violations: list[str] = []
    if static["selected_task_ids"] != ["repair", "a-low-yield"]:
        violations.append("static lexical selection changed")
    if policy_plan.selected_task_ids != ("repair", "z-high-yield"):
        violations.append("verified-progress budget selection changed")
    if not comparison["budget_policy_has_more_expected_verified_progress"]:
        violations.append("budget policy did not improve declared expected verified progress")
    if not all(repair_priority.values()):
        violations.append("repair priority was not preserved")
    if not all(case["passed"] for case in dimension_cases.values()):
        violations.append("one or more hard-budget dimensions failed open")
    if not unknown_case["passed"]:
        violations.append("unknown estimates did not fail closed")

    scope = {
        "offline": True,
        "deterministic": True,
        "controlled_estimates": True,
        "real_policy_under_test": "VerifiedProgressBudgetPolicy",
        "does_not_measure": (
            "real LLM quality, estimate calibration, wall-clock acceleration, "
            "provider pricing, physical CPU/GPU/RAM/VRAM utilization, or production throughput"
        ),
        "interpretation": (
            "This controlled estimate benchmark validates deterministic utility ordering "
            "and declared hard-budget admission. It is not evidence of real-model "
            "performance or real-world cost savings."
        ),
    }
    return {
        "benchmark": BENCHMARK_NAME,
        "benchmark_version": BENCHMARK_VERSION,
        "valid": not violations,
        "violations": violations,
        "scenario": {
            "graph_relative": True,
            "max_parallelism": MAX_PARALLELISM,
            "limits": limits.model_dump(mode="json"),
            "usage_before": usage.model_dump(mode="json"),
            "task_estimates": {
                task_id: estimate.model_dump(mode="json")
                for task_id, estimate in sorted(estimates.items())
            },
        },
        "static_lexical": static,
        "verified_progress_budget": budget_policy,
        "comparison": comparison,
        "repair_priority": repair_priority,
        "unknown_estimate_case": unknown_case,
        "hard_budget_dimension_cases": dimension_cases,
        "scope": scope,
    }


def main() -> int:
    report = run_benchmark()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "BENCHMARK_NAME",
    "BENCHMARK_VERSION",
    "MAX_PARALLELISM",
    "STATIC_POLICY_ID",
    "main",
    "run_benchmark",
]
