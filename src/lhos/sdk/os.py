"""LongHorizonOS Public SDK — AgentOS facade (E1, composition root).

`AgentOS` wires a real Agent Kernel + Verified Progress Graph + D2 Scheduler +
D3 into one object so a user can Agent/Goal/run without manual wiring.  It is a
composition/lifecycle facade — NOT a new authority.  Core owns semantic state,
ownership (Kernel Lease), and repair.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import os
import sqlite3
import tempfile
import time
import warnings
from collections.abc import Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from lhos.agent_os.context.estimator import DeterministicByteTokenEstimator
from lhos.agent_os.context.models import ContextManifest
from lhos.agent_os.context.service import ContextService
from lhos.agent_os.sdk.client import create_kernel
from lhos.provenance import (
    ActionGateway,
    CoverageReport,
    ExecutionContext,
)
from lhos.runtimes.multi_agent import (
    AgentDescriptor,
    AgentRegistry,
    AgentSnapshot,
    AsyncWorkerPool,
    CooperativeInterrupt,
    InterruptDelivery,
    InterruptDeliveryStatus,
    InterruptTransition,
    OwnershipHandoffIntent,
    OwnershipHandoffResult,
    WorkerJob,
    create_scheduler,
)
from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.graph_store import GraphStore
from lhos.runtimes.verified_progress.models import (
    ArtifactVersionBinding,
    GoalNode,
    LeaseCommitGuard,
)
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    GraphPatchProposal,
)

from .agent import Agent, _is_async_callable  # runtime import (used by open_run)
from .automatic_rebase import (
    AutomaticRebaseDecision,
    delta_view_for_loaded_context,
    plan_automatic_rebase,
)
from .errors import (
    ConfigurationError,
    ExecutionError,
    SchedulingError,
    VerificationError,
)
from .goal import Goal  # runtime import (used by save_run/_serialize_goal)
from .observability import StatusView  # re-export for CLI
from .observation import ObservationToken
from .provenance import (
    build_coverage_report,
    create_execution_context,
    enforce_verification_coverage,
    resolve_executor_api,
)
from .provider_routing import (
    ComputeProviderRegistry,
    ProviderRoute,
    ProviderRoutingError,
)
from .providers import (
    FactsProvider,
    KernelCapabilityProvider,
    KernelLeaseProvider,
    KernelProcessProvider,
    VPGFacade,
)
from .result import OnlineExecutionLoopResult, RepairOutcome, RunResult
from .runtime_state import GlobalRuntimeState, build_runtime_state_view
from .status import StatusSnapshot
from .verification import VerificationOutcome

if TYPE_CHECKING:
    from .compute_budget import (
        ComputeBudgetLimits,
        ComputeBudgetUsage,
        TaskComputeEstimate,
        VerifiedProgressBudgetPlan,
    )
    from .conflict_graph import ConflictGraph
    from .frontier_policy import FrontierRankingStrategy


_logger = logging.getLogger(__name__)


def _bounded_audit_error(exc: BaseException, *, limit: int = 240) -> str:
    """Return a deterministic, bounded error string for audit metadata."""

    text = f"{type(exc).__name__}: {exc}".replace("\r", " ").replace("\n", " ").strip()
    return text[:limit] if text else type(exc).__name__


def _bounded_hash(value: Any) -> str:
    """Stable hash for bounded in-memory scheduling metadata."""

    payload = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


_MAX_ADAPTIVE_SCHEDULER_SKIPS = 256
_MAX_ADAPTIVE_SCHEDULER_SKIP_TASK_ID_LENGTH = 160
_MAX_ADAPTIVE_SCHEDULER_SKIP_REASON_LENGTH = 240


def _bounded_scheduler_skip_audit(skipped: Iterable[Any]) -> dict[str, Any]:
    """Return a bounded, immutable projection of Scheduler skip outcomes.

    ``Scheduler.run_pass`` is the authority for admission outcomes.  Adaptive
    policy metadata must therefore retain its ``(task_id, reason)`` records
    rather than inferring skips from the policy's deferred set.  A large ready
    frontier can produce many policy-deferred records, so the diagnostic
    projection is capped while preserving a count/truncation bit.
    """

    records: list[tuple[str, str]] = []
    count = 0
    for item in skipped:
        try:
            raw_task_id, raw_reason = item
        except (TypeError, ValueError):
            continue
        task_id = str(raw_task_id).strip()
        reason = str(raw_reason).replace("\r", " ").replace("\n", " ").strip()
        if not task_id or not reason:
            continue
        count += 1
        if len(records) >= _MAX_ADAPTIVE_SCHEDULER_SKIPS:
            continue
        records.append(
            (
                task_id[:_MAX_ADAPTIVE_SCHEDULER_SKIP_TASK_ID_LENGTH],
                reason[:_MAX_ADAPTIVE_SCHEDULER_SKIP_REASON_LENGTH],
            )
        )
    return {
        "scheduler_skipped": tuple(records),
        "scheduler_skipped_count": count,
        "scheduler_skipped_truncated": count > _MAX_ADAPTIVE_SCHEDULER_SKIPS,
    }


_RESOURCE_AWARE_RUN_AUDIT_SCHEMA_VERSION = "resource-aware-run-audit.v1"
_MAX_RESOURCE_AUDIT_ASSIGNMENTS = 32
_MAX_RESOURCE_AUDIT_DECISIONS = 64
_MAX_RESOURCE_AUDIT_UNAVAILABLE = 32
_MAX_RESOURCE_AUDIT_POOL_IDS = 32
_MAX_RESOURCE_AUDIT_BLOCKERS = 4
_MAX_RESOURCE_AUDIT_MODEL_SLOTS = 4
_MAX_RESOURCE_AUDIT_ID_LENGTH = 160
_MAX_RESOURCE_AUDIT_REASON_LENGTH = 240
_COMPUTE_BUDGET_RUN_AUDIT_SCHEMA_VERSION = "compute-budget-run-audit.v1"
_MAX_COMPUTE_BUDGET_AUDIT_TASK_IDS = 256
_MAX_COMPUTE_BUDGET_AUDIT_TASK_ID_LENGTH = 160
_UNIFIED_ADAPTIVE_RUN_AUDIT_SCHEMA_VERSION = "unified-adaptive-run-audit.v1"


def _bounded_resource_audit_text(value: Any, *, limit: int) -> tuple[str, bool]:
    """Normalize one resource-audit string and report whether it was cut."""

    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    return text[:limit], len(text) > limit


def _bounded_resource_vector_audit(vector: Any) -> tuple[dict[str, Any], bool]:
    """Project a fixed resource vector without retaining arbitrary payloads."""

    slots: list[dict[str, Any]] = []
    slot_count = 0
    truncated = False
    for item in getattr(vector, "model_slots", ()) or ():
        slot_count += 1
        if len(slots) >= _MAX_RESOURCE_AUDIT_MODEL_SLOTS:
            continue
        name, name_truncated = _bounded_resource_audit_text(
            getattr(item, "name", ""),
            limit=_MAX_RESOURCE_AUDIT_ID_LENGTH,
        )
        truncated = truncated or name_truncated
        slots.append(
            {
                "name": name,
                "quantity": max(0, int(getattr(item, "quantity", 0))),
            }
        )
    slots_truncated = slot_count > _MAX_RESOURCE_AUDIT_MODEL_SLOTS
    return (
        {
            "cpu_millis": max(0, int(getattr(vector, "cpu_millis", 0))),
            "ram_bytes": max(0, int(getattr(vector, "ram_bytes", 0))),
            "gpu_count": max(0, int(getattr(vector, "gpu_count", 0))),
            "vram_bytes": max(0, int(getattr(vector, "vram_bytes", 0))),
            "model_slots": tuple(slots),
            "model_slot_count": slot_count,
            "model_slots_truncated": slots_truncated,
        },
        truncated or slots_truncated,
    )


def _bounded_resource_aware_epoch_audit(epoch: Any) -> dict[str, Any]:
    """Return a compact, payload-free resource-policy projection for RunResult.

    The durable ``SchedulingEpoch`` event intentionally keeps its existing
    schema.  This richer diagnostic is only attached to in-memory
    ``RunResult.meta["adaptive_epochs"]`` records.  It admits fixed resource
    scalars and bounded identifiers/reasons only; prompts, Context manifests,
    arbitrary task metadata, and candidate payloads are never copied.
    """

    assignments: list[dict[str, Any]] = []
    assignment_count = 0
    assignment_strings_truncated = False
    decisions: list[dict[str, Any]] = []
    decision_count = 0
    decision_strings_truncated = False
    unavailable: list[dict[str, str]] = []
    unavailable_count = 0
    unavailable_strings_truncated = False
    pool_ids: list[str] = []
    pool_ids_seen: set[str] = set()
    pool_id_count = 0
    pool_id_strings_truncated = False

    def observe_pool_id(raw_value: Any) -> str | None:
        nonlocal pool_id_count, pool_id_strings_truncated
        if raw_value is None:
            return None
        pool_id, was_truncated = _bounded_resource_audit_text(
            raw_value,
            limit=_MAX_RESOURCE_AUDIT_ID_LENGTH,
        )
        pool_id_strings_truncated = pool_id_strings_truncated or was_truncated
        if not pool_id:
            return None
        if pool_id not in pool_ids_seen:
            pool_ids_seen.add(pool_id)
            pool_id_count += 1
            if len(pool_ids) < _MAX_RESOURCE_AUDIT_POOL_IDS:
                pool_ids.append(pool_id)
        return pool_id

    for item in getattr(epoch, "assignments", ()) or ():
        assignment_count += 1
        pool_id = observe_pool_id(getattr(item, "pool_id", None))
        if len(assignments) >= _MAX_RESOURCE_AUDIT_ASSIGNMENTS:
            continue
        task_id, task_id_truncated = _bounded_resource_audit_text(
            getattr(item, "task_id", ""),
            limit=_MAX_RESOURCE_AUDIT_ID_LENGTH,
        )
        resources, resources_truncated = _bounded_resource_vector_audit(
            getattr(item, "resources", None)
        )
        assignment_strings_truncated = (
            assignment_strings_truncated or task_id_truncated or resources_truncated
        )
        assignments.append(
            {
                "task_id": task_id,
                "pool_id": pool_id,
                "resources": resources,
            }
        )

    for item in getattr(epoch, "decisions", ()) or ():
        decision_count += 1
        pool_id = observe_pool_id(getattr(item, "pool_id", None))
        if len(decisions) >= _MAX_RESOURCE_AUDIT_DECISIONS:
            continue
        task_id, task_id_truncated = _bounded_resource_audit_text(
            getattr(item, "task_id", ""),
            limit=_MAX_RESOURCE_AUDIT_ID_LENGTH,
        )
        action_value = getattr(
            getattr(item, "action", ""),
            "value",
            getattr(item, "action", ""),
        )
        action, action_truncated = _bounded_resource_audit_text(
            action_value,
            limit=_MAX_RESOURCE_AUDIT_ID_LENGTH,
        )
        reason, reason_truncated = _bounded_resource_audit_text(
            getattr(item, "reason", ""),
            limit=_MAX_RESOURCE_AUDIT_REASON_LENGTH,
        )
        blockers: list[str] = []
        blocker_count = 0
        blocker_strings_truncated = False
        raw_blockers = getattr(item, "blockers", None)
        if raw_blockers is None:
            # Unified decisions keep each guard's blockers separate.  Admit
            # only those fixed, typed fields into this bounded projection.
            raw_blockers = (
                tuple(getattr(item, "budget_blockers", ()) or ())
                + tuple(getattr(item, "resource_blockers", ()) or ())
                + tuple(getattr(item, "conflict_blockers", ()) or ())
            )
        for raw_blocker in raw_blockers or ():
            blocker_count += 1
            if len(blockers) >= _MAX_RESOURCE_AUDIT_BLOCKERS:
                continue
            blocker, blocker_truncated = _bounded_resource_audit_text(
                raw_blocker,
                limit=_MAX_RESOURCE_AUDIT_REASON_LENGTH,
            )
            blocker_strings_truncated = blocker_strings_truncated or blocker_truncated
            blockers.append(blocker)
        blockers_truncated = blocker_count > _MAX_RESOURCE_AUDIT_BLOCKERS
        decision_strings_truncated = (
            decision_strings_truncated
            or task_id_truncated
            or action_truncated
            or reason_truncated
            or blocker_strings_truncated
            or blockers_truncated
        )
        decisions.append(
            {
                "task_id": task_id,
                "action": action,
                "reason": reason,
                "pool_id": pool_id,
                "blockers": tuple(blockers),
                "blocker_count": blocker_count,
                "blockers_truncated": blockers_truncated,
            }
        )

    for item in getattr(epoch, "unavailable", ()) or ():
        unavailable_count += 1
        if len(unavailable) >= _MAX_RESOURCE_AUDIT_UNAVAILABLE:
            continue
        name, name_truncated = _bounded_resource_audit_text(
            getattr(item, "name", ""),
            limit=_MAX_RESOURCE_AUDIT_ID_LENGTH,
        )
        reason, reason_truncated = _bounded_resource_audit_text(
            getattr(item, "reason", ""),
            limit=_MAX_RESOURCE_AUDIT_REASON_LENGTH,
        )
        unavailable_strings_truncated = (
            unavailable_strings_truncated or name_truncated or reason_truncated
        )
        unavailable.append({"name": name, "reason": reason})

    source_schema_version, source_schema_truncated = _bounded_resource_audit_text(
        getattr(epoch, "schema_version", ""),
        limit=_MAX_RESOURCE_AUDIT_ID_LENGTH,
    )
    policy_id, policy_id_truncated = _bounded_resource_audit_text(
        getattr(epoch, "policy_id", ""),
        limit=_MAX_RESOURCE_AUDIT_ID_LENGTH,
    )
    conflict_graph_hash, conflict_hash_truncated = _bounded_resource_audit_text(
        getattr(epoch, "conflict_graph_hash", ""),
        limit=_MAX_RESOURCE_AUDIT_ID_LENGTH,
    )
    assignments_truncated = assignment_count > _MAX_RESOURCE_AUDIT_ASSIGNMENTS
    decisions_truncated = decision_count > _MAX_RESOURCE_AUDIT_DECISIONS
    unavailable_truncated = unavailable_count > _MAX_RESOURCE_AUDIT_UNAVAILABLE
    pool_ids_truncated = pool_id_count > _MAX_RESOURCE_AUDIT_POOL_IDS or pool_id_strings_truncated
    truncated = bool(
        assignments_truncated
        or decisions_truncated
        or unavailable_truncated
        or pool_ids_truncated
        or assignment_strings_truncated
        or decision_strings_truncated
        or unavailable_strings_truncated
        or source_schema_truncated
        or policy_id_truncated
        or conflict_hash_truncated
    )
    return {
        "schema_version": _RESOURCE_AWARE_RUN_AUDIT_SCHEMA_VERSION,
        "source_schema_version": source_schema_version,
        "policy_id": policy_id,
        "conflict_graph_hash": conflict_graph_hash,
        "parallelism_hint": max(0, int(getattr(epoch, "parallelism_hint", 0))),
        "safe_under_declared_resources": bool(
            getattr(epoch, "safe_under_declared_resources", False)
        ),
        "safe_under_constraints": bool(getattr(epoch, "safe_under_constraints", False)),
        "assignments": tuple(assignments),
        "assignment_count": assignment_count,
        "assignments_truncated": assignments_truncated,
        "decisions": tuple(decisions),
        "decision_count": decision_count,
        "decisions_truncated": decisions_truncated,
        "pool_ids": tuple(pool_ids),
        "pool_id_count": pool_id_count,
        "pool_ids_truncated": pool_ids_truncated,
        "unavailable": tuple(unavailable),
        "unavailable_count": unavailable_count,
        "unavailable_truncated": unavailable_truncated,
        "truncated": truncated,
    }


def _bounded_unified_epoch_audit(epoch: Any) -> dict[str, Any]:
    """Project one unified policy epoch without copying arbitrary payloads.

    ``UnifiedAdaptivePlan`` intentionally exposes both budget and logical
    resource/conflict decisions.  The existing resource audit already
    performs strict bounding of assignments, decisions, blockers, identifiers,
    and vectors; this wrapper adds only fixed scalar/task-id fields that are
    useful for distinguishing a unified epoch in ``RunResult.meta``.
    """

    selected, selected_count, selected_truncated = _bounded_compute_budget_ids(
        getattr(epoch, "selected_task_ids", ()) or ()
    )
    deferred, deferred_count, deferred_truncated = _bounded_compute_budget_ids(
        getattr(epoch, "deferred_task_ids", ()) or ()
    )
    source_schema = str(getattr(epoch, "schema_version", ""))[:160]
    policy_id = str(getattr(epoch, "policy_id", ""))[:160]
    decision_hash = str(getattr(epoch, "decision_hash", ""))[:64]
    conflict_hash = str(getattr(epoch, "conflict_graph_hash", ""))[:64]
    truncated = bool(
        selected_truncated
        or deferred_truncated
        or len(str(getattr(epoch, "schema_version", ""))) > 160
        or len(str(getattr(epoch, "policy_id", ""))) > 160
        or len(str(getattr(epoch, "decision_hash", ""))) > 64
        or len(str(getattr(epoch, "conflict_graph_hash", ""))) > 64
        or len(str(getattr(epoch, "graph_id", ""))) > 160
    )
    return {
        "schema_version": _UNIFIED_ADAPTIVE_RUN_AUDIT_SCHEMA_VERSION,
        "source_schema_version": source_schema,
        "policy_id": policy_id,
        "decision_hash": decision_hash,
        "conflict_graph_hash": conflict_hash,
        "epoch_id": max(0, int(getattr(epoch, "epoch_id", 0))),
        "graph_id": str(getattr(epoch, "graph_id", ""))[:160],
        "graph_version": max(0, int(getattr(epoch, "graph_version", 0))),
        "parallelism_hint": max(0, int(getattr(epoch, "parallelism_hint", 0))),
        "selected_task_ids": selected,
        "selected_task_count": selected_count,
        "deferred_task_ids": deferred,
        "deferred_task_count": deferred_count,
        "safe_under_declared_budget": bool(getattr(epoch, "safe_under_declared_budget", False)),
        "safe_under_declared_resources": bool(
            getattr(epoch, "safe_under_declared_resources", False)
        ),
        "safe_under_constraints": bool(getattr(epoch, "safe_under_constraints", False)),
        "truncated": truncated,
    }


def _bounded_compute_budget_ids(values: Iterable[Any]) -> tuple[tuple[str, ...], int, bool]:
    """Return a bounded, normalized task-id projection for budget audit."""

    records: list[str] = []
    count = 0
    strings_truncated = False
    for value in values:
        task_id = str(value).strip()
        if not task_id:
            continue
        count += 1
        strings_truncated = (
            strings_truncated or len(task_id) > _MAX_COMPUTE_BUDGET_AUDIT_TASK_ID_LENGTH
        )
        if len(records) < _MAX_COMPUTE_BUDGET_AUDIT_TASK_IDS:
            records.append(task_id[:_MAX_COMPUTE_BUDGET_AUDIT_TASK_ID_LENGTH])
    return (
        tuple(records),
        count,
        bool(strings_truncated or count > _MAX_COMPUTE_BUDGET_AUDIT_TASK_IDS),
    )


def _compute_budget_limits_audit(limits: Any) -> dict[str, int | None]:
    """Project only the five declared hard-budget ceilings."""

    return {
        "max_tokens": getattr(limits, "max_tokens", None),
        "max_wall_time_ms": getattr(limits, "max_wall_time_ms", None),
        "max_cost_microusd": getattr(limits, "max_cost_microusd", None),
        "max_context_tokens": getattr(limits, "max_context_tokens", None),
        "max_verification_tokens": getattr(limits, "max_verification_tokens", None),
    }


def _compute_budget_usage_audit(usage: Any) -> dict[str, int]:
    """Project only fixed integer consumption dimensions."""

    return {
        "tokens": max(0, int(getattr(usage, "tokens", 0))),
        "wall_time_ms": max(0, int(getattr(usage, "wall_time_ms", 0))),
        "cost_microusd": max(0, int(getattr(usage, "cost_microusd", 0))),
        "context_tokens": max(0, int(getattr(usage, "context_tokens", 0))),
        "verification_tokens": max(
            0,
            int(getattr(usage, "verification_tokens", 0)),
        ),
    }


def _bounded_compute_budget_epoch_audit(
    plan: Any,
    *,
    dispatched_declared_usage_after: Any,
    actual_dispatched_task_ids: Iterable[Any] = (),
) -> dict[str, Any]:
    """Return a compact budget audit without copying arbitrary task metadata.

    Both usage-after projections in this audit are *declared*, never measured:

    * ``planned_usage_after`` is the policy's reservation-style projection for
      every selected task.
    * ``dispatched_declared_usage_after`` sums the declared estimates of only
      the tasks the Scheduler actually dispatched, so partial admission charges
      a subset of the plan.  It is still a declared projection -- the real
      measured consumption is reported separately as ``measured_usage`` on the
      RunResult, never under an "actual"/"usage_after" label here.
    """

    selected, selected_count, selected_truncated = _bounded_compute_budget_ids(
        getattr(plan, "selected_task_ids", ()) or ()
    )
    deferred, deferred_count, deferred_truncated = _bounded_compute_budget_ids(
        getattr(plan, "deferred_task_ids", ()) or ()
    )
    dispatched, dispatched_count, dispatched_truncated = _bounded_compute_budget_ids(
        actual_dispatched_task_ids
    )
    decision_hash = str(getattr(plan, "decision_hash", ""))[:64]
    source_schema_version = str(getattr(plan, "schema_version", ""))[:160]
    policy_id = str(getattr(plan, "policy_id", ""))[:160]
    truncated = bool(
        selected_truncated
        or deferred_truncated
        or dispatched_truncated
        or len(str(getattr(plan, "decision_hash", ""))) > 64
        or len(str(getattr(plan, "schema_version", ""))) > 160
        or len(str(getattr(plan, "policy_id", ""))) > 160
    )
    return {
        "schema_version": _COMPUTE_BUDGET_RUN_AUDIT_SCHEMA_VERSION,
        "source_schema_version": source_schema_version,
        "policy_id": policy_id,
        "decision_hash": decision_hash,
        "limits": _compute_budget_limits_audit(getattr(plan, "limits", None)),
        "usage_before": _compute_budget_usage_audit(getattr(plan, "usage_before", None)),
        "planned_usage_after": _compute_budget_usage_audit(getattr(plan, "usage_after", None)),
        "dispatched_declared_usage_after": _compute_budget_usage_audit(
            dispatched_declared_usage_after
        ),
        "selected_task_ids": selected,
        "selected_task_count": selected_count,
        "selected_task_ids_truncated": selected_truncated,
        "deferred_task_ids": deferred,
        "deferred_task_count": deferred_count,
        "deferred_task_ids_truncated": deferred_truncated,
        "actual_dispatched_task_ids": dispatched,
        "actual_dispatched_task_count": dispatched_count,
        "actual_dispatched_task_ids_truncated": dispatched_truncated,
        "safe_under_declared_budget": bool(getattr(plan, "safe_under_declared_budget", False)),
        "truncated": truncated,
    }


def _prepare_compute_budget_run_inputs(
    *,
    budget_aware: bool,
    adaptive: bool,
    budget_estimates: Any,
    budget_limits: Any,
    budget_usage: Any,
    resource_aware: bool,
    conflict_graph: Any,
    automatic_rebase: bool,
) -> tuple[Any | None, Any | None, Any | None]:
    """Validate and snapshot the opt-in execution-time budget inputs.

    Budget and logical-resource admission can be composed by the explicit
    ``budget_aware=True, resource_aware=True`` mode.  In that mode the caller
    gets the unified policy; budget-only remains intentionally isolated from a
    caller-supplied conflict graph so an accidental partial composition cannot
    silently bypass resource/conflict guards.
    """

    from .compute_budget import ComputeBudgetLimits, ComputeBudgetUsage

    if not isinstance(budget_aware, bool):
        raise ConfigurationError("budget_aware must be a boolean")
    supplied = budget_estimates is not None or budget_limits is not None or budget_usage is not None
    if not budget_aware:
        if supplied:
            raise ConfigurationError(
                "budget_estimates, budget_limits, and budget_usage require budget_aware=True"
            )
        return None, None, None
    if not adaptive:
        raise ConfigurationError("budget_aware requires adaptive=True")
    if budget_estimates is None:
        raise ConfigurationError("budget_estimates is required when budget_aware=True")
    if budget_limits is None:
        raise ConfigurationError("budget_limits is required when budget_aware=True")
    if not isinstance(budget_limits, ComputeBudgetLimits):
        raise ConfigurationError("budget_limits must be a ComputeBudgetLimits")
    if budget_usage is not None and not isinstance(budget_usage, ComputeBudgetUsage):
        raise ConfigurationError("budget_usage must be a ComputeBudgetUsage or None")
    if conflict_graph is not None and not resource_aware:
        raise ConfigurationError(
            "budget-only execution with conflict_graph requires resource_aware=True"
        )
    if automatic_rebase:
        raise ConfigurationError(
            "budget_aware cannot be combined with automatic_rebase in compute-budget v1; "
            "pass automatic_rebase=False"
        )

    estimates_snapshot: Any
    if isinstance(budget_estimates, Mapping):
        # Snapshot the caller-owned container once so every scheduling epoch
        # sees the same declared estimates.  Individual malformed entries are
        # intentionally retained: the pure policy converts them to
        # fail-closed ``estimate_unknown`` decisions.
        estimates_snapshot = {
            key: dict(value) if isinstance(value, Mapping) else value
            for key, value in budget_estimates.items()
        }
    else:
        if isinstance(budget_estimates, (str, bytes, bytearray)):
            raise ConfigurationError("budget_estimates must be a mapping or iterable")
        try:
            estimates_snapshot = tuple(budget_estimates)
        except TypeError as exc:
            raise ConfigurationError(
                "budget_estimates must be a mapping or iterable",
                cause=exc,
            ) from exc
    effective_usage = ComputeBudgetUsage() if budget_usage is None else budget_usage
    return estimates_snapshot, budget_limits, effective_usage


def _accumulate_dispatched_compute_budget_usage(
    plan: Any,
    usage: Any,
    dispatched_task_ids: Iterable[Any],
) -> Any:
    """Charge declared estimates for exactly the Scheduler-dispatched tasks.

    Charging happens before executor/verifier work, so failed and
    stale-cognition attempts still consume the declared budget.  A graph race
    that returns no dispatches consumes nothing.  Any Scheduler result outside
    the policy-selected set is an invariant violation and fails closed.
    """

    selected = {
        str(task_id).strip()
        for task_id in (getattr(plan, "selected_task_ids", ()) or ())
        if str(task_id).strip()
    }
    decisions = {
        str(getattr(item, "task_id", "")).strip(): item
        for item in (getattr(plan, "decisions", ()) or ())
        if str(getattr(item, "task_id", "")).strip()
    }
    next_usage = usage
    for raw_task_id in dispatched_task_ids:
        task_id = str(raw_task_id).strip()
        decision = decisions.get(task_id)
        delta = None if decision is None else getattr(decision, "budget_delta", None)
        action = (
            ""
            if decision is None
            else str(
                getattr(
                    getattr(decision, "action", ""),
                    "value",
                    getattr(decision, "action", ""),
                )
            )
        )
        if task_id not in selected or action != "run" or delta is None:
            raise SchedulingError(
                "Scheduler dispatched a task without a selected, known compute estimate"
            )
        next_usage = next_usage.plus(delta)
    return next_usage


@dataclass(frozen=True)
class _BudgetMeasurementContext:
    """Run-scoped inputs needed to record measured usage for one goal.

    ``declared_by_task`` maps a task id to the *declared* (post-calibration)
    budget delta that admitted it, expressed as a :class:`UsageVector`.  It is
    the "declared" half every ledger record needs so the calibrator can later
    compare it against the measured half.  This is intentionally passed through
    the execution call rather than stored as long-lived mutable state.
    """

    goal_id: str
    declared_by_task: Mapping[str, Any]


def _extract_measured_computation_cost(outcome: Any) -> Any | None:
    """Best-effort extraction of executor/provider-supplied cost counters.

    Returns a :class:`ComputationCost` when the outcome (or a nested ``cost`` /
    ``computation_cost`` / ``usage`` attribute, or a mapping) exposes recognized
    token/cost counters, otherwise ``None``.  Never raises: measurement must not
    break execution.
    """

    from lhos.runtimes.multi_agent import ComputationCost

    if outcome is None:
        return None
    if isinstance(outcome, ComputationCost):
        return outcome
    source: Mapping[str, Any] | None = outcome if isinstance(outcome, Mapping) else None
    for attribute in ("cost", "computation_cost", "usage", "trace", "partial_outcome"):
        nested = source.get(attribute) if source is not None else getattr(outcome, attribute, None)
        if isinstance(nested, ComputationCost):
            return nested
        if nested is not None and nested is not outcome:
            extracted = _extract_measured_computation_cost(nested)
            if extracted is not None:
                return extracted
    fields = {
        "input_tokens": ("input_tokens", "prompt_tokens", "uncached_input_tokens"),
        "output_tokens": ("output_tokens", "completion_tokens"),
        "cached_input_tokens": ("cached_input_tokens", "cached_tokens"),
        "model_calls": ("model_calls",),
        "tool_calls": ("tool_calls", "tool_call_count"),
        "elapsed_ms": ("elapsed_ms", "wall_time_ms"),
        "monetary_micros": (
            "monetary_micros",
            "cost_microusd",
            "monetary_microusd",
        ),
    }
    values: dict[str, int] = {}
    for target, aliases in fields.items():
        for alias in aliases:
            raw = source.get(alias) if source is not None else getattr(outcome, alias, None)
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                continue
            try:
                coerced = max(0, int(raw))
            except (TypeError, ValueError):
                continue
            values[target] = coerced
            break
        if target == "cached_input_tokens" and target not in values:
            cache_parts: list[int] = []
            for alias in ("cache_read_tokens", "cache_write_tokens"):
                raw = source.get(alias) if source is not None else getattr(outcome, alias, None)
                if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                    continue
                cache_parts.append(max(0, int(raw)))
            if cache_parts:
                values[target] = sum(cache_parts)
    if not values:
        return None
    try:
        return ComputationCost(**values)
    except Exception:
        return None


def _record_measured_attempt(
    ledger: Any,
    *,
    goal_id: str,
    task_id: str,
    attempt_id: str,
    declared: Any,
    measured: Any,
) -> Any:
    """Record one attempt's declared estimate and MEASURED outcome on a ledger.

    Returns the (immutable) next ledger, or the input ledger unchanged when the
    identity is incomplete or the transition conflicts.  Accounting is
    observability, never a correctness gate: any failure is swallowed with a
    debug log so a measurement hiccup can never fail an otherwise valid attempt.
    """

    from .compute_usage import UsageVector

    goal_id = str(goal_id).strip()
    task_id = str(task_id).strip()
    attempt_id = str(attempt_id).strip()
    if not (goal_id and task_id and attempt_id):
        return ledger
    if not isinstance(declared, UsageVector) or not isinstance(measured, UsageVector):
        return ledger
    try:
        estimated_ledger = ledger.record_estimate(goal_id, task_id, attempt_id, declared)
        return estimated_ledger.record_measured(goal_id, task_id, attempt_id, measured)
    except Exception as exc:  # pragma: no cover - defensive; measurement is best-effort
        _logger.debug(
            "compute-usage measurement skipped for %s/%s/%s: %s",
            goal_id,
            task_id,
            attempt_id,
            exc,
        )
        return ledger


def _calibrate_and_build_measurement(
    ledger: Any,
    *,
    budget_aware: bool,
    estimates_snapshot: Any,
    goal_id: str,
    attempts: Any = (),
) -> tuple[Any, tuple[Any, ...], _BudgetMeasurementContext | None]:
    """Calibrate declared estimates from measured history and build the context.

    ``ledger`` is the measured history observed *before* this run.  With an
    empty ledger and no attempt history calibration is the identity, so the
    returned snapshot, plan, and decision hashes are byte-for-byte what they are
    today.  The returned :class:`_BudgetMeasurementContext` carries the declared
    (post-calibration) budget delta per task so the execution path can record
    the declared/measured pair the calibrator needs next time.

    Two different corrections are applied.  Cost dimensions are scaled by a
    measured/declared ratio.  The ranking inputs -- how often a task actually
    verifies, and how often its inputs held still long enough to commit -- are
    observable *rates*, so the observation replaces the declared opinion instead
    of scaling it.  Together these are what stop the budget policy from
    optimising numbers the caller made up.
    """

    if not budget_aware:
        return estimates_snapshot, (), None
    from .compute_budget import TaskComputeEstimate
    from .compute_calibration import (
        calibrate_estimates,
        calibrate_outcome_estimates,
        observe_task_outcomes,
    )
    from .compute_usage import UsageVector

    calibrated, cost_audits = calibrate_estimates(estimates_snapshot, ledger)
    audits: tuple[Any, ...] = tuple(cost_audits)
    outcomes = observe_task_outcomes(attempts or ())
    if outcomes:
        calibrated, outcome_audits = calibrate_outcome_estimates(calibrated, outcomes)
        audits = (*audits, *outcome_audits)
    if isinstance(calibrated, Mapping):
        items: Iterable[tuple[Any, Any]] = list(calibrated.items())
    else:
        items = [(getattr(item, "task_id", None), item) for item in calibrated]
    declared_by_task: dict[str, UsageVector] = {}
    for _key, estimate in items:
        if isinstance(estimate, TaskComputeEstimate) and estimate.known:
            delta = estimate.total_budget_delta
            declared_by_task[str(estimate.task_id)] = UsageVector(
                tokens=delta.tokens,
                wall_time_ms=delta.wall_time_ms,
                cost_microusd=delta.cost_microusd,
                context_tokens=delta.context_tokens,
                verification_tokens=delta.verification_tokens,
            )
    measurement = _BudgetMeasurementContext(
        goal_id=str(goal_id),
        declared_by_task=declared_by_task,
    )
    return calibrated, tuple(audits), measurement


def _bounded_compute_routing_summary(decision: Any) -> dict[str, Any]:
    """Redact a routing decision to a compact JSON/audit projection.

    Candidate locality scores can contain one entry per registered Agent and
    are intentionally omitted from per-epoch metadata.  The complete
    decision remains available from the explicit ``plan_compute_routing``
    facade; adaptive execution records only the stable recommendation and
    decision hash.
    """

    unavailable = tuple(
        sorted(
            {
                str(getattr(item, "name", "")).strip()
                for item in (getattr(decision, "unavailable", ()) or ())
                if str(getattr(item, "name", "")).strip()
            }
        )
    )
    reasons = tuple(
        str(item)[:160]
        for item in tuple(getattr(decision, "reasons", ()) or ())[:8]
        if str(item).strip()
    )
    return {
        "status": "ok",
        "task_id": str(getattr(decision, "task_id", "")),
        "action": getattr(getattr(decision, "action", None), "value", decision.action),
        "selected_agent_id": getattr(decision, "selected_agent_id", None),
        "locality_score": float(getattr(decision, "locality_score", 0.0)),
        "model_tier": getattr(getattr(decision, "model_tier", None), "value", decision.model_tier),
        "context_budget_tokens": int(getattr(decision, "context_budget_tokens", 0)),
        "verification_strength": getattr(
            getattr(decision, "verification_strength", None),
            "value",
            decision.verification_strength,
        ),
        "eligible": bool(getattr(decision, "eligible", False)),
        "unavailable": unavailable,
        "reasons": reasons,
        "decision_hash": str(getattr(decision, "decision_hash", "")),
    }


class _ClaimFenceLost(Exception):
    """Internal control flow: this execution no longer owns the task claim."""


class _StaleCognition(Exception):
    """Internal control flow for an attempt whose exact input read-set changed."""

    def __init__(self, reason: str) -> None:
        self.reason = str(reason)
        super().__init__(self.reason)


@dataclass(frozen=True, slots=True)
class _SDKDispatchResult:
    """Operational result passed from the SDK executor to the worker pool."""

    attempt_id: str
    executor_outcome: Any = None
    dispatched: bool = True
    error: str | None = None
    # Provenance is an observation attached to the operational dispatch.  It
    # is consumed by the SDK semantic commit path; it never becomes VPG truth
    # without the normal Evidence verifier.
    provenance_context: ExecutionContext | None = None
    coverage_report: CoverageReport | None = None
    executor_api: str = "legacy_task_id"
    provider_route: ProviderRoute | None = None
    provider_context: Any | None = None
    # Monotonic span this dispatcher observed around the async executor.  The
    # sync path times its executor directly; without this the async path could
    # only report counters the executor chose to supply, so an executor that
    # reported nothing looked free.
    executor_elapsed_ms: int | None = None


class _SDKExecutorDispatcher:
    """Adapt SDK Agent executors to the AsyncWorkerPool dispatcher protocol."""

    def __init__(
        self,
        os_: AgentOS,
        tasks: dict[str, Any],
        graph_id: str = "",
        *,
        goal: Goal | None = None,
        provider_routing_enabled: bool = False,
        context_overrides: Mapping[str, tuple[AutomaticRebaseDecision, ContextManifest]]
        | None = None,
    ) -> None:
        self._os = os_
        self._tasks = tasks
        self._graph_id = graph_id
        self._goal = goal
        self._provider_routing_enabled = provider_routing_enabled
        self._context_overrides = context_overrides or {}

    async def dispatch(
        self,
        *,
        agent_id: str,
        task_id: str,
        task_kind: str,
        claim_id: str,
        execution_spec: dict[str, Any],
        cancellation_token: Any | None = None,
        **kwargs: Any,
    ) -> _SDKDispatchResult:
        del task_kind, execution_spec, kwargs
        agent = self._os._agents.get(agent_id)
        if agent is None:
            raise ConfigurationError(f"scheduled agent {agent_id!r} is not registered")
        task = self._tasks.get(task_id)
        if task is None:
            raise ConfigurationError(f"scheduled task {task_id!r} is not in the Goal")
        api = resolve_executor_api(task=task, agent=agent)
        context_override = self._context_overrides.get(task_id)
        override_manifest = None if context_override is None else context_override[1]
        context = self._os._new_execution_context(
            self._graph_id or f"sdk-dispatch:{task_id}",
            task,
            claim_id=claim_id,
            executor_api=api,
            cancellation_token=cancellation_token if api == "context_v1" else None,
            context_manifest_override=override_manifest,
            automatic_rebase_decision=(None if context_override is None else context_override[0]),
        )
        if context_override is not None and getattr(context, "loaded_context", None) is not None:
            # Keep the full refreshed manifest available to a fresh callback,
            # while exposing a bounded changed-page projection for incremental
            # rebase-aware Harnesses/agents.
            context.automatic_rebase_delta = delta_view_for_loaded_context(
                context.loaded_context,
                context_override[0],
            )
        provider_route = None
        provider_context = context
        if self._provider_routing_enabled and self._goal is not None:
            provider_route = self._os._resolve_provider_route(
                self._goal,
                task,
                graph_id=self._graph_id,
                claim_id=claim_id,
            )
            if provider_route is not None:
                registry = self._os._provider_registry
                if registry is None:
                    raise ProviderRoutingError("provider registry unavailable")
                provider_context = registry.adapt_context(
                    provider_route,
                    str(task_id),
                    context,
                )
        outcome = None
        # Monotonic, not wall-clock: a clock adjustment must not be able to
        # produce a negative or absurd measured span.
        executor_started_at = time.monotonic()
        if provider_route is not None:
            registry = self._os._provider_registry
            if registry is None:
                raise ProviderRoutingError("provider registry unavailable")
            outcome = await registry.execute_async(
                provider_route,
                str(task_id),
                provider_context,
                agent.executor,
            )
        elif agent.executor is not None:
            outcome = await _invoke_executor_async(
                agent.executor,
                task_id,
                context=context,
                executor_api=api,
            )
        executor_elapsed_ms = max(0, int((time.monotonic() - executor_started_at) * 1000))
        report = build_coverage_report(task, context, executor_api=api)
        return _SDKDispatchResult(
            attempt_id=context.attempt_id or f"sdk-{claim_id}",
            executor_outcome=outcome,
            provenance_context=context,
            coverage_report=report,
            executor_api=api,
            provider_route=provider_route,
            provider_context=provider_context,
            executor_elapsed_ms=executor_elapsed_ms,
        )

    def supports_interrupt_token(self, job: WorkerJob) -> bool:
        """Opt into cooperative interrupts only for context-aware callbacks."""

        task = self._tasks.get(job.task_id)
        agent = self._os._agents.get(job.agent_id)
        if task is None or agent is None:
            return False
        return resolve_executor_api(task=task, agent=agent) == "context_v1"


class _SDKWorkerLifecycle:
    """Fence worker cleanup to the exact claims submitted by this SDK batch."""

    def __init__(self, scheduler: Any, jobs: list[WorkerJob]) -> None:
        self._scheduler = scheduler
        self._claim_by_task = {(job.graph_id, job.task_id): job.claim_id for job in jobs}

    def register_job(self, job: WorkerJob) -> None:
        """Admit a streaming-refill job into this lifecycle claim fence.

        Without this, ``mark_execution_started`` rejects the refilled claim as
        "unknown or terminal" because the fence was built from the initial batch.
        """
        self._claim_by_task[(job.graph_id, job.task_id)] = job.claim_id

    def _live_expected_claim(self, graph_id: str, task_id: str) -> Any | None:
        expected = self._claim_by_task.get((graph_id, task_id))
        claim = self._scheduler.active_claim_for_task(task_id, graph_id)
        if claim is None or claim.claim_id != expected:
            return None
        return claim

    def mark_execution_started(self, claim_id: str) -> Any | None:
        claim = next(
            (
                claim
                for claim in self._scheduler.claims
                if claim.claim_id == claim_id
                and self._live_expected_claim(claim.graph_id, claim.task_id) is not None
            ),
            None,
        )
        if claim is None:
            return None
        return self._scheduler.mark_execution_started(claim_id)

    def mark_execution_operationally_succeeded(self, claim_id: str) -> Any | None:
        claim = next(
            (
                claim
                for claim in self._scheduler.claims
                if claim.claim_id == claim_id
                and self._live_expected_claim(claim.graph_id, claim.task_id) is not None
            ),
            None,
        )
        if claim is None:
            raise _ClaimFenceLost
        return self._scheduler.mark_execution_operationally_succeeded(claim_id)

    def release_task(
        self,
        graph_id: str,
        task_id: str,
        *,
        reason: str = "execution_failed",
        retry: bool = True,
        expected_claim_id: str | None = None,
    ) -> Any:
        # ``expected_claim_id`` is part of the WorkerLifecycle protocol.  The
        # SDK computes the live claim from its fenced batch, but accepting the
        # optional argument keeps this adapter structurally compatible with
        # AsyncWorkerPool and lets callers explicitly reinforce the fence.
        claim = self._live_expected_claim(graph_id, task_id)
        if claim is None:
            return None
        if expected_claim_id is not None and str(expected_claim_id) != claim.claim_id:
            return None
        release = self._scheduler.release_task
        # Keep integrations that provide a legacy SchedulerSession-shaped
        # test double working, while the built-in API always receives the
        # fencing token.  Signature inspection avoids masking real TypeErrors
        # raised by the release implementation itself.
        try:
            signature = inspect.signature(release)
            supports_fence = "expected_claim_id" in signature.parameters or any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD
                for parameter in signature.parameters.values()
            )
        except (TypeError, ValueError):
            supports_fence = True
        kwargs = {"reason": reason, "retry": retry}
        if supports_fence:
            kwargs["expected_claim_id"] = claim.claim_id
        return release(graph_id, task_id, **kwargs)


class _ReadOnlyProcessProvider:
    def get(self, pid: str) -> Any | None:
        return None

    def list_all(self) -> list[Any]:
        return []

    def spawn(self, program_id: str | None = None) -> str:
        raise ConfigurationError("read-only AgentOS cannot spawn processes")

    def set_failed(self, pid: str) -> None:
        raise ConfigurationError("read-only AgentOS cannot change process state")


class _ReadOnlyLeaseProvider:
    def acquire_exclusive(self, pid: str, resource_id: str, ttl) -> Any | None:
        return None

    def release(self, lease_id: str) -> bool:
        return False

    def release_all_for_pid(self, pid: str) -> int:
        return 0

    def get(self, lease_id: str) -> Any | None:
        return None

    def list_for_resource(self, resource_id: str) -> list[Any]:
        return []

    def list_for_pid(self, pid: str) -> list[Any]:
        return []

    def reclaim_expired(self) -> int:
        return 0


class _ReadOnlyCapabilityProvider:
    def check(self, pid: str, resource: str, operation: str) -> bool:
        return False

    def capabilities_for(self, pid: str) -> list[Any]:
        return []


class _SDKContextCapabilities:
    """Narrow Context VM capability adapter for SDK-owned processes."""

    def __init__(self, process_provider: Any) -> None:
        self._process_provider = process_provider

    def can_context_operation(
        self,
        *,
        pid: str,
        operation: str,
        working_set_id: str | None = None,
        context_id: str | None = None,
    ) -> bool:
        del working_set_id, context_id
        return operation in {"load", "read", "snapshot", "close"} and (
            self._process_provider.get(pid) is not None
        )

    def can_artifact_read(self, *, pid: str, artifact_id: str, version: int) -> bool:
        del artifact_id, version
        return self._process_provider.get(pid) is not None


class AgentOS:
    """Top-level composition root for a Core-backed LongHorizonOS instance."""

    def __init__(
        self,
        db_path: str = ":memory:",
        *,
        facts: FactsProvider | None = None,
        read_only: bool = False,
        secure_mode: bool = False,
        action_gateway: ActionGateway | Any | None = None,
        context_service: ContextService | None = None,
        provider_registry: ComputeProviderRegistry | None = None,
    ) -> None:
        self._db_path = db_path
        self._read_only = read_only
        self._secure_mode = bool(secure_mode)
        self._action_gateway = action_gateway
        self._injected_context_service = context_service
        self._provider_registry = provider_registry
        self._closed = False
        # Create constructor-unwind state before any operation that can fail.
        # ``close`` is invoked from the constructor exception path.
        # A graph may have more than one concurrent ``run_async`` caller.
        # Index pools by the exact claim they own rather than by graph id;
        # otherwise a later run would overwrite the earlier pool and semantic
        # interrupts could be routed to the wrong worker (or cleanup of one
        # run could hide another).
        self._active_worker_pools: dict[str, AsyncWorkerPool] = {}
        self._ephemeral_db_path: Path | None = None
        if read_only and db_path == ":memory:":
            raise ConfigurationError(
                "read-only AgentOS requires an existing durable database path; "
                "':memory:' has no state to observe"
            )
        storage_db_path = db_path
        if db_path == ":memory:" and not read_only:
            fd, temp_path = tempfile.mkstemp(prefix="lhos-agentos-", suffix=".sqlite3")
            os.close(fd)
            self._ephemeral_db_path = Path(temp_path)
            storage_db_path = temp_path
        try:
            self._initialize_runtime(
                storage_db_path,
                db_path=db_path,
                facts=facts,
                read_only=read_only,
            )
        except BaseException:
            # Constructor failures must not strand a temporary SQLite backing
            # file or any handles that were opened before the failure.
            with suppress(BaseException):
                self.close()
            raise

    def _initialize_runtime(
        self,
        storage_db_path: str,
        *,
        db_path: str,
        facts: FactsProvider | None,
        read_only: bool,
    ) -> None:
        self._storage_db_path = storage_db_path
        if read_only:
            self._kernel = None
        elif self._secure_mode:
            self._kernel = create_kernel(
                storage_db_path,
                strict_effect_contracts=True,
            )
        else:
            # Preserve the legacy one-argument construction shape for
            # integrations that replace/create-kernel with a compatibility
            # factory. Strict admission is only meaningful in secure mode.
            self._kernel = create_kernel(storage_db_path)
        self._owns_facts = facts is None
        self._facts = facts or FactsProvider(
            storage_db_path,
            read_only=read_only,
            action_service=None if self._kernel is None else self._kernel._action_service,
        )
        self._read_only_conn: sqlite3.Connection | None = None
        vpg_store: GraphStore | str = storage_db_path
        if read_only:
            resolved_db = Path(db_path).resolve()
            if not resolved_db.exists():
                raise ConfigurationError(f"read-only database not found: {resolved_db}")
            self._read_only_conn = sqlite3.connect(
                f"file:{resolved_db.as_posix()}?mode=ro",
                uri=True,
                check_same_thread=False,
            )
            vpg_store = GraphStore(self._read_only_conn, read_only=True)
        self._vpg = VerifiedProgressRuntime(
            vpg_store, facts_artifact=self._facts, facts_kernel=self._facts
        )  # type: ignore[arg-type]
        self._vpg_surface = VPGFacade(self._vpg)
        self._proc = (
            _ReadOnlyProcessProvider() if read_only else KernelProcessProvider(self._kernel)
        )
        self._lease = _ReadOnlyLeaseProvider() if read_only else KernelLeaseProvider(self._kernel)
        self._cap = (
            _ReadOnlyCapabilityProvider() if read_only else KernelCapabilityProvider(self._kernel)
        )
        self._context_service = self._injected_context_service
        if self._context_service is None and not read_only:
            self._context_service = ContextService(
                content_supplier=self._facts,
                capability_checker=_SDKContextCapabilities(self._proc),
                estimator=DeterministicByteTokenEstimator(),
            )
        self._registry = AgentRegistry()
        self._scheduler = create_scheduler(
            self._registry,
            vpg=self._vpg_surface,
            process_provider=self._proc,
            lease_provider=self._lease,
            capability_provider=self._cap,
            # ``:memory:`` AgentOS uses a temporary SQLite backing file only
            # to let the Kernel/VPG share one connection path during this
            # process.  It is not reopenable, so do not pay the durable
            # Scheduler journal/snapshot cost for that ephemeral mode.
            state_path=(None if read_only or db_path == ":memory:" else storage_db_path),
        )
        self._agents: dict[str, Agent] = {}
        self._agent_pid: dict[str, str] = {}
        # Keep one stable composition-root owner for SDK operations that are
        # created before any user Agent is registered.  Creating a fresh
        # process on every call leaks durable process projections and makes
        # ownership/audit history depend on call count.
        self._sdk_root_pid: str | None = None
        self._next_artifact_version: dict[str, int] = {}
        self._goals: dict[str, Goal] = {}
        self._goal_gid: dict[str, str] = {}
        self._last_repair: RepairOutcome | None = None
        # Harnesses are execution units managed by the OS control plane.
        # Registration is deliberately in-memory: the Harness owns its own
        # session/checkpoint durability, while Scheduler/VPG remain the
        # authorities for Claims, Leases, and semantic Evidence.  The maps
        # are keyed by exact durable identities so a replacement session
        # cannot accidentally receive an old process' control request.
        self._harnesses_by_session: dict[str, Any] = {}
        self._harness_session_by_claim: dict[str, str] = {}
        self._harness_control_cache: dict[tuple[str, str], tuple[str, str, Any]] = {}
        # Immutable, in-memory ledger of MEASURED attempt usage.  It accumulates
        # across run()/run_async() invocations on this instance so the declared
        # estimates of later runs can be calibrated from earlier measurements.
        from .compute_usage import UsageLedger

        self._usage_ledger = UsageLedger.empty()

    @property
    def usage_ledger(self) -> Any:
        """Read-only view of the MEASURED compute-usage ledger for this OS."""

        return self._usage_ledger

    def _measured_usage_audit(self, goal_id: str) -> dict[str, int]:
        """Project the cumulative MEASURED usage observed for one goal."""

        aggregate = self._usage_ledger.aggregate(goal_id=str(goal_id))
        return _compute_budget_usage_audit(aggregate.measured)

    def _resolve_provider_route(
        self,
        goal: Goal,
        task: Any,
        *,
        graph_id: str,
        claim_id: str,
    ) -> ProviderRoute | None:
        """Resolve an explicit provider route after Claim/Context fencing."""

        del claim_id
        registry = self._provider_registry
        metadata = getattr(task, "metadata", {})
        if not isinstance(metadata, Mapping):
            return None
        routing = metadata.get("compute_routing")
        if not isinstance(routing, Mapping):
            return None
        provider = routing.get("provider_routing")
        if not isinstance(provider, Mapping) or not provider.get("enabled", False):
            return None
        if registry is None:
            raise ProviderRoutingError(
                "task opted into provider routing but AgentOS has no provider_registry"
            )
        try:
            state = self.runtime_state(goal)
            if state.graph_id != graph_id:
                raise ProviderRoutingError("provider route graph identity changed")
            return registry.route_task(state, str(task.task_id), metadata)
        except ProviderRoutingError:
            raise
        except Exception as exc:
            raise ProviderRoutingError(
                f"provider route resolution failed for task {task.task_id!r}: {exc}"
            ) from exc

    def _new_execution_context(
        self,
        graph_id: str,
        task: Any,
        *,
        claim_id: str = "",
        executor_api: str | None = None,
        cancellation_token: Any | None = None,
        context_manifest_override: ContextManifest | None = None,
        automatic_rebase_decision: AutomaticRebaseDecision | None = None,
    ) -> ExecutionContext:
        """Create an execution context bound to the live Scheduler attempt.

        The context is an observation recorder, not a semantic authority.  We
        nevertheless bind the current attempt/epoch here so any events emitted
        by a ``context_v1`` callback can be audited against the exact claim.
        """
        task_id = str(getattr(task, "task_id", task))
        attempt = self._scheduler.attempt_for_claim(claim_id) if claim_id else None
        manifest, handle, loaded_context, snapshot = self._materialize_attempt_context(
            task,
            claim_id=claim_id,
            attempt=attempt,
            manifest_override=context_manifest_override,
        )
        context = create_execution_context(
            graph_id,
            task_id,
            claim_id=claim_id,
            attempt_id="" if attempt is None else str(attempt.attempt_id),
            semantic_epoch=0 if attempt is None else int(attempt.semantic_epoch),
            executor_api=executor_api or resolve_executor_api(task=task),
            secure_mode=self._secure_mode,
            action_gateway=self._action_gateway,
            context_snapshot=snapshot,
            context_manifest_id=manifest.manifest_id,
            context_handle=handle,
            loaded_context=loaded_context,
        )
        claim = next(
            (item for item in self._scheduler.claims if item.claim_id == claim_id),
            None,
        )
        context.graph_version = (
            int(getattr(attempt, "graph_version", 0))
            if attempt is not None
            else int(getattr(claim, "graph_version", 0))
        )
        context.agent_id = (
            str(getattr(attempt, "agent_id", "") or "")
            if attempt is not None
            else str(getattr(claim, "agent_id", "") or "")
        )
        context.process_id = (
            str(getattr(attempt, "process_id", "") or "")
            if attempt is not None
            else str(getattr(claim, "process_id", "") or "")
        )
        context.lease_id = getattr(claim, "lease_id", None)
        context.lease_fencing_token = getattr(
            claim,
            "lease_fencing_token",
            None,
        )
        context.automatic_rebase_manifest = manifest
        if automatic_rebase_decision is not None:
            # These are deliberately opaque integration attributes on
            # ExecutionContext.  They are not semantic truth and are never
            # accepted as Evidence without the normal read-set fence.
            context.automatic_rebase_decision = automatic_rebase_decision
            context.automatic_rebase_action = automatic_rebase_decision.action.value
            context.automatic_rebase_source_attempt_id = automatic_rebase_decision.source_attempt_id
            context.automatic_rebase_delta_ref_ids = automatic_rebase_decision.delta_ref_ids
            if loaded_context is not None:
                context.automatic_rebase_delta = delta_view_for_loaded_context(
                    loaded_context,
                    automatic_rebase_decision,
                )
        if cancellation_token is not None:
            context.bind_cancellation_token(cancellation_token)
        return context

    def _materialize_attempt_context(
        self,
        task: Any,
        *,
        claim_id: str,
        attempt: Any,
        manifest_override: ContextManifest | None = None,
    ) -> tuple[ContextManifest, Any, Any, Any]:
        """Create and fence one real Context VM snapshot for an attempt."""

        if self._context_service is None:
            raise ConfigurationError("Context VM is unavailable in read-only AgentOS")
        if attempt is None or not str(getattr(attempt, "process_id", "")):
            raise ConfigurationError("Context VM requires a live scheduled execution attempt")

        pid = str(attempt.process_id)
        template = manifest_override or getattr(task, "context_manifest", None)
        if template is None:
            manifest = ContextManifest(
                manifest_id=f"ctxm-{attempt.attempt_id}",
                owner_pid=pid,
                refs=(),
                token_budget=1,
                metadata={"source": "sdk-empty-default"},
            )
        else:
            manifest = template.model_copy(update={"owner_pid": pid})

        key = f"attempt:{attempt.attempt_id}:context"
        try:
            handle, loaded = self._context_service.load(
                manifest=manifest,
                caller_pid=pid,
                idempotency_key=key,
            )
            snapshot = self._context_service.snapshot(
                pid=pid,
                context_id=loaded.context_id,
                idempotency_key=key,
            )
        except Exception as exc:
            raise ConfigurationError(
                "failed to materialize Task.context_manifest", cause=exc
            ) from exc

        bind = getattr(self._scheduler, "bind_attempt_context_snapshot", None)
        if not callable(bind):
            raise ConfigurationError(
                "scheduler integration cannot bind the Context VM snapshot to the attempt"
            )
        if not bind(
            claim_id,
            snapshot_id=snapshot.snapshot_id,
            manifest_id=manifest.manifest_id,
            manifest_hash=snapshot.manifest_hash,
            working_set_hash=snapshot.working_set_hash,
            materialized_hash=snapshot.materialized_hash,
        ):
            raise _ClaimFenceLost
        return manifest, handle, loaded, snapshot

    def close(self) -> None:
        """Release the kernel and VPG database handles."""
        if self._closed:
            return
        # Constructor failure can call ``close`` before the runtime has
        # finished initializing these optional registries.  Teardown must be
        # idempotent and must not mask the original initialization error (or
        # strand the temporary SQLite backing file).
        errors: list[BaseException] = []
        harnesses = tuple(
            {
                id(harness): harness
                for harness in getattr(self, "_harnesses_by_session", {}).values()
            }.values()
        )
        for harness in harnesses:
            closer = getattr(harness, "close", None)
            if not callable(closer):
                continue
            try:
                closer()
            except BaseException as exc:
                errors.append(RuntimeError(f"failed to close Harness adapter: {exc}"))
        getattr(self, "_active_worker_pools", {}).clear()
        getattr(self, "_harnesses_by_session", {}).clear()
        getattr(self, "_harness_session_by_claim", {}).clear()
        getattr(self, "_harness_control_cache", {}).clear()
        closers: list[tuple[str, Any]] = [
            ("scheduler", getattr(getattr(self, "_scheduler", None), "close", None)),
            ("vpg", getattr(getattr(self, "_vpg", None), "close", None)),
            (
                "facts",
                getattr(getattr(self, "_facts", None), "close", None)
                if getattr(self, "_owns_facts", False)
                else None,
            ),
            ("read_only_conn", getattr(getattr(self, "_read_only_conn", None), "close", None)),
            ("kernel", getattr(getattr(self, "_kernel", None), "close", None)),
        ]
        for name, closer in closers:
            if not callable(closer):
                continue
            try:
                closer()
            except BaseException as exc:
                errors.append(RuntimeError(f"failed to close {name}: {exc}"))

        if self._ephemeral_db_path is not None:
            for suffix in ("", "-wal", "-shm"):
                try:
                    Path(f"{self._ephemeral_db_path}{suffix}").unlink()
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    errors.append(
                        RuntimeError(
                            f"failed to remove temporary database "
                            f"{self._ephemeral_db_path}{suffix}: {exc}"
                        )
                    )
        self._closed = True
        if errors:
            raise errors[0]

    def __enter__(self) -> AgentOS:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ── agents ───────────────────────────────────────────────────────────────
    def add_agent(self, agent: Agent) -> Agent:
        if self._read_only:
            raise ConfigurationError("read-only AgentOS cannot register agents")
        pid = self._proc.spawn(agent.name)
        agent._bind_process(pid)
        caps = (
            ("shell", "filesystem", "network") if agent.capabilities is None else agent.capabilities
        )
        self._grant_capabilities(pid, caps)
        self._registry.register(
            AgentDescriptor(
                agent_id=agent.name,
                process_id=pid,
                supported_task_kinds=agent.supported_task_kinds,
                supported_tools=(
                    tuple(caps) if agent.supported_tools is None else agent.supported_tools
                ),
                specializations=tuple(sorted(agent.specializations)),
                max_concurrency=agent.max_concurrency,
                cost_weight=max(1, round(agent.cost_weight * 100)),
                resource_capacity=agent.resource_capacity,
            )
        )
        self._agents[agent.name] = agent
        self._agent_pid[agent.name] = pid
        # A durable scheduler projection may contain claims owned by a worker
        # process from an earlier AgentOS instance.  Fence that process before
        # this descriptor becomes eligible for new work.
        self._scheduler.retire_agent_process(agent.name, pid)
        self._scheduler.refresh_registry_resources()
        return agent

    def apply_host_capacity(
        self,
        pool_id: str,
        telemetry: Any,
        policy: Any | None = None,
    ) -> Any:
        """Explicitly map one host sample onto one registered logical pool.

        This is a caller-triggered logical-admission update only.  It does not
        start telemetry polling, place processes/devices, isolate memory,
        enforce OS quotas, or update any pool other than ``pool_id``.
        """

        from .host_capacity import (
            HostCapacityApplyResult,
            derive_host_capacity,
        )

        if self._read_only:
            raise ConfigurationError("read-only AgentOS cannot apply host capacity")
        normalized_pool_id = str(pool_id).strip()
        if not normalized_pool_id:
            raise ConfigurationError("pool_id must be non-empty")
        descriptor = self._registry.get(normalized_pool_id)
        agent = self._agents.get(normalized_pool_id)
        if descriptor is None or agent is None:
            raise ConfigurationError(
                f"host capacity pool {normalized_pool_id!r} is not a registered Agent pool"
            )

        decision = derive_host_capacity(telemetry, policy)
        previous = self._scheduler.resource_manager.capacity(normalized_pool_id)
        if not decision.available or decision.capacity is None:
            return HostCapacityApplyResult(
                pool_id=normalized_pool_id,
                decision=decision,
                applied=False,
                previous_capacity=previous,
                reason="host capacity decision is unavailable; logical pool unchanged",
            )

        capacity = decision.capacity
        # Scheduler owns one lifecycle lock for the allocator + Registry
        # update, so no scheduling pass can observe a half-published capacity.
        try:
            previous = self._scheduler.update_registered_resource_capacity(
                normalized_pool_id,
                capacity,
            )
        except Exception as exc:
            raise ConfigurationError(
                f"derived host capacity is below active reservations or cannot be applied: {exc}",
                cause=exc,
            ) from exc

        try:
            agent.resource_capacity = capacity
        except Exception as exc:
            agent.resource_capacity = previous
            raise ConfigurationError(
                f"failed to update SDK Agent resource capacity: {exc}",
                cause=exc,
            ) from exc

        return HostCapacityApplyResult(
            pool_id=normalized_pool_id,
            decision=decision,
            applied=True,
            previous_capacity=previous,
            applied_capacity=capacity,
            reason="derived host capacity applied to named logical Scheduler pool",
        )

    def _grant_capabilities(self, pid: str, caps: tuple[str, ...]) -> None:
        import contextlib

        from .providers import make_capability

        if self._kernel is None:
            raise ConfigurationError("read-only AgentOS cannot grant capabilities")
        for pat in caps:
            with contextlib.suppress(Exception):
                self._kernel._capability_service.grant(
                    pid, make_capability(pat, ("read", "write", "execute"))
                )

    # ── goals ────────────────────────────────────────────────────────────────
    def goal(self, goal_id: str, *, tasks: tuple = ()) -> Goal:
        from .goal import Goal

        g = Goal(goal_id, tasks=tasks)
        self._goals[goal_id] = g
        return g

    def _gid_for(self, goal_id: str, compile_if_missing: bool = False) -> str | None:
        # compiled Goal -> graph_id map is kept in _goal_gid
        gid = self._goal_gid.get(goal_id)
        if gid is None and compile_if_missing:
            g = self._goals.get(goal_id)
            if g is not None:
                gid = self._compile_goal(g)
        return gid

    def observe_artifact(
        self,
        goal: Goal | str,
        artifact_id: str,
        version: int,
        content: str | bytes | None = None,
        *,
        expected_hash: str | None = None,
    ) -> ObservationToken:
        """Issue a graph-bound observation token for an exact artifact snapshot.

        ``content`` is optional only when the exact ``artifact_id@version`` is
        already present in the FactsProvider.  New observations must provide
        bytes (or use :meth:`observe_workspace_artifact`); a bare integer can
        never mint synthetic ``body-vN`` content through this API.
        """

        if self._read_only:
            raise ExecutionError("read-only AgentOS cannot issue observation tokens")
        goal_id = goal.goal_id if isinstance(goal, Goal) else str(goal)
        gid = self._gid_for(goal_id, compile_if_missing=True)
        if gid is None:
            raise ConfigurationError(f"goal {goal_id!r} not registered")
        try:
            return self._facts.issue_observation(
                artifact_id,
                version,
                graph_id=gid,
                content=content,
                expected_hash=expected_hash,
            )
        except (TypeError, ValueError, RuntimeError) as exc:
            raise ConfigurationError(
                f"cannot issue artifact observation: {exc}", cause=exc
            ) from exc

    def register_external_fact(
        self,
        resource_uri: str,
        version: int,
        content: str | bytes,
        *,
        expected_hash: str | None = None,
    ) -> str:
        """Register one externally observed resource version with Facts.

        This is the public authority bridge used by mediated API/tool
        adapters.  It records exact caller-supplied bytes under a positive,
        immutable version; it does not perform network I/O, discover hidden
        dependencies, issue an ObservationToken, or infer trust from a
        transport validator.

        HTTP(S) identities are canonicalized with the same rule as
        ``HTTPProvenanceGateway`` so the eventual AgentSnapshot read guard
        matches this authority entry exactly.  Other resource schemes retain
        their explicit URI spelling.
        """

        if self._read_only:
            raise ExecutionError("read-only AgentOS cannot register external facts")
        canonical_uri = str(resource_uri).strip()
        if not canonical_uri:
            raise ConfigurationError("external fact resource_uri must be non-empty")
        if canonical_uri.lower().startswith(("http://", "https://")):
            # Lazy import preserves the SDK/Core dependency direction until
            # an HTTP fact is explicitly registered.
            from lhos.integrations.tools.provenance_http import canonical_http_url

            try:
                canonical_uri = canonical_http_url(canonical_uri)
            except Exception as exc:
                raise ConfigurationError(
                    f"invalid external HTTP resource URI: {exc}",
                    cause=exc,
                ) from exc
        payload = content.encode("utf-8") if isinstance(content, str) else bytes(content)
        digest = self._facts.content_hash(payload)
        if expected_hash is not None:
            normalized_expected = str(expected_hash).strip().lower().removeprefix("sha256:")
            if normalized_expected != digest:
                raise ConfigurationError(
                    f"external fact hash mismatch for {canonical_uri!r}@{version}"
                )
        try:
            self._facts.add_version(canonical_uri, version, payload)
        except (TypeError, ValueError, RuntimeError) as exc:
            raise ConfigurationError(
                f"cannot register external fact: {exc}",
                cause=exc,
            ) from exc
        return canonical_uri

    def observe_workspace_artifact(
        self,
        goal: Goal | str,
        workspace: Any,
        rel: str,
        *,
        version: int | None = None,
        expected_hash: str | None = None,
    ) -> ObservationToken:
        """Observe bytes from a root-scoped WorkspaceTool and issue a token.

        If ``version`` is omitted, unchanged bytes reuse the latest registered
        ArtifactVersion and changed bytes select the next monotonic version.
        This makes watcher initialization idempotent across process restarts:
        reopening the same durable database cannot manufacture a new version
        solely because the in-memory watcher baseline was lost.  The selected
        version is still backed by the exact bytes read from ``workspace``.
        """

        try:
            payload = workspace.read_bytes(rel)
        except Exception as exc:
            raise ConfigurationError(
                f"cannot read workspace artifact {rel!r}: {exc}", cause=exc
            ) from exc
        if version is None:
            artifact_id = self._facts.normalize_artifact_id(rel)
            latest = self._facts.latest(artifact_id)
            if latest is None:
                version = 1
            else:
                latest_hash = self._facts.read_hash(
                    "sdk-workspace-observation",
                    artifact_id,
                    latest,
                )
                payload_hash = self._facts.content_hash(payload)
                version = latest if latest_hash == payload_hash else latest + 1
        return self.observe_artifact(
            goal,
            rel,
            version,
            payload,
            expected_hash=expected_hash,
        )

    def workspace_gateway(
        self,
        workspace: Any,
        context: ExecutionContext,
        task: Any | None = None,
        *,
        readable: tuple[str, ...] = (),
        writable: tuple[str, ...] = (),
        strict: bool | None = None,
        version_validator: Any | None = None,
        version_authority: Any | None = None,
    ) -> Any:
        """Create a provenance-aware workspace gateway bound to this ``AgentOS``.

        The standalone :class:`WorkspaceProvenanceGateway` intentionally
        requires callers to provide a version authority when a strict
        ``version=`` binding is used.  This composition-root helper wires the
        current ``FactsProvider`` automatically, so SDK callbacks do not need
        to reach into the private ``_facts`` field or duplicate authority
        plumbing.

        ``task`` may be supplied to derive read/write capabilities from its
        declared ``inputs``/``outputs``.  Otherwise pass explicit
        ``readable``/``writable`` resources.  An explicit
        ``version_validator`` or ``version_authority`` always takes
        precedence; the helper never registers a version or turns a caller
        integer into a fact.  Consequently an unregistered strict version
        still fails closed in the gateway.

        This is a mediated filesystem boundary only.  Direct ``open``/Path,
        network, browser, and subprocess I/O remain outside its coverage.
        """

        # Import lazily to keep the SDK composition root independent from the
        # optional integration package at module import time.
        from lhos.integrations.tools.provenance_workspace import (
            WorkspaceProvenanceGateway,
        )

        bind: Any
        bind_kwargs: dict[str, Any]
        if task is not None:
            if readable or writable:
                raise ConfigurationError(
                    "workspace_gateway accepts either task capabilities or "
                    "explicit readable/writable resources, not both"
                )
            bind = WorkspaceProvenanceGateway.for_task
            bind_kwargs = {
                "strict": strict,
                "version_validator": version_validator,
                "version_authority": version_authority,
            }
        else:
            bind = WorkspaceProvenanceGateway
            bind_kwargs = {
                "readable": readable,
                "writable": writable,
                "strict": strict,
                "version_validator": version_validator,
                "version_authority": version_authority,
            }

        # FactsProvider is the SDK's durable artifact authority.  Inject it
        # for strict bindings (including secure contexts) only when the caller
        # did not provide an alternative validator or authority.  Explicit
        # ``strict=False`` keeps the standalone gateway's compatibility
        # behavior: an unregistered caller version remains an auditable
        # ``version_source="caller"`` binding rather than becoming an
        # unexpected authority lookup failure.
        effective_strict = (
            bool(strict) if strict is not None else bool(getattr(context, "secure_mode", False))
        )
        if effective_strict and version_validator is None and version_authority is None:
            bind_kwargs["version_authority"] = self._facts

        if task is not None:
            return bind(workspace, context, task, **bind_kwargs)
        return bind(workspace, context, **bind_kwargs)

    def workspace_watcher(
        self,
        goal: Goal | str,
        workspace: Any,
        resources: Any,
        *,
        task_ids_by_resource: Mapping[str, Any] | None = None,
    ) -> Any:
        """Create a bounded polling watcher for explicit workspace inputs.

        The watcher observes only the supplied resources and issues
        authority-backed observation tokens for changed bytes.  It is an
        online observation/control primitive: callers still decide whether
        to persist interrupt proposals or deliver cooperative control to a
        running Harness.  Unmediated Python, network, browser, and subprocess
        I/O remain outside its coverage.
        """

        from .watchers import WorkspaceObservationWatcher

        return WorkspaceObservationWatcher(
            self,
            goal,
            workspace,
            resources,
            task_ids_by_resource=task_ids_by_resource,
        )

    def poll_workspace_and_route(
        self,
        watcher: Any,
        *,
        epoch_id: int = 0,
        persist: bool = False,
        reconcile_after_delivery: bool = False,
    ) -> Any:
        """Poll an explicit workspace watcher and deliver cooperative actions.

        This is a small ``AgentOS`` convenience facade over
        :meth:`WorkspaceObservationWatcher.poll_and_route`.  It intentionally
        accepts an already-created watcher so its in-memory observation
        baseline is preserved across calls.  The method performs one
        caller-invoked pass only; it never starts a background watcher,
        scheduler, or daemon.

        Ownership and semantic state remain with the existing Scheduler/VPG
        authorities.  The watcher itself remains fail-closed for undeclared,
        deleted, and incomplete observations.
        """

        route = getattr(watcher, "poll_and_route", None)
        if not callable(route):
            raise ConfigurationError(
                "poll_workspace_and_route requires a WorkspaceObservationWatcher "
                "created by AgentOS.workspace_watcher(...)"
            )
        return route(
            epoch_id=epoch_id,
            persist=persist,
            reconcile_after_delivery=reconcile_after_delivery,
        )

    def route_workspace_observation(
        self,
        watcher: Any,
        observation: Any,
        *,
        epoch_id: int = 0,
        persist: bool = False,
        reconcile_after_delivery: bool = False,
    ) -> Any:
        """Route a previously captured workspace observation through the OS.

        ``observation`` may be a ``WorkspaceWatchPoll`` returned by
        :meth:`poll_workspace_and_route`/``watcher.poll`` or an explicit
        iterable of immutable ``WorkspaceObservationChange`` values.  This
        facade does not broaden the observation scope and does not create
        Claims or Leases.
        """

        route = getattr(watcher, "route_observation", None)
        if not callable(route):
            raise ConfigurationError(
                "route_workspace_observation requires a WorkspaceObservationWatcher "
                "created by AgentOS.workspace_watcher(...)"
            )
        return route(
            observation,
            epoch_id=epoch_id,
            persist=persist,
            reconcile_after_delivery=reconcile_after_delivery,
        )

    def _compile_goal(self, goal: Goal) -> str:
        """Compile a Goal + Tasks into a real VPG patch; returns graph_id."""
        self._goals.setdefault(goal.goal_id, goal)
        pid = self._owner_pid()
        gid = self._vpg.create_graph(owner_pid=pid).graph_id
        ops: list[Any] = [
            AddNodeOp(
                node_id=goal.goal_id,
                graph_id=gid,
                node_type="goal",
                created_by_pid=pid,
                title=goal.goal_id,
            )
        ]
        for t in goal.tasks:
            metadata = dict(t.metadata)
            scheduler_metadata = dict(metadata.get("scheduler", {}))
            scheduler_metadata.update(
                {
                    "task_kind": t.task_kind,
                    "required_specializations": list(t.required_specializations),
                    "required_tools": list(t.required_tools),
                    "max_attempts": t.max_attempts,
                    "resources": t.resources.model_dump(mode="json"),
                }
            )
            metadata["scheduler"] = scheduler_metadata
            sdk_metadata = dict(metadata.get("sdk", {}))
            sdk_metadata["agent"] = t.agent
            metadata["sdk"] = sdk_metadata
            ops.append(
                AddNodeOp(
                    node_id=t.task_id,
                    graph_id=gid,
                    node_type="task",
                    created_by_pid=pid,
                    task_kind=t.task_kind,
                    metadata=metadata,
                ),
            )
            ops.append(
                AddEdgeOp(
                    edge_type="depends_on",
                    source_node_id=goal.goal_id,
                    target_node_id=t.task_id,
                    created_by_pid=pid,
                )
            )
            for dep in t.depends_on:
                ops.append(
                    AddEdgeOp(
                        edge_type="depends_on",
                        source_node_id=t.task_id,
                        target_node_id=dep.task_id,
                        created_by_pid=pid,
                    )
                )
        self._submit_compiled_ops(gid, pid, goal.goal_id, ops)
        self._goal_gid[goal.goal_id] = gid
        return gid

    def _submit_compiled_ops(self, gid: str, pid: str, goal_id: str, ops: list[Any]) -> None:
        """Publish a compiled Goal as one trusted atomic graph transaction.

        User-authored patches remain bounded by ``MAX_PATCH_OPS``.  Goal
        compilation is a trusted composition-root operation, so it uses the
        private large-operation admission path.  This prevents a partially
        compiled graph from becoming visible to readiness/scheduling.
        """
        self._vpg.submit_patch(
            GraphPatchProposal(
                graph_id=gid,
                expected_graph_version=self._vpg.get_graph(gid).current_version,
                author_pid=pid,
                idempotency_key=f"compile-{goal_id}",
                operations=tuple(ops),
            ),
            _allow_large_operations=True,
        )

    def _owner_pid(self) -> str:
        if self._agents:
            return next(iter(self._agent_pid.values()))
        if self._sdk_root_pid is None:
            self._sdk_root_pid = self._proc.spawn("sdk-root")
        return self._sdk_root_pid

    def _coerce_adaptive_conflict_graph(
        self,
        goal: Goal,
        conflict_graph: ConflictGraph | None,
    ) -> ConflictGraph | None:
        """Resolve the optional conflict graph for the adaptive run path.

        A caller-provided graph is trusted only as a *policy input* and is
        validated before use.  When omitted, we derive a conservative graph
        from the task-level ``inputs``/``outputs`` declarations already
        present in the SDK.  A task with no declarations is marked
        ``known=False``; the policy will therefore keep it serial-only rather
        than pretending that it is independent.  This is not automatic
        provenance discovery: hidden file/API/tool/Python reads remain outside
        this bounded path.
        """

        from .conflict_graph import ConflictGraph, TaskAccessSet

        if conflict_graph is not None:
            if not isinstance(conflict_graph, ConflictGraph):
                raise ConfigurationError(
                    "conflict_graph must be a ConflictGraph when adaptive execution is enabled"
                )
            return conflict_graph

        access_sets: list[TaskAccessSet] = []
        for task in goal.tasks:
            reads = tuple(
                str(value).strip() for value in getattr(task, "inputs", ()) if str(value).strip()
            )
            writes = tuple(
                str(value).strip() for value in getattr(task, "outputs", ()) if str(value).strip()
            )
            # Explicit declarations are the only safe source for this
            # derived graph.  Empty declarations are unknown, not empty.
            access_sets.append(
                TaskAccessSet(
                    task_id=str(task.task_id),
                    read_set=reads,
                    write_set=writes,
                    known=bool(reads or writes),
                )
            )
        return ConflictGraph.from_access_sets(access_sets)

    def _declared_read_keys_by_task(self, goal: Goal) -> dict[str, tuple[str, ...]]:
        """Per-task declared reads, for agent context-residency matching.

        Emits both the raw declared string and its normalized artifact id
        because provenance records the raw ``resource_uri`` while graph-side
        declarations are frequently written as bare artifact ids.  Only
        explicitly declared inputs are used: an undeclared read must not be
        able to earn a locality bonus it cannot prove.
        """

        result: dict[str, tuple[str, ...]] = {}
        for task in tuple(getattr(goal, "tasks", ()) or ()):
            task_id = str(getattr(task, "task_id", "")).strip()
            if not task_id:
                continue
            keys: set[str] = set()
            for value in tuple(getattr(task, "inputs", ()) or ()):
                raw = str(value).strip()
                if not raw:
                    continue
                keys.add(raw)
                normalized = raw
                for prefix in ("workspace://", "vpg://workspace/"):
                    if normalized.startswith(prefix):
                        normalized = normalized[len(prefix) :].lstrip("/")
                        break
                normalized = FactsProvider.normalize_artifact_id(normalized)
                if normalized:
                    keys.add(normalized)
            if keys:
                result[task_id] = tuple(sorted(keys))
        return result

    def _plan_adaptive_epoch(
        self,
        goal: Goal,
        *,
        epoch_id: int,
        max_parallelism: int,
        conflict_graph: ConflictGraph | None,
    ) -> Any:
        """Observe state and produce one bounded WHAT/WHEN policy proposal.

        This method has no side effects.  The returned task ids are handed to
        ``SchedulerSession.run_pass`` as both an advisory filter and a dispatch
        *ranking*; Scheduler eligibility, resource admission, Claim, and Kernel
        Lease fencing remain the execution authorities.

        The ranking uses graph utility (repair first, then critical-path
        position, then downstream unlock value) rather than task-id order.
        Lexical order would make the ranking carry no information, which is the
        difference between an OS that schedules and one that merely admits.
        """

        if isinstance(max_parallelism, bool) or not isinstance(max_parallelism, int):
            raise ConfigurationError("adaptive max_parallelism must be an integer")
        if max_parallelism < 1:
            raise ConfigurationError("adaptive max_parallelism must be >= 1")
        state = self.runtime_state(goal)
        if conflict_graph is None:
            from .frontier_policy import FrontierPolicy, FrontierRankingStrategy

            return FrontierPolicy(
                max_parallelism=max_parallelism,
                ranking_strategy=FrontierRankingStrategy.GRAPH_UTILITY,
            ).plan(
                state,
                epoch_id=epoch_id,
            )
        from .conflict_graph import DynamicParallelismPolicy

        return DynamicParallelismPolicy(max_parallelism=max_parallelism).suggest(
            state,
            conflict_graph,
            epoch_id=epoch_id,
        )

    def _plan_resource_aware_epoch(
        self,
        goal: Goal,
        *,
        epoch_id: int,
        max_parallelism: int,
        conflict_graph: ConflictGraph | None,
    ) -> Any:
        """Plan one resource-aware advisory epoch without side effects."""

        from .resource_policy import ResourceAwareParallelismPolicy

        state = self.runtime_state(goal)
        graph = conflict_graph or self._coerce_adaptive_conflict_graph(goal, None)
        if graph is None:
            raise ConfigurationError("resource-aware planning requires a ConflictGraph")
        requests = {str(task.task_id): getattr(task, "resources", None) for task in goal.tasks}
        return ResourceAwareParallelismPolicy(max_parallelism=max_parallelism).suggest(
            state,
            graph,
            requests,
            epoch_id=epoch_id,
        )

    def _plan_unified_adaptive_epoch(
        self,
        goal: Goal,
        *,
        epoch_id: int,
        max_parallelism: int,
        conflict_graph: ConflictGraph | None,
        estimates: Any,
        limits: Any,
        usage: Any,
    ) -> Any:
        """Plan one unified budget/conflict/resource adaptive epoch.

        This is the composition-root adapter for
        :class:`UnifiedAdaptivePolicy`.  The policy itself remains pure and
        graph-relative; this method only supplies the current immutable
        runtime projection plus the task declarations already owned by the
        SDK.
        """

        from .unified_policy import UnifiedAdaptivePolicy

        if conflict_graph is None:
            raise ConfigurationError("unified adaptive planning requires a ConflictGraph")
        state = self.runtime_state(goal)
        task_resources = {
            str(task.task_id): getattr(task, "resources", None) for task in goal.tasks
        }
        return UnifiedAdaptivePolicy(max_parallelism=max_parallelism).plan(
            state,
            conflict_graph,
            task_resources,
            estimates,
            limits,
            usage,
            epoch_id=epoch_id,
        )

    def _plan_budget_aware_epoch(
        self,
        goal: Goal,
        *,
        epoch_id: int,
        max_parallelism: int,
        estimates: Any,
        limits: Any,
        usage: Any,
    ) -> Any:
        """Plan one graph-fenced verified-progress budget epoch."""

        from .compute_budget import VerifiedProgressBudgetPolicy

        state = self.runtime_state(goal)
        return VerifiedProgressBudgetPolicy().plan(
            state,
            estimates,
            limits,
            usage,
            epoch_id=epoch_id,
            max_parallelism=max_parallelism,
        )

    def _adaptive_epoch_metadata(
        self,
        goal: Goal,
        epoch: Any,
        *,
        resource_aware: bool = False,
        unified_control: bool = False,
    ) -> dict[str, Any]:
        """Build bounded audit metadata for one adaptive scheduling epoch.

        ``Task.metadata["compute_routing"]`` is an explicit, opt-in policy
        input.  We evaluate it only as a read-only advisory after the
        frontier/conflict policy has produced its epoch.  The returned
        summaries never alter the selected batch, Scheduler claims, Kernel
        leases, provider/model choice, or Context VM state.  A
        ``resource_aware`` epochs additionally receive the bounded
        ``resource_audit`` projection; unified epochs also receive a compact
        ``unified_audit`` projection.  These projections are RunResult
        metadata only and do not alter the durable SchedulingEpoch schema.

        The policy is deliberately fail-soft here: malformed metadata or a
        racing graph observation is recorded as a bounded audit status rather
        than making an otherwise valid execution fail.  This keeps compute
        routing an observability surface until a future provider-backed
        integration can establish stronger contracts.
        """

        metadata: dict[str, Any] = {
            "epoch_id": epoch.epoch_id,
            "selected_task_ids": tuple(epoch.selected_task_ids),
            "deferred_task_ids": tuple(epoch.deferred_task_ids),
            "decision_hash": epoch.decision_hash,
            "fallback_attempted": False,
        }
        if resource_aware:
            metadata["resource_audit"] = _bounded_resource_aware_epoch_audit(epoch)
        if unified_control:
            metadata["unified_audit"] = _bounded_unified_epoch_audit(epoch)
        task_by_id = {str(task.task_id): task for task in goal.tasks}
        candidate_ids = tuple(
            sorted(
                {
                    str(task_id).strip()
                    for task_id in (
                        getattr(epoch, "candidate_task_ids", ())
                        or (tuple(epoch.selected_task_ids) + tuple(epoch.deferred_task_ids))
                    )
                    if str(task_id).strip()
                }
            )
        )
        explicit_tasks = tuple(
            task_by_id[task_id]
            for task_id in candidate_ids
            if task_id in task_by_id
            and isinstance(getattr(task_by_id[task_id], "metadata", None), Mapping)
            and isinstance(
                getattr(task_by_id[task_id], "metadata", {}).get("compute_routing"),
                Mapping,
            )
        )
        if not explicit_tasks:
            return metadata

        routing_audit = self._compute_routing_audit(
            goal,
            explicit_tasks,
            expected_graph_version=int(getattr(epoch, "graph_version", -1)),
        )
        if routing_audit:
            metadata["compute_routing"] = routing_audit
        return metadata

    def _compute_routing_audit(
        self,
        goal: Goal,
        tasks: tuple[Any, ...],
        *,
        expected_graph_version: int,
    ) -> dict[str, dict[str, Any]]:
        """Evaluate explicit per-task routing metadata for audit only.

        Only fields understood by :class:`CandidateTaskMetadata` are copied;
        arbitrary task metadata is never passed to the policy.  Summaries are
        intentionally bounded (no candidate score vectors or untrusted
        prompts/contents) and are deterministic for a pinned runtime state.
        """

        from .compute_routing import CandidateTaskMetadata, ComputeRoutingPolicy

        try:
            state = self.runtime_state(goal)
        except Exception as exc:
            return {
                str(task.task_id): {
                    "status": "unavailable",
                    "reason": _bounded_audit_error(exc),
                }
                for task in tasks
            }
        if int(state.progress.graph_version) != int(expected_graph_version):
            reason = (
                "runtime_state_graph_version_changed:"
                f"expected={expected_graph_version},"
                f"observed={state.progress.graph_version}"
            )
            return {
                str(task.task_id): {
                    "status": "unavailable",
                    "reason": reason,
                }
                for task in tasks
            }

        policy = ComputeRoutingPolicy()
        # Measured verification history: a task that keeps failing needs a
        # stronger model, and that is evidence rather than a declared opinion.
        from .compute_calibration import observe_task_outcomes

        routing_task_outcomes = observe_task_outcomes(self._scheduler.attempts)
        decisions: dict[str, dict[str, Any]] = {}
        allowed_fields = frozenset(CandidateTaskMetadata.model_fields)
        for task in sorted(tasks, key=lambda item: str(item.task_id)):
            task_id = str(task.task_id)
            raw_metadata = getattr(task, "metadata", {})
            raw = raw_metadata.get("compute_routing") if isinstance(raw_metadata, Mapping) else None
            if not isinstance(raw, Mapping):
                # This branch is defensive; callers are filtered before
                # reaching this helper, but preserving a bounded status makes
                # the audit robust to mutable Task metadata.
                decisions[task_id] = {
                    "status": "invalid",
                    "reason": "metadata.compute_routing must be a mapping",
                }
                continue
            candidate_payload = {
                key: raw[key] for key in sorted(raw) if key in allowed_fields and key != "task_id"
            }
            candidate_payload["task_id"] = task_id
            try:
                candidate = CandidateTaskMetadata.model_validate(candidate_payload)
                decision = policy.route(
                    state,
                    candidate,
                    task_outcomes=routing_task_outcomes,
                )
            except Exception as exc:
                decisions[task_id] = {
                    "status": "invalid",
                    "reason": _bounded_audit_error(exc),
                }
                continue
            decisions[task_id] = _bounded_compute_routing_summary(decision)
        return decisions

    def _persist_scheduling_epoch(self, epoch: Any) -> None:
        """Append one bounded adaptive SchedulingEpoch audit event.

        The policy output is an immutable proposal; persisting it must not
        claim work or alter VPG/Kernel state.  Scheduler owns the journal and
        deterministic event identity, so this facade only forwards the
        bounded epoch fields.
        """

        record = getattr(self._scheduler, "record_scheduling_epoch", None)
        if not callable(record):
            raise ConfigurationError("scheduler integration cannot persist SchedulingEpoch audits")
        try:
            record(
                graph_id=str(epoch.graph_id),
                graph_version=int(epoch.graph_version),
                epoch_id=int(epoch.epoch_id),
                policy_id=str(epoch.policy_id),
                decision_hash=str(epoch.decision_hash),
                projection_hash=str(getattr(epoch, "projection_hash", "")),
                candidate_task_ids=tuple(getattr(epoch, "candidate_task_ids", ())),
                selected_task_ids=tuple(getattr(epoch, "selected_task_ids", ())),
                deferred_task_ids=tuple(getattr(epoch, "deferred_task_ids", ())),
                parallelism_hint=int(getattr(epoch, "parallelism_hint", 0)),
                unavailable=tuple(getattr(epoch, "unavailable", ())),
            )
        except Exception as exc:
            raise ConfigurationError(
                "failed to persist adaptive SchedulingEpoch audit",
                cause=exc,
            ) from exc

    # ── run ──────────────────────────────────────────────────────────────────
    def run(
        self,
        goal: Goal,
        *,
        max_dispatches: int = 8,
        max_steps: int = 20,
        adaptive: bool = False,
        conflict_graph: ConflictGraph | None = None,
        max_parallelism: int = 1,
        resource_aware: bool = False,
        budget_aware: bool = False,
        budget_estimates: (
            Mapping[str, TaskComputeEstimate] | Iterable[TaskComputeEstimate] | None
        ) = None,
        budget_limits: ComputeBudgetLimits | None = None,
        budget_usage: ComputeBudgetUsage | None = None,
        persist_adaptive_epochs: bool = True,
        automatic_rebase: bool = True,
        max_automatic_rebase_dispatches: int = 1,
    ) -> RunResult:
        """Execute a Goal, optionally using the bounded adaptive policy.

        ``adaptive=False`` preserves the historical Scheduler path exactly.
        With ``adaptive=True`` the runtime observes a fresh
        :class:`GlobalRuntimeState` at each epoch and passes the policy's
        selected task ids to the authoritative Scheduler as an advisory
        ``allowed_task_ids`` filter.  Claims, leases, eligibility, and
        resource admission remain Scheduler/Kernel decisions.

        ``automatic_rebase`` enables a bounded synchronous fresh-attempt repair
        when commit-time read-set validation quarantines an attempt as
        ``STALE_COGNITION``.  The replacement is admitted through the normal
        Scheduler/Claim/Lease path and requires an explicit
        ``Task.context_manifest``; hidden or unversioned reads fail closed.

        ``budget_aware=True`` is an explicit compute-budget v1 path.  It
        requires ``adaptive=True``, explicit ``budget_estimates`` and
        ``budget_limits``, and ``automatic_rebase=False``.  Budget-only uses
        :class:`VerifiedProgressBudgetPolicy`; combining it with
        ``resource_aware=True`` uses the unified budget/conflict/logical-
        resource policy.  Neither budgeted mode allows an unfiltered Scheduler
        fallback.  Declared estimates are charged when Scheduler dispatches
        work, including failed/stale attempts.
        """
        if self._read_only:
            raise ExecutionError("read-only AgentOS cannot execute goals")
        if not isinstance(adaptive, bool):
            raise ConfigurationError("adaptive must be a boolean")
        if not isinstance(resource_aware, bool):
            raise ConfigurationError("resource_aware must be a boolean")
        if resource_aware and not adaptive:
            raise ConfigurationError("resource_aware requires adaptive=True")
        if not isinstance(persist_adaptive_epochs, bool):
            raise ConfigurationError("persist_adaptive_epochs must be a boolean")
        if isinstance(max_dispatches, bool) or not isinstance(max_dispatches, int):
            raise ConfigurationError("max_dispatches must be an integer")
        if isinstance(max_steps, bool) or not isinstance(max_steps, int):
            raise ConfigurationError("max_steps must be an integer")
        if max_dispatches < 0 or max_steps < 0:
            raise ConfigurationError("max_dispatches and max_steps must be >= 0")
        if isinstance(max_parallelism, bool) or not isinstance(max_parallelism, int):
            raise ConfigurationError("max_parallelism must be an integer")
        if max_parallelism < 1:
            raise ConfigurationError("max_parallelism must be >= 1")
        if not isinstance(automatic_rebase, bool):
            raise ConfigurationError("automatic_rebase must be a boolean")
        if (
            isinstance(max_automatic_rebase_dispatches, bool)
            or not isinstance(max_automatic_rebase_dispatches, int)
            or max_automatic_rebase_dispatches < 0
        ):
            raise ConfigurationError("max_automatic_rebase_dispatches must be an integer >= 0")
        if not adaptive and conflict_graph is not None:
            raise ConfigurationError("conflict_graph requires adaptive=True")
        (
            budget_estimates_snapshot,
            budget_limits_value,
            budget_usage_current,
        ) = _prepare_compute_budget_run_inputs(
            budget_aware=budget_aware,
            adaptive=adaptive,
            budget_estimates=budget_estimates,
            budget_limits=budget_limits,
            budget_usage=budget_usage,
            resource_aware=resource_aware,
            conflict_graph=conflict_graph,
            automatic_rebase=automatic_rebase,
        )
        (
            budget_estimates_snapshot,
            budget_calibration_audits,
            budget_measurement,
        ) = _calibrate_and_build_measurement(
            self._usage_ledger,
            budget_aware=budget_aware,
            estimates_snapshot=budget_estimates_snapshot,
            goal_id=goal.goal_id,
            attempts=self._scheduler.attempts,
        )
        self._goals.setdefault(goal.goal_id, goal)
        gid = self._gid_for(goal.goal_id, compile_if_missing=True)
        if gid is None:
            raise ConfigurationError(f"goal {goal.goal_id!r} not registered")
        adaptive_graph = (
            self._coerce_adaptive_conflict_graph(goal, conflict_graph)
            if adaptive and (not budget_aware or resource_aware)
            else None
        )
        steps = 0
        tasks_by_id = {task.task_id: task for task in goal.tasks}
        declared_read_keys = self._declared_read_keys_by_task(goal)
        failures: list[str] = []
        regular_dispatches = 0
        automatic_rebase_dispatches = 0
        pending_rebases: dict[str, tuple[AutomaticRebaseDecision, ContextManifest]] = {}
        automatic_rebase_records: list[dict[str, Any]] = []
        adaptive_epochs: list[dict[str, Any]] = []
        while (regular_dispatches < max_dispatches and steps < max_steps) or (
            pending_rebases and automatic_rebase_dispatches < max_automatic_rebase_dispatches
        ):
            res = None
            scheduler_skipped_records: tuple[Any, ...] = ()
            repair_only = bool(
                pending_rebases and automatic_rebase_dispatches < max_automatic_rebase_dispatches
            )
            try:
                # Never acquire more claims than this invocation can execute.
                # Otherwise the unexecuted tail remains ACTIVE and is skipped by
                # every later scheduling pass.
                remaining = (
                    max_automatic_rebase_dispatches - automatic_rebase_dispatches
                    if repair_only
                    else max_dispatches - regular_dispatches
                )
                allowed_task_ids: tuple[str, ...] | None = None
                adaptive_epoch_meta: dict[str, Any] = {}
                if repair_only:
                    repair_task_ids = tuple(sorted(pending_rebases)[:1])
                    allowed_task_ids = repair_task_ids
                    repair_graph_version = int(self._vpg.get_graph(gid).current_version)
                    repair_hash = _bounded_hash(
                        {
                            "kind": "automatic_rebase",
                            "graph_id": gid,
                            "graph_version": repair_graph_version,
                            "task_ids": repair_task_ids,
                            "decisions": tuple(
                                pending_rebases[item][0].decision_hash for item in repair_task_ids
                            ),
                        }
                    )
                    adaptive_epoch_meta = {
                        "epoch_id": steps,
                        "graph_id": gid,
                        "graph_version": repair_graph_version,
                        "projection_hash": repair_hash,
                        "selected_task_ids": repair_task_ids,
                        "deferred_task_ids": (),
                        "decision_hash": repair_hash,
                        "automatic_rebase": True,
                        "automatic_rebase_task_ids": repair_task_ids,
                        "fallback_attempted": False,
                        "actual_dispatched_task_ids": (),
                        "fallback_dispatched_task_ids": (),
                        "scheduler_skipped": (),
                        "scheduler_skipped_count": 0,
                        "scheduler_skipped_truncated": False,
                    }
                    adaptive_epochs.append(adaptive_epoch_meta)
                elif adaptive:
                    epoch = (
                        self._plan_unified_adaptive_epoch(
                            goal,
                            epoch_id=steps,
                            max_parallelism=max_parallelism,
                            conflict_graph=adaptive_graph,
                            estimates=budget_estimates_snapshot,
                            limits=budget_limits_value,
                            usage=budget_usage_current,
                        )
                        if budget_aware and resource_aware
                        else self._plan_budget_aware_epoch(
                            goal,
                            epoch_id=steps,
                            max_parallelism=max_parallelism,
                            estimates=budget_estimates_snapshot,
                            limits=budget_limits_value,
                            usage=budget_usage_current,
                        )
                        if budget_aware
                        else self._plan_resource_aware_epoch(
                            goal,
                            epoch_id=steps,
                            max_parallelism=max_parallelism,
                            conflict_graph=adaptive_graph,
                        )
                        if resource_aware
                        else self._plan_adaptive_epoch(
                            goal,
                            epoch_id=steps,
                            max_parallelism=max_parallelism,
                            conflict_graph=adaptive_graph,
                        )
                    )
                    if persist_adaptive_epochs:
                        self._persist_scheduling_epoch(epoch)
                    allowed_task_ids = tuple(epoch.selected_task_ids)
                    adaptive_epoch_meta = {
                        "epoch_id": epoch.epoch_id,
                        "graph_id": str(epoch.graph_id),
                        "graph_version": int(epoch.graph_version),
                        "projection_hash": str(epoch.projection_hash),
                        "selected_task_ids": allowed_task_ids,
                        "deferred_task_ids": tuple(epoch.deferred_task_ids),
                        "decision_hash": epoch.decision_hash,
                        "fallback_attempted": False,
                        "actual_dispatched_task_ids": (),
                        "fallback_dispatched_task_ids": (),
                        "scheduler_skipped": (),
                        "scheduler_skipped_count": 0,
                        "scheduler_skipped_truncated": False,
                    }
                    adaptive_epoch_meta.update(
                        self._adaptive_epoch_metadata(
                            goal,
                            epoch,
                            resource_aware=resource_aware,
                            unified_control=budget_aware and resource_aware,
                        )
                    )
                    if budget_aware:
                        adaptive_epoch_meta["budget_audit"] = _bounded_compute_budget_epoch_audit(
                            epoch,
                            dispatched_declared_usage_after=budget_usage_current,
                        )
                    adaptive_epochs.append(adaptive_epoch_meta)
                    if not allowed_task_ids:
                        break
                if repair_only:
                    res = self._scheduler.run_pass(
                        gid,
                        max_claims=1,
                        allowed_task_ids=allowed_task_ids,
                        dispatch_order=allowed_task_ids,
                        task_read_keys_by_task=declared_read_keys,
                        expected_graph_version=int(adaptive_epoch_meta["graph_version"]),
                    )
                    scheduler_skipped_records = tuple(getattr(res, "skipped", ()))
                    if getattr(res, "policy_stale", False):
                        adaptive_epoch_meta["policy_stale"] = True
                        adaptive_epoch_meta["policy_stale_reason"] = str(
                            getattr(res, "policy_stale_reason", "")
                        )[:240]
                        automatic_rebase_records.append(
                            {
                                "status": "blocked",
                                "reason": "automatic_rebase:graph_version_race",
                                "task_ids": tuple(allowed_task_ids or ()),
                            }
                        )
                        break
                elif adaptive:
                    res = self._scheduler.run_pass(
                        gid,
                        max_claims=remaining,
                        allowed_task_ids=allowed_task_ids,
                        dispatch_order=allowed_task_ids,
                        task_read_keys_by_task=declared_read_keys,
                        expected_graph_version=int(epoch.graph_version),
                    )
                    scheduler_skipped_records = tuple(getattr(res, "skipped", ()))
                    # The policy plan is tied to the graph snapshot observed
                    # above.  If that snapshot was superseded before or during
                    # admission, do not execute the old selection and do not
                    # use the unfiltered fallback.  The next bounded loop
                    # iteration observes and replans against the new version.
                    if getattr(res, "policy_stale", False):
                        adaptive_epoch_meta["policy_stale"] = True
                        adaptive_epoch_meta["policy_stale_reason"] = str(
                            getattr(res, "policy_stale_reason", "")
                        )[:240]
                        adaptive_epoch_meta["observed_graph_version"] = getattr(
                            res, "observed_graph_version", None
                        )
                        adaptive_epoch_meta["policy_cleanup_required"] = bool(
                            getattr(res, "policy_cleanup_required", False)
                        )
                        adaptive_epoch_meta["policy_cleanup_errors"] = tuple(
                            getattr(res, "policy_cleanup_errors", ())
                        )[:8]
                        adaptive_epoch_meta.update(
                            _bounded_scheduler_skip_audit(scheduler_skipped_records)
                        )
                        if getattr(res, "policy_cleanup_required", False):
                            raise SchedulingError(
                                "adaptive policy became stale and exact-claim "
                                "cleanup was incomplete"
                            )
                        steps += 1
                        continue
                    # A policy proposal is advisory.  If its selected task
                    # cannot pass authoritative Scheduler eligibility/resource
                    # admission, make one bounded unfiltered pass so an
                    # eligible later frontier task is not starved.  This never
                    # bypasses Scheduler checks and is recorded for audit.
                    if adaptive and not budget_aware and not res.dispatched and allowed_task_ids:
                        adaptive_epoch_meta["fallback_attempted"] = True
                        adaptive_epoch_meta["fallback_reason"] = (
                            "policy_selected_tasks_not_dispatchable"
                        )
                        # Keep the bounded fallback serial.  The policy's
                        # selected batch may have been rejected by
                        # authoritative eligibility/resource admission; an
                        # unfiltered fallback must not accidentally re-enable
                        # parallel dispatch of conflicting tasks.
                        adaptive_epoch_meta["fallback_parallelism"] = 1
                        fallback_res = self._scheduler.run_pass(
                            gid,
                            max_claims=1,
                            allowed_task_ids=None,
                            # Relax the filter, keep the ranking: liveness must
                            # not cost the policy's ordering and hand dispatch
                            # back to static graph order.
                            dispatch_order=allowed_task_ids,
                            task_read_keys_by_task=declared_read_keys,
                            expected_graph_version=int(epoch.graph_version),
                        )
                        scheduler_skipped_records += tuple(getattr(fallback_res, "skipped", ()))
                        res = fallback_res
                else:
                    res = self._scheduler.run_pass(gid, max_claims=remaining)
            except Exception as e:  # surface scheduler errors
                raise SchedulingError("scheduler pass failed", cause=e) from e
            if adaptive or repair_only:
                # Preserve the authoritative Scheduler admission transcript.
                # If the advisory pass required fallback, append both passes
                # so policy-deferred and resource/eligibility rejections are
                # distinguishable from the eventual fallback dispatch.
                skip_audit = _bounded_scheduler_skip_audit(scheduler_skipped_records)
                adaptive_epoch_meta.update(skip_audit)
            if not res.dispatched:
                break
            if budget_aware:
                budget_dispatched_task_ids = tuple(str(item["task_id"]) for item in res.dispatched)
                try:
                    budget_usage_current = _accumulate_dispatched_compute_budget_usage(
                        epoch,
                        budget_usage_current,
                        budget_dispatched_task_ids,
                    )
                except SchedulingError:
                    self._release_unexecuted_dispatches(
                        gid,
                        res.dispatched,
                        reason="compute_budget_accounting_fence",
                    )
                    raise
                adaptive_epoch_meta["budget_audit"] = _bounded_compute_budget_epoch_audit(
                    epoch,
                    dispatched_declared_usage_after=budget_usage_current,
                    actual_dispatched_task_ids=budget_dispatched_task_ids,
                )
            async_agent_ids = sorted(
                {
                    d["agent_id"]
                    for d in res.dispatched
                    if (agent := self._agents.get(d["agent_id"])) is not None
                    and agent.executor_is_async
                }
            )
            if async_agent_ids:
                self._release_unexecuted_dispatches(
                    gid,
                    res.dispatched,
                    reason="async_executor_requires_run_async",
                )
                names = ", ".join(async_agent_ids)
                raise ConfigurationError(
                    f"Scheduled Agent executor is asynchronous ({names}); "
                    "use `await AgentOS.run_async(...)`"
                )
            if adaptive:
                actual_dispatched_task_ids = tuple(str(item["task_id"]) for item in res.dispatched)
                adaptive_epoch_meta["actual_dispatched_task_ids"] = actual_dispatched_task_ids
                adaptive_epoch_meta["dispatch_order_applied"] = tuple(
                    getattr(res, "dispatch_order_applied", ())
                )
                adaptive_epoch_meta["locality_matched_task_ids"] = tuple(
                    getattr(res, "locality_matched", ())
                )
                if adaptive_epoch_meta["fallback_attempted"]:
                    adaptive_epoch_meta["fallback_dispatched_task_ids"] = actual_dispatched_task_ids
            for index, d in enumerate(res.dispatched):
                task_id = d["task_id"]
                agent_id = d["agent_id"]
                claim = self._scheduler.active_claim_for_task(task_id, gid)
                attempt_number = getattr(claim, "attempt_number", 0)
                rebase_entry = (
                    pending_rebases.pop(task_id, None)
                    if repair_only
                    else pending_rebases.get(task_id)
                )
                try:
                    self._execute_and_verify(
                        gid,
                        task_id,
                        agent_id,
                        goal,
                        claim_id=d.get("claim_id", ""),
                        attempt_number=attempt_number,
                        provider_routing_enabled=adaptive,
                        context_manifest_override=(
                            None if rebase_entry is None else rebase_entry[1]
                        ),
                        automatic_rebase_decision=(
                            None if rebase_entry is None else rebase_entry[0]
                        ),
                        budget_measurement=budget_measurement,
                    )
                except ConfigurationError:
                    self._release_unexecuted_dispatches(
                        gid,
                        res.dispatched[index + 1 :],
                        reason="executor_configuration_error",
                    )
                    raise
                # The synchronous execution path has no worker callback in
                # which to schedule a replacement immediately.  Inspect the
                # exact Attempt after execution and plan a fresh Context VM
                # manifest for the next bounded pass.
                if automatic_rebase:
                    attempt = self._scheduler.attempt_for_claim(d.get("claim_id", ""))
                    attempt_state = str(getattr(getattr(attempt, "state", None), "value", "") or "")
                    if attempt_state == "stale_cognition":
                        task_obj = tasks_by_id.get(task_id)
                        source_manifest = (
                            rebase_entry[1]
                            if rebase_entry is not None
                            else (
                                getattr(task_obj, "context_manifest", None)
                                if task_obj is not None
                                else None
                            )
                        )
                        source_attempt_id = str(getattr(attempt, "attempt_id", ""))
                        try:
                            current_graph_version = int(self._vpg.get_graph(gid).current_version)
                            decision, replacement_manifest = plan_automatic_rebase(
                                task_id=task_id,
                                graph_id=gid,
                                graph_version=current_graph_version,
                                agent_snapshot=getattr(attempt, "agent_snapshot", None),
                                context_manifest=source_manifest,
                                facts=self._facts,
                                claim_id=str(d.get("claim_id", "")),
                                attempt_id=source_attempt_id,
                                reason=(
                                    getattr(attempt, "error", "") or "read-set freshness fence"
                                ),
                            )
                        except Exception as exc:
                            decision = None
                            replacement_manifest = None
                            automatic_rebase_records.append(
                                {
                                    "task_id": task_id,
                                    "source_claim_id": str(d.get("claim_id", "")),
                                    "source_attempt_id": source_attempt_id,
                                    "status": "blocked",
                                    "reason": (
                                        "automatic rebase planner failed: "
                                        + _bounded_audit_error(exc)
                                    ),
                                }
                            )
                        if decision is not None:
                            automatic_rebase_records.append(decision.as_dict())
                        if (
                            decision is not None
                            and replacement_manifest is not None
                            and decision.redispatchable
                            and automatic_rebase_dispatches
                            + int(repair_only)
                            + len(pending_rebases)
                            < max_automatic_rebase_dispatches
                        ):
                            pending_rebases[task_id] = (
                                decision,
                                replacement_manifest,
                            )
                        elif decision is not None and decision.redispatchable:
                            failures.append(f"{task_id}: automatic_rebase_budget_exhausted")
                        else:
                            failures.append(f"{task_id}: automatic_rebase_blocked")
                if repair_only:
                    automatic_rebase_dispatches += 1
                else:
                    regular_dispatches += 1
                if regular_dispatches >= max_dispatches and not pending_rebases:
                    break
            steps += 1
            if steps >= max_steps and not pending_rebases:
                break
        result = self.result(gid)
        result.failures.extend(failures)
        if (
            automatic_rebase_dispatches
            or automatic_rebase_records
            or pending_rebases
            or not automatic_rebase
            or max_automatic_rebase_dispatches != 1
        ):
            result.meta.update(
                {
                    "regular_dispatches": regular_dispatches,
                    "automatic_rebase_dispatches": automatic_rebase_dispatches,
                    "automatic_rebase_enabled": automatic_rebase,
                    "automatic_rebase_max_dispatches": max_automatic_rebase_dispatches,
                    "automatic_rebase_records": tuple(automatic_rebase_records),
                    "automatic_rebase_pending": tuple(sorted(pending_rebases)),
                }
            )
        if adaptive:
            result.meta.update(
                {
                    "adaptive": True,
                    "adaptive_policy": (
                        "unified-adaptive-control"
                        if budget_aware and resource_aware
                        else "verified-progress-budget"
                        if budget_aware
                        else "resource-aware-conflict"
                        if resource_aware
                        else "conflict-aware"
                        if adaptive_graph is not None
                        else "frontier"
                    ),
                    "resource_aware": bool(resource_aware),
                    "unified_control": bool(budget_aware and resource_aware),
                    "adaptive_epochs": adaptive_epochs,
                }
            )
            if budget_aware:
                result.meta.update(
                    {
                        "budget_aware": True,
                        "budget_limits": _compute_budget_limits_audit(budget_limits_value),
                        "budget_usage": _compute_budget_usage_audit(budget_usage_current),
                        "measured_usage": self._measured_usage_audit(goal.goal_id),
                        "budget_calibration": tuple(
                            audit.as_dict() for audit in budget_calibration_audits
                        ),
                    }
                )
        return result

    async def run_async(
        self,
        goal: Goal,
        *,
        max_dispatches: int = 8,
        max_steps: int = 20,
        max_concurrency: int = 4,
        adaptive: bool = False,
        conflict_graph: ConflictGraph | None = None,
        max_parallelism: int = 1,
        resource_aware: bool = False,
        budget_aware: bool = False,
        budget_estimates: (
            Mapping[str, TaskComputeEstimate] | Iterable[TaskComputeEstimate] | None
        ) = None,
        budget_limits: ComputeBudgetLimits | None = None,
        budget_usage: ComputeBudgetUsage | None = None,
        persist_adaptive_epochs: bool = True,
        automatic_rebase: bool = True,
        max_automatic_rebase_dispatches: int = 1,
        preempt_superseded: bool = False,
        dispatch_lookahead: int = 1,
        streaming_dispatch: bool = False,
    ) -> RunResult:
        """Schedule and execute ready tasks concurrently.

        Scheduler claims and Kernel leases remain the ownership authority.
        Agent executors overlap under the global and per-Agent concurrency
        bounds.  Independent synchronous verifiers run after operational
        success; Facts/Evidence/VPG commits are then serialized to avoid
        graph-version races while preserving executor concurrency.

        ``max_dispatches=0`` is a strict no-work budget: after registering or
        compiling the Goal, the method returns its current projection without
        planning/persisting an adaptive epoch, invoking the Scheduler,
        creating ownership, or executing user code.

        ``budget_aware=True`` requires explicit declared estimates/limits,
        ``adaptive=True``, and ``automatic_rebase=False``.  Combining it with
        ``resource_aware=True`` enables unified budget/conflict/logical-
        resource admission.  Budget-only and unified execution never use the
        unfiltered adaptive fallback.

        ``preempt_superseded=True`` opts into semantic preemption: while a batch
        is in flight, a sibling commit that advances the graph and supersedes a
        still-running peer's declared input delivers a cooperative interrupt to
        that exact peer attempt through :meth:`deliver_interrupt`.  It defaults
        to off so scheduling and decision hashes are byte-identical to prior
        runs unless the caller opts in.
        """
        if self._read_only:
            raise ExecutionError("read-only AgentOS cannot execute goals")
        if not isinstance(adaptive, bool):
            raise ConfigurationError("adaptive must be a boolean")
        if not isinstance(resource_aware, bool):
            raise ConfigurationError("resource_aware must be a boolean")
        if resource_aware and not adaptive:
            raise ConfigurationError("resource_aware requires adaptive=True")
        if isinstance(max_parallelism, bool) or not isinstance(max_parallelism, int):
            raise ConfigurationError("max_parallelism must be an integer")
        if max_parallelism < 1:
            raise ConfigurationError("max_parallelism must be >= 1")
        if not isinstance(persist_adaptive_epochs, bool):
            raise ConfigurationError("persist_adaptive_epochs must be a boolean")
        if not isinstance(automatic_rebase, bool):
            raise ConfigurationError("automatic_rebase must be a boolean")
        if (
            isinstance(max_automatic_rebase_dispatches, bool)
            or not isinstance(max_automatic_rebase_dispatches, int)
            or max_automatic_rebase_dispatches < 0
        ):
            raise ConfigurationError("max_automatic_rebase_dispatches must be an integer >= 0")
        if not adaptive and conflict_graph is not None:
            raise ConfigurationError("conflict_graph requires adaptive=True")
        if not isinstance(preempt_superseded, bool):
            raise ConfigurationError("preempt_superseded must be a boolean")
        if isinstance(dispatch_lookahead, bool) or not isinstance(dispatch_lookahead, int):
            raise ConfigurationError("dispatch_lookahead must be an integer")
        if dispatch_lookahead < 1:
            raise ConfigurationError("dispatch_lookahead must be >= 1")
        if not isinstance(streaming_dispatch, bool):
            raise ConfigurationError("streaming_dispatch must be a boolean")
        if streaming_dispatch and not adaptive:
            raise ConfigurationError("streaming_dispatch requires adaptive=True")
        if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
            raise ConfigurationError("max_concurrency must be an integer")
        if max_concurrency < 1:
            raise ConfigurationError("max_concurrency must be >= 1")
        if isinstance(max_dispatches, bool) or not isinstance(max_dispatches, int):
            raise ConfigurationError("max_dispatches must be an integer")
        if isinstance(max_steps, bool) or not isinstance(max_steps, int):
            raise ConfigurationError("max_steps must be an integer")
        if max_dispatches < 0 or max_steps < 0:
            raise ConfigurationError("max_dispatches and max_steps must be >= 0")
        (
            budget_estimates_snapshot,
            budget_limits_value,
            budget_usage_current,
        ) = _prepare_compute_budget_run_inputs(
            budget_aware=budget_aware,
            adaptive=adaptive,
            budget_estimates=budget_estimates,
            budget_limits=budget_limits,
            budget_usage=budget_usage,
            resource_aware=resource_aware,
            conflict_graph=conflict_graph,
            automatic_rebase=automatic_rebase,
        )
        (
            budget_estimates_snapshot,
            budget_calibration_audits,
            budget_measurement,
        ) = _calibrate_and_build_measurement(
            self._usage_ledger,
            budget_aware=budget_aware,
            estimates_snapshot=budget_estimates_snapshot,
            goal_id=goal.goal_id,
            attempts=self._scheduler.attempts,
        )

        self._goals.setdefault(goal.goal_id, goal)
        gid = self._gid_for(goal.goal_id, compile_if_missing=True)
        if gid is None:
            raise ConfigurationError(f"goal {goal.goal_id!r} not registered")
        adaptive_graph = (
            self._coerce_adaptive_conflict_graph(goal, conflict_graph)
            if adaptive and (not budget_aware or resource_aware)
            else None
        )

        tasks_by_id = {task.task_id: task for task in goal.tasks}
        declared_read_keys = self._declared_read_keys_by_task(goal)
        semantic_commit_lock = asyncio.Lock()
        usage_accounting_lock = asyncio.Lock()
        usage_accounted_claims: set[str] = set()
        failures: list[str] = []
        fatal_errors: list[VerificationError] = []
        dispatched = 0
        steps = 0
        regular_dispatches = 0
        automatic_rebase_dispatches = 0
        pending_rebases: dict[str, tuple[AutomaticRebaseDecision, ContextManifest]] = {}
        automatic_rebase_records: list[dict[str, Any]] = []
        adaptive_epochs: list[dict[str, Any]] = []
        streaming_refills: list[dict[str, Any]] = []

        while (regular_dispatches < max_dispatches and steps < max_steps) or (
            pending_rebases and automatic_rebase_dispatches < max_automatic_rebase_dispatches
        ):
            # A planned repair has priority over new work.  This keeps the
            # bounded retry tied to the stale task instead of allowing an
            # unrelated READY task to consume the repair budget.
            repair_only = bool(
                pending_rebases and automatic_rebase_dispatches < max_automatic_rebase_dispatches
            )
            if repair_only:
                remaining = max_automatic_rebase_dispatches - automatic_rebase_dispatches
            else:
                remaining = max_dispatches - regular_dispatches
            # Admitting exactly ``max_concurrency`` tasks makes every batch a
            # barrier: the pool cannot start anything new until the slowest task
            # in the batch finishes, even though ``AsyncWorkerPool`` refills a
            # freed slot from *within* a batch on its own (its ``_UnitLimiter``
            # queues surplus jobs and admits them the moment capacity returns).
            # Measured on a 42-task workload, the barrier left 26-39% of the
            # configured concurrency unusable -- and more for the *better*
            # scheduler, because prioritising the critical path leaves a queue of
            # runnable-but-deferred cheap work a barrier cannot serve.
            #
            # A lookahead above 1 therefore hands the pool surplus work and lets
            # it stay work-conserving -- but it is **measured to be a bad trade**
            # and defaults to 1 for that reason.  On the 42-task critical-path
            # workload, raising it cut reclaimable capacity from 40% to 23% while
            # `chain_priority` degraded 0.34 -> 0.58 -> 0.68 (lookahead 1/2/4),
            # i.e. toward the static plan's 0.74, and wall-clock did not improve
            # (4.34s -> 4.30s, inside noise).  The two effects cancel: ranking is
            # applied once over a larger set, so committing to an order earlier
            # loses the per-batch re-ranking that made the adaptive arm win.
            #
            # Work-conserving dispatch therefore has to refill from a *re-ranked*
            # frontier rather than by admitting more work up front.  Kept as an
            # instrument so the trade stays reproducible, not as a tuning knob.
            batch_limit = min(remaining, max_concurrency * dispatch_lookahead)
            if batch_limit <= 0:
                break
            scheduler_skipped_records: tuple[Any, ...] = ()
            try:
                allowed_task_ids: tuple[str, ...] | None = None
                adaptive_epoch_meta: dict[str, Any] = {}
                if repair_only:
                    # A repair epoch is explicitly scoped to the task whose
                    # previous Attempt was quarantined.  It uses the normal
                    # Scheduler admission/lease path with a graph-version
                    # fence, but never falls back to unrelated work.
                    repair_task_ids = tuple(sorted(pending_rebases)[:batch_limit])
                    allowed_task_ids = repair_task_ids
                    repair_graph_version = int(self._vpg.get_graph(gid).current_version)
                    repair_hash = _bounded_hash(
                        {
                            "kind": "automatic_rebase",
                            "graph_id": gid,
                            "graph_version": repair_graph_version,
                            "task_ids": repair_task_ids,
                            "decisions": tuple(
                                pending_rebases[item][0].decision_hash for item in repair_task_ids
                            ),
                        }
                    )
                    adaptive_epoch_meta = {
                        "epoch_id": steps,
                        "graph_id": gid,
                        "graph_version": repair_graph_version,
                        "projection_hash": repair_hash,
                        "selected_task_ids": repair_task_ids,
                        "deferred_task_ids": (),
                        "decision_hash": repair_hash,
                        "automatic_rebase": True,
                        "automatic_rebase_task_ids": repair_task_ids,
                        "fallback_attempted": False,
                        "actual_dispatched_task_ids": (),
                        "fallback_dispatched_task_ids": (),
                        "scheduler_skipped": (),
                        "scheduler_skipped_count": 0,
                        "scheduler_skipped_truncated": False,
                    }
                    adaptive_epochs.append(adaptive_epoch_meta)
                elif adaptive:
                    epoch = (
                        self._plan_unified_adaptive_epoch(
                            goal,
                            epoch_id=steps,
                            max_parallelism=min(batch_limit, max_parallelism),
                            conflict_graph=adaptive_graph,
                            estimates=budget_estimates_snapshot,
                            limits=budget_limits_value,
                            usage=budget_usage_current,
                        )
                        if budget_aware and resource_aware
                        else self._plan_budget_aware_epoch(
                            goal,
                            epoch_id=steps,
                            max_parallelism=min(batch_limit, max_parallelism),
                            estimates=budget_estimates_snapshot,
                            limits=budget_limits_value,
                            usage=budget_usage_current,
                        )
                        if budget_aware
                        else self._plan_resource_aware_epoch(
                            goal,
                            epoch_id=steps,
                            max_parallelism=min(batch_limit, max_parallelism),
                            conflict_graph=adaptive_graph,
                        )
                        if resource_aware
                        else self._plan_adaptive_epoch(
                            goal,
                            epoch_id=steps,
                            max_parallelism=batch_limit,
                            conflict_graph=adaptive_graph,
                        )
                    )
                    if persist_adaptive_epochs:
                        self._persist_scheduling_epoch(epoch)
                    allowed_task_ids = tuple(epoch.selected_task_ids)
                    adaptive_epoch_meta = {
                        "epoch_id": epoch.epoch_id,
                        "graph_id": str(epoch.graph_id),
                        "graph_version": int(epoch.graph_version),
                        "projection_hash": str(epoch.projection_hash),
                        "selected_task_ids": allowed_task_ids,
                        "deferred_task_ids": tuple(epoch.deferred_task_ids),
                        "decision_hash": epoch.decision_hash,
                        "fallback_attempted": False,
                        "actual_dispatched_task_ids": (),
                        "fallback_dispatched_task_ids": (),
                        "scheduler_skipped": (),
                        "scheduler_skipped_count": 0,
                        "scheduler_skipped_truncated": False,
                    }
                    adaptive_epoch_meta.update(
                        self._adaptive_epoch_metadata(
                            goal,
                            epoch,
                            resource_aware=resource_aware,
                            unified_control=budget_aware and resource_aware,
                        )
                    )
                    if budget_aware:
                        adaptive_epoch_meta["budget_audit"] = _bounded_compute_budget_epoch_audit(
                            epoch,
                            dispatched_declared_usage_after=budget_usage_current,
                        )
                    adaptive_epochs.append(adaptive_epoch_meta)
                    if not allowed_task_ids:
                        break
                if repair_only:
                    schedule_result = self._scheduler.run_pass(
                        gid,
                        max_claims=batch_limit,
                        allowed_task_ids=allowed_task_ids,
                        dispatch_order=allowed_task_ids,
                        task_read_keys_by_task=declared_read_keys,
                        expected_graph_version=int(adaptive_epoch_meta["graph_version"]),
                    )
                    scheduler_skipped_records = tuple(getattr(schedule_result, "skipped", ()))
                    if getattr(schedule_result, "policy_stale", False):
                        adaptive_epoch_meta["policy_stale"] = True
                        adaptive_epoch_meta["policy_stale_reason"] = str(
                            getattr(schedule_result, "policy_stale_reason", "")
                        )[:240]
                        adaptive_epoch_meta["observed_graph_version"] = getattr(
                            schedule_result, "observed_graph_version", None
                        )
                        adaptive_epoch_meta.update(
                            _bounded_scheduler_skip_audit(scheduler_skipped_records)
                        )
                        # A graph race means the repair plan no longer has an
                        # authoritative admission fence.  Stop this bounded
                        # invocation fail-closed; the caller may observe and
                        # submit a new epoch.
                        failures.append("automatic_rebase:graph_version_race")
                        break
                elif adaptive:
                    schedule_result = self._scheduler.run_pass(
                        gid,
                        max_claims=batch_limit,
                        allowed_task_ids=allowed_task_ids,
                        dispatch_order=allowed_task_ids,
                        task_read_keys_by_task=declared_read_keys,
                        expected_graph_version=int(epoch.graph_version),
                    )
                    scheduler_skipped_records = tuple(getattr(schedule_result, "skipped", ()))
                    # A stale policy snapshot must never fall through to the
                    # unfiltered fallback: doing so would execute a plan that
                    # was derived from obsolete semantic state.  Replan on the
                    # next bounded epoch instead.
                    if getattr(schedule_result, "policy_stale", False):
                        adaptive_epoch_meta["policy_stale"] = True
                        adaptive_epoch_meta["policy_stale_reason"] = str(
                            getattr(schedule_result, "policy_stale_reason", "")
                        )[:240]
                        adaptive_epoch_meta["observed_graph_version"] = getattr(
                            schedule_result, "observed_graph_version", None
                        )
                        adaptive_epoch_meta["policy_cleanup_required"] = bool(
                            getattr(schedule_result, "policy_cleanup_required", False)
                        )
                        adaptive_epoch_meta["policy_cleanup_errors"] = tuple(
                            getattr(schedule_result, "policy_cleanup_errors", ())
                        )[:8]
                        adaptive_epoch_meta.update(
                            _bounded_scheduler_skip_audit(scheduler_skipped_records)
                        )
                        if getattr(schedule_result, "policy_cleanup_required", False):
                            raise SchedulingError(
                                "adaptive policy became stale and exact-claim "
                                "cleanup was incomplete"
                            )
                        steps += 1
                        continue
                    if (
                        adaptive
                        and not budget_aware
                        and not schedule_result.dispatched
                        and allowed_task_ids
                    ):
                        adaptive_epoch_meta["fallback_attempted"] = True
                        adaptive_epoch_meta["fallback_reason"] = (
                            "policy_selected_tasks_not_dispatchable"
                        )
                        adaptive_epoch_meta["fallback_parallelism"] = 1
                        fallback_result = self._scheduler.run_pass(
                            gid,
                            # Keep the bounded fallback serial.  The policy's
                            # conflict/access proof applies only to its
                            # selected batch; an unfiltered fallback must not
                            # accidentally dispatch conflicting tasks together.
                            max_claims=1,
                            allowed_task_ids=None,
                            dispatch_order=allowed_task_ids,
                            task_read_keys_by_task=declared_read_keys,
                            expected_graph_version=int(epoch.graph_version),
                        )
                        scheduler_skipped_records += tuple(getattr(fallback_result, "skipped", ()))
                        schedule_result = fallback_result
                else:
                    schedule_result = self._scheduler.run_pass(
                        gid,
                        max_claims=batch_limit,
                    )
            except Exception as exc:
                raise SchedulingError("scheduler pass failed", cause=exc) from exc
            if adaptive:
                # Keep the exact Scheduler skip reasons, including partial
                # admission where one selected task is rejected while another
                # selected task is admitted.  Fallback outcomes are appended
                # to the first pass rather than replacing it.
                skip_audit = _bounded_scheduler_skip_audit(scheduler_skipped_records)
                adaptive_epoch_meta.update(skip_audit)
            if not schedule_result.dispatched:
                break

            jobs: list[WorkerJob] = []
            attempt_by_claim: dict[str, int] = {}
            for dispatch in schedule_result.dispatched:
                task_id = dispatch["task_id"]
                agent_id = dispatch["agent_id"]
                claim_id = dispatch.get("claim_id", "")
                claim = self._scheduler.active_claim_for_task(task_id, gid)
                if claim is None or claim.claim_id != claim_id:
                    continue
                task = tasks_by_id.get(task_id)
                jobs.append(
                    WorkerJob(
                        graph_id=gid,
                        graph_version=getattr(claim, "graph_version", None),
                        task_id=task_id,
                        claim_id=claim_id,
                        agent_id=agent_id,
                        task_kind="" if task is None else task.task_kind,
                    )
                )
                attempt_by_claim[claim_id] = int(getattr(claim, "attempt_number", 0))
            if not jobs:
                break
            # Capture the exact freshness/identity basis of every dispatched
            # attempt *before* a sibling can commit and advance the graph.
            # Semantic preemption compares a still-running peer's declared read
            # versions against this dispatch-time baseline.  This reads state
            # only (no mutation, no wall clock, no decision hash), and it is
            # skipped entirely unless the caller opted into ``preempt_superseded``.
            dispatch_freshness: dict[str, dict[str, int]] = {}
            dispatch_identity: dict[str, tuple[str, int | None, int | None]] = {}
            if preempt_superseded:
                for job in jobs:
                    baseline: dict[str, int] = {}
                    for read_key in declared_read_keys.get(job.task_id, ()):
                        latest = self._facts.latest(read_key)
                        if isinstance(latest, int) and not isinstance(latest, bool):
                            baseline[read_key] = int(latest)
                    dispatch_freshness[job.claim_id] = baseline
                    peer_attempt = self._scheduler.attempt_for_claim(job.claim_id)
                    if peer_attempt is None:
                        dispatch_identity[job.claim_id] = ("", None, None)
                    else:
                        raw_epoch = getattr(peer_attempt, "semantic_epoch", None)
                        raw_gv = getattr(peer_attempt, "graph_version", None)
                        dispatch_identity[job.claim_id] = (
                            str(getattr(peer_attempt, "attempt_id", "") or ""),
                            (
                                int(raw_epoch)
                                if isinstance(raw_epoch, int) and not isinstance(raw_epoch, bool)
                                else None
                            ),
                            (
                                int(raw_gv)
                                if isinstance(raw_gv, int) and not isinstance(raw_gv, bool)
                                else None
                            ),
                        )
            if adaptive or repair_only:
                actual_dispatched_task_ids = tuple(job.task_id for job in jobs)
                adaptive_epoch_meta["actual_dispatched_task_ids"] = actual_dispatched_task_ids
                adaptive_epoch_meta["dispatch_order_applied"] = tuple(
                    getattr(schedule_result, "dispatch_order_applied", ())
                )
                adaptive_epoch_meta["locality_matched_task_ids"] = tuple(
                    getattr(schedule_result, "locality_matched", ())
                )
                if adaptive_epoch_meta.get("fallback_attempted", False):
                    adaptive_epoch_meta["fallback_dispatched_task_ids"] = actual_dispatched_task_ids
                if repair_only:
                    adaptive_epoch_meta["automatic_rebase_dispatched_task_ids"] = tuple(
                        task_id
                        for task_id in actual_dispatched_task_ids
                        if task_id in pending_rebases
                    )
            if budget_aware:
                try:
                    budget_usage_current = _accumulate_dispatched_compute_budget_usage(
                        epoch,
                        budget_usage_current,
                        actual_dispatched_task_ids,
                    )
                except SchedulingError:
                    self._release_unexecuted_dispatches(
                        gid,
                        schedule_result.dispatched,
                        reason="compute_budget_accounting_fence",
                    )
                    raise
                adaptive_epoch_meta["budget_audit"] = _bounded_compute_budget_epoch_audit(
                    epoch,
                    dispatched_declared_usage_after=budget_usage_current,
                    actual_dispatched_task_ids=actual_dispatched_task_ids,
                )

            lifecycle = _SDKWorkerLifecycle(self._scheduler, jobs)

            async def account_measured_usage(
                job: WorkerJob,
                source: Any,
                *,
                elapsed_ms: int | None,
                _attempt_by_claim: dict[str, int] = attempt_by_claim,
            ) -> None:
                if budget_measurement is None:
                    return
                declared = budget_measurement.declared_by_task.get(job.task_id)
                if declared is None:
                    return
                measured_cost = _extract_measured_computation_cost(source)
                if measured_cost is None and elapsed_ms is None:
                    return
                from .compute_calibration import computation_cost_to_usage_vector

                measured_usage = computation_cost_to_usage_vector(
                    measured_cost,
                    elapsed_ms=max(0, int(elapsed_ms or 0)),
                )
                async with usage_accounting_lock:
                    if job.claim_id in usage_accounted_claims:
                        return
                    self._usage_ledger = _record_measured_attempt(
                        self._usage_ledger,
                        goal_id=budget_measurement.goal_id,
                        task_id=job.task_id,
                        attempt_id=(
                            job.claim_id
                            or f"{job.task_id}#attempt-{_attempt_by_claim.get(job.claim_id, 0)}"
                        ),
                        declared=declared,
                        measured=measured_usage,
                    )
                    usage_accounted_claims.add(job.claim_id)

            async def verify_and_commit(
                job: WorkerJob,
                dispatch_result: Any,
                lifecycle: _SDKWorkerLifecycle = lifecycle,
                attempt_by_claim: dict[str, int] = attempt_by_claim,
                rebase_dispatch_count: int = automatic_rebase_dispatches,
                preempt_superseded: bool = preempt_superseded,
                dispatch_freshness: dict[str, dict[str, int]] = dispatch_freshness,
                dispatch_identity: dict[
                    str, tuple[str, int | None, int | None]
                ] = dispatch_identity,
                jobs: list[WorkerJob] = jobs,
            ) -> None:
                task = tasks_by_id.get(job.task_id)
                agent = self._agents.get(job.agent_id)
                if task is None:
                    lifecycle.release_task(
                        gid,
                        job.task_id,
                        reason="missing_task",
                    )
                    failures.append(f"{job.task_id}: missing_task")
                    return
                if agent is None:
                    lifecycle.release_task(
                        gid,
                        job.task_id,
                        reason="missing_agent",
                    )
                    failures.append(f"{job.task_id}: missing_agent")
                    return

                executor_outcome = getattr(dispatch_result, "executor_outcome", None)
                executor_elapsed_ms = getattr(dispatch_result, "executor_elapsed_ms", None)
                provenance_context = getattr(dispatch_result, "provenance_context", None)
                provider_route = getattr(dispatch_result, "provider_route", None)
                provider_context = (
                    getattr(dispatch_result, "provider_context", None) or provenance_context
                )
                coverage_report = getattr(dispatch_result, "coverage_report", None)
                executor_api = getattr(dispatch_result, "executor_api", "legacy_task_id")
                await account_measured_usage(
                    job,
                    executor_outcome,
                    elapsed_ms=executor_elapsed_ms,
                )
                try:
                    if provider_route is not None:
                        registry = self._provider_registry
                        if registry is None:
                            raise ProviderRoutingError("provider registry unavailable")
                        outcome = await registry.verify_async(
                            provider_route,
                            job.task_id,
                            provider_context,
                            executor_outcome,
                            task.verify,
                        )
                    elif task.verify is not None:
                        outcome = await _invoke_verifier_async(
                            task.verify,
                            task_id=job.task_id,
                            context=provenance_context,
                            executor_api=executor_api,
                        )
                    else:
                        outcome = executor_outcome
                except Exception as exc:
                    # A verifier is part of the same cognition attempt as
                    # the executor.  If a semantic interrupt was requested
                    # while it was running, a verifier exception must not be
                    # downgraded to an ordinary ``verifier_failed`` retry:
                    # the result was computed from stale cognition.  Quarantine
                    # the exact claim first, then re-raise a cooperative
                    # interrupt so the worker pool emits its durable
                    # CANCELLED transition.  The worker's exact-claim release
                    # is intentionally idempotent after quarantine.
                    interrupt_token = getattr(
                        provenance_context,
                        "cancellation_token",
                        None,
                    )
                    interrupt_pending = bool(
                        provenance_context is not None
                        and (
                            getattr(provenance_context, "interrupt_requested", False)
                            or getattr(provenance_context, "interrupt_observed", False)
                        )
                    )
                    if interrupt_pending and interrupt_token is not None:
                        # ``observe_interrupt`` advances the durable
                        # observation boundary for a verifier that noticed
                        # the request but raised before calling
                        # ``raise_if_interrupted`` itself.
                        with suppress(Exception):
                            observe = getattr(provenance_context, "observe_interrupt", None)
                            if callable(observe):
                                observe()
                        action = str(getattr(interrupt_token, "action", "") or "preempt")
                        # A replacement owner may have won the exact claim
                        # fence concurrently.  The stale marker (if any) is
                        # still authoritative; do not turn the semantic
                        # interrupt into a generic verifier error.
                        with suppress(_ClaimFenceLost):
                            self._quarantine_stale_cognition(
                                gid,
                                job.task_id,
                                job.claim_id,
                                reason=f"semantic_interrupt:{action}",
                            )
                        failures.append(f"{job.task_id}: semantic_interrupt:{action}")
                        if isinstance(exc, CooperativeInterrupt):
                            raise
                        raise CooperativeInterrupt(interrupt_token) from exc
                    lifecycle.release_task(
                        gid,
                        job.task_id,
                        reason=f"verifier_failed:{type(exc).__name__}",
                    )
                    failures.append(f"{job.task_id}: verifier_failed:{type(exc).__name__}")
                    return

                if not isinstance(outcome, VerificationOutcome):
                    lifecycle.release_task(
                        gid,
                        job.task_id,
                        reason="missing_verifier"
                        if outcome is None
                        else "invalid_verifier_outcome",
                    )
                    reason = "missing_verifier" if outcome is None else "invalid_verifier_outcome"
                    failures.append(f"{job.task_id}: {reason}")
                    return
                if not outcome.passed:
                    lifecycle.release_task(
                        gid,
                        job.task_id,
                        reason="verification_failed",
                    )
                    failures.append(f"{job.task_id}: verification_failed")
                    return

                committed_ok = False
                async with semantic_commit_lock:
                    try:
                        # A cooperative request may arrive after operational
                        # success while an independent verifier is still
                        # running.  Re-check the bound token at the semantic
                        # commit fence so that late cognition cannot publish
                        # VERIFIED Evidence.
                        if provenance_context is not None and getattr(
                            provenance_context, "interrupt_requested", False
                        ):
                            action = str(
                                getattr(
                                    getattr(provenance_context, "cancellation_token", None),
                                    "action",
                                    "preempt",
                                )
                                or "preempt"
                            )
                            self._quarantine_stale_cognition(
                                gid,
                                job.task_id,
                                job.claim_id,
                                reason=f"semantic_interrupt:{action}",
                            )
                            failures.append(f"{job.task_id}: semantic_interrupt:{action}")
                            return
                        committed = self._commit_verified_outcome(
                            gid,
                            job.task_id,
                            job.agent_id,
                            outcome,
                            attempt_number=attempt_by_claim.get(job.claim_id, 0),
                            claim_id=job.claim_id,
                            task=task,
                            coverage_report=coverage_report,
                            provenance_context=provenance_context,
                        )
                        if not committed:
                            attempt_now = self._scheduler.attempt_for_claim(job.claim_id)
                            attempt_state = str(
                                getattr(
                                    getattr(attempt_now, "state", None),
                                    "value",
                                    "",
                                )
                                or ""
                            )
                            if automatic_rebase and attempt_state == "stale_cognition":
                                source_manifest = getattr(
                                    provenance_context,
                                    "automatic_rebase_manifest",
                                    None,
                                ) or getattr(task, "context_manifest", None)
                                source_attempt_id = str(
                                    getattr(attempt_now, "attempt_id", "")
                                    or getattr(
                                        provenance_context,
                                        "attempt_id",
                                        "",
                                    )
                                )
                                source_snapshot = getattr(
                                    attempt_now,
                                    "agent_snapshot",
                                    None,
                                )
                                try:
                                    current_graph_version = int(
                                        self._vpg.get_graph(gid).current_version
                                    )
                                    decision, replacement_manifest = plan_automatic_rebase(
                                        task_id=job.task_id,
                                        graph_id=gid,
                                        graph_version=current_graph_version,
                                        agent_snapshot=source_snapshot,
                                        context_manifest=source_manifest,
                                        facts=self._facts,
                                        claim_id=job.claim_id,
                                        attempt_id=source_attempt_id,
                                        reason=(
                                            getattr(attempt_now, "error", "")
                                            or "read-set freshness fence"
                                        ),
                                    )
                                except Exception as exc:
                                    decision = None
                                    replacement_manifest = None
                                    automatic_rebase_records.append(
                                        {
                                            "task_id": job.task_id,
                                            "source_claim_id": job.claim_id,
                                            "source_attempt_id": source_attempt_id,
                                            "status": "blocked",
                                            "reason": (
                                                "automatic rebase planner failed: "
                                                + _bounded_audit_error(exc)
                                            ),
                                        }
                                    )
                                if decision is not None:
                                    automatic_rebase_records.append(decision.as_dict())
                                if (
                                    decision is not None
                                    and replacement_manifest is not None
                                    and decision.redispatchable
                                    and rebase_dispatch_count + len(pending_rebases)
                                    < max_automatic_rebase_dispatches
                                ):
                                    # The old Claim has already been
                                    # quarantined/released by the commit
                                    # fence.  The replacement is admitted by
                                    # the ordinary Scheduler path below.
                                    pending_rebases[job.task_id] = (
                                        decision,
                                        replacement_manifest,
                                    )
                                else:
                                    reason = (
                                        "automatic_rebase_blocked"
                                        if decision is None
                                        else (
                                            "automatic_rebase_budget_exhausted"
                                            if decision.redispatchable
                                            else "automatic_rebase_blocked"
                                        )
                                    )
                                    failures.append(f"{job.task_id}: {reason}")
                            else:
                                failures.append(f"{job.task_id}: stale_verifier_outcome")
                        else:
                            committed_ok = True
                    except _ClaimFenceLost:
                        # A semantic interrupt may quarantine the Attempt
                        # while the verifier is still returning.  Release
                        # only this exact claim; Scheduler preserves the
                        # STALE_COGNITION marker and the expected-claim fence
                        # makes this a no-op if a replacement owner won.
                        lifecycle.release_task(
                            gid,
                            job.task_id,
                            reason="stale_cognition:commit_fence_lost",
                            expected_claim_id=job.claim_id,
                        )
                        failures.append(f"{job.task_id}: claim_fence_lost")
                        return
                    except Exception as exc:
                        lifecycle.release_task(
                            gid,
                            job.task_id,
                            reason=f"evidence_attachment_failed:{type(exc).__name__}",
                        )
                        error = VerificationError(
                            f"failed to attach Evidence for task {job.task_id!r}",
                            cause=exc,
                        )
                        fatal_errors.append(error)
                        raise error from exc

                # A successful commit is the only event that can advance the
                # graph and thereby supersede a still-running peer's declared
                # input.  Detect and interrupt exactly those peers now, before
                # the batch is awaited to completion.
                if preempt_superseded and committed_ok:
                    self._preempt_superseded_peers(
                        goal,
                        gid=gid,
                        committer_claim_id=job.claim_id,
                        jobs=jobs,
                        declared_read_keys=declared_read_keys,
                        dispatch_freshness=dispatch_freshness,
                        dispatch_identity=dispatch_identity,
                    )

            async def account_worker_failure(job: WorkerJob, outcome: Any) -> None:
                await account_measured_usage(
                    job,
                    getattr(outcome, "dispatch_result", None),
                    elapsed_ms=None,
                )

            pool = AsyncWorkerPool(
                _SDKExecutorDispatcher(
                    self,
                    tasks_by_id,
                    graph_id=gid,
                    goal=goal,
                    provider_routing_enabled=adaptive,
                    context_overrides=pending_rebases,
                ),
                scheduler=lifecycle,
                max_concurrency=max_concurrency,
                agent_concurrency={
                    agent_id: max(1, self._agents[agent_id].max_concurrency)
                    for agent_id in sorted({job.agent_id for job in jobs})
                    if agent_id in self._agents
                },
                on_success=verify_and_commit,
                on_failure=account_worker_failure,
                on_interrupt_transition=self._record_interrupt_transition,
            )
            for job in jobs:
                self._active_worker_pools[job.claim_id] = pool

            async def _try_refill(completed_outcome: Any) -> list:
                """事件驱动补位：slot 释放后从最新 frontier 选最高优先任务补上。

                P0 限制：仅基础 adaptive 路径（非 budget_aware / resource_aware）。
                版本 fence：图版本前进则不补位，让外层 replan。
                """
                if not streaming_dispatch or repair_only or not adaptive:
                    return []
                if budget_aware or resource_aware:
                    return []
                nonlocal dispatched
                if regular_dispatches + dispatched >= max_dispatches:
                    return []
                if pool.active_jobs >= max_concurrency:
                    return []
                # 版本一致性由 scheduler.run_pass(expected_graph_version=...) 保证；
                # 正常 Evidence commit 也会推进 graph_version，这里不再自行 fence，
                # 否则每次任务完成都会误判为"图结构变化"而拒绝补位。
                try:
                    current_gv = int(self._vpg.get_graph(gid).current_version)
                except Exception:
                    return []
                # 直接从 runtime_state 取 ready_frontier，而不是用 _plan_adaptive_epoch。
                # 原因：policy 会因"已完成但 state 未刷新的 active 任务 access view 不可用"
                # 而保守 defer 全部任务；scheduler.run_pass 的 lease/admission 才是权威。
                try:
                    _st = self.runtime_state(goal)
                    ready = tuple(getattr(_st.progress, "ready_frontier", ()))
                    verified_set = set(getattr(_st.progress, "verified_task_ids", ()))
                    invalid_set = set(getattr(_st.progress, "invalid_task_ids", ()))
                except Exception:
                    return []
                # 在飞 = 已提交但未完成的任务（jobs 列表 - 已完成 outcomes）
                completed_ids = {getattr(o, "task_id", "") for o in outcomes}
                in_flight = {j.task_id for j in jobs} - completed_ids
                candidates = tuple(
                    t for t in ready
                    if t not in in_flight and t not in verified_set and t not in invalid_set
                    and t != getattr(completed_outcome, "task_id", "")
                )
                if not candidates:
                    return []
                try:
                    refill_result = self._scheduler.run_pass(
                        gid, max_claims=1, allowed_task_ids=candidates,
                        dispatch_order=candidates,
                        task_read_keys_by_task=declared_read_keys,
                        expected_graph_version=current_gv,
                    )
                except Exception:
                    return []
                if not getattr(refill_result, "dispatched", None):
                    return []
                new_jobs = []
                for dispatch in refill_result.dispatched:
                    task_id = dispatch["task_id"]
                    claim_id = dispatch.get("claim_id", "")
                    claim = self._scheduler.active_claim_for_task(task_id, gid)
                    if claim is None or claim.claim_id != claim_id:
                        continue
                    task = tasks_by_id.get(task_id)
                    job = WorkerJob(
                        graph_id=gid,
                        graph_version=getattr(claim, "graph_version", None),
                        task_id=task_id, claim_id=claim_id,
                        agent_id=dispatch["agent_id"],
                        task_kind="" if task is None else task.task_kind,
                    )
                    new_jobs.append(job)
                    attempt_by_claim[claim_id] = int(getattr(claim, "attempt_number", 0))
                    jobs.append(job)
                    dispatched += 1
                    if preempt_superseded:
                        baseline: dict[str, int] = {}
                        for rk in declared_read_keys.get(task_id, ()):
                            latest = self._facts.latest(rk)
                            if isinstance(latest, int) and not isinstance(latest, bool):
                                baseline[rk] = int(latest)
                        dispatch_freshness[claim_id] = baseline
                        peer = self._scheduler.attempt_for_claim(claim_id)
                        dispatch_identity[claim_id] = (
                            str(getattr(peer, "attempt_id", "") or ""),
                            (int(getattr(peer, "semantic_epoch", 0))
                             if isinstance(getattr(peer, "semantic_epoch", None), int) else None),
                            (int(getattr(peer, "graph_version", 0))
                             if isinstance(getattr(peer, "graph_version", None), int) else None),
                        )
                for job in new_jobs:
                    lifecycle.register_job(job)
                    self._active_worker_pools[job.claim_id] = pool
                    pool.submit(job)
                if new_jobs:
                    streaming_refills.append({
                        "triggered_by": getattr(completed_outcome, "task_id", ""),
                        "refilled_task_ids": tuple(j.task_id for j in new_jobs),
                        "graph_version": current_gv,
                    })
                return new_jobs

            try:
                if (
                    streaming_dispatch and adaptive and not repair_only
                    and not budget_aware and not resource_aware
                ):
                    outcomes: list = []
                    async for outcome in pool.run_streaming(jobs):
                        outcomes.append(outcome)
                        await _try_refill(outcome)
                else:
                    outcomes = await pool.run(jobs)
            except asyncio.CancelledError as cancellation:
                # Cancellation is the primary control signal.  Cleanup must
                # therefore be best-effort and exact-claim fenced: a failure
                # releasing one job must not prevent subsequent jobs from
                # being attempted, and a replacement owner must never be
                # touched by this stale batch.  ``release_task`` is normally
                # synchronous, but integrations/test doubles may still raise
                # arbitrary BaseExceptions; collect those diagnostics rather
                # than masking the original CancelledError.
                cleanup_errors: list[str] = []
                for job in jobs:
                    try:
                        lifecycle.release_task(
                            job.graph_id,
                            job.task_id,
                            reason="worker_cancelled",
                            expected_claim_id=job.claim_id,
                        )
                    except BaseException as exc:
                        release_error = _bounded_audit_error(exc)
                        cleanup_errors.append(f"{job.claim_id}:{release_error}")
                        # A failed exact-Claim release is not merely a log
                        # message: the ownership epoch may still have a live
                        # Kernel lease after this coroutine is gone.  Persist
                        # a journal-only recovery marker tied to the original
                        # Claim/Attempt.  Marker publication is itself
                        # best-effort so neither a storage failure nor an
                        # integration test double can replace CancelledError.
                        try:
                            attempt = self._scheduler.attempt_for_claim(job.claim_id)
                            claim = next(
                                (
                                    item
                                    for item in self._scheduler.claims
                                    if item.claim_id == job.claim_id
                                ),
                                None,
                            )
                            self._scheduler.record_cleanup_required(
                                graph_id=job.graph_id,
                                task_id=job.task_id,
                                claim_id=job.claim_id,
                                attempt_id=(
                                    ""
                                    if attempt is None
                                    else str(getattr(attempt, "attempt_id", "") or "")
                                ),
                                lease_id=(
                                    ""
                                    if claim is None
                                    else str(getattr(claim, "lease_id", "") or "")
                                ),
                                reason="run_async cancellation exact-claim release failed",
                                error=release_error,
                            )
                        except BaseException as marker_exc:
                            cleanup_errors.append(
                                f"{job.claim_id}:cleanup_marker:{_bounded_audit_error(marker_exc)}"
                            )
                try:
                    self._scheduler.reconcile()
                except BaseException as exc:
                    cleanup_errors.append(f"reconcile:{_bounded_audit_error(exc)}")
                if cleanup_errors:
                    # ``add_note`` is intentionally guarded: diagnostics must
                    # never replace the cancellation signal if an exotic
                    # exception object rejects notes.
                    _logger.warning(
                        "run_async cancellation cleanup errors: %s",
                        "; ".join(cleanup_errors),
                    )
                    with suppress(BaseException):
                        cancellation.add_note(
                            "run_async cancellation cleanup errors: " + "; ".join(cleanup_errors)
                        )
                raise
            finally:
                for job in jobs:
                    if self._active_worker_pools.get(job.claim_id) is pool:
                        self._active_worker_pools.pop(job.claim_id, None)

            dispatched += len(jobs)
            if repair_only:
                automatic_rebase_dispatches += len(jobs)
                for job in jobs:
                    pending_rebases.pop(job.task_id, None)
            else:
                regular_dispatches += len(jobs)
            steps += 1
            for outcome in outcomes:
                if not outcome.ok:
                    failures.append(f"{outcome.task_id}: {outcome.error or outcome.status.value}")
            if fatal_errors:
                raise fatal_errors[0]

        result = self.result(gid)
        result.failures.extend(failures)
        # Keep the long-standing default metadata contract stable for callers
        # that do not exercise automatic stale-cognition repair.  The
        # additional counters/audit transcript are exposed whenever repair
        # happened, was blocked, or the caller explicitly changed the repair
        # policy.  This makes the new control-plane observability additive
        # without forcing every existing async consumer to handle new keys.
        result.meta.update(
            {
                "execution_mode": "async",
                "dispatched": dispatched,
                "max_concurrency": max_concurrency,
            }
        )
        if (
            automatic_rebase_dispatches
            or automatic_rebase_records
            or pending_rebases
            or not automatic_rebase
            or max_automatic_rebase_dispatches != 1
        ):
            result.meta.update(
                {
                    "regular_dispatches": regular_dispatches,
                    "automatic_rebase_dispatches": automatic_rebase_dispatches,
                    "automatic_rebase_enabled": automatic_rebase,
                    "automatic_rebase_max_dispatches": max_automatic_rebase_dispatches,
                    "automatic_rebase_records": tuple(automatic_rebase_records),
                    "automatic_rebase_pending": tuple(sorted(pending_rebases)),
                }
            )
        if adaptive:
            result.meta.update(
                {
                    "adaptive": True,
                    "adaptive_policy": (
                        "unified-adaptive-control"
                        if budget_aware and resource_aware
                        else "verified-progress-budget"
                        if budget_aware
                        else "resource-aware-conflict"
                        if resource_aware
                        else "conflict-aware"
                        if adaptive_graph is not None
                        else "frontier"
                    ),
                    "resource_aware": bool(resource_aware),
                    "unified_control": bool(budget_aware and resource_aware),
                    "adaptive_epochs": adaptive_epochs,
                    "streaming_dispatch": bool(streaming_dispatch),
                    "streaming_refills": tuple(streaming_refills),
                }
            )
            if budget_aware:
                result.meta.update(
                    {
                        "budget_aware": True,
                        "budget_limits": _compute_budget_limits_audit(budget_limits_value),
                        "budget_usage": _compute_budget_usage_audit(budget_usage_current),
                        "measured_usage": self._measured_usage_audit(goal.goal_id),
                        "budget_calibration": tuple(
                            audit.as_dict() for audit in budget_calibration_audits
                        ),
                    }
                )
        return result

    def _record_interrupt_transition(self, transition: InterruptTransition) -> None:
        """Persist one bounded worker interrupt phase.

        Worker callbacks carry only exact execution identities and bounded
        diagnostics.  The Scheduler journal is the durable audit authority;
        this adapter never mutates Claims or Leases.
        """

        graph_version = transition.graph_version
        if graph_version is None:
            return
        try:
            self._scheduler.record_interrupt_acknowledgement(
                graph_id=transition.graph_id,
                graph_version=int(graph_version),
                claim_id=transition.claim_id,
                attempt_id=transition.attempt_id,
                interrupt_id=transition.interrupt_id,
                action=transition.action or "preempt",
                status=transition.phase.value,
                decision_hash=transition.decision_hash,
                reason=transition.reason,
            )
        except Exception:
            # A journal failure must not turn a cooperative cancellation
            # signal into an unhandled callback exception.  Reconciliation
            # remains responsible for surfacing durable-state failures.
            return

    def deliver_interrupt(
        self,
        goal_or_graph: Goal | str,
        *,
        claim_id: str,
        action: str,
        expected_graph_version: int | None = None,
        expected_semantic_epoch: int | None = None,
        attempt_id: str | None = None,
        task_id: str | None = None,
        interrupt_id: str = "",
        decision_hash: str = "",
        reason: str = "semantic interrupt requested",
        superseded_from_graph_version: int | None = None,
    ) -> InterruptDelivery:
        """Deliver a cooperative semantic interrupt to one active async job.

        The request is accepted only when graph, claim, task, attempt, and
        semantic epoch all match the current Scheduler projection.  This is a
        control-plane operation: Claims/Leases are not released here; the
        worker lifecycle performs exact fenced cleanup after the callback
        observes (or ignores) the token.

        ``superseded_from_graph_version`` opts into the internal semantic-
        preemption fence used by the execution loop: instead of requiring the
        attempt to sit on the *current* graph version, delivery requires the
        attempt to still carry exactly the observed dispatch version while the
        authoritative graph has advanced beyond it (a declared input was
        superseded).  The claim/task/attempt/semantic-epoch identity fences are
        never relaxed, so a replacement (e.g. rebased) attempt is still
        rejected.  When it is ``None`` the strict "attempt must be current"
        behaviour is byte-identical to before.
        """

        normalized_claim = str(claim_id).strip()
        normalized_action = str(action).strip().lower()
        if not normalized_claim:
            raise ConfigurationError("claim_id must be non-empty")
        if normalized_action not in {"preempt", "rebase"}:
            return InterruptDelivery(
                claim_id=normalized_claim,
                status=InterruptDeliveryStatus.UNSUPPORTED_ACTION,
                action=normalized_action,
                interrupt_id=str(interrupt_id).strip(),
                decision_hash=str(decision_hash).strip().lower(),
                reason=str(reason).strip(),
            )

        if isinstance(goal_or_graph, Goal):
            gid = self._gid_for(goal_or_graph.goal_id, compile_if_missing=False)
        else:
            raw = str(goal_or_graph).strip()
            gid = raw if raw in set(self._goal_gid.values()) else self._gid_for(raw)
        if not gid:
            raise ConfigurationError("goal/graph is not compiled")

        current_version = int(self._vpg_surface.current_graph_version(gid))
        if expected_graph_version is not None and (
            isinstance(expected_graph_version, bool) or not isinstance(expected_graph_version, int)
        ):
            raise ConfigurationError("expected_graph_version must be an integer")
        if superseded_from_graph_version is not None and (
            isinstance(superseded_from_graph_version, bool)
            or not isinstance(superseded_from_graph_version, int)
        ):
            raise ConfigurationError("superseded_from_graph_version must be an integer")
        if expected_graph_version is not None and expected_graph_version != current_version:
            return InterruptDelivery(
                claim_id=normalized_claim,
                status=InterruptDeliveryStatus.STALE_GRAPH,
                graph_id=gid,
                graph_version=current_version,
                action=normalized_action,
                interrupt_id=str(interrupt_id).strip(),
                decision_hash=str(decision_hash).strip().lower(),
                reason=str(reason).strip(),
            )

        claim = next(
            (item for item in self._scheduler.claims if item.claim_id == normalized_claim),
            None,
        )
        if claim is None or claim.graph_id != gid:
            return InterruptDelivery(
                claim_id=normalized_claim,
                status=InterruptDeliveryStatus.IDENTITY_MISMATCH,
                graph_id=gid,
                graph_version=current_version,
                action=normalized_action,
                interrupt_id=str(interrupt_id).strip(),
                decision_hash=str(decision_hash).strip().lower(),
                reason=str(reason).strip(),
            )
        if task_id is not None and str(task_id) != claim.task_id:
            return InterruptDelivery(
                claim_id=normalized_claim,
                status=InterruptDeliveryStatus.IDENTITY_MISMATCH,
                graph_id=gid,
                graph_version=current_version,
                task_id=claim.task_id,
                action=normalized_action,
                interrupt_id=str(interrupt_id).strip(),
                decision_hash=str(decision_hash).strip().lower(),
                reason=str(reason).strip(),
            )
        if getattr(claim.state, "value", claim.state) != "active":
            return InterruptDelivery(
                claim_id=normalized_claim,
                status=InterruptDeliveryStatus.NOT_ACTIONABLE,
                graph_id=gid,
                graph_version=current_version,
                task_id=claim.task_id,
                action=normalized_action,
                interrupt_id=str(interrupt_id).strip(),
                decision_hash=str(decision_hash).strip().lower(),
                reason=str(reason).strip(),
            )

        attempt = self._scheduler.attempt_for_claim(normalized_claim)
        if attempt is None or attempt.claim_id != normalized_claim:
            return InterruptDelivery(
                claim_id=normalized_claim,
                status=InterruptDeliveryStatus.IDENTITY_MISMATCH,
                graph_id=gid,
                graph_version=current_version,
                task_id=claim.task_id,
                action=normalized_action,
                interrupt_id=str(interrupt_id).strip(),
                decision_hash=str(decision_hash).strip().lower(),
                reason=str(reason).strip(),
            )
        # Do not rely solely on the optional caller-supplied fence.  The
        # attempt itself carries the graph snapshot on which this cognition
        # was computed; accepting an interrupt for an attempt from an older
        # graph would route a control signal into stale execution.  This
        # unconditional check also protects callers that omit
        # ``expected_graph_version``.
        attempt_graph_version = int(getattr(attempt, "graph_version", current_version))
        if superseded_from_graph_version is None:
            attempt_graph_is_stale = attempt_graph_version != current_version
        else:
            # Internal semantic-preemption path.  The caller observed this exact
            # attempt running at ``superseded_from_graph_version`` and the
            # authoritative graph has since advanced past it, superseding one of
            # the attempt's declared inputs.  Fence on the *observed* basis
            # rather than a moving ``current_version``: refuse unless the attempt
            # still carries the exact version we observed *and* the graph really
            # advanced beyond it.  The claim/task/attempt/semantic-epoch identity
            # fences above are never relaxed, so a replacement (e.g. rebased)
            # attempt — which necessarily carries a different attempt id, epoch,
            # or graph version — is rejected here or by those fences.
            attempt_graph_is_stale = (
                attempt_graph_version != superseded_from_graph_version
                or current_version <= superseded_from_graph_version
            )
        if attempt_graph_is_stale:
            return InterruptDelivery(
                claim_id=normalized_claim,
                status=InterruptDeliveryStatus.STALE_GRAPH,
                graph_id=gid,
                graph_version=current_version,
                task_id=claim.task_id,
                attempt_id=attempt.attempt_id,
                semantic_epoch=attempt.semantic_epoch,
                action=normalized_action,
                interrupt_id=str(interrupt_id).strip(),
                decision_hash=str(decision_hash).strip().lower(),
                reason=str(reason).strip(),
            )
        if attempt_id is not None and str(attempt_id) != attempt.attempt_id:
            return InterruptDelivery(
                claim_id=normalized_claim,
                status=InterruptDeliveryStatus.IDENTITY_MISMATCH,
                graph_id=gid,
                graph_version=current_version,
                task_id=claim.task_id,
                attempt_id=attempt.attempt_id,
                semantic_epoch=attempt.semantic_epoch,
                action=normalized_action,
                interrupt_id=str(interrupt_id).strip(),
                decision_hash=str(decision_hash).strip().lower(),
                reason=str(reason).strip(),
            )
        if expected_semantic_epoch is not None and (
            isinstance(expected_semantic_epoch, bool)
            or not isinstance(expected_semantic_epoch, int)
        ):
            raise ConfigurationError("expected_semantic_epoch must be an integer")
        if expected_semantic_epoch is not None and (
            expected_semantic_epoch != attempt.semantic_epoch
        ):
            return InterruptDelivery(
                claim_id=normalized_claim,
                status=InterruptDeliveryStatus.STALE_EPOCH,
                graph_id=gid,
                graph_version=current_version,
                task_id=claim.task_id,
                attempt_id=attempt.attempt_id,
                semantic_epoch=attempt.semantic_epoch,
                action=normalized_action,
                interrupt_id=str(interrupt_id).strip(),
                decision_hash=str(decision_hash).strip().lower(),
                reason=str(reason).strip(),
            )

        # The registry is keyed by the exact claim identity.  Looking up by
        # graph would be ambiguous when independent ``run_async`` calls share
        # one graph.
        pool = self._active_worker_pools.get(normalized_claim)
        if pool is None:
            return InterruptDelivery(
                claim_id=normalized_claim,
                status=InterruptDeliveryStatus.NOT_RUNNING,
                graph_id=gid,
                graph_version=current_version,
                task_id=claim.task_id,
                attempt_id=attempt.attempt_id,
                semantic_epoch=attempt.semantic_epoch,
                action=normalized_action,
                interrupt_id=str(interrupt_id).strip(),
                decision_hash=str(decision_hash).strip().lower(),
                reason=str(reason).strip(),
            )
        return pool.request_interrupt(
            normalized_claim,
            action=normalized_action,
            interrupt_id=interrupt_id,
            reason=reason,
            decision_hash=decision_hash,
        )

    def _preempt_superseded_peers(
        self,
        goal: Goal,
        *,
        gid: str,
        committer_claim_id: str,
        jobs: list[WorkerJob],
        declared_read_keys: Mapping[str, tuple[str, ...]],
        dispatch_freshness: Mapping[str, Mapping[str, int]],
        dispatch_identity: Mapping[str, tuple[str, int | None, int | None]],
    ) -> None:
        """Interrupt still-running batch peers whose declared inputs were just
        superseded by the sibling commit that advanced the graph.

        This is the one place the execution loop delivers a semantic interrupt
        on its own behalf.  Detection is deterministic and uses no wall clock:
        a peer is superseded when one of its declared read keys now resolves to
        a Facts version strictly greater than the version observed when the peer
        was dispatched (``dispatch_freshness``).  Delivery is routed through the
        public :meth:`deliver_interrupt` so every claim/task/attempt/semantic-
        epoch identity fence is enforced; the observed dispatch graph version is
        pinned via ``superseded_from_graph_version`` so a replacement (e.g.
        rebased) attempt can never be hit and valid work is never interrupted.
        """

        pool = self._active_worker_pools.get(committer_claim_id)
        if pool is None:
            return
        # Only claims the pool still reports as running are candidates; a peer
        # that already finished (or is mid-cleanup) is not in this map.
        running_claims = set(getattr(pool, "interrupt_tokens", {}))
        for job in sorted(jobs, key=lambda item: item.claim_id):
            peer_claim = job.claim_id
            if peer_claim == committer_claim_id or peer_claim not in running_claims:
                continue
            baseline = dispatch_freshness.get(peer_claim) or {}
            if not baseline:
                continue
            superseded_key = ""
            for read_key in declared_read_keys.get(job.task_id, ()):
                observed = baseline.get(read_key)
                if observed is None:
                    continue
                latest = self._facts.latest(read_key)
                if isinstance(latest, int) and not isinstance(latest, bool) and latest > observed:
                    superseded_key = read_key
                    break
            if not superseded_key:
                continue
            attempt_id, semantic_epoch, graph_version = dispatch_identity.get(
                peer_claim, ("", None, None)
            )
            if not attempt_id or semantic_epoch is None or graph_version is None:
                # Without the exact observed identity we cannot fence delivery;
                # never interrupt on a partial identity.
                continue
            decision = _bounded_hash(
                {
                    "kind": "preempt_superseded",
                    "graph_id": gid,
                    "claim_id": peer_claim,
                    "task_id": job.task_id,
                    "attempt_id": attempt_id,
                    "semantic_epoch": semantic_epoch,
                    "observed_graph_version": int(graph_version),
                    "superseded_key": superseded_key,
                }
            )
            # Route through the public delivery method: it re-validates the
            # live claim/task/attempt/semantic-epoch and the observed graph
            # basis before touching the exact worker pool.
            self.deliver_interrupt(
                goal,
                claim_id=peer_claim,
                task_id=job.task_id,
                attempt_id=attempt_id,
                action="preempt",
                expected_semantic_epoch=semantic_epoch,
                superseded_from_graph_version=int(graph_version),
                interrupt_id=decision,
                decision_hash=decision,
                reason=f"preempt_superseded:{superseded_key}",
            )

    # ── Harness control-plane bridge ─────────────────────────────────────
    def _durable_harness_history(
        self,
        identity: Any,
    ) -> tuple[list[Any], list[Any]]:
        """Return replayable events for ``identity`` and foreign claim events.

        Harness session state is intentionally not folded into Scheduler Claim
        rows.  Instead, each accepted control request journals a bounded
        before/after snapshot in the Scheduler event chain.  This helper keeps
        the recovery boundary explicit: a session can only be reopened from a
        complete history for the exact Claim/Attempt/session identity.
        """

        from lhos.runtimes.multi_agent.events import SchedulerEventType

        matching: list[Any] = []
        foreign: list[Any] = []
        # Scheduler event models are intentionally lightweight audit DTOs and
        # are not frozen.  For a file-backed runtime, reload through the
        # hash-chain-verified state store instead of trusting a caller-mutated
        # in-memory alias exposed by ``SchedulerSession.events``.
        event_source = self._scheduler.events
        state_store = getattr(self._scheduler, "_state_store", None)
        if state_store is not None:
            try:
                event_source = state_store.load().events
            except Exception as exc:
                raise ConfigurationError("durable Harness history could not be verified") from exc
        for event in event_source:
            if event.event_type is not SchedulerEventType.HARNESS_CONTROL:
                continue
            metadata = event.metadata if isinstance(event.metadata, Mapping) else {}
            session_id = str(metadata.get("session_id", "")).strip()
            if event.claim_id == identity.claim_id and session_id == identity.session_id:
                if event.attempt_id and event.attempt_id != identity.attempt_id:
                    raise ConfigurationError(
                        "durable Harness history has an attempt identity conflict"
                    )
                matching.append(event)
            elif session_id == identity.session_id:
                # Session ids are global identities, not claim-local aliases.
                # A same-id event attached to another claim/attempt must not
                # be silently ignored during registration.
                raise ConfigurationError(
                    "durable Harness history reuses this session identity for another Claim/Attempt"
                )
            elif event.claim_id == identity.claim_id and session_id:
                foreign.append(event)
            elif event.claim_id == identity.claim_id:
                # Events written by an older schema cannot be safely associated
                # with a replacement Harness session.  Keep them separate so
                # registration can fail closed rather than silently replaying
                # an unrelated history.
                foreign.append(event)
        return matching, foreign

    @staticmethod
    def _durable_snapshot_from_metadata(metadata: Mapping[str, Any], key: str) -> Any:
        from .harness import HarnessSessionSnapshot

        raw = metadata.get(key)
        if not isinstance(raw, Mapping):
            raise ConfigurationError(
                f"durable Harness event is missing {key}; session replay is unavailable"
            )
        try:
            return HarnessSessionSnapshot.model_validate(raw)
        except Exception as exc:
            raise ConfigurationError(f"durable Harness event contains an invalid {key}") from exc

    def _restore_harness_from_durable_history(
        self,
        harness: Any,
        identity: Any,
    ) -> None:
        """Restore a Harness logical snapshot and durable request index.

        This method never resumes user code.  It only reconstructs the
        versioned session projection and idempotency results from the
        Scheduler's hash-verified event journal.  Adapters opt in through the
        ``restore_durable_snapshot`` method; adapters without that method must
        already expose the exact latest snapshot and are otherwise rejected.
        """

        from .harness import (
            HarnessControlResult,
            HarnessOperation,
            HarnessResultStatus,
            HarnessSessionSnapshot,
        )

        events, foreign = self._durable_harness_history(identity)
        if foreign:
            # A claim cannot silently acquire a replacement session while an
            # older session has durable control history.  The ownership
            # handoff protocol is intentionally not implemented yet.
            raise ConfigurationError(
                f"Claim {identity.claim_id!r} has durable Harness history for "
                "another or legacy session; explicit ownership handoff is required"
            )
        if not events:
            return

        current = harness.snapshot
        if not isinstance(current, HarnessSessionSnapshot):
            raise ConfigurationError("Harness snapshot must be a HarnessSessionSnapshot")
        cursor: HarnessSessionSnapshot | None = None
        replay_cache: list[tuple[str, str, Any, Any]] = []
        seen_requests: dict[str, tuple[str, Any]] = {}
        for event in events:
            metadata = event.metadata if isinstance(event.metadata, Mapping) else {}
            if metadata.get("schema_version") != "harness-session.v1":
                raise ConfigurationError(
                    "durable Harness history has an unsupported schema version"
                )
            before = self._durable_snapshot_from_metadata(metadata, "before_snapshot")
            after = self._durable_snapshot_from_metadata(metadata, "after_snapshot")
            if (
                before.identity != identity
                or after.identity != identity
                or event.graph_id != identity.graph_id
                or event.task_id != identity.task_id
                or event.agent_id != identity.agent_id
                or event.attempt_id != identity.attempt_id
                or event.graph_version != identity.graph_version
            ):
                raise ConfigurationError(
                    "durable Harness history does not match the exact session identity"
                )
            if cursor is not None and before != cursor:
                raise ConfigurationError(
                    "durable Harness history has a revision/identity discontinuity"
                )
            if cursor is None and before.revision != 0:
                raise ConfigurationError(
                    "durable Harness history does not start at session revision zero"
                )
            if after.revision < before.revision or after.revision > before.revision + 1:
                raise ConfigurationError(
                    "durable Harness history contains an invalid revision transition"
                )
            status = str(metadata.get("status", "")).strip()
            operation = str(metadata.get("operation", "")).strip()
            request_id = str(metadata.get("request_id", "")).strip()
            request_fingerprint = str(metadata.get("request_fingerprint", "")).strip()
            control_signature = str(metadata.get("control_signature", "")).strip()
            if (
                not request_id
                or len(request_fingerprint) != 64
                or any(char not in "0123456789abcdef" for char in request_fingerprint.lower())
                or len(control_signature) != 64
                or any(char not in "0123456789abcdef" for char in control_signature.lower())
            ):
                raise ConfigurationError(
                    "durable Harness history lacks a valid request id/signature; "
                    "session replay is unavailable"
                )
            try:
                normalized_operation = HarnessOperation(operation)
                normalized_status = HarnessResultStatus(status)
            except ValueError as exc:
                raise ConfigurationError(
                    "durable Harness history contains an unknown operation/status"
                ) from exc
            before_revision = metadata.get("before_revision")
            after_revision = metadata.get("after_revision")
            before_state = str(metadata.get("before_state", "")).strip()
            after_state = str(metadata.get("after_state", "")).strip()
            if (
                before_revision != before.revision
                or after_revision != after.revision
                or before_state != before.state.value
                or after_state != after.state.value
            ):
                raise ConfigurationError(
                    "durable Harness history metadata disagrees with its snapshots"
                )
            if normalized_status is HarnessResultStatus.APPLIED:
                if after.revision != before.revision + 1:
                    raise ConfigurationError(
                        "durable applied Harness control must advance the revision"
                    )
            elif after.revision != before.revision:
                raise ConfigurationError(
                    "durable rejected Harness control cannot advance the revision"
                )
            expected_event_key = "|".join(
                (
                    identity.claim_id,
                    request_fingerprint,
                    normalized_status.value,
                    str(after.revision),
                    str(after.identity.graph_version),
                    str(after.identity.semantic_epoch),
                )
            )
            expected_event_id = (
                "harness-control-" + hashlib.sha256(expected_event_key.encode()).hexdigest()
            )
            if event.event_id != expected_event_id:
                raise ConfigurationError(
                    "durable Harness history event identity does not match its payload"
                )
            result = HarnessControlResult(
                request_id=request_id,
                operation=normalized_operation,
                status=normalized_status,
                before=before,
                after=after,
                message=str(event.reason or ""),
            )
            previous = seen_requests.get(request_id)
            if previous is not None:
                previous_signature, previous_result = previous
                if (
                    previous_signature != control_signature
                    or previous_result.operation is not result.operation
                    or previous_result.status is not result.status
                    or previous_result.before != result.before
                    or previous_result.after != result.after
                ):
                    raise ConfigurationError(
                        f"durable Harness request_id {request_id!r} has conflicting outcomes"
                    )
                raise ConfigurationError(
                    f"durable Harness request_id {request_id!r} appears more than once"
                )
            seen_requests[request_id] = (control_signature, result)
            replay_cache.append((request_id, control_signature, normalized_operation.value, result))
            cursor = after

        assert cursor is not None
        if current.revision > cursor.revision:
            raise ConfigurationError("registered Harness is ahead of the durable control journal")
        if current.revision == cursor.revision and current != cursor:
            raise ConfigurationError(
                "registered Harness snapshot conflicts with the durable control journal"
            )
        if current.revision < cursor.revision:
            restore = getattr(harness, "restore_durable_snapshot", None)
            if not callable(restore):
                raise ConfigurationError(
                    "Harness adapter cannot restore the durable session snapshot"
                )
            try:
                restore(cursor)
            except Exception as exc:
                raise ConfigurationError(
                    "Harness adapter rejected the durable session snapshot"
                ) from exc
        # Reject duplicate request ids with different semantics before exposing
        # any replay cache.  This also catches a tampered-but-hash-valid event
        # sequence produced by an untrusted migration.
        for request_id, signature, operation, result in replay_cache:
            self._harness_control_cache[(identity.claim_id, request_id)] = (
                signature,
                operation,
                result,
            )

    def register_harness(self, harness: Any) -> Any:
        """Bind one Harness session to an exact live Scheduler Attempt.

        A Harness is an execution unit, not a source of semantic truth.  The
        bridge therefore requires an already-active Claim and an exact
        Attempt/graph/epoch identity before the session can be controlled.
        Registration itself is in-memory, but a file-backed AgentOS replays the
        bounded Harness session projection and request idempotency records from
        durable ``HARNESS_CONTROL`` events before exposing the binding.  This
        does not resume arbitrary callback/model state.
        """

        from .harness import HarnessSessionAdapter, HarnessSessionSnapshot

        if self._read_only:
            raise ConfigurationError("read-only AgentOS cannot register Harness sessions")
        if not isinstance(harness, HarnessSessionAdapter):
            raise ConfigurationError("harness must implement the HarnessSessionAdapter protocol")
        snapshot = harness.snapshot
        if not isinstance(snapshot, HarnessSessionSnapshot):
            raise ConfigurationError("Harness snapshot must be a HarnessSessionSnapshot")
        identity = snapshot.identity
        claim = next(
            (item for item in self._scheduler.claims if item.claim_id == identity.claim_id),
            None,
        )
        if claim is None or getattr(claim.state, "value", claim.state) != "active":
            raise ConfigurationError(
                f"Harness claim {identity.claim_id!r} is not an active Scheduler Claim"
            )
        attempt = self._scheduler.attempt_for_claim(identity.claim_id)
        if attempt is None:
            raise ConfigurationError(
                f"Harness claim {identity.claim_id!r} has no Scheduler Attempt"
            )
        expected = (
            identity.graph_id,
            identity.graph_version,
            identity.task_id,
            identity.agent_id,
            identity.claim_id,
            identity.attempt_id,
            identity.semantic_epoch,
        )
        actual = (
            claim.graph_id,
            claim.graph_version,
            claim.task_id,
            claim.agent_id,
            claim.claim_id,
            attempt.attempt_id,
            attempt.semantic_epoch,
        )
        if expected != actual:
            raise ConfigurationError(
                "Harness identity does not match the exact Scheduler Claim/Attempt"
            )
        existing = self._harnesses_by_session.get(identity.session_id)
        if existing is not None and existing is not harness:
            raise ConfigurationError(
                f"Harness session {identity.session_id!r} is already registered"
            )
        existing_session = self._harness_session_by_claim.get(identity.claim_id)
        if existing_session is not None and existing_session != identity.session_id:
            raise ConfigurationError(
                f"Claim {identity.claim_id!r} already has Harness session {existing_session!r}"
            )
        # Perform durable replay only after duplicate/ownership checks.  A
        # failed registration must not mutate an unbound replacement adapter's
        # logical snapshot as a side effect.
        self._restore_harness_from_durable_history(harness, identity)
        self._harnesses_by_session[identity.session_id] = harness
        self._harness_session_by_claim[identity.claim_id] = identity.session_id
        return harness

    def unregister_harness(
        self,
        session_id: str,
        *,
        claim_id: str | None = None,
        close_adapter: bool = True,
    ) -> bool:
        """Remove a Harness binding without changing Scheduler ownership.

        External Harness adapters commonly own processes, sockets, or watcher
        tasks. They are closed by default after the exact binding is detached.
        Callers performing a non-terminal transfer may explicitly opt out.
        """

        normalized = str(session_id).strip()
        harness = self._harnesses_by_session.get(normalized)
        if harness is None:
            return False
        identity = getattr(getattr(harness, "snapshot", None), "identity", None)
        if claim_id is not None and identity is not None and identity.claim_id != str(claim_id):
            return False
        self._harnesses_by_session.pop(normalized, None)
        if (
            identity is not None
            and self._harness_session_by_claim.get(identity.claim_id) == normalized
        ):
            self._harness_session_by_claim.pop(identity.claim_id, None)
        if close_adapter:
            closer = getattr(harness, "close", None)
            if callable(closer):
                try:
                    closer()
                except Exception as exc:
                    raise ConfigurationError(
                        f"Harness session {normalized!r} detached but close failed",
                        cause=exc,
                    ) from exc
        return True

    def harness_for_claim(self, claim_id: str) -> Any | None:
        """Return the registered Harness for an exact Claim, if any."""

        session_id = self._harness_session_by_claim.get(str(claim_id).strip())
        return self._harnesses_by_session.get(session_id) if session_id else None

    async def control_harness(
        self,
        claim_id: str,
        operation: Any,
        *,
        request_id: str | None = None,
        reason: str = "",
        target_graph_version: int | None = None,
        target_semantic_epoch: int | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> Any:
        """Send one exact-identity control request to a registered Harness.

        The request is fenced against the current Claim and Attempt before
        entering user/Harness code.  A successful Harness transition is
        journaled as a bounded ``HARNESS_CONTROL`` event; it does not
        complete/release a Claim and does not publish VPG Evidence.
        """

        from lhos.runtimes.multi_agent.events import SchedulerEventType, record_event

        from .harness import (
            HarnessControlRequest,
            HarnessOperation,
            HarnessResultStatus,
            HarnessSessionSnapshot,
        )

        normalized_claim = str(claim_id).strip()
        if not normalized_claim:
            raise ConfigurationError("claim_id must be non-empty")
        harness = self.harness_for_claim(normalized_claim)
        if harness is None:
            raise ConfigurationError(f"no Harness is registered for claim {normalized_claim!r}")
        current_snapshot = harness.snapshot
        if not isinstance(current_snapshot, HarnessSessionSnapshot):
            raise ConfigurationError("registered Harness returned an invalid snapshot")
        claim = next(
            (item for item in self._scheduler.claims if item.claim_id == normalized_claim),
            None,
        )
        attempt = self._scheduler.attempt_for_claim(normalized_claim)
        if (
            claim is None
            or getattr(claim.state, "value", claim.state) != "active"
            or attempt is None
            or current_snapshot.identity.claim_id != normalized_claim
            or current_snapshot.identity.attempt_id != attempt.attempt_id
            or current_snapshot.identity.graph_id != claim.graph_id
            or current_snapshot.identity.task_id != claim.task_id
            or current_snapshot.identity.agent_id != claim.agent_id
            or current_snapshot.identity.graph_version != claim.graph_version
            or current_snapshot.identity.semantic_epoch != attempt.semantic_epoch
        ):
            raise ConfigurationError(
                "Harness control fence failed: Claim/Attempt/session identity changed"
            )
        try:
            normalized_operation = HarnessOperation(operation)
        except ValueError as exc:
            raise ConfigurationError(f"unsupported Harness operation: {operation!r}") from exc

        # Harness REBASE/PREEMPT changes the Harness lifecycle/identity.  The
        # Scheduler currently has no atomic claim-to-Harness handoff API, so
        # allowing either operation here would leave an ACTIVE Claim paired
        # with a detached or advanced session.  Keep those operations on the
        # explicit interrupt/ownership paths until that transaction exists.
        if normalized_operation in {HarnessOperation.REBASE, HarnessOperation.PREEMPT}:
            raise ConfigurationError(
                f"Harness {normalized_operation.value} requires an explicit "
                "Scheduler/Kernel handoff; use deliver_interrupt(...) for "
                "cooperative SDK attempts"
            )

        # Treat ``request_id`` as the idempotency key at the OS↔Harness
        # boundary.  The live Claim/Attempt fence above is still checked on
        # every replay, but a retried request must not be rebuilt against the
        # Harness' *new* revision (which would spuriously look like a
        # conflicting request).  Reusing the same id for another operation is
        # rejected rather than silently replaying the wrong control action.
        control_signature = hashlib.sha256(
            json.dumps(
                {
                    "operation": normalized_operation.value,
                    "reason": str(reason).strip(),
                    "target_graph_version": target_graph_version,
                    "target_semantic_epoch": target_semantic_epoch,
                    "payload": dict(payload or {}),
                },
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode()
        ).hexdigest()
        if request_id:
            cached = self._harness_control_cache.get((normalized_claim, str(request_id)))
            if cached is not None:
                cached_signature, cached_operation, cached_result = cached
                if cached_signature != control_signature:
                    raise ConfigurationError(
                        f"Harness request_id {request_id!r} was already used for "
                        f"a different {cached_operation!r} control request"
                    )
                cached_after = getattr(cached_result, "after", None)
                live_snapshot = getattr(harness, "snapshot", None)
                if cached_after is not None and live_snapshot != cached_after:
                    raise ConfigurationError(
                        f"Harness request_id {request_id!r} was already used for "
                        "a control request from an older session revision"
                    )
                return cached_result

        # Build through the adapter when possible so checkpoint fencing and
        # adapter-specific request construction stay in one place.
        make_request = getattr(harness, "make_request", None)
        if callable(make_request):
            request = make_request(
                normalized_operation,
                request_id=request_id,
                reason=reason,
                target_graph_version=target_graph_version,
                target_semantic_epoch=target_semantic_epoch,
                payload=dict(payload or {}),
            )
        else:
            expected_checkpoint_id = (
                current_snapshot.checkpoint_id
                if normalized_operation.value in {"continue", "rebase"}
                and current_snapshot.state.value == "checkpointed"
                else None
            )
            fallback_request_id = request_id or (
                "harness-request-"
                + hashlib.sha256(
                    f"{normalized_claim}|{current_snapshot.identity.session_id}|"
                    f"{current_snapshot.revision}|{normalized_operation.value}".encode()
                ).hexdigest()[:32]
            )
            request = HarnessControlRequest(
                request_id=fallback_request_id,
                operation=normalized_operation,
                session=current_snapshot.identity,
                expected_revision=current_snapshot.revision,
                expected_checkpoint_id=expected_checkpoint_id,
                target_graph_version=target_graph_version,
                target_semantic_epoch=target_semantic_epoch,
                reason=reason,
                payload=dict(payload or {}),
            )
        result = await harness.control(request)
        if not hasattr(result, "status") or not hasattr(result, "after"):
            raise ConfigurationError("Harness control returned an invalid result")

        after = result.after
        status = getattr(result.status, "value", str(result.status))
        event_key = "|".join(
            (
                normalized_claim,
                request.fingerprint(),
                status,
                str(getattr(after, "revision", "")),
                str(getattr(getattr(after, "identity", None), "graph_version", "")),
                str(getattr(getattr(after, "identity", None), "semantic_epoch", "")),
            )
        )
        event_id = "harness-control-" + hashlib.sha256(event_key.encode()).hexdigest()
        metadata = {
            "schema_version": "harness-session.v1",
            "request_id": request.request_id,
            "request_fingerprint": request.fingerprint(),
            "control_signature": control_signature,
            "operation": normalized_operation.value,
            "status": status,
            "session_id": current_snapshot.identity.session_id,
            "before_revision": current_snapshot.revision,
            "after_revision": getattr(after, "revision", current_snapshot.revision),
            "before_state": current_snapshot.state.value,
            "after_state": getattr(getattr(after, "state", None), "value", ""),
            # Bounded snapshots are metadata-only (identity, revision, state,
            # checkpoint id, and progress).  They make the Harness logical
            # session replayable without persisting prompts, outputs, or
            # arbitrary callback memory.
            "before_snapshot": current_snapshot.model_dump(mode="json"),
            "after_snapshot": after.model_dump(mode="json"),
        }
        existing = next(
            (event for event in self._scheduler.events if event.event_id == event_id),
            None,
        )
        if existing is None:
            self._scheduler.record_event(
                record_event(
                    event_id=event_id,
                    event_type=SchedulerEventType.HARNESS_CONTROL,
                    graph_id=claim.graph_id,
                    task_id=claim.task_id,
                    agent_id=claim.agent_id,
                    claim_id=claim.claim_id,
                    attempt_id=attempt.attempt_id,
                    graph_version=claim.graph_version,
                    reason=str(getattr(result, "message", "") or reason)[:512],
                    metadata=metadata,
                )
            )
        elif existing.metadata != metadata:
            raise ConfigurationError(f"conflicting Harness control event {event_id!r}")
        # Explicitly keep the ownership projection untouched.  Callers must
        # use the normal Scheduler/VPG paths after a Harness transition.
        if status not in {item.value for item in HarnessResultStatus}:
            raise ConfigurationError(f"unknown Harness control status {status!r}")
        if request.request_id:
            self._harness_control_cache[(normalized_claim, request.request_id)] = (
                control_signature,
                normalized_operation.value,
                result,
            )
        return result

    def control_harness_sync(self, claim_id: str, operation: Any, **kwargs: Any) -> Any:
        """Synchronous wrapper for :meth:`control_harness` outside an event loop."""

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.control_harness(claim_id, operation, **kwargs))
        raise RuntimeError(
            "control_harness_sync cannot run inside an active event loop; "
            "await control_harness(...)"
        )

    def _release_unexecuted_dispatches(
        self,
        gid: str,
        dispatches: list[dict[str, Any]],
        *,
        reason: str,
    ) -> None:
        """Release only the exact still-live claims in an abandoned sync batch."""
        for dispatch in dispatches:
            task_id = str(dispatch.get("task_id", "")).strip()
            claim_id = dispatch.get("claim_id", "")
            if task_id and claim_id:
                self._release_after_failure(
                    gid,
                    task_id,
                    claim_id,
                    reason=reason,
                )

    def _online_claim_terminal(
        self,
        gid: str,
        task_id: str,
        claim_id: str,
    ) -> bool:
        """Return whether the exact online-epoch Claim is already terminal."""

        terminal_states = {"released", "lost", "completed"}
        for claim in self._scheduler.claims:
            if (
                getattr(claim, "graph_id", None) == gid
                and getattr(claim, "task_id", None) == task_id
                and getattr(claim, "claim_id", None) == claim_id
            ):
                return getattr(getattr(claim, "state", None), "value", None) in (terminal_states)
        return False

    def _release_online_dispatches(
        self,
        gid: str,
        dispatches: Iterable[Any],
        *,
        reason: str,
    ) -> tuple[
        tuple[str, ...],
        tuple[str, ...],
        tuple[str, ...],
        tuple[str, ...],
    ]:
        """Best-effort exact-Claim cleanup for an online epoch.

        The first three return values are newly released, already-terminal,
        and still-live Claim ids.  The last contains bounded cleanup errors.
        A failed cleanup never silently releases a replacement Claim because
        every release carries ``expected_claim_id``.
        """

        released: list[str] = []
        already_terminal: list[str] = []
        retained: list[str] = []
        errors: list[str] = []
        seen: set[str] = set()
        for item in dispatches:
            task_id = str(getattr(item, "task_id", "")).strip()
            claim_id = str(getattr(item, "claim_id", "")).strip()
            if not task_id or not claim_id or claim_id in seen:
                continue
            seen.add(claim_id)
            did_release = False
            try:
                did_release = self._scheduler.release_task(
                    gid,
                    task_id,
                    reason=reason,
                    retry=True,
                    expected_claim_id=claim_id,
                )
            except Exception as exc:
                errors.append(f"{claim_id}:{_bounded_audit_error(exc)}")
            if did_release:
                released.append(claim_id)
            elif self._online_claim_terminal(gid, task_id, claim_id):
                already_terminal.append(claim_id)
            else:
                retained.append(claim_id)
        return (
            tuple(released),
            tuple(already_terminal),
            tuple(retained),
            tuple(errors),
        )

    def _execute_and_verify(
        self,
        gid: str,
        task_id: str,
        agent_id: str,
        goal: Goal,
        *,
        claim_id: str = "",
        attempt_number: int = 0,
        provider_routing_enabled: bool = False,
        context_manifest_override: ContextManifest | None = None,
        automatic_rebase_decision: AutomaticRebaseDecision | None = None,
        budget_measurement: _BudgetMeasurementContext | None = None,
    ) -> None:
        """Execute a dispatched task and independently attach verified Evidence.

        A task with no verifier/executor stays unverified (VPG-G2/G3): without
        a Verification->Evidence path the SDK must NOT fabricate VERIFIED.
        """
        task = next((t for t in goal.tasks if t.task_id == task_id), None)
        agent = self._agents.get(agent_id)
        claim = self._live_claim(
            gid,
            task_id,
            claim_id=claim_id,
            agent_id=agent_id,
        )
        if claim is None:
            # A late/stale dispatch must not execute and, importantly, must not
            # release a newer owner's claim.
            return
        claim_id = claim.claim_id

        if task is None:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason="missing_task",
            )
            return
        if agent is None:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason="missing_agent",
            )
            return

        try:
            self._mark_execution_started(claim)
        except Exception as e:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason=f"execution_start_failed:{type(e).__name__}",
            )
            return

        try:
            api = resolve_executor_api(task=task, agent=agent)
            context = self._new_execution_context(
                gid,
                task,
                claim_id=claim_id,
                executor_api=api,
                context_manifest_override=context_manifest_override,
                automatic_rebase_decision=automatic_rebase_decision,
            )
            provider_route = None
            provider_context: Any = context
            if provider_routing_enabled:
                provider_route = self._resolve_provider_route(
                    goal,
                    task,
                    graph_id=gid,
                    claim_id=claim_id,
                )
                if provider_route is not None:
                    registry = self._provider_registry
                    if registry is None:
                        raise ProviderRoutingError("provider registry unavailable")
                    provider_context = registry.adapt_context(
                        provider_route,
                        task_id,
                        context,
                    )
        except ConfigurationError:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason="context_snapshot_bind_failed",
            )
            raise
        except _ClaimFenceLost:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason="context_snapshot_bind_lost",
            )
            return
        except Exception as exc:
            # Provider context adaptation is user/provider code executed after
            # the Claim/Context fence.  Any unexpected adapter failure must
            # release only this exact claim before surfacing the error.
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason=f"provider_context_failed:{type(exc).__name__}",
            )
            raise
        executor_outcome: Any = None
        # Monotonic clock only: this measured wall-clock never enters a decision
        # hash, and a monotonic source is immune to wall-clock adjustments.
        executor_started_at = time.monotonic()
        if provider_route is not None:
            try:
                registry = self._provider_registry
                if registry is None:
                    raise ProviderRoutingError("provider registry unavailable")
                executor_outcome = registry.execute_sync(
                    provider_route,
                    task_id,
                    provider_context,
                    agent.executor,
                )
            except Exception as e:
                self._release_after_failure(
                    gid,
                    task_id,
                    claim_id,
                    reason=f"provider_executor_failed:{type(e).__name__}",
                )
                raise
        elif agent.executor is not None:
            try:
                executor_outcome = _invoke_executor(
                    agent.executor,
                    task_id,
                    context=context,
                    executor_api=api,
                )
            except ConfigurationError:
                self._release_after_failure(
                    gid,
                    task_id,
                    claim_id,
                    reason="executor_configuration_error",
                )
                raise
            except Exception as e:
                self._release_after_failure(
                    gid,
                    task_id,
                    claim_id,
                    reason=f"executor_failed:{type(e).__name__}",
                )
                return

        # The executor has completed for this attempt: record what was really
        # spent.  Wall-clock is the authoritative measured monotonic elapsed;
        # token/cost counters are used only when the executor/provider supplied
        # them.  This flows through the existing ComputationCost structure and
        # persists via UsageLedger.record_measured so the MEASURED state stops
        # being dead code.
        if budget_measurement is not None:
            from .compute_calibration import computation_cost_to_usage_vector

            elapsed_ms = max(0, int((time.monotonic() - executor_started_at) * 1000))
            declared = budget_measurement.declared_by_task.get(task_id)
            if declared is not None:
                measured_cost = _extract_measured_computation_cost(executor_outcome)
                measured_usage = computation_cost_to_usage_vector(
                    measured_cost, elapsed_ms=elapsed_ms
                )
                self._usage_ledger = _record_measured_attempt(
                    self._usage_ledger,
                    goal_id=budget_measurement.goal_id,
                    task_id=task_id,
                    attempt_id=(claim_id or f"{task_id}#attempt-{attempt_number}"),
                    declared=declared,
                    measured=measured_usage,
                )

        # Operational completion is a separate durable milestone from the
        # later semantic Evidence commit.  Record it before invoking an
        # independent verifier so the verifier can audit the exact boundary.
        try:
            marked = self._scheduler.mark_execution_operationally_succeeded(claim_id)
            if marked is None:
                return
        except Exception as e:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason=f"execution_completion_failed:{type(e).__name__}",
            )
            return

        # Task.verify is the independent semantic authority when present.
        # With no Agent.executor, retaining Task.verify as the combined legacy
        # executor/verifier preserves the existing SDK API.  An executor that
        # directly returns VerificationOutcome is also accepted when no
        # separate verifier was supplied, matching Agent's documented API.
        try:
            if provider_route is not None:
                registry = self._provider_registry
                if registry is None:
                    raise ProviderRoutingError("provider registry unavailable")
                outcome = registry.verify_sync(
                    provider_route,
                    task_id,
                    provider_context,
                    executor_outcome,
                    task.verify,
                )
            elif task.verify is not None:
                outcome = _invoke_verifier_sync(
                    task.verify,
                    task_id=task_id,
                    context=context,
                    executor_api=api,
                )
            elif isinstance(executor_outcome, VerificationOutcome):
                outcome = executor_outcome
            else:
                self._release_after_failure(
                    gid,
                    task_id,
                    claim_id,
                    reason="missing_verifier",
                )
                return
        except ConfigurationError:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason="verifier_configuration_error",
            )
            raise
        except Exception as e:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason=f"verifier_failed:{type(e).__name__}",
            )
            return

        if not isinstance(outcome, VerificationOutcome):
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason="invalid_verifier_outcome",
            )
            return
        if not outcome.passed:
            # FAIL/INCONCLUSIVE => no VERIFIED; record failure
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason="verification_failed",
            )
            return

        # Fence the result again after arbitrary user code ran.  A result from
        # an expired/lost/reassigned claim must never enter Facts or the VPG.
        if (
            self._live_claim(
                gid,
                task_id,
                claim_id=claim_id,
                agent_id=agent_id,
            )
            is None
        ):
            # A semantic interrupt can quarantine the attempt while the
            # verifier is returning.  Do not leave the old ACTIVE claim
            # stranded: release only this claim, preserving a replacement
            # owner's fence if one already won the race.
            attempt_now = self._scheduler.attempt_for_claim(claim_id)
            attempt_state = getattr(
                getattr(attempt_now, "state", None),
                "value",
                "",
            )
            reason = (
                "stale_cognition:commit_fence_lost"
                if attempt_state == "stale_cognition"
                else "claim_fence_lost"
            )
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason=reason,
            )
            return

        coverage_report = build_coverage_report(task, context, executor_api=api)
        try:
            committed = self._commit_verified_outcome(
                gid,
                task_id,
                agent_id,
                outcome,
                attempt_number=attempt_number,
                claim_id=claim_id,
                task=task,
                coverage_report=coverage_report,
                provenance_context=context,
            )
            if not committed:
                return
        except _ClaimFenceLost:
            # Ownership was lost while user/fact-store code was running. This
            # stale result must not release or otherwise mutate a newer claim.
            # The release helper is claim-fenced, so it safely cleans up an
            # ACTIVE stale attempt while remaining a no-op after reassignment.
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason="stale_cognition:commit_fence_lost",
            )
            return
        except Exception as e:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason=f"evidence_attachment_failed:{type(e).__name__}",
            )
            raise VerificationError(
                f"failed to attach Evidence for task {task_id!r}",
                cause=e,
            ) from e

    def _commit_verified_outcome(
        self,
        gid: str,
        task_id: str,
        agent_id: str,
        outcome: VerificationOutcome,
        *,
        attempt_number: int,
        claim_id: str,
        task: Any | None = None,
        coverage_report: CoverageReport | None = None,
        provenance_context: ExecutionContext | None = None,
    ) -> bool:
        """Commit one PASS outcome and let VPG observation complete its claim."""
        # A live Kernel lease is not sufficient to authorize semantic commit.
        # The Scheduler Attempt is a second lifecycle fence: once cognition
        # is quarantined (or the attempt otherwise reaches a terminal state),
        # a late verifier must not publish Facts/Evidence under the old
        # reasoning snapshot.
        if claim_id:
            self._require_live_claim(
                gid,
                task_id,
                claim_id=claim_id,
                agent_id=agent_id,
            )
        if task is not None and coverage_report is not None:
            enforce_verification_coverage(
                task,
                coverage_report,
                executor_api=getattr(provenance_context, "executor_api", None),
            )
        # Seal the canonical coverage digest on the exact execution attempt
        # before any Action/Evidence side effect is published.  The Scheduler
        # persists this value with the attempt and later requires the Evidence
        # digest to match it, so a stale worker cannot invent a fresh-looking
        # provenance report after ownership changes.  Legacy test doubles that
        # do not expose this bounded hook retain the pre-digest compatibility
        # path.
        if claim_id and coverage_report is not None:
            bind_digest = getattr(self._scheduler, "bind_attempt_provenance", None)
            if callable(bind_digest):
                bound = bind_digest(claim_id, coverage_report.report_hash)
                if bound is False:
                    raise _ClaimFenceLost

        # Seal the exact cognition/read-set observed by this attempt before
        # any output fact or Evidence side effect is published.  The durable
        # GraphStore repeats the read-guard check inside its writer
        # transaction; this early check gives callers a deterministic
        # fail-fast path and avoids publishing a new output fact for an
        # already-obsolete cognition snapshot.
        agent_snapshot = self._bind_agent_snapshot_for_commit(
            claim_id=claim_id,
            provenance_context=provenance_context,
        )
        try:
            read_guards = self._prepare_read_guards(
                task=task,
                snapshot=agent_snapshot,
            )
            self._preflight_read_guards(read_guards)
            self._preflight_mediated_read_sets(provenance_context)
        except VPGError as exc:
            if exc.code in {VPGCode.STALE_COGNITION, VPGCode.READ_SET_UNAVAILABLE}:
                self._quarantine_stale_cognition(
                    gid,
                    task_id,
                    claim_id,
                    reason=f"{exc.code.value.lower()}:{exc}",
                )
                return False
            raise

        latest_version = self._facts.latest(outcome.artifact_id)
        if latest_version is not None and latest_version > outcome.version:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason=(
                    "stale_verifier_outcome:"
                    f"{outcome.artifact_id}@{outcome.version}"
                    f"<latest@{latest_version}"
                ),
            )
            return False
        produced_pid = self._agent_pid.get(agent_id) or self._owner_pid()
        if latest_version is None or latest_version < outcome.version:
            self._require_live_claim(
                gid,
                task_id,
                claim_id=claim_id,
                agent_id=agent_id,
            )
            self._facts.add_version(
                outcome.artifact_id,
                outcome.version,
                outcome.content or "",
            )
            self._require_live_claim(
                gid,
                task_id,
                claim_id=claim_id,
                agent_id=agent_id,
            )
        try:
            self._attach_evidence(
                gid,
                task_id,
                agent_id,
                outcome,
                produced_pid,
                attempt_number=attempt_number,
                claim_id=claim_id,
                coverage_report=coverage_report,
                provenance_context=provenance_context,
                read_guards=read_guards,
            )
        except VPGError as exc:
            if exc.code in {VPGCode.STALE_COGNITION, VPGCode.READ_SET_UNAVAILABLE}:
                self._quarantine_stale_cognition(
                    gid,
                    task_id,
                    claim_id,
                    reason=f"{exc.code.value.lower()}:{exc}",
                )
                return False
            raise
        self._scheduler.observe_vpg(gid)
        return True

    def _quarantine_stale_cognition(
        self,
        gid: str,
        task_id: str,
        claim_id: str,
        *,
        reason: str,
    ) -> None:
        """Mark and release an attempt whose cognition is no longer current."""

        if not claim_id:
            raise _ClaimFenceLost
        mark_stale = getattr(self._scheduler, "mark_stale_cognition", None)
        if not callable(mark_stale) or mark_stale(claim_id, reason) is False:
            raise _ClaimFenceLost
        # ``release_task`` preserves the STALE_COGNITION marker when the
        # reason starts with ``stale_cognition``.  Fence cleanup to this exact
        # claim so a replacement owner cannot be touched.
        self._release_after_failure(
            gid,
            task_id,
            claim_id,
            reason=f"stale_cognition:{reason}",
        )

    def _bind_agent_snapshot_for_commit(
        self,
        *,
        claim_id: str,
        provenance_context: ExecutionContext | None,
    ) -> AgentSnapshot | None:
        """Build and durably bind one immutable AgentSnapshot to the attempt."""

        if not claim_id:
            return None
        attempt = self._scheduler.attempt_for_claim(claim_id)
        if attempt is None:
            raise _ClaimFenceLost
        bind = getattr(self._scheduler, "bind_agent_snapshot", None)
        if not callable(bind):
            # Older injected scheduler doubles are allowed to keep the
            # compatibility path; the built-in SchedulerSession implements the
            # binding API and therefore receives the stronger guarantee.
            return None
        try:
            snapshot = AgentSnapshot.from_attempt(
                attempt,
                execution_context=provenance_context,
                captured_at=None,
            )
        except (TypeError, ValueError) as exc:
            raise VerificationError(
                "cannot construct durable AgentSnapshot for semantic commit",
                cause=exc,
            ) from exc
        if not bind(claim_id, snapshot):
            raise _ClaimFenceLost
        return snapshot

    def _prepare_read_guards(
        self,
        *,
        task: Any | None,
        snapshot: AgentSnapshot | None,
    ) -> tuple[ArtifactVersionBinding, ...]:
        """Convert exact artifact reads into immutable VPG freshness guards.

        Unknown/partial reads are deliberately not guessed.  A strict
        provenance policy fails closed; audit/legacy policies retain their
        compatibility behavior but do not claim universal stale-cognition
        coverage for those observations.
        """

        if snapshot is None:
            return ()
        strict = getattr(getattr(task, "provenance_policy", None), "value", None)
        strict = (
            strict == "strict" or str(strict or getattr(task, "provenance_policy", "")) == "strict"
        )
        # ``secure_mode`` is a runtime-wide safety boundary, not merely an
        # effect-gateway setting.  An Agent can still observe an unidentifiable
        # input through ``context_v1`` (for example via
        # ``ctx.observe_unknown(...)`` or a malformed provenance event).  Such
        # cognition has no immutable version/hash guard and therefore must not
        # be promoted to VERIFIED in secure mode, even when the task retained
        # the legacy/audit compatibility policy.  Route it through the
        # existing READ_SET_UNAVAILABLE -> stale-cognition quarantine path.
        fail_closed = bool(strict or self._secure_mode)
        guards: dict[tuple[str, str, int, str], ArtifactVersionBinding] = {}
        unknown: list[str] = []
        for binding in snapshot.read_set:
            if not binding.known:
                unknown.append(binding.resource_uri or binding.artifact_id or "<unknown>")
                continue
            artifact_id = str(binding.artifact_id or "").strip().removeprefix("vpg://")
            # Context/provenance adapters are allowed to emit only the
            # authority-canonical VPG URI.  Derive the artifact identity in
            # that narrow case, matching ``plan_automatic_rebase``.  Never
            # infer an artifact id from arbitrary external/workspace URIs:
            # those schemes may have aliases or escaping rules that this
            # commit-time guard cannot prove.
            resource_uri = str(binding.resource_uri or "").strip()
            if not artifact_id and resource_uri.startswith("vpg://"):
                artifact_id = resource_uri.removeprefix("vpg://").strip("/")
            version = binding.version
            content_hash = str(binding.content_hash or "").strip().lower()
            if not artifact_id or version is None or version < 1 or not content_hash:
                unknown.append(binding.resource_uri or artifact_id or "<unidentified>")
                continue
            canonical_uri = resource_uri or f"vpg://{artifact_id}"
            guard = ArtifactVersionBinding(
                canonical_uri=canonical_uri,
                artifact_id=artifact_id,
                version=int(version),
                content_hash=content_hash,
            )
            guards[(guard.canonical_uri, guard.artifact_id, guard.version, guard.content_hash)] = (
                guard
            )
        if fail_closed and unknown:
            raise VPGError(
                VPGCode.READ_SET_UNAVAILABLE,
                "cognition freshness requires identifiable artifact reads: "
                + ", ".join(sorted(set(unknown))),
            )
        return tuple(guards[key] for key in sorted(guards))

    def _preflight_read_guards(
        self,
        guards: tuple[ArtifactVersionBinding, ...],
    ) -> None:
        """Check guards against the host FactsProvider before output staging."""

        if not guards:
            return
        latest_fn = getattr(self._facts, "latest", None)
        hash_fn = getattr(self._facts, "read_hash", None)
        if not callable(latest_fn) or not callable(hash_fn):
            raise VPGError(
                VPGCode.READ_SET_UNAVAILABLE,
                "FactsProvider cannot authoritatively validate Agent read-set freshness",
            )
        for guard in guards:
            latest = latest_fn(guard.artifact_id)
            if latest != guard.version:
                raise VPGError(
                    VPGCode.STALE_COGNITION,
                    f"read binding {guard.canonical_uri}@{guard.version} is stale; "
                    f"current is @{latest}",
                )
            current_hash = hash_fn(
                "sdk-cognition",
                guard.canonical_uri,
                guard.version,
            ) or hash_fn("sdk-cognition", guard.artifact_id, guard.version)
            if current_hash is None or str(current_hash).lower() != guard.content_hash:
                raise VPGError(
                    VPGCode.STALE_COGNITION,
                    f"read binding {guard.canonical_uri}@{guard.version} hash changed",
                )

    def _preflight_mediated_read_sets(
        self,
        context: ExecutionContext | None,
    ) -> None:
        """Point-in-time check of bounded mediated workspace read-sets.

        Artifact/Facts versions remain the semantic authority. This additive
        guard closes a narrower gap: bytes read through a registered
        ``WorkspaceProvenanceGateway`` may be changed directly before commit
        without an observer having published a new ArtifactVersion yet.

        The check is intentionally bounded and fail-closed. It is not a
        filesystem lock or cross-plane transaction, so a mutation after this
        point can still race the later Evidence commit.
        """

        if context is None:
            return
        gateways = tuple(getattr(context, "_workspace_provenance_gateways", ()) or ())
        if len(gateways) > 16:
            raise VPGError(
                VPGCode.READ_SET_UNAVAILABLE,
                "mediated workspace validation exceeds the 16-gateway commit limit",
            )

        seen: set[str] = set()
        for gateway in gateways:
            gateway_id = str(getattr(gateway, "gateway_id", "")).strip()
            identity = gateway_id or f"object:{id(gateway)}"
            if identity in seen:
                continue
            seen.add(identity)
            validate = getattr(gateway, "validate_read_set_current", None)
            if not callable(validate):
                raise VPGError(
                    VPGCode.READ_SET_UNAVAILABLE,
                    "registered workspace provenance gateway has no read-set validator",
                )
            try:
                report = validate(max_resources=256)
            except Exception as exc:
                raise VPGError(
                    VPGCode.READ_SET_UNAVAILABLE,
                    "workspace read-set validation failed: " + _bounded_audit_error(exc),
                ) from exc
            if bool(getattr(report, "current", False)):
                continue

            stale = tuple(getattr(report, "stale_resources", ()) or ())
            unavailable = tuple(getattr(report, "unavailable_resources", ()) or ())
            unknown = tuple(getattr(report, "unknown_resources", ()) or ())
            truncated = bool(getattr(report, "truncated", False))
            details = tuple(
                str(value).strip()
                for value in (*stale, *unavailable, *unknown)
                if str(value).strip()
            )[:8]
            code = (
                VPGCode.READ_SET_UNAVAILABLE
                if unavailable or unknown or truncated
                else VPGCode.STALE_COGNITION
            )
            reason = "workspace read-set changed before semantic commit"
            if details:
                reason += ": " + ", ".join(details)
            if truncated:
                reason += " (validation limit exceeded)"
            raise VPGError(code, reason)

    def _mark_execution_started(self, claim: Any) -> None:
        """Move the matching Attempt to RUNNING before invoking user code."""
        core = getattr(self._scheduler, "_s", None)
        start = getattr(core, "mark_execution_started", None)
        if callable(start):
            start(claim)
            return
        # Backward-compatible fallback while older scheduler cores lack the
        # public transition helper.
        attempt = self._scheduler.attempt_for_claim(claim.claim_id)
        manager = getattr(core, "_attempts_", None)
        mark_running = getattr(manager, "mark_running", None)
        if attempt is not None and callable(mark_running):
            mark_running(attempt)

    def _live_claim(
        self,
        gid: str,
        task_id: str,
        *,
        claim_id: str,
        agent_id: str,
    ) -> Any | None:
        """Return the exact currently-owned claim only while its lease is live."""
        claim = self._scheduler.active_claim_for_task(task_id, gid)
        if claim is None:
            return None
        if claim_id and claim.claim_id != claim_id:
            return None
        if claim.agent_id != agent_id:
            return None
        if getattr(getattr(claim, "state", None), "value", None) != "active":
            return None
        lease_id = getattr(claim, "lease_id", None)
        if not lease_id:
            return None

        core = getattr(self._scheduler, "_s", None)
        leases = getattr(core, "_leases", None)
        get_lease = getattr(leases, "get", None)
        is_active = getattr(leases, "is_lease_active", None)
        if not callable(get_lease) or not callable(is_active):
            return None
        try:
            lease = get_lease(lease_id)
        except Exception:
            return None
        if not is_active(lease):
            return None
        if getattr(lease, "owner_pid", claim.process_id) != claim.process_id:
            return None
        if getattr(lease, "resource_id", claim.lease_resource) != claim.lease_resource:
            return None
        if getattr(claim, "lease_owner_pid", None) != getattr(lease, "owner_pid", None):
            return None
        if getattr(claim, "lease_fencing_token", None) != getattr(lease, "fencing_token", None):
            return None
        # Claims intentionally remain ACTIVE while a stale cognition event is
        # being observed/rebased.  Fence on the Attempt state too; otherwise
        # a worker with a still-live lease could submit a late semantic result.
        attempt_for_claim = getattr(self._scheduler, "attempt_for_claim", None)
        if callable(attempt_for_claim):
            try:
                attempt = attempt_for_claim(claim.claim_id)
            except Exception:
                return None
            if attempt is not None:
                attempt_state = getattr(getattr(attempt, "state", None), "value", None)
                if attempt_state not in {
                    "dispatched",
                    "running",
                    "succeeded_operationally",
                }:
                    return None
        return claim

    def _require_live_claim(
        self,
        gid: str,
        task_id: str,
        *,
        claim_id: str,
        agent_id: str,
    ) -> Any:
        """Return the exact live claim or stop a stale execution commit."""
        claim = self._live_claim(
            gid,
            task_id,
            claim_id=claim_id,
            agent_id=agent_id,
        )
        if claim is None:
            raise _ClaimFenceLost
        return claim

    def _claim_commit_guard(
        self,
        gid: str,
        task_id: str,
        *,
        claim_id: str,
        agent_id: str,
    ) -> LeaseCommitGuard:
        """Freeze the exact live claim lease generation for GraphStore CAS."""

        claim = self._require_live_claim(
            gid,
            task_id,
            claim_id=claim_id,
            agent_id=agent_id,
        )
        if not claim.lease_id or not claim.lease_owner_pid or claim.lease_fencing_token is None:
            raise _ClaimFenceLost
        core = getattr(self._scheduler, "_s", None)
        leases = getattr(core, "_leases", None)
        get_lease = getattr(leases, "get", None)
        is_active = getattr(leases, "is_lease_active", None)
        if not callable(get_lease) or not callable(is_active):
            raise _ClaimFenceLost
        lease = get_lease(claim.lease_id)
        if (
            lease is None
            or not is_active(lease)
            or getattr(lease, "resource_id", None) != claim.lease_resource
            or getattr(lease, "owner_pid", None) != claim.lease_owner_pid
            or getattr(lease, "fencing_token", None) != claim.lease_fencing_token
            or getattr(lease, "expires_at", None) is None
        ):
            raise _ClaimFenceLost
        return LeaseCommitGuard(
            lease_id=claim.lease_id,
            resource_id=claim.lease_resource,
            owner_pid=claim.lease_owner_pid,
            fencing_token=claim.lease_fencing_token,
            expires_at=lease.expires_at,
        )

    def _release_after_failure(
        self,
        gid: str,
        task_id: str,
        claim_id: str,
        *,
        reason: str,
    ) -> None:
        """Release only this execution's claim; never clobber a reassignment."""
        current = self._scheduler.active_claim_for_task(task_id, gid)
        if current is None or current.claim_id != claim_id:
            return
        try:
            release = self._scheduler.release_task
            try:
                signature = inspect.signature(release)
                supports_fence = "expected_claim_id" in signature.parameters or any(
                    parameter.kind == inspect.Parameter.VAR_KEYWORD
                    for parameter in signature.parameters.values()
                )
            except (TypeError, ValueError):
                supports_fence = True
            kwargs: dict[str, Any] = {"reason": reason, "retry": True}
            if supports_fence:
                kwargs["expected_claim_id"] = claim_id
            release(gid, task_id, **kwargs)
        except Exception:
            # Best-effort reconciliation is safer than allowing a cleanup
            # exception to hide the executor/verifier/evidence root cause.
            with suppress(Exception):
                self._scheduler.reconcile()

    def _attach_evidence(
        self,
        gid: str,
        task_id: str,
        agent_id: str,
        outcome: VerificationOutcome,
        pid: str,
        *,
        attempt_number: int = 0,
        claim_id: str = "",
        coverage_report: CoverageReport | None = None,
        provenance_context: ExecutionContext | None = None,
        read_guards: tuple[ArtifactVersionBinding, ...] = (),
    ) -> None:
        def fence() -> None:
            if claim_id:
                self._require_live_claim(
                    gid,
                    task_id,
                    claim_id=claim_id,
                    agent_id=agent_id,
                )

        suffix = "" if attempt_number == 0 else f"-a{attempt_number}"
        # Graph-local task ids are intentionally reusable across independent
        # Goals.  Every durable identity emitted by the SDK must therefore
        # include the graph id; otherwise a task named ``build`` in graph B can
        # reuse graph A's Kernel Action or overwrite A's VPG projection rows.
        # The attempt suffix keeps retries distinct while remaining
        # deterministic/idempotent for replay of the same attempt.
        identity = f"{gid}-{task_id}-{outcome.version}{suffix}"
        vid = f"V-{identity}"
        evid = f"E-{identity}"
        artref_id = f"AR-{identity}"
        action_id = f"sdk-act-{identity}"
        fence()
        self._facts.commit_action(action_id, pid=pid)
        fence()
        binding = ArtifactVersionBinding(
            canonical_uri=f"vpg://{outcome.artifact_id}",
            artifact_id=outcome.artifact_id,
            version=outcome.version,
            content_hash=self._facts.read_hash(pid, outcome.artifact_id, outcome.version) or "",
        )
        attempt = self._scheduler.attempt_for_claim(claim_id) if claim_id else None
        if claim_id and attempt is None:
            raise _ClaimFenceLost
        provenance_digest = (
            getattr(attempt, "provenance_digest", None) if attempt is not None else None
        )
        if provenance_digest is None and coverage_report is not None:
            provenance_digest = coverage_report.report_hash
        context_snapshot_metadata = (
            {}
            if provenance_context is None
            else {
                "snapshot_id": getattr(provenance_context, "context_snapshot_id", ""),
                "manifest_id": getattr(provenance_context, "context_manifest_id", ""),
                "manifest_hash": getattr(provenance_context, "context_manifest_hash", ""),
                "working_set_hash": getattr(provenance_context, "context_working_set_hash", ""),
                "materialized_hash": getattr(provenance_context, "context_materialized_hash", ""),
            }
        )
        if attempt is not None and getattr(attempt, "context_snapshot_id", None):
            expected_context_metadata = {
                "snapshot_id": attempt.context_snapshot_id,
                "manifest_id": attempt.context_manifest_id,
                "manifest_hash": attempt.context_manifest_hash,
                "working_set_hash": attempt.context_working_set_hash,
                "materialized_hash": attempt.context_materialized_hash,
            }
            if context_snapshot_metadata != expected_context_metadata:
                raise _ClaimFenceLost
        cur = self._vpg.get_graph(gid).current_version
        # Artifact pin + Verification + Evidence + all supporting edges are a
        # single atomic graph transition.  The validator processes operations
        # in order against one candidate projection, so newly-added nodes can
        # safely be referenced by later edge operations in the same proposal.
        fence()
        commit_guard = (
            self._claim_commit_guard(
                gid,
                task_id,
                claim_id=claim_id,
                agent_id=agent_id,
            )
            if claim_id
            else None
        )
        try:
            self._vpg.submit_patch(
                GraphPatchProposal(
                    graph_id=gid,
                    expected_graph_version=cur,
                    author_pid=pid,
                    idempotency_key=f"evidence-{identity}",
                    operations=(
                        AddNodeOp(
                            node_id=artref_id,
                            graph_id=gid,
                            node_type="artifact_ref",
                            created_by_pid=pid,
                            canonical_uri=f"vpg://{outcome.artifact_id}",
                            artifact_id=outcome.artifact_id,
                            version=outcome.version,
                            content_hash=self._facts.read_hash(
                                pid, outcome.artifact_id, outcome.version
                            )
                            or "",
                            metadata={"scheduler": {"task_kind": task_id}},
                        ),
                        AddNodeOp(
                            node_id=vid,
                            graph_id=gid,
                            node_type="verification",
                            created_by_pid=pid,
                            verification_kind="command_result",
                            obligation={"kind": "produced_artifact"},
                            source_action_id=action_id,
                            metadata={"scheduler": {"task_kind": task_id}},
                        ),
                        AddNodeOp(
                            node_id=evid,
                            graph_id=gid,
                            node_type="evidence",
                            created_by_pid=pid,
                            evidence_kind="command_result",
                            result="pass",
                            source_verification_id=vid,
                            evidence_source_action_id=action_id,
                            artifact_bindings=(binding,),
                            produced_by_pid=pid,
                            claim_id=claim_id or None,
                            attempt_id=None if attempt is None else attempt.attempt_id,
                            semantic_epoch=None if attempt is None else attempt.semantic_epoch,
                            lease_fencing_token=(
                                None if commit_guard is None else commit_guard.fencing_token
                            ),
                            provenance_digest=provenance_digest,
                            context_snapshot_id=(
                                None if attempt is None else attempt.context_snapshot_id
                            ),
                            context_manifest_id=(
                                None if attempt is None else attempt.context_manifest_id
                            ),
                            context_manifest_hash=(
                                None if attempt is None else attempt.context_manifest_hash
                            ),
                            context_working_set_hash=(
                                None if attempt is None else attempt.context_working_set_hash
                            ),
                            context_materialized_hash=(
                                None if attempt is None else attempt.context_materialized_hash
                            ),
                            source_event_ids=()
                            if provenance_context is None
                            else tuple(event.event_id for event in provenance_context.events),
                            metadata={
                                "scheduler": {"task_kind": task_id},
                                "provenance": (
                                    {}
                                    if coverage_report is None
                                    else coverage_report.model_dump(mode="json")
                                ),
                                "context_snapshot": context_snapshot_metadata,
                            },
                        ),
                        AddEdgeOp(
                            edge_type="produces",
                            source_node_id=task_id,
                            target_node_id=artref_id,
                            created_by_pid=pid,
                        ),
                        AddEdgeOp(
                            edge_type="verifies",
                            source_node_id=vid,
                            target_node_id=task_id,
                            created_by_pid=pid,
                        ),
                        AddEdgeOp(
                            edge_type="produces",
                            source_node_id=vid,
                            target_node_id=evid,
                            created_by_pid=pid,
                        ),
                    ),
                ),
                _commit_guard=commit_guard,
                _read_guards=read_guards,
            )
        except VPGError as exc:
            if exc.code == VPGCode.LEASE_FENCE_LOST:
                raise _ClaimFenceLost from exc
            raise

    # ── result / status ──────────────────────────────────────────────────────
    def result(self, gid: str) -> RunResult:
        nodes, _ = self._vpg.snapshot_projection(gid)
        tasks = {n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "task"}
        task_states = {tid: n.validity.value for tid, n in tasks.items()}
        verified = [t for t, s in task_states.items() if s == "verified"]
        stale = [t for t, s in task_states.items() if s == "stale"]
        ready = [candidate.task_id for candidate in self._vpg.query_ready_frontier(gid)]
        goal_nodes = {n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "goal"}
        goal_node = next(iter(goal_nodes.values()), None)
        owner = {}
        for claim in self._scheduler.claims:
            if claim.graph_id != gid:
                continue
            state = getattr(claim.state, "value", claim.state)
            if state in {"active", "acquiring"} or claim.task_id not in owner:
                owner[claim.task_id] = claim.agent_id
        artifacts = {}
        for aid, vs in self._facts.versions().items():
            for ver in vs:
                artifacts[aid] = (
                    ver,
                    self._facts.read_hash("sdk-observer", aid, ver) or "",
                )
        return RunResult(
            goal_id=gid,
            goal_state=(
                "closed"
                if goal_node is not None
                and getattr(goal_node.lifecycle, "value", goal_node.lifecycle) == "closed"
                else "open"
            ),
            task_states=task_states,
            verified=verified,
            stale=stale,
            ready=ready,
            owner_by_task=owner,
            artifacts=artifacts,
        )

    def status(self, goal: Goal) -> StatusSnapshot:
        self._goals.setdefault(goal.goal_id, goal)
        gid = self._gid_for(goal.goal_id)
        if gid is None:
            gid = self._compile_goal(goal)
        r = self.result(gid)
        return StatusSnapshot(
            goal_id=goal.goal_id,
            version=self._vpg.get_graph(gid).current_version,
            tasks=r.task_states,
            verified=r.verified,
            stale=r.stale,
            ready=r.ready,
            unverified=[t for t, s in r.task_states.items() if s == "unverified"],
            goal_closed=(r.goal_state == "closed"),
            owner_by_task=r.owner_by_task,
        )

    def runtime_state(self, goal: Goal | str) -> GlobalRuntimeState:
        """Return a read-only four-plane runtime-state projection.

        Unlike :meth:`status`, observation never compiles a missing Goal and
        never mutates VPG, Scheduler, Context, or resource state.  Callers may
        pass a compiled ``Goal`` or its id; an uncompiled/unknown Goal fails
        closed with ``ConfigurationError``.
        """

        return build_runtime_state_view(self, goal)

    def waste_projection(self, goal: Goal | str) -> Any:
        """Return a read-only projection of the five wastes for one Goal.

        This is the denominator of the system's objective: verified progress per
        unit of token, time, and cost is meaningless without an observed measure
        of what was wasted.  Dimensions that the durable state cannot support are
        reported unavailable rather than zero.
        """

        from .waste_projection import build_waste_projection

        return build_waste_projection(self, goal)

    def computation_controller(
        self,
        goal: Goal | str,
        *,
        dispatcher: Any | None = None,
        state_provider: Any | None = None,
        observe: Any | None = None,
        reconcile: Any | None = None,
        event_provider: Any | None = None,
        max_parallelism: int = 1,
        conflict_graph: Any | None = None,
        enabled: bool = True,
        initial_epoch: int = 0,
        dispatch_continuations: bool = False,
        dispatch_deferred: bool = False,
    ) -> Any:
        """Create an explicit online-computation control-plane controller.

        The returned :class:`~lhos.sdk.computation_control.OnlineComputationController`
        observes the already-compiled Goal through :meth:`runtime_state` and
        emits bounded ``START``/``CONTINUE``/``REBASE``/``PREEMPT`` proposals.
        It does **not** claim tasks, acquire Kernel Leases, execute user code,
        or publish Evidence.  Those responsibilities remain with the
        authoritative Scheduler/Kernel and an explicitly supplied dispatcher.

        ``dispatcher`` is intentionally opt-in.  Omitting it produces an
        auditable advisory controller whose actions are marked
        ``NOT_DISPATCHED``.  ``state_provider``/``observe`` may be supplied by
        an embedding runtime (for example a Harness integration); when they
        are omitted, the provider is the read-only ``AgentOS.runtime_state``
        projection for ``goal``.  A Goal must already be compiled: this
        factory never compiles or mutates semantic state.
        """

        # Import lazily to keep the composition root free of an import cycle
        # (``computation_control`` itself imports Harness and runtime DTOs).
        from .computation_control import OnlineComputationController

        goal_id = str(getattr(goal, "goal_id", goal)).strip()
        if not goal_id:
            raise ConfigurationError("computation_controller requires a non-empty goal")
        if self._gid_for(goal_id) is None:
            raise ConfigurationError(
                f"goal {goal_id!r} is not compiled; compile the Goal before creating "
                "an online computation controller"
            )
        if state_provider is not None and observe is not None:
            raise ConfigurationError("state_provider and observe are mutually exclusive")

        provider = state_provider or observe
        if provider is None:

            def observe_goal_state() -> GlobalRuntimeState:
                return self.runtime_state(goal)

            provider = observe_goal_state

        return OnlineComputationController(
            state_provider=provider,
            reconcile=reconcile,
            event_provider=event_provider,
            dispatcher=dispatcher,
            max_parallelism=max_parallelism,
            conflict_graph=conflict_graph,
            enabled=enabled,
            initial_epoch=initial_epoch,
            dispatch_continuations=dispatch_continuations,
            dispatch_deferred=dispatch_deferred,
        )

    # A concise alias for callers that use the terminology from the design
    # documents.  Both names deliberately share the exact factory semantics.
    def online_control(self, goal: Goal | str, **kwargs: Any) -> Any:
        """Alias for :meth:`computation_controller`."""

        return self.computation_controller(goal, **kwargs)

    def event_supervisor(self, goal: Goal | str, **kwargs: Any) -> Any:
        """Create a caller-owned bounded event-driven execution supervisor.

        The returned :class:`~lhos.sdk.event_supervisor.EventDrivenSupervisor`
        is inert until the caller explicitly invokes ``start()`` and
        ``step()``/``run()``.  This factory never starts a background thread,
        watcher, daemon, or implicit retry loop.  It requires an already
        compiled Goal and composes the existing ``runtime_state`` and
        ``execute_online_epoch`` authorities.

        ``kwargs`` are forwarded to ``EventDrivenSupervisor`` (for example
        ``watcher``, ``max_epochs``, ``max_concurrency``,
        ``poll_workspace``, and ``route_workspace``).
        """

        from .event_supervisor import EventDrivenSupervisor

        goal_id = str(getattr(goal, "goal_id", goal)).strip()
        if not goal_id:
            raise ConfigurationError("event_supervisor requires a non-empty goal")
        if self._gid_for(goal_id) is None:
            raise ConfigurationError(
                f"goal {goal_id!r} is not compiled; compile the Goal before creating "
                "an event-driven supervisor"
            )
        return EventDrivenSupervisor(self, goal, **kwargs)

    def online_supervisor(self, goal: Goal | str, **kwargs: Any) -> Any:
        """Alias for :meth:`event_supervisor` using online-control terminology."""

        return self.event_supervisor(goal, **kwargs)

    async def execute_online_epoch(
        self,
        goal: Goal,
        *,
        max_concurrency: int = 1,
        max_dispatches: int | None = None,
        max_parallelism: int | None = None,
        resource_aware: bool = False,
        conflict_graph: ConflictGraph | None = None,
        persist_epoch: bool = True,
        automatic_rebase: bool = True,
        max_automatic_rebase_dispatches: int = 1,
    ) -> RunResult:
        """Execute one bounded adaptive scheduling epoch.

        This is the smallest end-to-end online-control vertical slice exposed
        by the SDK.  The method deliberately delegates to the existing
        :meth:`run_async` path with ``adaptive=True`` and ``max_steps=1``;
        therefore Scheduler/Claim/Attempt/Lease admission, Harness/Agent
        execution, independent verification, and the serialized VPG Evidence
        commit remain the existing authorities.  It does not call
        :meth:`schedule_online_epoch` (which is a plan/admission-only API) and
        it does not return an ``OnlineEpochScheduleResult``.

        ``max_dispatches`` bounds the total work admitted by this epoch.  If
        omitted, it defaults to ``max_concurrency``.  A value of zero is a
        strict no-work budget: the Goal may be registered/compiled and its
        current result observed, but no policy epoch is planned or persisted,
        no Scheduler admission occurs, and no user code runs.
        """

        if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
            raise ConfigurationError("max_concurrency must be an integer")
        if max_concurrency < 1:
            raise ConfigurationError("max_concurrency must be >= 1")
        if max_dispatches is None:
            dispatch_limit = max_concurrency
        else:
            if isinstance(max_dispatches, bool) or not isinstance(max_dispatches, int):
                raise ConfigurationError("max_dispatches must be an integer or None")
            if max_dispatches < 0:
                raise ConfigurationError("max_dispatches must be >= 0")
            dispatch_limit = max_dispatches
        if max_parallelism is None:
            parallelism_limit = max_concurrency
        else:
            if isinstance(max_parallelism, bool) or not isinstance(max_parallelism, int):
                raise ConfigurationError("max_parallelism must be an integer or None")
            if max_parallelism < 1:
                raise ConfigurationError("max_parallelism must be >= 1")
            parallelism_limit = max_parallelism
        if not isinstance(resource_aware, bool):
            raise ConfigurationError("resource_aware must be a boolean")
        if not isinstance(persist_epoch, bool):
            raise ConfigurationError("persist_epoch must be a boolean")

        result = await self.run_async(
            goal,
            max_dispatches=dispatch_limit,
            max_steps=1,
            max_concurrency=max_concurrency,
            adaptive=True,
            conflict_graph=conflict_graph,
            max_parallelism=parallelism_limit,
            resource_aware=resource_aware,
            persist_adaptive_epochs=persist_epoch,
            automatic_rebase=automatic_rebase,
            max_automatic_rebase_dispatches=max_automatic_rebase_dispatches,
        )

        # Keep this metadata additive and explicit: callers can distinguish a
        # real execution epoch from ``schedule_online_epoch``'s admission-only
        # transcript without relying on wall-clock timing or log messages.
        adaptive_epochs = result.meta.get("adaptive_epochs", ())
        first_epoch = adaptive_epochs[0] if adaptive_epochs else {}
        policy_selected = tuple(first_epoch.get("selected_task_ids", ()))
        actual_dispatched = tuple(first_epoch.get("actual_dispatched_task_ids", ()))
        fallback_dispatched = tuple(first_epoch.get("fallback_dispatched_task_ids", ()))
        scheduler_skipped = tuple(first_epoch.get("scheduler_skipped", ()))
        scheduler_skipped_count = int(
            first_epoch.get("scheduler_skipped_count", len(scheduler_skipped))
        )
        scheduler_skipped_truncated = bool(first_epoch.get("scheduler_skipped_truncated", False))
        planned_graph_id = str(first_epoch.get("graph_id", ""))
        planned_graph_version = first_epoch.get("graph_version")
        if planned_graph_version is not None:
            planned_graph_version = int(planned_graph_version)
        planned_projection_hash = str(first_epoch.get("projection_hash", ""))
        final_graph_version = int(
            self._vpg_surface.current_graph_version(self._gid_for(goal.goal_id) or "")
        )
        no_work_budget = dispatch_limit == 0
        has_dispatch = bool(actual_dispatched)
        has_failures = bool(result.failures)
        executed_phases: tuple[str, ...]
        if no_work_budget:
            outcome = "no_work_budget"
            # No policy/scheduler work is performed for a zero budget.
            executed_phases = ("observe",)
        elif not has_dispatch:
            outcome = "no_dispatch"
            # A bounded epoch can plan and attempt admission without ever
            # starting user code (already-closed Goal, empty frontier, or
            # authoritative admission refusal).  Do not claim execution,
            # verification, or semantic commit happened.
            executed_phases = (
                "observe",
                "reconcile",
                "plan",
                "admit",
                "observe",
            )
        elif has_failures:
            outcome = "completed_with_failures"
            # At least one dispatch was attempted, but the returned RunResult
            # contains an execution/verification failure.  A semantic commit
            # is therefore not guaranteed for this epoch; keep the phase
            # transcript conservative and omit ``commit``.
            executed_phases = (
                "observe",
                "reconcile",
                "plan",
                "admit",
                "execute",
                "verify",
                "observe",
            )
        else:
            outcome = "completed"
            executed_phases = (
                "observe",
                "reconcile",
                "plan",
                "admit",
                "execute",
                "verify",
                "commit",
                "observe",
            )
        result.meta["online_epoch"] = {
            "schema_version": "online-execution.v1",
            "phases": executed_phases,
            "executed_phases": executed_phases,
            "outcome": outcome,
            "bounded": True,
            "adaptive": True,
            "max_steps": 1,
            "max_concurrency": max_concurrency,
            "max_dispatches": dispatch_limit,
            "max_parallelism": parallelism_limit,
            "resource_aware": resource_aware,
            "automatic_rebase": automatic_rebase,
            "max_automatic_rebase_dispatches": max_automatic_rebase_dispatches,
            "persist_epoch": persist_epoch,
            "planned_graph_id": planned_graph_id,
            "planned_graph_version": planned_graph_version,
            "planned_projection_hash": planned_projection_hash,
            "final_graph_version": final_graph_version,
            # Policy selection is advisory and may differ from the exact tasks
            # admitted by Scheduler fallback.  Keep both surfaces explicit.
            "selected_task_ids": policy_selected,
            "policy_selected_task_ids": policy_selected,
            "actual_dispatched_task_ids": actual_dispatched,
            # The ranking the Scheduler actually dispatched under, and which of
            # those dispatches reused an Agent that already held the Task's
            # declared reads.  Without these at this boundary a self-driving
            # caller cannot tell a critical-path dispatch from an arbitrary one.
            "dispatch_order_applied": tuple(first_epoch.get("dispatch_order_applied", ())),
            "locality_matched_task_ids": tuple(first_epoch.get("locality_matched_task_ids", ())),
            "fallback_attempted": bool(first_epoch.get("fallback_attempted", False)),
            "fallback_dispatched_task_ids": fallback_dispatched,
            # Preserve the authoritative Scheduler admission transcript at
            # the one-shot wrapper boundary.  This is intentionally additive:
            # the nested ``adaptive_epochs`` remains the complete bounded
            # audit, while callers need not unpack it for skip reasons.
            "scheduler_skipped": scheduler_skipped,
            "scheduler_skipped_count": scheduler_skipped_count,
            "scheduler_skipped_truncated": scheduler_skipped_truncated,
            "scheduler_authority": "Scheduler.run_pass",
            "semantic_authority": "VPG.Evidence",
            "result_kind": "RunResult",
        }
        return result

    async def execute_online_epochs(
        self,
        goal: Goal,
        *,
        max_epochs: int = 1,
        max_concurrency: int = 1,
        max_dispatches_per_epoch: int | None = None,
        max_parallelism: int | None = None,
        resource_aware: bool = False,
        conflict_graph: ConflictGraph | None = None,
        persist_epoch: bool = True,
        stop_when_closed: bool = True,
        automatic_rebase: bool = True,
        max_automatic_rebase_dispatches: int = 1,
    ) -> OnlineExecutionLoopResult:
        """Run a bounded sequence of explicit online execution epochs.

        This is intentionally a caller-driven convenience loop: every
        iteration invokes :meth:`execute_online_epoch`, observes the resulting
        VPG projection, and then decides whether to continue.  It does not
        create a daemon, retain Python call stacks, or bypass the existing
        Claim/Lease/verification authorities.  A failure in one epoch is
        returned in that epoch's :class:`RunResult`; the loop stops fail-closed
        rather than silently retrying an unsuccessful computation.
        """

        if isinstance(max_epochs, bool) or not isinstance(max_epochs, int):
            raise ConfigurationError("max_epochs must be an integer")
        if max_epochs < 0:
            raise ConfigurationError("max_epochs must be >= 0")
        if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
            raise ConfigurationError("max_concurrency must be an integer")
        if max_concurrency < 1:
            raise ConfigurationError("max_concurrency must be >= 1")
        if max_dispatches_per_epoch is not None:
            if isinstance(max_dispatches_per_epoch, bool) or not isinstance(
                max_dispatches_per_epoch, int
            ):
                raise ConfigurationError("max_dispatches_per_epoch must be an integer or None")
            if max_dispatches_per_epoch < 0:
                raise ConfigurationError("max_dispatches_per_epoch must be >= 0")
        if max_parallelism is not None:
            if isinstance(max_parallelism, bool) or not isinstance(max_parallelism, int):
                raise ConfigurationError("max_parallelism must be an integer or None")
            if max_parallelism < 1:
                raise ConfigurationError("max_parallelism must be >= 1")
        if not isinstance(resource_aware, bool):
            raise ConfigurationError("resource_aware must be a boolean")
        if not isinstance(persist_epoch, bool):
            raise ConfigurationError("persist_epoch must be a boolean")
        if not isinstance(stop_when_closed, bool):
            raise ConfigurationError("stop_when_closed must be a boolean")

        self._goals.setdefault(goal.goal_id, goal)
        # Compile/register before a zero-epoch return so the result still
        # reports the authoritative current Goal projection.
        gid = self._gid_for(goal.goal_id, compile_if_missing=True)
        if gid is None:
            raise ConfigurationError(f"goal {goal.goal_id!r} not registered")

        if max_epochs == 0:
            current = self.result(gid)
            return OnlineExecutionLoopResult(
                goal_id=goal.goal_id,
                epochs=(),
                final_result=current,
                stop_reason="max_epochs",
                max_epochs=max_epochs,
                max_concurrency=max_concurrency,
                max_dispatches_per_epoch=max_dispatches_per_epoch,
                max_parallelism=max_parallelism,
                resource_aware=resource_aware,
                stop_when_closed=stop_when_closed,
            )

        epochs: list[RunResult] = []
        stop_reason = "max_epochs"
        for _ in range(max_epochs):
            result = await self.execute_online_epoch(
                goal,
                max_concurrency=max_concurrency,
                max_dispatches=max_dispatches_per_epoch,
                max_parallelism=max_parallelism,
                resource_aware=resource_aware,
                conflict_graph=conflict_graph,
                persist_epoch=persist_epoch,
                automatic_rebase=automatic_rebase,
                max_automatic_rebase_dispatches=max_automatic_rebase_dispatches,
            )
            epochs.append(result)
            if stop_when_closed and result.goal_state == "closed":
                stop_reason = "goal_closed"
                break
            if result.failures:
                stop_reason = "epoch_failed"
                break
            online = result.meta.get("online_epoch", {})
            if online.get("outcome") == "no_work_budget":
                stop_reason = "no_work_budget"
                break
            if not online.get("actual_dispatched_task_ids", ()):
                stop_reason = "no_dispatch"
                break
        return OnlineExecutionLoopResult(
            goal_id=goal.goal_id,
            epochs=tuple(epochs),
            final_result=epochs[-1] if epochs else self.result(gid),
            stop_reason=stop_reason,
            max_epochs=max_epochs,
            max_concurrency=max_concurrency,
            max_dispatches_per_epoch=max_dispatches_per_epoch,
            max_parallelism=max_parallelism,
            resource_aware=resource_aware,
            stop_when_closed=stop_when_closed,
        )

    def schedule_online_epoch(
        self,
        goal: Goal | str,
        *,
        controller: Any | None = None,
        event_proposals: Iterable[Any] = (),
        max_parallelism: int = 1,
        conflict_graph: Any | None = None,
        plan_only: bool = True,
        keep_claims: bool = False,
        persist_epoch: bool = False,
    ) -> Any:
        """Plan, and optionally admit, one real online scheduling epoch.

        The :class:`OnlineComputationController` owns the WHAT/WHEN proposal.
        When ``plan_only`` is false, its ``selected_task_ids`` are passed as
        an advisory filter to the existing authoritative
        :meth:`SchedulerSession.run_pass`.  That pass still performs VPG
        readiness, eligibility, logical-resource admission, Claim creation,
        Attempt creation, and Kernel Lease fencing.

        This method never invokes an Agent executor, a Harness hook, a
        verifier, or semantic commit code.  An ownerless ``START`` proposal
        therefore cannot be registered as a Harness session.  Only exact
        Scheduler dispatches containing a live Claim, Attempt, and Lease are
        returned.

        Safety defaults are deliberately conservative:

        * ``plan_only=True`` creates no operational ownership.
        * An explicit scheduling pass (``plan_only=False``) automatically
          releases every newly-created Claim unless ``keep_claims=True``.
        * With ``keep_claims=True`` the caller owns execution or exact-claim
          cleanup and must eventually call :meth:`release_online_epoch` (or
          complete the normal execution/verification lifecycle).
        """

        from .computation_control import (
            ControlLoopStatus,
            OnlineComputationController,
        )
        from .online_epoch import (
            OnlineEpochDispatch,
            OnlineEpochScheduleResult,
            OnlineEpochSkip,
            OnlineEpochStatus,
        )

        if not isinstance(plan_only, bool):
            raise ConfigurationError("plan_only must be a boolean")
        if not isinstance(keep_claims, bool):
            raise ConfigurationError("keep_claims must be a boolean")
        if not isinstance(persist_epoch, bool):
            raise ConfigurationError("persist_epoch must be a boolean")
        if plan_only and keep_claims:
            raise ConfigurationError("keep_claims requires plan_only=False")
        if self._read_only and (not plan_only or persist_epoch):
            raise ExecutionError(
                "read-only AgentOS can only run a non-persistent plan-only online epoch"
            )

        goal_id = str(getattr(goal, "goal_id", goal)).strip()
        if not goal_id:
            raise ConfigurationError("schedule_online_epoch requires a non-empty goal")
        gid = self._gid_for(goal_id)
        if gid is None:
            raise ConfigurationError(
                f"goal {goal_id!r} is not compiled; compile the Goal before "
                "scheduling an online epoch"
            )

        if controller is None:
            controller = self.computation_controller(
                goal,
                max_parallelism=max_parallelism,
                conflict_graph=conflict_graph,
            )
        elif not isinstance(controller, OnlineComputationController):
            raise ConfigurationError("controller must be an OnlineComputationController")

        # Dispatch is always disabled here.  START remains an ownerless
        # proposal until the authoritative Scheduler has created the exact
        # Claim/Attempt/Lease identities below.
        audit = controller.step(
            event_proposals=tuple(event_proposals),
            dispatch=False,
        )
        if audit.graph_id != gid:
            raise SchedulingError(
                "online controller returned a graph identity that does not match the Goal"
            )

        current_version = int(self._vpg_surface.current_graph_version(gid))
        if audit.graph_version != current_version:
            return OnlineEpochScheduleResult.create(
                status=OnlineEpochStatus.GRAPH_CHANGED,
                graph_id=gid,
                graph_version=current_version,
                control_audit=audit,
                selected_task_ids=tuple(audit.selected_task_ids),
                reason=(
                    "graph version changed after online planning: "
                    f"planned={audit.graph_version}, current={current_version}"
                ),
            )

        accepted_statuses = {
            ControlLoopStatus.PLANNED,
            ControlLoopStatus.NOOP,
        }
        if audit.status not in accepted_statuses:
            status = (
                OnlineEpochStatus.GRAPH_CHANGED
                if audit.status is ControlLoopStatus.GRAPH_CHANGED
                else OnlineEpochStatus.POLICY_REJECTED
            )
            return OnlineEpochScheduleResult.create(
                status=status,
                graph_id=gid,
                graph_version=current_version,
                control_audit=audit,
                selected_task_ids=tuple(audit.selected_task_ids),
                reason=audit.reason or f"control loop status={audit.status.value}",
            )

        selected = tuple(
            dict.fromkeys(
                str(task_id).strip() for task_id in audit.selected_task_ids if str(task_id).strip()
            )
        )
        persisted = False
        if persist_epoch:
            try:
                self._scheduler.record_scheduling_epoch(
                    graph_id=gid,
                    graph_version=audit.graph_version,
                    epoch_id=audit.epoch_id,
                    policy_id=audit.controller_id,
                    decision_hash=audit.decision_hash,
                    projection_hash=audit.projection_hash,
                    candidate_task_ids=audit.candidate_task_ids,
                    selected_task_ids=selected,
                    deferred_task_ids=audit.deferred_task_ids,
                    parallelism_hint=audit.parallelism_hint,
                )
            except Exception as exc:
                raise SchedulingError(
                    "failed to persist online scheduling epoch audit",
                    cause=exc,
                ) from exc
            persisted = True

        if not selected:
            return OnlineEpochScheduleResult.create(
                status=OnlineEpochStatus.NOOP,
                graph_id=gid,
                graph_version=current_version,
                control_audit=audit,
                selected_task_ids=(),
                scheduling_audit_persisted=persisted,
                reason=audit.reason or "online policy selected no schedulable task",
            )
        if plan_only:
            return OnlineEpochScheduleResult.create(
                status=OnlineEpochStatus.PLANNED_ONLY,
                graph_id=gid,
                graph_version=current_version,
                control_audit=audit,
                selected_task_ids=selected,
                scheduling_audit_persisted=persisted,
                reason="policy plan only; Scheduler ownership was not requested",
            )

        try:
            scheduled = self._scheduler.run_pass(
                gid,
                max_claims=min(len(selected), max(1, audit.parallelism_hint)),
                allowed_task_ids=selected,
                expected_graph_version=int(audit.graph_version),
            )
        except Exception as exc:
            raise SchedulingError("online scheduler pass failed", cause=exc) from exc

        raw_dispatches = list(scheduled.dispatched)
        if getattr(scheduled, "policy_stale", False):
            # The selected policy snapshot was superseded before ownership
            # could be handed out.  Return a typed graph-change result rather
            # than exposing a dispatch from an obsolete epoch.
            return OnlineEpochScheduleResult.create(
                status=OnlineEpochStatus.GRAPH_CHANGED,
                graph_id=gid,
                graph_version=int(getattr(scheduled, "observed_graph_version", current_version)),
                control_audit=audit,
                selected_task_ids=selected,
                scheduling_audit_persisted=persisted,
                reason=str(
                    getattr(
                        scheduled,
                        "policy_stale_reason",
                        "online policy graph version became stale",
                    )
                )[:512],
            )
        exact_dispatches: list[OnlineEpochDispatch] = []
        try:
            for dispatch in raw_dispatches:
                task_id = str(dispatch.get("task_id", "")).strip()
                agent_id = str(dispatch.get("agent_id", "")).strip()
                claim_id = str(dispatch.get("claim_id", "")).strip()
                claim = self._live_claim(
                    gid,
                    task_id,
                    claim_id=claim_id,
                    agent_id=agent_id,
                )
                attempt = (
                    None if claim is None else self._scheduler.attempt_for_claim(claim.claim_id)
                )
                if (
                    claim is None
                    or attempt is None
                    or attempt.claim_id != claim.claim_id
                    or attempt.graph_id != claim.graph_id
                    or attempt.task_id != claim.task_id
                    or attempt.agent_id != claim.agent_id
                    or attempt.graph_version != claim.graph_version
                    or attempt.process_id != claim.process_id
                    or not claim.lease_id
                    or claim.lease_fencing_token is None
                ):
                    raise SchedulingError(
                        "Scheduler dispatch lacks an exact live Claim/Attempt/Lease fence"
                    )
                exact_dispatches.append(
                    OnlineEpochDispatch(
                        graph_id=claim.graph_id,
                        graph_version=claim.graph_version,
                        semantic_epoch=attempt.semantic_epoch,
                        task_id=claim.task_id,
                        agent_id=claim.agent_id,
                        process_id=claim.process_id,
                        claim_id=claim.claim_id,
                        attempt_id=attempt.attempt_id,
                        lease_id=claim.lease_id,
                        lease_fencing_token=claim.lease_fencing_token,
                    )
                )
        except Exception:
            # No validation failure may strand the ownership acquired by this
            # call.  Release only the exact returned claims; a replacement
            # owner can never be released by this compensation path.
            self._release_unexecuted_dispatches(
                gid,
                raw_dispatches,
                reason="online_epoch_dispatch_validation_failed",
            )
            raise

        skipped = tuple(
            OnlineEpochSkip(task_id=str(task_id), reason=str(reason))
            for task_id, reason in scheduled.skipped
            if str(task_id).strip() and str(reason).strip()
        )
        post_schedule_version = int(self._vpg_surface.current_graph_version(gid))
        graph_raced = post_schedule_version != audit.graph_version or any(
            item.graph_version != audit.graph_version for item in exact_dispatches
        )
        if graph_raced:
            (
                released_after_race,
                terminal_after_race,
                retained_after_race,
                cleanup_errors,
            ) = self._release_online_dispatches(
                gid,
                exact_dispatches,
                reason="online_epoch_graph_changed",
            )
            released_after_race = released_after_race + terminal_after_race
            status = (
                OnlineEpochStatus.CLEANUP_REQUIRED
                if retained_after_race
                else OnlineEpochStatus.GRAPH_CHANGED
            )
            cleanup_note = f"; cleanup_errors={','.join(cleanup_errors)}" if cleanup_errors else ""
            return OnlineEpochScheduleResult.create(
                status=status,
                graph_id=gid,
                graph_version=post_schedule_version,
                control_audit=audit,
                selected_task_ids=selected,
                dispatches=tuple(exact_dispatches),
                skipped=skipped,
                released_claim_ids=released_after_race,
                retained_claim_ids=retained_after_race,
                scheduler_invoked=True,
                claims_retained=bool(retained_after_race),
                scheduling_audit_persisted=persisted,
                reason=(
                    "graph changed between online planning and Scheduler "
                    f"admission (planned={audit.graph_version}, "
                    f"current={post_schedule_version}){cleanup_note}"
                ),
            )

        if not exact_dispatches:
            return OnlineEpochScheduleResult.create(
                status=OnlineEpochStatus.NOOP,
                graph_id=gid,
                graph_version=post_schedule_version,
                control_audit=audit,
                selected_task_ids=selected,
                skipped=skipped,
                scheduler_invoked=True,
                scheduling_audit_persisted=persisted,
                reason="authoritative Scheduler admitted no selected task",
            )

        released: list[str] = []
        retained: list[str] = []
        if keep_claims:
            retained = [item.claim_id for item in exact_dispatches]
            status = OnlineEpochStatus.CLAIMS_ACQUIRED
            reason = (
                "caller retained exact Scheduler ownership; execute/register a "
                "Harness or release_online_epoch is required"
            )
        else:
            (
                released_now,
                terminal_now,
                retained_now,
                cleanup_errors,
            ) = self._release_online_dispatches(
                gid,
                exact_dispatches,
                reason="online_epoch_auto_release",
            )
            released.extend(released_now)
            # A release that completed before a provider exception is still
            # safe to report as cleaned up; no live ownership remains.
            released.extend(terminal_now)
            retained.extend(retained_now)
            if retained:
                status = OnlineEpochStatus.CLEANUP_REQUIRED
                reason = "one or more exact Claims could not be auto-released"
            else:
                status = OnlineEpochStatus.CLAIMS_RELEASED
                reason = "Scheduler admission completed; all unexecuted Claims auto-released"
            if cleanup_errors:
                reason = f"{reason}; cleanup_errors={','.join(cleanup_errors)}"

        return OnlineEpochScheduleResult.create(
            status=status,
            graph_id=gid,
            graph_version=post_schedule_version,
            control_audit=audit,
            selected_task_ids=selected,
            dispatches=tuple(exact_dispatches),
            skipped=skipped,
            released_claim_ids=tuple(released),
            retained_claim_ids=tuple(retained),
            scheduler_invoked=True,
            claims_retained=bool(retained),
            scheduling_audit_persisted=persisted,
            reason=reason,
        )

    def handoff_online_epoch_to_harness(
        self,
        result: Any,
        harnesses: Mapping[str, Any],
    ) -> Any:
        """Bind retained online-epoch ownership to exact Harness sessions.

        ``schedule_online_epoch(..., keep_claims=True)`` deliberately stops at
        Scheduler/Kernel admission.  This method is the narrow next seam:
        callers provide one :class:`HarnessSessionAdapter` per retained Claim,
        and the bridge verifies the complete dispatch fence
        ``graph/task/agent/process/claim/attempt/semantic_epoch/lease`` before
        calling the existing :meth:`register_harness` authority.

        The method never releases a Claim or Lease.  A failed preflight
        therefore cannot release a replacement owner, and a registration
        failure only removes Harness bindings created by this call (not
        Scheduler ownership).  Harness control/execution and VPG verification
        remain separate operations.
        """

        from .harness import HarnessSessionAdapter, HarnessSessionSnapshot
        from .online_epoch import (
            OnlineEpochHarnessBinding,
            OnlineEpochHarnessHandoffResult,
            OnlineEpochHarnessHandoffStatus,
            OnlineEpochScheduleResult,
        )

        if self._read_only:
            raise ConfigurationError(
                "read-only AgentOS cannot hand off online epoch Claims to Harnesses"
            )
        if not isinstance(result, OnlineEpochScheduleResult):
            raise ConfigurationError("result must be an OnlineEpochScheduleResult")
        if not isinstance(harnesses, Mapping):
            raise ConfigurationError("harnesses must be a mapping keyed by Claim id")

        known_graphs = set(self._goal_gid.values())
        if result.graph_id not in known_graphs:
            raise ConfigurationError("online epoch result does not belong to this AgentOS instance")
        if not result.claims_retained or not result.retained_claim_ids:
            raise ConfigurationError("online epoch result does not retain live Claim ownership")
        dispatch_by_claim = {item.claim_id: item for item in result.dispatches}
        requested = tuple(result.retained_claim_ids)
        if set(requested) != set(dispatch_by_claim):
            raise ConfigurationError(
                "retained Claim ids must exactly match online epoch dispatches"
            )
        normalized_harnesses: dict[str, Any] = {}
        for raw_claim_id, harness in harnesses.items():
            claim_id = str(raw_claim_id).strip()
            if not claim_id:
                raise ConfigurationError("Harness mapping Claim ids must be non-empty")
            if claim_id in normalized_harnesses:
                raise ConfigurationError(f"duplicate Harness mapping for Claim {claim_id!r}")
            normalized_harnesses[claim_id] = harness
        if set(normalized_harnesses) != set(requested):
            missing = sorted(set(requested) - set(normalized_harnesses))
            extra = sorted(set(normalized_harnesses) - set(requested))
            details: list[str] = []
            if missing:
                details.append("missing=" + ",".join(missing))
            if extra:
                details.append("extra=" + ",".join(extra))
            raise ConfigurationError(
                "Harness mapping must cover exactly retained Claims"
                + (f" ({'; '.join(details)})" if details else "")
            )

        def _result(
            *,
            status: OnlineEpochHarnessHandoffStatus,
            bound: tuple[str, ...] = (),
            replayed: tuple[str, ...] = (),
            refused: tuple[str, ...] = (),
            bindings: tuple[Any, ...] = (),
            errors: tuple[str, ...] = (),
            reason: str,
        ) -> Any:
            return OnlineEpochHarnessHandoffResult.create(
                status=status,
                graph_id=result.graph_id,
                graph_version=result.graph_version,
                source_result_hash=result.result_hash,
                requested_claim_ids=requested,
                bound_claim_ids=bound,
                replayed_claim_ids=replayed,
                refused_claim_ids=refused,
                bindings=bindings,
                errors=errors,
                reason=reason,
            )

        # Do not bind an epoch whose graph basis has already been superseded.
        # The Claim may still carry a live lease, but handing a Harness a stale
        # cognition basis would make the ownership bridge appear successful
        # while violating the online epoch contract.
        try:
            current_version = int(self._vpg_surface.current_graph_version(result.graph_id))
        except Exception as exc:
            return _result(
                status=OnlineEpochHarnessHandoffStatus.FAILED_CLOSED,
                refused=requested,
                errors=(_bounded_audit_error(exc),),
                reason="could not observe current graph version",
            )
        if current_version != result.graph_version:
            return _result(
                status=OnlineEpochHarnessHandoffStatus.REFUSED,
                refused=requested,
                errors=(
                    f"graph version changed: epoch={result.graph_version}, "
                    f"current={current_version}",
                ),
                reason="online epoch graph basis is stale",
            )

        # Preflight every dispatch before mutating the Harness registry.  This
        # gives the all-or-nothing common path and prevents one malformed
        # session from leaving a half-bound epoch.
        preflight: dict[str, tuple[Any, Any, Any, Any, bool]] = {}
        refused: list[str] = []
        errors: list[str] = []
        replayed: list[str] = []
        for claim_id in requested:
            dispatch = dispatch_by_claim[claim_id]
            harness = normalized_harnesses[claim_id]
            try:
                if not isinstance(harness, HarnessSessionAdapter):
                    raise ConfigurationError("Harness does not implement HarnessSessionAdapter")
                snapshot = harness.snapshot
                if not isinstance(snapshot, HarnessSessionSnapshot):
                    raise ConfigurationError("Harness returned an invalid HarnessSessionSnapshot")
                if not harness.capabilities.supports("start"):
                    raise ConfigurationError("Harness does not declare START support")
                claim = next(
                    (item for item in self._scheduler.claims if item.claim_id == claim_id),
                    None,
                )
                attempt = self._scheduler.attempt_for_claim(claim_id)
                live = self._live_claim(
                    result.graph_id,
                    dispatch.task_id,
                    claim_id=claim_id,
                    agent_id=dispatch.agent_id,
                )
                if claim is None or attempt is None or live is None:
                    raise ConfigurationError("Claim/Attempt/Lease is not currently live")
                expected_dispatch = (
                    result.graph_id,
                    result.graph_version,
                    dispatch.semantic_epoch,
                    dispatch.task_id,
                    dispatch.agent_id,
                    dispatch.process_id,
                    dispatch.claim_id,
                    dispatch.attempt_id,
                    dispatch.lease_id,
                    dispatch.lease_fencing_token,
                )
                if (
                    dispatch.graph_id != result.graph_id
                    or dispatch.graph_version != result.graph_version
                ):
                    raise ConfigurationError(
                        "dispatch graph identity/version does not match online epoch"
                    )
                actual_claim = (
                    claim.graph_id,
                    claim.graph_version,
                    attempt.semantic_epoch,
                    claim.task_id,
                    claim.agent_id,
                    claim.process_id,
                    claim.claim_id,
                    attempt.attempt_id,
                    claim.lease_id,
                    claim.lease_fencing_token,
                )
                actual_attempt = (
                    attempt.graph_id,
                    attempt.graph_version,
                    attempt.semantic_epoch,
                    attempt.task_id,
                    attempt.agent_id,
                    attempt.process_id,
                    attempt.claim_id,
                    attempt.attempt_id,
                    claim.lease_id,
                    claim.lease_fencing_token,
                )
                if expected_dispatch != actual_claim or expected_dispatch != actual_attempt:
                    raise ConfigurationError(
                        "dispatch does not match exact Claim/Attempt/Lease identity"
                    )
                identity = snapshot.identity
                expected_session = (
                    result.graph_id,
                    result.graph_version,
                    dispatch.semantic_epoch,
                    dispatch.task_id,
                    dispatch.agent_id,
                    dispatch.claim_id,
                    dispatch.attempt_id,
                )
                actual_session = (
                    identity.graph_id,
                    identity.graph_version,
                    identity.semantic_epoch,
                    identity.task_id,
                    identity.agent_id,
                    identity.claim_id,
                    identity.attempt_id,
                )
                if expected_session != actual_session:
                    raise ConfigurationError(
                        "Harness session identity does not match exact dispatch"
                    )
                existing = self.harness_for_claim(claim_id)
                if existing is not None and existing is not harness:
                    raise ConfigurationError(
                        "Claim already has a different registered Harness session"
                    )
                is_replay = existing is harness
                preflight[claim_id] = (dispatch, claim, attempt, harness, is_replay)
                if is_replay:
                    replayed.append(claim_id)
            except Exception as exc:
                refused.append(claim_id)
                errors.append(f"{claim_id}:{_bounded_audit_error(exc)}")

        if refused:
            return _result(
                status=OnlineEpochHarnessHandoffStatus.REFUSED,
                refused=tuple(requested),
                errors=tuple(errors),
                reason="Harness handoff preflight refused; no Claim/Lease was released",
            )

        newly_bound: list[str] = []
        bindings: list[Any] = []
        try:
            for claim_id in requested:
                dispatch, claim, attempt, harness, is_replay = preflight[claim_id]
                if not is_replay:
                    self.register_harness(harness)
                    newly_bound.append(claim_id)
                snapshot = harness.snapshot
                bindings.append(
                    OnlineEpochHarnessBinding(
                        graph_id=dispatch.graph_id,
                        graph_version=dispatch.graph_version,
                        semantic_epoch=dispatch.semantic_epoch,
                        task_id=dispatch.task_id,
                        agent_id=dispatch.agent_id,
                        process_id=dispatch.process_id,
                        claim_id=dispatch.claim_id,
                        attempt_id=dispatch.attempt_id,
                        lease_id=dispatch.lease_id,
                        lease_fencing_token=dispatch.lease_fencing_token,
                        session_id=snapshot.identity.session_id,
                        snapshot_revision=snapshot.revision,
                        snapshot_state=snapshot.state.value,
                        outcome="replayed" if is_replay else "bound",
                    )
                )
        except Exception as exc:
            # Remove only mappings created by this call.  This is deliberately
            # not Scheduler cleanup: no old Harness handoff may release a
            # replacement Claim/Lease.
            cleanup_errors: list[str] = []
            for claim_id in reversed(newly_bound):
                try:
                    bound_harness = normalized_harnesses[claim_id]
                    session_id = bound_harness.snapshot.identity.session_id
                    self.unregister_harness(session_id, claim_id=claim_id)
                except Exception as cleanup_exc:
                    cleanup_errors.append(f"{claim_id}:{_bounded_audit_error(cleanup_exc)}")
            details = [_bounded_audit_error(exc)]
            details.extend(cleanup_errors)
            return _result(
                status=OnlineEpochHarnessHandoffStatus.FAILED_CLOSED,
                refused=requested,
                errors=tuple(details),
                reason=(
                    "Harness registration failed closed; Scheduler/Kernel "
                    "ownership was not released"
                ),
            )

        bound = tuple(item for item in requested if item not in replayed)
        replayed_tuple = tuple(replayed)
        if bound and replayed_tuple:
            status = OnlineEpochHarnessHandoffStatus.PARTIAL
            reason = "retained Claims bound; existing Harness sessions replayed"
        elif bound:
            status = OnlineEpochHarnessHandoffStatus.BOUND
            reason = "all retained Claims bound to exact Harness sessions"
        else:
            status = OnlineEpochHarnessHandoffStatus.REPLAYED
            reason = "all retained Harness bindings replayed idempotently"
        return _result(
            status=status,
            bound=bound,
            replayed=replayed_tuple,
            bindings=tuple(bindings),
            reason=reason,
        )

    def release_online_epoch(
        self,
        result: Any,
        *,
        reason: str = "online_epoch_unexecuted",
    ) -> Any:
        """Release the exact still-live Claims retained by an online epoch."""

        from .online_epoch import (
            OnlineEpochReleaseResult,
            OnlineEpochScheduleResult,
        )

        if self._read_only:
            raise ExecutionError("read-only AgentOS cannot release online epoch Claims")
        if not isinstance(result, OnlineEpochScheduleResult):
            raise ConfigurationError("result must be an OnlineEpochScheduleResult")
        normalized_reason = str(reason).strip()
        if not normalized_reason:
            raise ConfigurationError("release reason must be non-empty")

        known_graphs = set(self._goal_gid.values())
        if result.graph_id not in known_graphs:
            raise ConfigurationError("online epoch result does not belong to this AgentOS instance")
        by_claim = {item.claim_id: item for item in result.dispatches}
        requested = tuple(
            dict.fromkeys(
                claim_id for claim_id in result.retained_claim_ids if claim_id in by_claim
            )
        )
        (
            released,
            already_terminal,
            not_released,
            cleanup_errors,
        ) = self._release_online_dispatches(
            result.graph_id,
            (by_claim[claim_id] for claim_id in requested),
            reason=normalized_reason,
        )
        result_reason = normalized_reason
        if cleanup_errors:
            result_reason = f"{normalized_reason}; cleanup_errors={','.join(cleanup_errors)}"
        return OnlineEpochReleaseResult.create(
            graph_id=result.graph_id,
            requested_claim_ids=requested,
            released_claim_ids=released,
            already_terminal_claim_ids=already_terminal,
            not_released_claim_ids=not_released,
            reason=result_reason,
        )

    def plan_context_rebase(
        self,
        old_graph_version: int,
        new_graph_version: int,
        old_bindings: Any = None,
        graph_delta: Any = None,
        *,
        old_context_manifest: ContextManifest | None = None,
        old_read_bindings: Any = None,
        required_refs: Any = None,
    ) -> Any:
        """Return a pure, bounded Context VM reuse/rebase plan.

        All context bindings and graph changes are explicit caller inputs.
        This facade does not inspect VPG/world state, discover dependencies,
        mutate Context VM, claim work, deliver interrupts, or alter the
        ``run``/``run_async`` execution path.  Semantic and execution
        authorities therefore remain unchanged.
        """

        from .context_delta import plan_context_rebase

        return plan_context_rebase(
            old_graph_version,
            new_graph_version,
            old_bindings,
            graph_delta,
            old_context_manifest=old_context_manifest,
            old_read_bindings=old_read_bindings,
            required_refs=required_refs,
        )

    def plan_live_context_rebase(
        self,
        goal: Goal | str,
        *,
        task_id: str,
        claim_id: str,
        agent_id: str,
        graph_delta: Any,
        target_graph_version: int | None = None,
        target_semantic_epoch: int | None = None,
        context_snapshot: Any | None = None,
        reason: str = "",
    ) -> Any:
        """Plan a rebase decision for one exact live Claim/Attempt/Harness.

        This is an authority-backed *planning* seam, not an automatic rebase
        loop.  It reads the already-bound ``AgentSnapshot`` and registered
        Harness session, validates their identities, and returns an immutable
        ``LiveContextRebasePlan``.  Applying ``REBASE`` still requires the
        explicit Scheduler/Kernel handoff path; this method never mutates VPG,
        Context VM, Claims, Leases, or Harness state.

        ``graph_delta`` and (when available) ``context_snapshot`` must be
        supplied explicitly.  Hidden reads are not discovered here.
        """

        from .rebase_runtime import (
            LiveContextRebasePlan,
            RebaseRuntimeBridge,
        )

        normalized_task = str(task_id).strip()
        normalized_claim = str(claim_id).strip()
        normalized_agent = str(agent_id).strip()
        if not normalized_task or not normalized_claim or not normalized_agent:
            raise ConfigurationError("task_id, claim_id, and agent_id must be non-empty")
        goal_id = str(getattr(goal, "goal_id", goal)).strip()
        if not goal_id:
            raise ConfigurationError("live Context rebase requires a non-empty goal")
        gid = self._gid_for(goal_id)
        if gid is None:
            raise ConfigurationError(
                f"goal {goal_id!r} is not compiled; live Context rebase is read-only"
            )
        claim = self._live_claim(
            gid,
            normalized_task,
            claim_id=normalized_claim,
            agent_id=normalized_agent,
        )
        if claim is None:
            raise ConfigurationError(
                "live Context rebase requires the exact active Claim and live Lease"
            )
        attempt = self._scheduler.attempt_for_claim(normalized_claim)
        if attempt is None or attempt.claim_id != normalized_claim:
            raise ConfigurationError("live Context rebase requires an exact Scheduler Attempt")
        snapshot = getattr(attempt, "agent_snapshot", None)
        if snapshot is None:
            raise ConfigurationError(
                "live Context rebase requires a durable AgentSnapshot bound to the Attempt"
            )
        harness = self.harness_for_claim(normalized_claim)
        if harness is None:
            raise ConfigurationError("live Context rebase requires a registered Harness session")
        harness_snapshot = getattr(harness, "snapshot", None)
        identity = getattr(harness_snapshot, "identity", None)
        if identity is None:
            raise ConfigurationError("registered Harness returned no session identity")
        expected_identity = (
            gid,
            int(getattr(claim, "graph_version", 0)),
            normalized_task,
            normalized_agent,
            normalized_claim,
            str(getattr(attempt, "attempt_id", "")),
            int(getattr(attempt, "semantic_epoch", 0)),
        )
        observed_identity = (
            str(getattr(identity, "graph_id", "")),
            int(getattr(identity, "graph_version", -1)),
            str(getattr(identity, "task_id", "")),
            str(getattr(identity, "agent_id", "")),
            str(getattr(identity, "claim_id", "")),
            str(getattr(identity, "attempt_id", "")),
            int(getattr(identity, "semantic_epoch", -1)),
        )
        if observed_identity != expected_identity:
            raise ConfigurationError("Harness identity does not match the live Claim/Attempt")

        current_graph_version = int(self._vpg.get_graph(gid).current_version)
        target = current_graph_version if target_graph_version is None else target_graph_version
        if isinstance(target, bool) or not isinstance(target, int) or target < 0:
            raise ConfigurationError("target_graph_version must be a non-negative integer")
        if target != current_graph_version:
            raise ConfigurationError(
                "target_graph_version must equal the current authoritative VPG version"
            )
        if target < int(getattr(attempt, "graph_version", 0)):
            raise ConfigurationError(
                "target_graph_version cannot precede the Attempt graph version"
            )
        bridge = RebaseRuntimeBridge()
        decision = bridge.plan(
            snapshot,
            context_snapshot=context_snapshot,
            graph_delta=graph_delta,
            current_graph_version=target,
            target_semantic_epoch=target_semantic_epoch,
            harness=harness,
            reason=reason,
        )
        context_identity = getattr(snapshot, "context_identity", None)
        if context_identity is None:
            raise ConfigurationError("AgentSnapshot has no sealed Context identity")
        target_epoch = (
            int(getattr(attempt, "semantic_epoch", 0))
            if target_semantic_epoch is None
            else target_semantic_epoch
        )
        if isinstance(target_epoch, bool) or not isinstance(target_epoch, int) or target_epoch < 0:
            raise ConfigurationError("target_semantic_epoch must be a non-negative integer")
        return LiveContextRebasePlan(
            graph_id=gid,
            graph_version=target,
            source_graph_version=int(getattr(attempt, "graph_version", 0)),
            target_semantic_epoch=target_epoch,
            task_id=normalized_task,
            agent_id=normalized_agent,
            process_id=str(getattr(attempt, "process_id", "")),
            claim_id=normalized_claim,
            attempt_id=str(getattr(attempt, "attempt_id", "")),
            source_semantic_epoch=int(getattr(attempt, "semantic_epoch", 0)),
            context_snapshot_id=str(context_identity.snapshot_id),
            context_snapshot_hash=str(context_identity.materialized_hash),
            agent_snapshot_fingerprint=snapshot.fingerprint(),
            harness_session_id=str(identity.session_id),
            harness_revision=int(getattr(harness_snapshot, "revision", 0)),
            decision=decision,
            graph_delta_hash=decision.plan.context_delta.delta_hash,
            plan_hash=decision.plan.plan_hash,
        )

    async def apply_live_context_rebase(self, plan: Any) -> Any:
        """Apply only the safe portion of a live rebase plan.

        ``REUSE`` may issue a normal fenced Harness ``START``/``CONTINUE``.
        ``REBASE`` and ``FULL_RELOAD`` use a bounded durable
        Scheduler-owned handoff to a **fresh Attempt**.  This is deliberately
        release-then-acquire, not a cross-plane atomic transaction: the old
        Harness is detached, the source Claim is fenced, and callers must
        register a new Harness for the replacement Attempt before execution.
        The method revalidates every identity before entering Harness code and
        never mutates VPG or Context VM.
        """

        from .context_delta import ContextRebaseAction
        from .rebase_runtime import LiveContextRebaseApplyResult, LiveContextRebasePlan

        if not isinstance(plan, LiveContextRebasePlan):
            raise ConfigurationError("plan must be a LiveContextRebasePlan")
        decision = plan.decision
        if (
            decision.graph_id != plan.graph_id
            or decision.task_id != plan.task_id
            or decision.agent_id != plan.agent_id
            or decision.attempt_id != plan.attempt_id
            or decision.plan.plan_hash != plan.plan_hash
            or decision.plan.context_delta.delta_hash != plan.graph_delta_hash
            or decision.plan.old_graph_version != plan.source_graph_version
            or decision.plan.new_graph_version != plan.graph_version
            or decision.freshness.graph_id != plan.graph_id
            or decision.freshness.observed_graph_version != plan.source_graph_version
            or decision.freshness.current_graph_version != plan.graph_version
        ):
            return LiveContextRebaseApplyResult(
                plan=plan,
                refused=True,
                reason="rebase plan integrity or identity check failed",
            )
        # REBASE/FULL_RELOAD handoffs use a deterministic idempotency key.
        # Compute it before the old Claim lookup so a repeated application can
        # recover a previously committed transfer after the source Claim and
        # Harness binding have been detached.
        handoff_id: str | None = None
        if decision.action not in {
            ContextRebaseAction.REUSE,
            ContextRebaseAction.BLOCKED,
        }:
            handoff_id = (
                "live-context-"
                + hashlib.sha256(
                    "|".join(
                        (
                            plan.graph_id,
                            plan.task_id,
                            plan.claim_id,
                            plan.attempt_id,
                            str(plan.graph_version),
                            str(plan.target_semantic_epoch),
                            "rebase",
                        )
                    ).encode("utf-8")
                ).hexdigest()
            )
        claim = self._live_claim(
            plan.graph_id,
            plan.task_id,
            claim_id=plan.claim_id,
            agent_id=plan.agent_id,
        )
        if claim is None:
            if handoff_id is not None:
                recovery_error: str | None = None
                try:
                    recovered = self.recover_handoff(handoff_id)
                except Exception as exc:
                    recovered = None
                    recovery_error = _bounded_audit_error(exc)
                recovered_status = str(getattr(getattr(recovered, "status", None), "value", ""))
                if recovered is not None and recovered_status in {
                    "committed",
                    "replayed",
                }:
                    intent = getattr(recovered, "intent", None)
                    durable_identity = (
                        str(getattr(intent, "graph_id", "")),
                        str(getattr(intent, "task_id", "")),
                        str(getattr(intent, "source_claim_id", "")),
                        str(getattr(intent, "source_attempt_id", "")),
                        str(getattr(intent, "source_agent_id", "")),
                        str(getattr(intent, "replacement_agent_id", "")),
                        int(getattr(intent, "source_graph_version", -1)),
                        int(getattr(intent, "source_semantic_epoch", -1)),
                        str(getattr(intent, "action", "")),
                    )
                    expected_durable_identity = (
                        plan.graph_id,
                        plan.task_id,
                        plan.claim_id,
                        plan.attempt_id,
                        plan.agent_id,
                        plan.agent_id,
                        plan.source_graph_version,
                        plan.source_semantic_epoch,
                        "rebase",
                    )
                    # ``handoff_id`` intentionally excludes the Agent id so
                    # retries can recover after the source Claim is detached.
                    # That also means a forged/tampered plan must not be
                    # accepted solely because it hashes to the same id.
                    if durable_identity != expected_durable_identity:
                        return LiveContextRebaseApplyResult(
                            plan=plan,
                            handoff_result=recovered,
                            handoff_id=handoff_id,
                            handoff_attempted=True,
                            handoff_replayed=True,
                            refused=True,
                            ownership_unchanged=False,
                            reason=(
                                "durable handoff identity does not match "
                                "the requested live rebase plan"
                            ),
                        )
                    return LiveContextRebaseApplyResult(
                        plan=plan,
                        handoff_result=recovered,
                        handoff_id=handoff_id,
                        handoff_attempted=True,
                        handoff_replayed=True,
                        replacement_claim_id=getattr(recovered, "replacement_claim_id", None),
                        replacement_attempt_id=getattr(recovered, "replacement_attempt_id", None),
                        applied=True,
                        replayed=True,
                        ownership_unchanged=False,
                        reason=("fresh Attempt handoff replayed from durable Scheduler history"),
                    )
                # A missing source Claim after a release-first handoff is not
                # equivalent to an ordinary stale lease.  Preserve the
                # durable recovery witness (or the recovery failure) so the
                # caller can distinguish a safe abort from an IN_DOUBT /
                # FAILED_CLOSED transfer.  Previously this branch collapsed
                # every outcome into "fence no longer holds", losing the only
                # evidence needed to resume recovery after a crash.
                if recovered is not None:
                    source_released = getattr(recovered, "source_lease_released", None)
                    fail_closed = recovered_status in {
                        "in_doubt",
                        "failed_closed",
                        "committing",
                        "prepared",
                    }
                    return LiveContextRebaseApplyResult(
                        plan=plan,
                        handoff_result=recovered,
                        handoff_id=handoff_id,
                        handoff_attempted=True,
                        handoff_replayed=recovered_status == "replayed",
                        replacement_claim_id=getattr(recovered, "replacement_claim_id", None),
                        replacement_attempt_id=getattr(recovered, "replacement_attempt_id", None),
                        refused=True,
                        recovery_required=fail_closed,
                        ownership_unchanged=source_released is False,
                        reason=(
                            "durable handoff recovery requires caller action: "
                            + str(getattr(recovered, "reason", "") or recovered_status)
                            if fail_closed
                            else (
                                "durable handoff recovery returned "
                                f"{recovered_status or 'unknown'}: "
                                + str(
                                    getattr(recovered, "reason", "")
                                    or "source Claim is no longer live"
                                )
                            )
                        ),
                    )
                if recovery_error is not None:
                    return LiveContextRebaseApplyResult(
                        plan=plan,
                        handoff_id=handoff_id,
                        handoff_attempted=True,
                        refused=True,
                        recovery_required=True,
                        ownership_unchanged=False,
                        reason=(
                            "live Claim/Lease fence no longer holds; "
                            "durable handoff recovery failed: " + recovery_error
                        ),
                    )
            return LiveContextRebaseApplyResult(
                plan=plan,
                handoff_id=handoff_id,
                refused=True,
                reason="live Claim/Lease fence no longer holds",
            )
        # A live source still requires the exact graph version used at plan
        # time.  This check intentionally happens *after* the source-Claim
        # recovery branch above: a crash-retry may arrive after unrelated VPG
        # progress advanced the graph, but a durable handoff replay remains
        # authoritative and must not be hidden behind a stale-plan error.
        try:
            current_graph_version = int(self._vpg.get_graph(plan.graph_id).current_version)
        except Exception:
            return LiveContextRebaseApplyResult(
                plan=plan,
                handoff_id=handoff_id,
                refused=True,
                reason="authoritative VPG graph no longer exists",
            )
        if plan.graph_version != current_graph_version:
            return LiveContextRebaseApplyResult(
                plan=plan,
                handoff_id=handoff_id,
                refused=True,
                reason="authoritative VPG graph version changed since planning",
            )
        attempt = self._scheduler.attempt_for_claim(plan.claim_id)
        harness = self.harness_for_claim(plan.claim_id)
        if attempt is None or harness is None:
            return LiveContextRebaseApplyResult(
                plan=plan,
                refused=True,
                reason="live Attempt or Harness binding no longer exists",
            )
        snapshot = getattr(attempt, "agent_snapshot", None)
        harness_snapshot = getattr(harness, "snapshot", None)
        identity = getattr(harness_snapshot, "identity", None)
        context_identity = getattr(snapshot, "context_identity", None)
        claim_identity = (
            str(getattr(claim, "graph_id", "")),
            int(getattr(claim, "graph_version", -1)),
            str(getattr(claim, "task_id", "")),
            str(getattr(claim, "agent_id", "")),
            str(getattr(claim, "process_id", "")),
            str(getattr(claim, "claim_id", "")),
        )
        expected_claim_identity = (
            plan.graph_id,
            plan.source_graph_version,
            plan.task_id,
            plan.agent_id,
            plan.process_id,
            plan.claim_id,
        )
        attempt_identity = (
            str(getattr(attempt, "graph_id", "")),
            int(getattr(attempt, "graph_version", -1)),
            str(getattr(attempt, "task_id", "")),
            str(getattr(attempt, "agent_id", "")),
            str(getattr(attempt, "process_id", "")),
            str(getattr(attempt, "claim_id", "")),
            str(getattr(attempt, "attempt_id", "")),
            int(getattr(attempt, "semantic_epoch", -1)),
        )
        expected_attempt_identity = (
            plan.graph_id,
            plan.source_graph_version,
            plan.task_id,
            plan.agent_id,
            plan.process_id,
            plan.claim_id,
            plan.attempt_id,
            plan.source_semantic_epoch,
        )
        harness_identity = (
            str(getattr(identity, "graph_id", "")),
            int(getattr(identity, "graph_version", -1)),
            str(getattr(identity, "task_id", "")),
            str(getattr(identity, "agent_id", "")),
            str(getattr(identity, "claim_id", "")),
            str(getattr(identity, "attempt_id", "")),
            int(getattr(identity, "semantic_epoch", -1)),
        )
        expected_harness_identity = (
            plan.graph_id,
            plan.source_graph_version,
            plan.task_id,
            plan.agent_id,
            plan.claim_id,
            plan.attempt_id,
            plan.source_semantic_epoch,
        )
        if (
            snapshot is None
            or identity is None
            or context_identity is None
            or claim_identity != expected_claim_identity
            or attempt_identity != expected_attempt_identity
            or harness_identity != expected_harness_identity
            or snapshot.fingerprint() != plan.agent_snapshot_fingerprint
            or str(context_identity.snapshot_id) != plan.context_snapshot_id
            or str(context_identity.materialized_hash) != plan.context_snapshot_hash
            or str(identity.session_id) != plan.harness_session_id
            or str(getattr(attempt, "attempt_id", "")) != plan.attempt_id
            or int(getattr(attempt, "semantic_epoch", -1)) != plan.source_semantic_epoch
        ):
            return LiveContextRebaseApplyResult(
                plan=plan,
                refused=True,
                reason="live Context/Harness identity changed since planning",
            )
        if decision.action is ContextRebaseAction.BLOCKED:
            return LiveContextRebaseApplyResult(
                plan=plan,
                refused=True,
                reason=decision.reason or "rebase decision is blocked",
            )
        if decision.action is not ContextRebaseAction.REUSE:
            # The in-place Harness hook is intentionally not called here:
            # changing its cognition basis while retaining the old Claim
            # would create a split-brain ownership window.  Instead, persist a
            # deterministic handoff intent and let the authoritative
            # Scheduler fence the old Attempt before admitting a replacement.
            # FULL_RELOAD is still a semantic invalidation/replacement, not a
            # user-requested cooperative stop.  Record it as ``rebase`` so
            # the source Attempt is durably marked STALE_COGNITION and the
            # replacement remains eligible for the normal repair frontier.
            action_name = "rebase"
            assert handoff_id is not None
            try:
                prepared = self.prepare_handoff(
                    plan.graph_id,
                    plan.task_id,
                    source_claim_id=plan.claim_id,
                    replacement_agent_id=plan.agent_id,
                    expected_attempt_id=plan.attempt_id,
                    expected_semantic_epoch=plan.source_semantic_epoch,
                    handoff_id=handoff_id,
                    action=action_name,
                    reason=decision.reason or "live Context changed",
                )
                committed = self.commit_handoff(prepared.intent)
            except Exception as exc:
                return LiveContextRebaseApplyResult(
                    plan=plan,
                    handoff_id=handoff_id,
                    handoff_attempted=True,
                    refused=True,
                    recovery_required=True,
                    reason=(
                        "fresh Attempt handoff could not be prepared/committed: "
                        + _bounded_audit_error(exc)
                    ),
                )
            status = str(getattr(getattr(committed, "status", None), "value", ""))
            transferred = status in {"committed", "replayed"}
            replacement_claim_id = getattr(committed, "replacement_claim_id", None)
            replacement_attempt_id = getattr(committed, "replacement_attempt_id", None)
            if not transferred:
                return LiveContextRebaseApplyResult(
                    plan=plan,
                    handoff_result=committed,
                    handoff_id=handoff_id,
                    handoff_attempted=True,
                    handoff_replayed=status == "replayed",
                    replacement_claim_id=replacement_claim_id,
                    replacement_attempt_id=replacement_attempt_id,
                    refused=True,
                    recovery_required=status in {"in_doubt", "failed_closed"},
                    reason=(
                        "fresh Attempt handoff was refused/failed closed: "
                        + str(getattr(committed, "reason", "") or status)
                    ),
                )
            # The old Harness is no longer allowed to issue control requests
            # for the fenced Claim.  Detach it best-effort; failure is
            # surfaced as a recovery witness rather than silently ignored.
            detached = self.unregister_harness(
                plan.harness_session_id,
                claim_id=plan.claim_id,
            )
            return LiveContextRebaseApplyResult(
                plan=plan,
                handoff_result=committed,
                handoff_id=handoff_id,
                handoff_attempted=True,
                handoff_replayed=status == "replayed",
                replacement_claim_id=replacement_claim_id,
                replacement_attempt_id=replacement_attempt_id,
                applied=True,
                replayed=status == "replayed",
                refused=False,
                ownership_unchanged=False,
                recovery_required=not detached,
                reason=(
                    "fresh Attempt admitted; old Harness detached; register a "
                    "new Harness for the replacement Attempt"
                    if detached
                    else ("fresh Attempt admitted but old Harness detachment requires recovery")
                ),
            )
        request = decision.control_request
        if request is None:
            return LiveContextRebaseApplyResult(
                plan=plan,
                refused=True,
                reason="reuse decision has no fenced Harness request",
            )
        cache_key = (plan.claim_id, request.request_id)
        replayed = cache_key in self._harness_control_cache
        if int(getattr(harness_snapshot, "revision", -1)) != plan.harness_revision and not replayed:
            return LiveContextRebaseApplyResult(
                plan=plan,
                refused=True,
                reason="live Context/Harness identity changed since planning",
            )
        try:
            result = await self.control_harness(
                plan.claim_id,
                request.operation,
                request_id=request.request_id,
                reason=request.reason,
                payload=request.payload,
            )
        except Exception as exc:
            return LiveContextRebaseApplyResult(
                plan=plan,
                refused=True,
                reason=f"Harness control refused: {_bounded_audit_error(exc)}",
            )
        applied = bool(getattr(result, "applied", False))
        return LiveContextRebaseApplyResult(
            plan=plan,
            harness_result=result,
            applied=applied,
            replayed=replayed and applied,
            refused=not applied,
            ownership_unchanged=True,
            reason=(
                "Harness reuse control replayed"
                if replayed and applied
                else "Harness reuse control applied"
                if applied
                else "Harness rejected reuse control"
            ),
        )

    def plan_frontier(
        self,
        goal: Goal | str,
        *,
        epoch_id: int = 0,
        max_parallelism: int = 1,
        ranking_strategy: FrontierRankingStrategy | str = "repair_first_lexical",
        persist: bool = False,
    ) -> Any:
        """Return an opt-in, read-only WHAT/WHEN frontier suggestion.

        This method deliberately composes :meth:`runtime_state` rather than
        calling ``status`` or ``run_pass``.  Consequently it never compiles a
        missing Goal, creates Claims, acquires Leases, reserves resources, or
        changes the default ``run``/``run_async`` execution loop.  The default
        ranking preserves repair-first lexical behavior; callers may
        explicitly select ``"graph_utility"`` to rank by declared-VPG
        critical-path and immediate downstream-unlock signals.
        """

        from .frontier_policy import FrontierPolicy, FrontierRankingStrategy

        state = self.runtime_state(goal)
        epoch = FrontierPolicy(
            max_parallelism=max_parallelism,
            ranking_strategy=FrontierRankingStrategy(ranking_strategy),
        ).plan(
            state,
            epoch_id=epoch_id,
        )
        if persist:
            if self._read_only:
                raise ConfigurationError("read-only AgentOS cannot persist SchedulingEpoch audits")
            self._persist_scheduling_epoch(epoch)
        return epoch

    def plan_budgeted_frontier(
        self,
        goal: Goal | str,
        estimates: Mapping[str, TaskComputeEstimate] | Iterable[TaskComputeEstimate],
        limits: ComputeBudgetLimits,
        usage: ComputeBudgetUsage | None = None,
        *,
        epoch_id: int = 0,
        max_parallelism: int = 1,
        persist: bool = False,
    ) -> VerifiedProgressBudgetPlan:
        """Return a read-only, graph-fenced budget admission proposal.

        The compute-budget policy is deliberately advisory: this facade only
        observes an already-compiled Goal and delegates to the pure policy.
        It never compiles a missing Goal, creates a Claim, acquires a Kernel
        Lease, reserves resources, or dispatches ``run``/``run_async`` work.
        ``persist=True`` appends the policy's bounded SchedulingEpoch audit
        through the same journal-only path used by :meth:`plan_frontier`.
        """

        from .compute_budget import ComputeBudgetUsage, VerifiedProgressBudgetPolicy

        state = self.runtime_state(goal)
        effective_usage = ComputeBudgetUsage() if usage is None else usage
        plan = VerifiedProgressBudgetPolicy().plan(
            state,
            estimates,
            limits,
            effective_usage,
            epoch_id=epoch_id,
            max_parallelism=max_parallelism,
        )
        if persist:
            if self._read_only:
                raise ConfigurationError("read-only AgentOS cannot persist SchedulingEpoch audits")
            self._persist_scheduling_epoch(plan)
        return plan

    def plan_compute_routing(
        self,
        goal: Goal | str,
        candidate: Any,
        agents: Any = (),
        *,
        reuse_threshold: float = 0.60,
        max_context_budget_tokens: int = 128_000,
    ) -> Any:
        """Return an advisory cognitive-locality/compute-routing decision.

        ``candidate`` and ``agents`` are explicit policy inputs.  This facade
        never parses task descriptions, discovers hidden dependencies, claims
        work, acquires leases, starts a process, or changes the normal
        ``run``/``run_async`` execution path.  Omitting ``agents`` is
        conservative: the policy may use only active cognition snapshots
        already present in the read-only ``RuntimeStateView``.
        """

        from .compute_routing import ComputeRoutingPolicy

        state = self.runtime_state(goal)
        return ComputeRoutingPolicy(
            reuse_threshold=reuse_threshold,
            max_context_budget_tokens=max_context_budget_tokens,
        ).route(
            state,
            candidate,
            agents,
        )

    def suggest_parallel_batch(
        self,
        goal: Goal | str,
        conflict_graph: Any,
        *,
        epoch_id: int = 0,
        max_parallelism: int = 1,
    ) -> Any:
        """Return an opt-in conflict-aware batch suggestion.

        ``conflict_graph`` must be an explicit ``ConflictGraph`` built by the
        caller.  The policy is advisory only; Scheduler eligibility,
        resource admission, Claim, Lease, and fencing remain authoritative.
        """

        from .conflict_graph import DynamicParallelismPolicy

        state = self.runtime_state(goal)
        return DynamicParallelismPolicy(max_parallelism=max_parallelism).suggest(
            state,
            conflict_graph,
            epoch_id=epoch_id,
        )

    def suggest_resource_aware_batch(
        self,
        goal: Goal | str,
        conflict_graph: Any,
        task_resources: Mapping[str, Any] | Iterable[Any] | None = None,
        *,
        requests: Mapping[str, Any] | Iterable[Any] | None = None,
        epoch_id: int = 0,
        max_parallelism: int = 1,
    ) -> Any:
        """Return a read-only resource/conflict-aware batch suggestion.

        The policy combines the immutable ``RuntimeStateView`` logical pool
        projection with explicit task resource requests and a caller-supplied
        ``ConflictGraph``.  When no request map is supplied, the SDK derives
        requests from the compiled ``Task.resources`` declarations.  This is
        still advisory: Scheduler/Kernel revalidate eligibility, resource
        admission, Claims, Leases, and fencing before execution.
        """

        from .resource_policy import ResourceAwareParallelismPolicy

        state = self.runtime_state(goal)
        if task_resources is None and requests is None:
            goal_id = str(getattr(goal, "goal_id", goal)).strip()
            compiled = self._goals.get(goal_id)
            if compiled is not None:
                task_resources = {
                    str(task.task_id): getattr(task, "resources", None) for task in compiled.tasks
                }
        return ResourceAwareParallelismPolicy(max_parallelism=max_parallelism).suggest(
            state,
            conflict_graph,
            task_resources,
            requests=requests,
            epoch_id=epoch_id,
        )

    def plan_interrupts(
        self,
        goal: Goal | str,
        interrupts: Any,
        *,
        epoch_id: int = 0,
        persist: bool = False,
    ) -> Any:
        """Route explicit semantic interrupts into read-only proposals.

        The returned epoch may contain ``PREEMPT`` or ``REBASE`` suggestions,
        but this facade does not deliver signals, stop callbacks, release
        Claims, renew Leases, or acknowledge a rebase.  Set ``persist=True``
        to append a bounded ``SEMANTIC_INTERRUPT_PROPOSED`` audit event.
        Persisting a proposal does not execute it.
        """

        from .semantic_interrupt import SemanticInterruptPolicy

        state = self.runtime_state(goal)
        epoch = SemanticInterruptPolicy().plan(
            state,
            interrupts,
            epoch_id=epoch_id,
        )
        if not persist:
            return epoch
        if self._read_only:
            raise ConfigurationError(
                "read-only AgentOS cannot persist semantic-interrupt proposals"
            )

        # Persist only stable IDs, versions, actions, and hashes.  Prompts,
        # model outputs, artifact contents, and unbounded context are never
        # copied into the Scheduler journal.
        from lhos.runtimes.multi_agent.events import SchedulerEventType, record_event

        event_key = hashlib.sha256(
            f"{epoch.graph_id}|{epoch.epoch_id}|{epoch.decision_hash}".encode()
        ).hexdigest()
        metadata = {
            "schema_version": epoch.schema_version,
            "policy_id": epoch.policy_id,
            "epoch_id": epoch.epoch_id,
            "interrupt_ids": list(epoch.interrupt_ids),
            "decisions": [
                {
                    "target_kind": decision.target_kind,
                    "target_id": decision.target_id,
                    "action": decision.action.value,
                }
                for decision in epoch.decisions
            ],
            "unhandled_interrupt_ids": list(epoch.unhandled_interrupt_ids),
            "unavailable_fields": [field.name for field in epoch.unavailable],
        }
        event_id = f"semantic-interrupt-{event_key}"
        # Reusing a deterministic event identity must not manufacture a new
        # timestamp on retries (the durable journal validates the full event
        # payload).  If the exact proposal is already present, this is a
        # journal idempotent replay; a same-id/different-hash collision fails
        # closed.
        existing = next(
            (event for event in self._scheduler.events if event.event_id == event_id),
            None,
        )
        if existing is not None:
            if (
                existing.event_type is not SchedulerEventType.SEMANTIC_INTERRUPT_PROPOSED
                or existing.decision_hash != epoch.decision_hash
            ):
                raise ConfigurationError(
                    f"conflicting semantic-interrupt proposal event {event_id!r}"
                )
        else:
            self._scheduler.record_event(
                record_event(
                    event_id=event_id,
                    event_type=SchedulerEventType.SEMANTIC_INTERRUPT_PROPOSED,
                    graph_id=epoch.graph_id,
                    graph_version=epoch.graph_version,
                    decision_hash=epoch.decision_hash,
                    reason="semantic interrupt policy proposal",
                    metadata=metadata,
                )
            )
        return epoch

    # ── E3 observability (read-only) ────────────────────────────────────────
    def status_view(self, goal_id: str) -> StatusView:
        from .observability_service import build_status_view

        return build_status_view(self, goal_id)

    def explain(self, goal_id: str, task_id: str) -> list[str]:
        sv = self.status_view(goal_id)
        tv = sv.tasks.get(task_id, {})
        lines = []
        if not tv:
            return [f"task {task_id!r} not found"]
        lines.append(f"Task {task_id}: {tv.get('validity', '?').upper()}")
        if tv.get("validity") == "verified":
            if tv.get("supporting_evidence"):
                lines.append(
                    f"  VERIFIED because: Evidence {tv['supporting_evidence']} PASS "
                    f"binds {tv.get('artifact')}@{tv.get('artifact_version')} and is current"
                )
            else:
                lines.append(
                    "  VERIFIED because: required dependencies valid + applicable Evidence exists"
                )
        elif tv.get("validity") == "stale":
            lines.append(
                f"  STALE because: {tv.get('artifact', '?')} changed version; "
                f"old Evidence not current for the new artifact version"
            )
        elif tv.get("validity") == "unverified":
            lines.append("  UNVERIFIED because: no applicable Evidence yet")
        return lines

    def graph_lines(self, goal_id: str) -> list[str]:
        sv = self.status_view(goal_id)
        gid = self._gid_for(goal_id)
        if gid is None:
            return []
        _, edges = self.vpg.snapshot_projection(gid)
        # goal -> task deps (depends_on) rendered as an indented tree
        deps: dict[str, list[str]] = {}
        roots = []
        for e in edges:
            if e.edge_type.value == "depends_on":
                deps.setdefault(e.source_node_id, []).append(e.target_node_id)
                if e.source_node_id == goal_id:
                    roots.append(e.target_node_id)
        lines = [f"Goal: {goal_id} [{sv.goal_state}]"]
        seen: set[str] = set()
        for root in sorted(roots):
            _render_tree(root, deps, sv, lines, seen, prefix="", is_last=True)
        return lines

    # ── repair (D3) ────────────────────────────────────────────────────────
    def repair(
        self,
        goal: Goal,
        *,
        observation: ObservationToken | dict[str, Any] | str | None = None,
        new_artifact_version: int | None = None,
        artifact_id: str | None = None,
        _seed_task_ids: tuple[str, ...] = (),
        _seed_old_version: int | None = None,
        _seed_previous_token_id: str | None = None,
    ) -> RepairOutcome:
        if self._read_only:
            raise ExecutionError("read-only AgentOS cannot mutate or repair graphs")
        """Run D3 invalidation on a goal and return affected/preserved/frontier.

        If `new_artifact_version` and `artifact_id` are given, the SDK first
        records the new ArtifactVersion (so a subsequent `run` re-verifies with
        the new Evidence).  The D3 cone marks only affected semantic descendants
        STALE and derives the minimal Repair Frontier.
        """
        self._goals.setdefault(goal.goal_id, goal)
        gid = self._gid_for(goal.goal_id, compile_if_missing=True)
        if gid is None:
            raise ConfigurationError(f"goal {goal.goal_id!r} not registered")
        observed_token: ObservationToken | None = None
        if observation is not None:
            try:
                observed_token = ObservationToken.parse(observation)
                self._facts.validate_observation(observed_token, graph_id=gid)
            except (TypeError, ValueError, KeyError) as exc:
                raise ConfigurationError(f"invalid observation token: {exc}", cause=exc) from exc
            # Avoid two independently supplied mutation identities.  Matching
            # values are accepted to make migration from the old API painless;
            # conflicting values fail closed.
            if artifact_id is not None:
                normalized_artifact = FactsProvider.normalize_artifact_id(artifact_id)
                if normalized_artifact != observed_token.artifact_id:
                    raise ConfigurationError(
                        "artifact_id conflicts with observation token artifact_id"
                    )
            if (
                new_artifact_version is not None
                and int(new_artifact_version) != observed_token.version
            ):
                raise ConfigurationError(
                    "new_artifact_version conflicts with observation token version"
                )
            artifact_id = observed_token.artifact_id
            new_artifact_version = observed_token.version
        elif new_artifact_version is not None:
            # The integer-only path is retained for v0.x compatibility, but
            # cannot provide a cryptographic observation proof.  New callers
            # should use ``observe_artifact`` and pass its token to repair.
            warnings.warn(
                "AgentOS.repair(new_artifact_version=...) is an unsafe "
                "compatibility API; use AgentOS.observe_artifact(...).",
                DeprecationWarning,
                stacklevel=2,
            )
        from lhos.runtimes.invalidation.engine import (
            EngineInputs,
            build_invalidation_result,
            run_invalidation_engine,
        )
        from lhos.runtimes.invalidation.models import InvalidationCause

        # Snapshot semantic bindings before mutating Artifact facts.  Repair
        # must be causally grounded in an existing ArtifactRef; never invent a
        # seed task or create an orphan artifact for a typo.
        nodes, edges = self._vpg.snapshot_projection(gid)
        curver = self._vpg.get_graph(gid).current_version
        task_nodes = {n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "task"}
        artifact_refs_by_task: dict[str, list[Any]] = {}
        for edge in edges:
            if edge.edge_type.value != "produces" or edge.source_node_id not in task_nodes:
                continue
            ref = nodes.get(edge.target_node_id)
            if getattr(ref, "node_type", "") == "artifact_ref":
                artifact_refs_by_task.setdefault(edge.source_node_id, []).append(ref)

        # A reconciled external workspace input may be declared on a Task but
        # intentionally has no PRODUCES/ArtifactRef edge in the VPG.  The
        # trusted watcher path supplies the exact seed tasks and predecessor
        # version after validating the graph-bound ObservationToken.  Add a
        # local, non-persisted binding solely so the existing D3 cause
        # construction remains shared with ordinary artifact repair.
        seed_task_ids = tuple(
            sorted({str(item).strip() for item in _seed_task_ids if str(item).strip()})
        )
        if seed_task_ids:
            if observed_token is None or _seed_old_version is None:
                raise ConfigurationError(
                    "internal reconciliation seeds require an observation and previous version"
                )
            if _seed_old_version >= observed_token.version:
                raise ConfigurationError(
                    "reconciliation predecessor must be older than observation"
                )
            canonical_goal = self._goals.get(goal.goal_id)
            if canonical_goal is None:
                raise ConfigurationError("reconciliation Goal is not registered")

            def _declared_resource(value: Any) -> str:
                raw = str(value).strip()
                for prefix in ("workspace://", "vpg://workspace/"):
                    if raw.startswith(prefix):
                        raw = raw[len(prefix) :].lstrip("/")
                        break
                return FactsProvider.normalize_artifact_id(raw)

            canonical_tasks = {
                str(item.task_id): item
                for item in tuple(getattr(canonical_goal, "tasks", ()) or ())
            }
            for task_id in seed_task_ids:
                if task_id not in task_nodes:
                    raise ConfigurationError(
                        f"reconciliation seed task {task_id!r} is not in the Goal"
                    )
                canonical_task = canonical_tasks.get(task_id)
                if canonical_task is None:
                    raise ConfigurationError(
                        f"reconciliation seed task {task_id!r} is not in the registered Goal"
                    )
                declared = tuple(getattr(canonical_task, "inputs", ()) or ()) + tuple(
                    getattr(canonical_task, "outputs", ()) or ()
                )
                if not any(
                    _declared_resource(item) == observed_token.artifact_id for item in declared
                ):
                    raise ConfigurationError(
                        f"reconciliation seed task {task_id!r} does not declare "
                        f"{observed_token.artifact_id!r}"
                    )
                if task_nodes[task_id].validity.value not in {"verified", "stale"}:
                    raise ConfigurationError(
                        f"reconciliation seed task {task_id!r} is neither VERIFIED nor "
                        "already STALE"
                    )
                artifact_refs_by_task.setdefault(task_id, []).append(
                    SimpleNamespace(
                        artifact_id=observed_token.artifact_id,
                        version=int(_seed_old_version),
                    )
                )

        artifact_ids = sorted(
            {
                str(ref.artifact_id)
                for refs in artifact_refs_by_task.values()
                for ref in refs
                if getattr(ref, "artifact_id", None)
            }
        )
        if artifact_id is None:
            if new_artifact_version is None or len(artifact_ids) != 1:
                raise ConfigurationError(
                    "repair requires an explicit artifact_id and new_artifact_version "
                    "when the invalidation cause is not uniquely identifiable"
                )
            artifact_id = artifact_ids[0]

        normalized_artifact_id = FactsProvider.normalize_artifact_id(artifact_id)
        matching_refs = [
            ref
            for refs in artifact_refs_by_task.values()
            for ref in refs
            if FactsProvider.normalize_artifact_id(str(getattr(ref, "artifact_id", "")))
            == normalized_artifact_id
        ]
        if not matching_refs:
            raise ConfigurationError(
                f"cannot repair unknown or unreferenced artifact {normalized_artifact_id!r}"
            )
        artifact_id = normalized_artifact_id

        cur = self._facts.latest(artifact_id) or 0
        if new_artifact_version is not None and new_artifact_version < cur:
            raise ConfigurationError(
                f"cannot repair {artifact_id!r} at stale version "
                f"{new_artifact_version}; current version is {cur}"
            )

        # If the caller omits a version, only an already-recorded newer fact is
        # unambiguous.  Otherwise fail closed rather than guessing a mutation.
        if new_artifact_version is None:
            newver = cur
            if newver <= max(int(getattr(ref, "version", 0)) for ref in matching_refs):
                raise ConfigurationError(f"no newer version recorded for artifact {artifact_id!r}")
        else:
            newver = new_artifact_version

        causes: list[InvalidationCause] = []
        cause_task_ids = seed_task_ids or tuple(sorted(task_nodes))
        for tid in cause_task_ids:
            refs = [
                ref
                for ref in artifact_refs_by_task.get(tid, [])
                if FactsProvider.normalize_artifact_id(str(getattr(ref, "artifact_id", "")))
                == artifact_id
            ]
            if not refs:
                continue
            # A task may have historical refs from multiple repairs.  The
            # highest binding strictly below the new version is the immediate
            # predecessor and therefore the auditable old_version.
            predecessor_versions = [
                int(getattr(ref, "version", 0))
                for ref in refs
                if int(getattr(ref, "version", 0)) < newver
            ]
            if not predecessor_versions:
                continue
            oldver = max(predecessor_versions)
            causes.append(
                InvalidationCause(
                    cause_id=f"c:{artifact_id}:{tid}:v{oldver}-v{newver}",
                    graph_id=gid,
                    graph_version=curver,
                    cause_type="ARTIFACT_VERSION_SUPERSEDED",
                    source_node_id=tid,
                    artifact_id=artifact_id,
                    old_version=oldver,
                    new_version=newver,
                    reason=f"{artifact_id} version {oldver} superseded by {newver}",
                )
            )
        if not causes:
            raise ConfigurationError(
                f"artifact {artifact_id!r} has no prior graph binding below version {newver}"
            )

        # Register a requested new fact only after the semantic cause has been
        # validated.  Equal-to-current means the external mutation was already
        # registered and must not create a duplicate version.
        if (
            observed_token is None
            and new_artifact_version is not None
            and new_artifact_version > cur
        ):
            self._facts.add_version(
                artifact_id,
                new_artifact_version,
                f"body-v{new_artifact_version}",
            )

        goal_nodes = {n.node_id: n for n in nodes.values() if isinstance(n, GoalNode)}
        goal_direct_tasks: dict[str, tuple[str, ...]] = {}
        for goal_node_id in sorted(goal_nodes):
            direct = sorted(
                {
                    edge.target_node_id
                    for edge in edges
                    if edge.edge_type.value == "depends_on"
                    and edge.source_node_id == goal_node_id
                    and edge.target_node_id in task_nodes
                }
            )
            goal_direct_tasks[goal_node_id] = tuple(direct)
        inp = EngineInputs(
            graph_id=gid,
            current_version=curver,
            task_nodes=task_nodes,
            goal_nodes=goal_nodes,
            evidence_nodes={
                n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "evidence"
            },
            edges=edges,
            explicit_causes=tuple(causes),
            goal_direct_tasks=goal_direct_tasks,
        )
        er = run_invalidation_engine(inp)
        ir = build_invalidation_result(inp, er)
        # D3 is a pure derivation over a graph snapshot.  Before publishing
        # the outcome, re-check the optimistic version contract.  A concurrent
        # writer must never receive an affected/frontier report computed from
        # an older snapshot.
        if self._vpg.get_graph(gid).current_version != curver:
            raise ExecutionError(
                f"invalidation computed on graph version {curver}, "
                f"but graph is now at {self._vpg.get_graph(gid).current_version}; "
                "retry repair against the latest graph"
            )
        d3_payload = ir.as_dict()
        d3_payload["record_id"] = f"d3:{gid}:v{curver}:{ir.result_hash}"
        d3_payload["graph_id"] = gid
        d3_payload["base_graph_version"] = curver
        d3_payload["committed_graph_version"] = curver + 1
        if observed_token is not None:
            d3_payload["observation_token"] = observed_token.as_dict()
        if seed_task_ids:
            d3_payload["reconciliation_task_ids"] = list(seed_task_ids)
            d3_payload["previous_observation_token_id"] = _seed_previous_token_id
        outcome = RepairOutcome(
            affected=list(ir.stale_nodes),
            preserved=list(ir.preserved_nodes),
            frontier=[c.task_id for c in ir.frontier.candidates],
            causes=[c.reason for c in ir.causes],
            cause_details=[c.model_dump(mode="json") for c in ir.causes],
        )
        self._vpg.refresh_derived_state(
            gid,
            author_pid=self._owner_pid(),
            reason="D3 artifact/invalidation refresh",
            expected_graph_version=curver,
            idempotency_key=(
                "reconcile-observation-"
                f"{gid}-{observed_token.token_id}-"
                f"{_seed_previous_token_id or ''}-"
                f"{hashlib.sha256('|'.join(seed_task_ids).encode()).hexdigest()[:16]}"
                if seed_task_ids and observed_token is not None
                else None
            ),
            d3_record=d3_payload,
            invalidation_seed_task_ids=seed_task_ids,
            read_guards=(
                ()
                if observed_token is None
                else (
                    ArtifactVersionBinding(
                        canonical_uri=observed_token.canonical_uri,
                        artifact_id=observed_token.artifact_id,
                        version=observed_token.version,
                        content_hash=observed_token.content_hash,
                    ),
                )
            ),
        )
        self._last_repair = outcome
        return outcome

    def reconcile_observation(
        self,
        goal: Goal,
        *,
        observation: ObservationToken | dict[str, Any] | str,
        previous_observation: ObservationToken | dict[str, Any] | str,
        affected_task_ids: Iterable[str] | None = None,
        resource_uri: str | None = None,
    ) -> RepairOutcome:
        """Reconcile one authoritative external-input change into the VPG.

        This is deliberately narrower than :meth:`repair`: workspace inputs
        are often declared on ``Task.inputs`` without a ``PRODUCES`` /
        ``ArtifactRef`` node.  The method therefore accepts only an
        authority-issued previous/current ObservationToken pair and Task ids
        that are explicitly declared consumers of that resource.  It then
        seeds the normal derived-state transaction so the affected tasks,
        causal downstream, Goal lifecycle, and repair frontier update
        atomically.

        It does not discover hidden dependencies, synthesize deletion
        versions, claim/release work, or deliver/force interrupts.
        """

        if self._read_only:
            raise ExecutionError("read-only AgentOS cannot reconcile observations")
        self._goals.setdefault(goal.goal_id, goal)
        canonical_goal = self._goals[goal.goal_id]
        gid = self._gid_for(goal.goal_id, compile_if_missing=True)
        if gid is None:
            raise ConfigurationError(f"goal {goal.goal_id!r} not registered")

        try:
            current = ObservationToken.parse(observation)
            previous = ObservationToken.parse(previous_observation)
            self._facts.validate_observation(current, graph_id=gid)
            self._facts.validate_observation(previous, graph_id=gid)
        except (TypeError, ValueError, KeyError) as exc:
            raise ConfigurationError(f"invalid observation transition: {exc}", cause=exc) from exc

        if current.artifact_id != previous.artifact_id:
            raise ConfigurationError("observation transition must reference one artifact")
        if current.version <= previous.version:
            raise ConfigurationError(
                "observation transition must advance to a newer artifact version"
            )
        if current.content_hash == previous.content_hash:
            raise ConfigurationError("observation transition must change the artifact content hash")
        latest = self._facts.latest(current.artifact_id)
        if latest != current.version:
            raise ConfigurationError(
                f"observation @{current.version} is not the latest authoritative "
                f"version (latest is @{latest})"
            )

        def _resource_identity(value: Any) -> str:
            raw = str(value).strip()
            for prefix in ("workspace://", "vpg://workspace/"):
                if raw.startswith(prefix):
                    raw = raw[len(prefix) :].lstrip("/")
                    break
            return FactsProvider.normalize_artifact_id(raw)

        if resource_uri is not None and _resource_identity(resource_uri) != current.artifact_id:
            raise ConfigurationError(
                "resource_uri conflicts with the observation artifact identity"
            )

        task_by_id = {
            str(task.task_id): task
            for task in tuple(getattr(canonical_goal, "tasks", ()) or ())
            if str(getattr(task, "task_id", "")).strip()
        }
        declared_consumers: set[str] = set()
        for task_id, task in task_by_id.items():
            declared = tuple(getattr(task, "inputs", ()) or ()) + tuple(
                getattr(task, "outputs", ()) or ()
            )
            if any(_resource_identity(item) == current.artifact_id for item in declared):
                declared_consumers.add(task_id)

        if affected_task_ids is None:
            seed_task_ids = tuple(sorted(declared_consumers))
        else:
            if isinstance(affected_task_ids, str):
                affected_task_ids = (affected_task_ids,)
            seed_task_ids = tuple(
                sorted({str(item).strip() for item in affected_task_ids if str(item).strip()})
            )
            unknown = sorted(set(seed_task_ids) - declared_consumers)
            if unknown:
                raise ConfigurationError(
                    "reconciliation task ids are not declared consumers of "
                    f"{current.artifact_id!r}: {', '.join(unknown)}"
                )
        if not seed_task_ids:
            raise ConfigurationError(
                f"no Goal Task explicitly declares workspace/artifact input {current.artifact_id!r}"
            )

        def _replay_existing() -> RepairOutcome | None:
            """Return the durable result for this exact transition, if any.

            The lookup is deliberately repeated after a racing repair fails:
            the first caller commits the VPG projection and D3 envelope in one
            transaction, but the second caller may have taken its semantic
            snapshot just before that commit and subsequently fail the
            ``seed is VERIFIED`` guard.  A deterministic token/previous/task
            identity lets the loser converge to the winner's result.
            """

            for record in reversed(self._vpg.get_d3_results(gid)):
                token_payload = record.get("observation_token")
                if (
                    not isinstance(token_payload, Mapping)
                    or token_payload.get("token_id") != current.token_id
                ):
                    continue
                frontier = record.get("frontier", {})
                causes = record.get("causes", ())
                recorded_previous = str(record.get("previous_observation_token_id") or "")
                recorded_tasks = tuple(
                    sorted(
                        str(item).strip()
                        for item in tuple(record.get("reconciliation_task_ids", ()))
                        if str(item).strip()
                    )
                )
                if recorded_previous != previous.token_id or recorded_tasks != seed_task_ids:
                    raise ConfigurationError(
                        "observation token was already reconciled with a different "
                        "previous token or task scope"
                    )
                return RepairOutcome(
                    affected=list(record.get("stale_nodes", ())),
                    preserved=list(record.get("preserved_nodes", ())),
                    frontier=[
                        str(item.get("task_id"))
                        for item in tuple(frontier.get("candidates", ()))
                        if isinstance(item, Mapping) and str(item.get("task_id", "")).strip()
                    ],
                    causes=[
                        str(item.get("reason", ""))
                        for item in tuple(causes)
                        if isinstance(item, Mapping)
                    ],
                    cause_details=[
                        dict(item) for item in tuple(causes) if isinstance(item, Mapping)
                    ],
                )
            return None

        # Fast path for a retry after a completed reconciliation.
        replayed = _replay_existing()
        if replayed is not None:
            return replayed

        try:
            outcome = self.repair(
                canonical_goal,
                observation=current,
                _seed_task_ids=seed_task_ids,
                _seed_old_version=previous.version,
                _seed_previous_token_id=previous.token_id,
            )
            # ``repair`` may itself have taken the VPG idempotency-replay
            # branch after another caller won the commit.  Its locally
            # computed outcome was based on the loser's newer snapshot, so
            # always prefer the winner's durable D3 envelope when available.
            return _replay_existing() or outcome
        except (ConfigurationError, ExecutionError, VPGError):
            # A concurrent caller may have committed between the fast-path
            # lookup and ``repair``'s seed validation/graph CAS.  Only swallow
            # the error when the exact durable transition is now present;
            # unrelated configuration or graph errors remain fail-closed.
            replayed = _replay_existing()
            if replayed is not None:
                return replayed
            raise

    def reconcile_observations(
        self,
        goal: Goal,
        transitions: Iterable[Any] | None = None,
        *,
        observations: Iterable[Any] | None = None,
        affected_task_ids_by_resource: Mapping[str, Iterable[str]] | None = None,
        task_ids_by_resource: Mapping[str, Iterable[str]] | None = None,
    ) -> RepairOutcome:
        """Atomically reconcile several authoritative observation changes.

        ``transitions`` accepts a sequence of either:

        * ``(previous_token, current_token)``;
        * ``(previous_token, current_token, task_ids)``; or
        * mappings/objects with ``previous_observation`` (or ``previous``),
          ``observation`` (or ``current``), optional ``affected_task_ids`` and
          optional ``resource_uri`` fields.

        Task ids may also be supplied through
        ``affected_task_ids_by_resource`` (the
        ``task_ids_by_resource`` spelling is retained as a compatibility
        alias).  Every transition is fully validated before any VPG write is
        attempted.  A single derived-state refresh then commits all
        invalidation causes, stale propagation, Goal reopening, repair
        frontier, read guards, and one durable D3 envelope in one transaction.

        This is still a *graph-relative* operation: only resources and
        consumers explicitly declared by the registered Goal are admitted.
        It does not discover hidden I/O dependencies, create deletion
        versions, transfer Leases, or interrupt running callbacks.
        """

        if self._read_only:
            raise ExecutionError("read-only AgentOS cannot reconcile observations")
        if transitions is not None and observations is not None:
            raise ConfigurationError("provide either transitions or observations, not both")
        raw_transitions = observations if observations is not None else transitions
        if raw_transitions is None:
            raise ConfigurationError("reconcile_observations requires transitions")
        if isinstance(raw_transitions, (str, bytes, Mapping)):
            raw_transitions = (raw_transitions,)

        # Keep the old name as a strict alias.  Supplying both mappings is
        # almost certainly a caller bug and could otherwise make scope
        # ambiguous.
        if affected_task_ids_by_resource is not None and task_ids_by_resource is not None:
            raise ConfigurationError(
                "provide either affected_task_ids_by_resource or task_ids_by_resource"
            )
        task_map = (
            affected_task_ids_by_resource
            if affected_task_ids_by_resource is not None
            else task_ids_by_resource
        )

        self._goals.setdefault(goal.goal_id, goal)
        canonical_goal = self._goals[goal.goal_id]
        gid = self._gid_for(goal.goal_id, compile_if_missing=True)
        if gid is None:
            raise ConfigurationError(f"goal {goal.goal_id!r} not registered")

        def _resource_identity(value: Any) -> str:
            raw = str(value).strip()
            for prefix in ("workspace://", "vpg://workspace/"):
                if raw.startswith(prefix):
                    raw = raw[len(prefix) :].lstrip("/")
                    break
            return FactsProvider.normalize_artifact_id(raw)

        task_by_id = {
            str(task.task_id): task
            for task in tuple(getattr(canonical_goal, "tasks", ()) or ())
            if str(getattr(task, "task_id", "")).strip()
        }
        declared_by_resource: dict[str, set[str]] = {}
        for task_id, task in task_by_id.items():
            declared = tuple(getattr(task, "inputs", ()) or ()) + tuple(
                getattr(task, "outputs", ()) or ()
            )
            for item in declared:
                try:
                    declared_by_resource.setdefault(_resource_identity(item), set()).add(task_id)
                except (TypeError, ValueError):
                    # Malformed declarations are not silently broadened.  A
                    # transition for such a resource simply fails closed
                    # below because it has no declared consumer.
                    continue

        def _field(value: Any, *names: str, default: Any = None) -> Any:
            if isinstance(value, Mapping):
                for name in names:
                    if name in value:
                        return value[name]
                return default
            for name in names:
                if hasattr(value, name):
                    return getattr(value, name)
            return default

        def _normalize_task_ids(value: Any, resource: str) -> tuple[str, ...]:
            if value is None and task_map is not None:
                # Mapping keys may be supplied as ``workspace://x`` or plain
                # artifact ids; normalize before lookup.
                for key, candidate in task_map.items():
                    try:
                        if _resource_identity(key) == resource:
                            value = candidate
                            break
                    except (TypeError, ValueError):
                        continue
            if value is None:
                values = declared_by_resource.get(resource, set())
            elif isinstance(value, str):
                values = {value}
            else:
                try:
                    values = set(value)
                except TypeError as exc:
                    raise ConfigurationError(
                        f"invalid reconciliation task ids for {resource!r}"
                    ) from exc
            normalized = tuple(sorted({str(item).strip() for item in values if str(item).strip()}))
            unknown = sorted(set(normalized) - declared_by_resource.get(resource, set()))
            if unknown:
                raise ConfigurationError(
                    "reconciliation task ids are not declared consumers of "
                    f"{resource!r}: {', '.join(unknown)}"
                )
            if not normalized:
                raise ConfigurationError(
                    f"no Goal Task explicitly declares workspace/artifact input {resource!r}"
                )
            return normalized

        normalized: list[dict[str, Any]] = []
        for item in raw_transitions:
            previous_raw: Any
            current_raw: Any
            task_ids_raw: Any = None
            resource_uri: str | None = None
            if isinstance(item, (tuple, list)):
                if len(item) not in (2, 3, 4):
                    raise ConfigurationError(
                        "each observation transition tuple must have 2, 3, or 4 items"
                    )
                previous_raw, current_raw = item[0], item[1]
                if len(item) >= 3:
                    task_ids_raw = item[2]
                if len(item) == 4:
                    resource_uri = str(item[3]) if item[3] is not None else None
            else:
                previous_raw = _field(
                    item,
                    "previous_observation",
                    "previous",
                    "previous_token",
                )
                current_raw = _field(
                    item,
                    "observation",
                    "current_observation",
                    "current",
                    "current_token",
                )
                task_ids_raw = _field(
                    item,
                    "affected_task_ids",
                    "task_ids",
                    "seed_task_ids",
                )
                raw_uri = _field(item, "resource_uri", "resource")
                resource_uri = str(raw_uri) if raw_uri is not None else None
            if previous_raw is None or current_raw is None:
                raise ConfigurationError(
                    "each observation transition requires previous and current tokens"
                )
            try:
                current = ObservationToken.parse(current_raw)
                previous = ObservationToken.parse(previous_raw)
                self._facts.validate_observation(current, graph_id=gid)
                self._facts.validate_observation(previous, graph_id=gid)
            except (TypeError, ValueError, KeyError) as exc:
                raise ConfigurationError(
                    f"invalid observation transition: {exc}", cause=exc
                ) from exc
            if current.artifact_id != previous.artifact_id:
                raise ConfigurationError("observation transition must reference one artifact")
            if current.version <= previous.version:
                raise ConfigurationError(
                    "observation transition must advance to a newer artifact version"
                )
            if current.content_hash == previous.content_hash:
                raise ConfigurationError(
                    "observation transition must change the artifact content hash"
                )
            latest = self._facts.latest(current.artifact_id)
            if latest != current.version:
                raise ConfigurationError(
                    f"observation @{current.version} is not the latest authoritative "
                    f"version (latest is @{latest})"
                )
            if resource_uri is not None and _resource_identity(resource_uri) != current.artifact_id:
                raise ConfigurationError(
                    "resource_uri conflicts with the observation artifact identity"
                )
            normalized_task_ids = _normalize_task_ids(task_ids_raw, current.artifact_id)
            normalized.append(
                {
                    "current": current,
                    "previous": previous,
                    "task_ids": normalized_task_ids,
                    "resource_uri": resource_uri,
                    # Idempotency must be tied to the authority's canonical
                    # artifact identity, not to a caller spelling/alias
                    # (`workspace://x`, `vpg://workspace/x`, or omitted).
                    # Keep the supplied URI for audit/display, but use this
                    # stable URI when deriving the batch identity below.
                    "canonical_resource_uri": current.canonical_uri,
                }
            )

        if not normalized:
            raise ConfigurationError("reconcile_observations requires at least one transition")

        # A batch contains at most one version transition per artifact.  Exact
        # duplicate rows are harmless and collapse to one cause; conflicting
        # rows fail before any semantic write.
        by_artifact: dict[str, dict[str, Any]] = {}
        for entry in normalized:
            artifact_id = entry["current"].artifact_id
            prior = by_artifact.get(artifact_id)
            if prior is None:
                by_artifact[artifact_id] = entry
                continue
            if (
                prior["current"].token_id != entry["current"].token_id
                or prior["previous"].token_id != entry["previous"].token_id
                or prior["task_ids"] != entry["task_ids"]
            ):
                raise ConfigurationError(
                    f"multiple conflicting observation transitions for {artifact_id!r}"
                )
        normalized = [by_artifact[key] for key in sorted(by_artifact)]

        nodes, edges = self._vpg.snapshot_projection(gid)
        curver = self._vpg.get_graph(gid).current_version
        task_nodes = {n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "task"}
        for entry in normalized:
            for task_id in entry["task_ids"]:
                if task_id not in task_nodes:
                    raise ConfigurationError(
                        f"reconciliation seed task {task_id!r} is not in the Goal"
                    )
                if task_nodes[task_id].validity.value not in {"verified", "stale"}:
                    raise ConfigurationError(
                        f"reconciliation seed task {task_id!r} is neither VERIFIED nor "
                        "already STALE"
                    )

        from lhos.runtimes.invalidation.engine import (
            EngineInputs,
            build_invalidation_result,
            run_invalidation_engine,
        )
        from lhos.runtimes.invalidation.models import InvalidationCause

        causes: list[InvalidationCause] = []
        aggregate_seed_task_ids: set[str] = set()
        for entry in normalized:
            current = entry["current"]
            previous = entry["previous"]
            for task_id in entry["task_ids"]:
                aggregate_seed_task_ids.add(task_id)
                causes.append(
                    InvalidationCause(
                        cause_id=(
                            f"c:{current.artifact_id}:{task_id}:"
                            f"v{previous.version}-v{current.version}"
                        ),
                        graph_id=gid,
                        graph_version=curver,
                        cause_type="ARTIFACT_VERSION_SUPERSEDED",
                        source_node_id=task_id,
                        artifact_id=current.artifact_id,
                        old_version=previous.version,
                        new_version=current.version,
                        reason=(
                            f"{current.artifact_id} version {previous.version} "
                            f"superseded by {current.version}"
                        ),
                    )
                )

        goal_nodes = {n.node_id: n for n in nodes.values() if isinstance(n, GoalNode)}
        goal_direct_tasks: dict[str, tuple[str, ...]] = {}
        for goal_node_id in sorted(goal_nodes):
            goal_direct_tasks[goal_node_id] = tuple(
                sorted(
                    {
                        edge.target_node_id
                        for edge in edges
                        if edge.edge_type.value == "depends_on"
                        and edge.source_node_id == goal_node_id
                        and edge.target_node_id in task_nodes
                    }
                )
            )
        inp = EngineInputs(
            graph_id=gid,
            current_version=curver,
            task_nodes=task_nodes,
            goal_nodes=goal_nodes,
            evidence_nodes={
                n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "evidence"
            },
            edges=edges,
            explicit_causes=tuple(causes),
            goal_direct_tasks=goal_direct_tasks,
        )
        er = run_invalidation_engine(inp)
        ir = build_invalidation_result(inp, er)
        if self._vpg.get_graph(gid).current_version != curver:
            raise ExecutionError(
                f"invalidation computed on graph version {curver}, "
                f"but graph is now at {self._vpg.get_graph(gid).current_version}; "
                "retry reconciliation against the latest graph"
            )

        transition_payload = [
            {
                "artifact_id": entry["current"].artifact_id,
                "previous_observation_token_id": entry["previous"].token_id,
                "observation_token_id": entry["current"].token_id,
                "previous_version": entry["previous"].version,
                "new_version": entry["current"].version,
                "reconciliation_task_ids": list(entry["task_ids"]),
                "resource_uri": entry["resource_uri"],
                "canonical_resource_uri": entry["canonical_resource_uri"],
            }
            for entry in normalized
        ]
        # Do not let URI aliases alter the durable idempotency identity.  The
        # full transition payload remains in D3 for audit/debugging, while the
        # hash input uses only authority-canonical resource identity.
        identity_payload = [
            {
                "artifact_id": item["artifact_id"],
                "previous_observation_token_id": item["previous_observation_token_id"],
                "observation_token_id": item["observation_token_id"],
                "previous_version": item["previous_version"],
                "new_version": item["new_version"],
                "reconciliation_task_ids": item["reconciliation_task_ids"],
                "canonical_resource_uri": item["canonical_resource_uri"],
            }
            for item in transition_payload
        ]
        batch_material = json.dumps(
            identity_payload,
            sort_keys=True,
            separators=(",", ":"),
        )
        batch_id = hashlib.sha256(f"{gid}|{batch_material}".encode()).hexdigest()
        d3_payload = ir.as_dict()
        d3_payload.update(
            {
                "record_id": f"d3-batch:{gid}:v{curver}:{batch_id}",
                "graph_id": gid,
                "base_graph_version": curver,
                "committed_graph_version": curver + 1,
                "reconciliation_batch_id": batch_id,
                "reconciliation_transitions": transition_payload,
                "reconciliation_task_ids": sorted(aggregate_seed_task_ids),
                # Keep the singular fields for consumers that understand the
                # original one-observation envelope.
                "observation_token": (
                    normalized[0]["current"].as_dict() if len(normalized) == 1 else None
                ),
                "previous_observation_token_id": (
                    normalized[0]["previous"].token_id if len(normalized) == 1 else None
                ),
            }
        )
        outcome = RepairOutcome(
            affected=list(ir.stale_nodes),
            preserved=list(ir.preserved_nodes),
            frontier=[c.task_id for c in ir.frontier.candidates],
            causes=[c.reason for c in ir.causes],
            cause_details=[c.model_dump(mode="json") for c in ir.causes],
        )

        # Replaying the same deterministic batch is safe and returns the
        # durable aggregate result rather than manufacturing another graph
        # version or D3 row.  The lookup is repeated after a racing writer
        # loses the graph-version CAS below, so identical concurrent callers
        # converge just like the singular reconciliation path.
        def _replay_batch_existing() -> RepairOutcome | None:
            for record in reversed(self._vpg.get_d3_results(gid)):
                if record.get("reconciliation_batch_id") != batch_id:
                    continue
                frontier = record.get("frontier", {})
                causes_payload = record.get("causes", ())
                replayed = RepairOutcome(
                    affected=list(record.get("stale_nodes", ())),
                    preserved=list(record.get("preserved_nodes", ())),
                    frontier=[
                        str(item.get("task_id"))
                        for item in tuple(frontier.get("candidates", ()))
                        if isinstance(item, Mapping) and str(item.get("task_id", "")).strip()
                    ],
                    causes=[
                        str(item.get("reason", ""))
                        for item in tuple(causes_payload)
                        if isinstance(item, Mapping)
                    ],
                    cause_details=[
                        dict(item) for item in tuple(causes_payload) if isinstance(item, Mapping)
                    ],
                )
                self._last_repair = replayed
                return replayed
            return None

        replayed = _replay_batch_existing()
        if replayed is not None:
            return replayed

        read_guards = tuple(
            ArtifactVersionBinding(
                canonical_uri=entry["current"].canonical_uri,
                artifact_id=entry["current"].artifact_id,
                version=entry["current"].version,
                content_hash=entry["current"].content_hash,
            )
            for entry in normalized
        )
        try:
            self._vpg.refresh_derived_state(
                gid,
                author_pid=self._owner_pid(),
                reason="D3 batched observation reconciliation",
                expected_graph_version=curver,
                idempotency_key=f"reconcile-observations-{gid}-{batch_id}",
                d3_record=d3_payload,
                invalidation_seed_task_ids=tuple(sorted(aggregate_seed_task_ids)),
                read_guards=read_guards,
            )
        except (ConfigurationError, ExecutionError, VPGError):
            # A concurrent identical batch may have committed after the
            # preflight replay lookup but before this writer acquired the
            # GraphStore CAS.  Converge only when the exact durable batch is
            # now present; unrelated validation/authority failures remain
            # fail-closed.
            replayed = _replay_batch_existing()
            if replayed is not None:
                return replayed
            raise
        self._last_repair = outcome
        return outcome

    def clear_repair(self) -> None:
        """Clear the last D3 repair overlay (used after reclosure)."""
        self._last_repair = None

    # ── real workspace <-> ArtifactVersion bridge (E2, §15/§16) ─────────────
    def register_workspace_artifact(self, workspace, rel: str, version: int) -> str:
        """Register a real workspace file's current bytes as the exact
        ArtifactVersion.  Returns the artifact_id.  (The physical file and the
        Artifact FS authority are kept consistent; the version identity is the
        file content hash + the caller-selected version.)"""
        content = workspace.byte_content(rel)
        self._facts.add_version(rel, version, content)
        return rel

    def workspace_latest_version(self, workspace, rel: str) -> int:
        return self._facts.latest(rel) or 0

    def apply_workspace_mutation(
        self, workspace, rel: str, content: str, *, next_version: int | None = None
    ) -> int:
        if self._read_only:
            raise ExecutionError("read-only AgentOS cannot mutate workspaces")
        """Write to the real workspace, then register the new ArtifactVersion.
        Returns the new version (so D3 later sees applicability loss)."""
        written = workspace.write(rel, content)
        if written is False or getattr(written, "ok", True) is False:
            detail = getattr(written, "error", "")
            suffix = f": {detail}" if detail else ""
            raise ExecutionError(f"workspace write failed for {rel!r}{suffix}")
        ver = next_version or (self._facts.latest(rel) or 0) + 1
        self._facts.add_version(rel, ver, content)
        return ver

    # ── lower-level access (still public, for advanced users / future E3) ──
    @property
    def kernel(self):
        return self._kernel

    @property
    def vpg(self):
        return self._vpg

    def prepare_handoff(
        self,
        graph_id: str,
        task_id: str,
        **kwargs: Any,
    ) -> OwnershipHandoffResult:
        """Durably prepare a bounded Scheduler ownership handoff.

        This convenience wrapper delegates to the Scheduler intent protocol.
        It does not call a Harness, mutate VPG state, or provide cross-plane
        atomicity.
        """

        if self._read_only:
            raise ConfigurationError("read-only AgentOS cannot prepare handoffs")
        return self._scheduler.prepare_handoff(graph_id, task_id, **kwargs)

    def commit_handoff(
        self,
        intent: OwnershipHandoffIntent | OwnershipHandoffResult | Mapping[str, Any],
    ) -> OwnershipHandoffResult:
        """Commit a previously prepared bounded ownership handoff."""

        if self._read_only:
            raise ConfigurationError("read-only AgentOS cannot commit handoffs")
        if isinstance(intent, Mapping):
            intent = dict(intent)
        return self._scheduler.commit_handoff(intent)

    def recover_handoff(self, handoff_id: str) -> OwnershipHandoffResult:
        """Recover a durable handoff intent without guessing ownership."""

        if self._read_only:
            raise ConfigurationError("read-only AgentOS cannot recover handoffs")
        return self._scheduler.recover_handoff(handoff_id)

    @property
    def scheduler(self):
        return self._scheduler

    # ── run persistence for CLI observability (E3) ──────────────────────────
    def save_run(self, manifest_path: str) -> None:
        """Persist a run manifest so a later CLI/process can re-open it read-only.
        (Agent specs + goal graph + db path are stored; kernel/vpg state already
        durable in the db.)"""
        import json

        manifest_file = Path(manifest_path).resolve()
        if self._db_path == ":memory:":
            raise ConfigurationError(
                "cannot save a reopenable run manifest for an in-memory database"
            )
        manifest = {
            "db_path": str(Path(self._db_path).resolve()),
            "goals": {gid: self._serialize_goal(g) for gid, g in self._goals.items()},
            "agents": [
                {
                    "name": a.name,
                    "specializations": list(a.specializations),
                    "max_concurrency": a.max_concurrency,
                    "cost_weight": a.cost_weight,
                    "resource_capacity": a.resource_capacity.model_dump(mode="json"),
                }
                for a in self._agents.values()
            ],
        }
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        with open(manifest_file, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

    def _serialize_goal(self, g: Goal) -> dict:
        return {
            "goal_id": g.goal_id,
            "graph_id": self._goal_gid.get(g.goal_id),
            "tasks": [
                {
                    "task_id": t.task_id,
                    "agent": t.agent,
                    "depends_on": list(t.dependency_ids),
                    "task_kind": t.task_kind,
                    "required_specializations": list(t.required_specializations),
                    "required_tools": list(t.required_tools),
                    "max_attempts": t.max_attempts,
                    "metadata": t.metadata,
                    "resources": t.resources.model_dump(mode="json"),
                    "context_manifest": (
                        None
                        if t.context_manifest is None
                        else t.context_manifest.model_dump(mode="json")
                    ),
                }
                for t in g.tasks
            ],
        }

    @classmethod
    def open_run(cls, manifest_path: str) -> AgentOS:
        """Re-open a saved run (read-only observability; does not re-run)."""
        import json

        with open(manifest_path, encoding="utf-8") as f:
            m = json.load(f)
        manifest = Path(manifest_path).resolve()
        raw_db_path = m.get("db_path", ":memory:")
        if raw_db_path != ":memory:":
            db_path = (
                str((manifest.parent / raw_db_path).resolve())
                if not Path(raw_db_path).is_absolute()
                else raw_db_path
            )
        else:
            db_path = raw_db_path
        os_ = cls(db_path, read_only=True)
        for agent_spec in m.get("agents", []):
            agent = Agent(
                agent_spec["name"],
                specializations=tuple(agent_spec.get("specializations", ["python"])),
                max_concurrency=agent_spec.get("max_concurrency", 4),
                cost_weight=agent_spec.get("cost_weight", 1.0),
                resource_capacity=agent_spec.get("resource_capacity"),
            )
            # Preserve manifest configuration for inspection/re-serialization,
            # but do not create a process or register a runnable descriptor.
            os_._agents[agent.name] = agent
        for gid, gm in m.get("goals", {}).items():
            g = os_.goal(gid)
            stored_gid = gm.get("graph_id")
            graph_exists = False
            if stored_gid:
                try:
                    os_.vpg.get_graph(stored_gid)
                    graph_exists = True
                except Exception:
                    graph_exists = False
            if stored_gid and graph_exists:
                # Reuse the durable graph; do NOT re-compile (keeps verified state).
                os_._goal_gid[gid] = stored_gid
                for t in gm.get("tasks", []):
                    deps = [
                        next((tt for tt in g.tasks if tt.task_id == d), None)
                        for d in t.get("depends_on", [])
                    ]
                    g.task(
                        t["task_id"],
                        agent=t.get("agent", ""),
                        depends_on=tuple(x for x in deps if x is not None),
                        task_kind=t.get("task_kind", "task"),
                        required_specializations=tuple(
                            t.get("required_specializations", ["python"])
                        ),
                        required_tools=tuple(t.get("required_tools", [])),
                        max_attempts=t.get("max_attempts", 3),
                        metadata=t.get("metadata", {}),
                        resources=t.get("resources"),
                        context_manifest=t.get("context_manifest"),
                    )
            else:
                raise ConfigurationError(
                    f"stored graph {stored_gid!r} for goal {gid!r} is missing; "
                    "read-only recovery will not rebuild or mutate the run"
                )
        return os_


# ergonomic alias
OS = AgentOS


def _render_tree(node_id: str, deps, sv, lines, seen, prefix: str, is_last: bool) -> None:
    if node_id in seen:
        return
    seen.add(node_id)
    tv = sv.tasks.get(node_id, {})
    mark = {"verified": "v", "stale": "x", "unverified": ".", "invalid": "!"}.get(
        tv.get("validity", ""), "?"
    )
    star = " * REPAIR" if tv.get("in_repair_frontier") else ""
    branch = "`-- " if is_last else "|-- "
    lines.append(f"{prefix}{branch}{mark} {node_id} [{tv.get('validity', '?').upper()}]{star}")
    children = sorted(deps.get(node_id, []))
    child_prefix = prefix + ("    " if is_last else "|   ")
    for i, c in enumerate(children):
        _render_tree(c, deps, sv, lines, seen, child_prefix, is_last=(i == len(children) - 1))


def _invoke_executor(
    executor: Any,
    task_id: str,
    *,
    context: ExecutionContext | None = None,
    executor_api: str = "legacy_task_id",
) -> Any:
    """Call documented one-arg executors or zero-arg Verifier-style executors.

    Signature binding is done before invocation so an executor's *internal*
    TypeError is never mistaken for an arity mismatch and retried.
    """
    if executor_api == "context_v1":
        return _require_sync_executor_result(
            _invoke_callable_with_context(executor, task_id, context)
        )
    try:
        signature = inspect.signature(executor)
    except (TypeError, ValueError):
        result = executor(task_id)
        return _require_sync_executor_result(result)
    try:
        signature.bind(task_id)
    except TypeError as one_arg_error:
        try:
            signature.bind()
        except TypeError:
            raise ConfigurationError(
                "Agent.executor must accept either task_id or no arguments",
                cause=one_arg_error,
            ) from one_arg_error
        result = executor()
    else:
        result = executor(task_id)
    return _require_sync_executor_result(result)


async def _invoke_executor_async(
    executor: Any,
    task_id: str,
    *,
    context: ExecutionContext | None = None,
    executor_api: str = "legacy_task_id",
) -> Any:
    """Invoke a documented executor without blocking the event loop."""
    if executor_api == "context_v1":
        return await _invoke_callable_with_context_async(executor, task_id, context)
    try:
        signature = inspect.signature(executor)
    except (TypeError, ValueError):
        if _is_async_callable(executor):
            return await executor(task_id)
        return await asyncio.to_thread(executor, task_id)
    try:
        signature.bind(task_id)
    except TypeError as one_arg_error:
        try:
            signature.bind()
        except TypeError:
            raise ConfigurationError(
                "Agent.executor must accept either task_id or no arguments",
                cause=one_arg_error,
            ) from one_arg_error
        args: tuple[Any, ...] = ()
    else:
        args = (task_id,)

    if _is_async_callable(executor):
        return await executor(*args)
    result = await asyncio.to_thread(executor, *args)
    if inspect.isawaitable(result):
        return await result
    return result


def _invoke_verifier_sync(
    verifier: Any,
    *,
    task_id: str = "",
    context: ExecutionContext | None = None,
    executor_api: str = "legacy_task_id",
) -> Any:
    """Call a synchronous Task verifier and reject hidden awaitables.

    ``AgentOS.run`` intentionally remains a synchronous API.  A verifier that
    returns a coroutine (whether it is declared ``async`` or wrapped by a
    synchronous callable) is rejected before semantic commit and its
    coroutine is closed to avoid an un-awaited-coroutine warning.
    """
    if executor_api == "context_v1":
        outcome = _invoke_callable_with_context(verifier, task_id, context)
    else:
        outcome = verifier()
    if inspect.isawaitable(outcome):
        close = getattr(outcome, "close", None)
        if callable(close):
            close()
        raise ConfigurationError(
            "Task.verify returned an awaitable in AgentOS.run; use `await AgentOS.run_async(...)`"
        )
    return outcome


async def _invoke_verifier_async(
    verifier: Any,
    *,
    task_id: str = "",
    context: ExecutionContext | None = None,
    executor_api: str = "legacy_task_id",
) -> Any:
    """Invoke a sync or async Task verifier without blocking the event loop."""
    if executor_api == "context_v1":
        return await _invoke_callable_with_context_async(verifier, task_id, context)
    if _is_async_callable(verifier):
        return await verifier()
    outcome = await asyncio.to_thread(verifier)
    if inspect.isawaitable(outcome):
        return await outcome
    return outcome


def _callable_accepts_context(callable_: Any) -> bool:
    """Whether a callback can receive ``(context, task_id)`` or ``context``."""
    try:
        sig = inspect.signature(callable_)
    except (TypeError, ValueError):
        return False
    for args in ((context_placeholder := object(), ""), (context_placeholder,), ("",)):
        try:
            sig.bind(*args)
            if len(args) >= 1 and args[0] is context_placeholder:
                return len(args) in (1, 2)
        except TypeError:
            continue
    return False


def _invoke_callable_with_context(
    callable_: Any,
    task_id: str,
    context: ExecutionContext | None,
) -> Any:
    """Invoke context_v1 callbacks while retaining a small compatibility form."""
    if context is None:
        raise ConfigurationError("context_v1 callback requires an ExecutionContext")
    try:
        sig = inspect.signature(callable_)
    except (TypeError, ValueError):
        return callable_(context, task_id)
    candidates = [(context, task_id), (context,)]
    if not context.secure_mode:
        candidates.append((task_id,))
    for args in candidates:
        try:
            sig.bind(*args)
        except TypeError:
            continue
        return callable_(*args)
    accepted = (
        "(context, task_id) or (context)"
        if context.secure_mode
        else ("(context, task_id), (context), or (task_id)")
    )
    raise ConfigurationError(f"context_v1 callback must accept {accepted}")


async def _invoke_callable_with_context_async(
    callable_: Any,
    task_id: str,
    context: ExecutionContext | None,
) -> Any:
    if context is None:
        raise ConfigurationError("context_v1 callback requires an ExecutionContext")
    args: tuple[Any, ...] | None = None
    try:
        sig = inspect.signature(callable_)
    except (TypeError, ValueError):
        args = (context, task_id)
    else:
        candidates = [(context, task_id), (context,)]
        if not context.secure_mode:
            candidates.append((task_id,))
        for candidate in candidates:
            try:
                sig.bind(*candidate)
            except TypeError:
                continue
            args = candidate
            break
        if args is None:
            accepted = (
                "(context, task_id) or (context)"
                if context.secure_mode
                else ("(context, task_id), (context), or (task_id)")
            )
            raise ConfigurationError(f"context_v1 callback must accept {accepted}")
    if _is_async_callable(callable_):
        result = callable_(*args)
        return await result if inspect.isawaitable(result) else result
    result = await asyncio.to_thread(callable_, *args)
    if inspect.isawaitable(result):
        return await result
    return result


def _require_sync_executor_result(result: Any) -> Any:
    """Reject an awaitable returned through an otherwise synchronous wrapper."""
    if inspect.isawaitable(result):
        close = getattr(result, "close", None)
        if callable(close):
            close()
        raise ConfigurationError(
            "Agent.executor returned an awaitable in synchronous AgentOS.run; "
            "use `await AgentOS.run_async(...)`"
        )
    return result
