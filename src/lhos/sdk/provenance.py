"""SDK compatibility helpers for mediated provenance.

The public SDK historically accepted callbacks of the form
``executor(task_id)``.  Those callbacks cannot prove which files, APIs, or
tools they touched, so the compatibility path is deliberately represented as
``UNKNOWN`` provenance.  New ``context_v1`` callbacks receive an
``ExecutionContext`` and can record mediated observations explicitly.

This module is intentionally a small adapter layer.  It does not mutate VPG
state or attach Evidence; the AgentOS composition root can use the returned
coverage report/decision immediately before its normal semantic commit.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from lhos.provenance import (
    ActionGateway,
    CoverageDecision,
    CoveragePolicy,
    CoverageReport,
    ExecutionContext,
    InMemoryProvenanceStore,
    ProvenanceEvent,
    ProvenanceOperation,
    assess_coverage,
    evaluate_coverage,
)

from .errors import ConfigurationError, VerificationError
from .task import ExecutorAPI, _coerce_executor_api, _coerce_provenance_policy

LEGACY_RESOURCE_HINT = "legacy://executor"


def resolve_executor_api(
    *,
    task: Any | None = None,
    agent: Any | None = None,
    explicit: ExecutorAPI | str | None = None,
    default: ExecutorAPI | str = "legacy_task_id",
) -> ExecutorAPI:
    """Resolve the callback convention deterministically.

    Precedence is ``explicit > task.executor_api > agent.executor_api >
    default``.  ``None`` means "not specified" and is skipped.  This keeps
    old Agents/tasks source-compatible while allowing a task to opt into
    ``context_v1`` independently of its Agent's default.
    """

    for value, field_name in (
        (explicit, "executor_api"),
        (getattr(task, "executor_api", None), "Task.executor_api"),
        (getattr(agent, "executor_api", None), "Agent.executor_api"),
        (default, "executor_api"),
    ):
        if value is None:
            continue
        resolved = _coerce_executor_api(value, field_name=field_name)
        if resolved is None:  # defensive; allow_none is not used above
            continue
        return resolved
    return "legacy_task_id"


def create_execution_context(
    graph_id: str,
    task_id: str,
    *,
    claim_id: str = "",
    attempt_id: str = "",
    semantic_epoch: int = 0,
    store: Any | None = None,
    source: str = "sdk",
    executor_api: ExecutorAPI | str = "context_v1",
    secure_mode: bool = False,
    action_gateway: ActionGateway | Any | None = None,
    context_snapshot: Any | None = None,
    context_manifest_id: str = "",
    context_handle: Any | None = None,
    loaded_context: Any | None = None,
) -> ExecutionContext:
    """Create an execution context carrying ownership/provenance metadata.

    ``claim_id`` is retained as a context attribute for the SDK commit layer;
    the standalone provenance event schema remains intentionally independent
    of Kernel ownership fields.  For the legacy callback convention an
    explicit unknown observation is persisted immediately.
    """

    api = resolve_executor_api(explicit=executor_api)
    if secure_mode and api == "legacy_task_id":
        raise ConfigurationError(
            "secure_mode requires executor_api='context_v1'; "
            "legacy_task_id callbacks cannot receive the mediated effect boundary"
        )
    context = ExecutionContext(
        str(graph_id),
        task_id=str(task_id),
        attempt_id=str(attempt_id),
        semantic_epoch=int(semantic_epoch),
        store=store or InMemoryProvenanceStore(),
        source=source,
        secure_mode=secure_mode,
        action_gateway=action_gateway,
    )
    # ExecutionContext is a lightweight Python facade, so these attributes
    # remain available to an AgentOS adapter without changing event wire data.
    context.claim_id = str(claim_id)
    context.executor_api = api
    if context_snapshot is not None:
        context.bind_context_snapshot(
            snapshot_id=str(context_snapshot.snapshot_id),
            manifest_id=str(context_manifest_id),
            manifest_hash=str(context_snapshot.manifest_hash),
            working_set_hash=str(context_snapshot.working_set_hash),
            materialized_hash=str(context_snapshot.materialized_hash),
            handle=context_handle,
            loaded_context=loaded_context,
            snapshot=context_snapshot,
        )
    if api == "legacy_task_id":
        context.observe_unknown(
            resource_hint=LEGACY_RESOURCE_HINT,
            executor_api=api,
        )
    return context


def ensure_legacy_unknown(context: ExecutionContext) -> ProvenanceEvent:
    """Ensure a legacy context has an explicit UNKNOWN observation."""

    for event in context.events:
        if not event.known:
            return event
    return context.observe_unknown(
        resource_hint=LEGACY_RESOURCE_HINT,
        executor_api="legacy_task_id",
    )


def _declared_inputs(task_or_inputs: Any) -> tuple[str, ...]:
    if task_or_inputs is None:
        return ()
    raw = getattr(task_or_inputs, "declared_inputs", None)
    if raw is None:
        raw = getattr(task_or_inputs, "inputs", task_or_inputs)
    if isinstance(raw, Mapping):
        raw = raw.keys()
    elif isinstance(raw, str):
        raw = (raw,)
    return tuple(sorted({str(value).strip() for value in raw if str(value).strip()}))


def build_coverage_report(
    task_or_inputs: Any,
    context: ExecutionContext | Iterable[ProvenanceEvent],
    *,
    graph_id: str | None = None,
    task_id: str | None = None,
    executor_api: ExecutorAPI | str | None = None,
    required_operations: Iterable[str | ProvenanceOperation] | None = None,
) -> CoverageReport:
    """Build a deterministic coverage report for one SDK execution."""

    task = task_or_inputs if hasattr(task_or_inputs, "task_id") else None
    events = context.events if isinstance(context, ExecutionContext) else tuple(context)
    resolved_api = resolve_executor_api(
        task=task,
        explicit=executor_api,
        default=getattr(context, "executor_api", "context_v1"),
    )
    if isinstance(context, ExecutionContext) and resolved_api == "legacy_task_id":
        ensure_legacy_unknown(context)
        events = context.events

    resolved_graph = graph_id or str(getattr(context, "graph_id", "") or "")
    resolved_task = task_id or str(getattr(task_or_inputs, "task_id", "") or "")
    return assess_coverage(
        _declared_inputs(task_or_inputs),
        events,
        graph_id=resolved_graph,
        task_id=resolved_task,
        required_operations=required_operations,
    )


def evaluate_verification_coverage(
    task: Any,
    report: CoverageReport,
    *,
    executor_api: ExecutorAPI | str | None = None,
    policy: CoveragePolicy | str | None = None,
) -> CoverageDecision:
    """Apply the task's provenance policy before semantic verification."""

    resolved_api = resolve_executor_api(task=task, explicit=executor_api)
    selected_policy = (
        _coerce_provenance_policy(policy, field_name="provenance_policy")
        if policy is not None
        else _coerce_provenance_policy(
            getattr(task, "provenance_policy", CoveragePolicy.LEGACY),
            field_name="Task.provenance_policy",
        )
    )
    # A legacy callback is intrinsically UNKNOWN even when an adapter passed
    # an accidentally empty/COMPLETE report.  Never allow strict mode to
    # promote that compatibility path.
    if resolved_api == "legacy_task_id" and report.status != "UNKNOWN":
        report = report.model_copy(
            update={
                "status": "UNKNOWN",
                "unknown_inputs": tuple(
                    sorted(set(report.unknown_inputs) | {LEGACY_RESOURCE_HINT})
                ),
                "warnings": tuple(
                    [*report.warnings, "legacy_task_id executor has no mediated read-set"]
                ),
                "report_hash": "",
            }
        ).with_hash()
    return evaluate_coverage(report, selected_policy)


def enforce_verification_coverage(
    task: Any,
    report: CoverageReport,
    *,
    executor_api: ExecutorAPI | str | None = None,
    policy: CoveragePolicy | str | None = None,
) -> CoverageDecision:
    """Fail closed for strict/ineligible provenance before VERIFIED commit."""

    decision = evaluate_verification_coverage(
        task,
        report,
        executor_api=executor_api,
        policy=policy,
    )
    if not decision.allowed:
        error = VerificationError(
            "provenance coverage denied semantic verification: "
            + ("; ".join(decision.reasons) or decision.status)
        )
        # Additive diagnostic for callers that need to expose the exact report.
        error.coverage_decision = decision  # type: ignore[attr-defined]
        raise error
    return decision


# Short aliases used by integrations while the SDK API settles.
new_execution_context = create_execution_context
coverage_report_for = build_coverage_report
gate_verification = enforce_verification_coverage


__all__ = [
    "LEGACY_RESOURCE_HINT",
    "build_coverage_report",
    "coverage_report_for",
    "create_execution_context",
    "enforce_verification_coverage",
    "ensure_legacy_unknown",
    "evaluate_verification_coverage",
    "gate_verification",
    "new_execution_context",
    "resolve_executor_api",
]
