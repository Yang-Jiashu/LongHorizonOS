"""Scheduler Event model + event-type enumeration.

Events form an append-only audit log of everyScheduler decision.
They MUST NOT carry full prompts, model outputs, artifact contents,
or full Context snapshots — only IDs, hashes, versions, proof references,
and reason strings (Section 28).
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _uuid() -> str:
    from uuid import uuid4

    return uuid4().hex


class SchedulerEventType(StrEnum):
    # Agent lifecycle
    AGENT_REGISTERED = "agent_registered"
    AGENT_ENABLED = "agent_enabled"
    AGENT_DISABLED = "agent_disabled"
    AGENT_REMOVED = "agent_removed"

    # Eligibility / matching
    ELIGIBILITY_EVALUATED = "eligibility_evaluated"
    MATCH_DECISION_CREATED = "match_decision_created"

    # Claim lifecycle
    CLAIM_PROPOSED = "claim_proposed"
    CLAIM_LEASE_ACQUIRED = "claim_Lease_acquired"
    CLAIM_LEASE_RENEWED = "claim_lease_renewed"
    CLAIM_ACTIVATED = "claim_activated"
    CLAIM_RELEASED = "claim_released"
    CLAIM_LOST = "claim_lost"
    CLAIM_COMPLETED = "claim_completed"
    CLAIM_REJECTED = "claim_rejected"

    # Execution lifecycle
    EXECUTION_DISPATCHED = "execution_dispatched"
    EXECUTION_STARTED = "execution_started"
    EXECUTION_FAILED = "execution_failed"
    EXECUTION_CRASHED = "execution_crashed"
    EXECUTION_OPERATIONALLY_SUCCEEDED = "execution_operationally_succeeded"
    EXECUTION_SEMANTICALLY_VERIFIED = "execution_semantically_verified"
    EXECUTION_SNAPSHOT_BOUND = "execution_snapshot_bound"
    EXECUTION_STALE_COGNITION = "execution_stale_cognition"
    # A worker may lose the ability to release its exact Claim/Lease while
    # propagating cancellation (for example, a provider outage or an
    # unexpected BaseException).  These events are durable *cleanup
    # bookkeeping* only: they never imply that ownership was released.
    EXECUTION_CLEANUP_REQUIRED = "execution_cleanup_required"
    EXECUTION_CLEANUP_RESOLVED = "execution_cleanup_resolved"
    EXECUTION_CLEANUP_SUPERSEDED = "execution_cleanup_superseded"

    # Online policy observations.  These events are proposals/audit records;
    # they do not by themselves stop callbacks, release Claims, or transfer
    # Kernel Leases.
    SEMANTIC_INTERRUPT_PROPOSED = "semantic_interrupt_proposed"
    SEMANTIC_INTERRUPT_ACKNOWLEDGED = "semantic_interrupt_acknowledged"
    # Harness control remains an explicit, in-process adapter boundary.  The
    # event records the request/result identity and bounded status; it never
    # implies that Scheduler Claims or Kernel Leases were changed.
    HARNESS_CONTROL = "harness_control"
    # Adaptive WHAT/WHEN policy observations.  This is a journal-only record:
    # it captures the immutable SchedulingEpoch proposal but never claims
    # tasks, acquires leases, or mutates semantic VPG state.
    SCHEDULING_EPOCH_PLANNED = "scheduling_epoch_planned"

    # Reassignment / recovery
    # Bounded ownership-handoff intent protocol.  These events are durable
    # Scheduler records only; they do not claim that Kernel/Harness/VPG
    # mutations were committed atomically.
    TASK_HANDOFF_PREPARED = "task_handoff_prepared"
    TASK_HANDOFF_COMMITTING = "task_handoff_committing"
    TASK_HANDOFF_COMMITTED = "task_handoff_committed"
    TASK_HANDOFF_RECOVERY = "task_handoff_recovery"
    TASK_REASSIGNMENT_STARTED = "task_reassignment_started"
    TASK_REASSIGNED = "task_reassigned"

    SCHEDULER_RECOVERY_STARTED = "scheduler_recovery_started"
    SCHEDULER_RECOVERY_COMPLETED = "scheduler_recovery_completed"


class SchedulerEvent(BaseModel):
    """Immutable append-only record of one Scheduler decision / observation."""

    event_id: str = Field(default_factory=_uuid)
    event_type: SchedulerEventType
    graph_id: str = ""
    task_id: str = ""
    agent_id: str = ""
    claim_id: str = ""
    attempt_id: str = ""

    # References (IDs + versions only — never full payloads)
    event_version: int = 0
    graph_version: int = 0
    readiness_graph_version: int | None = None
    decision_hash: str = ""
    claim_state: str = ""

    reason: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)

    created_at: datetime = Field(default_factory=_utcnow)


def record_event(
    event_type: SchedulerEventType,
    *,
    event_id: str | None = None,
    graph_id: str = "",
    task_id: str = "",
    agent_id: str = "",
    claim_id: str = "",
    attempt_id: str = "",
    event_version: int = 0,
    graph_version: int = 0,
    readiness_graph_version: int | None = None,
    decision_hash: str = "",
    claim_state: str = "",
    reason: str = "",
    metadata: dict[str, Any] | None = None,
) -> SchedulerEvent:
    """Convenience factory that normalises empty metadata."""
    return SchedulerEvent(
        event_id=event_id or _uuid(),
        event_type=event_type,
        graph_id=graph_id,
        task_id=task_id,
        agent_id=agent_id,
        claim_id=claim_id,
        attempt_id=attempt_id,
        event_version=event_version,
        graph_version=graph_version,
        readiness_graph_version=readiness_graph_version,
        decision_hash=decision_hash,
        claim_state=claim_state,
        reason=reason,
        metadata=dict(metadata or {}),
    )
