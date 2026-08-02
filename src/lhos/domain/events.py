"""Runtime events (spec section 5). The event log is the source of truth."""

from datetime import datetime
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class EventType:
    """Recommended event type constants (spec section 5.2)."""

    RUN_CREATED = "RUN_CREATED"
    GOAL_REGISTERED = "GOAL_REGISTERED"
    NODE_ADDED = "NODE_ADDED"
    NODE_UPDATED = "NODE_UPDATED"
    NODE_STATE_CHANGED = "NODE_STATE_CHANGED"
    EDGE_ADDED = "EDGE_ADDED"
    EDGE_REMOVED = "EDGE_REMOVED"
    NODE_LEASED = "NODE_LEASED"
    NODE_LEASE_RELEASED = "NODE_LEASE_RELEASED"
    EXECUTION_STARTED = "EXECUTION_STARTED"
    EXECUTION_FINISHED = "EXECUTION_FINISHED"
    EXECUTION_FAILED = "EXECUTION_FAILED"
    TOOL_CALL_REQUESTED = "TOOL_CALL_REQUESTED"
    TOOL_CALL_COMPLETED = "TOOL_CALL_COMPLETED"
    TOOL_CALL_FAILED = "TOOL_CALL_FAILED"
    ARTIFACT_CREATED = "ARTIFACT_CREATED"
    ARTIFACT_UPDATED = "ARTIFACT_UPDATED"
    FACT_OBSERVED = "FACT_OBSERVED"
    CONSTRAINT_CHANGED = "CONSTRAINT_CHANGED"
    CLAIM_SUBMITTED = "CLAIM_SUBMITTED"
    VERIFICATION_STARTED = "VERIFICATION_STARTED"
    VERIFICATION_PASSED = "VERIFICATION_PASSED"
    VERIFICATION_FAILED = "VERIFICATION_FAILED"
    NODE_MARKED_STALE = "NODE_MARKED_STALE"
    NODE_INVALIDATED = "NODE_INVALIDATED"
    INVALIDATION_PROPAGATED = "INVALIDATION_PROPAGATED"
    CHECKPOINT_CREATED = "CHECKPOINT_CREATED"
    CHECKPOINT_RESTORED = "CHECKPOINT_RESTORED"
    BUDGET_UPDATED = "BUDGET_UPDATED"
    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    RUN_PAUSED = "RUN_PAUSED"
    RUN_RESUMED = "RUN_RESUMED"
    RUN_COMPLETED = "RUN_COMPLETED"
    RUN_FAILED = "RUN_FAILED"
    RUN_ABORTED = "RUN_ABORTED"


class ActorType:
    SYSTEM = "system"
    PLANNER = "planner"
    WORKER = "worker"
    SCHEDULER = "scheduler"
    VERIFIER = "verifier"
    RECONCILER = "reconciler"
    CLI = "cli"


class RuntimeEvent(BaseModel):
    """Append-only fact (spec section 5.1).

    ``sequence`` is assigned by the event store on append; pass 0 when
    constructing a new event.
    """

    id: str = Field(default_factory=lambda: uuid4().hex)
    run_id: str
    sequence: int = 0
    event_type: str
    actor_type: str
    actor_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    evidence_ids: list[str] = Field(default_factory=list)
    causation_id: str | None = None
    correlation_id: str | None = None
    idempotency_key: str | None = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now().astimezone()
    )
