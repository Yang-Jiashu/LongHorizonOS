"""Core domain models (spec sections 4.4, 4.5, 11.3, 19)."""

from datetime import datetime
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field

from lhos.domain.enums import EdgeKind, NodeKind, NodeState


def _utcnow() -> datetime:
    return datetime.now().astimezone()


class GraphNode(BaseModel):
    """Progress graph node (spec section 4.4)."""

    id: str
    run_id: str
    kind: NodeKind
    title: str
    specification: str
    state: NodeState = NodeState.PENDING
    version: int = 1
    schedulable: bool = False
    priority: float = 0.0
    progress_weight: float = 1.0
    estimated_token_cost: int | None = None
    estimated_time_ms: int | None = None
    estimated_tool_calls: int | None = None
    actual_token_cost: int = 0
    actual_time_ms: int = 0
    actual_tool_calls: int = 0
    max_attempts: int = 3
    attempt_count: int = 0
    # Step 4: separate counters per failure type.
    verification_attempts: int = 0
    parse_attempts: int = 0
    tool_attempts: int = 0
    verification_spec: dict[str, Any] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)


class GraphEdge(BaseModel):
    """Progress graph edge; mirrors the ``edges`` table (spec section 19)."""

    id: str = Field(default_factory=lambda: uuid4().hex)
    run_id: str
    source_node_id: str
    target_node_id: str
    kind: EdgeKind
    active: bool = True
    version: int = 1
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)


class EvidenceRef(BaseModel):
    """Evidence reference (spec section 4.5)."""

    id: str = Field(default_factory=lambda: uuid4().hex)
    run_id: str
    evidence_type: str
    source_event_id: str
    uri: str | None = None
    content_hash: str | None = None
    summary: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_utcnow)


class ResourceClaim(BaseModel):
    """Declared read/write sets for future parallel execution (spec section 11.3)."""

    read_keys: list[str] = Field(default_factory=list)
    write_keys: list[str] = Field(default_factory=list)


class Run(BaseModel):
    """A single runtime run; mirrors the ``runs`` table."""

    id: str
    goal: str
    status: str = "pending"
    config: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)


class ExecutionRecord(BaseModel):
    """One node execution attempt; mirrors the ``executions`` table."""

    id: str = Field(default_factory=lambda: uuid4().hex)
    run_id: str
    node_id: str
    attempt_number: int
    context_hash: str
    model_name: str | None = None
    status: str = "running"
    input_tokens: int = 0
    output_tokens: int = 0
    tool_calls: int = 0
    cost_usd: float = 0.0
    started_at: datetime = Field(default_factory=_utcnow)
    finished_at: datetime | None = None
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    checkpoint_before: str | None = None
    checkpoint_after: str | None = None
