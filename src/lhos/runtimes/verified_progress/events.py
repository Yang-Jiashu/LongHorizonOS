"""Graph Event types + immutable GraphEvent model.

D1 freezes the complete event vocabulary.  GraphEvents never store large
payload contents — only IDs, hashes, versions and state transitions.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


def _uuid() -> str:
    from uuid import uuid4

    return uuid4().hex


def _utcnow() -> datetime:
    return datetime.now(UTC)


class GraphEventType(StrEnum):
    # graph lifecycle
    GRAPH_CREATED = "graph.created"
    GRAPH_PATCH_ACCEPTED = "graph.patch.accepted"
    GRAPH_PATCH_REJECTED = "graph.patch.rejected"
    GRAPH_VERSION_COMMITTED = "graph.version.committed"
    GRAPH_CLOSED = "graph.closed"

    # nodes
    NODE_ADDED = "node.added"
    NODE_ADMITTED = "node.admitted"
    NODE_INVALID = "node.invalid"

    # edges
    EDGE_ADDED = "edge.added"

    # attachments
    ARTIFACT_ATTACHED = "artifact.attached"
    EVIDENCE_ATTACHED = "evidence.attached"

    # execution observation
    EXECUTION_ATTEMPT_OBSERVED = "execution.attempt_observed"

    # derived state
    TASK_VERIFIED_DERIVED = "task.verified.derived"
    TASK_STALE_DERIVED = "task.stale.derived"
    TASK_CLOSED_DERIVED = "task.closed.derived"
    TASK_REOPENED_DERIVED = "task.reopened.derived"

    GOAL_CLOSED_DERIVED = "goal.closed.derived"
    GOAL_REOPENED_DERIVED = "goal.reopened.derived"

    READY_FRONTIER_UPDATED = "ready.frontier.updated"

    # recovery
    GRAPH_RECOVERY_STARTED = "graph.recovery.started"
    GRAPH_RECOVERY_COMPLETED = "graph.recovery.completed"
    GRAPH_RECOVERY_FAILED = "graph.recovery.failed"


class GraphEvent(BaseModel):
    """Append-only, immutable event record.  Only IDs/hashes/versions stored."""

    event_id: str = Field(default_factory=_uuid)
    graph_id: str

    event_type: GraphEventType
    causation_patch_id: str | None = None

    # node / edge / artifact / action / evidence IDs referenced
    subject_id: str | None = None
    subject_kind: str | None = None

    # new state snapshot fields
    node_id: str | None = None
    to_lifecycle: str | None = None
    to_validity: str | None = None

    # task derivation context
    verification_ids: tuple[str, ...] = ()
    evidence_ids: tuple[str, ...] = ()
    artifact_bindings: tuple[dict[str, Any], ...] = ()
    dependency_task_ids: tuple[str, ...] = ()
    ready_frontier: tuple[str, ...] = ()
    # Newer durable event rows store a constant-size summary of the READY
    # frontier instead of repeating every task id in every version.  The
    # in-memory event still carries the full tuple while it is being assembled
    # (which keeps the derivation API backwards compatible); GraphStore applies
    # the compact representation at the persistence boundary.  Legacy rows
    # that contain a full JSON list continue to populate ``ready_frontier``.
    ready_frontier_count: int | None = None
    ready_frontier_hash: str | None = None
    graph_version: int | None = None

    payload: dict[str, Any] = Field(default_factory=dict)

    recorded_at: datetime = Field(default_factory=_utcnow)


def ready_frontier_hash(frontier: tuple[str, ...] | list[str]) -> str:
    """Return the canonical digest used by compact frontier event rows.

    Ordering is significant: ``compute_ready_frontier`` has a deterministic
    order and the digest therefore detects both membership and ordering
    changes.  Separators avoid insignificant JSON whitespace differences.
    """

    payload = json.dumps(
        list(frontier),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
