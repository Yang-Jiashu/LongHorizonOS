"""Deterministic single-process coordinator.

The coordinator:
  - queries the READY front così der,
  - picks the first deterministic candidate,
  - returns a TaskDispatchCandidate to the caller (Kernel / Harness),
  - records an execution-attempt observation,
  - observes subsequent committed Actions,
  - accepts ArtifactRef + Evidence patches after execution.

D1 does NOT implement multi-agent claims, leases, or prompt/template logic.
"""

from __future__ import annotations

from datetime import UTC, datetime

from .events import GraphEvent, GraphEventType
from .models import TaskDispatchCandidate
from .readiness import compute_ready_frontier


def _utcnow() -> datetime:
    return datetime.now(UTC)


class ExecutionAttemptRef:
    """Reference to an observed Kernel Action attempt for a Task."""

    def __init__(
        self,
        attempt_id: str,
        task_id: str,
        process_id: str,
        action_id: str,
        observed_state: str,
    ) -> None:
        self.attempt_id = attempt_id
        self.task_id = task_id
        self.process_id = process_id
        self.action_id = action_id
        self.observed_state = observed_state


class DeterministicSingleProcessCoordinator:
    """Single-process, deterministic coordinator for D1.

    This is a thin logic-layer wrapper; all persistence happens via the
    VerifiedProgressRuntime.commit_patch / record_observation / query APIs.
    """

    def __init__(self, owner_pid: str) -> None:
        self.owner_pid = owner_pid

    def select_next_candidate(
        self,
        *,
        graph_id: str,
        graph_version: int,
        nodes: dict,
        edges: list,
    ) -> TaskDispatchCandidate | None:
        """Return the FIRST deterministic candidate, or None if frontier empty."""
        frontier = compute_ready_frontier(graph_id, graph_version, nodes, edges)
        return frontier[0] if frontier else None

    def observe_attempt(
        self,
        *,
        graph_id: str,
        graph_version: int,
        attempt: ExecutionAttemptRef,
    ) -> GraphEvent:
        """Produce an EXECUTION_ATTEMPT_OBSERVED event."""
        return GraphEvent(
            graph_id=graph_id,
            event_type=GraphEventType.EXECUTION_ATTEMPT_OBSERVED,
            subject_id=attempt.attempt_id,
            subject_kind="execution_attempt",
            node_id=attempt.task_id,
            payload={
                "process_id": attempt.process_id,
                "action_id": attempt.action_id,
                "observed_state": attempt.observed_state,
            },
            graph_version=graph_version,
        )
