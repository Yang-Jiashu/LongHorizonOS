"""Termination evaluation for the main loop (spec section 18)."""

from __future__ import annotations

from pydantic import BaseModel

from lhos.domain.enums import NodeKind, NodeState
from lhos.graph.queries import ProgressGraph


class TerminationDecision(BaseModel):
    should_stop: bool
    status: str = "running"  # completed | failed | paused | running
    reason: str = ""


class TerminationEvaluator:
    def evaluate(self, graph: ProgressGraph) -> TerminationDecision:
        schedulable = [
            n for n in graph.nodes.values() if n.kind == NodeKind.SUBTASK and n.schedulable
        ]
        if not schedulable:
            return TerminationDecision(
                should_stop=True, status="completed", reason="no schedulable nodes"
            )
        verified = [n for n in schedulable if n.state == NodeState.VERIFIED]
        if len(verified) == len(schedulable):
            return TerminationDecision(
                should_stop=True, status="completed", reason="all subtasks verified"
            )
        active = [
            n
            for n in schedulable
            if n.state in {NodeState.READY, NodeState.RUNNING, NodeState.WAITING}
        ]
        if active:
            return TerminationDecision(should_stop=False)
        # Nothing ready/running/waiting but work remains: can anything recover?
        recoverable = [
            n
            for n in schedulable
            if n.state in {NodeState.PENDING, NodeState.STALE, NodeState.INVALIDATED}
            or (n.state == NodeState.FAILED and n.attempt_count < n.max_attempts)
        ]
        if not recoverable:
            return TerminationDecision(
                should_stop=True,
                status="failed",
                reason="remaining nodes cannot make progress",
            )
        # Readiness already ran in the main loop; a recoverable node that is
        # still not READY means the run is stuck (e.g. dead dependency branch).
        return TerminationDecision(
            should_stop=True,
            status="failed",
            reason="no ready nodes and no waiting nodes: run is stuck",
        )
