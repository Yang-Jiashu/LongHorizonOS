"""Termination evaluation for the main loop (spec section 18).

Step 8: each ``TerminationDecision`` now includes a ``primary_failure_code``
that categorizes the failure for diagnostic reporting. The controller uses
this code in the ``RUN_FAILED`` event payload.

Step 5 (Milestone 2.2): Added specific failure codes for node attempts
exhausted, verification spec issues, and local budget exhaustion.
``run_stuck`` is now only used as a last resort when no more specific
reason can be found.
"""

from __future__ import annotations

from pydantic import BaseModel

from lhos.domain.enums import NodeKind, NodeState
from lhos.graph.queries import ProgressGraph

# Failure codes (Step 8): structured reasons for run termination.
FAILURE_NO_READY_NODES = "no_ready_nodes"
FAILURE_ALL_NODES_EXHAUSTED = "all_nodes_exhausted"
FAILURE_RUN_STUCK = "run_stuck"
FAILURE_BUDGET_EXHAUSTED = "budget_exhausted"
FAILURE_CONTROLLER_MAX_ITERATIONS = "controller_max_iterations"
FAILURE_VERIFICATION_REPEATEDLY_FAILED = "verification_repeatedly_failed"
FAILURE_TOOL_EXECUTION = "tool_execution_failure"
FAILURE_NODE_ATTEMPTS_EXHAUSTED = "node_attempts_exhausted"
FAILURE_NODE_LOCAL_BUDGET_EXHAUSTED = "node_local_budget_exhausted"


class TerminationDecision(BaseModel):
    should_stop: bool
    status: str = "running"  # completed | failed | paused | running
    reason: str = ""
    primary_failure_code: str | None = None


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
            # No recoverable nodes — all remaining are permanently failed.
            failed_nodes = [n for n in schedulable if n.state == NodeState.FAILED]
            if failed_nodes:
                # Check if any node has repeated verification failures.
                has_repeated = any(n.metadata.get("repeated_failure") is True for n in failed_nodes)
                if has_repeated:
                    return TerminationDecision(
                        should_stop=True,
                        status="failed",
                        reason="nodes failed verification repeatedly with same failure signature",
                        primary_failure_code=FAILURE_VERIFICATION_REPEATEDLY_FAILED,
                    )
                return TerminationDecision(
                    should_stop=True,
                    status="failed",
                    reason="all remaining nodes have exhausted their attempts",
                    primary_failure_code=FAILURE_ALL_NODES_EXHAUSTED,
                )
            return TerminationDecision(
                should_stop=True,
                status="failed",
                reason="remaining nodes cannot make progress",
                primary_failure_code=FAILURE_ALL_NODES_EXHAUSTED,
            )

        # There are recoverable nodes but none became READY after the readiness
        # refresher ran. This means the run is genuinely stuck — dependencies
        # are not satisfiable or the graph has a dead branch.
        return TerminationDecision(
            should_stop=True,
            status="failed",
            reason="no ready nodes and no waiting nodes: run is stuck",
            primary_failure_code=FAILURE_RUN_STUCK,
        )
