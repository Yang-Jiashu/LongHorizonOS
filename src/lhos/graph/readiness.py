"""Readiness computation (spec section 9).

A subtask becomes READY iff:
- state is PENDING / STALE / FAILED, AND
- all active depends_on targets are VERIFIED, AND
- no active blocks edge from a non-verified node, AND
- preconditions hold, AND
- attempt_count < max_attempts, AND
- budget allows, AND
- required resources are available.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from lhos.domain.budgets import BudgetLimits, BudgetState, budget_exhausted
from lhos.domain.enums import EdgeKind, NodeKind, NodeState
from lhos.domain.events import ActorType
from lhos.domain.models import GraphNode
from lhos.graph.queries import ProgressGraph


class EnvironmentSnapshot(BaseModel):
    facts: dict[str, Any] = Field(default_factory=dict)
    available_resources: list[str] = Field(default_factory=list)


class ReadinessEvaluator:
    """Spec section 9 interface.

    The spec's ``evaluate`` signature carries ``budget: BudgetState``; limits
    are injected at construction so the signature can stay as specified.
    """

    def __init__(self, limits: BudgetLimits | None = None):
        self._limits = limits or BudgetLimits()

    def evaluate(
        self,
        node: GraphNode,
        graph: ProgressGraph,
        environment: EnvironmentSnapshot,
        budget: BudgetState,
    ) -> bool:
        if node.kind != NodeKind.SUBTASK or not node.schedulable:
            return False
        if node.state not in {NodeState.PENDING, NodeState.STALE, NodeState.FAILED}:
            return False
        for dep in graph.dependencies(node.id):
            if dep.state != NodeState.VERIFIED:
                return False
        for edge in graph.in_edges(node.id, EdgeKind.BLOCKS):
            blocker = graph.nodes.get(edge.source_node_id)
            if blocker is not None and blocker.state != NodeState.VERIFIED:
                return False
        if not node.metadata.get("precondition_met", True):
            return False
        if node.attempt_count >= node.max_attempts:
            return False
        if budget_exhausted(self._limits, budget):
            return False
        required = node.metadata.get("required_resources", [])
        if any(r not in environment.available_resources for r in required):
            return False
        return True


class ReadinessRefresher:
    """Applies the evaluator to a stored run and persists READY transitions."""

    def __init__(self, graph_store, evaluator: ReadinessEvaluator):  # noqa: ANN001
        self._store = graph_store
        self._evaluator = evaluator

    def refresh(self, run_id: str, budget: BudgetState | None = None) -> list[GraphNode]:
        graph = self._store.load_graph(run_id)
        budget = budget or BudgetState()
        environment = EnvironmentSnapshot()
        now = datetime.now().astimezone().isoformat()
        newly_ready: list[GraphNode] = []
        for node in sorted(graph.nodes.values(), key=lambda n: n.id):
            if node.state not in {NodeState.PENDING, NodeState.STALE, NodeState.FAILED}:
                continue
            if self._evaluator.evaluate(node, graph, environment, budget):
                from_state = node.state
                updated = self._store.set_state(
                    node.id,
                    NodeState.READY,
                    actor=ActorType.SYSTEM,
                )
                changed = False
                if "ready_at" not in updated.metadata:
                    updated.metadata["ready_at"] = now
                    changed = True
                # Track what state the node came from (stale/failed/pending)
                # so EXECUTION_STARTED can record a retry_reason — input for
                # the re-executed-work metrics (spec 15, 24.3).
                if updated.metadata.get("ready_from_state") != str(from_state):
                    updated.metadata["ready_from_state"] = str(from_state)
                    changed = True
                if changed:
                    # metadata rides along in the projection without a version
                    # bump (bookkeeping, not semantics).
                    self._store.update_node(
                        updated,
                        actor=ActorType.SYSTEM,
                        bump_version=False,
                    )
                newly_ready.append(updated)
        return newly_ready
