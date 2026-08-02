"""Cost-aware scheduler: deterministic heuristic scoring (spec section 11.2).

No LLM is involved. All components are deterministic functions of the graph,
node metadata, and an injectable ``now`` (for age), so repeated selection over
the same state yields the same node.

score(node) =
    w_criticality * criticality(node)
  + w_unlock      * unlock_score(node)
  + w_progress    * expected_progress(node)
  + w_age         * starvation_age(node)
  - w_token       * normalized_token_cost(node)
  - w_time        * normalized_time_cost(node)
  - w_risk        * execution_risk(node)
  - w_switch      * context_switch_cost(node)
"""

from __future__ import annotations

from datetime import datetime

from lhos.domain.budgets import BudgetState
from lhos.domain.models import GraphNode
from lhos.graph import critical_path
from lhos.graph.queries import ProgressGraph
from lhos.runtime.scheduler import ResourceState

DEFAULT_WEIGHTS: dict[str, float] = {
    "criticality": 1.0,
    "unlock": 0.7,
    "progress": 1.0,
    "age": 0.1,
    "token_cost": 0.3,
    "time_cost": 0.3,
    "risk": 0.5,
    "context_switch": 0.1,
}

_DEFAULT_TOKEN_COST = 1000
_DEFAULT_TIME_MS = 1000

_SIDE_EFFECT_RISK = {
    "read_only": 0.0,
    "local_write": 0.3,
    "external_write": 0.7,
    "destructive": 1.0,
}

# Success-probability heuristic (spec 11.2): first run 1.0, one failure 0.7,
# two failures 0.4; high side-effect nodes get an extra 0.8 multiplier.
_SUCCESS_PROB = {0: 1.0, 1: 0.7, 2: 0.4}


class CostAwareScheduler:
    def __init__(self, weights: dict[str, float] | None = None):
        self._w = dict(DEFAULT_WEIGHTS)
        if weights:
            self._w.update(weights)

    def expected_progress(self, node: GraphNode) -> float:
        prob = _SUCCESS_PROB.get(node.attempt_count, 0.4)
        side_effect = node.metadata.get("side_effect_level", "read_only")
        if side_effect in {"external_write", "destructive"}:
            prob *= 0.8
        return node.progress_weight * prob

    def execution_risk(self, node: GraphNode) -> float:
        attempt_risk = node.attempt_count / max(node.max_attempts, 1)
        side_effect = node.metadata.get("side_effect_level", "read_only")
        side_risk = _SIDE_EFFECT_RISK.get(side_effect, 0.5)
        stale_risk = 0.2 if node.metadata.get("depends_on_stale") else 0.0
        return min(1.0, 0.5 * attempt_risk + 0.3 * side_risk + stale_risk)

    def context_switch_cost(self, node: GraphNode, resources: ResourceState) -> float:
        tool_type = node.metadata.get("tool_type")
        if resources.last_tool_type is None or tool_type is None:
            return 0.0
        return 0.0 if tool_type == resources.last_tool_type else 1.0

    def score(
        self,
        node: GraphNode,
        graph: ProgressGraph,
        resources: ResourceState,
        max_token_cost: float,
        max_time_ms: float,
        max_age_seconds: float,
        now: datetime,
    ) -> float:
        w = self._w
        token_cost = node.estimated_token_cost or _DEFAULT_TOKEN_COST
        time_ms = node.estimated_time_ms or _DEFAULT_TIME_MS
        ready_at_raw = node.metadata.get("ready_at") or node.created_at.isoformat()
        ready_at = datetime.fromisoformat(ready_at_raw)
        age_seconds = max(0.0, (now - ready_at).total_seconds())
        return (
            w["criticality"] * critical_path.criticality(graph, node.id)
            + w["unlock"] * critical_path.unlock_score(graph, node.id)
            + w["progress"] * self.expected_progress(node)
            + w["age"] * (age_seconds / max_age_seconds if max_age_seconds else 0.0)
            - w["token_cost"] * (token_cost / max_token_cost if max_token_cost else 0.0)
            - w["time_cost"] * (time_ms / max_time_ms if max_time_ms else 0.0)
            - w["risk"] * self.execution_risk(node)
            - w["context_switch"] * self.context_switch_cost(node, resources)
        )

    def select(
        self,
        ready_nodes: list[GraphNode],
        graph: ProgressGraph,
        budget: BudgetState,
        resources: ResourceState,
        now: datetime | None = None,
    ) -> GraphNode | None:
        if not ready_nodes:
            return None
        now = now or datetime.now().astimezone()
        max_token = max((n.estimated_token_cost or _DEFAULT_TOKEN_COST) for n in ready_nodes)
        max_time = max((n.estimated_time_ms or _DEFAULT_TIME_MS) for n in ready_nodes)
        ages = []
        for n in ready_nodes:
            raw = n.metadata.get("ready_at") or n.created_at.isoformat()
            ages.append(max(0.0, (now - datetime.fromisoformat(raw)).total_seconds()))
        max_age = max(ages) if ages else 0.0
        scored = [
            (
                self.score(n, graph, resources, max_token, max_time, max_age, now),
                n,
            )
            for n in ready_nodes
        ]
        # Deterministic tie-break: ready_at, then node id.
        scored.sort(
            key=lambda t: (
                -t[0],
                t[1].metadata.get("ready_at") or t[1].created_at.isoformat(),
                t[1].id,
            )
        )
        return scored[0][1]
