"""FIFO scheduler: the baseline (spec section 11.2).

Selects the ready node with the smallest ``metadata["ready_at"]``; falls back
to ``created_at`` when readiness bookkeeping is absent. Ties break by node id,
so selection is deterministic.
"""

from lhos.domain.budgets import BudgetState
from lhos.domain.models import GraphNode
from lhos.graph.queries import ProgressGraph
from lhos.runtime.scheduler import ResourceState


class FifoScheduler:
    def select(
        self,
        ready_nodes: list[GraphNode],
        graph: ProgressGraph,
        budget: BudgetState,
        resources: ResourceState,
    ) -> GraphNode | None:
        if not ready_nodes:
            return None

        def key(node: GraphNode) -> tuple[str, str]:
            return (
                node.metadata.get("ready_at") or node.created_at.isoformat(),
                node.id,
            )

        return min(ready_nodes, key=key)
