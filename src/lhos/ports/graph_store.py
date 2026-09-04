"""Graph store port: materialized control state (spec sections 4, 7, 18)."""

from typing import Any, Protocol

from lhos.domain.enums import NodeState
from lhos.domain.models import EvidenceRef, ExecutionRecord, GraphEdge, GraphNode, Run


class GraphStore(Protocol):
    # Runs
    def create_run(self, run_id: str, goal: str, config: dict[str, Any]) -> Run: ...

    def get_run(self, run_id: str) -> Run: ...

    def set_run_status(self, run_id: str, status: str) -> Run: ...

    # Nodes
    def add_node(self, node: GraphNode, actor: str = "system") -> GraphNode: ...

    def get_node(self, node_id: str) -> GraphNode: ...

    def set_state(
        self,
        node_id: str,
        target: NodeState,
        actor: str,
        evidence_ids: list[str] | None = None,
        event_type: str = "NODE_STATE_CHANGED",
        payload_extra: dict[str, Any] | None = None,
    ) -> GraphNode: ...

    def update_node(
        self,
        node: GraphNode,
        actor: str,
        expected_version: int | None = None,
        event_type: str = "NODE_UPDATED",
        payload_extra: dict[str, Any] | None = None,
    ) -> GraphNode: ...

    # Edges
    def add_edge(self, edge: GraphEdge, actor: str = "system") -> GraphEdge: ...

    def remove_edge(self, edge_id: str, actor: str = "system") -> GraphEdge: ...

    # Evidence
    def add_evidence(
        self, evidence: EvidenceRef, actor: str = "system", event_type: str = "ARTIFACT_CREATED"
    ) -> EvidenceRef: ...

    def evidence_exists(self, evidence_id: str) -> bool: ...

    def list_evidence(self, run_id: str) -> list[EvidenceRef]: ...

    # Leases
    def acquire_lease(self, node_id: str, worker_id: str, lease_seconds: int) -> bool: ...

    def release_lease(self, node_id: str, actor: str = "system") -> None: ...

    # Queries
    def list_ready_nodes(self, run_id: str) -> list[GraphNode]: ...

    def has_waiting_nodes(self, run_id: str) -> bool: ...

    def list_nodes(self, run_id: str, state: NodeState | None = None) -> list[GraphNode]: ...

    def load_graph(self, run_id: str) -> "ProgressGraph": ...

    # Executions
    def insert_execution(self, record: ExecutionRecord) -> ExecutionRecord: ...

    def finish_execution(
        self,
        execution_id: str,
        status: str,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        checkpoint_after: str | None = None,
    ) -> None: ...

    def list_executions(self, run_id: str, node_id: str | None = None) -> list[ExecutionRecord]: ...


# Imported for the Protocol signature; concrete class lives in graph/queries.py.
from lhos.graph.queries import ProgressGraph  # noqa: E402
