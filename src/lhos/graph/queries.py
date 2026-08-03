"""In-memory progress graph and traversal helpers (spec sections 4, 7, 9, 15).

Edge direction convention (spec 8.1): ``source DEPENDS_ON target`` means
"source depends on target", so execution flows target -> source.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import networkx as nx

from lhos.domain.enums import NON_REMAINING_STATES, EdgeKind
from lhos.domain.errors import NodeNotFoundError
from lhos.domain.models import GraphEdge, GraphNode


@dataclass
class ProgressGraph:
    run_id: str
    nodes: dict[str, GraphNode] = field(default_factory=dict)
    edges: list[GraphEdge] = field(default_factory=list)

    def get_node(self, node_id: str) -> GraphNode:
        try:
            return self.nodes[node_id]
        except KeyError as err:
            raise NodeNotFoundError(f"node {node_id} not found in run {self.run_id}") from err

    def active_edges(self, kind: EdgeKind | None = None) -> list[GraphEdge]:
        return [e for e in self.edges if e.active and (kind is None or e.kind == kind)]

    def out_edges(
        self, node_id: str, kind: EdgeKind | None = None, active_only: bool = True
    ) -> list[GraphEdge]:
        return [
            e
            for e in self.edges
            if e.source_node_id == node_id
            and (not active_only or e.active)
            and (kind is None or e.kind == kind)
        ]

    def in_edges(
        self, node_id: str, kind: EdgeKind | None = None, active_only: bool = True
    ) -> list[GraphEdge]:
        return [
            e
            for e in self.edges
            if e.target_node_id == node_id
            and (not active_only or e.active)
            and (kind is None or e.kind == kind)
        ]

    def dependencies(self, node_id: str) -> list[GraphNode]:
        """Active DEPENDS_ON targets: nodes that ``node_id`` depends on."""
        return [
            self.nodes[e.target_node_id]
            for e in self.out_edges(node_id, EdgeKind.DEPENDS_ON)
            if e.target_node_id in self.nodes
        ]

    def dependents(self, node_id: str) -> list[GraphNode]:
        """Active DEPENDS_ON sources: nodes that depend on ``node_id``."""
        return [
            self.nodes[e.source_node_id]
            for e in self.in_edges(node_id, EdgeKind.DEPENDS_ON)
            if e.source_node_id in self.nodes
        ]

    def direct_consumers(self, node_id: str) -> list[GraphNode]:
        """Nodes with an active ``X CONSUMES node_id`` edge (spec section 15)."""
        return [
            self.nodes[e.source_node_id]
            for e in self.in_edges(node_id, EdgeKind.CONSUMES)
            if e.source_node_id in self.nodes
        ]

    def produced_artifacts(self, node_id: str) -> list[GraphNode]:
        """Artifact nodes that ``node_id`` produces."""
        return [
            self.nodes[e.target_node_id]
            for e in self.out_edges(node_id, EdgeKind.PRODUCES)
            if e.target_node_id in self.nodes
        ]

    def producers_of(self, artifact_node_id: str) -> list[GraphNode]:
        return [
            self.nodes[e.source_node_id]
            for e in self.in_edges(artifact_node_id, EdgeKind.PRODUCES)
            if e.source_node_id in self.nodes
        ]

    def consumed_artifacts(self, node_id: str) -> list[GraphNode]:
        return [
            self.nodes[e.target_node_id]
            for e in self.out_edges(node_id, EdgeKind.CONSUMES)
            if e.target_node_id in self.nodes
        ]

    def remaining_nodes(self) -> list[GraphNode]:
        """Schedulable subtasks that are not yet VERIFIED or ABORTED."""
        return [
            n for n in self.nodes.values() if n.schedulable and n.state not in NON_REMAINING_STATES
        ]

    def depends_on_digraph(self, remaining_only: bool = False) -> nx.DiGraph:
        """Active DEPENDS_ON subgraph in EXECUTION order (dependency -> dependent)."""
        g = nx.DiGraph()
        if remaining_only:
            remaining = {n.id for n in self.remaining_nodes()}
            g.add_nodes_from(remaining)
        else:
            g.add_nodes_from(self.nodes.keys())
        for e in self.active_edges(EdgeKind.DEPENDS_ON):
            if e.source_node_id in g and e.target_node_id in g:
                # target must execute before source.
                g.add_edge(e.target_node_id, e.source_node_id)
        return g

    def would_create_cycle(self, source_node_id: str, target_node_id: str) -> bool:
        """True if adding ``source DEPENDS_ON target`` closes a cycle."""
        g = self.depends_on_digraph()
        g.add_edge(target_node_id, source_node_id)  # execution order
        return not nx.is_directed_acyclic_graph(g)
