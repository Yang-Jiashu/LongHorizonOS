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
    # Lazily built adjacency indexes (rebuilt on any edges-length change).
    _index_edge_count: int = field(default=-1, repr=False, compare=False)
    _out_index: dict[str, list[GraphEdge]] = field(default_factory=dict, repr=False, compare=False)
    _in_index: dict[str, list[GraphEdge]] = field(default_factory=dict, repr=False, compare=False)
    _active_edges: list[GraphEdge] = field(default_factory=list, repr=False, compare=False)
    # True iff the active DEPENDS_ON subgraph contains a cycle. Computed once
    # per index rebuild (Kahn) so ``would_create_cycle`` stays O(reachable)
    # on the hot path while remaining semantically identical to a full
    # is_directed_acyclic_graph check even on a corrupted (non-DAG) graph.
    _index_has_depends_cycle: bool = field(default=False, repr=False, compare=False)

    def _ensure_index(self) -> None:
        if self._index_edge_count == len(self.edges):
            return
        out: dict[str, list[GraphEdge]] = {}
        inn: dict[str, list[GraphEdge]] = {}
        active: list[GraphEdge] = []
        for edge in self.edges:
            out.setdefault(edge.source_node_id, []).append(edge)
            inn.setdefault(edge.target_node_id, []).append(edge)
            if edge.active:
                active.append(edge)
        self._out_index = out
        self._in_index = inn
        self._active_edges = active
        self._index_edge_count = len(self.edges)
        self._index_has_depends_cycle = self._compute_depends_cycle()

    def _compute_depends_cycle(self) -> bool:
        """Kahn topology sort over the active DEPENDS_ON subgraph."""
        indeg: dict[str, int] = {}
        for edge in self._active_edges:
            if edge.kind != EdgeKind.DEPENDS_ON:
                continue
            indeg[edge.source_node_id] = indeg.get(edge.source_node_id, 0) + 1
            indeg.setdefault(edge.target_node_id, 0)
        stack = [nid for nid, d in indeg.items() if d == 0]
        seen = 0
        while stack:
            nid = stack.pop()
            seen += 1
            # execution-order successors of ``nid`` are the nodes that depend
            # on it: DEPENDS_ON edges whose target == nid.
            for edge in self._in_index.get(nid, ()):
                if edge.kind != EdgeKind.DEPENDS_ON or not edge.active:
                    continue
                nxt = edge.source_node_id
                indeg[nxt] -= 1
                if indeg[nxt] == 0:
                    stack.append(nxt)
        return seen != len(indeg)

    def get_node(self, node_id: str) -> GraphNode:
        try:
            return self.nodes[node_id]
        except KeyError as err:
            raise NodeNotFoundError(f"node {node_id} not found in run {self.run_id}") from err

    def active_edges(self, kind: EdgeKind | None = None) -> list[GraphEdge]:
        self._ensure_index()
        if kind is None:
            return self._active_edges
        return [e for e in self._active_edges if e.kind == kind]

    def out_edges(
        self, node_id: str, kind: EdgeKind | None = None, active_only: bool = True
    ) -> list[GraphEdge]:
        self._ensure_index()
        return [
            e
            for e in self._out_index.get(node_id, ())
            if (not active_only or e.active)
            and (kind is None or e.kind == kind)
        ]

    def in_edges(
        self, node_id: str, kind: EdgeKind | None = None, active_only: bool = True
    ) -> list[GraphEdge]:
        self._ensure_index()
        return [
            e
            for e in self._in_index.get(node_id, ())
            if (not active_only or e.active)
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
        g: nx.DiGraph = nx.DiGraph()
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
        """True if adding ``source DEPENDS_ON target`` closes a cycle.

        In the execution-order DAG (dependency -> dependent) adding the edge
        ``target -> source`` closes a cycle iff ``source`` already reaches
        ``target``; a self-loop also closes a cycle. When the active
        DEPENDS_ON subgraph already contains a cycle (a corrupted non-DAG
        state) we conservatively reject, identical to the historical
        ``is_directed_acyclic_graph`` semantics. Hot path is O(reachable set)
        instead of a full-graph DAG check.
        """
        self._ensure_index()
        if source_node_id == target_node_id:
            return True
        if self._index_has_depends_cycle:
            return True
        # BFS from source over dependents (execution-order successors):
        # active DEPENDS_ON edges whose target == current node.
        stack = [source_node_id]
        seen = {source_node_id}
        while stack:
            nid = stack.pop()
            for edge in self._in_index.get(nid, ()):
                if edge.kind != EdgeKind.DEPENDS_ON or not edge.active:
                    continue
                nxt = edge.source_node_id
                if nxt == target_node_id:
                    return True
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        return False
