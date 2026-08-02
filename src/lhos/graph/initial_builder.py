"""Deterministic initial graph builder from a JSON spec (spec section 8.1).

The LLM planner arrives in a later phase; this builder consumes the exact
JSON shape of section 8.1 and is fully deterministic (stable node ids derived
from run_id + temp_id), which keeps event replay reproducible.
"""

from __future__ import annotations

from typing import Any

from lhos.domain.enums import EdgeKind
from lhos.domain.errors import CycleError, PatchValidationError
from lhos.domain.events import ActorType
from lhos.domain.models import GraphEdge, GraphNode
from lhos.domain.verification import VerificationSpec


class InitialGraphBuilder:
    def __init__(self, graph_store):  # noqa: ANN001 - SqliteGraphStore
        self._store = graph_store

    @staticmethod
    def node_id_for(run_id: str, temp_id: str) -> str:
        return f"{run_id}:{temp_id}"

    def build(self, run_id: str, spec: dict[str, Any]) -> dict[str, str]:
        """Build nodes + edges from a section 8.1 JSON spec.

        Returns the temp_id -> node_id mapping. Raises before writing anything
        if the DEPENDS_ON subgraph would contain a cycle.
        """
        raw_nodes = spec.get("nodes", [])
        raw_edges = spec.get("edges", [])
        if not raw_nodes:
            raise PatchValidationError("initial spec must contain at least one node")

        id_map: dict[str, str] = {}
        nodes: list[GraphNode] = []
        for raw in raw_nodes:
            temp_id = raw.get("temp_id") or raw.get("id")
            if not temp_id:
                raise PatchValidationError("every initial node needs temp_id")
            if temp_id in id_map:
                raise PatchValidationError(f"duplicate temp_id {temp_id!r}")
            node_id = self.node_id_for(run_id, temp_id)
            id_map[temp_id] = node_id
            verification_spec = raw.get("verification_spec")
            if verification_spec is not None:
                # Normalize the compact planner shape into the 14.3 shape.
                verification_spec = VerificationSpec.from_raw(
                    verification_spec
                ).model_dump()
            nodes.append(
                GraphNode(
                    id=node_id,
                    run_id=run_id,
                    kind=raw.get("kind", "subtask"),
                    title=raw.get("title", ""),
                    specification=raw.get("specification", ""),
                    schedulable=raw.get("schedulable", False),
                    priority=raw.get("priority", 0.0),
                    progress_weight=raw.get("progress_weight", 1.0),
                    estimated_token_cost=raw.get("estimated_token_cost"),
                    estimated_time_ms=raw.get("estimated_time_ms"),
                    estimated_tool_calls=raw.get("estimated_tool_calls"),
                    max_attempts=raw.get("max_attempts", 3),
                    verification_spec=verification_spec,
                    metadata=raw.get("metadata", {}),
                )
            )

        edges: list[GraphEdge] = []
        for raw in raw_edges:
            source = id_map.get(raw["source"])
            target = id_map.get(raw["target"])
            if source is None or target is None:
                raise PatchValidationError(
                    f"edge references unknown temp_id: {raw!r}"
                )
            edges.append(
                GraphEdge(
                    run_id=run_id,
                    source_node_id=source,
                    target_node_id=target,
                    kind=EdgeKind(raw.get("kind", "depends_on")),
                    metadata=raw.get("metadata", {}),
                )
            )

        # Cycle check on the full edge set before writing anything.
        import networkx as nx

        from lhos.graph.queries import ProgressGraph

        graph = ProgressGraph(
            run_id=run_id, nodes={n.id: n for n in nodes}, edges=list(edges)
        )
        if not nx.is_directed_acyclic_graph(graph.depends_on_digraph()):
            raise CycleError("initial DEPENDS_ON subgraph contains a cycle")

        with self._store._db.transaction():
            for node in nodes:
                self._store.add_node(node, actor=ActorType.PLANNER)
            for edge in edges:
                self._store.add_edge(edge, actor=ActorType.PLANNER)
        return id_map
