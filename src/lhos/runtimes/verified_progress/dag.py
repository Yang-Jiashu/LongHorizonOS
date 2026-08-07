"""Deterministic cycle detection for the depends_on edge subgraph.

DAG validation must consider BOTH the existing projection edges AND
all new depends_on edges in the current patch — otherwise a patch that
adds two mutually-dependent edges would slip through per-edge checks.

Uses an iterative DFS (no recursion depth issues for large graphs).
Returns the concrete cycle path when found so callers can produce useful
error messages.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable

from .models import EdgeType, VPGEdge


def _depends_on_adj(
    existing: Iterable[VPGEdge],
    new_edges: Iterable[VPGEdge],
) -> dict[str, list[str]]:
    """Build adjacency: source -> [target] for depends_on edges only.

    ``X depends_on Y`` is stored as an edge ``X -[depends_on]-> Y``.
    So Y must become DONE/VERIFIED before X can READY.
    """

    adj: dict[str, list[str]] = defaultdict(list)
    for e in existing:
        if e.edge_type == EdgeType.DEPENDS_ON:
            adj[e.source_node_id].append(e.target_node_id)
    for e in new_edges:
        if e.edge_type == EdgeType.DEPENDS_ON:
            adj[e.source_node_id].append(e.target_node_id)
    return adj


def detect_cycle(
    existing_edges: Iterable[VPGEdge],
    new_edges: Iterable[VPGEdge],
) -> list[str] | None:
    """Return a cycle path (list of node-ids) or None.

    Iterative DFS over every node that appears in the unified depends_on
    graph (existing + new edges for this patch).
    """

    adj = _depends_on_adj(existing_edges, new_edges)

    nodes: set[str] = set()
    for src, tgts in adj.items():
        nodes.add(src)
        for t in tgts:
            nodes.add(t)

    if not nodes:
        return None

    UNVISITED, IN_STACK, DONE = 0, 1, 2
    state: dict[str, int] = {n: UNVISITED for n in nodes}

    for start in nodes:
        if state[start] != UNVISITED:
            continue
        # stack of (node, next_child_index)
        stack: list[tuple[str, int]] = [(start, 0)]
        path: list[str] = []

        while stack:
            node, idx = stack[-1]
            if state[node] == UNVISITED:
                state[node] = IN_STACK
                path.append(node)

            children = adj.get(node, [])
            if idx < len(children):
                stack[-1] = (node, idx + 1)
                child = children[idx]
                if state[child] == IN_STACK:
                    cycle_start = path.index(child)
                    return path[cycle_start:] + [child]
                if state[child] == UNVISITED:
                    stack.append((child, 0))
            else:
                stack.pop()
                state[node] = DONE
                if path and path[-1] == node:
                    path.pop()

    return None


def is_self_loop(edge: VPGEdge) -> bool:
    """True iff the edge is a depends_on self-loop."""
    return edge.edge_type == EdgeType.DEPENDS_ON and edge.source_node_id == edge.target_node_id
