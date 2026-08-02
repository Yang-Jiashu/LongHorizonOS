"""Critical path analysis over the active DEPENDS_ON DAG (spec 11.2, Phase 6).

Works on the *remaining* subgraph (nodes not VERIFIED / ABORTED) in execution
order (dependency -> dependent). Deterministic: ties broken by node id.
"""

from __future__ import annotations

import networkx as nx

from lhos.graph.queries import ProgressGraph


def remaining_dag(graph: ProgressGraph) -> nx.DiGraph:
    return graph.depends_on_digraph(remaining_only=True)


def longest_path_from(dag: nx.DiGraph, node_id: str) -> int:
    """Longest chain (node count) starting at ``node_id``, inclusive."""
    best = 0
    for succ in sorted(dag.successors(node_id)):
        best = max(best, longest_path_from(dag, succ))
    return 1 + best


def longest_remaining_path_length(dag: nx.DiGraph) -> int:
    if dag.number_of_nodes() == 0:
        return 0
    if dag.number_of_nodes() == 1:
        return 1
    try:
        path = nx.dag_longest_path(dag)
        return len(path)
    except nx.NetworkXError:
        return 1


def criticality(graph: ProgressGraph, node_id: str) -> float:
    """longest remaining path through node / global longest remaining path."""
    dag = remaining_dag(graph)
    total = longest_remaining_path_length(dag)
    if total == 0 or node_id not in dag:
        return 0.0
    return longest_path_from(dag, node_id) / total


def unlock_score(graph: ProgressGraph, node_id: str) -> float:
    """blocked descendant count / total remaining nodes (spec 11.2)."""
    dag = remaining_dag(graph)
    total = dag.number_of_nodes()
    if total == 0 or node_id not in dag:
        return 0.0
    descendants = nx.descendants(dag, node_id)
    return len(descendants) / total
