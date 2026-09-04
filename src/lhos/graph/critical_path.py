"""Critical path analysis over the active DEPENDS_ON DAG (spec 11.2, Phase 6).

Works on the *remaining* subgraph (nodes not VERIFIED / ABORTED) in execution
order (dependency -> dependent). Deterministic: ties broken by node id.

Complexity note: the per-node scorers (``criticality`` / ``unlock_score``)
are computed via single topological-pass dynamic programming.  The batch
helpers (``criticality_profile`` / ``unlock_profile``) compute every node's
score in one DP so a scheduler can build them once per round and query each
ready node in O(1) instead of re-walking the whole DAG per candidate.
"""

from __future__ import annotations

import networkx as nx

from lhos.graph.queries import ProgressGraph


def remaining_dag(graph: ProgressGraph) -> nx.DiGraph:
    return graph.depends_on_digraph(remaining_only=True)


def _topological_order(dag: nx.DiGraph) -> list[str] | None:
    """Topological order of ``dag``, or ``None`` when it is not a DAG."""
    try:
        return list(nx.topological_sort(dag))
    except nx.NetworkXError:
        return None


def longest_path_from(dag: nx.DiGraph, node_id: str) -> int:
    """Longest chain (node count) starting at ``node_id``, inclusive.

    Memoized DFS: each node's reachable subtree is computed at most once, so a
    single call is O(V+E) instead of the naive O(V*(V+E)) recursion.
    """
    memo: dict[str, int] = {}

    def _lp(node: str) -> int:
        cached = memo.get(node)
        if cached is not None:
            return cached
        best = 0
        for succ in sorted(dag.successors(node)):
            best = max(best, _lp(succ))
        memo[node] = 1 + best
        return memo[node]

    return _lp(node_id)


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


def _longest_from_all(dag: nx.DiGraph) -> dict[str, int] | None:
    """Longest-path-from for every node via reverse-topological DP. O(V+E)."""
    order = _topological_order(dag)
    if order is None:
        return None
    longest: dict[str, int] = {}
    for node in reversed(order):
        best = 0
        for succ in dag.successors(node):
            value = longest[succ]
            if value > best:
                best = value
        longest[node] = 1 + best
    return longest


def criticality_profile(graph: ProgressGraph) -> dict[str, float]:
    """criticality for every remaining node in one DP: longest-from / global-longest."""
    dag = remaining_dag(graph)
    longest = _longest_from_all(dag)
    if longest is None:
        return {}
    total = max(longest.values()) if longest else 0
    if total == 0:
        return {}
    return {node: longest[node] / total for node in longest}


def criticality(graph: ProgressGraph, node_id: str) -> float:
    """longest remaining path through node / global longest remaining path."""
    return criticality_profile(graph).get(node_id, 0.0)


def _descendants_counts(dag: nx.DiGraph) -> dict[str, set[str]] | None:
    """Descendant set for every node via reverse-topological DP."""
    order = _topological_order(dag)
    if order is None:
        return None
    descendants: dict[str, set[str]] = {}
    for node in reversed(order):
        down: set[str] = set()
        for succ in dag.successors(node):
            down.add(succ)
            down.update(descendants[succ])
        descendants[node] = down
    return descendants


def unlock_profile(graph: ProgressGraph) -> dict[str, float]:
    """unlock score for every remaining node in one DP: descendants / total."""
    dag = remaining_dag(graph)
    total = float(dag.number_of_nodes())
    if total == 0:
        return {}
    descendants = _descendants_counts(dag)
    if descendants is None:
        return {}
    return {node: len(down) / total for node, down in descendants.items()}


def unlock_score(graph: ProgressGraph, node_id: str) -> float:
    """blocked descendant count / total remaining nodes (spec 11.2)."""
    return unlock_profile(graph).get(node_id, 0.0)