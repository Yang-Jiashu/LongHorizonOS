"""Oracle computation for controlled tasks (spec 22, 24.3).

Runs offline over the generated DEPENDS_ON DAG, before any mode executes.
The oracle knows: the true time-critical path (denominator of Critical-path
Stretch), per-node criticality (priority hints for oracle modes), and the true
affected scope of every scripted environment event (denominator of Replanning
Amplification).
"""

from __future__ import annotations

from typing import Any

import networkx as nx

from lhos.benchmarks.controlled.task_schema import OracleInfo

_DEFAULT_TIME_MS = 1000


def _depends_on_dag(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> nx.DiGraph:
    dag = nx.DiGraph()
    for n in nodes:
        dag.add_node(n["temp_id"])
    for e in edges:
        if e.get("kind", "depends_on") == "depends_on":
            # Spec 8.1: source = dependent, target = dependency; the execution
            # DAG points dependency -> dependent.
            dag.add_edge(e["target"], e["source"])
    return dag


def _transitive_dependents(dag: nx.DiGraph, temp_id: str) -> list[str]:
    if temp_id not in dag:
        return []
    return sorted(nx.descendants(dag, temp_id))


def compute_oracle(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    environment_events: list[dict[str, Any]],
) -> OracleInfo:
    dag = _depends_on_dag(nodes, edges)
    time_ms = {
        n["temp_id"]: float(n.get("estimated_time_ms") or _DEFAULT_TIME_MS)
        for n in nodes
    }

    # Time-longest path: longest path weighted by estimated node time.
    dist: dict[str, float] = {}
    parent: dict[str, str | None] = {}
    for node_id in nx.topological_sort(dag):
        best = 0.0
        best_pred: str | None = None
        for pred in dag.predecessors(node_id):
            if dist[pred] > best:
                best = dist[pred]
                best_pred = pred
        dist[node_id] = best + time_ms.get(node_id, _DEFAULT_TIME_MS)
        parent[node_id] = best_pred
    critical_path: list[str] = []
    if dist:
        cursor = max(dist, key=lambda n: (dist[n], n))
        while cursor is not None:
            critical_path.append(cursor)
            cursor = parent[cursor]
        critical_path.reverse()
    cp_seconds = sum(time_ms.get(t, _DEFAULT_TIME_MS) for t in critical_path) / 1000.0

    # Oracle criticality: (longest time-chain through the node) / (critical
    # path time), mirroring runtime critical_path.criticality but time-based
    # and computed over the full (not remaining) graph.
    total = cp_seconds or 1.0
    longest_from: dict[str, float] = {}

    def _from(node_id: str) -> float:
        if node_id in longest_from:
            return longest_from[node_id]
        best = 0.0
        for succ in dag.successors(node_id):
            best = max(best, _from(succ))
        longest_from[node_id] = time_ms.get(node_id, _DEFAULT_TIME_MS) + best
        return longest_from[node_id]

    longest_to: dict[str, float] = {}

    def _to(node_id: str) -> float:
        if node_id in longest_to:
            return longest_to[node_id]
        best = 0.0
        for pred in dag.predecessors(node_id):
            best = max(best, _to(pred))
        longest_to[node_id] = time_ms.get(node_id, _DEFAULT_TIME_MS) + best
        return longest_to[node_id]

    priorities = {
        n["temp_id"]: round(
            min(1.0, (_to(n["temp_id"]) + _from(n["temp_id"]) - time_ms.get(n["temp_id"], _DEFAULT_TIME_MS)) / total),
            6,
        )
        for n in nodes
    }

    # True affected scope per scripted event, keyed by the changed node's
    # temp_id: declared victims plus their transitive dependents, minus the
    # injector node that fires the event (its own work is the change itself).
    affected_by_event: dict[str, list[str]] = {}
    for raw in environment_events:
        changed = raw.get("node_id")
        victims = list(raw.get("invalidates") or raw.get("oracle_victims") or [])
        source_node = raw.get("source_node")
        scope: set[str] = set()
        for victim in victims:
            scope.add(victim)
            scope.update(_transitive_dependents(dag, victim))
        scope.discard(source_node)
        scope.discard(changed)
        if changed:
            affected_by_event[changed] = sorted(scope)
            # Embed the scope in the event payload itself: it is appended to
            # the event log verbatim, so scoring can read the oracle
            # denominator without re-deriving the topology.
            raw["oracle_affected"] = sorted(scope)

    return OracleInfo(
        critical_path=critical_path,
        critical_path_seconds=round(cp_seconds, 6),
        affected_by_event=affected_by_event,
        priorities=priorities,
    )
