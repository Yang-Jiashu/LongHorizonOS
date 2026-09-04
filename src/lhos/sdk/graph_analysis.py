"""Deterministic graph-derived scheduling signals.

The helpers in this module are deliberately relative to the *declared* VPG
dependency graph.  They do not infer hidden file, tool, model, or semantic
dependencies.  ``X depends_on Y`` is represented by a VPG edge
``X -> Y``; execution/readiness therefore follows the reverse direction
``Y -> X``.

The result is an observation-only projection:

* ``critical_path`` is the longest unresolved task chain leading to the goal,
  reported in prerequisite-to-consumer order;
* ``downstream_unlock_values`` counts direct consumers whose remaining
  declared prerequisites are already VERIFIED (an immediate structural unlock
  signal, not a success/cost prediction);
* ``parallel_frontier`` is the deterministic VPG READY frontier.  The D1
  readiness predicate makes it a dependency antichain because a READY task
  cannot depend on another unverified/STALE READY task.

These are intentionally conservative structural signals.  Resource admission,
worker eligibility, explicit ConflictGraph access sets, and ownership remain
the responsibility of the existing scheduler.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lhos.runtimes.verified_progress.models import (
    AnyNode,
    EdgeType,
    NodeType,
    NodeValidity,
    VPGEdge,
)

_OPEN_VALIDITIES = frozenset(
    {
        NodeValidity.UNVERIFIED.value,
        NodeValidity.STALE.value,
    }
)


@dataclass(frozen=True)
class DownstreamUnlock:
    """Structural downstream value for one task."""

    task_id: str
    value: int


@dataclass(frozen=True)
class GraphAnalysis:
    """Pure graph-derived signals for one immutable VPG projection."""

    critical_path: tuple[str, ...] = ()
    downstream_unlock_values: tuple[DownstreamUnlock, ...] = ()
    parallel_frontier: tuple[str, ...] = ()


def derive_graph_analysis(
    *,
    goal_id: str,
    nodes: dict[str, AnyNode] | dict[str, Any],
    edges: list[VPGEdge] | list[Any],
    ready_frontier: tuple[str, ...] | list[str] = (),
    repair_ready_frontier: tuple[str, ...] | list[str] = (),
) -> GraphAnalysis:
    """Derive deterministic control-state signals from one graph snapshot.

    ``nodes`` and ``edges`` are treated as immutable inputs.  Unknown edge
    endpoints are ignored, matching the VPG readiness behavior for legacy
    projections while keeping the analysis fail-closed.
    """

    task_nodes = {
        node_id: node for node_id, node in nodes.items() if _node_type(node) == NodeType.TASK.value
    }
    depends_on = _task_dependency_index(task_nodes, edges)
    consumers = _reverse_dependency_index(depends_on)
    goal_tasks = tuple(
        sorted(
            target
            for source, target in _depends_on_pairs(edges)
            if source == goal_id and target in task_nodes
        )
    )
    critical_path = _critical_path(
        goal_tasks=goal_tasks,
        task_nodes=task_nodes,
        depends_on=depends_on,
    )
    unlock_values = tuple(
        DownstreamUnlock(
            task_id=task_id,
            value=_immediate_unlock_value(
                task_id,
                consumers,
                task_nodes,
                depends_on,
            ),
        )
        for task_id in sorted(task_nodes)
    )
    parallel_frontier = _parallel_frontier(
        ready_frontier=ready_frontier,
        repair_ready_frontier=repair_ready_frontier,
        depends_on=depends_on,
    )
    return GraphAnalysis(
        critical_path=critical_path,
        downstream_unlock_values=unlock_values,
        parallel_frontier=parallel_frontier,
    )


def _depends_on_pairs(edges: list[VPGEdge] | list[Any]) -> tuple[tuple[str, str], ...]:
    pairs = {
        (str(getattr(edge, "source_node_id", "")), str(getattr(edge, "target_node_id", "")))
        for edge in edges
        if _edge_type(edge) == EdgeType.DEPENDS_ON.value
        and str(getattr(edge, "source_node_id", ""))
        and str(getattr(edge, "target_node_id", ""))
    }
    return tuple(sorted(pairs))


def _depends_on_pairs_set(edges: list[VPGEdge] | list[Any]) -> frozenset[tuple[str, str]]:
    """Unordered ``depends_on`` edge set used as an exact structural signature.

    Identical to the set built inside :func:`_depends_on_pairs` but without the
    sort, since a signature only needs equality, not ordering.
    """

    return frozenset(
        (str(getattr(edge, "source_node_id", "")), str(getattr(edge, "target_node_id", "")))
        for edge in edges
        if _edge_type(edge) == EdgeType.DEPENDS_ON.value
        and str(getattr(edge, "source_node_id", ""))
        and str(getattr(edge, "target_node_id", ""))
    )


def _task_dependency_index(
    task_nodes: dict[str, Any],
    edges: list[VPGEdge] | list[Any],
) -> dict[str, tuple[str, ...]]:
    """Return consumer -> task prerequisites, sorted and task-only."""

    index: dict[str, set[str]] = {task_id: set() for task_id in task_nodes}
    for source, target in _depends_on_pairs(edges):
        if source in task_nodes and target in task_nodes:
            index[source].add(target)
    return {task_id: tuple(sorted(values)) for task_id, values in index.items()}


def _reverse_dependency_index(
    depends_on: dict[str, tuple[str, ...]],
) -> dict[str, tuple[str, ...]]:
    """Return prerequisite -> direct task consumers."""

    consumers: dict[str, set[str]] = {task_id: set() for task_id in depends_on}
    for consumer, prerequisites in depends_on.items():
        for prerequisite in prerequisites:
            consumers.setdefault(prerequisite, set()).add(consumer)
    return {task_id: tuple(sorted(values)) for task_id, values in consumers.items()}


def _immediate_unlock_value(
    task_id: str,
    consumers: dict[str, tuple[str, ...]],
    task_nodes: dict[str, Any],
    depends_on: dict[str, tuple[str, ...]],
) -> int:
    """Count direct consumers this task would structurally unblock next.

    The count is linear in the declared dependency edges and keeps the
    serialized RuntimeStateView O(V), avoiding a transitive-descendant list for
    every task.
    """

    value = 0
    for consumer in consumers.get(task_id, ()):
        consumer_node = task_nodes.get(consumer)
        if consumer_node is None or not _is_open(consumer_node):
            continue
        other_dependencies = (
            dependency for dependency in depends_on.get(consumer, ()) if dependency != task_id
        )
        if all(
            dependency in task_nodes and _is_verified(task_nodes[dependency])
            for dependency in other_dependencies
        ):
            value += 1
    return value


def _critical_path(
    *,
    goal_tasks: tuple[str, ...],
    task_nodes: dict[str, Any],
    depends_on: dict[str, tuple[str, ...]],
) -> tuple[str, ...]:
    """Find the longest unresolved path ending at a goal task.

    Verified prerequisites are traversed but omitted from the returned path;
    this keeps the signal focused on work that may still consume compute.
    Invalid tasks are excluded because they are not safely executable until a
    separate semantic repair/reconciliation step changes their state.
    """

    memo: dict[str, tuple[str, ...]] = {}
    _fill_best_to(memo, task_nodes, depends_on, goal_tasks)
    return _best_path([memo[task_id] for task_id in goal_tasks])


def _fill_best_to(
    memo: dict[str, tuple[str, ...]],
    task_nodes: dict[str, Any],
    depends_on: dict[str, tuple[str, ...]],
    targets: tuple[str, ...],
) -> None:
    """Populate ``memo[t] = best_to(t)`` for every ``t`` in ``targets``.

    ``best_to(t)`` is the longest chain of *open* tasks ending at ``t``,
    traversing declared prerequisites.  It is a pure function of the task
    validities and the (immutable) dependency structure, so any entries already
    present in ``memo`` are reused verbatim.  Callers doing incremental updates
    pre-populate ``memo`` with the still-valid entries and drop only the stale
    ones (the changed tasks and their transitive consumers); everything else is
    recomputed on demand by the recursion below.  The result is therefore
    identical to a from-scratch fill regardless of which entries were retained.
    """

    visiting: set[str] = set()

    def best_to(task_id: str) -> tuple[str, ...]:
        if task_id in memo:
            return memo[task_id]
        if task_id in visiting:
            # VPG admission rejects cycles.  Keep the observer fail-closed if
            # it is handed a malformed legacy projection.
            return ()
        visiting.add(task_id)
        node = task_nodes.get(task_id)
        if node is None or _is_invalid(node):
            visiting.remove(task_id)
            memo[task_id] = ()
            return ()
        candidates = [best_to(dep) for dep in depends_on.get(task_id, ())]
        prefix = _best_path(candidates)
        result = prefix + ((task_id,) if _is_open(node) else ())
        visiting.remove(task_id)
        memo[task_id] = result
        return result

    for task_id in targets:
        best_to(task_id)


def _best_path(paths: list[tuple[str, ...]]) -> tuple[str, ...]:
    """Longest path with lexical tie-breaking for byte-stable output."""

    if not paths:
        return ()
    return min(paths, key=lambda path: (-len(path), path))


def _parallel_frontier(
    *,
    ready_frontier: tuple[str, ...] | list[str],
    repair_ready_frontier: tuple[str, ...] | list[str],
    depends_on: dict[str, tuple[str, ...]],
) -> tuple[str, ...]:
    """Return the deterministic dependency-safe logical parallel frontier."""

    candidates: list[str] = []
    seen: set[str] = set()
    for task_id in sorted((*ready_frontier, *repair_ready_frontier)):
        normalized = str(task_id).strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            candidates.append(normalized)
    candidate_set = set(candidates)
    return tuple(
        candidate
        for candidate in candidates
        if not candidate_set.intersection(depends_on.get(candidate, ()))
    )


def _is_open(node: Any) -> bool:
    validity = getattr(getattr(node, "validity", None), "value", getattr(node, "validity", ""))
    return str(validity) in _OPEN_VALIDITIES


def _is_verified(node: Any) -> bool:
    validity = getattr(getattr(node, "validity", None), "value", getattr(node, "validity", ""))
    return str(validity) == NodeValidity.VERIFIED.value


def _is_invalid(node: Any) -> bool:
    validity = getattr(getattr(node, "validity", None), "value", getattr(node, "validity", ""))
    return str(validity) == NodeValidity.INVALID.value


def _node_type(node: Any) -> str:
    return str(getattr(getattr(node, "node_type", None), "value", getattr(node, "node_type", "")))


def _edge_type(edge: Any) -> str:
    return str(getattr(getattr(edge, "edge_type", None), "value", getattr(edge, "edge_type", "")))


def _validity_value(node: Any) -> str:
    return str(getattr(getattr(node, "validity", None), "value", getattr(node, "validity", "")))


def _transitive_consumers(
    seeds: list[str] | set[str],
    consumers: dict[str, tuple[str, ...]],
) -> set[str]:
    """Return ``seeds`` plus every task that transitively depends on a seed.

    ``consumers`` maps prerequisite -> direct consumers, so this walk follows
    the execution (unblock) direction.  ``best_to(x)`` is a pure function of the
    validities of ``x`` and its transitive prerequisites, therefore a validity
    change at a seed can only alter ``best_to`` for the seed itself and for its
    transitive consumers — exactly the set returned here.
    """

    affected: set[str] = set()
    stack = list(seeds)
    while stack:
        current = stack.pop()
        if current in affected:
            continue
        affected.add(current)
        stack.extend(consumers.get(current, ()))
    return affected


class IncrementalGraphAnalyzer:
    """Stateful, epoch-to-epoch incremental form of :func:`derive_graph_analysis`.

    The declared VPG dependency structure is immutable across a scheduling run;
    between epochs only task *validities* change (typically one task becomes
    VERIFIED).  Recomputing the critical path and downstream-unlock values from
    scratch every epoch is therefore mostly wasted work.

    ``analyze`` caches the structural indices plus the previous critical-path
    memo and unlock values.  On each call it:

    * rebuilds a cheap exact structural signature (the task-id set and the
      ``depends_on`` edge set) and falls back to a full recompute whenever the
      structure — or the goal — changes, or on the first call;
    * otherwise recomputes only the affected region:

      - *critical path*: the changed tasks and their transitive consumers have
        their memoized ``best_to`` dropped and refilled; every other entry is
        reused verbatim (see :func:`_fill_best_to`);
      - *unlock values*: only tasks whose value can structurally change are
        recomputed.  ``unlock(x)`` reads the validity of ``x``'s consumers and
        of *their* prerequisites, so a change at ``t`` affects ``t``'s
        prerequisites and their sibling prerequisites — ``depends_on[t]`` plus
        ``depends_on[C]`` for every consumer ``C`` of ``t`` — never ``t``'s
        descendants.

    The result is byte-for-byte identical to :func:`derive_graph_analysis`; the
    output is a deterministic function of the graph snapshot alone, independent
    of how many epochs or in what order the caller arrived at it.  No wall-clock
    or RNG is consulted.  The ``parallel_frontier`` depends on the per-call
    ready inputs and is (cheaply) recomputed every call.
    """

    def __init__(self) -> None:
        self._ready = False
        self._goal_id = ""
        self._task_ids: frozenset[str] = frozenset()
        self._sorted_task_ids: tuple[str, ...] = ()
        self._pairs: frozenset[tuple[str, str]] = frozenset()
        self._depends_on: dict[str, tuple[str, ...]] = {}
        self._consumers: dict[str, tuple[str, ...]] = {}
        self._goal_tasks: tuple[str, ...] = ()
        self._validity: dict[str, str] = {}
        self._memo: dict[str, tuple[str, ...]] = {}
        self._unlock: dict[str, int] = {}

    def analyze(
        self,
        *,
        goal_id: str,
        nodes: dict[str, AnyNode] | dict[str, Any],
        edges: list[VPGEdge] | list[Any],
        ready_frontier: tuple[str, ...] | list[str] = (),
        repair_ready_frontier: tuple[str, ...] | list[str] = (),
    ) -> GraphAnalysis:
        """Return the graph analysis for one snapshot, reusing prior work."""

        task_nodes = {
            node_id: node
            for node_id, node in nodes.items()
            if _node_type(node) == NodeType.TASK.value
        }
        task_ids = frozenset(task_nodes)
        pairs = _depends_on_pairs_set(edges)

        structure_matches = (
            self._ready
            and goal_id == self._goal_id
            and task_ids == self._task_ids
            and pairs == self._pairs
        )
        if not structure_matches:
            return self._recompute_full(
                goal_id=goal_id,
                task_nodes=task_nodes,
                task_ids=task_ids,
                pairs=pairs,
                edges=edges,
                ready_frontier=ready_frontier,
                repair_ready_frontier=repair_ready_frontier,
            )

        changed = [
            task_id
            for task_id, node in task_nodes.items()
            if _validity_value(node) != self._validity.get(task_id)
        ]
        if changed:
            self._apply_validity_changes(changed, task_nodes)

        critical_path = _best_path([self._memo[task_id] for task_id in self._goal_tasks])
        unlock_values = tuple(
            DownstreamUnlock(task_id=task_id, value=self._unlock[task_id])
            for task_id in self._sorted_task_ids
        )
        parallel_frontier = _parallel_frontier(
            ready_frontier=ready_frontier,
            repair_ready_frontier=repair_ready_frontier,
            depends_on=self._depends_on,
        )
        return GraphAnalysis(
            critical_path=critical_path,
            downstream_unlock_values=unlock_values,
            parallel_frontier=parallel_frontier,
        )

    def _recompute_full(
        self,
        *,
        goal_id: str,
        task_nodes: dict[str, Any],
        task_ids: frozenset[str],
        pairs: frozenset[tuple[str, str]],
        edges: list[VPGEdge] | list[Any],
        ready_frontier: tuple[str, ...] | list[str],
        repair_ready_frontier: tuple[str, ...] | list[str],
    ) -> GraphAnalysis:
        depends_on = _task_dependency_index(task_nodes, edges)
        consumers = _reverse_dependency_index(depends_on)
        goal_tasks = tuple(
            sorted(
                target
                for source, target in _depends_on_pairs(edges)
                if source == goal_id and target in task_nodes
            )
        )
        memo: dict[str, tuple[str, ...]] = {}
        _fill_best_to(memo, task_nodes, depends_on, goal_tasks)
        critical_path = _best_path([memo[task_id] for task_id in goal_tasks])
        unlock = {
            task_id: _immediate_unlock_value(task_id, consumers, task_nodes, depends_on)
            for task_id in task_nodes
        }
        sorted_task_ids = tuple(sorted(task_nodes))
        unlock_values = tuple(
            DownstreamUnlock(task_id=task_id, value=unlock[task_id]) for task_id in sorted_task_ids
        )
        parallel_frontier = _parallel_frontier(
            ready_frontier=ready_frontier,
            repair_ready_frontier=repair_ready_frontier,
            depends_on=depends_on,
        )

        self._ready = True
        self._goal_id = goal_id
        self._task_ids = task_ids
        self._sorted_task_ids = sorted_task_ids
        self._pairs = pairs
        self._depends_on = depends_on
        self._consumers = consumers
        self._goal_tasks = goal_tasks
        self._validity = {task_id: _validity_value(node) for task_id, node in task_nodes.items()}
        self._memo = memo
        self._unlock = unlock

        return GraphAnalysis(
            critical_path=critical_path,
            downstream_unlock_values=unlock_values,
            parallel_frontier=parallel_frontier,
        )

    def _apply_validity_changes(
        self,
        changed: list[str],
        task_nodes: dict[str, Any],
    ) -> None:
        # Critical path: drop the changed tasks and their transitive consumers
        # from the memo, then refill along the goal tasks.  Retained entries are
        # provably still correct, so the refilled memo matches a full rebuild.
        for stale in _transitive_consumers(changed, self._consumers):
            self._memo.pop(stale, None)
        _fill_best_to(self._memo, task_nodes, self._depends_on, self._goal_tasks)

        # Unlock values: recompute only the tasks whose value can change.
        affected_unlock: set[str] = set()
        for task_id in changed:
            affected_unlock.update(self._depends_on.get(task_id, ()))
            for consumer in self._consumers.get(task_id, ()):
                affected_unlock.update(self._depends_on.get(consumer, ()))
            affected_unlock.add(task_id)
        for task_id in affected_unlock:
            if task_id in task_nodes:
                self._unlock[task_id] = _immediate_unlock_value(
                    task_id, self._consumers, task_nodes, self._depends_on
                )

        for task_id in changed:
            self._validity[task_id] = _validity_value(task_nodes[task_id])


__all__ = [
    "DownstreamUnlock",
    "GraphAnalysis",
    "IncrementalGraphAnalyzer",
    "derive_graph_analysis",
]
