"""The batch planner's indexed lookups must equal the model's scan-based ones.

``DynamicParallelismPolicy.suggest`` used to answer every question by scanning a
tuple on the frozen model: ``access_for`` walked the access sets and
``conflicts_with`` walked the conflict pairs. Both were called once per candidate,
and ``conflicts_with`` additionally once per already-selected task, so the pass
degraded to ``O(candidates^2 * conflicts)``. It now indexes once per pass.

The planner's decision logic was not touched -- only those three lookups were
rewired. So proving the lookups are equivalent on arbitrary inputs is what proves
the suggestion payload is unchanged, and that matters because the payload is
hashed and callers compare those hashes.

The indexes are built per pass rather than cached on ``ConflictGraph`` on purpose.
The model is frozen and its payload is hashed, so an instance-level cache would
risk entering that payload, and ``model_copy`` carries private attributes across,
which would let a cache outlive the ``conflicts`` tuple it was derived from.
"""

from __future__ import annotations

import random

from lhos.sdk.conflict_graph import ConflictGraph, TaskAccessSet


def _random_graph(seed: int, task_count: int) -> ConflictGraph:
    rnd = random.Random(seed)
    resources = [f"artifact://r{index}" for index in range(max(3, task_count // 3))]
    access_sets = []
    for index in range(task_count):
        access_sets.append(
            TaskAccessSet(
                task_id=f"t{index:03d}",
                read_set=tuple(rnd.sample(resources, rnd.randint(0, min(3, len(resources))))),
                write_set=tuple(rnd.sample(resources, rnd.randint(0, min(2, len(resources))))),
                known=rnd.random() > 0.15,
            )
        )
    return ConflictGraph.from_access_sets(access_sets)


def _indexes(graph: ConflictGraph) -> tuple[dict, dict]:
    """Rebuild exactly what ``suggest`` builds, so the two stay in step."""

    access_by_task = {item.task_id: item for item in graph.access_sets}
    conflict_neighbours: dict[str, set[str]] = {}
    for pair in graph.conflicts:
        conflict_neighbours.setdefault(pair.left_task_id, set()).add(pair.right_task_id)
        conflict_neighbours.setdefault(pair.right_task_id, set()).add(pair.left_task_id)
    return access_by_task, conflict_neighbours


def test_indexed_access_lookup_matches_the_scan() -> None:
    for seed in range(40):
        graph = _random_graph(seed, task_count=(seed % 30) + 1)
        access_by_task, _neighbours = _indexes(graph)
        for task_id in graph.task_ids:
            assert access_by_task.get(task_id) == graph.access_for(task_id)
        # Absent ids must resolve to None through both paths.
        for missing in ("nope", "t999", ""):
            assert access_by_task.get(missing.strip()) is graph.access_for(missing)


def test_indexed_conflict_lookup_matches_the_scan_on_every_pair() -> None:
    """Exhaustive over all ordered pairs, including self-pairs."""

    for seed in range(30):
        graph = _random_graph(seed, task_count=(seed % 18) + 2)
        _access, neighbours = _indexes(graph)
        ids = graph.task_ids
        for left in ids:
            for right in ids:
                indexed = left != right and right in neighbours.get(left, ())
                assert indexed == graph.conflicts_with(left, right), (seed, left, right)


def test_conflict_index_is_symmetric() -> None:
    """Conflicts are undirected; the pairs are stored lexically ordered only."""

    for seed in range(20):
        graph = _random_graph(seed, task_count=(seed % 15) + 2)
        _access, neighbours = _indexes(graph)
        for task_id, others in neighbours.items():
            for other in others:
                assert task_id in neighbours[other], (task_id, other)


def test_index_covers_every_conflict_pair() -> None:
    """No pair may be dropped, or two conflicting tasks would be co-scheduled."""

    for seed in range(20):
        graph = _random_graph(seed, task_count=(seed % 20) + 2)
        _access, neighbours = _indexes(graph)
        edges = {
            tuple(sorted((task_id, other)))
            for task_id, others in neighbours.items()
            for other in others
        }
        assert edges == {(pair.left_task_id, pair.right_task_id) for pair in graph.conflicts}
