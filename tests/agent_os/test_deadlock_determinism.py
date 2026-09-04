"""Deadlock detection must be deterministic and must not recurse.

``_recover_deadlock`` kills a victim chosen from ``cycles[0]``, so the order and
contents of the detected cycles decide **which process gets killed**. The previous
implementation iterated an unsorted ``set`` of neighbours, which makes traversal
order depend on ``PYTHONHASHSEED``. On the wait-for graph
``{p1: {p2, p3}, p2: {p3, p1}, p3: {p1, p2}}`` four seeds produced four different
answers, so identical state selected different victims across processes.

This is the kind of nondeterminism a normal single-process test run cannot see:
hash randomisation is fixed for the lifetime of one interpreter. The seed sweep
below is therefore run as a subprocess matrix.

The traversal is also iterative now. A deep wait chain previously risked
exhausting the recursion limit inside deadlock recovery, which is the worst
possible time to raise ``RecursionError``.
"""

from __future__ import annotations

import os
import subprocess
import sys

from lhos.agent_os.services.lease_service import LeaseService

_AMBIGUOUS_GRAPH = "{'p1': {'p2','p3'}, 'p2': {'p3','p1'}, 'p3': {'p1','p2'}}"


def test_cycles_are_identical_across_hash_seeds() -> None:
    """The regression itself: same graph, different PYTHONHASHSEED, same answer."""

    script = (
        "from lhos.agent_os.services.lease_service import LeaseService;"
        f"print(LeaseService._find_cycles({_AMBIGUOUS_GRAPH}))"
    )
    results = set()
    for seed in ("0", "1", "2", "3", "7", "42"):
        completed = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            check=True,
            env={**os.environ, "PYTHONHASHSEED": seed},
        )
        results.add(completed.stdout.strip())
    assert len(results) == 1, f"hash seed changed the detected cycles: {sorted(results)}"


def test_result_is_sorted_and_deduplicated() -> None:
    cycles = LeaseService._find_cycles({"p1": {"p2", "p3"}, "p2": {"p3", "p1"}, "p3": {"p1", "p2"}})
    assert cycles == sorted(cycles)
    assert len({tuple(cycle) for cycle in cycles}) == len(cycles)


def test_every_reported_cycle_is_a_real_cycle() -> None:
    """Soundness. A false cycle would kill a process that was not deadlocked."""

    graphs = [
        {"a": {"b"}, "b": {"c"}, "c": {"a"}},
        {"p1": {"p2"}, "p2": {"p1"}},
        {"x": {"y", "z"}, "y": {"z"}, "z": {"x"}},
        {"n0": {"n1"}, "n1": {"n2"}, "n2": {"n0", "n3"}, "n3": {"n4"}, "n4": {"n2"}},
    ]
    for wait_for in graphs:
        for cycle in LeaseService._find_cycles(wait_for):
            assert len(cycle) >= 2
            for index, node in enumerate(cycle):
                following = cycle[(index + 1) % len(cycle)]
                assert following in wait_for.get(node, set()), (cycle, node, following)


def test_cycles_start_at_their_smallest_pid() -> None:
    """Rotation is what makes two discoveries of one cycle compare equal."""

    for cycle in LeaseService._find_cycles({"p3": {"p1"}, "p1": {"p2"}, "p2": {"p3"}}):
        assert cycle[0] == min(cycle)


def test_acyclic_graphs_report_nothing() -> None:
    assert LeaseService._find_cycles({}) == []
    assert LeaseService._find_cycles({"p1": set()}) == []
    # A chain, and a diamond: both have no cycle.
    assert LeaseService._find_cycles({"p1": {"p2"}, "p2": {"p3"}}) == []
    assert LeaseService._find_cycles({"a": {"b", "c"}, "b": {"d"}, "c": {"d"}, "d": set()}) == []


def test_at_least_one_cycle_is_found_per_deadlocked_group() -> None:
    """Liveness. Missing a cycle entirely means a deadlock is never recovered."""

    two_groups = {
        "a1": {"a2"},
        "a2": {"a1"},
        "b1": {"b2"},
        "b2": {"b3"},
        "b3": {"b1"},
        "free": {"a1"},
    }
    cycles = LeaseService._find_cycles(two_groups)
    covered = {node for cycle in cycles for node in cycle}
    assert {"a1", "a2"} <= covered
    assert {"b1", "b2", "b3"} <= covered
    assert "free" not in covered


def test_a_deep_wait_chain_does_not_exhaust_the_recursion_limit() -> None:
    """The old recursive DFS would raise RecursionError during recovery."""

    depth = sys.getrecursionlimit() * 2
    chain = {f"p{index}": {f"p{index + 1}"} for index in range(depth)}
    chain[f"p{depth}"] = {"p0"}

    cycles = LeaseService._find_cycles(chain)

    assert len(cycles) == 1
    assert len(cycles[0]) == depth + 1
    assert cycles[0][0] == min(cycles[0])
