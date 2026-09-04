"""Conflict derivation: equivalence against brute force, plus the scaling win.

Two tasks can only conflict through a resource they share, so comparing every
pair is wasted work.  The derivation inverts the access sets instead.  That is
only a safe change if the output is *identical* -- the derived graph's payload is
hashed, so both the pairs and their emission order are part of its identity.

So the real test here is not "does it look right" but a randomized differential
against a brute-force reference, including the awkward cases: unknown access
(which conflicts with everything), self-overlapping read/write sets, duplicate
resources, and tasks sharing nothing at all.
"""

from __future__ import annotations

import random
import time

from lhos.sdk.conflict_graph import (
    ConflictReason,
    TaskAccessSet,
    _derive_conflicts,
)


def _reference(access_sets: tuple[TaskAccessSet, ...]) -> list[tuple]:
    """The original O(T^2) pairwise scan, kept as an independent oracle."""

    out: list[tuple] = []
    for index, left in enumerate(access_sets):
        for right in access_sets[index + 1 :]:
            reasons: set[ConflictReason] = set()
            resources: set[str] = set()
            if not left.known or not right.known:
                reasons.add(ConflictReason.UNKNOWN_ACCESS)
            else:
                ww = set(left.write_set) & set(right.write_set)
                rw = (set(left.write_set) & set(right.read_set)) | (
                    set(right.write_set) & set(left.read_set)
                )
                if ww:
                    reasons.add(ConflictReason.WRITE_WRITE)
                    resources.update(ww)
                if rw:
                    reasons.add(ConflictReason.READ_WRITE)
                    resources.update(rw)
            if reasons:
                out.append(
                    (
                        left.task_id,
                        right.task_id,
                        tuple(sorted(resources)),
                        tuple(sorted(reasons, key=lambda item: item.value)),
                    )
                )
    return out


def _actual(access_sets: tuple[TaskAccessSet, ...]) -> list[tuple]:
    return [
        (pair.left_task_id, pair.right_task_id, pair.resources, pair.reasons)
        for pair in _derive_conflicts(access_sets)
    ]


def _random_sets(rng: random.Random, count: int, pool: int) -> tuple[TaskAccessSet, ...]:
    """Build access sets with lexically ordered ids.

    ``ConflictPair`` requires its endpoints to be distinct and lexically
    ordered, and the real caller sorts by ``task_id`` before deriving.  Ids are
    zero-padded so lexical order matches numeric order -- unpadded ``t10`` sorts
    before ``t2`` and would violate the precondition.
    """

    resources = [f"artifact://r{index}" for index in range(pool)]
    sets = []
    for index in range(count):
        reads = tuple(rng.sample(resources, rng.randint(0, min(3, pool))))
        writes = tuple(rng.sample(resources, rng.randint(0, min(2, pool))))
        known = rng.random() > 0.15
        sets.append(
            TaskAccessSet(
                task_id=f"t{index:03d}",
                read_set=reads,
                write_set=writes,
                known=known and bool(reads or writes),
            )
        )
    return tuple(sets)


def test_matches_brute_force_across_randomized_graphs() -> None:
    rng = random.Random(20260817)

    for _ in range(200):
        access_sets = _random_sets(rng, rng.randint(0, 14), rng.randint(1, 6))

        assert _actual(access_sets) == _reference(access_sets)


def test_matches_brute_force_when_everything_shares_one_resource() -> None:
    """The degenerate case where the inverted index cannot help."""

    access_sets = tuple(
        TaskAccessSet(
            task_id=f"t{index:03d}",
            read_set=("artifact://hot",),
            write_set=("artifact://hot",),
            known=True,
        )
        for index in range(10)
    )

    assert _actual(access_sets) == _reference(access_sets)


def test_matches_brute_force_when_nothing_is_shared() -> None:
    access_sets = tuple(
        TaskAccessSet(
            task_id=f"t{index:03d}",
            read_set=(f"artifact://in{index}",),
            write_set=(f"artifact://out{index}",),
            known=True,
        )
        for index in range(10)
    )

    assert _actual(access_sets) == []
    assert _reference(access_sets) == []


def test_unknown_access_still_conflicts_with_everything() -> None:
    access_sets = (
        TaskAccessSet(task_id="t000-unknown", known=False),
        *(
            TaskAccessSet(
                task_id=f"t{index:03d}-known",
                read_set=(f"artifact://in{index}",),
                known=True,
            )
            for index in range(1, 5)
        ),
    )

    actual = _actual(access_sets)
    assert actual == _reference(access_sets)
    assert len(actual) == 4
    assert all(ConflictReason.UNKNOWN_ACCESS in pair[3] for pair in actual)


def test_sparse_graph_is_much_cheaper_than_the_pairwise_scan() -> None:
    """Independent tasks must not cost a quadratic scan."""

    access_sets = tuple(
        TaskAccessSet(
            task_id=f"t{index:03d}",
            read_set=(f"artifact://in{index}",),
            write_set=(f"artifact://out{index}",),
            known=True,
        )
        for index in range(600)
    )

    started = time.perf_counter()
    assert _derive_conflicts(access_sets) == ()
    indexed = time.perf_counter() - started

    started = time.perf_counter()
    assert _reference(access_sets) == []
    brute = time.perf_counter() - started

    # 600 independent tasks: 179,700 pairs for the scan versus none for the
    # index.  A wide margin is asserted rather than a tight one so host noise
    # cannot make this flaky, while a regression to quadratic still fails.
    assert indexed * 5 < brute
