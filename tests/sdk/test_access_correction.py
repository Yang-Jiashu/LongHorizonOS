"""Observed undeclared reads must correct future conflict decisions.

The conflict graph is built from *declared* access sets, so an undeclared read is
the dangerous error: two genuinely conflicting tasks look independent and get
co-scheduled. Detecting that after the fact was already possible; the loop was
open because the next epoch would repeat the same wrong batching.

The two properties worth pinning: corrections only ever *widen* a read set, and
an unobserved attempt corrects nothing rather than being read as confirmation.
"""

from __future__ import annotations

from types import SimpleNamespace

from lhos.sdk.access_correction import correct_access_set, correct_access_sets
from lhos.sdk.conflict_graph import ConflictGraph, TaskAccessSet


def _report(*, observed=True, undeclared=(), unread=()) -> SimpleNamespace:
    return SimpleNamespace(
        observed=observed,
        observed_reads=tuple(undeclared),
        undeclared_reads=tuple(undeclared),
        declared_but_unread=tuple(unread),
    )


def test_undeclared_read_is_folded_into_the_declared_set() -> None:
    declared = TaskAccessSet(task_id="t1", read_set=("a",), write_set=("out1",), known=True)

    correction = correct_access_set(declared, _report(undeclared=("hidden",)))

    assert correction.observed is True
    assert correction.added_reads == ("hidden",)
    assert correction.corrected.read_set == ("a", "hidden")
    assert correction.changed is True


def test_correction_makes_a_previously_missed_conflict_visible() -> None:
    """This is the whole point: the batching decision changes."""

    writer = TaskAccessSet(task_id="t1", read_set=(), write_set=("hidden",), known=True)
    reader = TaskAccessSet(task_id="t2", read_set=("a",), write_set=("out2",), known=True)

    before = ConflictGraph.from_access_sets((writer, reader))
    assert before.conflicts_with("t1", "t2") is False

    corrected, _audits = correct_access_sets(
        (writer, reader), {"t2": _report(undeclared=("hidden",))}
    )
    after = ConflictGraph.from_access_sets(corrected)

    assert after.conflicts_with("t1", "t2") is True


def test_declared_but_unread_never_shrinks_the_read_set() -> None:
    """A task may read an input only on some paths; dropping it would lie."""

    declared = TaskAccessSet(task_id="t1", read_set=("a", "b"), write_set=(), known=True)

    correction = correct_access_set(declared, _report(unread=("b",)))

    assert correction.corrected.read_set == ("a", "b")
    assert correction.declared_but_unread == ("b",)
    assert correction.changed is False


def test_unobserved_attempt_corrects_nothing_and_says_so() -> None:
    declared = TaskAccessSet(task_id="t1", read_set=("a",), write_set=(), known=True)

    absent = correct_access_set(declared, None)
    unobserved = correct_access_set(declared, _report(observed=False, undeclared=("hidden",)))

    for correction in (absent, unobserved):
        assert correction.observed is False
        assert correction.corrected == declared
        assert correction.added_reads == ()
        assert any(item.name == "observed_reads" for item in correction.unavailable)


def test_already_declared_read_is_not_duplicated() -> None:
    declared = TaskAccessSet(task_id="t1", read_set=("a",), write_set=(), known=True)

    correction = correct_access_set(declared, _report(undeclared=("a",)))

    assert correction.added_reads == ()
    assert correction.corrected.read_set == ("a",)


def test_collection_order_is_preserved_and_audited_per_task() -> None:
    sets = (
        TaskAccessSet(task_id="t1", read_set=("a",), known=True),
        TaskAccessSet(task_id="t2", read_set=("b",), known=True),
    )

    corrected, audits = correct_access_sets(sets, {"t2": _report(undeclared=("z",))})

    assert [item.task_id for item in corrected] == ["t1", "t2"]
    assert [item.task_id for item in audits] == ["t1", "t2"]
    assert audits[0].changed is False
    assert audits[1].added_reads == ("z",)
