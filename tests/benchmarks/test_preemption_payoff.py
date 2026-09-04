"""Regression tests for the mid-flight preemption payoff benchmark.

Wall-clock and the size of the saving are deliberately not gated: they are real
measurements and therefore noisy.  What is gated are the facts that make the
measurement *mean* anything at all:

* the supersession genuinely landed while victims were still running -- an
  earlier benchmark in this repo measured nothing because its change landed
  after the batch had finished;
* preemption actually killed something, observed from the child's own
  ``terminated_by`` report rather than inferred from a shorter runtime;
* the bystander, whose input never changed, was never touched.

That last one is the important one. A preemption that fires too broadly would
destroy valid computation, which is far worse than never firing at all.
"""

from __future__ import annotations

import pytest

from lhos.benchmarks.preemption_payoff import BYSTANDER, VICTIMS, run_benchmark

_FAST = {"repeat": 1, "victim_seconds": 0.6, "bump_seconds": 0.02}


@pytest.fixture(scope="module")
def report() -> dict:
    return run_benchmark(**_FAST)


def test_supersession_lands_midflight_only_where_the_declaration_is_wrong(
    report: dict,
) -> None:
    """The complete-declaration cell is the control and must NOT race.

    Its whole purpose is to show the conflict graph preventing the situation
    outright, so demanding that every cell land mid-flight would assert the
    control away.
    """

    incomplete = [row for row in report["runs"] if row["declaration"] == "incomplete"]
    complete = [row for row in report["runs"] if row["declaration"] == "complete"]

    assert incomplete, "no cell exercised an incomplete declaration"
    assert all(row["supersession_landed_midflight"] for row in incomplete)
    # First line of defence: a correct declaration means no concurrent race.
    assert complete
    assert not any(row["supersession_landed_midflight"] for row in complete)


def test_preemption_actually_kills_a_doomed_task(report: dict) -> None:
    preempt_runs = [row for row in report["runs"] if row["preempt_superseded"]]
    assert preempt_runs, "no arm exercised preemption"
    # Observed from the child's own termination report, not from timing.
    assert any(row["victims_killed"] for row in preempt_runs)
    assert report["headline"]["preemption_ever_fired"] is True


def test_valid_work_is_never_interrupted(report: dict) -> None:
    """The bystander reads an artifact that never changes."""

    assert report["headline"]["bystander_ever_harmed"] is False
    for row in report["runs"]:
        assert row["bystander_terminated_by"] != "semantic_interrupt"


def test_without_preemption_doomed_work_runs_to_completion(report: dict) -> None:
    baseline = [row for row in report["runs"] if not row["preempt_superseded"]]
    assert baseline
    for row in baseline:
        assert row["victims_killed"] == ()


def test_racing_runs_observed_every_task(report: dict) -> None:
    """Only the racing cells co-dispatch everything.

    In the control cell the conflict graph deliberately defers the readers to a
    later batch, so not every task is observed within the measured window.
    """

    expected = {*VICTIMS, BYSTANDER}
    racing = [row for row in report["runs"] if row["declaration"] == "incomplete"]
    assert racing
    for row in racing:
        assert expected.issubset(set(row["observed_tasks"]))
