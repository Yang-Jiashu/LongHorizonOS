"""Wrong-direction continuation: the fifth waste dimension.

This dimension was previously reported unobservable.  The obstruction was not
that the information is unknowable but that the two durable journals -- the
Scheduler event log and the VPG version log -- share no total order, and the
Scheduler's terminal events stamped the *dispatch* graph version rather than the
version in effect when an attempt finished.  Stamping the live version closes
that gap without any wall-clock correlation.

The magnitude is deliberately an upper bound: durable state records no
per-instant progress, so the split between work done before the graph moved and
work wasted after it moved is not observable.  A bound labelled as a bound is
worth having; a fabricated exact figure is not.
"""

from __future__ import annotations

from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome
from lhos.sdk.waste_projection import WasteDimension, build_waste_projection


def _pass(task_id: str, version: int = 1) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=f"out-{task_id}",
        version=version,
        content=f"{task_id}:v{version}",
    )


def test_stable_graph_reports_measured_zero_not_unavailable() -> None:
    """A run where nothing was superseded must read 0, not "unknown"."""

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("waste-wrong-direction-stable")
        goal.task("a", agent="worker", verify=lambda: _pass("a"))
        os_.run(goal, max_dispatches=1, max_steps=2)

        report = build_waste_projection(os_, goal).dimension(
            WasteDimension.WRONG_DIRECTION_CONTINUATION
        )

        assert report.observable is True
        assert report.total_count == 0
        assert report.by_task == ()
    finally:
        os_.close()


def test_terminal_events_carry_the_live_graph_version_stamp() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("waste-wrong-direction-stamp")
        goal.task("a", agent="worker", verify=lambda: _pass("a"))
        os_.run(goal, max_dispatches=1, max_steps=2)

        stamped = [
            event
            for event in os_.scheduler.events
            if isinstance(getattr(event, "metadata", None), dict)
            and "live_graph_version" in event.metadata
        ]

        assert stamped, "no terminal event carried a live_graph_version stamp"
        assert all(
            event.metadata["live_graph_version"] is None
            or isinstance(event.metadata["live_graph_version"], int)
            for event in stamped
        )
    finally:
        os_.close()


def test_a_non_verified_attempt_under_a_moved_graph_is_counted() -> None:
    """Selection logic, driven directly so the condition is unambiguous."""

    from types import SimpleNamespace

    from lhos.sdk.waste_projection import _wrong_direction_report

    def attempt(attempt_id: str, state: str, dispatched: int) -> SimpleNamespace:
        return SimpleNamespace(
            attempt_id=attempt_id,
            claim_id=attempt_id,
            task_id=attempt_id,
            state=state,
            graph_version=dispatched,
            semantic_epoch=0,
            attempt_number=1,
            agent_snapshot=None,
        )

    def event(attempt_id: str, live: int) -> SimpleNamespace:
        return SimpleNamespace(attempt_id=attempt_id, metadata={"live_graph_version": live})

    attempts = [
        attempt("moved", "failed", 3),  # graph advanced to 5 while it ran
        attempt("still", "failed", 5),  # graph did not advance
        attempt("won", "verified_semantically", 3),  # its own commit bumped it
    ]
    events = [event("moved", 5), event("still", 5), event("won", 4)]

    report = _wrong_direction_report(attempts, events, {})

    assert report.observable is True
    assert report.total_count == 1
    assert [item.task_id for item in report.by_task] == ["moved"]
    # The bound must be labelled as a bound rather than an exact figure.
    assert any(item.name == "avoidable_fraction" for item in report.unavailable)


def test_projection_is_deterministic_for_one_durable_state() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("waste-wrong-direction-determinism")
        goal.task("a", agent="worker", verify=lambda: _pass("a"))
        os_.run(goal, max_dispatches=1, max_steps=2)

        first = build_waste_projection(os_, goal)
        second = build_waste_projection(os_, goal)

        assert first.projection_hash == second.projection_hash
        assert first.as_dict() == second.as_dict()
    finally:
        os_.close()


def test_unstamped_journal_stays_unavailable_rather_than_reporting_zero() -> None:
    """Without any stamp the dimension must not claim "no superseded work"."""

    from lhos.sdk.waste_projection import _wrong_direction_report

    report = _wrong_direction_report([], (), {})

    assert report.observable is False
    assert report.total_count is None
    assert any(item.name == "live_graph_version" for item in report.unavailable)
