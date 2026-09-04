"""Integration tests for the explicit workspace observation watcher."""

from __future__ import annotations

import threading
from unittest.mock import patch

import pytest

from lhos.integrations.tools.workspace import WorkspaceTool
from lhos.runtimes.multi_agent import InterruptDelivery, InterruptDeliveryStatus
from lhos.sdk import (
    Agent,
    AgentOS,
    ConfigurationError,
    Goal,
    SemanticInterrupt,
    SemanticInterruptKind,
    WorkspaceChangeKind,
    WorkspaceWatchPoll,
)
from lhos.sdk.verification import scripted_executor


def _goal(os_: AgentOS) -> Goal:
    os_.add_agent(Agent("worker", specializations=("python",)))
    goal = Goal("watch-goal")
    goal.task(
        "consumer",
        agent="worker",
        inputs=("workspace://input.txt",),
        verify=scripted_executor(artifact_id="artifact", version=1, content="v1"),
    )
    # Compile the graph before the watcher polls so the interrupt can carry a
    # current graph version without relying on an implicit mutation.
    goal.compile(os_)
    return goal


def test_watcher_initializes_and_emits_only_on_hash_change(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path / "workspace")
    assert workspace.write("input.txt", b"v1").ok
    os_ = AgentOS(":memory:")
    try:
        goal = _goal(os_)
        watcher = os_.workspace_watcher(
            goal,
            workspace,
            ("workspace://input.txt",),
            task_ids_by_resource={"workspace://input.txt": ("consumer",)},
        )

        first = watcher.poll()
        assert first[0].kind is WorkspaceChangeKind.INITIALIZED
        assert first[0].observation is not None
        assert watcher.baseline[0] == first[0].observation

        unchanged = watcher.poll()
        assert unchanged[0].kind is WorkspaceChangeKind.UNCHANGED
        assert unchanged[0].observation is None

        assert workspace.write("input.txt", b"v2").ok
        changed = watcher.poll()
        assert changed[0].kind is WorkspaceChangeKind.CHANGED
        assert changed[0].previous_observation == first[0].observation
        assert changed[0].observation is not None
        assert changed[0].observation.version > first[0].observation.version
        assert changed[0].observation.content_hash != first[0].observation.content_hash

        repeat = watcher.poll()
        assert repeat[0].kind is WorkspaceChangeKind.UNCHANGED
    finally:
        os_.close()


def test_watcher_change_becomes_graph_bound_interrupt_and_policy_epoch(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path / "workspace")
    assert workspace.write("input.txt", b"v1").ok
    os_ = AgentOS(":memory:")
    try:
        goal = _goal(os_)
        watcher = os_.workspace_watcher(
            goal,
            workspace,
            ("input.txt",),
            task_ids_by_resource={"input.txt": ("consumer",)},
        )
        watcher.initialize()
        assert workspace.write("input.txt", b"v2").ok

        poll = watcher.poll_and_plan(epoch_id=4)
        assert len(poll.changes) == 1
        assert poll.changes[0].kind is WorkspaceChangeKind.CHANGED
        assert len(poll.interrupts) == 1
        interrupt = poll.interrupts[0]
        assert interrupt.kind is SemanticInterruptKind.ARTIFACT_CHANGED
        assert interrupt.graph_id == poll.graph_id
        assert interrupt.affected_task_ids == ("consumer",)
        assert interrupt.metadata["previous_version"] == 1
        assert interrupt.metadata["new_version"] == 2
        assert poll.epoch is not None
        # There is no active Attempt in this fixture, so the policy emits a
        # task-level DEFER proposal rather than pretending it can preempt code.
        assert poll.epoch.unhandled_interrupt_ids == ()
        assert poll.epoch.decisions[0].target_id == "consumer"
    finally:
        os_.close()


def test_watcher_derives_affected_tasks_from_goal_declarations(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path / "workspace")
    assert workspace.write("input.txt", b"v1").ok
    os_ = AgentOS(":memory:")
    try:
        goal = _goal(os_)
        watcher = os_.workspace_watcher(goal, workspace, ("input.txt",))
        watcher.initialize()
        assert workspace.write("input.txt", b"v2").ok

        change = watcher.poll()[0]
        interrupt = watcher.interrupt_for(change)
        assert interrupt is not None
        assert interrupt.affected_task_ids == ("consumer",)
    finally:
        os_.close()


def test_watcher_deletion_is_reported_without_synthetic_observation(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path / "workspace")
    assert workspace.write("input.txt", b"v1").ok
    os_ = AgentOS(":memory:")
    try:
        goal = _goal(os_)
        watcher = os_.workspace_watcher(
            goal,
            workspace,
            ("input.txt",),
            task_ids_by_resource={"input.txt": ("consumer",)},
        )
        watcher.initialize()
        workspace.resolve("input.txt").unlink()

        deleted = watcher.poll()
        assert deleted[0].kind is WorkspaceChangeKind.DELETED
        assert deleted[0].previous_observation is not None
        assert deleted[0].observation is None
        interrupt = watcher.interrupt_for(deleted[0])
        assert interrupt is not None
        assert interrupt.kind is SemanticInterruptKind.ARTIFACT_CHANGED
        assert interrupt.metadata["deleted"] is True

        # A second poll does not emit the same deletion transition forever.
        still_deleted = watcher.poll()
        assert still_deleted[0].kind is WorkspaceChangeKind.UNCHANGED
    finally:
        os_.close()


def test_watcher_initial_missing_file_is_not_treated_as_a_change(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path / "workspace")
    os_ = AgentOS(":memory:")
    try:
        goal = _goal(os_)
        watcher = os_.workspace_watcher(goal, workspace, ("missing.txt",))
        first = watcher.poll()
        assert first[0].kind is WorkspaceChangeKind.DELETED
        assert watcher.interrupt_for(first[0]) is None
    finally:
        os_.close()


def test_watcher_poll_and_reconcile_marks_declared_cone_stale_only(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path / "workspace")
    assert workspace.write("input.txt", b"v1").ok
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("reconcile-goal")
        consumer = goal.task(
            "consumer",
            agent="worker",
            inputs=("workspace://input.txt",),
            verify=scripted_executor(artifact_id="consumer.out", version=1, content="v1"),
        )
        dependent = goal.task(
            "dependent",
            agent="worker",
            depends_on=(consumer,),
            verify=scripted_executor(artifact_id="dependent.out", version=1, content="v1"),
        )
        goal.task(
            "independent",
            agent="worker",
            verify=scripted_executor(artifact_id="independent.out", version=1, content="v1"),
        )
        result = os_.run(goal, max_dispatches=8)
        assert result.goal_state == "closed"

        watcher = os_.workspace_watcher(goal, workspace, ("input.txt",))
        watcher.initialize()
        assert workspace.write("input.txt", b"v2").ok

        poll = watcher.poll_and_reconcile(epoch_id=2)
        assert len(poll.repair_outcomes) == 1
        repaired = poll.repair_outcomes[0]
        assert repaired["affected"] == ["consumer", "dependent"]
        assert repaired["repair_frontier"] == ["consumer"]
        assert repaired["preserved"] == ["independent"]
        status = os_.result(os_._gid_for(goal.goal_id))
        assert status.task_states["consumer"] == "stale"
        assert status.task_states["dependent"] == "stale"
        assert status.task_states["independent"] == "verified"
        assert status.goal_state == "open"
    finally:
        os_.close()


def test_reconcile_rejects_unlisted_task_mapping(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path / "workspace")
    assert workspace.write("input.txt", b"v1").ok
    os_ = AgentOS(":memory:")
    try:
        goal = _goal(os_)
        watcher = os_.workspace_watcher(goal, workspace, ("input.txt",))
        initial = watcher.initialize()[0].observation
        assert initial is not None
        assert workspace.write("input.txt", b"v2").ok
        changed = watcher.poll()[0]
        assert changed.observation is not None
        with pytest.raises(Exception, match="declared consumers"):
            os_.reconcile_observation(
                goal,
                observation=changed.observation,
                previous_observation=initial,
                affected_task_ids=("not-a-consumer",),
                resource_uri="workspace://input.txt",
            )
    finally:
        os_.close()


def test_reconcile_same_transition_is_idempotent_under_concurrency(tmp_path) -> None:
    """Two callers racing the same observation must publish one D3 commit."""

    workspace = WorkspaceTool(tmp_path / "workspace")
    assert workspace.write("input.txt", b"v1").ok
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("concurrent-reconcile-goal")
        consumer = goal.task(
            "consumer",
            agent="worker",
            inputs=("workspace://input.txt",),
            verify=scripted_executor(artifact_id="consumer.out", version=1, content="v1"),
        )
        goal.task(
            "dependent",
            agent="worker",
            depends_on=(consumer,),
            verify=scripted_executor(artifact_id="dependent.out", version=1, content="v1"),
        )
        assert os_.run(goal, max_dispatches=8).goal_state == "closed"
        before_version = os_._vpg.get_graph(os_._gid_for(goal.goal_id)).current_version

        watcher = os_.workspace_watcher(goal, workspace, ("input.txt",))
        initial = watcher.initialize()[0].observation
        assert initial is not None
        assert workspace.write("input.txt", b"v2").ok
        changed = watcher.poll()[0]
        current = changed.observation
        assert current is not None

        # Force both callers to enter repair together.  The first commit wins;
        # the second must converge through the durable token/idempotency path.
        barrier = threading.Barrier(2)
        original_repair = os_.repair

        def synchronized_repair(*args, **kwargs):
            barrier.wait(timeout=10)
            return original_repair(*args, **kwargs)

        outcomes: list[dict] = []
        errors: list[BaseException] = []

        def reconcile_once() -> None:
            try:
                outcomes.append(
                    os_.reconcile_observation(
                        goal,
                        observation=current,
                        previous_observation=initial,
                        affected_task_ids=("consumer",),
                        resource_uri="workspace://input.txt",
                    ).as_dict()
                )
            except BaseException as exc:  # pragma: no cover - assertion below
                errors.append(exc)

        with patch.object(os_, "repair", side_effect=synchronized_repair):
            threads = [threading.Thread(target=reconcile_once) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=20)

        assert not errors
        assert len(outcomes) == 2
        assert outcomes[0] == outcomes[1]
        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        assert os_._vpg.get_graph(gid).current_version == before_version + 1
        assert len(os_._vpg.get_d3_results(gid)) == 1
    finally:
        os_.close()


def test_watcher_retries_failed_reconcile_without_losing_change(tmp_path) -> None:
    """A transient reconcile error must not advance the watcher baseline."""

    workspace = WorkspaceTool(tmp_path / "workspace")
    assert workspace.write("input.txt", b"v1").ok
    os_ = AgentOS(":memory:")
    try:
        goal = _goal(os_)
        assert os_.run(goal, max_dispatches=4).goal_state == "closed"
        watcher = os_.workspace_watcher(goal, workspace, ("input.txt",))
        initial = watcher.initialize()[0].observation
        assert initial is not None
        assert workspace.write("input.txt", b"v2").ok

        with (
            patch.object(
                os_,
                "reconcile_observations",
                side_effect=RuntimeError("transient reconcile failure"),
            ),
            pytest.raises(RuntimeError, match="transient reconcile failure"),
        ):
            watcher.poll_and_reconcile()

        assert watcher.baseline[0] == initial
        assert len(watcher.pending_changes) == 1

        retried = watcher.poll_and_reconcile()
        assert len(retried.repair_outcomes) == 1
        assert retried.repair_outcomes[0]["affected"] == ["consumer"]
        assert watcher.pending_changes == ()
        assert watcher.baseline[0].version == 2
    finally:
        os_.close()


def test_watcher_reopen_reuses_unchanged_durable_artifact_version(tmp_path) -> None:
    db = tmp_path / "state.sqlite"
    workspace = WorkspaceTool(tmp_path / "workspace")
    assert workspace.write("input.txt", b"v1").ok

    writer = AgentOS(str(db))
    try:
        first_goal = _goal(writer)
        first_watcher = writer.workspace_watcher(
            first_goal,
            workspace,
            ("input.txt",),
        )
        first = first_watcher.initialize()[0]
        assert first.kind is WorkspaceChangeKind.INITIALIZED
        assert first.observation is not None
        assert first.observation.version == 1
        assert writer._facts.versions()["input.txt"] == [1]
    finally:
        writer.close()

    reopened = AgentOS(str(db))
    try:
        reopened_goal = _goal(reopened)
        reopened_watcher = reopened.workspace_watcher(
            reopened_goal,
            workspace,
            ("input.txt",),
        )
        baseline = reopened_watcher.initialize()[0]
        assert baseline.kind is WorkspaceChangeKind.INITIALIZED
        assert baseline.observation is not None
        assert baseline.observation.version == 1
        assert baseline.observation.content_hash == first.observation.content_hash
        # Recompiling after reopen creates a new graph binding, but unchanged
        # bytes must not turn into a synthetic ArtifactVersion.
        assert reopened._facts.versions()["input.txt"] == [1]

        assert workspace.write("input.txt", b"v2").ok
        changed = reopened_watcher.poll()[0]
        assert changed.kind is WorkspaceChangeKind.CHANGED
        assert changed.observation is not None
        assert changed.observation.version == 2
        assert reopened._facts.versions()["input.txt"] == [1, 2]
    finally:
        reopened.close()


def _multi_resource_goal(os_: AgentOS) -> Goal:
    """Build a small graph whose one seed task consumes two workspace files."""

    os_.add_agent(Agent("worker", specializations=("python",)))
    goal = Goal("multi-resource-goal")
    consumer = goal.task(
        "consumer",
        agent="worker",
        inputs=("workspace://a.txt", "workspace://b.txt"),
        verify=scripted_executor(artifact_id="consumer.out", version=1, content="v1"),
    )
    goal.task(
        "dependent",
        agent="worker",
        depends_on=(consumer,),
        verify=scripted_executor(artifact_id="dependent.out", version=1, content="v1"),
    )
    goal.task(
        "independent",
        agent="worker",
        verify=scripted_executor(artifact_id="independent.out", version=1, content="v1"),
    )
    assert os_.run(goal, max_dispatches=12).goal_state == "closed"
    return goal


def _multi_resource_watcher(tmp_path, os_: AgentOS, goal: Goal):
    workspace = WorkspaceTool(tmp_path / "workspace")
    assert workspace.write("a.txt", b"a1").ok
    assert workspace.write("b.txt", b"b1").ok
    watcher = os_.workspace_watcher(goal, workspace, ("a.txt", "b.txt"))
    watcher.initialize()
    return workspace, watcher


def test_watcher_batches_multiple_resource_changes_into_one_d3_commit(tmp_path) -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _multi_resource_goal(os_)
        workspace, watcher = _multi_resource_watcher(tmp_path, os_, goal)
        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        before_version = os_._vpg.get_graph(gid).current_version
        assert workspace.write("a.txt", b"a2").ok
        assert workspace.write("b.txt", b"b2").ok

        poll = watcher.poll_and_reconcile(epoch_id=7)

        assert len(poll.changes) == 2
        assert all(change.kind is WorkspaceChangeKind.CHANGED for change in poll.changes)
        assert len(poll.repair_outcomes) == 1
        repaired = poll.repair_outcomes[0]
        assert repaired["affected"] == ["consumer", "dependent"]
        assert repaired["preserved"] == ["independent"]
        assert {cause["artifact_id"] for cause in repaired["cause_details"]} == {
            "a.txt",
            "b.txt",
        }
        assert os_._vpg.get_graph(gid).current_version == before_version + 1
        assert len(os_._vpg.get_d3_results(gid)) == 1
        status = os_.result(gid)
        assert status.task_states["consumer"] == "stale"
        assert status.task_states["dependent"] == "stale"
        assert status.task_states["independent"] == "verified"
    finally:
        os_.close()


def test_batch_reconcile_validates_all_transitions_before_writing(tmp_path) -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _multi_resource_goal(os_)
        workspace, watcher = _multi_resource_watcher(tmp_path, os_, goal)
        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        before_version = os_._vpg.get_graph(gid).current_version
        before_d3 = len(os_._vpg.get_d3_results(gid))
        assert workspace.write("a.txt", b"a2").ok
        assert workspace.write("b.txt", b"b2").ok
        changes = watcher.poll()
        assert len(changes) == 2
        first, second = changes
        assert first.observation is not None and first.previous_observation is not None
        assert second.observation is not None and second.previous_observation is not None

        # The first transition is valid; the second has an undeclared seed.
        # Validation must fail before the first transition can mutate VPG.
        with pytest.raises(Exception, match="declared consumers"):
            os_.reconcile_observations(
                goal,
                (
                    (
                        first.previous_observation,
                        first.observation,
                        ("consumer",),
                        first.resource_uri,
                    ),
                    (
                        second.previous_observation,
                        second.observation,
                        ("not-a-consumer",),
                        second.resource_uri,
                    ),
                ),
            )

        assert os_._vpg.get_graph(gid).current_version == before_version
        assert len(os_._vpg.get_d3_results(gid)) == before_d3
        status = os_.result(gid)
        assert status.task_states["consumer"] == "verified"
        assert status.task_states["dependent"] == "verified"
    finally:
        os_.close()


def test_batch_reconcile_retry_after_transaction_failure_is_atomic(tmp_path) -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _multi_resource_goal(os_)
        workspace, watcher = _multi_resource_watcher(tmp_path, os_, goal)
        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        assert workspace.write("a.txt", b"a2").ok
        assert workspace.write("b.txt", b"b2").ok

        original_refresh = os_._vpg.refresh_derived_state

        def fail_once(*args, **kwargs):
            os_._vpg.refresh_derived_state = original_refresh
            raise RuntimeError("transient batch failure")

        with (
            patch.object(os_._vpg, "refresh_derived_state", side_effect=fail_once),
            pytest.raises(RuntimeError, match="transient batch failure"),
        ):
            watcher.poll_and_reconcile()

        assert len(watcher.pending_changes) == 2
        assert watcher.baseline[0].version == 1
        assert watcher.baseline[1].version == 1
        assert os_._vpg.get_graph(gid).current_version == 4
        assert len(os_._vpg.get_d3_results(gid)) == 0

        retried = watcher.poll_and_reconcile()
        assert len(retried.repair_outcomes) == 1
        assert watcher.pending_changes == ()
        assert watcher.baseline[0].version == 2
        assert watcher.baseline[1].version == 2
        assert os_._vpg.get_graph(gid).current_version == 5
        assert len(os_._vpg.get_d3_results(gid)) == 1
    finally:
        os_.close()


def test_batch_reconcile_is_idempotent_for_same_transition_set(tmp_path) -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _multi_resource_goal(os_)
        workspace, watcher = _multi_resource_watcher(tmp_path, os_, goal)
        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        assert workspace.write("a.txt", b"a2").ok
        assert workspace.write("b.txt", b"b2").ok
        changes = watcher.poll()
        transitions = tuple(
            (
                change.previous_observation,
                change.observation,
                ("consumer",),
                change.resource_uri,
            )
            for change in changes
            if change.previous_observation is not None and change.observation is not None
        )
        first = os_.reconcile_observations(goal, transitions)
        after_first = os_._vpg.get_graph(gid).current_version
        second = os_.reconcile_observations(goal, transitions)

        assert second.as_dict() == first.as_dict()
        assert os_._vpg.get_graph(gid).current_version == after_first
        assert len(os_._vpg.get_d3_results(gid)) == 1
    finally:
        os_.close()


def test_batch_reconcile_uri_aliases_share_idempotency_identity(tmp_path) -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _multi_resource_goal(os_)
        workspace, watcher = _multi_resource_watcher(tmp_path, os_, goal)
        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        assert workspace.write("a.txt", b"a2").ok
        assert workspace.write("b.txt", b"b2").ok
        changes = watcher.poll()
        direct = tuple(
            (
                change.previous_observation,
                change.observation,
                ("consumer",),
                change.resource_uri,
            )
            for change in changes
            if change.previous_observation is not None and change.observation is not None
        )
        aliased = tuple(
            (
                change.previous_observation,
                change.observation,
                ("consumer",),
                f"vpg://workspace/{change.artifact_id}",
            )
            for change in reversed(changes)
            if change.previous_observation is not None and change.observation is not None
        )

        first = os_.reconcile_observations(goal, direct)
        after_first = os_._vpg.get_graph(gid).current_version
        second = os_.reconcile_observations(goal, aliased)

        assert second.as_dict() == first.as_dict()
        assert os_._vpg.get_graph(gid).current_version == after_first
        d3 = os_._vpg.get_d3_results(gid)
        assert len(d3) == 1
        assert {item["canonical_resource_uri"] for item in d3[0]["reconciliation_transitions"]} == {
            "vpg://a.txt",
            "vpg://b.txt",
        }
    finally:
        os_.close()


def test_batch_failure_does_not_rollback_unassigned_interrupt_only_resource(tmp_path) -> None:
    """Only resources submitted to the semantic batch become retry-pending."""

    os_ = AgentOS(":memory:")
    try:
        goal = _multi_resource_goal(os_)
        workspace = WorkspaceTool(tmp_path / "workspace")
        assert workspace.write("a.txt", b"a1").ok
        assert workspace.write("b.txt", b"b1").ok
        assert workspace.write("unassigned.txt", b"u1").ok
        watcher = os_.workspace_watcher(
            goal,
            workspace,
            ("a.txt", "unassigned.txt"),
            task_ids_by_resource={"a.txt": ("consumer",)},
        )
        watcher.initialize()
        assert workspace.write("a.txt", b"a2").ok
        assert workspace.write("unassigned.txt", b"u2").ok

        with (
            patch.object(
                os_,
                "reconcile_observations",
                side_effect=RuntimeError("transient batch failure"),
            ),
            pytest.raises(RuntimeError, match="transient batch failure"),
        ):
            watcher.poll_and_reconcile()

        # The assigned transition is retried; the unassigned resource is an
        # interrupt-only observation and must not be rolled back or made
        # permanently pending by a failed batch.
        assert tuple(change.artifact_id for change in watcher.pending_changes) == ("a.txt",)
        assert watcher.baseline[0].version == 1
        assert watcher.baseline[1].version == 2

        # A later poll sees only the assigned transition and can reconcile it.
        retried = watcher.poll_and_reconcile()
        assert len(retried.repair_outcomes) == 1
        assert watcher.pending_changes == ()
        assert watcher.baseline[0].version == 2
        assert watcher.baseline[1].version == 2
    finally:
        os_.close()


def test_batch_reconcile_concurrent_identical_calls_publish_one_commit(tmp_path) -> None:
    """Racing callers of one batch converge through durable idempotency."""

    os_ = AgentOS(":memory:")
    try:
        goal = _multi_resource_goal(os_)
        workspace, watcher = _multi_resource_watcher(tmp_path, os_, goal)
        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        before_version = os_._vpg.get_graph(gid).current_version
        assert workspace.write("a.txt", b"a2").ok
        assert workspace.write("b.txt", b"b2").ok
        changes = watcher.poll()
        transitions = tuple(
            (
                change.previous_observation,
                change.observation,
                ("consumer",),
                change.resource_uri,
            )
            for change in changes
            if change.previous_observation is not None and change.observation is not None
        )
        assert len(transitions) == 2

        barrier = threading.Barrier(2)
        original_refresh = os_._vpg.refresh_derived_state

        def synchronized_refresh(*args, **kwargs):
            barrier.wait(timeout=10)
            return original_refresh(*args, **kwargs)

        outcomes: list[dict] = []
        errors: list[BaseException] = []

        def reconcile_once() -> None:
            try:
                outcomes.append(os_.reconcile_observations(goal, transitions).as_dict())
            except BaseException as exc:  # pragma: no cover - assertion below
                errors.append(exc)

        with patch.object(
            os_._vpg,
            "refresh_derived_state",
            side_effect=synchronized_refresh,
        ):
            threads = [threading.Thread(target=reconcile_once) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=20)

        assert not errors
        assert len(outcomes) == 2
        assert outcomes[0] == outcomes[1]
        assert os_._vpg.get_graph(gid).current_version == before_version + 1
        assert len(os_._vpg.get_d3_results(gid)) == 1
    finally:
        os_.close()


def test_watcher_post_commit_retry_reuses_observation_and_d3(tmp_path) -> None:
    """A lost response after commit must not publish a second repair."""

    workspace = WorkspaceTool(tmp_path / "workspace")
    assert workspace.write("input.txt", b"v1").ok
    os_ = AgentOS(":memory:")
    try:
        goal = _goal(os_)
        assert os_.run(goal, max_dispatches=5).goal_state == "closed"
        watcher = os_.workspace_watcher(goal, workspace, ("input.txt",))
        watcher.initialize()
        assert workspace.write("input.txt", b"v2").ok

        original = os_.reconcile_observations

        def commit_then_lose_response(*args, **kwargs):
            result = original(*args, **kwargs)
            raise RuntimeError("response lost after durable commit")

        with (
            patch.object(
                os_,
                "reconcile_observations",
                side_effect=commit_then_lose_response,
            ),
            pytest.raises(RuntimeError, match="response lost"),
        ):
            watcher.poll_and_reconcile()

        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        committed_version = os_._vpg.get_graph(gid).current_version
        committed_d3 = len(os_._vpg.get_d3_results(gid))
        assert committed_d3 == 1
        assert len(watcher.pending_changes) == 1

        retried = watcher.poll_and_reconcile()
        assert len(retried.repair_outcomes) == 1
        assert watcher.pending_changes == ()
        assert os_._vpg.get_graph(gid).current_version == committed_version
        assert len(os_._vpg.get_d3_results(gid)) == committed_d3
    finally:
        os_.close()


def test_agent_os_workspace_route_facades_delegate_one_shot_and_reject_invalid_watcher(
    tmp_path,
) -> None:
    """The composition root exposes the bounded watcher control seam."""

    workspace = WorkspaceTool(tmp_path / "workspace")
    assert workspace.write("input.txt", b"v1").ok
    os_ = AgentOS(":memory:")
    try:
        goal = _goal(os_)
        watcher = os_.workspace_watcher(
            goal,
            workspace,
            ("input.txt",),
            task_ids_by_resource={"input.txt": ("consumer",)},
        )

        initial = os_.poll_workspace_and_route(watcher, epoch_id=10)
        assert initial.poll.changes[0].kind is WorkspaceChangeKind.INITIALIZED
        assert initial.epoch.epoch_id == 10

        assert workspace.write("input.txt", b"v2").ok
        routed = os_.poll_workspace_and_route(watcher, epoch_id=11)
        assert routed.poll.changes[0].kind is WorkspaceChangeKind.CHANGED
        assert routed.poll.epoch == routed.epoch
        assert routed.epoch.epoch_id == 11
        # No live async Attempt exists in this fixture, so policy remains
        # task-level and the route records a bounded non-delivery.
        assert routed.deliveries == ()
        assert any(item.get("reason") == "policy_action_not_deliverable" for item in routed.blocked)

        replay = os_.route_workspace_observation(
            watcher,
            routed.poll,
            epoch_id=12,
        )
        assert replay.poll.epoch == replay.epoch
        assert replay.epoch.epoch_id == 12
    finally:
        os_.close()


def test_agent_os_workspace_route_facade_requires_watcher_protocol() -> None:
    os_ = AgentOS(":memory:")
    try:
        with pytest.raises(ConfigurationError, match="WorkspaceObservationWatcher"):
            os_.poll_workspace_and_route(object())
        with pytest.raises(ConfigurationError, match="WorkspaceObservationWatcher"):
            os_.route_workspace_observation(object(), ())
    finally:
        os_.close()


def test_route_reconcile_failure_restores_baseline_and_retries_transition(tmp_path) -> None:
    """A failed route reconciliation must not turn a changed input into UNCHANGED."""

    workspace = WorkspaceTool(tmp_path / "workspace")
    assert workspace.write("input.txt", b"v1").ok
    os_ = AgentOS(":memory:")
    try:
        goal = _goal(os_)
        assert os_.run(goal, max_dispatches=4).goal_state == "closed"
        watcher = os_.workspace_watcher(
            goal,
            workspace,
            ("input.txt",),
            task_ids_by_resource={"input.txt": ("consumer",)},
        )
        initial = watcher.initialize()[0].observation
        assert initial is not None
        assert workspace.write("input.txt", b"v2").ok

        original = os_.reconcile_observations
        with patch.object(
            os_,
            "reconcile_observations",
            side_effect=RuntimeError("route reconcile failed"),
        ):
            failed = os_.poll_workspace_and_route(
                watcher,
                epoch_id=20,
                reconcile_after_delivery=True,
            )

        assert failed.reconcile_error is not None
        assert "route reconcile failed" in failed.reconcile_error
        assert watcher.baseline[0] == initial
        assert tuple(change.artifact_id for change in watcher.pending_changes) == ("input.txt",)

        # Restore the authority and retry.  Because the baseline was rolled
        # back, the same bytes are observed as CHANGED rather than silently
        # disappearing as UNCHANGED.
        with patch.object(os_, "reconcile_observations", side_effect=original):
            retried = os_.poll_workspace_and_route(
                watcher,
                epoch_id=21,
                reconcile_after_delivery=True,
            )
        assert retried.reconcile_error is None
        assert len(retried.reconcile_outcomes) == 1
        assert watcher.pending_changes == ()
        assert watcher.baseline[0].version == 2
    finally:
        os_.close()


@pytest.mark.parametrize(
    "status",
    (
        InterruptDeliveryStatus.STALE_GRAPH,
        InterruptDeliveryStatus.STALE_EPOCH,
        InterruptDeliveryStatus.IDENTITY_MISMATCH,
        InterruptDeliveryStatus.NOT_RUNNING,
    ),
)
def test_route_consumes_supplied_poll_interrupt_and_blocks_rejected_delivery(
    tmp_path,
    status: InterruptDeliveryStatus,
) -> None:
    """Poll-carried runtime events are routed, but refused delivery is not success."""

    workspace = WorkspaceTool(tmp_path / "workspace")
    assert workspace.write("input.txt", b"v1").ok
    os_ = AgentOS(":memory:")
    try:
        goal = _goal(os_)
        watcher = os_.workspace_watcher(goal, workspace, ("input.txt",))
        initial_changes = watcher.initialize()
        scheduled = os_.schedule_online_epoch(
            goal,
            plan_only=False,
            keep_claims=True,
        )
        dispatch = scheduled.dispatches[0]
        interrupt = SemanticInterrupt(
            interrupt_id=f"write-conflict-{status.value}",
            graph_id=dispatch.graph_id,
            graph_version=dispatch.graph_version,
            kind=SemanticInterruptKind.WRITE_CONFLICT,
            reason="explicit write conflict",
            affected_attempt_ids=(dispatch.attempt_id,),
            resource_keys=("workspace://input.txt",),
        )
        poll = WorkspaceWatchPoll(
            goal_id=goal.goal_id,
            graph_id=dispatch.graph_id,
            changes=initial_changes,
            interrupts=(interrupt,),
        )
        refused = InterruptDelivery(
            claim_id=dispatch.claim_id,
            status=status,
            graph_id=dispatch.graph_id,
            graph_version=dispatch.graph_version,
            task_id=dispatch.task_id,
            attempt_id=dispatch.attempt_id,
            semantic_epoch=dispatch.semantic_epoch,
            action="preempt",
            interrupt_id=interrupt.interrupt_id,
            preemptible=True,
            delivered=False,
            observed=False,
        )

        with patch.object(os_, "deliver_interrupt", return_value=refused) as deliver:
            routed = os_.route_workspace_observation(
                watcher,
                poll,
                epoch_id=31,
            )

        assert routed.epoch.interrupt_ids == (interrupt.interrupt_id,)
        assert routed.poll.interrupts == (interrupt,)
        assert routed.epoch.decisions[0].action.value == "preempt"
        assert routed.deliveries == ()
        rejected = [item for item in routed.blocked if item.get("reason") == "delivery_rejected"]
        assert len(rejected) == 1
        assert rejected[0]["delivery_status"] == status.value
        assert rejected[0]["attempt_id"] == dispatch.attempt_id
        deliver.assert_called_once()
    finally:
        os_.close()
