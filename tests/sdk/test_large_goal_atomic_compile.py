"""SDK-level regression for atomic publication of large Goals (P0-6)."""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress.patch_validator import MAX_PATCH_OPS
from lhos.sdk import Agent, AgentOS, Goal


def test_large_goal_compile_publishes_one_graph_version_with_complete_projection() -> None:
    """A Goal whose compilation exceeds MAX_PATCH_OPS must not be split into
    visible intermediate graph versions.

    Each independent task contributes two operations (task node + Goal→Task
    dependency edge).  This deliberately creates 503 operations, just above
    the ordinary 500-operation patch ceiling, while keeping the test small
    enough for the regular SDK suite.
    """

    task_count = MAX_PATCH_OPS // 2 + 1
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("compiler", specializations=("python",)))
        goal = Goal("large-goal")
        for index in range(task_count):
            goal.task(f"task-{index}", agent="compiler")

        graph_id = os_._compile_goal(goal)
        nodes, edges = os_.vpg.snapshot_projection(graph_id)

        # v0 is the immutable empty graph snapshot; compilation itself should
        # be exactly one atomic semantic publication at v1.
        assert os_.vpg.get_graph(graph_id).current_version == 1
        assert (
            os_.vpg.store.conn.execute(
                "SELECT COUNT(*) AS n FROM graph_patches WHERE graph_id = ?",
                (graph_id,),
            ).fetchone()["n"]
            == 1
        )
        assert (
            os_.vpg.store.conn.execute(
                "SELECT COUNT(*) AS n FROM graph_versions WHERE graph_id = ?",
                (graph_id,),
            ).fetchone()["n"]
            == 2
        )

        assert len(nodes) == task_count + 1
        assert "large-goal" in nodes
        assert {f"task-{index}" for index in range(task_count)} <= set(nodes)

        goal_edges = [
            edge
            for edge in edges
            if edge.edge_type.value == "depends_on" and edge.source_node_id == "large-goal"
        ]
        assert len(edges) == task_count
        assert {edge.target_node_id for edge in goal_edges} == {
            f"task-{index}" for index in range(task_count)
        }
    finally:
        os_.close()


@pytest.mark.slow
def test_xl_goal_far_beyond_ceiling_builds_with_correct_ready_frontier() -> None:
    """A Goal an order of magnitude past the old ceiling still compiles as one
    atomic graph version and yields the exact deterministic READY frontier.

    The public compile path used to reject anything past ~165 tasks because a
    Goal emits a single patch bounded by ``MAX_PATCH_OPS`` (500).  The trusted
    composition root now publishes the whole compiled Goal as one atomic patch,
    and the derived-state pass is linear in graph size, so a graph well beyond
    1000 tasks builds in one version.

    Layout gives a frontier that is neither empty nor everything, so the check
    is meaningful: ``root-k`` has no dependency (must be READY); ``leaf-k``
    depends on ``root-k`` and — since nothing is VERIFIED in a fresh build —
    must NOT be READY.  600 roots + 600 leaves compiles to ~3000 operations,
    six times the ordinary ceiling.
    """

    root_count = 600
    total_tasks = root_count * 2
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("compiler", specializations=("python",)))
        goal = Goal("xl-goal")
        roots = [goal.task(f"root-{index}", agent="compiler") for index in range(root_count)]
        for index, root in enumerate(roots):
            goal.task(f"leaf-{index}", agent="compiler", depends_on=(root,))

        graph_id = goal.compile(os_)

        # The whole build is a single atomic semantic publication at v1 — a
        # half-built Goal must never be observable to readiness/scheduling.
        assert os_.vpg.get_graph(graph_id).current_version == 1
        assert (
            os_.vpg.store.conn.execute(
                "SELECT COUNT(*) AS n FROM graph_patches WHERE graph_id = ?",
                (graph_id,),
            ).fetchone()["n"]
            == 1
        )

        nodes, _edges = os_.vpg.snapshot_projection(graph_id)
        assert len(nodes) == total_tasks + 1  # every task plus the Goal node

        frontier = os_.vpg.query_ready_frontier(graph_id)
        ready_ids = {candidate.task_id for candidate in frontier}
        # Exactly the dependency-free roots are READY; every leaf is gated by an
        # unverified root.
        assert ready_ids == {f"root-{index}" for index in range(root_count)}
    finally:
        os_.close()
