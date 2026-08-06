"""DeterministicSingleProcessCoordinator: head selection + attempt observation."""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.coordinator import (
    DeterministicSingleProcessCoordinator,
    ExecutionAttemptRef,
)
from lhos.runtimes.verified_progress.events import GraphEventType
from lhos.runtimes.verified_progress.patches import AddNodeOp, GraphPatchProposal


def _patch(rt, gid, kid, ops):
    return rt.submit_patch(GraphPatchProposal(
        graph_id=gid, expected_graph_version=rt.get_graph(gid).current_version,
        author_pid="p1", idempotency_key=kid, operations=ops))


class TestCoordinatorSelection:
    def test_select_next_candidate_returns_head(self, graph):
        gid, rt = graph
        _patch(rt, gid, "setup", (
            AddNodeOp(node_id="zzz", graph_id=gid, node_type="task", created_by_pid="p1", metadata={"priority": 1}),
            AddNodeOp(node_id="aaa", graph_id=gid, node_type="task", created_by_pid="p1", metadata={"priority": 1}),
        ))
        coord = DeterministicSingleProcessCoordinator(owner_pid="p1")
        n, ed = rt.snapshot_projection(gid)
        cand = coord.select_next_candidate(
            graph_id=gid, graph_version=rt.get_graph(gid).current_version, nodes=n, edges=ed,
        )
        assert cand is not None
        # head of deterministic frontier (node_id asc tiebreak)
        assert cand.task_id == "aaa"

    def test_select_next_candidate_empty_none(self, graph):
        gid, rt = graph
        coord = DeterministicSingleProcessCoordinator(owner_pid="p1")
        cand = coord.select_next_candidate(
            graph_id=gid, graph_version=rt.get_graph(gid).current_version, nodes={}, edges=[],
        )
        assert cand is None

    def test_observe_attempt_emits_event(self, graph):
        gid, rt = graph
        coord = DeterministicSingleProcessCoordinator(owner_pid="p1")
        attempt = ExecutionAttemptRef(
            attempt_id="att-1", task_id="t1", process_id="p1",
            action_id="act-1", observed_state="committed",
        )
        ev = coord.observe_attempt(
            graph_id=gid, graph_version=rt.get_graph(gid).current_version, attempt=attempt,
        )
        assert ev.event_type == GraphEventType.EXECUTION_ATTEMPT_OBSERVED
        assert ev.node_id == "t1"
        assert ev.payload["process_id"] == "p1"
        assert ev.payload["observed_state"] == "committed"
