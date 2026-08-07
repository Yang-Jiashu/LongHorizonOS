"""Edge-type combination enforcement."""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    GraphPatchProposal,
)


def _p(rt, gid, kid, ops):
    return rt.submit_patch(
        GraphPatchProposal(
            graph_id=gid,
            expected_graph_version=rt.get_graph(gid).current_version,
            author_pid="p1",
            idempotency_key=kid,
            operations=ops,
        )
    )


def _setup_two_tasks(rt, gid):
    return _p(
        rt,
        gid,
        "base",
        (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1"),
            AddNodeOp(node_id="t2", graph_id=gid, node_type="task", created_by_pid="p1"),
        ),
    )


class TestInvalidCombinations:
    def test_goal_to_artifact_ref_rejected(self, graph):
        gid, rt = graph
        with pytest.raises(VPGError) as ei:
            _p(
                rt,
                gid,
                "ga-ar",
                (
                    AddNodeOp(
                        node_id="g1", graph_id=gid, node_type="goal", created_by_pid="p1", title="G"
                    ),
                    AddNodeOp(
                        node_id="ar1",
                        graph_id=gid,
                        node_type="artifact_ref",
                        created_by_pid="p1",
                        canonical_uri="u",
                        artifact_id="a",
                        version=1,
                        content_hash="h",
                    ),
                    AddEdgeOp(
                        edge_id="e1",
                        edge_type="depends_on",
                        source_node_id="g1",
                        target_node_id="ar1",
                        created_by_pid="p1",
                    ),
                ),
            )
        assert ei.value.code == VPGCode.INVALID_EDGE_TYPE_COMBINATION

    def test_goal_to_goal_rejected(self, graph):
        gid, rt = graph
        with pytest.raises(VPGError) as ei:
            _p(
                rt,
                gid,
                "gg",
                (
                    AddNodeOp(
                        node_id="g1", graph_id=gid, node_type="goal", created_by_pid="p1", title="G"
                    ),
                    AddNodeOp(
                        node_id="g2",
                        graph_id=gid,
                        node_type="goal",
                        created_by_pid="p1",
                        title="G2",
                    ),
                    AddEdgeOp(
                        edge_id="e1",
                        edge_type="depends_on",
                        source_node_id="g1",
                        target_node_id="g2",
                        created_by_pid="p1",
                    ),
                ),
            )
        assert ei.value.code == VPGCode.INVALID_EDGE_TYPE_COMBINATION

    def test_depends_on_task_to_task_ok(self, graph):
        gid, rt = graph
        _setup_two_tasks(rt, gid)
        r = _p(
            rt,
            gid,
            "dep",
            (
                AddEdgeOp(
                    edge_id="e1",
                    edge_type="depends_on",
                    source_node_id="t1",
                    target_node_id="t2",
                    created_by_pid="p1",
                ),
            ),
        )
        assert r.patch_applied

    def test_depends_on_task_to_task_cycle_rejected(self, graph):
        gid, rt = graph
        _setup_two_tasks(rt, gid)
        _p(
            rt,
            gid,
            "dep1",
            (
                AddEdgeOp(
                    edge_id="e1",
                    edge_type="depends_on",
                    source_node_id="t1",
                    target_node_id="t2",
                    created_by_pid="p1",
                ),
            ),
        )
        with pytest.raises(VPGError) as ei:
            _p(
                rt,
                gid,
                "dep2",
                (
                    AddEdgeOp(
                        edge_id="e2",
                        edge_type="depends_on",
                        source_node_id="t2",
                        target_node_id="t1",
                        created_by_pid="p1",
                    ),
                ),
            )
        assert ei.value.code == VPGCode.GRAPH_EXECUTION_CYCLE

    def test_goal_depends_on_itself_rejected(self, graph):
        gid, rt = graph
        with pytest.raises(VPGError) as ei:
            _p(
                rt,
                gid,
                "gself",
                (
                    AddNodeOp(
                        node_id="g1", graph_id=gid, node_type="goal", created_by_pid="p1", title="G"
                    ),
                    AddEdgeOp(
                        edge_id="e1",
                        edge_type="depends_on",
                        source_node_id="g1",
                        target_node_id="g1",
                        created_by_pid="p1",
                    ),
                ),
            )
        assert ei.value.code == VPGCode.GRAPH_EXECUTION_CYCLE
