"""DAG cycle detection: self-loops, 2-node, 3-node, and mixed cycles."""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.dag import detect_cycle, is_self_loop
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.models import EdgeType, VPGEdge
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    GraphPatchProposal,
)


def _p(rt, gid, kid, ops):
    return rt.submit_patch(GraphPatchProposal(
        graph_id=gid,
        expected_graph_version=rt.get_graph(gid).current_version,
        author_pid="p1",
        idempotency_key=kid,
        operations=ops,
    ))


def _edge(eid, src, tgt):
    return VPGEdge(
        edge_id=eid, graph_id="G", edge_type=EdgeType.DEPENDS_ON,
        source_node_id=src, target_node_id=tgt,
        created_in_version=0, created_by_pid="p1",
    )


class TestSelfLoop:
    def test_self_loop_rejected_at_submit(self, graph):
        gid, rt = graph
        with pytest.raises(VPGError) as ei:
            _p(rt, gid, "self", (
                AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1"),
                AddEdgeOp(edge_id="e1", edge_type="depends_on",
                          source_node_id="t1", target_node_id="t1", created_by_pid="p1"),
            ))
        assert ei.value.code == VPGCode.GRAPH_EXECUTION_CYCLE

    def test_is_self_loop_helper(self):
        assert is_self_loop(_edge("e", "a", "a")) is True
        assert is_self_loop(_edge("e", "a", "b")) is False


class TestCycleDetection:
    def test_no_cycle_empty(self):
        assert detect_cycle([], []) is None

    def test_no_cycle_linear(self):
        existing = [_edge("e1", "a", "b"), _edge("e2", "b", "c")]
        assert detect_cycle(existing, []) is None

    def test_two_node_cycle_via_two_patches(self, graph):
        gid, rt = graph
        _p(rt, gid, "n", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1"),
            AddNodeOp(node_id="t2", graph_id=gid, node_type="task", created_by_pid="p1"),
        ))
        _p(rt, gid, "e1", (
            AddEdgeOp(edge_id="e1", edge_type="depends_on",
                      source_node_id="t1", target_node_id="t2", created_by_pid="p1"),
        ))
        with pytest.raises(VPGError) as ei:
            _p(rt, gid, "e2", (
                AddEdgeOp(edge_id="e2", edge_type="depends_on",
                          source_node_id="t2", target_node_id="t1", created_by_pid="p1"),
            ))
        assert ei.value.code == VPGCode.GRAPH_EXECUTION_CYCLE

    def test_three_node_cycle_in_single_patch(self, graph):
        gid, rt = graph
        _p(rt, gid, "n", (
            AddNodeOp(node_id="a", graph_id=gid, node_type="task", created_by_pid="p1"),
            AddNodeOp(node_id="b", graph_id=gid, node_type="task", created_by_pid="p1"),
            AddNodeOp(node_id="c", graph_id=gid, node_type="task", created_by_pid="p1"),
        ))
        with pytest.raises(VPGError) as ei:
            _p(rt, gid, "c3", (
                AddEdgeOp(edge_id="e1", edge_type="depends_on", source_node_id="a", target_node_id="b", created_by_pid="p1"),
                AddEdgeOp(edge_id="e2", edge_type="depends_on", source_node_id="b", target_node_id="c", created_by_pid="p1"),
                AddEdgeOp(edge_id="e3", edge_type="depends_on", source_node_id="c", target_node_id="a", created_by_pid="p1"),
            ))
        assert ei.value.code == VPGCode.GRAPH_EXECUTION_CYCLE

    def test_cycle_detected_among_tasks_in_goal_graph(self, graph):
        gid, rt = graph
        _p(rt, gid, "n", (
            AddNodeOp(node_id="g1", graph_id=gid, node_type="goal", created_by_pid="p1", title="G"),
            AddNodeOp(node_id="a", graph_id=gid, node_type="task", created_by_pid="p1"),
            AddNodeOp(node_id="b", graph_id=gid, node_type="task", created_by_pid="p1"),
            AddNodeOp(node_id="c", graph_id=gid, node_type="task", created_by_pid="p1"),
        ))
        with pytest.raises(VPGError) as ei:
            _p(rt, gid, "mc", (
                AddEdgeOp(edge_id="e1", edge_type="depends_on", source_node_id="a", target_node_id="b", created_by_pid="p1"),
                AddEdgeOp(edge_id="e2", edge_type="depends_on", source_node_id="b", target_node_id="c", created_by_pid="p1"),
                AddEdgeOp(edge_id="e3", edge_type="depends_on", source_node_id="c", target_node_id="a", created_by_pid="p1"),
            ))
        assert ei.value.code == VPGCode.GRAPH_EXECUTION_CYCLE

    def test_cycle_path_structure(self):
        existing = [_edge("e1", "a", "b"), _edge("e2", "b", "c")]
        new = [_edge("e3", "c", "a")]
        path = detect_cycle(existing, new)
        assert path is not None
        assert path[0] == path[-1]
        assert len(path) > 1

    def test_diamond_is_acyclic(self, graph):
        gid, rt = graph
        _p(rt, gid, "n", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1"),
            AddNodeOp(node_id="t2", graph_id=gid, node_type="task", created_by_pid="p1"),
            AddNodeOp(node_id="t3", graph_id=gid, node_type="task", created_by_pid="p1"),
            AddNodeOp(node_id="t4", graph_id=gid, node_type="task", created_by_pid="p1"),
        ))
        r = _p(rt, gid, "diamond", (
            AddEdgeOp(edge_id="e1", edge_type="depends_on", source_node_id="t1", target_node_id="t2", created_by_pid="p1"),
            AddEdgeOp(edge_id="e2", edge_type="depends_on", source_node_id="t1", target_node_id="t3", created_by_pid="p1"),
            AddEdgeOp(edge_id="e3", edge_type="depends_on", source_node_id="t2", target_node_id="t4", created_by_pid="p1"),
            AddEdgeOp(edge_id="e4", edge_type="depends_on", source_node_id="t3", target_node_id="t4", created_by_pid="p1"),
        ))
        assert r.patch_applied
