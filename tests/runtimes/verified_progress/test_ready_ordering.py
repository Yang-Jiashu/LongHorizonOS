"""Ready-frontier deterministic ordering.

Ordering rule: priority DESC, topo_depth ASC, created_in_version ASC,
node_id ASC.
"""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.patches import AddEdgeOp, AddNodeOp, GraphPatchProposal


def _patch(rt, gid, kid, ops):
    return rt.submit_patch(GraphPatchProposal(
        graph_id=gid, expected_graph_version=rt.get_graph(gid).current_version,
        author_pid="p1", idempotency_key=kid, operations=ops))


def _ready_ids(rt, gid):
    return [c.task_id for c in rt.query_ready_frontier(gid)]


class TestOrdering:
    def test_priority_desc_first(self, graph):
        gid, rt = graph
        _patch(rt, gid, "setup", (
            AddNodeOp(node_id="lo", graph_id=gid, node_type="task", created_by_pid="p1", title="lo", metadata={"priority": 1}),
            AddNodeOp(node_id="hi", graph_id=gid, node_type="task", created_by_pid="p1", title="hi", metadata={"priority": 100}),
        ))
        assert _ready_ids(rt, gid) == ["hi", "lo"]

    def test_deterministic_across_repeated_queries(self, graph):
        gid, rt = graph
        for i in range(8):
            _patch(rt, gid, f"t{i}", (
                AddNodeOp(node_id=f"t{i}", graph_id=gid, node_type="task", created_by_pid="p1",
                           metadata={"priority": i}),
            ))
        a = _ready_ids(rt, gid)
        b = _ready_ids(rt, gid)
        c = _ready_ids(rt, gid)
        assert a == b == c
        # highest priority first (priority == i, so t7, t6, ..., t0)
        assert a == [f"t{i}" for i in range(7, -1, -1)]

    def test_tiebreak_node_id_asc(self, graph):
        gid, rt = graph
        # same priority, no deps -> tie broken by node_id asc
        _patch(rt, gid, "setup", (
            AddNodeOp(node_id="zzz", graph_id=gid, node_type="task", created_by_pid="p1", metadata={"priority": 5}),
            AddNodeOp(node_id="aaa", graph_id=gid, node_type="task", created_by_pid="p1", metadata={"priority": 5}),
            AddNodeOp(node_id="mmm", graph_id=gid, node_type="task", created_by_pid="p1", metadata={"priority": 5}),
        ))
        assert _ready_ids(rt, gid) == ["aaa", "mmm", "zzz"]

    def test_tasks_with_deps_frontier_is_topologically_sensible(self, graph):
        gid, rt = graph
        # chain: t3 -> t2 -> t1 ; t3 highest priority but depends on t2
        # only t1 should be READY initially regardless of priority.
        _patch(rt, gid, "setup", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", metadata={"priority": 1}),
            AddNodeOp(node_id="t2", graph_id=gid, node_type="task", created_by_pid="p1", metadata={"priority": 10}),
            AddNodeOp(node_id="t3", graph_id=gid, node_type="task", created_by_pid="p1", metadata={"priority": 100}),
            AddEdgeOp(edge_id="e1", edge_type="depends_on", source_node_id="t2", target_node_id="t1", created_by_pid="p1"),
            AddEdgeOp(edge_id="e2", edge_type="depends_on", source_node_id="t3", target_node_id="t2", created_by_pid="p1"),
        ))
        assert _ready_ids(rt, gid) == ["t1"]
