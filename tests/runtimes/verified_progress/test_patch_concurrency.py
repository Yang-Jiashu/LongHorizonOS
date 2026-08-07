"""Optimistic concurrency: stale expected_graph_version is rejected."""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.patches import AddNodeOp, GraphPatchProposal


def _p(graph_id, expected_version, kid, nid):
    return GraphPatchProposal(
        graph_id=graph_id,
        expected_graph_version=expected_version,
        author_pid="p1",
        idempotency_key=kid,
        operations=(
            AddNodeOp(node_id=nid, graph_id=graph_id, node_type="task", created_by_pid="p1"),
        ),
    )


class TestOptimisticConflict:
    def test_two_patches_based_on_v0_exactly_one_succeeds(self, graph):
        gid, rt = graph
        r1 = rt.submit_patch(_p(gid, 0, "k1", "a"))
        assert r1.patch_applied is True
        assert r1.committed_graph_version == 1
        with pytest.raises(VPGError) as ei:
            rt.submit_patch(_p(gid, 0, "k2", "b"))
        assert ei.value.code == VPGCode.GRAPH_VERSION_CONFLICT

    def test_rejected_conflict_does_not_bump_version(self, graph):
        gid, rt = graph
        rt.submit_patch(_p(gid, 0, "k1", "a"))
        before = rt.get_graph(gid).current_version
        with pytest.raises(VPGError):
            rt.submit_patch(_p(gid, 0, "k2", "b"))
        after = rt.get_graph(gid).current_version
        assert after == before == 1

    def test_only_first_commit_applies(self, graph):
        gid, rt = graph
        rt.submit_patch(_p(gid, 0, "k1", "a"))
        with pytest.raises(VPGError):
            rt.submit_patch(_p(gid, 0, "k2", "b"))
        # node 'a' exists, node 'b' was never committed
        assert rt.inspect_node(gid, "a") is not None
        assert rt.inspect_node(gid, "b") is None

    def test_sequential_commits_accepted(self, graph):
        gid, rt = graph
        rt.submit_patch(_p(gid, 0, "k1", "a"))
        rt.submit_patch(_p(gid, 1, "k2", "b"))
        rt.submit_patch(_p(gid, 2, "k3", "c"))
        assert rt.get_graph(gid).current_version == 3
        for n in ("a", "b", "c"):
            assert rt.inspect_node(gid, n) is not None
