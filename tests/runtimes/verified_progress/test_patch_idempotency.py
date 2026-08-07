"""Idempotent replay: same idempotency_key returns same version."""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
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


class TestIdempotency:
    def test_same_key_twice_idempotent_replay(self, graph):
        gid, rt = graph
        r1 = rt.submit_patch(_p(gid, 0, "idem-1", "a"))
        assert r1.patch_applied is True
        assert r1.idempotent_replay is False
        assert r1.committed_graph_version == 1
        # replay must target current version
        r2 = rt.submit_patch(_p(gid, 1, "idem-1", "a"))
        assert r2.idempotent_replay is True
        assert r2.patch_applied is False
        assert r2.committed_graph_version == 1

    def test_idempotent_replay_does_not_bump_version(self, graph):
        gid, rt = graph
        rt.submit_patch(_p(gid, 0, "idem-1", "a"))
        rt.submit_patch(_p(gid, 1, "idem-1", "a"))
        assert rt.get_graph(gid).current_version == 1

    def test_distinct_keys_both_apply(self, graph):
        gid, rt = graph
        r1 = rt.submit_patch(_p(gid, 0, "k1", "a"))
        assert r1.patch_applied
        r2 = rt.submit_patch(_p(gid, 1, "k2", "b"))
        assert r2.patch_applied
        assert rt.get_graph(gid).current_version == 2

    def test_idempotent_replay_returns_same_patch_id(self, graph):
        gid, rt = graph
        r1 = rt.submit_patch(_p(gid, 0, "idem-1", "a"))
        r2 = rt.submit_patch(_p(gid, 1, "idem-1", "a"))
        assert r1.patch_id == r2.patch_id
