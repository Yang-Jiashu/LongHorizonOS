"""Idempotent replay: same idempotency_key returns same version."""

from __future__ import annotations

import hashlib
import sqlite3

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.graph_store import GraphStore
from lhos.runtimes.verified_progress.models import ArtifactVersionBinding
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

    def test_idempotent_replay_rechecks_read_guard_freshness(self):
        conn = sqlite3.connect(":memory:")
        store = GraphStore(conn)
        conn.execute(
            """
            CREATE TABLE sdk_artifact_facts (
                artifact_id TEXT NOT NULL,
                canonical_uri TEXT NOT NULL,
                version INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                PRIMARY KEY (artifact_id, version),
                UNIQUE (canonical_uri, version)
            )
            """
        )
        digest_v1 = hashlib.sha256(b"v1").hexdigest()
        digest_v2 = hashlib.sha256(b"v2").hexdigest()
        conn.execute(
            "INSERT INTO sdk_artifact_facts "
            "(artifact_id, canonical_uri, version, content_hash) VALUES (?, ?, ?, ?)",
            ("input", "vpg://input", 1, digest_v1),
        )
        conn.commit()
        rt = VerifiedProgressRuntime(store)
        gid = rt.create_graph(owner_pid="p1").graph_id
        guard = ArtifactVersionBinding(
            canonical_uri="vpg://input",
            artifact_id="input",
            version=1,
            content_hash=digest_v1,
        )
        proposal = _p(gid, 0, "guarded-idem", "a")
        first = rt.submit_patch(proposal, _read_guards=(guard,))
        assert first.patch_applied

        # An unchanged replay remains idempotent.
        replay = rt.submit_patch(
            proposal.model_copy(update={"expected_graph_version": 1}),
            _read_guards=(guard,),
        )
        assert replay.idempotent_replay

        conn.execute(
            "INSERT INTO sdk_artifact_facts "
            "(artifact_id, canonical_uri, version, content_hash) VALUES (?, ?, ?, ?)",
            ("input", "vpg://input", 2, digest_v2),
        )
        conn.commit()
        with pytest.raises(VPGError) as exc_info:
            rt.submit_patch(
                proposal.model_copy(update={"expected_graph_version": 1}),
                _read_guards=(guard,),
            )
        assert exc_info.value.code == VPGCode.STALE_COGNITION
