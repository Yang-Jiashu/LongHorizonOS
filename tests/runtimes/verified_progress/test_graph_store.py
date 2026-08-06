"""GraphStore basics: create, get_record, monotonic version, idempotency."""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.graph_store import GraphStore
from lhos.runtimes.verified_progress.models import GraphRecord
from lhos.runtimes.verified_progress.patches import AddNodeOp, GraphPatchProposal


@pytest.fixture
def store():
    return GraphStore(":memory:")


@pytest.fixture
def base_store():
    store = GraphStore(":memory:")
    rt = VerifiedProgressRuntime(store)
    rec = rt.create_graph(owner_pid="p1")
    gid = rec.graph_id
    for i in range(3):
        rt.submit_patch(
            GraphPatchProposal(
                graph_id=gid,
                expected_graph_version=rt.get_graph(gid).current_version,
                author_pid="p1",
                idempotency_key=f"k{i}",
                operations=(
                    AddNodeOp(
                        node_id=f"n{i}",
                        graph_id=gid,
                        node_type="task",
                        created_by_pid="p1",
                    ),
                ),
            )
        )
    return store, gid, rt


class TestCreate:
    def test_create_returns_record(self, store):
        rec = GraphRecord(graph_id="g1", owner_pid="p1")
        out = store.create_graph(rec)
        assert out.graph_id == "g1"
        assert out.owner_pid == "p1"
        assert out.current_version == 0

    def test_get_record(self, store):
        store.create_graph(GraphRecord(graph_id="g1", owner_pid="p1"))
        got = store.get_record("g1")
        assert got is not None
        assert got.graph_id == "g1"

    def test_get_record_missing_returns_none(self, store):
        assert store.get_record("nope") is None


class TestMonotonicVersion:
    def test_initial_version_zero(self, store):
        store.create_graph(GraphRecord(graph_id="g1", owner_pid="p1"))
        assert store.get_record("g1").current_version == 0

    def test_version_advances_on_commit(self, base_store):
        store, gid, rt = base_store
        assert store.get_record(gid).current_version == 3

    def test_version_sequence_contiguous(self, base_store):
        store, gid, rt = base_store
        for v in range(4):
            gv = store.get_version(gid, v)
            assert gv is not None
            assert gv.version == v


class TestIdempotency:
    def test_idempotency_key_recorded(self):
        store = GraphStore(":memory:")
        rt = VerifiedProgressRuntime(store)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        rt.submit_patch(GraphPatchProposal(
            graph_id=gid, expected_graph_version=0, author_pid="p1",
            idempotency_key="k1",
            operations=(AddNodeOp(node_id="n1", graph_id=gid, node_type="task", created_by_pid="p1"),)))
        assert store.has_idempotency(("p1", gid, "k1")) is not None
        assert store.has_idempotency(("p1", gid, "missing")) is None


class TestDuplicateGraph:
    def test_duplicate_graph_rejected(self, store):
        store.create_graph(GraphRecord(graph_id="g1", owner_pid="p1"))
        with pytest.raises(VPGError) as ei:
            store.create_graph(GraphRecord(graph_id="g1", owner_pid="p2"))
        assert ei.value.code == VPGCode.GRAPH_ALREADY_EXISTS

    def test_duplicate_call_safe(self, store):
        store.create_graph(GraphRecord(graph_id="g1", owner_pid="p1"))
        with pytest.raises(VPGError) as ei:
            store.create_graph(GraphRecord(graph_id="g1", owner_pid="p1"))
        assert ei.value.code == VPGCode.GRAPH_ALREADY_EXISTS
