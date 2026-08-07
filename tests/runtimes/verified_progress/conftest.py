"""Shared fixtures for the Verified Progress Graph (Phase D1) runtime test suite.

Each test file in this directory is self-contained and may import these
fixtures for convenience, but must not assume any cross-file ordering or
shared mutable state. Fixtures return fresh instances on every call.
"""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.patches import (
    AddNodeOp,
    GraphPatchProposal,
)


@pytest.fixture
def rt() -> VerifiedProgressRuntime:
    """A fresh in-memory runtime with no injected facts providers (the
    "pure graph" / no-facts path). Artifact bindings are accepted; evidence
    validation against the kernel journal is NOT wired."""
    return VerifiedProgressRuntime(":memory:")


@pytest.fixture
def graph(rt: VerifiedProgressRuntime):
    """Create a fresh graph and return (graph_id, rt)."""
    rec = rt.create_graph(owner_pid="p1")
    return rec.graph_id, rt


@pytest.fixture
def make_add_node():
    """Factory that builds an AddNodeOp with sensible defaults.

    Usage::

        op = make_add_node("task", node_id="t1", title="T1")
    """

    def _make(node_type: str = "task", **overrides) -> AddNodeOp:
        defaults = dict(
            node_id="node-1",
            graph_id="graph-1",
            node_type=node_type,
            created_by_pid="p1",
        )
        defaults.update(overrides)
        return AddNodeOp(**defaults)

    return _make


@pytest.fixture
def fake_facts():
    """A simple fake facts provider implementing both ArtifactFactProvider
    and KernelEventProvider. Used to drive the verification branch end-to-end
    without real Agent OS SDK wiring.

    The fake returns canned "committed" actions whose artifact_refs and
    hashes match whatever the caller asserts, which lets a Test transition
    to VERIFIED/CLOSED deterministically.
    """
    from lhos.runtimes.verified_progress.models import ArtifactVersionBinding

    class FakeAction:
        def __init__(self, action_id, pid="p1", state="committed", result=None, artifact_refs=()):
            self.action_id = action_id
            self.pid = pid
            self.state = state
            self.result = result or {}
            self.artifact_refs = tuple(
                a
                if isinstance(a, dict)
                else a.model_dump()
                if isinstance(a, ArtifactVersionBinding)
                else a
                for a in artifact_refs
            )

    class FakeFacts:
        def __init__(self, actions=None, hashes=None):
            self.actions = actions or {}
            self.hashes = hashes or {}

        def get_action(self, action_id):
            return self.actions.get(action_id)

        def has_event(self, event_id):
            return False

        def list_events_for_pid(self, pid):
            return []

        def artifact_exists(self, pid, canonical_uri, version):
            return True

        def read_hash(self, pid, canonical_uri, version):
            return self.hashes.get((canonical_uri, version))

        def verify_binding(self, pid, binding):
            return True

        def can_read(self, pid, artifact_id, version):
            return True

    return FakeFacts


def patch(rt, graph_id, kid, ops, expected_graph_version=None):
    """Helper to build + commit a patch, advancing cur_version tracking."""
    from lhos.runtimes.verified_progress.patches import GraphPatchProposal

    ver = (
        expected_graph_version
        if expected_graph_version is not None
        else rt.get_graph(graph_id).current_version
    )
    p = GraphPatchProposal(
        graph_id=graph_id,
        expected_graph_version=ver,
        author_pid="p1",
        idempotency_key=kid,
        operations=ops,
    )
    return rt.submit_patch(p)


@pytest.fixture
def commit():
    """Bound commit helper usable as ``commit(rt, gid, 'key', (ops,))``."""
    return patch
