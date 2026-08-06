"""AttachArtifact behavior in the no-facts path (D1 default).

With the default in-memory runtime (no facts provider), artifact bindings
are accepted without SDK wiring. This is the D1 default: the runtime
enforces the graph structure; the existential check on the artifact is
deferred to the injected ArtifactFactProvider.
"""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.models import ArtifactVersionBinding
from lhos.runtimes.verified_progress.patches import (
    AddNodeOp,
    AttachArtifactOp,
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


class TestNoFactsArtifactAccepted:
    def test_artifact_binding_accepted_with_no_provider(self, graph):
        gid, rt = graph  # default runtime: facts_artifact=None
        _p(rt, gid, "task", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1"),
        ))
        bind = ArtifactVersionBinding(
            canonical_uri="workspace:///out.md",
            artifact_id="art-1",
            version=2,
            content_hash="abc",
            media_type="text/markdown",
        )
        r = _p(rt, gid, "art", (
            AttachArtifactOp(task_node_id="t1", artifact=bind, created_by_pid="p1", edge_id="prod1"),
        ))
        assert r.patch_applied
        # a produces edge + artifact_ref node were added
        edges = rt.store.get_all_edges(gid)
        assert any(e.edge_type.value == "produces" and e.source_node_id == "t1" for e in edges)
        nodes = {n.node_id: n for n in rt.store.get_all_nodes(gid)}
        artifact_nodes = [n for n in nodes.values() if n.node_type.value == "artifact_ref"]
        assert len(artifact_nodes) == 1
        ar = artifact_nodes[0]
        assert ar.canonical_uri == "workspace:///out.md"
        assert ar.artifact_id == "art-1"
        assert ar.version == 2
        assert ar.content_hash == "abc"
        assert ar.media_type == "text/markdown"

    def test_artifact_binding_multiple_versions(self, graph):
        gid, rt = graph
        _p(rt, gid, "task", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1"),
        ))
        _p(rt, gid, "art1", (
            AttachArtifactOp(task_node_id="t1",
                             artifact=ArtifactVersionBinding(canonical_uri="u", artifact_id="a", version=1, content_hash="h1"),
                             created_by_pid="p1", edge_id="p1"),
        ))
        r = _p(rt, gid, "art2", (
            AttachArtifactOp(task_node_id="t1",
                             artifact=ArtifactVersionBinding(canonical_uri="u", artifact_id="a", version=2, content_hash="h2"),
                             created_by_pid="p1", edge_id="p2"),
        ))
        assert r.patch_applied
        artifact_nodes = [n for n in rt.store.get_all_nodes(gid) if n.node_type.value == "artifact_ref"]
        assert len(artifact_nodes) == 2
        versions = sorted(ar.version for ar in artifact_nodes)
        assert versions == [1, 2]
