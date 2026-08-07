"""Demo 2 — Evidence-Backed Verification.

Contract:
    Action COMMITTED without Evidence -> Task stays UNVERIFIED.
    PASS Evidence bound to the task's current artifact pins -> Task VERIFIED.
    FAIL Evidence -> task stays UNVERIFIED.
    INCONCLUSIVE Evidence -> task stays UNVERIFIED.

In D1, an Evidence's artifact_bindings MUST match the task's currently
pinned output ArtifactRefs (the produced versions). Evidence content_ref
carries larger outputs (test report, logs) stored in Artifact FS.
"""

from __future__ import annotations

import hashlib
import sys

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.models import ArtifactVersionBinding
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    AttachArtifactOp,
    GraphPatchProposal,
)
from lhos.runtimes.verified_progress.protocols import (
    ArtifactFactProvider,
    KernelEventProvider,
)


class _Ac:
    def __init__(self, action_id, pid, state):
        self.action_id = action_id
        self.pid = pid
        self.state = state
        self.result = {"exit_code": 0}
        self.artifact_refs = ()


class FakeFacts(KernelEventProvider, ArtifactFactProvider):
    def __init__(self, actions, artifacts):
        self._actions = actions
        self._artifacts = artifacts

    def get_action(self, aid):
        return self._actions.get(aid)

    def has_event(self, eid):
        return eid in self._actions

    def artifact_exists(self, pid, uri, v):
        return (uri, v) in self._artifacts

    def read_hash(self, pid, uri, v):
        return self._artifacts.get((uri, v))

    def verify_binding(self, pid, binding):
        return self._artifacts.get((binding.canonical_uri, binding.version)) == binding.content_hash

    def can_read(self, pid, aid, v):
        return True

    def list_events_for_pid(self, pid):
        return []


def main() -> int:
    code = b"print('hello')\n"
    code_hash = hashlib.sha256(code).hexdigest()

    artifacts = {
        ("artifact://ns-p1/build/code.py", 1): code_hash,
    }
    actions = {"test-action-1": _Ac("test-action-1", "agent-1", "committed")}

    rt = VerifiedProgressRuntime(
        ":memory:",
        facts_artifact=FakeFacts(actions, artifacts),
        facts_kernel=FakeFacts(actions, artifacts),
    )
    rec = rt.create_graph(owner_pid="agent-1")
    gid = rec.graph_id

    def patch(ops, key):
        return rt.submit_patch(
            GraphPatchProposal(
                graph_id=gid,
                expected_graph_version=rt.get_graph(gid).current_version,
                author_pid="agent-1",
                operations=ops,
                idempotency_key=key,
            )
        )

    patch(
        (
            AddNodeOp(
                node_id="t1",
                graph_id=gid,
                node_type="task",
                created_by_pid="agent-1",
                title="Build",
            ),
            AddNodeOp(
                node_id="v1", graph_id=gid, node_type="verification", created_by_pid="agent-1"
            ),
            AddEdgeOp(
                edge_id="v1-t1",
                edge_type="verifies",
                source_node_id="v1",
                target_node_id="t1",
                created_by_pid="agent-1",
            ),
        ),
        "p1",
    )
    patch(
        (
            AttachArtifactOp(
                task_node_id="t1",
                artifact=ArtifactVersionBinding(
                    canonical_uri="artifact://ns-p1/build/code.py",
                    artifact_id="aid-code",
                    version=1,
                    content_hash=code_hash,
                ),
                created_by_pid="agent-1",
                edge_id="t1-a1",
            ),
        ),
        "p2",
    )

    pre = rt.inspect_node(gid, "t1")
    print(f"pre-evidence T1.validity={pre.validity.value}  lifecycle={pre.lifecycle.value}")
    assert pre.validity.value == "unverified"

    patch(
        (
            AddNodeOp(
                node_id="ev-pass",
                graph_id=gid,
                node_type="evidence",
                created_by_pid="agent-1",
                result="pass",
                evidence_source_action_id="test-action-1",
                source_verification_id="v1",
                # evidence binds to task's pinned produced artifact
                artifact_bindings=(
                    ArtifactVersionBinding(
                        canonical_uri="artifact://ns-p1/build/code.py",
                        artifact_id="aid-code",
                        version=1,
                        content_hash=code_hash,
                    ),
                ),
                evidence_hash=code_hash,
                produced_by_pid="agent-1",
            ),
            AddEdgeOp(
                edge_id="v1-ev-pass",
                edge_type="produces",
                source_node_id="v1",
                target_node_id="ev-pass",
                created_by_pid="agent-1",
            ),
        ),
        "p3",
    )

    post = rt.inspect_node(gid, "t1")
    print(f"post-evidence T1.validity={post.validity.value}  lifecycle={post.lifecycle.value}")
    assert post.validity.value == "verified", "PASS evidence should verify the task"
    assert post.lifecycle.value == "closed", "verified task transitions to CLOSED"

    print("\nDemo 2 PASSED — agent cannot fabricate VERIFIED without matching Evidence.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
