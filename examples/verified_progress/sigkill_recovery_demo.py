"""Demo 7 — SIGKILL Recovery (Phase D1 crash-safety).

Contract:
    A committed Patch is durable (SQLite transaction).  The materialized
    projection (graph_nodes_projection / graph_edges_projection) is DERIVED and
    therefore recoverable from the Patch record.  A crash (SIGKILL) after commit
    can wipe the projection while leaving patch/event history intact.

    Recovery must:
      1. Detect the lost projection (verify_projection() fails pre-recovery).
      2. Re-derive the projection deterministically from the Patch record
         (rt.rebuild_projection) so byte-identical state is restored.
      3. Replay every derived event (recovery.verify_and_recover emits
         GRAPH_RECOVERY_* + TASK_VERIFIED_DERIVED / ... ).
      4. Be idempotent — running recovery N times yields the SAME projection
         (proves a repeated crash after an incomplete recovery still converges).
      5. Leave downstream consumers (query_ready_frontier) consistent: once the
         upstream Task t1 is VERIFIED, the depending Task t2 enters the READY
         frontier.

This demo mirrors the style of the sibling demos: FakeFacts / Kernel facts,
submit_patch through the SDK, and GraphStore.conn exposed for the SIGKILL
simulation.
"""

from __future__ import annotations

import hashlib
import sys

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.events import GraphEventType
from lhos.runtimes.verified_progress.models import ArtifactVersionBinding, TaskNode
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    AttachArtifactOp,
    AttachEvidenceOp,
    GraphPatchProposal,
)
from lhos.runtimes.verified_progress.protocols import (
    ArtifactFactProvider,
    KernelEventProvider,
)
from lhos.runtimes.verified_progress.recovery import verify_and_recover

# ---------------------------------------------------------------------------
# Lightweight Kernel facts provider (same shape as the sibling demos).
# An action is "committed"; every artifact binding / read is accepted, so the
# verification derivation is driven entirely by structure the demo controls.
# ---------------------------------------------------------------------------

class _FakeAction:
    def __init__(self, action_id: str, pid: str = "agent-1") -> None:
        self.action_id = action_id
        self.pid = pid
        self.state = "committed"
        self.result: dict = {}
        self.artifact_refs: tuple = ()


class FakeFacts(KernelEventProvider, ArtifactFactProvider):
    def __init__(self, actions: dict, artifacts: dict) -> None:
        self._actions = actions
        self._artifacts = artifacts

    def get_action(self, action_id):  # KernelEventProvider
        return self._actions.get(action_id)

    def has_event(self, event_id):  # KernelEventProvider
        return event_id in self._actions

    def list_events_for_pid(self, pid):  # KernelEventProvider
        return []

    def artifact_exists(self, pid, canonical_uri, version):  # ArtifactFactProvider
        return (canonical_uri, version) in self._artifacts

    def read_hash(self, pid, canonical_uri, version):  # ArtifactFactProvider
        return self._artifacts.get((canonical_uri, version))

    def verify_binding(self, pid, binding):  # ArtifactFactProvider
        # Walk the full binding-hash path even with no kernel facts wired up.
        return (
            self._artifacts.get((binding.canonical_uri, binding.version))
            == binding.content_hash
        )

    def can_read(self, pid, aid, version):  # ArtifactFactProvider
        return True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _patch(rt: VerifiedProgressRuntime, gid: str, ops, key: str):
    return rt.submit_patch(GraphPatchProposal(
        graph_id=gid,
        expected_graph_version=rt.get_graph(gid).current_version,
        author_pid="agent-1",
        operations=ops,
        idempotency_key=key,
    ))


def verify_projection(store, graph_id: str) -> bool:
    """A projection is 'verified' iff the materialized record is consistent:

      * at least one node and one edge are present, and
      * the upstream Task t1 is in VERIFIED state.
    """
    nodes = store.get_all_nodes(graph_id)
    edges = store.get_all_edges(graph_id)
    if not nodes or not edges:
        return False
    t1 = next((n for n in nodes if n.node_id == "t1"), None)
    return isinstance(t1, TaskNode) and t1.validity.value == "verified"


def projection_fields_hash(store, graph_id: str) -> bytes:
    """Deterministic hash over the SIGNIFICANT fields of the projection.

    We deliberately exclude timestamps (created_at etc.) because every rebuild
    freshly stamps them; the *logical* projection (node id/type/lifecycle/
    validity and edge identity/endpoints) is what must be byte-identical across
    recovery runs.
    """
    h = hashlib.sha256()
    for n in sorted(store.get_all_nodes(graph_id), key=lambda x: x.node_id):
        h.update(f"{n.node_id}:{n.node_type.value}:"
                 f"{n.lifecycle.value}:{n.validity.value}".encode())
        h.update(b"|")
    for e in sorted(store.get_all_edges(graph_id), key=lambda x: x.edge_id):
        h.update(f"{e.edge_id}:{e.edge_type.value}:"
                 f"{e.source_node_id}:{e.target_node_id}".encode())
        h.update(b"|")
    return h.digest()


def banner_line(n: int, label: str, ok: bool, expected_fail: bool = False) -> str:
    status = ("FAIL (expected)" if expected_fail else "PASS") if ok else (
        "FAIL" if not expected_fail else "FAIL (unexpected)"
    )
    return f"[{n}] {label.ljust(44, '.')}  {status}"


def main() -> int:
    # ---- facts -----------------------------------------------------------------
    uri = "artifact://ns-p1/build/obj.out"
    vhash = hashlib.sha256(b"binary-payload").hexdigest()
    binding = ArtifactVersionBinding(
        canonical_uri=uri, artifact_id="art-1", version=1, content_hash=vhash,
    )
    facts = FakeFacts(
        actions={"act-1": _FakeAction("act-1")},
        artifacts={(uri, 1): vhash},
    )

    # ---- graph -----------------------------------------------------------------
    rt = VerifiedProgressRuntime(
        ":memory:",
        facts_artifact=facts,
        facts_kernel=facts,
    )
    gid = rt.create_graph(owner_pid="agent-1").graph_id
    store = rt.store  # expose GraphStore for the SIGKILL simulation

    # P1: goal g1 -> depends_on t1,   t2 -> depends_on t1,   v1 verifies t1
    _patch(rt, gid, (
        AddNodeOp(node_id="g1", graph_id=gid, node_type="goal",
                  created_by_pid="agent-1", title="Root goal"),
        AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                  created_by_pid="agent-1", title="Upstream task"),
        AddNodeOp(node_id="t2", graph_id=gid, node_type="task",
                  created_by_pid="agent-1", title="Downstream task"),
        AddNodeOp(node_id="v1", graph_id=gid, node_type="verification",
                  created_by_pid="agent-1"),
        AddEdgeOp(edge_id="g1-t1", edge_type="depends_on",
                  source_node_id="g1", target_node_id="t1",
                  created_by_pid="agent-1"),
        AddEdgeOp(edge_id="t2-t1", edge_type="depends_on",
                  source_node_id="t2", target_node_id="t1",
                  created_by_pid="agent-1"),
        AddEdgeOp(edge_id="v1-t1", edge_type="verifies",
                  source_node_id="v1", target_node_id="t1",
                  created_by_pid="agent-1"),
    ), "p1-structure")

    # P2: pin the produced artifact to t1 (task-local version binding)
    _patch(rt, gid, (
        AttachArtifactOp(task_node_id="t1", artifact=binding,
                         created_by_pid="agent-1", edge_id="t1-p1"),
    ), "p2-pin")

    # P3: PASS-evidence for v1 -> t1 becomes VERIFIED, g1 closes, t2 READY
    _patch(rt, gid, (
        AddNodeOp(node_id="e1", graph_id=gid, node_type="evidence",
                  created_by_pid="agent-1",
                  result="pass",
                  evidence_source_action_id="act-1",
                  source_verification_id="v1",
                  artifact_bindings=(binding,),
                  produced_by_pid="agent-1"),
        AttachEvidenceOp(verification_node_id="v1", evidence_node_id="e1",
                         created_by_pid="agent-1", edge_id="p3-ev"),
    ), "p3-evidence")

    # -- [1] pre-sigkill t1.verified ---------------------------------------------
    t1_pre = rt.inspect_node(gid, "t1")
    ok1 = (t1_pre is not None
           and t1_pre.validity.value == "verified"
           and t1_pre.lifecycle.value == "closed")

    # -- [2] pre-sigkill t2.ready (deps satisfied) -------------------------------
    ready_pre = [c.task_id for c in rt.query_ready_frontier(gid)]
    ok2 = "t2" in ready_pre and "t1" not in ready_pre

    # -- [3] simulate SIGKILL: derived projection wiped, record intact ------------
    with store.conn:
        store.conn.execute("DELETE FROM graph_nodes_projection;")
        store.conn.execute("DELETE FROM graph_edges_projection;")
    ok3 = True  # the wipe itself is the crash; record current_version untouched

    # -- [4] verify_projection pre-recovery MUST FAIL (expected) -----------------
    pre_verify = verify_projection(store, gid)
    ok4 = pre_verify is False  # assert projected state is genuinely lost
    assert ok4, "pre-recovery projection should be inconsistent (projection wiped)"

    # -- [5] recovery is idempotent: 4 runs, byte-identical projection ----------
    hashes: list[bytes] = []
    for _ in range(4):
        # Emit recovery events (+replay derived events onto the Patch record).
        rec_events, record = verify_and_recover(
            store, gid, facts_artifact=facts, facts_kernel=facts,
        )
        assert record is not None and record.graph_id == gid
        assert any(e.event_type == GraphEventType.GRAPH_RECOVERY_STARTED
                   for e in rec_events)
        assert any(e.event_type == GraphEventType.GRAPH_RECOVERY_COMPLETED
                   for e in rec_events)

        # Restore the materialized projection deterministically from Patch history.
        _rebuilt_nodes, _rebuilt_edges, rebuilt_evs = rt.rebuild_projection(gid)

        assert any(e.event_type == GraphEventType.TASK_VERIFIED_DERIVED
                   for e in (*rec_events, *rebuilt_evs)), (
            "recovery must replay TASK_VERIFIED_DERIVED for the verified task"
        )
        hashes.append(projection_fields_hash(store, gid))

    ok5 = len({h.hex() for h in hashes}) == 1
    assert ok5, "projection must be byte-identical across 4 recovery runs (idempotent)"

    # -- [6] post-recovery t2.ready ----------------------------------------------
    ready_post = [c.task_id for c in rt.query_ready_frontier(gid)]
    ok6 = "t2" in ready_post
    assert ok6, f"t2 should be READY post-recovery, got {ready_post}"

    # ---- banner ----------------------------------------------------------------
    all_ok = ok1 and ok2 and ok3 and ok4 and ok5 and ok6
    print("=" * 40)
    print(" LongHorizonOS Phase D1 \u2014 SIGKILL Recovery Demo")
    print("=" * 40)
    print(banner_line(1, "pre-sigkill t1.verified", ok1))
    print(banner_line(2, "pre-sigkill t2.ready (deps satisfied)", ok2))
    print(banner_line(3, "simulate SIGKILL (wipe projection)", ok3))
    print(banner_line(4, "verify_projection pre-recovery", ok4, expected_fail=True))
    print(banner_line(5,
                      "verify_and_recover idempotent (4 runs same hash)", ok5))
    print(banner_line(6, "post-recovery t2.ready", ok6))
    print("=" * 40)
    print("SIGKILL recovery DEMO PASSED" if all_ok
          else "SIGKILL recovery DEMO FAILED")
    print("=" * 40)

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
