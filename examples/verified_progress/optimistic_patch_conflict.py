"""Demo 4 — Optimistic Patch Conflict + Idempotency.

Contract:
    Two concurrent writers attempt patches at the SAME expected_graph_version.
    Exactly one succeeds; the other receives GRAPH_VERSION_CONFLICT.
    The winner's effect is visible; the loser is rejected atomically — no
    half-state.

    Idempotency: replaying the SAME patch (same composite_key) returns the
    previously committed result without applying again.  Graph version does
    NOT advance on replay.
"""

from __future__ import annotations

import sys

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.patches import (
    AddNodeOp,
    GraphPatchProposal,
)


def main() -> int:
    rt = VerifiedProgressRuntime(":memory:")
    gid = rt.create_graph(owner_pid="agent-1").graph_id

    def patch(ops, key, *, expect_v):
        return rt.submit_patch(
            GraphPatchProposal(
                graph_id=gid,
                expected_graph_version=expect_v,
                author_pid="agent-1",
                operations=ops,
                idempotency_key=key,
            )
        )

    # --- Patch A wins at expected=0 ---
    rA = patch(
        (
            AddNodeOp(
                node_id="x", graph_id=gid, node_type="task", created_by_pid="agent-1", title="X"
            ),
        ),
        key="A",
        expect_v=0,
    )
    print(
        f"patch A: v={rA.committed_graph_version} applied={rA.patch_applied} "
        f"replay={rA.idempotent_replay}"
    )
    assert rA.patch_applied is True
    assert rA.committed_graph_version == 1

    # --- Patch B attempts at the SAME expected version 0 -- conflict ---
    raised = False
    try:
        patch(
            (
                AddNodeOp(
                    node_id="y", graph_id=gid, node_type="task", created_by_pid="agent-1", title="Y"
                ),
            ),
            key="B",
            expect_v=0,
        )
    except VPGError as e:
        raised = True
        print(f"patch B: conflict → {e.code.value}")
        assert e.code == VPGCode.GRAPH_VERSION_CONFLICT
    assert raised, "expected GRAPH_VERSION_CONFLICT"

    # Graph version stays at 1 (no half-advance)
    assert rt.get_graph(gid).current_version == 1
    # Only task 'x' was created; 'y' does not exist
    assert rt.inspect_node(gid, "x") is not None
    assert rt.inspect_node(gid, "y") is None

    # --- Patch A replay (idempotent) ---
    rA2 = patch(
        (
            AddNodeOp(
                node_id="x", graph_id=gid, node_type="task", created_by_pid="agent-1", title="X"
            ),
        ),
        key="A",
        expect_v=99,  # version drift is ignored on idempotent replay
    )
    print(
        f"patch A replay: v={rA2.committed_graph_version} applied={rA2.patch_applied} "
        f"replay={rA2.idempotent_replay}"
    )
    assert rA2.idempotent_replay is True
    assert rA2.patch_applied is False
    assert rt.get_graph(gid).current_version == 1  # no advance

    # --- Patch C wins at expected=1 ---
    rC = patch(
        (
            AddNodeOp(
                node_id="z", graph_id=gid, node_type="task", created_by_pid="agent-1", title="Z"
            ),
        ),
        key="C",
        expect_v=1,
    )
    print(f"patch C: v={rC.committed_graph_version} applied={rC.patch_applied}")
    assert rC.committed_graph_version == 2

    print("\nDemo 4 PASSED — optimistic conflict rejects stale writer; idempotency honored.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
