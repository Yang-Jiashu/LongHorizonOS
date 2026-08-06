"""Demo 5 — Deterministic Ready Frontier.

Contract:
    Given a DAG of tasks with depends_on edges and priority metadata, the
    READY frontier is the set of tasks whose deps are all satisfied (VERIFIED)
    or absent.  The frontier is sorted deterministically:

        priority DESC, topo_depth ASC, created_in_version ASC, node_id ASC

    Ties are ALWAYS broken by node_id — so any two runs, across processes and
    restarts, produce identical frontier order.
"""

from __future__ import annotations

import sys

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    GraphPatchProposal,
)


def main() -> int:
    rt = VerifiedProgressRuntime(":memory:")
    gid = rt.create_graph(owner_pid="agent-1").graph_id

    def patch(ops, key):
        return rt.submit_patch(GraphPatchProposal(
            graph_id=gid,
            expected_graph_version=rt.get_graph(gid).current_version,
            author_pid="agent-1",
            operations=ops,
            idempotency_key=key,
        ))

    # DAG (deps point from child -> parents so READY requires parents done):
    #   t_a (priority=5,  no deps)     t_b (priority=10, no deps)
    #        \                                /
    #         v                              v
    #                t_c (priority=0,  deps a,b)
    #                      |
    #                      v
    #                  t_d (priority=5,  deps c)
    #
    # Plus  : t_e (priority=10, no deps)
    # Frontier, with all ADMITTED/UNVERIFIED (no dep-ver yet):
    #   t_b (p=10 depth=0), t_e (p=10 depth=0), t_a (p=5 depth=0),
    #   then t_c (p=0 depth=1) — t_d blocked by t_c.
    # Tie-break t_b vs t_e: same priority/depth/created -> node_id.
    # Expected order: ["t_b", "t_e", "t_a", "t_c"].

    patch((
        AddNodeOp(node_id="t_a", graph_id=gid, node_type="task",
                  created_by_pid="agent-1", metadata={"priority": 5}),
        AddNodeOp(node_id="t_b", graph_id=gid, node_type="task",
                  created_by_pid="agent-1", metadata={"priority": 10}),
        AddNodeOp(node_id="t_c", graph_id=gid, node_type="task",
                  created_by_pid="agent-1", metadata={"priority": 0}),
        AddNodeOp(node_id="t_d", graph_id=gid, node_type="task",
                  created_by_pid="agent-1", metadata={"priority": 5}),
        AddNodeOp(node_id="t_e", graph_id=gid, node_type="task",
                  created_by_pid="agent-1", metadata={"priority": 10}),
    ), "n1")

    patch((
        AddEdgeOp(edge_id="e-ac", edge_type="depends_on",
                  source_node_id="t_c", target_node_id="t_a",
                  created_by_pid="agent-1"),
        AddEdgeOp(edge_id="e-bc", edge_type="depends_on",
                  source_node_id="t_c", target_node_id="t_b",
                  created_by_pid="agent-1"),
        AddEdgeOp(edge_id="e-cd", edge_type="depends_on",
                  source_node_id="t_d", target_node_id="t_c",
                  created_by_pid="agent-1"),
    ), "e1")

    f1 = rt.query_ready_frontier(gid)
    ids1 = [c.task_id for c in f1]
    print(f"frontier (v={rt.get_graph(gid).current_version}): {ids1}")

    # Tasks with no dependencies are READY: t_b (p=10), t_e (p=10), t_a (p=5).
    # Tie on t_b/t_e (same priority=10, depth=0, same created_in_version)
    # breaks by node_id => t_b before t_e.
    # t_c blocked: depends on t_a,t_b which are not VERIFIED.
    # t_d blocked: depends on t_c.
    assert ids1 == ["t_b", "t_e", "t_a"], f"got {ids1}"
    assert "t_c" not in ids1, "t_c is blocked by unverified deps"
    assert "t_d" not in ids1, "t_d is blocked by t_c"

    # Determinism: repeated reads return identical order.
    f2 = rt.query_ready_frontier(gid)
    assert [c.task_id for c in f2] == ids1, "frontier must be stable on repeat"

    # Determinism across projection rebuild: same order.
    rt.rebuild_projection(gid)
    f3 = rt.query_ready_frontier(gid)
    assert [c.task_id for c in f3] == ids1, (
        f"frontier must equal post-rebuild, got {[c.task_id for c in f3]}"
    )

    print("\nPASSED — deterministic READY frontier:")
    print("  priority DESC | topo_depth ASC | created ASC | node_id ASC")
    for c in f1:
        md = rt.inspect_node(gid, c.task_id).metadata or {}
        print(f"   {c.task_id}  priority={md.get('priority')}  depth={c.readiness_proof.graph_version}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
