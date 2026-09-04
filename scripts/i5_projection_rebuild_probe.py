"""I5 probe — is the projection rebuild really byte-identical?

The architecture claims the semantic projection is a deterministic, rebuildable
view of an append-only log.  The existing tests assert that a rebuild yields the
same node/edge sets and the same derived validity.  They do NOT assert that the
rebuilt projection hashes to the stored ``projection_hash``.

This probe closes that gap directly:

  1. build a real graph through the public SDK and close it
  2. record the stored projection_hash at the committed version
  3. call rebuild_projection() (drops and replays the patch history)
  4. recompute the projection hash and compare
  5. repeat the rebuild N times and require all hashes identical

It also inspects the replay ordering, because ``rebuild_projection`` reads
patches with ``ORDER BY applied_at`` -- a wall-clock column.  If two patches can
share a timestamp, replay order is not total and the "deterministic rebuild"
claim rests on timestamp resolution rather than on a monotonic sequence.
"""

# ruff: noqa
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from lhos.sdk import Agent, AgentOS, Goal, scripted_executor


def build_closed_goal(n=12):
    rt = AgentOS(":memory:")
    rt.add_agent(Agent("w", specializations=("python",), max_concurrency=4))
    g = Goal("I5")
    prev = None
    for i in range(n):
        prev = g.task(
            f"t{i}",
            agent="w",
            depends_on=(prev,) if prev else (),
            verify=scripted_executor(artifact_id=f"a{i}.txt", version=1),
        )
    result = rt.run(g, max_dispatches=n * 6 + 40, max_steps=n * 6 + 40)
    return rt, g, result


def main() -> int:
    out = REPO / "artifacts" / "agent_os_phase_d3"
    out.mkdir(parents=True, exist_ok=True)

    rt, goal, result = build_closed_goal()
    vpg = rt.vpg
    gid = rt._gid(goal) if hasattr(rt, "_gid") else None
    if gid is None:  # discover the graph id from the runtime
        gid = next(iter(vpg.store.conn.execute("SELECT graph_id FROM graphs").fetchall()))[0]

    rec = vpg.get_graph(gid)
    stored_hash_row = vpg.store.conn.execute(
        "SELECT projection_hash, version FROM graph_versions "
        "WHERE graph_id=? ORDER BY version DESC LIMIT 1",
        (gid,),
    ).fetchone()
    stored_hash = stored_hash_row["projection_hash"]
    stored_version = stored_hash_row["version"]

    # --- replay-order determinism: is applied_at a total order? ---
    ts = [
        r["applied_at"]
        for r in vpg.store.conn.execute(
            "SELECT applied_at FROM graph_patches WHERE graph_id=?", (gid,)
        ).fetchall()
    ]
    dupes = {t: c for t, c in Counter(ts).items() if c > 1}

    # --- what does the stored projection_hash actually cover? ---
    # graph_store._compute_projection_hash is called with `nodes_to_upsert` /
    # `edges_to_upsert`, i.e. only the rows THIS patch touched -- it is a
    # per-patch delta digest, NOT a whole-projection digest.  So "rebuilt hash
    # == stored hash" is not a meaningful comparison.  We therefore compute an
    # independent whole-projection digest with the same construction and
    # compare the pre-rebuild projection against post-rebuild ones.
    import hashlib

    def whole_projection_digest(nodes, edges) -> str:
        h = hashlib.sha256()
        items = nodes.items() if hasattr(nodes, "items") else nodes
        for node_id, node in sorted(items, key=lambda x: x[0]):
            h.update(node_id.encode())
            h.update(node.model_dump_json().encode())
            h.update(b"|")
        for e in sorted(edges, key=lambda e: e.edge_id):
            h.update(e.edge_id.encode())
            h.update(e.source_node_id.encode())
            h.update(e.target_node_id.encode())
            h.update(b"|")
        return h.hexdigest()

    before_nodes, before_edges = vpg.snapshot_projection(gid)
    before_digest = whole_projection_digest(before_nodes, before_edges)

    hashes = []
    for _ in range(3):
        nodes, edges, _ = vpg.rebuild_projection(gid)
        hashes.append(whole_projection_digest(nodes, edges))

    all_equal = len(set(hashes)) == 1
    matches_stored = bool(hashes) and hashes[0] == before_digest

    report = {
        "probe": "I5_byte_identical_projection_rebuild",
        "goal_state": result.goal_state,
        "graph_id": gid,
        "committed_version": stored_version,
        "current_version": rec.current_version,
        "stored_projection_hash": stored_hash,
        "stored_hash_covers": (
            "per-patch delta only (graph_store._compute_projection_hash is called "
            "with nodes_to_upsert/edges_to_upsert), NOT the whole projection"
        ),
        "pre_rebuild_whole_projection_digest": before_digest,
        "rebuilt_hashes": hashes,
        "rebuilds_mutually_identical": all_equal,
        "rebuilt_matches_stored": matches_stored,
        "patch_count": len(ts),
        "duplicate_applied_at_timestamps": dupes,
        "replay_order_is_total": len(dupes) == 0,
        "verdict": (
            "BYTE-IDENTICAL"
            if (all_equal and matches_stored)
            else ("SELF-CONSISTENT ONLY" if all_equal else "NON-DETERMINISTIC")
        ),
    }
    (out / "i5-projection-rebuild-probe.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )

    print(f"goal_state                 : {result.goal_state}")
    print(f"patches replayed           : {len(ts)}")
    print(f"pre-rebuild digest         : {before_digest[:24]}...")
    print(f"rebuilt hashes             : {[h[:16] for h in hashes]}")
    print(f"rebuilds mutually identical: {all_equal}")
    print(f"rebuilt == pre-rebuild     : {matches_stored}")
    print(f"duplicate applied_at       : {dupes if dupes else 'none'}")
    print(f"replay order is a total order: {len(dupes) == 0}")
    print(f"VERDICT                    : {report['verdict']}")
    print(f"json: {out / 'i5-projection-rebuild-probe.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
