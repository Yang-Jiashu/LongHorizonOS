"""Same-Patch Cycle Attack — Phase D1.1 Step 11.

Proves: within a SINGLE patch, combining existing committed ``DEPENDS_ON``
edges with newly proposed ``DEPENDS_ON`` edges, the runtime MUST REJECT any
patch whose combined dependency graph contains a cycle.

The production VPGCode raised by the cycle detector is ``GRAPH_EXECUTION_CYCLE``
(via ``dag.detect_cycle`` + ``is_self_loop`` in patch_validator.py).  The test
specifier's label ``DEPENDS_ON_CYCLE`` maps to this same code.

One test per scenario.  Note on edge direction:
    X -[depends_on]-> Y   means "Y must be DONE before X can READY"
    (X depends on Y; arrow points from dependent to prerequisite).
"""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    GraphPatchProposal,
)

AUDIT_RESULTS: dict[str, dict] = {}

# Cycle errors come from errors.execution_cycle() → GRAPH_EXECUTION_CYCLE
_CYCLE_CODE = VPGCode.GRAPH_EXECUTION_CYCLE


def _make_rt():
    return VerifiedProgressRuntime(":memory:")


def _submit(rt, graph_id, kid, ops):
    return rt.submit_patch(
        GraphPatchProposal(
            graph_id=graph_id,
            expected_graph_version=rt.get_graph(graph_id).current_version,
            author_pid="p1",
            idempotency_key=kid,
            operations=ops,
        )
    )


def _chain(rt, graph_id, kids_prefix, node_ids):
    """Build the given Task nodes and commit initial depends_on chain.

    Edges: for consecutive (a, b), propose b depends_on a (edge source=b,
    target=a).  Returns nothing; side-effects the graph.
    """
    ops: list = []
    for n in node_ids:
        ops.append(
            AddNodeOp(
                node_id=n,
                graph_id=graph_id,
                node_type="task",
                created_by_pid="p1",
                title=n,
            )
        )
    # consecutive depends_on edges
    for i in range(len(node_ids) - 1):
        dependent = node_ids[i + 1]
        prereq = node_ids[i]
        ops.append(
            AddEdgeOp(
                edge_id=f"dep_{prereq}_{dependent}",
                edge_type="depends_on",
                source_node_id=dependent,
                target_node_id=prereq,
                created_by_pid="p1",
            )
        )
    _submit(rt, graph_id, f"{kids_prefix}_init", tuple(ops))


def _record(scenario_id, name, expected, verdict, evidence):
    AUDIT_RESULTS[scenario_id] = {
        "id": scenario_id,
        "step": 11,
        "name": name,
        "expected": expected,
        "verdict": verdict,
        "evidence": evidence,
    }


@pytest.fixture(autouse=True, scope="session")
def _dump_audit_results_after_session():
    yield
    import json
    from pathlib import Path

    scenarios = [AUDIT_RESULTS[k] for k in sorted(AUDIT_RESULTS)]
    surviving = [s for s in scenarios if s.get("verdict") == "RISK"]
    out = {
        "step": 11,
        "step_name": "SamePatchCycleAttack",
        "scenarios": scenarios,
        "overall_verdict": "RISK" if surviving else "PASS",
        "surviving_risks": [s["id"] for s in surviving],
    }
    path = (
        Path(__file__).resolve().parents[3]
        / "artifacts/agent_os_phase_d1_audit/step-11-cycle-attack.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(out, f, indent=2)


# ── S11a: self-loop ─────────────────────────────────────────────────────────
class TestS11a_SelfLoop:
    def test_self_loop_rejected(self):
        rt = _make_rt()
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _chain(rt, gid, "s11a", ["t1"])

        ver_before = rt.get_graph(gid).current_version
        before_edges = sorted(e.edge_id for e in rt.store.get_all_edges(gid))
        before_nodes = sorted(n.node_id for n in rt.store.get_all_nodes(gid))

        with pytest.raises(VPGError) as ei:
            _submit(rt, gid, "s11a_cycle", (
                AddEdgeOp(
                    edge_id="self_loop_t1",
                    edge_type="depends_on",
                    source_node_id="t1",
                    target_node_id="t1",
                    created_by_pid="p1",
                ),
            ))
        assert ei.value.code == _CYCLE_CODE

        # Graph unchanged by rejected patch
        assert rt.get_graph(gid).current_version == ver_before
        after_edges = sorted(e.edge_id for e in rt.store.get_all_edges(gid))
        after_nodes = sorted(n.node_id for n in rt.store.get_all_nodes(gid))
        assert after_edges == before_edges
        assert after_nodes == before_nodes

        _record(
            "S11a", "self_loop_rejected",
            "reject", "PASS",
            f"self-loop raised {ei.value.code.value}; "
            f"graph_version unchanged ({ver_before}); edge set unchanged",
        )


# ── S11b: direct reverse ─────────────────────────────────────────────────────
class TestS11b_DirectReverse:
    def test_direct_reverse_rejected(self):
        rt = _make_rt()
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _chain(rt, gid, "s11b", ["t1", "t2"])
        # committed: t2 depends_on t1
        ver_before = rt.get_graph(gid).current_version

        with pytest.raises(VPGError) as ei:
            _submit(rt, gid, "s11b_cycle", (
                AddEdgeOp(
                    edge_id="reverse_t1_t2",
                    edge_type="depends_on",
                    source_node_id="t1",
                    target_node_id="t2",
                    created_by_pid="p1",
                ),
            ))
        assert ei.value.code == _CYCLE_CODE
        assert rt.get_graph(gid).current_version == ver_before

        _record(
            "S11b", "direct_reverse_rejected",
            "reject", "PASS",
            f"reverse edge raised {ei.value.code.value}; "
            f"version unchanged ({ver_before})",
        )


# ── S11c: transitive triangle ────────────────────────────────────────────────
class TestS11c_TransitiveTriangle:
    def test_transitive_triangle_rejected(self):
        rt = _make_rt()
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _chain(rt, gid, "s11c", ["t1", "t2", "t3"])
        ver_before = rt.get_graph(gid).current_version

        with pytest.raises(VPGError) as ei:
            _submit(rt, gid, "s11c_cycle", (
                AddEdgeOp(
                    edge_id="tri_t1_t3",
                    edge_type="depends_on",
                    source_node_id="t1",
                    target_node_id="t3",
                    created_by_pid="p1",
                ),
            ))
        assert ei.value.code == _CYCLE_CODE
        assert rt.get_graph(gid).current_version == ver_before

        _record(
            "S11c", "transitive_triangle_rejected",
            "reject", "PASS",
            f"transitive triangle raised {ei.value.code.value}; "
            f"version unchanged ({ver_before})",
        )


# ── S11d: longer cycle ───────────────────────────────────────────────────────
class TestS11d_LongerCycle:
    def test_longer_cycle_rejected(self):
        rt = _make_rt()
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _chain(rt, gid, "s11d", ["t1", "t2", "t3", "t4"])
        ver_before = rt.get_graph(gid).current_version

        with pytest.raises(VPGError) as ei:
            _submit(rt, gid, "s11d_cycle", (
                AddEdgeOp(
                    edge_id="long_t1_t4",
                    edge_type="depends_on",
                    source_node_id="t1",
                    target_node_id="t4",
                    created_by_pid="p1",
                ),
            ))
        assert ei.value.code == _CYCLE_CODE
        assert rt.get_graph(gid).current_version == ver_before

        _record(
            "S11d", "longer_cycle_rejected",
            "reject", "PASS",
            f"longer cycle raised {ei.value.code.value}; "
            f"version unchanged ({ver_before})",
        )


# ── S11e: benign re-attach (idempotent-like, NOT a cycle) ────────────────────
class TestS11e_BenignReattach:
    def test_benign_reattach_rejected_or_idempotent(self):
        """Re-proposing t2 depends_on t1 with the SAME edge_id as the committed
        edge must either raise EDGE_ALREADY_EXISTS (new idempotency key) or be
        an idempotent replay (same idempotency key).  It must NOT create a
        duplicate edge and must NOT be falsely flagged as a cycle.
        """
        rt = _make_rt()
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _chain(rt, gid, "s11e", ["t1", "t2"])
        committed_edge_id = "dep_t1_t2"

        # Re-attach the SAME logical dependency with the SAME edge_id but a
        # DIFFERENT idempotency key — must raise EDGE_ALREADY_EXISTS.
        with pytest.raises(VPGError) as ei:
            _submit(rt, gid, "s11e_dup", (
                AddEdgeOp(
                    edge_id=committed_edge_id,
                    edge_type="depends_on",
                    source_node_id="t2",
                    target_node_id="t1",
                    created_by_pid="p1",
                ),
            ))
        assert ei.value.code == VPGCode.EDGE_ALREADY_EXISTS, (
            f"expected EDGE_ALREADY_EXISTS, got {ei.value.code}"
        )

        # The edge set must not have grown.
        dep_edges = [
            e for e in rt.store.get_all_edges(gid)
            if e.edge_type.value == "depends_on"
            and e.source_node_id == "t2"
            and e.target_node_id == "t1"
        ]
        assert len(dep_edges) == 1, f"expected 1 edge, got {len(dep_edges)}"

        _record(
            "S11e", "benign_reattach_idempotent",
            "reject/idempotent", "PASS",
            f"re-attach with same edge_id raised {ei.value.code.value}; "
            f"exactly one (t2->t1) depends_on edge exists (count={len(dep_edges)})",
        )


# ── S11f: self-loop alone (no-op patch) ──────────────────────────────────────
class TestS11f_SelfLoopAlone:
    def test_self_loop_alone_rejected(self):
        rt = _make_rt()
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _submit(rt, gid, "s11f_init", (
            AddNodeOp(
                node_id="t1", graph_id=gid, node_type="task",
                created_by_pid="p1", title="T1",
            ),
        ))
        ver_before = rt.get_graph(gid).current_version

        with pytest.raises(VPGError) as ei:
            _submit(rt, gid, "s11f_cycle", (
                AddEdgeOp(
                    edge_id="alone_loop",
                    edge_type="depends_on",
                    source_node_id="t1",
                    target_node_id="t1",
                    created_by_pid="p1",
                ),
            ))
        assert ei.value.code == _CYCLE_CODE
        assert rt.get_graph(gid).current_version == ver_before

        _record(
            "S11f", "self_loop_alone_rejected",
            "reject", "PASS",
            f"isolated self-loop raised {ei.value.code.value}; "
            f"version unchanged ({ver_before})",
        )
