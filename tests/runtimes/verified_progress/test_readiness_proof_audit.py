"""ReadinessProof Audit — Phase D1.1 Step 15.

Prove: when a task is in the READY frontier, the proof returned with it carries
the FULL justification AND ``proof.graph_version == current_graph_version_at_proof_time``.

Scenarios:
  S15a: t1 dep-less → READY; proof.graph_version == current_version.
  S15b: after a bumping patch, query_ready_frontier; proof_version == current.
  S15c: query returns no duplicates even after 20 calls interleaved with patches.
"""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    GraphPatchProposal,
)

AUDIT_RESULTS: dict[str, dict] = {}


def _make_rt():
    return VerifiedProgressRuntime(":memory:")


def _patch(rt, gid, kid, ops):
    return rt.submit_patch(
        GraphPatchProposal(
            graph_id=gid,
            expected_graph_version=rt.get_graph(gid).current_version,
            author_pid="p1",
            idempotency_key=kid,
            operations=ops,
        )
    )


def _ready(rt, gid):
    return rt.query_ready_frontier(gid)


def _record(sid, name, expected, verdict, evidence):
    AUDIT_RESULTS[sid] = {
        "id": sid, "step": 15, "name": name,
        "expected": expected, "verdict": verdict, "evidence": evidence,
    }


@pytest.fixture(autouse=True, scope="session")
def _dump_audit_results_after_session():
    yield
    import json
    from pathlib import Path
    scenarios = [AUDIT_RESULTS[k] for k in sorted(AUDIT_RESULTS)]
    surviving = [s for s in scenarios if s.get("verdict") == "RISK"]
    out = {
        "step": 15, "step_name": "ReadinessProofAudit",
        "scenarios": scenarios,
        "overall_verdict": "RISK" if surviving else "PASS",
        "surviving_risks": [s["id"] for s in surviving],
    }
    path = (
        Path(__file__).resolve().parents[3]
        / "artifacts/agent_os_phase_d1_audit/step-15-readiness-proof.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(out, f, indent=2)


class TestS15a_ProofVersionMatchesCurrent:
    def test_proof_version_matches_current(self):
        rt = _make_rt()
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(rt, gid, "init", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T1"),
        ))

        candidates = _ready(rt, gid)
        assert len(candidates) == 1
        c = candidates[0]
        current = rt.get_graph(gid).current_version
        assert c.readiness_proof.graph_version == current, (
            f"proof.graph_version={c.readiness_proof.graph_version} != current={current}"
        )
        assert c.readiness_proof.task_id == "t1"
        assert c.readiness_proof.lifecycle_ok is True
        assert c.readiness_proof.validity_ok is True
        assert c.readiness_proof.all_deps_verified is True
        _record("S15a", "proof_version_matches_current", "PASS", "PASS",
                f"proof.graph_version={c.readiness_proof.graph_version} == current={current}")


class TestS15b_ProofVersionAfterBump:
    def test_proof_version_after_bumping_patch(self):
        rt = _make_rt()
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(rt, gid, "init", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T1"),
        ))

        # Add an unrelated task to bump the graph version
        _patch(rt, gid, "bump", (
            AddNodeOp(node_id="aux", graph_id=gid, node_type="task", created_by_pid="p1", title="Aux"),
        ))

        candidates = _ready(rt, gid)
        current = rt.get_graph(gid).current_version
        versions = sorted({c.readiness_proof.graph_version for c in candidates})
        assert versions == [current], (
            f"expected all proof versions == {current}, got {versions}"
        )
        _record("S15b", "proof_version_after_bump", "PASS", "PASS",
                f"after bump: all proof.graph_version == current={current}")


class TestS15c_NoDuplicates:
    def test_no_duplicates_after_20_calls_interleaved(self):
        rt = _make_rt()
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(rt, gid, "init", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T1"),
            AddNodeOp(node_id="t2", graph_id=gid, node_type="task", created_by_pid="p1", title="T2"),
            AddEdgeOp(edge_id="e1", edge_type="depends_on", source_node_id="t2",
                      target_node_id="t1", created_by_pid="p1"),
        ))
        for i in range(20):
            cands = _ready(rt, gid)
            ids = [c.task_id for c in cands]
            assert len(ids) == len(set(ids)), f"duplicates in call #{i}: {ids}"
            if i % 5 == 0:
                # Interleave a bumping patch — adds a uniquely-named aux task
                _patch(rt, gid, f"bump_{i}", (
                    AddNodeOp(node_id=f"aux_{i}", graph_id=gid, node_type="task", created_by_pid="p1"),
                ))
        # After interleaving patches, the frontier has grown — but still must
        # contain t1 and must contain no duplicates within any single call.
        final = sorted(c.task_id for c in _ready(rt, gid))
        assert len(final) == len(set(final)), f"final frontier has duplicates: {final}"
        assert "t1" in final
        assert all(f"aux_{i}" in final for i in range(0, 20, 5))
        _record("S15c", "no_duplicates_interleaved", "PASS", "PASS",
                f"20 calls interleaved with bumps; no duplicates in any call; "
                f"final frontier size={len(final)}")
