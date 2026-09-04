"""Step 21 — GraphVersion Semantics Audit.

Proves that GraphVersion advances by exactly ONE per *applied* patch and that
invalid / conflicted / idempotency-replayed patches do not advance the
version counter and do not introduce gaps.

Procedure:
  * commit 1,000 valid patches  -> expect [] version == 1,000
  * attempt 1,001st patch with an invalid op       -> expect REJECTED, version unchanged
  * submit 100 further valid patches                -> expect [] version == 1,100
  * submit a patch with expected_graph_version == 0 (conflict) -> expect GRAPH_VERSION_CONFLICT
  * replay 100 previously-committed patches (idempotent)      -> expect version unchanged
  * confirm [] version is contiguous 0..N with no gaps
  * save graph-version-audit.json
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.patches import (
    AddNodeOp,
    GraphPatchProposal,
)

AUDIT_RESULTS: dict[str, dict] = {}
STEP = 21

# Magnitudes
N_VALID_BATCH_1 = 1000
N_INVALID = 100
N_VALID_BATCH_2 = 100
N_CONFLICT = 100
N_IDEMPOTENT = 100


@pytest.fixture(autouse=True, scope="session")
def _dump_results():
    yield
    _write()


def _write():
    out = {
        "step": STEP, "step_name": "GraphVersionSemantics",
        "scenarios": [AUDIT_RESULTS[k] for k in sorted(AUDIT_RESULTS)],
        "surviving_risks": [s["id"] for s in AUDIT_RESULTS.values() if s["verdict"] == "RISK"],
        "overall_verdict": "RISK" if any(
            s["verdict"] == "RISK" for s in AUDIT_RESULTS.values()
        ) else "PASS",
    }
    p = Path(__file__).resolve().parents[3] / "artifacts/agent_os_phase_d1_audit/step-21-graph-version.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(out, indent=2))


def _record(sid, name, expected, verdict, evidence, **extra):
    AUDIT_RESULTS[sid] = {
        "id": sid, "step": STEP, "name": name,
        "expected": expected, "actual": verdict, "verdict": verdict,
        "evidence": evidence, **extra,
    }


def _valid_patch(rt, gid, version, key_prefix, idx):
    return GraphPatchProposal(
        graph_id=gid, expected_graph_version=version, author_pid="p1",
        idempotency_key=f"{key_prefix}-{idx}",
        operations=(AddNodeOp(
            node_id=f"node-{key_prefix}-{idx}", graph_id=gid, node_type="task",
            created_by_pid="p1", title=f"N-{idx}",
        ),),
    )


class TestS21_GraphVersionSemantics:
    def test_graph_version_advances_and_is_contiguous(self):
        rt = VerifiedProgressRuntime(":memory:")
        rec = rt.create_graph(owner_pid="p1"); gid = rec.graph_id

        # Phase 1: 1,000 valid patches
        committed_keys = []
        for i in range(N_VALID_BATCH_1):
            p = _valid_patch(rt, gid, rt.get_graph(gid).current_version, "batch1", i)
            rt.submit_patch(p)
            committed_keys.append(p.idempotency_key)
        v_after_batch1 = rt.get_graph(gid).current_version
        assert v_after_batch1 == N_VALID_BATCH_1, (
            f"after {N_VALID_BATCH_1} valid patches, version should be {N_VALID_BATCH_1}; got {v_after_batch1}"
        )
        _record("S21_batch1", "valid_1000_advance", "PASS", "PASS",
                f"version == {v_after_batch1} after 1,000 valid commits")

        # Phase 2: 100 invalid patches → each REJECTED, version unchanged
        ver_before_invalid = rt.get_graph(gid).current_version
        invalid_count = 0
        for i in range(N_INVALID):
            bad = GraphPatchProposal(
                graph_id=gid, expected_graph_version=rt.get_graph(gid).current_version,
                author_pid="p1", idempotency_key=f"invalid-{i}",
                operations=(),  # PATCH_EMPTY
            )
            try:
                rt.submit_patch(bad)
                invalid_count += 1
            except VPGError as e:
                assert e.code == VPGCode.PATCH_EMPTY
        assert invalid_count == 0, "no invalid (empty) patch should have applied"
        ver_after_invalid = rt.get_graph(gid).current_version
        assert ver_after_invalid == ver_before_invalid, (
            f"invalid patches must not advance version: before={ver_before_invalid} after={ver_after_invalid}"
        )
        _record("S21_invalid", "invalid_100_rejected", "PASS", "PASS",
                f"{N_INVALID} empty patches rejected with PATCH_EMPTY; version unchanged at {ver_after_invalid}")

        # Phase 3: 100 additional valid patches
        for i in range(N_VALID_BATCH_2):
            p = _valid_patch(rt, gid, rt.get_graph(gid).current_version, "batch2", i)
            rt.submit_patch(p)
            committed_keys.append(p.idempotency_key)
        v_after_batch2 = rt.get_graph(gid).current_version
        expected_v = N_VALID_BATCH_1 + N_VALID_BATCH_2
        assert v_after_batch2 == expected_v, (
            f"after {N_VALID_BATCH_1}+{N_VALID_BATCH_2} valid patches, version should be {expected_v}; got {v_after_batch2}"
        )
        _record("S21_batch2", "valid_100_more", "PASS", "PASS",
                f"version == {v_after_batch2} after 1,100 valid commits")

        # Phase 4: 100 conflicting patches (stale expected_graph_version)
        conflict_count = 0
        for i in range(N_CONFLICT):
            stale = GraphPatchProposal(
                graph_id=gid, expected_graph_version=0, author_pid="p1",
                idempotency_key=f"stale-{i}",
                operations=(AddNodeOp(
                    node_id=f"stale-node-{i}", graph_id=gid, node_type="task",
                    created_by_pid="p1", title=f"S-{i}",
                ),),
            )
            try:
                rt.submit_patch(stale)
            except VPGError as e:
                assert e.code == VPGCode.GRAPH_VERSION_CONFLICT, (
                    f"expected GRAPH_VERSION_CONFLICT, got {e.code}"
                )
                conflict_count += 1
        v_after_conflict = rt.get_graph(gid).current_version
        assert v_after_conflict == v_after_batch2, (
            f"conflict patches must not advance version: before={v_after_batch2} after={v_after_conflict}"
        )
        assert conflict_count == N_CONFLICT
        _record("S21_conflict", "conflict_100_rejected", "PASS", "PASS",
                f"{N_CONFLICT} stale-version patches rejected with GRAPH_VERSION_CONFLICT; version unchanged")

        # Phase 5: 100 idempotent replays of already-committed patches
        ver_before_idem = rt.get_graph(gid).current_version
        idem_keys = committed_keys[:N_IDEMPOTENT]
        idem_replays = 0
        for kid in idem_keys:
            # Replay: minimal same-op patch
            replay = GraphPatchProposal(
                graph_id=gid, expected_graph_version=rt.get_graph(gid).current_version,
                author_pid="p1", idempotency_key=kid,
                operations=(AddNodeOp(
                    node_id=f"replay-{kid}", graph_id=gid, node_type="task",
                    created_by_pid="p1", title="R",
                ),),
            )
            r = rt.submit_patch(replay)
            if r.idempotent_replay and not r.patch_applied:
                idem_replays += 1
        v_after_idem = rt.get_graph(gid).current_version
        assert v_after_idem == ver_before_idem, (
            f"idempotent replays must not advance version: before={ver_before_idem} after={v_after_idem}"
        )
        _record("S21_idempotent", "idempotent_100_replays", "PASS", "PASS",
                f"{idem_replays}/{N_IDEMPOTENT} key replays detected; version unchanged at {v_after_idem}")

        # Phase 6: contiguity check — every version 1..N has a graph_versions row
        all_versions = [
            row[0] for row in rt.store.conn.execute(
                "SELECT version FROM graph_versions WHERE graph_id=? ORDER BY version", (gid,)
            ).fetchall()
        ]
        expected = list(range(v_after_idem + 1))  # 0..N
        assert all_versions == expected, (
            f"graph_versions must be contiguous 0..{v_after_idem}; "
            f"missing={set(expected) - set(all_versions)} extra={set(all_versions) - set(expected)}"
        )
        _record("S21_contiguity", "version_range_contiguous", "PASS", "PASS",
                f"versions contiguous 0..({v_after_idem}); {v_after_idem+1} rows in graph_versions")

        # Persist audit snapshot
        snapshot = {
            "final_graph_version": v_after_idem,
            "total_graph_version_rows": v_after_idem + 1,
            "valid_commits": N_VALID_BATCH_1 + N_VALID_BATCH_2,
            "invalid_rejected": N_INVALID,
            "conflict_rejected": N_CONFLICT,
            "idempotent_replays": idem_replays,
            "contiguous": True,
        }
        snap_path = Path(__file__).resolve().parents[3] / "artifacts/agent_os_phase_d1_audit/graph-version-audit.json"
        snap_path.parent.mkdir(parents=True, exist_ok=True)
        snap_path.write_text(json.dumps(snapshot, indent=2))
