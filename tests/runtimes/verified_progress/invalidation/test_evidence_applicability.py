"""D3 semantics tests — Evidence applicability + old-EVD-immutability + seeds."""

from __future__ import annotations

from lhos.runtimes.invalidation.evidence import (
    evidence_applicability_for_graph,
)
from lhos.runtimes.invalidation.models import InvalidationCause

from .helpers import Bound, FNode


# ---- §4 / §33: EVD stays immutable, applicability is derived ----
def test_evidence_history_is_immutable(run_engine, cause):
    """Old Evidence must NOT be mutated by invalidation."""
    evidence = {"E7": FNode("E7", artifact_bindings=(Bound("X", 7),))}
    # Snapshot hash of the historical evidence row.
    ev_before = (
        evidence["E7"].artifact_bindings[0].version,
        evidence["E7"].artifact_bindings[0].content_hash,
    )

    tasks = {}
    res = run_engine(
        evidence_nodes=evidence,
        current_output_versions={"X": 8},
        explicit_causes=(cause(artifact_id="X", old_version=7, new_version=8),),
    )

    # historical row unchanged after invalidation run
    ev_after = (
        evidence["E7"].artifact_bindings[0].version,
        evidence["E7"].artifact_bindings[0].content_hash,
    )
    assert ev_before == ev_after, "Evidence history was mutated"
    assert evidence["E7"].artifact_bindings[0].version == 7


# ---- §4: v7 evidence cannot verify v8 ----
def test_old_evidence_cannot_verify_new_version():
    """v7-bundled Evidence loses applicability when artifact is at v8."""
    evidence = {"E7": FNode("E7", artifact_bindings=(Bound("X", 7),))}
    verds = evidence_applicability_for_graph("g", 2, evidence, current_output_versions={"X": 8})
    v = next(a for a in verds if a.evidence_id == "E7")
    assert v.applies is False
    assert "superseded" in v.reason


# ---- §5 Seed A: current task output version changed ----
def test_seed_a_output_version_changed(run_engine, cause):
    from .helpers import TNode

    tasks = {"T": TNode("T", "verified")}
    res = run_engine(
        tasks=tasks,
        edges=[],
        explicit_causes=(cause(source_node_id="T", artifact_id="X", old_version=7, new_version=8),),
    )
    assert "T" in res.stale_nodes
    assert res.frontier.candidates[0].task_id == "T"


# ---- §5 Seed B: evidence backing artifact corrupted ----
def test_seed_b_backing_artifact_corrupt(run_engine, cause):
    """If the backing content hash no longer validates, EVD loses
    applicability, producing task STALE."""
    evidence = {"E7": FNode("E7", artifact_bindings=(Bound("X", 7, "h7"),))}
    # verify_binding returns False => artifact corrupted
    from lhos.runtimes.invalidation.evidence import (
        evidence_applicability_for_graph,
    )

    verds = evidence_applicability_for_graph(
        "g",
        1,
        evidence,
        verify_binding=lambda aid, ver, hsh: False,
    )
    v = next(a for a in verds if a.evidence_id == "E7")
    assert v.applies is False
    assert "invalid" in v.reason


# ---- §5 Seed C: source action invalid ----
def test_seed_c_source_action_invalid():
    evidence = {
        "E7": FNode("E7", artifact_bindings=(), source_action_id="act-1"),
    }
    verds = evidence_applicability_for_graph(
        "g", 1, evidence, action_valid=lambda aid: aid != "act-1"
    )
    v = next(a for a in verds if a.evidence_id == "E7")
    assert v.applies is False
    assert "action" in v.reason


# ---- §6: cause enum closed ----
def test_invalidation_cause_types_closed():
    for t in (
        "ARTIFACT_VERSION_SUPERSEDED",
        "EVIDENCE_ARTIFACT_INVALID",
        "SOURCE_ACTION_INVALID",
        "SOURCE_EVENT_INVALID",
    ):
        c = InvalidationCause(cause_id="x", graph_id="g", graph_version=1, cause_type=t, reason="r")
        assert c.cause_type == t
