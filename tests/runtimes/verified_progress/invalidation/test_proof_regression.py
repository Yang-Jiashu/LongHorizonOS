"""§32 regression for the D3.1 multi-seed proof defect.

D3-I5 requires: every stale semantic descendant has a valid causal path from
a root invalidation cause.  Earlier, build_proofs ran its BFS over
reverse_deps (dependents), so for a dependent task whose STALENESS comes from
an *upstream* dependency, it never found the root seed (empty root_causes) and
emitted a wrong causal_path.  This test reproduces it.
"""

from __future__ import annotations

from .helpers import TNode, depends_on


def _proof_ok(res, affected_task):
    """Every stale node must have >=1 root_cause AND a causal_path that ends at
    a root seed."""
    for p in res.proofs:
        if p.task_id == affected_task:
            return bool(p.root_causes) and p.causal_path and p.causal_path[-1] == p.task_id
    return False


def test_multi_seed_proof_has_root_cause(run_engine, cause):
    """A dependent of a seeded node must get a proof whose root_causes is
    non-empty and whose causal_path terminates at that dependent."""
    from .helpers import TNode, depends_on

    # two independent seeds A0 and B0, each with a dependent chain
    tasks = {
        "A0": TNode("A0"),
        "A1": TNode("A1"),  # A1 depends on A0
        "B0": TNode("B0"),
        "B1": TNode("B1"),  # B1 depends on B0
    }
    edges = [depends_on("A1", "A0"), depends_on("B1", "B0")]
    c = tuple(cause(graph_version=1, source_node_id=s) for s in ("A0", "B0"))
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=c)
    # A1 and B1 are stale dependents; each needs a root_cause proof.
    a1 = next(p for p in res.proofs if p.task_id == "A1")
    assert a1.root_causes, f"A1 proof missing root cause: path={a1.causal_path}"
    assert _proof_ok(res, "A1")
    assert _proof_ok(res, "B1")


def test_deep_chain_proof_path_reaches_seed(run_engine, cause):
    """T1->T2->T3 (T2 depends on T1, T3 depends on T2); seed T1.  T3's proof
    causal_path must rank T1 first (root) and T3 last."""
    tasks = {
        "T1": TNode("T1"),
        "T2": TNode("T2"),
        "T3": TNode("T3"),
    }
    edges = [depends_on("T2", "T1"), depends_on("T3", "T2")]
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=(cause(source_node_id="T1"),))
    t3 = next(p for p in res.proofs if p.task_id == "T3")
    assert "T1" in t3.causal_path, f"T3 proof path must include root T1: {t3.causal_path}"
    assert t3.root_causes, f"T3 proof missing root cause: {t3.causal_path}"


def test_proof_path_terminates_at_affected_node(run_engine, cause):
    """The last element of causal_path must be the affected task itself."""
    tasks = {"A0": TNode("A0"), "A1": TNode("A1")}
    edges = [depends_on("A1", "A0")]
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=(cause(source_node_id="A0"),))
    for p in res.proofs:
        assert p.causal_path and p.causal_path[-1] == p.task_id, f"{p.task_id}"
