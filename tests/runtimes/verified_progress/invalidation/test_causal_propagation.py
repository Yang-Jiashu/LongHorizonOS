"""D3 causal propagation semantics — over/under-invalidation, diamond,
multiple-dependency, branch preservation (§9-§12, §33)."""

from __future__ import annotations

from .helpers import TNode, depends_on


# ---- §9: over-invalidation forbidden (independent branch preserved) ----
def test_over_invalidation_independent_branch(run_engine, cause):
    """                   T1
            /            \
          T2(A)        T3(B)
          |             |
          T4           T5

    A v1->v2 only touches the T2 branch.  T3/T5 must stay VERIFIED.
    """
    tasks = {
        "T1": TNode("T1", "verified"),
        "T2": TNode("T2", "verified"),
        "T3": TNode("T3", "verified"),
        "T4": TNode("T4", "verified"),
        "T5": TNode("T5", "verified"),
    }
    # VPG semantics: S -depends_on-> T means S depends on T.
    # T1 depends on T2 and T3; T2 depends on T4; T3 depends on T5.
    edges = [
        depends_on("T1", "T2"),
        depends_on("T1", "T3"),
        depends_on("T2", "T4"),
        depends_on("T3", "T5"),
    ]
    c = cause(source_node_id="T2", artifact_id="A", old_version=1, new_version=2)
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))

    # T2's producing evidence lost -> T2 stale; T1 (depends on T2) stale.
    assert set(res.stale_nodes) == {"T1", "T2"}, res.stale_nodes
    # T3/T4/T5 untouched.
    assert set(res.preserved_nodes) == {"T3", "T4", "T5"}, res.preserved_nodes


# ---- §10: under-invalidation forbidden (downstream must follow) ----
def test_under_invalidation_chain(run_engine, cause):
    """T1 -> T2 -> T3 (T1 depends_on T2 depends_on T3?) Use a real chain.

    VPG: S depends_on T means 'S depends on T'; so if T1-first is the seed,
    we compose edges T3->T2->T1 so that T1 depends on T2 and T2 depends on T3.
    """
    tasks = {
        "T1": TNode("T1", "verified"),
        "T2": TNode("T2", "verified"),
        "T3": TNode("T3", "verified"),
    }
    # T1 depends on T2; T2 depends on T3.  Seed T3 (deepest) -> T2,T1 all stale.
    edges = [depends_on("T1", "T2"), depends_on("T2", "T3")]
    c = cause(source_node_id="T3")
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))
    assert set(res.stale_nodes) == {"T1", "T2", "T3"}, res.stale_nodes
    # Frontier must be minimal: only deepest T3 repairable (its deps empty).
    assert [x.task_id for x in res.frontier.candidates] == ["T3"]


# ---- §11: multiple dependencies — ANY non-verified dep blocks VERIFIED ----
def test_multiple_dependency_any_stale_blocked(run_engine, cause):
    """T3 depends_on T1 and T2.  If T1 stale -> T3 cannot stay VERIFIED."""
    tasks = {
        "T1": TNode("T1", "verified"),
        "T2": TNode("T2", "verified"),
        "T3": TNode("T3", "verified"),
    }
    edges = [depends_on("T3", "T1"), depends_on("T3", "T2")]
    c = cause(source_node_id="T1")
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))
    assert set(res.stale_nodes) == {"T1", "T3"}, res.stale_nodes
    assert "T2" in res.preserved_nodes


# ---- §12: diamond — one leg stays VERIFIED ----
def test_diamond_dependency(run_engine, cause):
    """       T1
        /    \
      T2    T3
            /
         T4

    Edge semantics: T1->T2 not needed here; T2 and T3 both depend on T1.
    T4 depends on T2 and T3.
    If T2 stale -> T4 stale, but T3 remains VERIFIED, and so does the
    {T3->?} no reverse infection.
    """
    tasks = {
        "T1": TNode("T1", "verified"),
        "T2": TNode("T2", "verified"),
        "T3": TNode("T3", "verified"),
        "T4": TNode("T4", "verified"),
    }
    # T2 depends on T1; T3 depends on T1; T4 depends on T2 AND T3.
    edges = [
        depends_on("T2", "T1"),
        depends_on("T3", "T1"),
        depends_on("T4", "T2"),
        depends_on("T4", "T3"),
    ]
    # Seed T2 (its own output changed): T2 stale, T4 stale; T3 preserved; T1 preserved.
    c = cause(source_node_id="T2")
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))
    assert set(res.stale_nodes) == {"T2", "T4"}, res.stale_nodes
    assert set(res.preserved_nodes) == {"T1", "T3"}, res.preserved_nodes


# ---- Reverse infection prohibited ----
def test_no_reverse_infection(run_engine, cause):
    """A downstream stale node must NOT invalidate its (already-verified)
    upstream dependency.  Propagation only follows depends_on direction."""
    tasks = {
        "T1": TNode("T1", "verified"),
        "T2": TNode("T2", "verified"),
    }
    # T2 depends_on T1.  Seed T2 (downstream) -> only T2 stale, T1 preserved.
    edges = [depends_on("T2", "T1")]
    c = cause(source_node_id="T2")
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))
    assert set(res.stale_nodes) == {"T2"}, res.stale_nodes
    assert set(res.preserved_nodes) == {"T1"}, res.preserved_nodes


def test_unverified_sibling_not_promoted_to_stale(run_engine, cause):
    """The VERIFIED-guard (§9) must never turn an UNVERIFIED task into STALE.
    Seed T1: its only dependent T2 (VERIFIED) becomes STALE, but an UNVERIFIED
    task T3 (that depends on T1 but is NOT verified) must not be promoted to
    STALE — STALE means 'was verified, then invalidated'."""
    tasks = {
        "T1": TNode("T1", "verified"),
        "T2": TNode("T2", "verified"),  # verified dependent of T1
        "T3": TNode("T3", "unverified"),  # dependent of T1, but never verified
    }
    edges = [depends_on("T2", "T1"), depends_on("T3", "T1")]
    c = cause(source_node_id="T1")
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))
    # T3 stays UNVERIFIED/preserved — it is NOT promoted to STALE.
    assert "T3" in res.preserved_nodes, "unverified dependent wrongly promoted to stale"


def test_unverified_neighbor_not_invalidated(run_engine, cause):
    """Even if the mutation removed the VERIFIED guard, an UNVERIFIED task that
    is NOT a causal dependent must never enter the affected set."""
    tasks = {
        "T1": TNode("T1", "verified"),
        "T9": TNode("T9", "unverified"),
    }
    res = run_engine(tasks=tasks, edges=[], explicit_causes=(cause(source_node_id="T1"),))
    assert "T1" in res.stale_nodes
    assert "T9" in res.preserved_nodes, "unrelated neighbor invalidated"
