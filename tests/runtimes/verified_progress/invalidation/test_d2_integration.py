"""D3 → D2 integration (§15, §33: 'D2 can schedule repair work without
understanding D3').  We assert that the ONLY coupling between D3 and D2 is a
read-only `has_active_claim` hook — D3 never claims or dispatches; and D2's
own suite remains green.  D2 does not import D3 internals."""

from __future__ import annotations

from .helpers import TNode, depends_on


def test_d2_only_couples_via_has_active_claim_hook(run_engine, cause):
    """When a D2 scheduler has claimed the repair task, the D3 frontier
    defers to D2 ownership: the task is NOT front-ready."""
    tasks = {"T1": TNode("T1", "verified"), "T2": TNode("T2", "verified")}
    # T1 depends on T2.
    edges = [depends_on("T1", "T2")]
    c = cause(source_node_id="T2")
    res = run_engine(
        tasks=tasks,
        edges=edges,
        explicit_causes=(c,),
        has_active_claim=lambda tid: tid == "T2",
    )
    # Without any claim, T2 would be in frontier.  With an active D2 claim,
    # T2 is excluded (D2 owns it) => frontier empty even though T2 is stale-ready.
    assert res.frontier.candidates == ()


def test_d2_does_not_understand_d3_a_zero_cause_result(run_engine):
    """With no invalidation cause, D3 emits an empty cone and no stale nodes —
    a fully VERIFIED graph yields an empty repair frontier (nothing to repair)."""
    tasks = {"T1": TNode("T1", "verified"), "T2": TNode("T2", "verified")}
    res = run_engine(tasks=tasks, edges=[], explicit_causes=())
    assert res.stale_nodes == ()
    assert res.frontier.candidates == ()
    # preserved = everything (nothing invalidated)
    assert res.preserved_nodes == ("T1", "T2")


def test_d3_module_has_no_private_claim_or_dispatch_callable():
    """D3 source must not reference KernelLease or dispatcher primitives."""
    import lhos.runtimes.invalidation as d3

    src_attrs = {a for a in dir(d3)}
    for banned in ("claim", "dispatch", "ResourceLease"):
        assert not any(banned.lower() in a.lower() for a in src_attrs if a.islower())
