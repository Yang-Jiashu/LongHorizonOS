"""Dispatch ordering and agent context-residency tests.

These cover the two dispatch inputs that are *not* derivable from the graph:

* ``dispatch_order`` — an upstream policy's ranking must be able to decide who
  a capacity-bounded pass offers a Claim to first, not merely which tasks are
  admissible.  A filter alone can subtract candidates but never promote a
  critical-path task ahead of the graph's static order.
* context residency — which agent has already read a resource.  Agent context
  is path-dependent, so a graph-derived readiness proof cannot carry it.

The namespace test is the load-bearing one: provenance records a raw
``resource_uri`` while graph-side declarations are often bare artifact ids.  If
those two key spaces stop meeting, the locality bonus silently becomes dead
code again, which is exactly the failure this suite exists to prevent.
"""

from __future__ import annotations

from datetime import timedelta

from lhos.runtimes.multi_agent.models import AgentSnapshot, ComputationCost, ResourceBinding
from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler


def _two_agents() -> dict[str, dict]:
    # a1 is cheaper, so it wins every tie unless another term overrides it.
    return {
        "a1": {
            "supported_task_kinds": ("*",),
            "specializations": ("python",),
            "max_concurrency": 5,
            "cost_weight": 100,
        },
        "a2": {
            "supported_task_kinds": ("*",),
            "specializations": ("python",),
            "max_concurrency": 5,
            "cost_weight": 200,
        },
    }


def _make_resident(sch, vpg, *, agent_id: str, reads: tuple[ResourceBinding, ...]) -> None:
    """Give one agent a durable read-set by completing a warm-up dispatch."""

    vpg.add_ready_task("warmup", metadata_extra={"sdk": {"agent": agent_id}})
    res = sch.schedule_once(vpg.graph_id)
    assert [d["agent_id"] for d in res.dispatched] == [agent_id]
    claim = sch.active_claim_for_task("warmup", vpg.graph_id)
    assert claim is not None
    attempt = sch.attempt_for_claim(claim.claim_id)
    assert attempt is not None
    snapshot = AgentSnapshot.from_attempt(
        attempt,
        progress=1.0,
        cost=ComputationCost(input_tokens=10, elapsed_ms=10),
        captured_at=attempt.started_at + timedelta(seconds=1),
    ).model_copy(update={"read_set": reads})
    assert sch.bind_agent_snapshot(claim.claim_id, snapshot)


def test_dispatch_order_decides_who_a_bounded_pass_admits() -> None:
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1")
    vpg.add_ready_task("t2")

    res = sch.schedule_once(vpg.graph_id, max_claims=1, dispatch_order=("t2", "t1"))

    assert [d["task_id"] for d in res.dispatched] == ["t2"]
    assert res.dispatch_order_applied == ("t2", "t1")


def test_default_pass_keeps_graph_order_when_no_ranking_is_supplied() -> None:
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1")
    vpg.add_ready_task("t2")

    res = sch.schedule_once(vpg.graph_id, max_claims=1)

    assert [d["task_id"] for d in res.dispatched] == ["t1"]
    assert res.dispatch_order_applied == ()


def test_dispatch_order_cannot_admit_a_task_outside_the_frontier() -> None:
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1")

    res = sch.schedule_once(vpg.graph_id, max_claims=2, dispatch_order=("not-ready", "t1"))

    assert [d["task_id"] for d in res.dispatched] == ["t1"]


def test_context_residency_overrides_the_cheaper_agent() -> None:
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    _make_resident(
        sch,
        vpg,
        agent_id="a2",
        reads=(
            ResourceBinding(
                operation="read",
                resource_uri="workspace://shared.py",
                artifact_id="shared.py",
                version=1,
            ),
        ),
    )

    vpg.add_ready_task("consumer")
    res = sch.schedule_once(
        vpg.graph_id,
        max_claims=1,
        dispatch_order=("consumer",),
        task_read_keys_by_task={"consumer": ("workspace://shared.py",)},
    )

    assert [(d["task_id"], d["agent_id"]) for d in res.dispatched] == [("consumer", "a2")]
    assert res.locality_matched == ("consumer",)


def test_declaration_matches_residency_across_uri_and_artifact_id_forms() -> None:
    """Provenance keeps ``workspace://shared.py``; declarations say ``shared.py``."""

    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    _make_resident(
        sch,
        vpg,
        agent_id="a2",
        reads=(
            ResourceBinding(
                operation="read",
                resource_uri="workspace://shared.py",
                artifact_id="shared.py",
                version=1,
            ),
        ),
    )

    vpg.add_ready_task("consumer")
    res = sch.schedule_once(
        vpg.graph_id,
        max_claims=1,
        task_read_keys_by_task={"consumer": ("shared.py",)},
    )

    assert [(d["task_id"], d["agent_id"]) for d in res.dispatched] == [("consumer", "a2")]
    assert res.locality_matched == ("consumer",)


def test_undeclared_read_earns_no_locality_bonus() -> None:
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    _make_resident(
        sch,
        vpg,
        agent_id="a2",
        reads=(
            ResourceBinding(
                operation="read",
                resource_uri="workspace://shared.py",
                artifact_id="shared.py",
                version=1,
            ),
        ),
    )

    vpg.add_ready_task("consumer")
    res = sch.schedule_once(vpg.graph_id, max_claims=1)

    assert [(d["task_id"], d["agent_id"]) for d in res.dispatched] == [("consumer", "a1")]
    assert res.locality_matched == ()


def test_residency_of_a_different_resource_does_not_transfer() -> None:
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    _make_resident(
        sch,
        vpg,
        agent_id="a2",
        reads=(
            ResourceBinding(
                operation="read",
                resource_uri="workspace://other.py",
                artifact_id="other.py",
                version=1,
            ),
        ),
    )

    vpg.add_ready_task("consumer")
    res = sch.schedule_once(
        vpg.graph_id,
        max_claims=1,
        task_read_keys_by_task={"consumer": ("shared.py",)},
    )

    assert [(d["task_id"], d["agent_id"]) for d in res.dispatched] == [("consumer", "a1")]
    assert res.locality_matched == ()
