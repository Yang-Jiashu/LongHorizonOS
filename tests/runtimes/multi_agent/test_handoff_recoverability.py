"""What actually happens when a handoff's replacement cannot be admitted.

``handoff_task`` is documented as deliberately non-atomic: the source lease is
released first, then a replacement is admitted through the normal gates, and a
failed admission returns ``FAILED_CLOSED`` with neither claim active.  That
"no owner" state has been treated as the reason live REBASE is refused.

These tests characterise how severe it really is, because the answer decides
whether an atomic handoff is a *correctness* fix or a *liveness* one:

* if the task becomes permanently stuck, the non-atomicity is a real defect;
* if a normal scheduling pass simply re-dispatches it, the loss is one wasted
  attempt and the safety invariant (never two owners) was never at risk.

**Measured result, and it narrows the concern:** an unregistered replacement is
rejected by a *pre-flight* check with status ``REFUSED``, before anything is
released -- the source claim stays ACTIVE and owned.  So the "no owner" window is
not reachable this way.

**What these tests therefore do NOT cover:** the actual dangerous window, which
opens only when a replacement passes pre-validation and then fails at the later
admission gate (resource exhaustion, a lease race, or a graph-version change
between release and acquire).  Reaching that state needs admission to be failed
mid-handoff, which these tests do not do.  Do not read a pass here as evidence
that the post-release window is safe.

Measure before rewriting the most audited invariant in the codebase.
"""

from __future__ import annotations

from lhos.runtimes.multi_agent import ClaimState
from lhos.runtimes.multi_agent.models import ClaimHandoffStatus
from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler


def _agents() -> dict[str, dict]:
    return {
        "a1": {
            "supported_task_kinds": ("*",),
            "specializations": ("python",),
            "max_concurrency": 5,
        },
        "a2": {
            "supported_task_kinds": ("*",),
            "specializations": ("python",),
            "max_concurrency": 5,
        },
    }


def _dispatch_one(sch, vpg):
    vpg.add_ready_task("t1", required_specializations=("python",))
    result = sch.schedule_once(vpg.graph_id)
    assert [item["task_id"] for item in result.dispatched] == ["t1"]
    claim = sch.active_claim_for_task("t1", vpg.graph_id)
    assert claim is not None
    attempt = sch.attempt_for_claim(claim.claim_id)
    assert attempt is not None
    return claim, attempt


def test_unregistered_replacement_is_refused_before_anything_is_released() -> None:
    vpg = FakeVPG()
    sch = fake_scheduler(_agents(), fake_vpg=vpg)
    claim, attempt = _dispatch_one(sch, vpg)

    result = sch.handoff_task(
        vpg.graph_id,
        "t1",
        source_claim_id=claim.claim_id,
        # An unregistered agent can never be admitted, so admission must fail.
        replacement_agent_id="not-registered",
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch,
    )

    # Pre-flight validation, so nothing was released and the owner is intact.
    assert result.status is ClaimHandoffStatus.REFUSED
    surviving = sch.active_claim_for_task("t1", vpg.graph_id)
    assert surviving is not None
    assert surviving.claim_id == claim.claim_id
    assert surviving.agent_id == claim.agent_id


def test_the_safety_invariant_holds_two_owners_never_coexist() -> None:
    """This is the invariant that would make non-atomicity a real defect."""

    vpg = FakeVPG()
    sch = fake_scheduler(_agents(), fake_vpg=vpg)
    claim, attempt = _dispatch_one(sch, vpg)

    sch.handoff_task(
        vpg.graph_id,
        "t1",
        source_claim_id=claim.claim_id,
        replacement_agent_id="not-registered",
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch,
    )

    active = [
        item
        for item in sch.claims
        if item.task_id == "t1" and item.state in {ClaimState.ACTIVE, ClaimState.ACQUIRING}
    ]
    assert len(active) <= 1, "two owners coexisted; non-atomicity would be a safety defect"


def test_a_refused_handoff_leaves_the_task_owned_and_not_redispatchable() -> None:
    """A refusal must not orphan the task, and must not double-dispatch it."""

    vpg = FakeVPG()
    sch = fake_scheduler(_agents(), fake_vpg=vpg)
    claim, attempt = _dispatch_one(sch, vpg)

    result = sch.handoff_task(
        vpg.graph_id,
        "t1",
        source_claim_id=claim.claim_id,
        replacement_agent_id="not-registered",
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch,
    )
    assert result.status is ClaimHandoffStatus.REFUSED

    again = sch.schedule_once(vpg.graph_id)
    redispatched = [item["task_id"] for item in again.dispatched]
    skipped = dict(again.skipped)

    # Still owned by the original claim, so a second dispatch must be refused
    # with that exact reason rather than creating a competing owner.
    assert redispatched == []
    assert "active claim" in skipped.get("t1", "")
