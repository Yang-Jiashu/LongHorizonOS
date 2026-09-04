from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from lhos.runtimes.multi_agent import (
    AgentDescriptor,
    AgentRegistry,
    ClaimHandoffStatus,
    ClaimState,
    OwnershipHandoffPhase,
    OwnershipHandoffStatus,
    create_scheduler,
)
from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler


def _scheduler():
    vpg = FakeVPG()
    scheduler = fake_scheduler(
        {
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
        },
        fake_vpg=vpg,
    )
    vpg.add_ready_task("t1", required_specializations=("python",))
    assert scheduler.schedule_once(vpg.graph_id).dispatched
    claim = scheduler.active_claim_for_task("t1", vpg.graph_id)
    assert claim is not None
    attempt = scheduler.attempt_for_claim(claim.claim_id)
    assert attempt is not None
    return vpg, scheduler, claim, attempt


def test_handoff_transfers_exact_claim_and_marks_source_terminal():
    vpg, scheduler, source, attempt = _scheduler()
    result = scheduler.handoff_task(
        vpg.graph_id,
        "t1",
        source_claim_id=source.claim_id,
        replacement_agent_id="a2",
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch,
        action="preempt",
        handoff_id="h1",
    )
    assert result.status is ClaimHandoffStatus.TRANSFERRED
    assert source.state is ClaimState.RELEASED
    assert (
        scheduler.active_claim_for_task("t1", vpg.graph_id).claim_id == result.replacement_claim_id
    )


def test_old_claim_fence_cannot_release_replacement():
    vpg, scheduler, source, attempt = _scheduler()
    result = scheduler.handoff_task(
        vpg.graph_id,
        "t1",
        source_claim_id=source.claim_id,
        replacement_agent_id="a2",
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch,
        handoff_id="h2",
    )
    assert result.transferred
    assert not scheduler.release_task(
        vpg.graph_id,
        "t1",
        expected_claim_id=source.claim_id,
    )
    active = scheduler.active_claim_for_task("t1", vpg.graph_id)
    assert active is not None and active.claim_id == result.replacement_claim_id


def test_attempt_and_epoch_mismatch_refuse_without_mutation():
    vpg, scheduler, source, attempt = _scheduler()
    bad_attempt = scheduler.handoff_task(
        vpg.graph_id,
        "t1",
        source_claim_id=source.claim_id,
        replacement_agent_id="a2",
        expected_attempt_id="wrong",
        expected_semantic_epoch=attempt.semantic_epoch,
        handoff_id="h3",
    )
    assert bad_attempt.status is ClaimHandoffStatus.REFUSED
    assert source.state is ClaimState.ACTIVE
    bad_epoch = scheduler.handoff_task(
        vpg.graph_id,
        "t1",
        source_claim_id=source.claim_id,
        replacement_agent_id="a2",
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch + 1,
        handoff_id="h4",
    )
    assert bad_epoch.status is ClaimHandoffStatus.REFUSED
    assert source.state is ClaimState.ACTIVE


def test_replacement_refusal_keeps_source_active():
    vpg, scheduler, source, attempt = _scheduler()
    result = scheduler.handoff_task(
        vpg.graph_id,
        "t1",
        source_claim_id=source.claim_id,
        replacement_agent_id="missing",
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch,
        handoff_id="h5",
    )
    assert result.status is ClaimHandoffStatus.REFUSED
    assert source.state is ClaimState.ACTIVE


def test_release_failure_fails_closed_without_replacement():
    vpg, scheduler, source, attempt = _scheduler()
    original = scheduler._s._leases.release
    scheduler._s._leases.release = lambda lease_id: False
    try:
        result = scheduler.handoff_task(
            vpg.graph_id,
            "t1",
            source_claim_id=source.claim_id,
            replacement_agent_id="a2",
            expected_attempt_id=attempt.attempt_id,
            expected_semantic_epoch=attempt.semantic_epoch,
            handoff_id="h6",
        )
    finally:
        scheduler._s._leases.release = original
    assert result.status is ClaimHandoffStatus.FAILED_CLOSED
    assert source.state is ClaimState.ACTIVE
    assert scheduler.active_claim_for_task("t1", vpg.graph_id).claim_id == source.claim_id


def test_handoff_replay_is_idempotent():
    vpg, scheduler, source, attempt = _scheduler()
    kwargs = dict(
        source_claim_id=source.claim_id,
        replacement_agent_id="a2",
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch,
        handoff_id="h7",
    )
    first = scheduler.handoff_task(vpg.graph_id, "t1", **kwargs)
    second = scheduler.handoff_task(vpg.graph_id, "t1", **kwargs)
    assert first.status is ClaimHandoffStatus.TRANSFERRED
    assert second.status is ClaimHandoffStatus.REPLAYED
    assert first.replacement_claim_id == second.replacement_claim_id
    assert len([c for c in scheduler.claims if c.handoff_id == "h7"]) == 1


def test_handoff_replay_rejects_conflicting_request_identity():
    """A handoff id is an idempotency key, not a transferable capability."""
    vpg, scheduler, source, attempt = _scheduler()
    kwargs = dict(
        source_claim_id=source.claim_id,
        replacement_agent_id="a2",
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch,
        handoff_id="h7-conflict",
    )
    first = scheduler.handoff_task(vpg.graph_id, "t1", **kwargs)
    assert first.status is ClaimHandoffStatus.TRANSFERRED

    # A stale/buggy caller must not be able to replay the existing transfer
    # while presenting a different source identity or replacement target.
    conflict = scheduler.handoff_task(
        vpg.graph_id,
        "t1",
        source_claim_id="old-claim",
        replacement_agent_id="a1",
        expected_attempt_id="old-attempt",
        expected_semantic_epoch=attempt.semantic_epoch,
        action="preempt",
        handoff_id="h7-conflict",
    )
    assert conflict.status is ClaimHandoffStatus.REFUSED
    assert "different request" in conflict.reason
    assert scheduler.active_claim_for_task("t1", vpg.graph_id).claim_id == (
        first.replacement_claim_id
    )
    assert len([c for c in scheduler.claims if c.handoff_id == "h7-conflict"]) == 1


def test_handoff_id_cannot_be_reused_for_another_task():
    vpg, scheduler, source, attempt = _scheduler()
    first = scheduler.handoff_task(
        vpg.graph_id,
        "t1",
        source_claim_id=source.claim_id,
        replacement_agent_id="a2",
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch,
        handoff_id="h7-scope",
    )
    assert first.status is ClaimHandoffStatus.TRANSFERRED

    vpg.add_ready_task("t2", required_specializations=("python",))
    assert scheduler.schedule_once(vpg.graph_id).dispatched
    second_source = scheduler.active_claim_for_task("t2", vpg.graph_id)
    second_attempt = scheduler.attempt_for_claim(second_source.claim_id)
    conflict = scheduler.handoff_task(
        vpg.graph_id,
        "t2",
        source_claim_id=second_source.claim_id,
        replacement_agent_id="a2",
        expected_attempt_id=second_attempt.attempt_id,
        expected_semantic_epoch=second_attempt.semantic_epoch,
        handoff_id="h7-scope",
    )
    assert conflict.status is ClaimHandoffStatus.REFUSED
    assert "another graph/task" in conflict.reason
    assert scheduler.active_claim_for_task("t2", vpg.graph_id).claim_id == (second_source.claim_id)


def test_handoff_result_has_stable_serializable_status_and_reason():
    vpg, scheduler, source, attempt = _scheduler()
    result = scheduler.handoff_task(
        vpg.graph_id,
        "t1",
        source_claim_id=source.claim_id,
        replacement_agent_id="missing",
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch,
        handoff_id="h7-json",
    )
    payload = json.loads(result.model_dump_json())
    assert payload["status"] == ClaimHandoffStatus.REFUSED.value
    assert isinstance(payload["reason"], str) and payload["reason"]
    assert payload["handoff_id"] == "h7-json"


def test_prepare_commit_handoff_persists_intent_and_is_replayable():
    vpg, scheduler, source, attempt = _scheduler()
    prepared = scheduler.prepare_handoff(
        vpg.graph_id,
        "t1",
        source_claim_id=source.claim_id,
        replacement_agent_id="a2",
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch,
        handoff_id="tx-1",
        action="preempt",
    )
    assert prepared.status is OwnershipHandoffStatus.PREPARED
    assert prepared.phase is OwnershipHandoffPhase.PREPARED
    assert source.state is ClaimState.ACTIVE

    committed = scheduler.commit_handoff(prepared.intent)
    assert committed.status is OwnershipHandoffStatus.COMMITTED
    assert committed.transferred
    assert committed.claim_result is not None
    assert source.state is ClaimState.RELEASED

    replay = scheduler.commit_handoff(prepared.intent)
    assert replay.status is OwnershipHandoffStatus.REPLAYED
    assert replay.replacement_claim_id == committed.replacement_claim_id


def test_prepare_recovery_aborts_without_touching_source():
    vpg, scheduler, source, attempt = _scheduler()
    prepared = scheduler.prepare_handoff(
        vpg.graph_id,
        "t1",
        source_claim_id=source.claim_id,
        replacement_agent_id="a2",
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch,
        handoff_id="tx-prepared-only",
    )
    recovered = scheduler.recover_handoff("tx-prepared-only")
    assert recovered.status is OwnershipHandoffStatus.ABORTED
    assert recovered.phase is OwnershipHandoffPhase.ABORTED
    assert source.state is ClaimState.ACTIVE


def test_commit_requires_prepare_witness_and_fails_closed():
    vpg, scheduler, source, attempt = _scheduler()
    # Build an intent from the live exact identity but do not call prepare.
    from lhos.runtimes.multi_agent import OwnershipHandoffIntent

    intent = OwnershipHandoffIntent(
        handoff_id="tx-no-prepare",
        graph_id=vpg.graph_id,
        task_id="t1",
        source_claim_id=source.claim_id,
        source_attempt_id=attempt.attempt_id,
        source_agent_id=source.agent_id,
        replacement_agent_id="a2",
        source_graph_version=source.graph_version,
        source_semantic_epoch=attempt.semantic_epoch,
        source_fencing_token=source.lease_fencing_token,
        source_lease_id=source.lease_id,
        action="rebase",
    )
    result = scheduler.commit_handoff(intent)
    assert result.status is OwnershipHandoffStatus.REFUSED
    assert source.state is ClaimState.ACTIVE


def test_recover_committing_intent_aborts_when_source_lease_is_intact(monkeypatch):
    """A crash after COMMITTING but before release must not guess a transfer."""

    vpg, scheduler, source, attempt = _scheduler()
    prepared = scheduler.prepare_handoff(
        vpg.graph_id,
        "t1",
        source_claim_id=source.claim_id,
        replacement_agent_id="a2",
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch,
        handoff_id="tx-crash-before-release",
    )

    core = scheduler._s
    original_handoff = core.handoff_task
    original_record = core._record_event
    from lhos.runtimes.multi_agent.events import SchedulerEventType

    def _crash_handoff(*_args, **_kwargs):
        raise RuntimeError("simulated crash after COMMITTING")

    def _crash_recovery_event(event):
        if event.event_type is SchedulerEventType.TASK_HANDOFF_RECOVERY:
            raise RuntimeError("simulated journal crash")
        return original_record(event)

    monkeypatch.setattr(core, "handoff_task", _crash_handoff)
    monkeypatch.setattr(core, "_record_event", _crash_recovery_event)
    with pytest.raises(RuntimeError, match="simulated journal crash"):
        scheduler.commit_handoff(prepared.intent)

    monkeypatch.setattr(core, "handoff_task", original_handoff)
    monkeypatch.setattr(core, "_record_event", original_record)
    # The lightweight fake lease authority intentionally does not expose a
    # lookup projection.  Inject the exact live lease witness so this test
    # exercises the safe-abort branch rather than the missing-lease IN_DOUBT
    # branch.
    monkeypatch.setattr(
        core,
        "_lease_lookup_for_claim",
        lambda _claim: SimpleNamespace(
            lease_id=source.lease_id,
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
        ),
    )
    recovered = scheduler.recover_handoff(prepared.intent.handoff_id)
    assert recovered.status is OwnershipHandoffStatus.ABORTED
    assert recovered.phase is OwnershipHandoffPhase.ABORTED
    assert source.state is ClaimState.ACTIVE


def test_post_release_replacement_refusal_cleans_scheduler_resources():
    vpg, scheduler, source, attempt = _scheduler()
    original = scheduler._s._leases.acquire
    scheduler._s._leases.acquire = lambda graph_id, task_id, pid: None
    try:
        result = scheduler.handoff_task(
            vpg.graph_id,
            "t1",
            source_claim_id=source.claim_id,
            replacement_agent_id="a2",
            expected_attempt_id=attempt.attempt_id,
            expected_semantic_epoch=attempt.semantic_epoch,
            handoff_id="h8",
        )
    finally:
        scheduler._s._leases.acquire = original
    assert result.status is ClaimHandoffStatus.FAILED_CLOSED
    assert source.state is ClaimState.RELEASED
    assert scheduler.active_claim_for_task("t1", vpg.graph_id) is None
    rejected = [claim for claim in scheduler.claims if claim.handoff_id == "h8"]
    assert len(rejected) == 1 and rejected[0].state is ClaimState.REJECTED
    assert scheduler.resource_manager.list_active() == []


class _LeaseAuthority:
    def __init__(self) -> None:
        self.leases: dict[str, SimpleNamespace] = {}
        self.token = 0

    def acquire_exclusive(self, pid, resource_id, ttl):
        if any(lease.resource_id == resource_id for lease in self.leases.values()):
            return None
        self.token += 1
        lease = SimpleNamespace(
            lease_id=f"lease-{self.token}",
            resource_id=resource_id,
            owner_pid=pid,
            mode="exclusive",
            fencing_token=self.token,
            expires_at=datetime.now(UTC) + ttl,
        )
        self.leases[lease.lease_id] = lease
        return lease

    def release(self, lease_id):
        return self.leases.pop(lease_id, None) is not None

    def renew(self, lease_id, ttl):
        lease = self.leases.get(lease_id)
        if lease is not None:
            lease.expires_at = datetime.now(UTC) + ttl
        return lease

    def release_all_for_pid(self, pid):
        ids = [lease_id for lease_id, lease in self.leases.items() if lease.owner_pid == pid]
        for lease_id in ids:
            self.leases.pop(lease_id)
        return len(ids)

    def get(self, lease_id):
        return self.leases.get(lease_id)

    def list_for_resource(self, resource_id):
        return [lease for lease in self.leases.values() if lease.resource_id == resource_id]

    def list_for_pid(self, pid):
        return [lease for lease in self.leases.values() if lease.owner_pid == pid]

    def reclaim_expired(self):
        return 0


class _ProcessAuthority:
    def get(self, pid):
        return SimpleNamespace(pid=pid, state="ready")

    def list_all(self):
        return []


class _CapabilityAuthority:
    def check(self, pid, resource, operation):
        return True

    def capabilities_for(self, pid):
        return []


def _durable_scheduler(vpg, leases, state_path):
    registry = AgentRegistry()
    for agent_id in ("a1", "a2"):
        registry.register(
            AgentDescriptor(
                agent_id=agent_id,
                process_id=f"pid-{agent_id}",
                supported_task_kinds=("*",),
                specializations=("python",),
                max_concurrency=5,
            )
        )
    return create_scheduler(
        registry,
        vpg=vpg,
        process_provider=_ProcessAuthority(),
        lease_provider=leases,
        capability_provider=_CapabilityAuthority(),
        state_path=str(state_path),
        lease_ttl=timedelta(minutes=5),
    )


def test_successful_handoff_replays_after_scheduler_restart(tmp_path):
    vpg = FakeVPG()
    leases = _LeaseAuthority()
    state_path = tmp_path / "handoff.sqlite"
    first = _durable_scheduler(vpg, leases, state_path)
    vpg.add_ready_task("t1", required_specializations=("python",))
    first.schedule_once(vpg.graph_id)
    source = first.active_claim_for_task("t1", vpg.graph_id)
    attempt = first.attempt_for_claim(source.claim_id)
    kwargs = dict(
        source_claim_id=source.claim_id,
        replacement_agent_id="a2",
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch,
        handoff_id="restart-handoff",
    )
    transferred = first.handoff_task(vpg.graph_id, "t1", **kwargs)
    first.close()

    reopened = _durable_scheduler(vpg, leases, state_path)
    replayed = reopened.handoff_task(vpg.graph_id, "t1", **kwargs)
    assert transferred.status is ClaimHandoffStatus.TRANSFERRED
    assert replayed.status is ClaimHandoffStatus.REPLAYED
    assert replayed.replacement_claim_id == transferred.replacement_claim_id
    reopened.close()
