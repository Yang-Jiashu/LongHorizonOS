"""GraphVersion race re-check (Section 19).

The Scheduler must re-check the VPG version between building the readiness
proof and linearizing ownership; a stale proof is rejected.
"""

from __future__ import annotations

from lhos.runtimes.multi_agent import AgentDescriptor, AgentRegistry, create_scheduler
from tests.runtimes.multi_agent.helpers import FakeVPG


def _make_scheduler(vpg):
    reg = AgentRegistry()
    reg.register(AgentDescriptor(
        agent_id="a", process_id="pid-a",
        supported_task_kinds=("*",), specializations=("python",),
        max_concurrency=5,
    ))

    class _Proc:
        def get(self, pid):
            return _P(pid)

        def list_all(self):
            return [_P("pid-a")]

    class _Lease:
        def acquire_exclusive(self, pid, resource_id, ttl):
            return _L(resource_id)

        def release(self, leak_id):
            return True

        def release_all_for_pid(self, pid):
            return 0

        def get(self, lease_id):
            return None

        def list_for_resource(self, resource_id):
            return []

        def list_for_pid(self, pid):
            return []

        def reclaim_expired(self):
            return 0

    class _L:
        def __init__(self, resource_id):
            from datetime import datetime, timedelta, timezone
            self.lease_id = f"lease-{resource_id}"
            self.resource_id = resource_id
            self.expires_at = datetime.now(timezone.utc) + timedelta(minutes=30)

    class _Cap:
        def check(self, pid, r, o):
            return True

        def capabilities_for(self, pid):
            return []

    return create_scheduler(
        reg, vpg=vpg, process_provider=_Proc(),
        lease_provider=_Lease(), capability_provider=_Cap(),
    )


class _P:
    def __init__(self, pid):
        self.pid = pid
        self.state = "ready"


def test_stale_graph_version_rejects_claim_acquisition():
    """After building the version-v1 readiness proof, bump the graph version
    to v2 before the claim is linearized; acquisition must fail and the
    task stays undispatchable (CLAIM_REJECTED event emitted)."""
    vpg = FakeVPG()
    sch = _make_scheduler(vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))

    # Hook into the VPG's version to simulate a concurrent commit.
    original = vpg.current_graph_version

    def racing_version(graph_id):
        # First call returns original; subsequent calls (the re-check
        # inside _acquire_claim) return bumped version.
        out = original(graph_id)
        if not hasattr(racing_version, "called"):
            racing_version.called = True  # type: ignore[attr-defined]
            return out
        return out + 1  # type: ignore[operator]

    vpg.current_graph_version = racing_version  # type: ignore[assignment]
    res = sch.schedule_once(vpg.graph_id)
    # Dispatch failed because of version mismatch.
    assert res.dispatched == []


def test_current_version_used_in_idempotency_key():
    """After bumping the graph version, a new task at the new version gets a
    fresh idempotency key and can be scheduled (agent has spare capacity)."""
    from tests.runtimes.multi_agent.helpers import fake_scheduler
    vpg = FakeVPG()
    # Agent needs max_concurrency > 1 so t2 isn't capacity-rejected while t1
    # remains ACTIVE in the same pass-chain.
    sch = fake_scheduler(
        {"a": {"supported_task_kinds": ("*",), "specializations": ("python",),
                "max_concurrency": 5}},
        fake_vpg=vpg,
    )
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    vpg.bump_version()
    vpg.add_ready_task("t2", required_specializations=("python",), version=vpg.current_version)
    res = sch.schedule_once(vpg.graph_id)
    assert len(res.dispatched) == 1
    assert res.dispatched[0]["task_id"] == "t2"
