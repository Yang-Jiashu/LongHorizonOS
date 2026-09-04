"""Lease-to-VPG conditional commit races.

These tests exercise the narrow guarantee provided by the guard: a graph
commit is authorized only by the exact live Kernel lease generation observed
by the owning claim.  Facts/Action side effects are intentionally outside this
transactional boundary.
"""

from __future__ import annotations

import os
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from lhos.agent_os.sdk.client import create_kernel
from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.models import EvidenceNode, LeaseCommitGuard
from lhos.runtimes.verified_progress.patches import GraphPatchProposal


def _prepare(tmp_path):
    db_path = tmp_path / "lease-vpg.db"
    kernel = create_kernel(str(db_path))
    pid = kernel._process_service.spawn("owner").pid
    resource = "vpg://g/task/t/claim"
    lease = kernel._lease_service.atomic_acquire(
        pid,
        [{"resource_id": resource, "mode": "exclusive"}],
        ttl=timedelta(minutes=5),
    )[0]
    runtime = VerifiedProgressRuntime(str(db_path))
    graph_id = runtime.create_graph(owner_pid=pid, graph_id="g").graph_id
    node = EvidenceNode(
        node_id="evidence-1",
        graph_id=graph_id,
        created_in_version=1,
        updated_in_version=1,
        created_by_pid=pid,
        produced_by_pid=pid,
    )
    patch = GraphPatchProposal(
        graph_id=graph_id,
        expected_graph_version=0,
        author_pid=pid,
        idempotency_key="guarded-evidence",
        operations=(),
    )
    guard = LeaseCommitGuard(
        lease_id=lease.lease_id,
        resource_id=resource,
        owner_pid=pid,
        fencing_token=lease.fencing_token,
        expires_at=lease.expires_at,
    )
    return kernel, runtime, lease, node, patch, guard


def _commit(runtime, node, patch, guard) -> None:
    runtime.store.commit_patch(
        patch,
        patch_id=patch.patch_id,
        committed_version=1,
        applied_at=datetime.now(UTC).isoformat(),
        events=(),
        nodes_to_upsert=[(node.node_id, node)],
        edges_to_upsert=(),
        projection_nodes=[node],
        projection_edges=(),
        commit_guard=guard,
    )


def _counts(runtime) -> dict[str, int]:
    conn = runtime.store.conn
    return {
        "patches": conn.execute("SELECT COUNT(*) FROM graph_patches").fetchone()[0],
        "version_1": conn.execute(
            "SELECT COUNT(*) FROM graph_versions WHERE version = 1"
        ).fetchone()[0],
        "snapshots_1": conn.execute(
            "SELECT COUNT(*) FROM graph_projection_snapshots WHERE version = 1"
        ).fetchone()[0],
        "node_history_1": conn.execute(
            "SELECT COUNT(*) FROM graph_node_history WHERE version = 1"
        ).fetchone()[0],
        "nodes": conn.execute("SELECT COUNT(*) FROM graph_nodes_projection").fetchone()[0],
        "evidence": conn.execute(
            "SELECT COUNT(*) FROM graph_nodes_projection WHERE node_type = 'evidence'"
        ).fetchone()[0],
    }


def test_agentos_memory_uses_one_shared_backing_and_cleans_it(tmp_path):
    from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome

    os_ = AgentOS(":memory:")
    try:
        assert os_._storage_db_path != ":memory:"
        assert os_.kernel._storage.db_path == os_._storage_db_path
        assert os_._facts._conn is not None
        facts_path = os_._facts._conn.execute("PRAGMA database_list").fetchone()[2]
        assert os.path.samefile(facts_path, os_._storage_db_path)
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("shared")
        goal.task(
            "task",
            agent="worker",
            verify=lambda: VerificationOutcome(
                passed=True,
                artifact_id="artifact",
                version=1,
                content="ok",
            ),
        )
        assert os_.run(goal, max_dispatches=1).verified == ["task"]
        assert os_._facts.get_action(next(iter(os_._facts._actions), "")) is not None or (
            os_.kernel._storage.conn.execute("SELECT COUNT(*) FROM actions_projection").fetchone()[
                0
            ]
            > 0
        )
        backing = os_._storage_db_path
    finally:
        os_.close()
    assert not Path(backing).exists()


def test_release_first_guard_failure_rolls_back_every_graph_row(tmp_path):
    kernel, runtime, lease, node, patch, guard = _prepare(tmp_path)
    entered = threading.Event()
    allow = threading.Event()
    errors: list[BaseException] = []

    def pause_release() -> int:
        entered.set()
        if not allow.wait(5):
            raise RuntimeError("release test did not receive release permission")
        return 0

    try:
        kernel._storage.conn.create_function("pause_release", 0, pause_release)
        kernel._storage.conn.execute(
            "CREATE TRIGGER pause_release_trigger "
            "BEFORE DELETE ON leases_projection "
            "BEGIN SELECT pause_release(); END"
        )

        def release() -> None:
            try:
                kernel._lease_service.release([lease.lease_id])
            except BaseException as exc:  # pragma: no cover - assertion below
                errors.append(exc)

        release_thread = threading.Thread(target=release)
        release_thread.start()
        assert entered.wait(5)

        commit_errors: list[BaseException] = []

        def stale_commit() -> None:
            try:
                _commit(runtime, node, patch, guard)
            except BaseException as exc:
                commit_errors.append(exc)

        commit_thread = threading.Thread(target=stale_commit)
        commit_thread.start()
        allow.set()
        release_thread.join(5)
        commit_thread.join(5)

        assert not release_thread.is_alive()
        assert not commit_thread.is_alive()
        assert errors == []
        assert len(commit_errors) == 1
        assert isinstance(commit_errors[0], VPGError)
        assert commit_errors[0].code == VPGCode.LEASE_FENCE_LOST
        assert runtime.get_graph("g").current_version == 0
        assert _counts(runtime) == {
            "patches": 0,
            "version_1": 0,
            "snapshots_1": 0,
            "node_history_1": 0,
            "nodes": 0,
            "evidence": 0,
        }
    finally:
        runtime.close()
        kernel.close()


def test_commit_first_wins_then_release_can_follow(tmp_path):
    kernel, runtime, lease, node, patch, guard = _prepare(tmp_path)
    entered = threading.Event()
    allow = threading.Event()
    errors: list[BaseException] = []

    def pause_commit() -> int:
        entered.set()
        if not allow.wait(5):
            raise RuntimeError("commit test did not receive commit permission")
        return 0

    try:
        runtime.store.conn.create_function("pause_commit", 0, pause_commit)
        runtime.store.conn.execute(
            "CREATE TRIGGER pause_commit_trigger "
            "BEFORE INSERT ON graph_idempotency "
            "BEGIN SELECT pause_commit(); END"
        )

        def commit() -> None:
            try:
                _commit(runtime, node, patch, guard)
            except BaseException as exc:  # pragma: no cover - assertion below
                errors.append(exc)

        commit_thread = threading.Thread(target=commit)
        commit_thread.start()
        assert entered.wait(5)

        release_done = threading.Event()

        def release() -> None:
            try:
                kernel._lease_service.release([lease.lease_id])
            except BaseException as exc:  # pragma: no cover - assertion below
                errors.append(exc)
            finally:
                release_done.set()

        release_thread = threading.Thread(target=release)
        release_thread.start()
        assert not release_done.wait(0.1)
        allow.set()
        commit_thread.join(5)
        release_thread.join(5)

        assert not commit_thread.is_alive()
        assert not release_thread.is_alive()
        assert errors == []
        assert runtime.get_graph("g").current_version == 1
        assert runtime.store.get_node("g", node.node_id) is not None
        assert kernel._lease_service.get_lease(lease.lease_id) is None
        assert _counts(runtime) == {
            "patches": 1,
            "version_1": 1,
            "snapshots_1": 1,
            "node_history_1": 1,
            "nodes": 1,
            "evidence": 1,
        }
    finally:
        runtime.close()
        kernel.close()


def test_reassigned_resource_rejects_old_guard_without_rows(tmp_path):
    kernel, runtime, lease, node, patch, guard = _prepare(tmp_path)
    try:
        assert kernel._lease_service.release([lease.lease_id]) == 1
        new_pid = kernel._process_service.spawn("new-owner").pid
        replacement = kernel._lease_service.atomic_acquire(
            new_pid,
            [{"resource_id": guard.resource_id, "mode": "exclusive"}],
            ttl=timedelta(minutes=5),
        )[0]
        assert replacement.fencing_token > guard.fencing_token
        with pytest.raises(VPGError) as caught:
            _commit(runtime, node, patch, guard)
        assert caught.value.code == VPGCode.LEASE_FENCE_LOST
        assert runtime.get_graph("g").current_version == 0
        assert _counts(runtime)["patches"] == 0
    finally:
        runtime.close()
        kernel.close()


def test_lease_renewal_keeps_same_guard_generation_authorized(tmp_path):
    kernel, runtime, lease, node, patch, guard = _prepare(tmp_path)
    try:
        renewed = kernel._lease_service.renew(
            lease.lease_id,
            ttl=timedelta(minutes=10),
        )
        assert renewed is not None
        assert renewed.fencing_token == guard.fencing_token
        assert renewed.expires_at != guard.expires_at
        # Expiry is liveness, not an ownership generation.  The old captured
        # guard remains valid because the lease id/token/owner are unchanged.
        _commit(runtime, node, patch, guard)
        assert runtime.get_graph("g").current_version == 1
    finally:
        runtime.close()
        kernel.close()


def test_shortening_renewal_keeps_same_guard_generation_authorized(tmp_path):
    kernel, runtime, lease, node, patch, guard = _prepare(tmp_path)
    try:
        renewed = kernel._lease_service.renew(
            lease.lease_id,
            ttl=timedelta(seconds=1),
        )
        assert renewed is not None
        assert renewed.fencing_token == guard.fencing_token
        assert renewed.expires_at < guard.expires_at
        _commit(runtime, node, patch, guard)
        assert runtime.get_graph("g").current_version == 1
    finally:
        runtime.close()
        kernel.close()


def test_guarded_idempotency_replay_rechecks_live_lease(tmp_path):
    kernel, runtime, lease, node, patch, guard = _prepare(tmp_path)
    try:
        _commit(runtime, node, patch, guard)
        assert kernel._lease_service.release([lease.lease_id]) == 1
        with pytest.raises(VPGError) as caught:
            runtime.submit_patch(patch, _commit_guard=guard)
        assert caught.value.code == VPGCode.LEASE_FENCE_LOST
        assert runtime.get_graph("g").current_version == 1
        assert _counts(runtime)["patches"] == 1
    finally:
        runtime.close()
        kernel.close()
