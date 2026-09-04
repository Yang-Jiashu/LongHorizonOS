"""LEASE-01 regression: exclusive-lease TOCTOU race is closed by using
BEGIN IMMEDIATE (write lock up front) so concurrent callers serialize.

Before the fix, `atomic_acquire` checked availability in one transaction
and inserted in a separate transaction. Two racing callers could both see
"available" and both insert exclusive leases — breaking the exclusive
guarantee. After the fix, acquisition happens entirely inside a single
IMMEDIATE transaction, which holds the write lock for the check+insert.

The test runs many concurrent racing acquires on the SAME resource with
the SAME storage and asserts exactly one lease survives.
"""

from __future__ import annotations

import asyncio

import pytest

from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.services.lease_service import LeaseService
from lhos.agent_os.storage.sqlite import SQLiteStorage


@pytest.fixture
def lease_env(tmp_path):
    storage = SQLiteStorage(str(tmp_path / "leases.db"))
    journal = JournalService(storage)
    lease_service = LeaseService(storage, journal)
    return {"storage": storage, "journal": journal, "lease_service": lease_service}


class TestExclusiveLeaseSerializesUnderContention:
    @pytest.mark.asyncio
    async def test_only_one_exclusive_winner_under_contention(self, lease_env):
        """20 concurrent goroutines race for the same exclusive lease."""
        lease_service = lease_env["lease_service"]
        pids = [f"p{i}" for i in range(20)]
        resource = "resource:contended"

        async def try_acquire(pid: str):
            # Synchronous service call in a worker thread
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None,
                lambda: lease_service.atomic_acquire(
                    pid, [{"resource_id": resource, "mode": "exclusive"}]
                ),
            )

        results = await asyncio.gather(
            *[try_acquire(pid) for pid in pids],
            return_exceptions=True,
        )
        winners = [r for r in results if not isinstance(r, BaseException)]
        losers = [r for r in results if isinstance(r, BaseException)]

        assert len(winners) == 1, f"expected one winner; got {len(winners)}"
        assert len(losers) == 19, f"expected 19 losers; got {len(losers)}"

        # One and only one lease must exist for the resource.
        active = lease_service.list_active_leases_for_resource(resource)
        assert len(active) == 1
        assert (
            active[0].owner_pid
            == pids[next(i for i, r in enumerate(results) if not isinstance(r, BaseException))]
        )

    def test_same_pid_exclusive_still_blocks_distinct(self, lease_env):
        """Same PID holding exclusive lease doesn't get duplicated;
        a different PID trying the same resource still fails."""
        lease_service = lease_env["lease_service"]
        lease_service.atomic_acquire("p1", [{"resource_id": "resource:one", "mode": "exclusive"}])
        with pytest.raises(Exception):
            lease_service.atomic_acquire(
                "p2", [{"resource_id": "resource:one", "mode": "exclusive"}]
            )
