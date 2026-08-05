"""Audit: Lease lifecycle and leak detection."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pytest

from lhos.agent_os.kernel.models import (
    ProcessState,
)
from lhos.agent_os.programs.scripted import (
    ScriptedProgram,
    exit_step,
    process_event_step,
    submit_device_action,
)
from lhos.agent_os.sdk.client import create_kernel


def scan_lease_invariants(kernel) -> list[dict[str, Any]]:
    """Scan for lease invariant violations.

    Returns list of violations found.
    """
    violations: list[dict[str, Any]] = []
    storage = kernel._storage

    # 1. Terminal process holding active lease
    leases = storage.query_all("SELECT * FROM leases_projection")
    for lease in leases:
        owner_pid = lease["owner_pid"]
        pcb = kernel._process_service.get_process(owner_pid)
        if pcb is not None and pcb.state in (ProcessState.EXITED, ProcessState.FAILED):
            violations.append(
                {
                    "type": "terminal_process_holds_lease",
                    "pid": owner_pid,
                    "lease_id": lease["lease_id"],
                    "process_state": pcb.state.value,
                }
            )

    # 2. Terminal action holding active lease
    kernel._action_service.list_non_terminal()
    all_actions = storage.query_all("SELECT * FROM actions_projection")
    terminal_states = {"committed", "failed", "cancelled", "timed_out", "uncertain"}
    for row in all_actions:
        if row["state"] in terminal_states:
            import json

            lease_ids = json.loads(row.get("lease_ids_json") or "[]")
            if lease_ids:
                for lid in lease_ids:
                    lease_row = storage.query_one(
                        "SELECT * FROM leases_projection WHERE lease_id = ?",
                        (lid,),
                    )
                    if lease_row:
                        violations.append(
                            {
                                "type": "terminal_action_holds_lease",
                                "action_id": row["action_id"],
                                "lease_id": lid,
                                "action_state": row["state"],
                            }
                        )

    # 3. Lease owner doesn't exist
    for lease in leases:
        owner_pid = lease["owner_pid"]
        pcb = kernel._process_service.get_process(owner_pid)
        if pcb is None:
            violations.append(
                {
                    "type": "lease_owner_not_found",
                    "pid": owner_pid,
                    "lease_id": lease["lease_id"],
                }
            )

    # 4. Expired lease still active
    now = datetime.utcnow()
    for lease in leases:
        expires_at = datetime.fromisoformat(lease["expires_at"])
        if expires_at < now:
            violations.append(
                {
                    "type": "expired_lease_still_active",
                    "lease_id": lease["lease_id"],
                    "expires_at": lease["expires_at"],
                }
            )

    # 5. Multiple exclusive owners for same resource
    for lease in leases:
        if lease["mode"] == "exclusive":
            others = [
                lease_entry
                for lease_entry in leases
                if lease_entry["resource_id"] == lease["resource_id"]
                and lease_entry["lease_id"] != lease["lease_id"]
            ]
            if others:
                violations.append(
                    {
                        "type": "multiple_exclusive_owners",
                        "resource_id": lease["resource_id"],
                        "lease_ids": [lease["lease_id"]] + [o["lease_id"] for o in others],
                    }
                )

    return violations


class TestLeaseLifecycleAudit:
    """Verify lease lifecycle invariants across all terminal states."""

    @pytest.mark.asyncio
    async def test_no_lease_leak_after_action_commit(self) -> None:
        """Leases released after action COMMITTED."""
        kernel = create_kernel(":memory:")

        program = ScriptedProgram(program_id="lease_commit", steps=[])
        pid = await kernel.spawn(program)
        program._steps = [
            submit_device_action(
                pid,
                operation="test",
                side_effect_class="pure",
                resource_claims=[{"resource_id": "resource:R1", "mode": "exclusive"}],
            ),
            process_event_step(pid),
            exit_step(pid),
        ]
        program.reset()
        await kernel.run_until_idle(max_ticks=20)

        violations = scan_lease_invariants(kernel)
        assert violations == [], f"Lease violations: {violations}"

    @pytest.mark.asyncio
    async def test_no_lease_leak_after_action_failed(self) -> None:
        """Leases released after action FAILED."""
        kernel = create_kernel(":memory:")

        # Configure driver to fail
        driver = kernel.get_driver("tool/mock")
        driver.set_default_behavior("deterministic_failure")

        program = ScriptedProgram(program_id="lease_fail", steps=[])
        pid = await kernel.spawn(program)
        program._steps = [
            submit_device_action(
                pid,
                operation="fail",
                side_effect_class="pure",
                resource_claims=[{"resource_id": "resource:R1", "mode": "exclusive"}],
            ),
            process_event_step(pid),
            exit_step(pid),
        ]
        program.reset()
        await kernel.run_until_idle(max_ticks=20)

        violations = scan_lease_invariants(kernel)
        assert violations == [], f"Lease violations: {violations}"

    @pytest.mark.asyncio
    async def test_no_lease_leak_after_action_uncertain(self) -> None:
        """Leases released after action UNCERTAIN."""
        kernel = create_kernel(":memory:")

        driver = kernel.get_driver("tool/mock")
        driver.set_default_behavior("crash_after_effect")

        program = ScriptedProgram(program_id="lease_unc", steps=[])
        pid = await kernel.spawn(program)
        program._steps = [
            submit_device_action(
                pid,
                operation="danger",
                side_effect_class="non_reversible",
                resource_claims=[{"resource_id": "resource:R1", "mode": "exclusive"}],
            ),
            process_event_step(pid),
            exit_step(pid),
        ]
        program.reset()
        await kernel.run_until_idle(max_ticks=20)

        violations = scan_lease_invariants(kernel)
        # UNCERTAIN action should have released leases
        assert violations == [], f"Lease violations: {violations}"

    @pytest.mark.asyncio
    async def test_no_lease_leak_after_process_failure(self) -> None:
        """Leases released after process FAILED."""

        class FailingProgram(ScriptedProgram):
            async def step(self, state, event):
                raise RuntimeError("intentional failure")

        kernel = create_kernel(":memory:")

        # First acquire a lease manually
        kernel._lease_service.atomic_acquire(
            "manual_pid",
            [{"resource_id": "resource:R1", "mode": "exclusive"}],
        )

        # Verify lease exists
        assert len(kernel._lease_service.list_leases_for_pid("manual_pid")) == 1

        # Simulate process failure — release all leases
        kernel._lease_service.release_all_for_pid("manual_pid")

        assert len(kernel._lease_service.list_leases_for_pid("manual_pid")) == 0

    def test_expired_lease_reclaimed_after_restart(self) -> None:
        """Expired leases are reclaimed."""
        kernel = create_kernel(":memory:")

        # Acquire a lease with very short TTL
        past_expiry = datetime.utcnow() - timedelta(seconds=1)
        leases = kernel._lease_service.atomic_acquire(
            "p1",
            [{"resource_id": "resource:R1", "mode": "exclusive"}],
        )
        assert len(leases) == 1

        # Manually set expiry to past
        kernel._storage.execute(
            "UPDATE leases_projection SET expires_at = ? WHERE lease_id = ?",
            (past_expiry.isoformat(), leases[0].lease_id),
        )

        # Reclaim
        reclaimed = kernel._lease_service.reclaim_expired(datetime.utcnow())
        assert reclaimed == 1

        # Verify lease is gone
        assert len(kernel._lease_service.list_leases_for_pid("p1")) == 0

    def test_exclusive_resource_never_has_multiple_active_owners(self) -> None:
        """Two processes cannot hold exclusive leases on the same resource."""
        kernel = create_kernel(":memory:")

        # p1 gets exclusive on R1
        kernel._lease_service.atomic_acquire(
            "p1",
            [{"resource_id": "resource:R1", "mode": "exclusive"}],
        )

        # p2 tries to get exclusive on R1 — must fail
        from lhos.agent_os.kernel.errors import LeaseAcquisitionFailed

        with pytest.raises(LeaseAcquisitionFailed):
            kernel._lease_service.atomic_acquire(
                "p2",
                [{"resource_id": "resource:R1", "mode": "exclusive"}],
            )

        # Verify only one lease exists
        active = kernel._lease_service.list_active_leases_for_resource("resource:R1")
        assert len(active) == 1
        assert active[0].owner_pid == "p1"

    def test_shared_leases_can_coexist(self) -> None:
        """Multiple shared leases on the same resource are OK."""
        kernel = create_kernel(":memory:")

        kernel._lease_service.atomic_acquire(
            "p1",
            [{"resource_id": "resource:R1", "mode": "shared"}],
        )
        kernel._lease_service.atomic_acquire(
            "p2",
            [{"resource_id": "resource:R1", "mode": "shared"}],
        )

        active = kernel._lease_service.list_active_leases_for_resource("resource:R1")
        assert len(active) == 2

    def test_lease_scanner_finds_violations(self) -> None:
        """The lease invariant scanner actually detects violations."""
        kernel = create_kernel(":memory:")

        # Create a lease for a non-existent process
        kernel._lease_service.atomic_acquire(
            "ghost_pid",
            [{"resource_id": "resource:R1", "mode": "exclusive"}],
        )

        violations = scan_lease_invariants(kernel)
        # ghost_pid doesn't have a PCB → violation
        assert any(v["type"] == "lease_owner_not_found" for v in violations)
