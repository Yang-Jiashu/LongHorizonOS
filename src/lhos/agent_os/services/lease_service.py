"""Lease Service — atomic resource acquisition, release, and expiry.

Phase B resources:
- model_slot:mock
- device_slot:mock
- workspace:p1, workspace:p2
- resource:R1, resource:R2

Key invariants:
- Atomic acquire: all-or-nothing.
- Exclusive lease: no concurrent owner.
- Shared lease: can coexist with other shared leases.
- Process EXITED/FAILED: all leases released.
- Expired leases can be reclaimed.
- Wait-for graph for deadlock detection.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from lhos.agent_os.kernel.errors import LeaseAcquisitionFailed
from lhos.agent_os.kernel.models import (
    KernelEvent,
    ResourceLease,
)
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.storage.sqlite import SQLiteStorage

DEFAULT_LEASE_TTL = timedelta(seconds=300)

# Phase B resources
DEFAULT_RESOURCES = [
    "model_slot:mock",
    "device_slot:mock",
    "workspace:p1",
    "workspace:p2",
    "resource:R1",
    "resource:R2",
]


class LeaseService:
    """Manages resource leases with atomic acquire and deadlock detection."""

    def __init__(
        self,
        storage: SQLiteStorage,
        journal: JournalService,
    ):
        self._storage = storage
        self._journal = journal

    # ── Acquire ────────────────────────────────────────────────────────────

    def atomic_acquire(
        self,
        pid: str,
        claims: list[dict[str, Any]],
        ttl: timedelta = DEFAULT_LEASE_TTL,
    ) -> list[ResourceLease]:
        """Atomically acquire all claims. All-or-nothing.

        Each claim: {"resource_id": str, "mode": "shared"|"exclusive"}
        Returns list of leases if all succeed.
        Raises LeaseAcquisitionFailed if any fail (no leases created).
        """
        if not claims:
            return []

        now = datetime.utcnow()
        expires_at = now + ttl

        # Check all claims first
        for claim in claims:
            resource_id = claim["resource_id"]
            mode = claim.get("mode", "exclusive")
            if not self._is_available(resource_id, mode, exclude_pid=pid):
                # Record waiter
                self._add_waiter(pid, resource_id)
                # Journal the failure
                ev = KernelEvent(
                    pid=pid,
                    event_type="LEASE_ACQUIRE_FAILED",
                    payload={
                        "resource_id": resource_id,
                        "mode": mode,
                        "reason": "resource_busy",
                    },
                )
                self._journal.append_event(ev)
                raise LeaseAcquisitionFailed(pid, resource_id)

        # All available — acquire all atomically
        leases: list[ResourceLease] = []
        with self._storage.transaction() as tx:
            for claim in claims:
                resource_id = claim["resource_id"]
                mode = claim.get("mode", "exclusive")
                lease = ResourceLease(
                    resource_id=resource_id,
                    owner_pid=pid,
                    mode=mode,
                    acquired_at=now,
                    expires_at=expires_at,
                )
                leases.append(lease)
                tx.execute(
                    """INSERT INTO leases_projection
                       (lease_id, resource_id, owner_pid, mode, acquired_at,
                        expires_at, renewable, revocable)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        lease.lease_id,
                        lease.resource_id,
                        lease.owner_pid,
                        lease.mode,
                        lease.acquired_at.isoformat(),
                        lease.expires_at.isoformat(),
                        int(lease.renewable),
                        int(lease.revocable),
                    ),
                )
            # Remove waiters for acquired resources
            for claim in claims:
                tx.execute(
                    "DELETE FROM lease_waiters WHERE pid = ? AND resource_id = ?",
                    (pid, claim["resource_id"]),
                )

        # Journal each lease
        events = [
            KernelEvent(
                pid=pid,
                event_type="LEASE_ACQUIRED",
                payload=lease.model_dump(mode="json"),
            )
            for lease in leases
        ]
        self._journal.append_events_atomically(events)

        return leases

    # ── Release ────────────────────────────────────────────────────────────

    def release(self, lease_ids: list[str]) -> int:
        """Release leases by ID. Returns count released."""
        if not lease_ids:
            return 0
        released = 0
        events: list[KernelEvent] = []
        with self._storage.transaction() as tx:
            for lid in lease_ids:
                row = tx.query_one(
                    "SELECT * FROM leases_projection WHERE lease_id = ?",
                    (lid,),
                )
                if not row:
                    continue
                tx.execute("DELETE FROM leases_projection WHERE lease_id = ?", (lid,))
                released += 1
                events.append(
                    KernelEvent(
                        pid=row["owner_pid"],
                        event_type="LEASE_RELEASED",
                        payload={
                            "lease_id": lid,
                            "resource_id": row["resource_id"],
                            "owner_pid": row["owner_pid"],
                        },
                    )
                )
        if events:
            self._journal.append_events_atomically(events)
        return released

    def release_all_for_pid(self, pid: str) -> int:
        """Release all leases owned by pid."""
        rows = self._storage.query_all(
            "SELECT lease_id FROM leases_projection WHERE owner_pid = ?",
            (pid,),
        )
        return self.release([r["lease_id"] for r in rows])

    # ── Renew ──────────────────────────────────────────────────────────────

    def renew(self, lease_id: str, ttl: timedelta = DEFAULT_LEASE_TTL) -> ResourceLease | None:
        row = self._storage.query_one(
            "SELECT * FROM leases_projection WHERE lease_id = ?",
            (lease_id,),
        )
        if not row:
            return None
        new_expiry = datetime.utcnow() + ttl
        with self._storage.transaction() as tx:
            tx.execute(
                "UPDATE leases_projection SET expires_at = ? WHERE lease_id = ?",
                (new_expiry.isoformat(), lease_id),
            )
        lease = self._row_to_lease(row)
        lease.expires_at = new_expiry

        ev = KernelEvent(
            pid=row["owner_pid"],
            event_type="LEASE_RENEWED",
            payload={"lease_id": lease_id, "new_expiry": new_expiry.isoformat()},
        )
        self._journal.append_event(ev)
        return lease

    # ── Reclaim expired ────────────────────────────────────────────────────

    def reclaim_expired(self, now: datetime) -> int:
        """Reclaim all expired leases. Returns count reclaimed."""
        rows = self._storage.query_all(
            "SELECT * FROM leases_projection WHERE expires_at < ?",
            (now.isoformat(),),
        )
        if not rows:
            return 0
        lease_ids = [r["lease_id"] for r in rows]
        count = self.release(lease_ids)
        for r in rows:
            ev = KernelEvent(
                pid=r["owner_pid"],
                event_type="LEASE_EXPIRED",
                payload={
                    "lease_id": r["lease_id"],
                    "resource_id": r["resource_id"],
                },
            )
            self._journal.append_event(ev)
        return count

    # ── Queries ────────────────────────────────────────────────────────────

    def get_lease(self, lease_id: str) -> ResourceLease | None:
        row = self._storage.query_one(
            "SELECT * FROM leases_projection WHERE lease_id = ?",
            (lease_id,),
        )
        return self._row_to_lease(row) if row else None

    def list_leases_for_pid(self, pid: str) -> list[ResourceLease]:
        rows = self._storage.query_all(
            "SELECT * FROM leases_projection WHERE owner_pid = ?",
            (pid,),
        )
        return [self._row_to_lease(r) for r in rows]

    def list_all_leases(self) -> list[ResourceLease]:
        rows = self._storage.query_all("SELECT * FROM leases_projection")
        return [self._row_to_lease(r) for r in rows]

    def list_active_leases_for_resource(self, resource_id: str) -> list[ResourceLease]:
        rows = self._storage.query_all(
            "SELECT * FROM leases_projection WHERE resource_id = ?",
            (resource_id,),
        )
        return [self._row_to_lease(r) for r in rows]

    def list_waiters(self, resource_id: str) -> list[str]:
        rows = self._storage.query_all(
            "SELECT pid FROM lease_waiters WHERE resource_id = ?",
            (resource_id,),
        )
        return [r["pid"] for r in rows]

    # ── Deadlock detection ─────────────────────────────────────────────────

    def detect_deadlocks(self) -> list[list[str]]:
        """Detect cycles in the wait-for graph.

        Returns list of cycles, each a list of pids.
        Only tracks resource ownership waits, NOT condition waits.
        """
        # Build wait-for graph: pid → set of pids it's waiting for
        waiters = self._storage.query_all("SELECT * FROM lease_waiters")
        leases = self._storage.query_all("SELECT * FROM leases_projection")

        # Map: resource_id → list of owner pids
        owners: dict[str, list[str]] = {}
        for lease_row in leases:
            owners.setdefault(lease_row["resource_id"], []).append(lease_row["owner_pid"])

        # pid → set of pids it waits for
        wait_for: dict[str, set[str]] = {}
        for w in waiters:
            pid = w["pid"]
            resource_id = w["resource_id"]
            for owner in owners.get(resource_id, []):
                if owner != pid:
                    wait_for.setdefault(pid, set()).add(owner)

        return self._find_cycles(wait_for)

    @staticmethod
    def _find_cycles(wait_for: dict[str, set[str]]) -> list[list[str]]:
        """Find all simple cycles using DFS."""
        cycles: list[list[str]] = []
        visited: set[str] = set()
        stack: list[str] = []
        on_stack: set[str] = set()

        def dfs(node: str) -> None:
            if node in on_stack:
                # Found a cycle
                idx = stack.index(node)
                cycle = stack[idx:]
                # Normalize: start from smallest pid for determinism
                min_idx = cycle.index(min(cycle))
                normalized = cycle[min_idx:] + cycle[:min_idx]
                if normalized not in cycles:
                    cycles.append(normalized)
                return

            if node in visited:
                return

            stack.append(node)
            on_stack.add(node)

            for neighbor in wait_for.get(node, set()):
                dfs(neighbor)

            stack.pop()
            on_stack.discard(node)
            visited.add(node)

        for node in sorted(wait_for.keys()):
            dfs(node)

        return cycles

    # ── Projection ─────────────────────────────────────────────────────────

    def handle_event(self, ev: KernelEvent) -> None:
        if ev.event_type == "LEASE_ACQUIRED":
            lease = ResourceLease(**ev.payload)
            with self._storage.transaction() as tx:
                tx.execute(
                    """INSERT OR REPLACE INTO leases_projection
                       (lease_id, resource_id, owner_pid, mode, acquired_at,
                        expires_at, renewable, revocable)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        lease.lease_id,
                        lease.resource_id,
                        lease.owner_pid,
                        lease.mode,
                        lease.acquired_at.isoformat(),
                        lease.expires_at.isoformat(),
                        int(lease.renewable),
                        int(lease.revocable),
                    ),
                )
        elif ev.event_type == "LEASE_RELEASED" or ev.event_type == "LEASE_EXPIRED":
            with self._storage.transaction() as tx:
                tx.execute(
                    "DELETE FROM leases_projection WHERE lease_id = ?",
                    (ev.payload["lease_id"],),
                )
        elif ev.event_type == "LEASE_RENEWED":
            with self._storage.transaction() as tx:
                tx.execute(
                    "UPDATE leases_projection SET expires_at = ? WHERE lease_id = ?",
                    (ev.payload["new_expiry"], ev.payload["lease_id"]),
                )

    # ── Internal ───────────────────────────────────────────────────────────

    def _is_available(self, resource_id: str, mode: str, exclude_pid: str | None = None) -> bool:
        existing = self._storage.query_all(
            "SELECT * FROM leases_projection WHERE resource_id = ?",
            (resource_id,),
        )
        for row in existing:
            if exclude_pid and row["owner_pid"] == exclude_pid:
                continue
            if mode == "exclusive" or row["mode"] == "exclusive":
                return False
        return True

    def _add_waiter(self, pid: str, resource_id: str) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT OR IGNORE INTO lease_waiters (pid, resource_id, wait_since)
                   VALUES (?, ?, ?)""",
                (pid, resource_id, datetime.utcnow().isoformat()),
            )

    @staticmethod
    def _row_to_lease(row: dict[str, Any]) -> ResourceLease:
        return ResourceLease(
            lease_id=row["lease_id"],
            resource_id=row["resource_id"],
            owner_pid=row["owner_pid"],
            mode=row["mode"],
            acquired_at=datetime.fromisoformat(row["acquired_at"]),
            expires_at=datetime.fromisoformat(row["expires_at"]),
            renewable=bool(row["renewable"]),
            revocable=bool(row["revocable"]),
        )
