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

from datetime import UTC, datetime, timedelta
from typing import Any

from lhos.agent_os.kernel.errors import KernelInvariantViolation, LeaseAcquisitionFailed
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
            self.clear_waiters_for_pid(pid)
            return []

        # A single request must describe each logical resource at most once.
        # Otherwise two rows can be inserted for the same owner/resource (even
        # two exclusive rows), making the action contract and exclusivity
        # semantics ambiguous. Reject before opening a transaction or creating
        # wait edges so failure is side-effect free.
        seen_resources: set[str] = set()
        for claim in claims:
            resource_id = claim["resource_id"]
            if resource_id in seen_resources:
                raise LeaseAcquisitionFailed(pid, resource_id)
            seen_resources.add(resource_id)

        # Phase 1: Acquire-or-fail inside ONE BEGIN IMMEDIATE transaction.
        # The write lock is taken up front so the check-then-insert is
        # serializable (closes the LEASE-01 TOCTOU window). Side effects
        # (_add_waiter, journal) MUST stay out of this txn: the storage uses
        # isolation_level=None, which forbids nested BEGINs.
        pending_waiters: list[tuple[str, str]] = []
        failed_resource: str | None = None
        failed_mode: str | None = None
        leases: list[ResourceLease] = []
        active_by_resource: dict[str, list[dict[str, Any]]] = {}
        with self._storage.transaction(immediate=True) as tx:
            # Re-check availability within the write-locked txn.  SELECT→check→INSERT
            # run under _write_lock so concurrent callers are serialized and two
            # threads cannot both observe 'no holding row' and both INSERT, which
            # would break the exclusive invariant (LEASE-01).
            for claim in claims:
                resource_id = claim["resource_id"]
                mode = claim.get("mode", "exclusive")
                rows = tx.query_all(
                    "SELECT owner_pid, mode, fencing_token "
                    "FROM leases_projection WHERE resource_id = ?",
                    (resource_id,),
                )
                active_by_resource[resource_id] = rows
                for row in rows:
                    existing_mode = row["mode"]
                    if mode == "exclusive" or existing_mode == "exclusive":
                        pending_waiters.append((pid, resource_id))
                        failed_resource = resource_id
                        failed_mode = mode
                        break
                if failed_resource is not None:
                    break

            # All still available under the write lock — acquire atomically.
            acquisition_claims = [] if pending_waiters else claims
            now = datetime.now(UTC)
            expires_at = now + ttl
            for claim in acquisition_claims:
                resource_id = claim["resource_id"]
                mode = claim.get("mode", "exclusive")
                active = active_by_resource.get(resource_id, [])
                if mode == "shared" and active:
                    # Shared holders belong to one read cohort. A later reader
                    # must not fence an earlier concurrent reader; all leases
                    # in the cohort therefore reuse the same generation.
                    cohort_tokens = {int(row["fencing_token"]) for row in active}
                    if len(cohort_tokens) != 1 or next(iter(cohort_tokens)) <= 0:
                        raise KernelInvariantViolation(
                            f"shared fencing cohort is inconsistent for {resource_id!r}"
                        )
                    fencing_token = next(iter(cohort_tokens))
                    token_row = tx.query_one(
                        "SELECT last_token FROM resource_fencing_tokens WHERE resource_id = ?",
                        (resource_id,),
                    )
                    if token_row is None or int(token_row["last_token"]) != fencing_token:
                        raise KernelInvariantViolation(
                            f"shared fencing cohort is superseded for {resource_id!r}"
                        )
                else:
                    # A new ownership epoch (first shared reader or exclusive
                    # owner) advances the durable resource fence.
                    token_row = tx.query_one(
                        "SELECT last_token FROM resource_fencing_tokens WHERE resource_id = ?",
                        (resource_id,),
                    )
                    fencing_token = int(token_row["last_token"]) + 1 if token_row else 1
                    tx.execute(
                        """
                        INSERT INTO resource_fencing_tokens(resource_id, last_token)
                        VALUES (?, ?)
                        ON CONFLICT(resource_id) DO UPDATE SET
                            last_token = excluded.last_token
                        """,
                        (resource_id, fencing_token),
                    )
                lease = ResourceLease(
                    resource_id=resource_id,
                    owner_pid=pid,
                    mode=mode,
                    fencing_token=fencing_token,
                    acquired_at=now,
                    expires_at=expires_at,
                )
                leases.append(lease)
                tx.execute(
                    """INSERT INTO leases_projection
                       (lease_id, resource_id, owner_pid, mode, fencing_token, acquired_at,
                        expires_at, renewable, revocable)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        lease.lease_id,
                        lease.resource_id,
                        lease.owner_pid,
                        lease.mode,
                        lease.fencing_token,
                        lease.acquired_at.isoformat(),
                        lease.expires_at.isoformat(),
                        int(lease.renewable),
                        int(lease.revocable),
                    ),
                )
            # A successful acquisition supersedes every previous wait intent
            # from this process, not only rows for the acquired resources.
            if acquisition_claims:
                tx.execute("DELETE FROM lease_waiters WHERE pid = ?", (pid,))
            # LEASE-01: coalesce the journal INSERT of LEASE_ACQUIRED events into
            # the SAME BEGIN IMMEDIATE txn; otherwise the post-txn side-effect
            # transaction races concurrent acquisitions.
            if leases:
                acq_events = [
                    KernelEvent(
                        pid=pid,
                        event_type="LEASE_ACQUIRED",
                        payload=lease.model_dump(mode="json"),
                    )
                    for lease in leases
                ]
                self._journal.append_events_tx(tx, acq_events)

        # Phase 2: Side effects AFTER the write lock is released.
        if pending_waiters:
            # A retry is a new all-or-nothing wait intent. Drop edges from a
            # previous request so stale rows cannot create false cycles.
            self.clear_waiters_for_pid(pid)
        for waiter_pid, waiter_resource in pending_waiters:
            self._add_waiter(waiter_pid, waiter_resource)
            self._journal.append_event(
                KernelEvent(
                    pid=waiter_pid,
                    event_type="LEASE_ACQUIRE_FAILED",
                    payload={
                        "resource_id": waiter_resource,
                        "mode": failed_mode or "exclusive",
                        "reason": "resource_busy",
                    },
                )
            )

        if failed_resource is not None:
            raise LeaseAcquisitionFailed(pid, failed_resource)

        return leases

    # ── Release ────────────────────────────────────────────────────────────

    def release(self, lease_ids: list[str]) -> int:
        """Release leases by ID. Returns count released."""
        if not lease_ids:
            return 0
        released = 0
        events: list[KernelEvent] = []
        # Release competes with guarded VPG commits for the same database-wide
        # SQLite writer lock.  Taking it up front gives the two operations one
        # deterministic linearization order: either deletion wins and the graph
        # guard fails, or the graph commit wins and deletion follows it.
        with self._storage.transaction(immediate=True) as tx:
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
        self.clear_waiters_for_pid(pid)
        return self.release([r["lease_id"] for r in rows])

    # ── Renew ──────────────────────────────────────────────────────────────

    def renew(self, lease_id: str, ttl: timedelta = DEFAULT_LEASE_TTL) -> ResourceLease | None:
        # LEASE-02: SELECT → UPDATE run inside BEGIN IMMEDIATE so a concurrent
        # release cannot land between the check and the UPDATE. We then inspect
        # rowcount: 0 means the lease was concurrently released or had already
        # expired, and we signal that to the caller (returns None).  The expiry
        # predicate also prevents a stale owner from reviving an expired row.
        new_expiry = datetime.now(UTC) + ttl
        check_now = datetime.now(UTC)
        with self._storage.transaction(immediate=True) as tx:
            cur = tx.execute(
                "UPDATE leases_projection SET expires_at = ? WHERE lease_id = ? AND expires_at > ?",
                (new_expiry.isoformat(), lease_id, check_now.isoformat()),
            )
            if cur.rowcount == 0:
                return None
            row = tx.query_one(
                "SELECT * FROM leases_projection WHERE lease_id = ?",
                (lease_id,),
            )
            if row is None:
                raise KernelInvariantViolation(
                    f"lease {lease_id!r} disappeared after a successful renewal update"
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
        """Atomically reclaim leases that are still expired at the writer lock."""
        cutoff = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
        cutoff_iso = cutoff.astimezone(UTC).isoformat()
        events: list[KernelEvent] = []
        reclaimed = 0

        # Renewal and reclamation compete for the same SQLite writer lock.
        # Re-read expiry only after BEGIN IMMEDIATE so a lease renewed first is
        # preserved, while a reclaim that linearizes first removes the row and
        # makes the later renewal return None.
        with self._storage.transaction(immediate=True) as tx:
            rows = tx.query_all(
                "SELECT * FROM leases_projection WHERE expires_at <= ?",
                (cutoff_iso,),
            )
            for row in rows:
                deleted = tx.execute(
                    "DELETE FROM leases_projection WHERE lease_id = ? AND expires_at <= ?",
                    (row["lease_id"], cutoff_iso),
                )
                if deleted.rowcount != 1:
                    continue
                reclaimed += 1
                # Preserve the existing observable release + expiry event pair,
                # but emit it only for a row actually deleted by this txn.
                events.extend(
                    (
                        KernelEvent(
                            pid=row["owner_pid"],
                            event_type="LEASE_RELEASED",
                            payload={
                                "lease_id": row["lease_id"],
                                "resource_id": row["resource_id"],
                                "owner_pid": row["owner_pid"],
                            },
                        ),
                        KernelEvent(
                            pid=row["owner_pid"],
                            event_type="LEASE_EXPIRED",
                            payload={
                                "lease_id": row["lease_id"],
                                "resource_id": row["resource_id"],
                            },
                        ),
                    )
                )
            self._journal.append_events_tx(tx, events)

        # Wait edges deliberately remain. They represent pending processes
        # waiting for the newly-free resource and are cleared by retry success
        # or explicit cancellation, not by expiry of the former owner's lease.
        return reclaimed

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

    def remove_waiter(self, pid: str, resource_id: str) -> int:
        """Remove one resource wait edge. Returns the number removed."""
        with self._storage.transaction() as tx:
            cur = tx.execute(
                "DELETE FROM lease_waiters WHERE pid = ? AND resource_id = ?",
                (pid, resource_id),
            )
        return cur.rowcount

    def clear_waiters_for_pid(self, pid: str) -> int:
        """Remove every wait edge owned by a process."""
        with self._storage.transaction() as tx:
            cur = tx.execute("DELETE FROM lease_waiters WHERE pid = ?", (pid,))
        return cur.rowcount

    def validate_action_contract(
        self,
        pid: str,
        claims: list[dict[str, Any]],
        lease_ids: list[str],
        now: datetime,
    ) -> tuple[bool, str | None]:
        """Validate persisted leases cover an action's resource claims."""
        if not claims:
            return (not lease_ids, None if not lease_ids else "unexpected_leases")
        if len(lease_ids) != len(claims) or len(set(lease_ids)) != len(lease_ids):
            return False, "lease_count_mismatch"

        leases: list[ResourceLease] = []
        check_now = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
        for lease_id in lease_ids:
            lease = self.get_lease(lease_id)
            if lease is None:
                return False, "lease_missing"
            if lease.owner_pid != pid:
                return False, "lease_owner_mismatch"
            expiry = lease.expires_at
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=UTC)
            if expiry <= check_now:
                return False, "lease_expired"
            leases.append(lease)

        expected = sorted(
            (claim["resource_id"], claim.get("mode", "exclusive")) for claim in claims
        )
        actual = sorted((lease.resource_id, lease.mode) for lease in leases)
        if actual != expected:
            return False, "lease_claim_mismatch"
        return True, None

    def capture_fencing_contract(
        self,
        pid: str,
        claims: list[dict[str, Any]],
        lease_ids: list[str],
        now: datetime,
    ) -> dict[str, int] | None:
        """Return the exact resource-token bundle currently authorizing work.

        The returned snapshot is immutable execution authority. A completion
        may commit only if every same lease still exists and no newer token
        has been issued for any resource.
        """
        valid, _ = self.validate_action_contract(pid, claims, lease_ids, now)
        if not valid:
            return None
        out: dict[str, int] = {}
        for lease_id in lease_ids:
            lease = self.get_lease(lease_id)
            if lease is None:
                return None
            out[lease.resource_id] = lease.fencing_token
        return out

    def validate_fencing_contract(
        self,
        pid: str,
        claims: list[dict[str, Any]],
        lease_ids: list[str],
        fencing_tokens: dict[str, int],
        now: datetime,
    ) -> tuple[bool, str | None]:
        """Validate an in-flight completion against monotonic resource fences."""
        valid, error = self.validate_action_contract(pid, claims, lease_ids, now)
        if not valid:
            return valid, error

        expected_resources = {claim["resource_id"] for claim in claims}
        if set(fencing_tokens) != expected_resources:
            return False, "fencing_token_count_mismatch"

        for lease_id in lease_ids:
            lease = self.get_lease(lease_id)
            if lease is None:
                return False, "lease_missing"
            expected_token = fencing_tokens.get(lease.resource_id)
            if expected_token != lease.fencing_token:
                return False, "lease_fencing_token_mismatch"
            token_row = self._storage.query_one(
                "SELECT last_token FROM resource_fencing_tokens WHERE resource_id = ?",
                (lease.resource_id,),
            )
            if token_row is None or int(token_row["last_token"]) != expected_token:
                return False, "resource_fencing_token_superseded"
        return True, None

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
                       (lease_id, resource_id, owner_pid, mode, fencing_token, acquired_at,
                        expires_at, renewable, revocable)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        lease.lease_id,
                        lease.resource_id,
                        lease.owner_pid,
                        lease.mode,
                        lease.fencing_token,
                        lease.acquired_at.isoformat(),
                        lease.expires_at.isoformat(),
                        int(lease.renewable),
                        int(lease.revocable),
                    ),
                )
                tx.execute(
                    """
                    INSERT INTO resource_fencing_tokens(resource_id, last_token)
                    VALUES (?, ?)
                    ON CONFLICT(resource_id) DO UPDATE SET
                        last_token = MAX(last_token, excluded.last_token)
                    """,
                    (lease.resource_id, lease.fencing_token),
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

    def _add_waiter(self, pid: str, resource_id: str) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT OR IGNORE INTO lease_waiters (pid, resource_id, wait_since)
                   VALUES (?, ?, ?)""",
                (pid, resource_id, datetime.now(UTC).isoformat()),
            )

    @staticmethod
    def _row_to_lease(row: dict[str, Any]) -> ResourceLease:
        return ResourceLease(
            lease_id=row["lease_id"],
            resource_id=row["resource_id"],
            owner_pid=row["owner_pid"],
            mode=row["mode"],
            fencing_token=int(row.get("fencing_token", 1)),
            acquired_at=datetime.fromisoformat(row["acquired_at"]),
            expires_at=datetime.fromisoformat(row["expires_at"]),
            renewable=bool(row["renewable"]),
            revocable=bool(row["revocable"]),
        )
