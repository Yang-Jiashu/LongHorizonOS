"""Deadlock-free, all-or-nothing resource-vector reservations."""

from __future__ import annotations

import threading
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field

from .models import ResourceVector


def _utcnow() -> datetime:
    return datetime.now(UTC)


class ResourceReservation(BaseModel):
    """One active reservation owned by a Scheduler claim."""

    reservation_id: str
    pool_id: str
    owner_id: str
    resources: ResourceVector
    created_at: datetime = Field(default_factory=_utcnow)


class ResourcePoolSnapshot(BaseModel):
    """Atomic, deterministic projection of one logical resource pool.

    This is intentionally a *logical scheduler* snapshot.  It reports only
    declared capacity and active reservations; it does not inspect host CPU,
    GPU, RAM, or VRAM telemetry and therefore must not be interpreted as
    physical device utilization.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    pool_id: str
    capacity: ResourceVector
    used: ResourceVector
    available: ResourceVector
    reservations: tuple[ResourceReservation, ...] = ()


class AtomicResourceManager:
    """Thread-safe resource accounting with no partial acquisition.

    A request is checked and committed while one lock is held. If any scalar
    or model-specific slot is unavailable, no resource is retained. This
    removes hold-and-wait from this allocator and therefore prevents resource
    deadlocks inside its authority boundary.
    """

    def __init__(self, capacities: dict[str, ResourceVector] | None = None) -> None:
        self._capacities = dict(capacities or {})
        self._reservations: dict[str, ResourceReservation] = {}
        self._owner_index: dict[str, str] = {}
        # Per-pool aggregate, maintained inside the same lock as the
        # reservations it summarizes.  ``used``/``available``/``shortages``/
        # ``can_reserve`` all resolve through it, and the Scheduler queries
        # shortages once per task per Agent, so recomputing the sum by scanning
        # every reservation made admission O(tasks * agents * reservations).
        self._used_by_pool: dict[str, ResourceVector] = {}
        self._lock = threading.RLock()

    def set_capacity(self, pool_id: str, capacity: ResourceVector) -> None:
        with self._lock:
            used = self._used_locked(pool_id)
            if not used.fits_within(capacity):
                raise ValueError(
                    f"capacity for {pool_id!r} is below active reservations: "
                    f"{used.shortages(capacity)}"
                )
            self._capacities[pool_id] = capacity

    def capacity(self, pool_id: str) -> ResourceVector:
        with self._lock:
            return self._capacities.get(pool_id, ResourceVector())

    def used(self, pool_id: str) -> ResourceVector:
        with self._lock:
            return self._used_locked(pool_id)

    def available(self, pool_id: str) -> ResourceVector:
        with self._lock:
            return self._capacities.get(pool_id, ResourceVector()).minus(self._used_locked(pool_id))

    def shortages(self, pool_id: str, request: ResourceVector) -> dict[str, int]:
        with self._lock:
            return request.shortages(self.available(pool_id))

    def can_reserve(self, pool_id: str, request: ResourceVector) -> bool:
        with self._lock:
            return request.fits_within(self.available(pool_id))

    def try_reserve(
        self,
        *,
        pool_id: str,
        owner_id: str,
        request: ResourceVector,
        reservation_id: str | None = None,
    ) -> ResourceReservation | None:
        """Reserve the complete vector or return ``None`` without side effects."""

        with self._lock:
            existing_id = self._owner_index.get(owner_id)
            if existing_id is not None:
                existing = self._reservations[existing_id]
                if existing.pool_id != pool_id or existing.resources != request:
                    raise ValueError(
                        f"owner {owner_id!r} already has a different resource reservation"
                    )
                return existing

            available = self.available(pool_id)
            if not request.fits_within(available):
                return None

            rid = reservation_id or f"reservation:{pool_id}:{owner_id}"
            if rid in self._reservations:
                raise ValueError(f"reservation_id {rid!r} already exists")
            reservation = ResourceReservation(
                reservation_id=rid,
                pool_id=pool_id,
                owner_id=owner_id,
                resources=request,
            )
            self._reservations[rid] = reservation
            self._owner_index[owner_id] = rid
            self._used_by_pool[pool_id] = self._used_by_pool.get(pool_id, ResourceVector()).plus(
                request
            )
            return reservation

    def release(self, reservation_or_owner_id: str) -> bool:
        with self._lock:
            reservation_id = self._owner_index.get(
                reservation_or_owner_id,
                reservation_or_owner_id,
            )
            reservation = self._reservations.pop(reservation_id, None)
            if reservation is None:
                return False
            self._owner_index.pop(reservation.owner_id, None)
            # ``minus`` validates non-negativity, so a drifted aggregate raises
            # here instead of silently under-reporting usage and over-admitting.
            self._used_by_pool[reservation.pool_id] = self._used_by_pool[reservation.pool_id].minus(
                reservation.resources
            )
            return True

    def get(self, reservation_id: str) -> ResourceReservation | None:
        with self._lock:
            return self._reservations.get(reservation_id)

    def for_owner(self, owner_id: str) -> ResourceReservation | None:
        with self._lock:
            reservation_id = self._owner_index.get(owner_id)
            return self._reservations.get(reservation_id) if reservation_id else None

    def list_active(self, pool_id: str | None = None) -> list[ResourceReservation]:
        with self._lock:
            reservations = list(self._reservations.values())
        if pool_id is not None:
            reservations = [item for item in reservations if item.pool_id == pool_id]
        return sorted(reservations, key=lambda item: item.reservation_id)

    def snapshot(self, pool_id: str | None = None) -> tuple[ResourcePoolSnapshot, ...]:
        """Return one deterministic, lock-consistent logical resource view.

        The capacity, aggregate usage, available vector, and reservation rows
        are captured under the same lock.  Callers can therefore reason about
        one coherent scheduler state instead of combining separate ``used`` /
        ``available`` / ``list_active`` calls that may observe different
        reservation epochs.  Physical host/device telemetry is deliberately
        outside this API.
        """

        with self._lock:
            pool_ids = set(self._capacities)
            pool_ids.update(item.pool_id for item in self._reservations.values())
            if pool_id is not None:
                pool_ids = {pool_id} if pool_id in pool_ids else set()

            snapshots: list[ResourcePoolSnapshot] = []
            for current_pool_id in sorted(pool_ids):
                capacity = self._capacities.get(current_pool_id, ResourceVector())
                reservations = tuple(
                    sorted(
                        (
                            item
                            for item in self._reservations.values()
                            if item.pool_id == current_pool_id
                        ),
                        key=lambda item: item.reservation_id,
                    )
                )
                # The audit boundary deliberately recomputes instead of trusting
                # the maintained aggregate.  Hot admission queries read the
                # aggregate for O(1), but a snapshot is where corrupted durable
                # state has to be caught, and an aggregate cannot see a
                # reservation row that was mutated behind its back.
                used = self._recompute_used_locked(current_pool_id)
                if not used.fits_within(capacity):
                    # This should be unreachable through the public mutation
                    # methods.  Refuse to publish a contradictory snapshot if
                    # a caller has corrupted internal durable state.
                    raise ValueError(
                        f"logical resource pool {current_pool_id!r} is overcommitted: "
                        f"{used.shortages(capacity)}"
                    )
                # Checked after overcommit so the louder, pre-existing failure
                # still wins.  This catches the quieter case: an aggregate that
                # drifted from its rows while still fitting inside capacity,
                # which no other path would ever notice.
                maintained = self._used_locked(current_pool_id)
                if used != maintained:
                    raise ValueError(
                        f"logical resource pool {current_pool_id!r} usage aggregate "
                        f"disagrees with its reservations: maintained {maintained}, "
                        f"summed {used}"
                    )
                snapshots.append(
                    ResourcePoolSnapshot(
                        pool_id=current_pool_id,
                        capacity=capacity,
                        used=used,
                        available=capacity.minus(used),
                        reservations=reservations,
                    )
                )
            return tuple(snapshots)

    def restore(self, reservations: list[ResourceReservation]) -> None:
        """Rebuild active accounting from durable Scheduler state.

        Restoration is fail-closed: duplicates or aggregate overcommit reject
        the entire candidate state before replacing the live projection.
        """

        rebuilt = AtomicResourceManager(self._capacities)
        for reservation in sorted(reservations, key=lambda item: item.reservation_id):
            restored = rebuilt.try_reserve(
                pool_id=reservation.pool_id,
                owner_id=reservation.owner_id,
                request=reservation.resources,
                reservation_id=reservation.reservation_id,
            )
            if restored is None:
                raise ValueError(
                    f"durable resource reservations overcommit pool {reservation.pool_id!r}"
                )
        with self._lock:
            self._reservations = rebuilt._reservations
            self._owner_index = rebuilt._owner_index
            self._used_by_pool = rebuilt._used_by_pool

    def _used_locked(self, pool_id: str) -> ResourceVector:
        return self._used_by_pool.get(pool_id, ResourceVector())

    def _recompute_used_locked(self, pool_id: str) -> ResourceVector:
        """Sum the reservations directly. Only for auditing the aggregate."""

        total = ResourceVector()
        for reservation in self._reservations.values():
            if reservation.pool_id == pool_id:
                total = total.plus(reservation.resources)
        return total
