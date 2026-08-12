"""Deadlock-free, all-or-nothing resource-vector reservations."""

from __future__ import annotations

import threading
from datetime import UTC, datetime

from pydantic import BaseModel, Field

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

    def _used_locked(self, pool_id: str) -> ResourceVector:
        total = ResourceVector()
        for reservation in self._reservations.values():
            if reservation.pool_id == pool_id:
                total = total.plus(reservation.resources)
        return total
