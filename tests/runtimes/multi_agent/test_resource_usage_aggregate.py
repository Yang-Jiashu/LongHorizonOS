"""The per-pool usage aggregate must never drift from the reservations it sums.

``used``/``available``/``shortages``/``can_reserve`` used to recompute usage by
scanning every reservation in every pool. The Scheduler asks for shortages once
per task per Agent, so admission was ``O(tasks * agents * reservations)``. The
aggregate is now maintained incrementally in ``try_reserve``/``release``.

That trades a scan for a durable invariant, and the invariant is a *safety*
property: an aggregate that under-reports usage would let the manager admit work
beyond capacity. So these tests do not check performance -- they check that the
maintained value equals the directly summed value after arbitrary interleavings,
using ``_recompute_used_locked`` as the oracle.

``ResourceVector.minus`` validates non-negativity, so drift in the unsafe
direction raises on release rather than silently corrupting the accounting. The
tests below cover the safe direction too, where a stale surplus would instead
cause spurious admission refusals.
"""

from __future__ import annotations

import random

import pytest

from lhos.runtimes.multi_agent.models import ResourceVector
from lhos.runtimes.multi_agent.resources import AtomicResourceManager

POOLS = ("pool-a", "pool-b", "pool-c")


def _vector(rnd: random.Random) -> ResourceVector:
    return ResourceVector(
        cpu_millis=rnd.randrange(0, 500),
        ram_bytes=rnd.randrange(0, 1024),
        gpu_count=rnd.randrange(0, 2),
        vram_bytes=rnd.randrange(0, 256),
        model_slots={
            name: rnd.randrange(1, 3) for name in rnd.sample(["m1", "m2"], rnd.randint(0, 2))
        },
    )


def _assert_aggregate_matches(manager: AtomicResourceManager) -> None:
    for pool_id in POOLS:
        assert manager.used(pool_id) == manager._recompute_used_locked(pool_id), pool_id


def test_aggregate_matches_direct_sum_under_random_reserve_and_release() -> None:
    """Randomised interleavings, checked against the scanning oracle each step."""

    capacity = ResourceVector(
        cpu_millis=10**7,
        ram_bytes=10**7,
        gpu_count=10**4,
        vram_bytes=10**7,
        model_slots={"m1": 10**4, "m2": 10**4},
    )
    for seed in range(25):
        rnd = random.Random(seed)
        manager = AtomicResourceManager({pool: capacity for pool in POOLS})
        live: list[str] = []
        for step in range(60):
            if live and rnd.random() < 0.4:
                manager.release(live.pop(rnd.randrange(len(live))))
            else:
                owner = f"owner-{seed}-{step}"
                pool = rnd.choice(POOLS)
                if (
                    manager.try_reserve(pool_id=pool, owner_id=owner, request=_vector(rnd))
                    is not None
                ):
                    live.append(owner)
            _assert_aggregate_matches(manager)

        for owner in list(live):
            manager.release(owner)
        _assert_aggregate_matches(manager)
        for pool_id in POOLS:
            assert manager.used(pool_id).is_zero, pool_id


def test_releasing_everything_returns_usage_to_zero() -> None:
    """``plus`` then ``minus`` must be exactly invertible.

    ``model_slots`` drops zero-valued entries during validation, so a vector that
    added a slot and then removed it has to compare equal to the empty vector
    rather than to one carrying ``{"m1": 0}``.
    """

    manager = AtomicResourceManager(
        {"pool-a": ResourceVector(cpu_millis=1000, model_slots={"m1": 4})}
    )
    request = ResourceVector(cpu_millis=250, model_slots={"m1": 2})
    assert manager.try_reserve(pool_id="pool-a", owner_id="o1", request=request) is not None
    assert manager.used("pool-a") == request

    assert manager.release("o1") is True
    assert manager.used("pool-a") == ResourceVector()
    assert manager.used("pool-a").is_zero


def test_idempotent_reserve_does_not_double_count() -> None:
    """Re-reserving the same owner returns the existing row and adds nothing."""

    manager = AtomicResourceManager({"pool-a": ResourceVector(cpu_millis=1000)})
    request = ResourceVector(cpu_millis=400)
    first = manager.try_reserve(pool_id="pool-a", owner_id="o1", request=request)
    second = manager.try_reserve(pool_id="pool-a", owner_id="o1", request=request)

    assert first is not None and second is not None
    assert second.reservation_id == first.reservation_id
    assert manager.used("pool-a") == request
    assert manager.used("pool-a") == manager._recompute_used_locked("pool-a")


def test_refused_reservation_leaves_usage_untouched() -> None:
    """A rejected request must not leak into the aggregate."""

    manager = AtomicResourceManager({"pool-a": ResourceVector(cpu_millis=500)})
    assert (
        manager.try_reserve(
            pool_id="pool-a", owner_id="big", request=ResourceVector(cpu_millis=900)
        )
        is None
    )
    assert manager.used("pool-a").is_zero
    assert manager.used("pool-a") == manager._recompute_used_locked("pool-a")


def test_failed_release_leaves_usage_untouched() -> None:
    manager = AtomicResourceManager({"pool-a": ResourceVector(cpu_millis=500)})
    manager.try_reserve(pool_id="pool-a", owner_id="o1", request=ResourceVector(cpu_millis=100))

    assert manager.release("does-not-exist") is False
    assert manager.used("pool-a") == ResourceVector(cpu_millis=100)
    assert manager.used("pool-a") == manager._recompute_used_locked("pool-a")


def test_pools_are_accounted_independently() -> None:
    """A shared aggregate would let one pool's usage suppress another's."""

    capacity = ResourceVector(cpu_millis=1000)
    manager = AtomicResourceManager({pool: capacity for pool in POOLS})
    manager.try_reserve(pool_id="pool-a", owner_id="a", request=ResourceVector(cpu_millis=600))
    manager.try_reserve(pool_id="pool-b", owner_id="b", request=ResourceVector(cpu_millis=200))

    assert manager.used("pool-a") == ResourceVector(cpu_millis=600)
    assert manager.used("pool-b") == ResourceVector(cpu_millis=200)
    assert manager.used("pool-c").is_zero
    assert manager.available("pool-a") == ResourceVector(cpu_millis=400)


def test_capacity_reduction_still_sees_active_usage() -> None:
    """``set_capacity`` reads the aggregate; a stale zero would allow overcommit."""

    manager = AtomicResourceManager({"pool-a": ResourceVector(cpu_millis=1000)})
    manager.try_reserve(pool_id="pool-a", owner_id="o1", request=ResourceVector(cpu_millis=800))

    with pytest.raises(ValueError, match="below active reservations"):
        manager.set_capacity("pool-a", ResourceVector(cpu_millis=500))


def test_snapshot_catches_aggregate_drift_that_still_fits_capacity() -> None:
    """The quiet corruption case, which nothing else would notice.

    ``snapshot`` recomputes usage from the reservation rows rather than trusting
    the maintained aggregate, precisely because an aggregate cannot see a row
    mutated behind its back. Overcommit is reported first because that check
    predates the aggregate; this covers the case where the rows and the aggregate
    disagree while both still fit inside capacity, so no other path fails.
    """

    manager = AtomicResourceManager({"pool-a": ResourceVector(cpu_millis=1000)})
    reservation = manager.try_reserve(
        pool_id="pool-a", owner_id="o1", request=ResourceVector(cpu_millis=100)
    )
    assert reservation is not None
    assert manager.snapshot("pool-a")  # healthy state publishes fine

    with manager._lock:
        manager._reservations[reservation.reservation_id] = reservation.model_copy(
            update={"resources": ResourceVector(cpu_millis=200)}
        )

    with pytest.raises(ValueError, match="disagrees with its reservations"):
        manager.snapshot("pool-a")
