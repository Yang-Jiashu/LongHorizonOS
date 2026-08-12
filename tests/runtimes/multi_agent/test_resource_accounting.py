"""Atomic CPU/GPU/RAM/VRAM/model-slot accounting."""

from __future__ import annotations

import threading

import pytest

from lhos.runtimes.multi_agent import AtomicResourceManager, ResourceVector
from lhos.runtimes.multi_agent.requirements import decode_task_requirements
from lhos.runtimes.multi_agent.resources import ResourceReservation


def _capacity() -> ResourceVector:
    return ResourceVector(
        cpu_millis=4000,
        ram_bytes=16_000,
        gpu_count=2,
        vram_bytes=48_000,
        model_slots={"large": 2, "small": 4},
    )


def test_resource_vector_rejects_negative_quantities() -> None:
    with pytest.raises(ValueError):
        ResourceVector(cpu_millis=-1)
    with pytest.raises(ValueError):
        ResourceVector(model_slots={"large": -1})


def test_task_requirements_decode_structured_resource_vector() -> None:
    requirements = decode_task_requirements(
        "task",
        {
            "metadata": {
                "scheduler": {
                    "resources": {
                        "cpu_millis": 1500,
                        "ram_bytes": 2048,
                        "gpu_count": 1,
                        "vram_bytes": 8192,
                        "model_slots": {"large": 1},
                    }
                }
            }
        },
    )
    assert requirements.resources == ResourceVector(
        cpu_millis=1500,
        ram_bytes=2048,
        gpu_count=1,
        vram_bytes=8192,
        model_slots={"large": 1},
    )


def test_reservation_is_all_or_nothing_and_idempotent() -> None:
    manager = AtomicResourceManager({"agent": _capacity()})
    first = manager.try_reserve(
        pool_id="agent",
        owner_id="claim-1",
        request=ResourceVector(cpu_millis=3000, gpu_count=1, model_slots={"large": 1}),
    )
    assert first is not None
    before = manager.available("agent")

    refused = manager.try_reserve(
        pool_id="agent",
        owner_id="claim-2",
        request=ResourceVector(cpu_millis=2000, gpu_count=1, model_slots={"large": 1}),
    )
    assert refused is None
    assert manager.available("agent") == before
    assert manager.for_owner("claim-2") is None

    replay = manager.try_reserve(
        pool_id="agent",
        owner_id="claim-1",
        request=first.resources,
    )
    assert replay == first


def test_atomic_vectors_remove_hold_and_wait_deadlock() -> None:
    manager = AtomicResourceManager(
        {
            "pool": ResourceVector(
                cpu_millis=1000,
                gpu_count=1,
            )
        }
    )
    barrier = threading.Barrier(3)
    results: list[bool] = []

    def reserve(owner: str) -> None:
        barrier.wait()
        reservation = manager.try_reserve(
            pool_id="pool",
            owner_id=owner,
            request=ResourceVector(cpu_millis=1000, gpu_count=1),
        )
        results.append(reservation is not None)

    threads = [
        threading.Thread(target=reserve, args=("cpu-first",)),
        threading.Thread(target=reserve, args=("gpu-first",)),
    ]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(2)
        assert not thread.is_alive()

    assert sorted(results) == [False, True]
    assert len(manager.list_active("pool")) == 1
    assert manager.available("pool") == ResourceVector()


def test_release_returns_every_resource_dimension() -> None:
    manager = AtomicResourceManager({"agent": _capacity()})
    reservation = manager.try_reserve(
        pool_id="agent",
        owner_id="claim",
        request=ResourceVector(
            cpu_millis=500,
            ram_bytes=1000,
            gpu_count=1,
            vram_bytes=12_000,
            model_slots={"large": 1, "small": 2},
        ),
    )
    assert reservation is not None
    assert manager.release(reservation.reservation_id)
    assert manager.available("agent") == _capacity()
    assert not manager.release(reservation.reservation_id)


def test_restore_fails_closed_on_overcommit_without_partial_state() -> None:
    manager = AtomicResourceManager(
        {"pool": ResourceVector(cpu_millis=1000, model_slots={"large": 1})}
    )
    original = manager.try_reserve(
        pool_id="pool",
        owner_id="live",
        request=ResourceVector(cpu_millis=500),
    )
    assert original is not None

    with pytest.raises(ValueError, match="overcommit"):
        manager.restore(
            [
                ResourceReservation(
                    reservation_id="r1",
                    pool_id="pool",
                    owner_id="one",
                    resources=ResourceVector(cpu_millis=1000),
                ),
                ResourceReservation(
                    reservation_id="r2",
                    pool_id="pool",
                    owner_id="two",
                    resources=ResourceVector(cpu_millis=1),
                ),
            ]
        )

    assert manager.list_active() == [original]
