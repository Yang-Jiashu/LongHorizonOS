"""Focused tests for the explicit host-telemetry capacity bridge."""

from __future__ import annotations

import threading
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

import lhos.sdk as sdk
from lhos.runtimes.multi_agent import ResourceVector
from lhos.sdk import Agent, AgentOS, ConfigurationError
from lhos.sdk.host_capacity import HostCapacityPolicy, derive_host_capacity
from lhos.sdk.resource_telemetry import (
    HostResourceTelemetry,
    ResourceTelemetryMetric,
)

OBSERVED_AT = datetime(2026, 8, 16, 0, 30, tzinfo=UTC)


def _metric(
    *,
    total: int | None,
    used: int | None,
    available: int | None,
    unit: str,
    is_available: bool = True,
    reason: str | None = None,
) -> ResourceTelemetryMetric:
    return ResourceTelemetryMetric(
        total=total,
        used=used,
        available=available,
        unit=unit,
        source="test",
        is_available=is_available,
        reason=reason,
    )


def _telemetry(
    *,
    cpu_available: int = 7,
    ram_available: int = 901,
    gpu_available: int | None = 2,
    vram_available: int | None = 777,
) -> HostResourceTelemetry:
    gpu_known = gpu_available is not None
    vram_known = vram_available is not None
    unavailable = tuple(
        name for name, known in (("gpu", gpu_known), ("vram", vram_known)) if not known
    )
    return HostResourceTelemetry(
        observed_at=OBSERVED_AT,
        platform="test-host",
        cpu=_metric(
            total=8,
            used=1,
            available=cpu_available,
            unit="cores",
        ),
        ram=_metric(
            total=1_001,
            used=100,
            available=ram_available,
            unit="bytes",
        ),
        gpu=(
            _metric(
                total=2,
                used=0,
                available=gpu_available,
                unit="devices",
            )
            if gpu_known
            else _metric(
                total=None,
                used=None,
                available=None,
                unit="devices",
                is_available=False,
                reason="no supported GPU probe",
            )
        ),
        vram=(
            _metric(
                total=1_000,
                used=223,
                available=vram_available,
                unit="bytes",
            )
            if vram_known
            else _metric(
                total=None,
                used=None,
                available=None,
                unit="bytes",
                is_available=False,
                reason="no supported VRAM probe",
            )
        ),
        available=True,
        complete=not unavailable,
        unavailable=unavailable,
    )


def test_derive_host_capacity_reserves_each_plane_and_rounds_down() -> None:
    policy = HostCapacityPolicy(
        cpu_reserve_fraction=0.10,
        ram_reserve_fraction=0.20,
        gpu_reserve_fraction=0.50,
        vram_reserve_fraction=0.25,
        require_gpu=True,
        model_slots={"reasoner": 2, "unused": 0},
    )

    decision = derive_host_capacity(_telemetry(), policy)

    assert decision.available
    assert not decision.fail_closed
    assert decision.capacity == ResourceVector(
        cpu_millis=6_300,
        ram_bytes=720,
        gpu_count=1,
        vram_bytes=582,
        model_slots={"reasoner": 2},
    )
    assert decision.unavailable == ()
    assert decision.observed_at == OBSERVED_AT
    assert len(decision.telemetry_hash) == 64
    assert len(decision.decision_hash) == 64
    assert decision.as_dict()["capacity"]["cpu_millis"] == 6_300


def test_derivation_is_deterministic_and_policy_changes_decision_identity() -> None:
    telemetry = _telemetry()
    policy = HostCapacityPolicy(require_gpu=True)

    first = derive_host_capacity(telemetry, policy)
    replay = derive_host_capacity(telemetry, policy)
    changed = derive_host_capacity(
        telemetry,
        policy.model_copy(update={"cpu_reserve_fraction": 0.20}),
    )

    assert first == replay
    assert first.telemetry_hash == changed.telemetry_hash
    assert first.decision_hash == replay.decision_hash
    assert first.decision_hash != changed.decision_hash


def test_cpu_only_policy_does_not_fabricate_gpu_capacity() -> None:
    decision = derive_host_capacity(
        _telemetry(gpu_available=None, vram_available=None),
        HostCapacityPolicy(
            require_gpu=False,
            cpu_reserve_fraction=0,
            ram_reserve_fraction=0,
            model_slots={"local-cpu-model": 1},
        ),
    )

    assert decision.available
    assert decision.capacity == ResourceVector(
        cpu_millis=7_000,
        ram_bytes=901,
        model_slots={"local-cpu-model": 1},
    )
    # Optional unknown physical planes stay explicit for audit. They do not
    # silently become observed zero-capacity devices.
    assert {item.name for item in decision.unavailable} == {"gpu", "vram"}


def test_required_unknown_gpu_fails_closed_without_partial_capacity() -> None:
    decision = derive_host_capacity(
        _telemetry(gpu_available=None, vram_available=None),
        HostCapacityPolicy(require_gpu=True),
    )

    assert not decision.available
    assert decision.fail_closed
    assert decision.capacity is None
    assert {item.name for item in decision.unavailable} == {"gpu", "vram"}


def test_invalid_authoritative_metric_fails_closed() -> None:
    telemetry = _telemetry().model_copy(
        update={
            "cpu": _metric(
                total=8,
                used=1,
                available=7,
                unit="bytes",
            )
        }
    )

    decision = derive_host_capacity(
        telemetry,
        HostCapacityPolicy(require_gpu=False),
    )

    assert not decision.available
    assert decision.capacity is None
    assert decision.unavailable[0].name == "cpu"
    assert "unit" in decision.unavailable[0].reason


@pytest.mark.parametrize(
    "kwargs",
    [
        {"cpu_reserve_fraction": -0.01},
        {"ram_reserve_fraction": 1.01},
        {"require_gpu": 1},
        {"model_slots": {"": 1}},
        {"model_slots": {"reasoner": -1}},
    ],
)
def test_host_capacity_policy_rejects_ambiguous_configuration(kwargs) -> None:
    with pytest.raises(ValidationError):
        HostCapacityPolicy(**kwargs)


def test_host_capacity_types_are_exported_from_public_sdk() -> None:
    assert sdk.HostCapacityPolicy is HostCapacityPolicy
    assert sdk.derive_host_capacity is derive_host_capacity
    assert sdk.HostCapacityDecision.__name__ == "HostCapacityDecision"
    assert sdk.HostCapacityApplyResult.__name__ == "HostCapacityApplyResult"


def test_apply_host_capacity_updates_only_the_named_logical_pool() -> None:
    runtime = AgentOS(":memory:")
    try:
        runtime.add_agent(
            Agent(
                "worker",
                resource_capacity={
                    "cpu_millis": 1_000,
                    "ram_bytes": 1_000,
                },
            )
        )
        runtime.add_agent(
            Agent(
                "other",
                resource_capacity={
                    "cpu_millis": 333,
                    "ram_bytes": 444,
                },
            )
        )
        other_before = runtime.scheduler.resource_manager.capacity("other")

        result = runtime.apply_host_capacity(
            "worker",
            _telemetry(gpu_available=None, vram_available=None),
            HostCapacityPolicy(
                require_gpu=False,
                cpu_reserve_fraction=0,
                ram_reserve_fraction=0,
                model_slots={"reasoner": 2},
            ),
        )

        expected = ResourceVector(
            cpu_millis=7_000,
            ram_bytes=901,
            model_slots={"reasoner": 2},
        )
        assert result.applied
        assert result.previous_capacity == ResourceVector(
            cpu_millis=1_000,
            ram_bytes=1_000,
        )
        assert result.applied_capacity == expected
        assert runtime.scheduler.resource_manager.capacity("worker") == expected
        assert runtime.scheduler.resource_manager.capacity("other") == other_before
        assert runtime._registry.get("worker").resource_capacity == expected
        assert runtime._agents["worker"].resource_capacity == expected
    finally:
        runtime.close()


def test_apply_host_capacity_rejects_unknown_pool_and_read_only_runtime(
    tmp_path,
) -> None:
    runtime = AgentOS(":memory:")
    try:
        with pytest.raises(ConfigurationError, match=r"unknown|registered"):
            runtime.apply_host_capacity(
                "missing",
                _telemetry(gpu_available=None, vram_available=None),
                HostCapacityPolicy(require_gpu=False),
            )
    finally:
        runtime.close()

    db = tmp_path / "read-only.sqlite"
    writable = AgentOS(str(db))
    writable.close()
    read_only = AgentOS(str(db), read_only=True)
    try:
        with pytest.raises(ConfigurationError, match="read-only"):
            read_only.apply_host_capacity(
                "worker",
                _telemetry(gpu_available=None, vram_available=None),
                HostCapacityPolicy(require_gpu=False),
            )
    finally:
        read_only.close()


def test_apply_host_capacity_does_not_mutate_on_unavailable_decision() -> None:
    runtime = AgentOS(":memory:")
    try:
        original = ResourceVector(cpu_millis=2_000, ram_bytes=4_000)
        runtime.add_agent(Agent("worker", resource_capacity=original))

        result = runtime.apply_host_capacity(
            "worker",
            _telemetry(gpu_available=None, vram_available=None),
            HostCapacityPolicy(require_gpu=True),
        )

        assert not result.applied
        assert result.applied_capacity is None
        assert result.decision.fail_closed
        assert runtime.scheduler.resource_manager.capacity("worker") == original
        assert runtime._registry.get("worker").resource_capacity == original
        assert runtime._agents["worker"].resource_capacity == original
    finally:
        runtime.close()


def test_apply_host_capacity_atomically_rejects_capacity_below_live_reservation() -> None:
    runtime = AgentOS(":memory:")
    try:
        original = ResourceVector(cpu_millis=4_000, ram_bytes=8_000)
        runtime.add_agent(Agent("worker", resource_capacity=original))
        reservation = runtime.scheduler.resource_manager.try_reserve(
            pool_id="worker",
            owner_id="live-claim",
            request=ResourceVector(cpu_millis=2_000, ram_bytes=2_000),
        )
        assert reservation is not None

        telemetry = _telemetry(
            cpu_available=1,
            ram_available=901,
            gpu_available=None,
            vram_available=None,
        )
        with pytest.raises(ConfigurationError, match=r"active reservations|below"):
            runtime.apply_host_capacity(
                "worker",
                telemetry,
                HostCapacityPolicy(
                    require_gpu=False,
                    cpu_reserve_fraction=0,
                    ram_reserve_fraction=0,
                ),
            )

        assert runtime.scheduler.resource_manager.capacity("worker") == original
        assert runtime.scheduler.resource_manager.for_owner("live-claim") == reservation
        assert runtime._registry.get("worker").resource_capacity == original
        assert runtime._agents["worker"].resource_capacity == original
    finally:
        runtime.close()


def test_apply_host_capacity_rolls_back_when_registry_update_fails(
    monkeypatch,
) -> None:
    runtime = AgentOS(":memory:")
    try:
        original = ResourceVector(cpu_millis=2_000, ram_bytes=3_000)
        runtime.add_agent(Agent("worker", resource_capacity=original))

        def fail_update(*args, **kwargs):
            del args, kwargs
            raise RuntimeError("registry write failed")

        monkeypatch.setattr(runtime._registry, "update", fail_update)
        with pytest.raises(ConfigurationError, match=r"registry write failed|apply"):
            runtime.apply_host_capacity(
                "worker",
                _telemetry(gpu_available=None, vram_available=None),
                HostCapacityPolicy(require_gpu=False),
            )

        assert runtime.scheduler.resource_manager.capacity("worker") == original
        assert runtime._registry.get("worker").resource_capacity == original
        assert runtime._agents["worker"].resource_capacity == original
    finally:
        runtime.close()


def test_apply_host_capacity_and_registry_refresh_share_scheduler_lock(
    monkeypatch,
) -> None:
    """A concurrent refresh cannot restore the old registry capacity mid-apply."""

    runtime = AgentOS(":memory:")
    try:
        original = ResourceVector(cpu_millis=2_000, ram_bytes=3_000)
        runtime.add_agent(Agent("worker", resource_capacity=original))
        entered_registry_update = threading.Event()
        release_registry_update = threading.Event()
        refresh_finished = threading.Event()
        failures: list[BaseException] = []
        original_update = runtime._registry.update

        def blocking_update(*args, **kwargs):
            entered_registry_update.set()
            if not release_registry_update.wait(timeout=2):
                raise TimeoutError("test did not release registry update")
            return original_update(*args, **kwargs)

        monkeypatch.setattr(runtime._registry, "update", blocking_update)

        def apply() -> None:
            try:
                runtime.apply_host_capacity(
                    "worker",
                    _telemetry(gpu_available=None, vram_available=None),
                    HostCapacityPolicy(
                        require_gpu=False,
                        cpu_reserve_fraction=0,
                        ram_reserve_fraction=0,
                    ),
                )
            except BaseException as exc:  # pragma: no cover - asserted below
                failures.append(exc)

        def refresh() -> None:
            try:
                runtime.scheduler.refresh_registry_resources()
                refresh_finished.set()
            except BaseException as exc:  # pragma: no cover - asserted below
                failures.append(exc)

        apply_thread = threading.Thread(target=apply)
        apply_thread.start()
        assert entered_registry_update.wait(timeout=2)

        refresh_thread = threading.Thread(target=refresh)
        refresh_thread.start()
        # The apply call still owns the Scheduler lifecycle lock while its
        # registry publication is paused, so refresh must not complete here.
        assert not refresh_finished.wait(timeout=0.05)

        release_registry_update.set()
        apply_thread.join(timeout=2)
        refresh_thread.join(timeout=2)

        assert not apply_thread.is_alive()
        assert not refresh_thread.is_alive()
        assert failures == []
        assert refresh_finished.is_set()
        expected = ResourceVector(cpu_millis=7_000, ram_bytes=901)
        assert runtime.scheduler.resource_manager.capacity("worker") == expected
        assert runtime._registry.get("worker").resource_capacity == expected
        assert runtime._agents["worker"].resource_capacity == expected
    finally:
        runtime.close()
