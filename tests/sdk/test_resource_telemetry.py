"""Focused tests for the optional host-resource telemetry adapter."""

from __future__ import annotations

from datetime import UTC, datetime
from subprocess import CompletedProcess

import pytest

import lhos.sdk.resource_telemetry as telemetry
from lhos.sdk import (
    RESOURCE_TELEMETRY_SCHEMA_VERSION,
    HostResourceTelemetry,
    collect_host_resource_telemetry,
)


def test_collect_cpu_ram_without_gpu_is_best_effort_and_jsonable() -> None:
    observed = datetime(2026, 8, 15, 23, 0, tzinfo=UTC)
    result = collect_host_resource_telemetry(
        include_gpu=False,
        now=lambda: observed,
    )

    assert isinstance(result, HostResourceTelemetry)
    assert result.schema_version == RESOURCE_TELEMETRY_SCHEMA_VERSION
    assert result.observed_at == observed
    assert result.cpu.is_available
    assert result.cpu.total is not None and result.cpu.total >= 1
    assert result.ram.is_available
    assert result.ram.total is not None and result.ram.total > 0
    assert not result.gpu.is_available
    assert not result.vram.is_available
    assert result.unavailable == ("gpu", "vram")
    assert result.available
    assert not result.complete
    assert result.as_dict()["schema_version"] == RESOURCE_TELEMETRY_SCHEMA_VERSION


def test_nvidia_csv_probe_is_aggregated_without_scheduler_mutation(monkeypatch) -> None:
    monkeypatch.setattr(telemetry.shutil, "which", lambda _: "nvidia-smi")

    calls: list[tuple[tuple[str, ...], float]] = []

    def runner(command, *, timeout):
        calls.append((tuple(command), timeout))
        return CompletedProcess(
            command,
            0,
            stdout="100,25,75\n200,50,150\n",
            stderr="",
        )

    result = collect_host_resource_telemetry(
        include_gpu=True,
        command_runner=runner,
        now=lambda: datetime(2026, 8, 15, tzinfo=UTC),
    )

    assert result.gpu.is_available
    assert result.gpu.total == 2
    assert result.gpu.available == 2
    assert result.vram.is_available
    assert result.vram.total == 300 * 1024 * 1024
    assert result.vram.used == 75 * 1024 * 1024
    assert result.vram.available == 225 * 1024 * 1024
    assert result.complete
    assert result.unavailable == ()
    assert calls and calls[0][1] == pytest.approx(1.5)


def test_missing_nvidia_tool_is_explicitly_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(telemetry.shutil, "which", lambda _: None)
    result = collect_host_resource_telemetry(include_gpu=True)

    assert not result.gpu.is_available
    assert not result.vram.is_available
    assert result.gpu.total is None
    assert "nvidia-smi" in (result.gpu.reason or "")
    assert result.unavailable == ("gpu", "vram")


def test_malformed_nvidia_output_fails_closed(monkeypatch) -> None:
    monkeypatch.setattr(telemetry.shutil, "which", lambda _: "nvidia-smi")

    def runner(command, *, timeout):
        return CompletedProcess(command, 0, stdout="not,csv\n", stderr="")

    result = collect_host_resource_telemetry(command_runner=runner)

    assert not result.gpu.is_available
    assert not result.vram.is_available
    assert result.gpu.reason and "malformed" in result.gpu.reason
    assert result.vram.reason == result.gpu.reason


def test_probe_configuration_is_validated() -> None:
    with pytest.raises(ValueError, match="gpu_timeout_seconds"):
        collect_host_resource_telemetry(gpu_timeout_seconds=0)
    with pytest.raises(TypeError, match="include_gpu"):
        collect_host_resource_telemetry(include_gpu=1)  # type: ignore[arg-type]
