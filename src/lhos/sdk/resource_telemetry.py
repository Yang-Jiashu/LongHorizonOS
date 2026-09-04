"""Optional host-resource telemetry for LongHorizonOS.

The scheduler's :class:`~lhos.sdk.runtime_state.ResourceRuntimeState` remains
the authority for *logical* admission.  This module is deliberately a
side-channel observation adapter: it samples host CPU/RAM and, when available,
NVIDIA GPU/VRAM information without changing scheduler decisions.

The adapter is best-effort and fail-closed.  Every resource plane carries its
own ``available`` bit and a bounded reason when the host cannot provide an
authoritative value (for example, a machine without ``nvidia-smi``).  Callers
must not interpret an unavailable value as zero capacity.
"""

from __future__ import annotations

import ctypes
import os
import platform
import shutil
import subprocess
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any, Final

from pydantic import BaseModel, ConfigDict, Field

RESOURCE_TELEMETRY_SCHEMA_VERSION: Final[str] = "resource-telemetry.v1"
_MAX_REASON_LENGTH: Final[int] = 240


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ResourceTelemetryMetric(_FrozenModel):
    """One host metric in a single unit.

    ``total``/``used``/``available`` are intentionally nullable.  ``None``
    means that the adapter could not establish an authoritative value; it does
    not mean zero.  ``unit`` is a stable machine-readable label (``cores``,
    ``bytes``, or ``devices``).
    """

    total: int | None = Field(default=None, ge=0)
    used: int | None = Field(default=None, ge=0)
    available: int | None = Field(default=None, ge=0)
    unit: str
    source: str = ""
    is_available: bool = False
    reason: str | None = None


class HostResourceTelemetry(_FrozenModel):
    """Point-in-time host resource observation.

    ``cpu`` reports logical CPU capacity (not a physical scheduler quota);
    ``ram`` reports host memory; ``gpu`` reports the number of NVIDIA devices
    visible to ``nvidia-smi``; and ``vram`` reports aggregate NVIDIA memory.
    The GPU planes are unavailable, rather than fabricated as zero, when no
    supported driver/tool is present.
    """

    schema_version: str = RESOURCE_TELEMETRY_SCHEMA_VERSION
    observed_at: datetime
    platform: str
    cpu: ResourceTelemetryMetric
    ram: ResourceTelemetryMetric
    gpu: ResourceTelemetryMetric
    vram: ResourceTelemetryMetric
    available: bool
    complete: bool
    unavailable: tuple[str, ...] = ()

    @property
    def any_available(self) -> bool:
        """Alias useful to callers that prefer an explicit name."""

        return self.available

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible immutable observation projection."""

        return self.model_dump(mode="json")


def _reason(value: str) -> str:
    text = " ".join(str(value).replace("\r", " ").replace("\n", " ").split())
    return text[:_MAX_REASON_LENGTH] or "telemetry unavailable"


def _unavailable(unit: str, reason: str, *, source: str = "") -> ResourceTelemetryMetric:
    return ResourceTelemetryMetric(
        unit=unit,
        source=source,
        is_available=False,
        reason=_reason(reason),
    )


def _available(
    *,
    total: int,
    used: int | None,
    available: int | None,
    unit: str,
    source: str,
) -> ResourceTelemetryMetric:
    # A provider returning contradictory values is treated as unavailable
    # instead of being silently clamped.  This makes downstream admission
    # policies fail closed.
    if total < 0 or (used is not None and used < 0) or (available is not None and available < 0):
        return _unavailable(unit, "provider returned a negative quantity", source=source)
    if used is not None and available is not None and used + available > total:
        return _unavailable(
            unit,
            "provider returned used + available greater than total",
            source=source,
        )
    return ResourceTelemetryMetric(
        total=int(total),
        used=None if used is None else int(used),
        available=None if available is None else int(available),
        unit=unit,
        source=source,
        is_available=True,
    )


def _collect_cpu() -> ResourceTelemetryMetric:
    source = "os.cpu_count"
    try:
        count = os.cpu_count()
    except Exception as exc:  # pragma: no cover - defensive provider boundary
        return _unavailable("cores", f"CPU probe failed: {type(exc).__name__}", source=source)
    if count is None or int(count) < 1:
        return _unavailable(
            "cores", "OS did not report a positive logical CPU count", source=source
        )
    count = int(count)
    return _available(
        total=count,
        used=None,
        available=count,
        unit="cores",
        source=source,
    )


def _collect_ram_windows() -> ResourceTelemetryMetric:
    source = "GlobalMemoryStatusEx"
    try:

        class _MemoryStatusEx(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = _MemoryStatusEx()
        status.dwLength = ctypes.sizeof(_MemoryStatusEx)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return _unavailable("bytes", "GlobalMemoryStatusEx returned failure", source=source)
        total = int(status.ullTotalPhys)
        available = int(status.ullAvailPhys)
        return _available(
            total=total,
            used=max(0, total - available),
            available=available,
            unit="bytes",
            source=source,
        )
    except Exception as exc:
        return _unavailable(
            "bytes", f"Windows RAM probe failed: {type(exc).__name__}", source=source
        )


def _collect_ram_posix() -> ResourceTelemetryMetric:
    source = "os.sysconf"
    try:
        # ``os.sysconf`` is absent from the Windows typeshed surface even
        # though this branch is only reached on POSIX.  Resolve it explicitly
        # and fail closed if a platform does not expose the probe.
        sysconf = getattr(os, "sysconf", None)
        if not callable(sysconf):
            return _unavailable("bytes", "os.sysconf is unavailable", source=source)
        page_size = int(sysconf("SC_PAGE_SIZE"))
        total_pages = int(sysconf("SC_PHYS_PAGES"))
        available_pages = int(sysconf("SC_AVPHYS_PAGES"))
        total = page_size * total_pages
        available = page_size * available_pages
        return _available(
            total=total,
            used=max(0, total - available),
            available=available,
            unit="bytes",
            source=source,
        )
    except Exception as exc:
        return _unavailable("bytes", f"POSIX RAM probe failed: {type(exc).__name__}", source=source)


def _collect_ram() -> ResourceTelemetryMetric:
    if os.name == "nt":
        return _collect_ram_windows()
    return _collect_ram_posix()


def _default_command_runner(
    command: Sequence[str],
    *,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _collect_nvidia(
    *,
    command_runner: Callable[..., subprocess.CompletedProcess[str]] = _default_command_runner,
    timeout_seconds: float = 1.5,
) -> tuple[ResourceTelemetryMetric, ResourceTelemetryMetric]:
    """Collect aggregate NVIDIA device and VRAM metrics.

    This deliberately uses the stable CSV interface and does not import a
    heavyweight GPU framework.  A missing executable, timeout, non-zero exit,
    or malformed row makes both GPU planes unavailable.
    """

    executable = shutil.which("nvidia-smi")
    if not executable:
        reason = "nvidia-smi is not installed or not on PATH"
        return (
            _unavailable("devices", reason, source="nvidia-smi"),
            _unavailable("bytes", reason, source="nvidia-smi"),
        )
    command = (
        executable,
        "--query-gpu=memory.total,memory.used,memory.free",
        "--format=csv,noheader,nounits",
    )
    try:
        completed = command_runner(command, timeout=float(timeout_seconds))
    except Exception as exc:
        reason = f"nvidia-smi probe failed: {type(exc).__name__}"
        return (
            _unavailable("devices", reason, source="nvidia-smi"),
            _unavailable("bytes", reason, source="nvidia-smi"),
        )
    if int(getattr(completed, "returncode", 1)) != 0:
        detail = str(getattr(completed, "stderr", "") or "").strip()
        reason = "nvidia-smi returned non-zero status"
        if detail:
            reason += f": {detail}"
        return (
            _unavailable("devices", reason, source="nvidia-smi"),
            _unavailable("bytes", reason, source="nvidia-smi"),
        )

    rows = [
        line.strip() for line in str(getattr(completed, "stdout", "")).splitlines() if line.strip()
    ]
    if not rows:
        reason = "nvidia-smi returned no GPU rows"
        return (
            _unavailable("devices", reason, source="nvidia-smi"),
            _unavailable("bytes", reason, source="nvidia-smi"),
        )
    totals = [0, 0, 0]
    try:
        for row in rows:
            values = [int(part.strip()) for part in row.split(",")]
            if len(values) != 3 or any(value < 0 for value in values):
                raise ValueError("expected three non-negative CSV quantities")
            if values[1] + values[2] > values[0]:
                raise ValueError("used + free exceeds total")
            for index, value in enumerate(values):
                # nvidia-smi reports MiB when nounits is requested.
                totals[index] += value * 1024 * 1024
    except (TypeError, ValueError) as exc:
        reason = f"nvidia-smi output is malformed: {exc}"
        return (
            _unavailable("devices", reason, source="nvidia-smi"),
            _unavailable("bytes", reason, source="nvidia-smi"),
        )

    device_count = len(rows)
    total, used, free = totals
    return (
        _available(
            total=device_count,
            used=None,
            available=device_count,
            unit="devices",
            source="nvidia-smi",
        ),
        _available(
            total=total,
            used=used,
            available=free,
            unit="bytes",
            source="nvidia-smi",
        ),
    )


def collect_host_resource_telemetry(
    *,
    include_gpu: bool = True,
    gpu_timeout_seconds: float = 1.5,
    command_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    now: Callable[[], datetime] | None = None,
) -> HostResourceTelemetry:
    """Sample host CPU/RAM and optional NVIDIA GPU/VRAM telemetry.

    The function performs no scheduler mutation and never treats a failed
    probe as zero.  ``command_runner`` and ``now`` are injectable solely for
    deterministic tests and embedding environments.
    """

    if isinstance(include_gpu, bool) is False:
        raise TypeError("include_gpu must be a bool")
    if gpu_timeout_seconds <= 0:
        raise ValueError("gpu_timeout_seconds must be > 0")
    cpu = _collect_cpu()
    ram = _collect_ram()
    if include_gpu:
        gpu, vram = _collect_nvidia(
            command_runner=command_runner or _default_command_runner,
            timeout_seconds=gpu_timeout_seconds,
        )
    else:
        reason = "GPU probe disabled by caller"
        gpu = _unavailable("devices", reason, source="disabled")
        vram = _unavailable("bytes", reason, source="disabled")

    metrics = (cpu, ram, gpu, vram)
    unavailable = tuple(
        name
        for name, metric in zip(("cpu", "ram", "gpu", "vram"), metrics, strict=True)
        if not metric.is_available
    )
    return HostResourceTelemetry(
        observed_at=(now or (lambda: datetime.now(UTC)))(),
        platform=platform.platform(),
        cpu=cpu,
        ram=ram,
        gpu=gpu,
        vram=vram,
        available=any(metric.is_available for metric in metrics),
        complete=all(metric.is_available for metric in metrics),
        unavailable=unavailable,
    )


__all__ = [
    "RESOURCE_TELEMETRY_SCHEMA_VERSION",
    "HostResourceTelemetry",
    "ResourceTelemetryMetric",
    "collect_host_resource_telemetry",
]
