"""Physical resource telemetry for subprocess work: child CPU time and peak RSS.

The project reports physical resource management as entirely absent.  For work
run in a child process the SDK controls that is partly wrong: the child's CPU
time is observable via ``resource.getrusage(RUSAGE_CHILDREN)`` on POSIX.  Where
that is unavailable (e.g. Windows without ``psutil``, which is not a declared
dependency) the field is reported *unavailable* rather than fabricated.
"""

from __future__ import annotations

import sys
import time

import pytest

from lhos.sdk.subprocess_harness import _ChildProcess

try:  # POSIX only; absent on Windows.
    import resource as _resource  # noqa: F401

    _HAS_RESOURCE = True
except ImportError:
    _HAS_RESOURCE = False

_TIMEOUT = 30.0


def _run(command: list[str]) -> dict:
    child = _ChildProcess(
        command,
        cwd=None,
        env=None,
        grace_period=1.0,
        hard_kill_timeout=1.0,
        wall_clock_timeout=_TIMEOUT,
        max_capture_bytes=64_000,
    )
    child.start()
    deadline = time.monotonic() + _TIMEOUT
    while child.poll() is None and time.monotonic() < deadline:
        time.sleep(0.02)
    usage = child.usage()
    child.close()
    assert child.poll() is not None  # reaped: no orphan left behind
    return usage


def _busy_child() -> list[str]:
    # Burn measurable CPU so a nonzero child CPU time is observable on POSIX.
    return [sys.executable, "-c", "s=0\nfor i in range(10_000_000):\n    s+=i\n"]


def _quick_child() -> list[str]:
    return [sys.executable, "-c", "pass"]


@pytest.mark.skipif(not _HAS_RESOURCE, reason="resource.getrusage is POSIX-only")
def test_cpu_time_is_measured_where_supported() -> None:
    usage = _run(_busy_child())

    assert usage["resource_metrics_available"] is True
    assert usage["resource_metrics_unavailable_reason"] is None
    assert isinstance(usage["cpu_time_ms"], int)
    assert usage["cpu_time_ms"] > 0
    # Peak RSS is reported (bytes) where getrusage supplies ru_maxrss.
    assert isinstance(usage["peak_rss_bytes"], int)
    assert usage["peak_rss_bytes"] >= 0


@pytest.mark.skipif(_HAS_RESOURCE, reason="only meaningful where resource is unavailable")
def test_cpu_time_is_unavailable_where_unsupported() -> None:
    usage = _run(_quick_child())

    assert usage["resource_metrics_available"] is False
    assert usage["cpu_time_ms"] is None
    assert usage["peak_rss_bytes"] is None
    assert usage["resource_metrics_unavailable_reason"]


def test_usage_dict_is_additive_and_preserves_the_existing_contract() -> None:
    """New telemetry keys are added; none of the pre-existing keys are removed."""

    usage = _run(_quick_child())

    for legacy_key in (
        "wall_time_ms",
        "tokens_in",
        "tokens_out",
        "cost_microusd",
        "measured_usage",
        "exit_code",
        "pid",
        "timed_out",
        "hard_killed",
        "terminated_by",
    ):
        assert legacy_key in usage, legacy_key

    for new_key in (
        "observed_reads",
        "reads_observed",
        "cpu_time_ms",
        "peak_rss_bytes",
        "resource_metrics_available",
        "resource_metrics_unavailable_reason",
    ):
        assert new_key in usage, new_key

    # The resource-availability flag is consistent with the platform.
    assert usage["resource_metrics_available"] is _HAS_RESOURCE
