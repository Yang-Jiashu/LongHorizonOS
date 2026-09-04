"""Undeclared-read discovery: a child's real reads checked against declarations.

These spawn real child processes (``python -m lhos.sdk.harness_child``) so the
recorder, its stdout protocol, and the parent-side collection are exercised end
to end rather than mocked.  The recorder monkeypatches ``open`` inside the child
only; the pure comparison never touches the process's own ``open``.
"""

from __future__ import annotations

import os
import sys
import time

import pytest

from lhos.sdk.read_recorder import READS_SENTINEL, format_reads_line, parse_reads_line
from lhos.sdk.subprocess_harness import _ChildProcess
from lhos.sdk.undeclared_reads import (
    UndeclaredReadReport,
    compare_reads,
    normalize_declared_input,
    report_from_usage,
)

_TIMEOUT = 30.0


def _child_cmd(*args: str) -> list[str]:
    return [sys.executable, "-m", "lhos.sdk.harness_child", *args]


def _run_child(*args: str, env: dict[str, str] | None = None) -> tuple[_ChildProcess, dict]:
    child = _ChildProcess(
        _child_cmd(*args),
        cwd=None,
        env=env,
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
    return child, usage


def _normcase(path: str) -> str:
    return os.path.normcase(os.path.abspath(path))


# ── discovery through a real child ──────────────────────────────────────────
def test_undeclared_read_is_detected(tmp_path) -> None:
    secret = tmp_path / "secret.txt"
    secret.write_text("x", encoding="utf-8")

    _child, usage = _run_child("--record-reads", "--read-file", str(secret))

    assert usage["reads_observed"] is True
    assert usage["observed_reads"] is not None
    report = compare_reads(usage["observed_reads"], [], workspace_root=str(tmp_path))
    assert report.observed is True
    assert any(_normcase(p) == _normcase(str(secret)) for p in report.undeclared_reads or ())
    assert report.declared_but_unread == ()


def test_child_reading_only_declared_files_reports_none(tmp_path) -> None:
    declared_file = tmp_path / "input.txt"
    declared_file.write_text("d", encoding="utf-8")

    _child, usage = _run_child("--record-reads", "--read-file", str(declared_file))

    report = compare_reads(
        usage["observed_reads"], ["workspace://input.txt"], workspace_root=str(tmp_path)
    )
    assert report.observed is True
    assert report.undeclared_reads == ()
    assert report.declared_but_unread == ()


def test_env_injection_activates_the_recorder(tmp_path) -> None:
    """The parent can turn recording on via child env, without a CLI flag."""

    target = tmp_path / "via-env.txt"
    target.write_text("e", encoding="utf-8")
    env = {**os.environ, "LHOS_HARNESS_RECORD_READS": "1"}

    _child, usage = _run_child("--read-file", str(target), env=env)

    assert usage["reads_observed"] is True
    report = compare_reads(usage["observed_reads"], [], workspace_root=str(tmp_path))
    assert any(_normcase(p) == _normcase(str(target)) for p in report.undeclared_reads or ())


def test_no_observation_reports_unavailable_not_zero(tmp_path) -> None:
    """Fail-closed: an unobserved attempt is never reported as 'no undeclared reads'."""

    # The child does not record: no reads line is emitted at all.
    _child, usage = _run_child("--sleep", "0")

    assert usage["reads_observed"] is False
    assert usage["observed_reads"] is None

    report = report_from_usage(usage, ["workspace://x"], workspace_root=str(tmp_path))
    assert report.observed is False
    assert report.undeclared_reads is None  # not ()
    assert report.declared_but_unread is None
    assert report.unavailable is not None
    assert report.unavailable.name == "undeclared_reads"


def test_corrupt_reads_line_does_not_crash_parent_and_is_unobserved(tmp_path) -> None:
    _child, usage = _run_child("--corrupt-reads", "--sleep", "0")

    # A malformed sentinel line parses to None: unobserved, not a false zero.
    assert usage["reads_observed"] is False
    assert usage["observed_reads"] is None
    report = report_from_usage(usage, [], workspace_root=str(tmp_path))
    assert report.observed is False


def test_recorder_child_leaves_no_orphan(tmp_path) -> None:
    target = tmp_path / "f.txt"
    target.write_text("x", encoding="utf-8")
    child, _usage = _run_child("--record-reads", "--read-file", str(target))
    assert child.poll() is not None  # reaped, no orphan


# ── pure normalization / comparison ─────────────────────────────────────────
def test_normalization_matches_workspace_bareid_and_abspath_forms(tmp_path) -> None:
    root = str(tmp_path)
    target = os.path.abspath(os.path.join(root, "sub", "a.txt"))

    for declared in ("workspace://sub/a.txt", "sub/a.txt", target):
        report = compare_reads([target], [declared], workspace_root=root)
        assert report.observed is True
        assert report.undeclared_reads == (), declared
        assert report.declared_but_unread == (), declared
        assert report.observed_reads == (target,)
        assert normalize_declared_input(declared, root) == target


def test_declared_but_unread_and_undeclared_are_sorted(tmp_path) -> None:
    root = str(tmp_path)
    declared_path = os.path.abspath(os.path.join(root, "declared.txt"))
    read_a = os.path.abspath(os.path.join(root, "a_undeclared.txt"))
    read_b = os.path.abspath(os.path.join(root, "b_undeclared.txt"))

    report = compare_reads([read_b, read_a], ["workspace://declared.txt"], workspace_root=root)
    assert report.undeclared_reads == (read_a, read_b)
    assert report.declared_but_unread == (declared_path,)


def test_empty_observation_is_distinct_from_unobserved(tmp_path) -> None:
    root = str(tmp_path)
    # Observed nothing: a real (empty) observation, not "unavailable".
    observed = compare_reads([], ["workspace://x"], workspace_root=root)
    assert observed.observed is True
    assert observed.undeclared_reads == ()
    assert observed.declared_but_unread == (normalize_declared_input("workspace://x", root),)
    assert observed.unavailable is None

    # No observation: fail-closed.
    missing = compare_reads(None, ["workspace://x"], workspace_root=root)
    assert missing.observed is False
    assert missing.undeclared_reads is None


def test_reads_line_protocol_roundtrip_and_defensive_parse() -> None:
    line = format_reads_line(["/b", "/a", "/a"])
    assert line.startswith(READS_SENTINEL)
    assert parse_reads_line(line) == ["/a", "/b"]

    # Empty list is a real observation of "read nothing".
    assert parse_reads_line(f"{READS_SENTINEL} []") == []

    # Defensive: none of these may raise; all resolve to None.
    assert parse_reads_line("just some log output") is None
    assert parse_reads_line(f"{READS_SENTINEL} {{not json") is None
    assert parse_reads_line(f'{READS_SENTINEL} {{"a": 1}}') is None  # object, not list
    assert parse_reads_line(f"{READS_SENTINEL} [1, 2, 3]") is None  # non-string entries
    assert parse_reads_line(READS_SENTINEL) is None


def test_report_model_is_frozen() -> None:
    report = compare_reads([], [], workspace_root=os.getcwd())
    assert isinstance(report, UndeclaredReadReport)
    with pytest.raises(Exception):
        report.observed = False  # type: ignore[misc]
