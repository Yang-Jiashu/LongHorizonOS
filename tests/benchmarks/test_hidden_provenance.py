"""Safety gate for the hidden-provenance benchmark."""

from __future__ import annotations

from lhos.benchmarks.hidden_provenance import run_hidden_provenance_benchmark


def test_hidden_provenance_benchmark_fails_closed() -> None:
    report = run_hidden_provenance_benchmark()

    assert report["valid"] is True
    assert report["missing_read_case"]["strict_denied"] is True
    assert report["unknown_read_case"]["strict_denied"] is True
    assert report["unknown_read_case"]["audit_allowed"] is True
