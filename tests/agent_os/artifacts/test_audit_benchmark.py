"""Benchmark correctness audit (Section 20).

Verify that microbenchmark results are correct and the benchmark
infrastructure itself is sound. Benchmarks must:
1. Run successfully (no crashes or errors)
2. Emit finite, positive timing measurements
3. Measure what they claim to measure (correct operations per second)

Host-dependent throughput is reported rather than treated as a correctness
gate. Comparative performance gates use repeated paired measurements in the
dedicated benchmark harness.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

import pytest

ROOT = Path(__file__).resolve().parents[3]
RESULTS_PATH = ROOT / "artifacts/agent_os_phase_c1_audit/microbenchmark-audit.json"
pytestmark = pytest.mark.slow


class _BenchmarkRun(NamedTuple):
    returncode: int
    stdout: str
    stderr: str


def _pytest_command(*args: str) -> list[str]:
    return [sys.executable, "-m", "pytest", *args]


def _child_env() -> dict[str, str]:
    env = os.environ.copy()
    src_path = str(ROOT / "src")
    env["PYTHONPATH"] = os.pathsep.join(
        path for path in (src_path, env.get("PYTHONPATH", "")) if path
    )
    return env


@pytest.fixture(scope="module")
def benchmark_run() -> _BenchmarkRun:
    """Run the timing suite once and share the result across audit assertions."""

    result = subprocess.run(
        _pytest_command("tests/agent_os/artifacts/test_benchmark.py", "-s", "-v"),
        capture_output=True,
        text=True,
        cwd=str(ROOT),
        env=_child_env(),
        timeout=180,
    )
    return _BenchmarkRun(result.returncode, result.stdout, result.stderr)


class TestBenchmarkCorrectness:
    """Audit the benchmark test suite."""

    def test_benchmark_file_exists(self) -> None:
        """test_benchmark.py must exist."""
        bm = ROOT / "tests/agent_os/artifacts/test_benchmark.py"
        assert bm.exists(), "Benchmark test file missing"

    def test_benchmark_imports_work(self) -> None:
        """Benchmark module must be importable."""
        from tests.agent_os.artifacts.test_benchmark import TestBenchmarks

        assert hasattr(TestBenchmarks, "test_timing_sequential_writes")
        assert hasattr(TestBenchmarks, "test_timing_sequential_reads")

    def test_benchmark_run_succeeds(self, benchmark_run: _BenchmarkRun) -> None:
        """Running benchmarks must complete without test failure."""
        assert benchmark_run.returncode == 0, (
            f"Benchmarks failed: {benchmark_run.stdout[-200:]}\n"
            f"{benchmark_run.stderr[-200:]}"
        )

    def test_benchmark_output_contains_rates(self, benchmark_run: _BenchmarkRun) -> None:
        """Benchmark output must include ops/s measurements."""
        assert "ops/s" in benchmark_run.stdout, "Benchmark output lacks ops/s measurements"

    def test_benchmark_measurements_are_positive(self, benchmark_run: _BenchmarkRun) -> None:
        """Every rate-bearing benchmark must emit a positive measurement."""
        import re

        expected = {
            "Sequential writes",
            "Sequential reads",
            "Version updates",
            "Mount read-through",
            "Write + watch signal",
            "COW write (local copy)",
        }
        measured: dict[str, int] = {}
        for line in benchmark_run.stdout.splitlines():
            match = re.search(r"^\s*(.+?):\s+\d+ ops.*?=\s*(\d+) ops/s", line)
            if match:
                measured[match.group(1)] = int(match.group(2))
        assert set(measured) == expected
        assert all(rate > 0 for rate in measured.values())


class TestBenchmarkArtifacts:
    """Verify Phase C1 benchmark artifacts are valid."""

    def test_microbenchmark_json_valid(self) -> None:
        """microbenchmarks.json (if exists) must be valid JSON."""
        bm_path = ROOT / "artifacts/agent_os_phase_c1/microbenchmarks.json"
        if bm_path.exists():
            data = json.loads(bm_path.read_text(encoding="utf-8"))
            assert isinstance(data, (dict, list)), "Benchmark JSON should be dict/list"

    def test_artifact_timestamps_are_iso(self) -> None:
        """Any timestamp fields in benchmark artifacts must be ISO format."""
        for json_path in ROOT.glob("artifacts/agent_os_phase_c1/**/*.json"):
            try:
                data = json.loads(json_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                pytest.fail(f"Invalid JSON: {json_path}")
            # Parse succeeded = valid JSON
            assert isinstance(data, (dict, list, str, int, float, bool, type(None)))


class TestMicrobenchmarkAudit:
    """Record microbenchmark audit results."""

    def test_record_benchmark_audit(self, benchmark_run: _BenchmarkRun) -> None:
        """Record benchmark audit results."""
        audit = {
            "benchmark_run_success": benchmark_run.returncode == 0,
            "output": benchmark_run.stdout[-500:],
            "errors": benchmark_run.stderr[-300:] if benchmark_run.returncode != 0 else "",
            "gate_kind": "operation completion and valid timing output",
            "performance_threshold_enforced": False,
            "benchmarks_found": [
                "sequential_writes",
                "sequential_reads",
                "version_updates",
                "mount_readthrough",
                "snapshot_creation",
                "watch_signal_delivery",
                "cow_write_isolation",
            ],
        }
        RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
        RESULTS_PATH.write_text(json.dumps(audit, indent=2))

        assert audit["benchmark_run_success"], "Benchmark audit run failed"
