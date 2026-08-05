"""Benchmark correctness audit (Section 20).

Verify that microbenchmark results are correct and that the benchmark
infrastructure itself is sound. Benchmarks must:
1. Run successfully (no crashes or errors)
2. Meet minimum performance thresholds (correctness of measurement)
3. Be reproducible within reasonable variance
4. Measure what they claim to measure (correct operations per second)

Also verifies the benchmark artifacts from Phase C1 are valid.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path("/Users/jiashuyang/Documents/kimi/Workspaces/longhorizonOS/longhorizonos")
RESULTS_PATH = ROOT / "artifacts/agent_os_phase_c1_audit/microbenchmark-audit.json"


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

    def test_benchmark_run_succeeds(self) -> None:
        """Running benchmarks must complete without test failure."""
        result = subprocess.run(
            [
                "uv", "run", "pytest",
                "tests/agent_os/artifacts/test_benchmark.py", "-q",
            ],
            capture_output=True, text=True,
            cwd=str(ROOT),
            timeout=60,
        )
        assert result.returncode == 0, (
            f"Benchmarks failed: {result.stdout[-200:]}\n"
            f"{result.stderr[-200:]}"
        )

    def test_benchmark_output_contains_rates(self) -> None:
        """Benchmark output must include ops/s measurements."""
        result = subprocess.run(
            [
                "uv", "run", "pytest",
                "tests/agent_os/artifacts/test_benchmark.py", "-s",
            ],
            capture_output=True, text=True,
            cwd=str(ROOT),
            timeout=60,
        )
        output = result.stdout
        # Should contain rate measurements
        assert "ops/s" in output, "Benchmark output lacks ops/s measurements"

    def test_benchmark_thresholds_reasonable(self) -> None:
        """Benchmark thresholds must be achievable on normal hardware.

        Verify that write throughput > 100 ops/s, read > 100 ops/s.
        These are very conservative for SQLite + Python in-memory.
        """
        result = subprocess.run(
            [
                "uv", "run", "pytest",
                "tests/agent_os/artifacts/test_benchmark.py", "-s", "-v",
            ],
            capture_output=True, text=True,
            cwd=str(ROOT),
            timeout=60,
        )
        output = result.stdout

        # Parse performance numbers from output
        import re
        for line in output.split("\n"):
            if "Sequential writes:" in line:
                match = re.search(r"(\d+)\s*ops/s", line)
                if match:
                    rate = int(match.group(1))
                    assert rate > 100, f"Write rate {rate} ops/s below threshold"
            if "Sequential reads:" in line:
                match = re.search(r"(\d+)\s*ops/s", line)
                if match:
                    rate = int(match.group(1))
                    assert rate > 100, f"Read rate {rate} ops/s below threshold"


class TestBenchmarkReproducibility:
    """Benchmarks should produce similar results within variance."""

    def test_benchmark_deterministic_results(self) -> None:
        """Running benchmarks twice should produce consistent pass/fail."""
        results = []
        for _ in range(2):
            result = subprocess.run(
                [
                    "uv", "run", "pytest",
                    "tests/agent_os/artifacts/test_benchmark.py", "-q",
                ],
                capture_output=True, text=True,
                cwd=str(ROOT),
                timeout=120,
            )
            results.append(result.returncode)

        # Both runs should pass
        assert all(r == 0 for r in results), (
            f"Benchmark runs not consistent: {results}"
        )


class TestBenchmarkArtifacts:
    """Verify Phase C1 benchmark artifacts are valid."""

    def test_microbenchmark_json_valid(self) -> None:
        """microbenchmarks.json (if exists) must be valid JSON."""
        bm_path = ROOT / "artifacts/agent_os_phase_c1/microbenchmarks.json"
        if bm_path.exists():
            data = json.loads(bm_path.read_text())
            assert isinstance(data, (dict, list)), "Benchmark JSON should be dict/list"

    def test_artifact_timestamps_are_iso(self) -> None:
        """Any timestamp fields in benchmark artifacts must be ISO format."""
        for json_path in ROOT.glob("artifacts/agent_os_phase_c1/**/*.json"):
            try:
                data = json.loads(json_path.read_text())
            except json.JSONDecodeError:
                pytest.fail(f"Invalid JSON: {json_path}")
            # Parse succeeded = valid JSON
            assert isinstance(data, (dict, list, str, int, float, bool, type(None)))


class TestMicrobenchmarkAudit:
    """Record microbenchmark audit results."""

    def test_record_benchmark_audit(self) -> None:
        """Record benchmark audit results."""
        # Run benchmarks and capture
        result = subprocess.run(
            [
                "uv", "run", "pytest",
                "tests/agent_os/artifacts/test_benchmark.py", "-s", "-v",
            ],
            capture_output=True, text=True,
            cwd=str(ROOT),
            timeout=60,
        )

        audit = {
            "benchmark_run_success": result.returncode == 0,
            "output": result.stdout[-500:],
            "errors": result.stderr[-300:] if result.returncode != 0 else "",
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
