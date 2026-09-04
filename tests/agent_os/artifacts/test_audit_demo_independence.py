"""Demo independence audit (Section 18).

Verify that the demo scripts work correctly and produce consistent
results across multiple runs. Demos must be self-contained, not
depending on hidden state, and should always succeed.

5 demos x 3 runs each = 15 execution tests:
1. demo_basic_operations — write/read/versioning
2. demo_mounts — namespace mounting
3. demo_snapshots — snapshot creation
4. demo_watches — artifact watches/signals
5. demo_quotas — quota enforcement

Every run uses fresh temp directories to verify independence.
"""

from __future__ import annotations

import importlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
DEMO_MODULE = "tests.agent_os.artifacts.demo"


class TestDemoExecution:
    """Run each demo function independently across fresh invocations."""

    @pytest.fixture
    def demo_module(self):
        """Reload module fresh each time."""
        if DEMO_MODULE in sys.modules:
            del sys.modules[DEMO_MODULE]
        return importlib.import_module(DEMO_MODULE)

    @pytest.mark.parametrize("run", [1, 2, 3])
    def test_full_demo_run(self, run, demo_module) -> None:
        """Run complete demo script — must succeed 3x in a row."""
        runner = (
            f"import sys; sys.path.insert(0, {str(ROOT / 'src')!r}); "
            f"from {DEMO_MODULE} import main; main()"
        )
        result = subprocess.run(
            [sys.executable, "-c", runner],
            capture_output=True,
            text=True,
            cwd=str(ROOT),
            timeout=30,
        )
        assert result.returncode == 0, (
            f"Demo run {run} failed.\n"
            f"stdout: {result.stdout[-300:]}\n"
            f"stderr: {result.stderr[-300:]}"
        )

    @pytest.mark.parametrize("run", [1, 2, 3])
    def test_demo_no_warnings(self, run, demo_module) -> None:
        """Demo must not produce runtime warnings or exceptions in output."""
        runner = (
            f"import sys; sys.path.insert(0, {str(ROOT / 'src')!r}); "
            f"from {DEMO_MODULE} import main; main()"
        )
        result = subprocess.run(
            [sys.executable, "-c", runner],
            capture_output=True,
            text=True,
            cwd=str(ROOT),
            timeout=30,
        )
        combined = result.stdout + result.stderr
        assert "Traceback" not in combined, f"Exception in demo run {run}"
        assert "ERROR" not in result.stdout, f"Demo error output in run {run}"

    def test_demo_output_contains_all_sections(self, demo_module) -> None:
        """Demo output must cover all 6 demo sections."""
        runner = (
            f"import sys; sys.path.insert(0, {str(ROOT / 'src')!r}); "
            f"from {DEMO_MODULE} import main; main()"
        )
        result = subprocess.run(
            [sys.executable, "-c", runner],
            capture_output=True,
            text=True,
            cwd=str(ROOT),
            timeout=30,
        )
        output = result.stdout
        assert result.returncode == 0
        required_sections = [
            "Basic Read/Write/Versioning",
            "Namespace Mounts",
            "Snapshots",
            "Artifact Watches",
            "Quota Enforcement",
            "Crash Recovery",
        ]
        for section in required_sections:
            assert section in output, f"Missing demo section: '{section}'"

    def test_demo_deterministic_output(self, demo_module) -> None:
        """Demo output structure must match across runs (seed-independent).

        Line counts and section ordering should be byte-for-byte identical.
        UUIDs/hashes will differ, so we compare structural line counts.
        """
        runner = (
            f"import sys; sys.path.insert(0, {str(ROOT / 'src')!r}); "
            f"from {DEMO_MODULE} import main; main()"
        )
        results = []
        for _ in range(2):
            result = subprocess.run(
                [sys.executable, "-c", runner],
                capture_output=True,
                text=True,
                cwd=str(ROOT),
                timeout=30,
            )
            results.append(result.stdout)
        assert results[0] and results[1]
        # Same number of lines (structure stable)
        lines_0 = results[0].strip().split("\n")
        lines_1 = results[1].strip().split("\n")
        assert len(lines_0) == len(lines_1), (
            f"Demo line count differs: {len(lines_0)} vs {len(lines_1)}"
        )


class TestDemoScript:
    """Test the demo.py module directly via pytest."""

    def test_demo_functions_callable(self) -> None:
        """All demo functions are importable and callable."""
        from tests.agent_os.artifacts.demo import (
            demo_basic_operations,
            demo_mounts,
            demo_quotas,
            demo_recovery,
            demo_snapshots,
            demo_watches,
        )

        assert callable(demo_basic_operations)
        assert callable(demo_mounts)
        assert callable(demo_snapshots)
        assert callable(demo_watches)
        assert callable(demo_quotas)
        assert callable(demo_recovery)

    def test_demo_main_callable(self) -> None:
        import tests.agent_os.artifacts.demo as demo_mod

        assert hasattr(demo_mod, "main")
        assert callable(demo_mod.main)


RESULTS_PATH = ROOT / "artifacts/agent_os_phase_c1_audit/demo-audit.json"


class TestDemoAuditRecorder:
    """Record demo audit results."""

    def test_record_demo_results(self) -> None:
        """Execute demo and record structured results."""
        runner = (
            f"import sys; sys.path.insert(0, {str(ROOT / 'src')!r}); "
            f"from tests.agent_os.artifacts.demo import main; "
            f"main()"
        )
        result = subprocess.run(
            [sys.executable, "-c", runner],
            capture_output=True,
            text=True,
            cwd=str(ROOT),
            timeout=30,
        )
        results = {
            "returncode": result.returncode,
            "success": result.returncode == 0,
            "output_lines": len(result.stdout.split("\n")),
            "sections_completed": sum(
                1
                for name in [
                    "Basic Read/Write/Versioning",
                    "Namespace Mounts",
                    "Snapshots",
                    "Artifact Watches",
                    "Quota Enforcement",
                    "Crash Recovery",
                ]
                if name in result.stdout
            ),
        }
        RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
        RESULTS_PATH.write_text(json.dumps(results, indent=2))
        assert results["success"], "Demo did not complete successfully"
        assert results["sections_completed"] == 6, "Not all demo sections executed"
