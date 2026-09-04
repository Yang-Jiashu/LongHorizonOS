"""External grader for the config_loader pilot task.

This grader runs INDEPENDENTLY of the LongHorizonOS runtime. It:
1. Runs the public tests.
2. Runs the hidden tests.
3. Checks for regressions in the initial repo.
4. Scores each requirement.

The runtime can only see the final score, never the hidden test content.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from pydantic import BaseModel


class RequirementResult(BaseModel):
    requirement_id: str
    passed: bool
    score: float
    evidence: str


class ExternalProgressScore(BaseModel):
    total_score: float
    max_score: float
    progress_ratio: float
    requirements: list[RequirementResult]


REQUIREMENTS = [
    "req_1_json_loading",
    "req_2_missing_file_error",
    "req_3_invalid_json_error",
    "req_4_migrate_caller",
    "req_5_public_tests",
    "req_6_readme_updated",
    "req_7_no_regression",
    "req_8_config_loader_class",
    "req_9_get_method",
    "req_10_nested_config",
]


def _run_tests(workspace: str, test_dir: str) -> tuple[bool, str]:
    """Run pytest in the workspace. Returns (success, output)."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", test_dir, "-v", "--tb=short"],
            cwd=workspace,
            capture_output=True,
            text=True,
            timeout=120,
        )
        return result.returncode == 0, result.stdout + result.stderr
    except Exception as exc:
        return False, str(exc)


def grade(workspace: str) -> ExternalProgressScore:
    """Grade the workspace. The workspace must be a Python project with the
    config_loader module implemented.

    Parameters
    ----------
    workspace : str
        Path to the workspace directory (the modified initial_repo).
    """
    results: list[RequirementResult] = []
    base_dir = Path(__file__).parent.parent
    public_tests = str(base_dir / "tests_public")
    hidden_tests = str(base_dir / "tests_hidden")

    # Install the workspace package.
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-e", workspace, "-q"],
        capture_output=True,
        timeout=60,
    )

    # Run public tests.
    pub_ok, pub_output = _run_tests(workspace, public_tests)

    # Run hidden tests.
    hid_ok, hid_output = _run_tests(workspace, hidden_tests)

    # Run initial repo tests (regression check).
    reg_ok, reg_output = _run_tests(workspace, ".")

    all_output = pub_output + "\n" + hid_output + "\n" + reg_output

    # Score each requirement.
    req_map = {
        "req_1_json_loading": pub_ok
        and "test_load_valid_json_config" in all_output
        and "PASSED" in all_output,
        "req_2_missing_file_error": pub_ok
        and "test_missing_file_raises_clear_error" in all_output
        and "PASSED" in all_output,
        "req_3_invalid_json_error": pub_ok
        and "test_invalid_json_raises_clear_error" in all_output
        and "PASSED" in all_output,
        "req_4_migrate_caller": pub_ok
        and "test_app_uses_config_loader" in all_output
        and "PASSED" in all_output,
        "req_5_public_tests": pub_ok,
        "req_6_readme_updated": hid_ok
        or "config" in Path(workspace).joinpath("README.md").read_text(encoding="utf-8").lower()
        if Path(workspace).joinpath("README.md").exists()
        else False,
        "req_7_no_regression": reg_ok,
        "req_8_config_loader_class": "ConfigLoader" in all_output,
        "req_9_get_method": hid_ok,
        "req_10_nested_config": hid_ok,
    }

    for req_id in REQUIREMENTS:
        passed = req_map.get(req_id, False)
        results.append(
            RequirementResult(
                requirement_id=req_id,
                passed=passed,
                score=1.0 if passed else 0.0,
                evidence=f"{'PASS' if passed else 'FAIL'}: {req_id}",
            )
        )

    total = sum(r.score for r in results)
    max_score = float(len(results))

    return ExternalProgressScore(
        total_score=total,
        max_score=max_score,
        progress_ratio=total / max_score if max_score > 0 else 0.0,
        requirements=results,
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python grader.py <workspace_dir>")
        sys.exit(1)
    score = grade(sys.argv[1])
    print(json.dumps(score.model_dump(), indent=2))
