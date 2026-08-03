"""External progress grader (audit Milestone 1E).

The external grader evaluates task completion based on criteria the Runtime
cannot control or inspect — hidden tests, requirement-level checks, artifact
schemas, environment target state — rather than VPG-internal progress weights.

This ensures that:
- Runtime internal progress (verified node count, VPG progress_weight sum) ≠
  external benchmark progress.
- Adding meaningless verified nodes does not increase external score.
- Modifying VPG progress_weight does not change external score.
- Deleting a real requirement artifact decreases external score.
- The Runtime cannot read the external grader's hidden tests.

AUPBC curves should be based on this external score, not VPG internal progress.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, Field


class ExternalProgressScore(BaseModel):
    """Result of an external grading pass."""

    score: float  # [0.0, 1.0] — fraction of requirements met
    requirements_total: int
    requirements_passed: int
    hidden_tests_total: int = 0
    hidden_tests_passed: int = 0
    regression_tests_total: int = 0
    regression_tests_passed: int = 0
    details: list[dict[str, Any]] = Field(default_factory=list)


class ExternalProgressGrader(Protocol):
    """Independent external grader that scores workspace state.

    The Runtime must NEVER have access to the grader's hidden tests or
    grading rules. The grader only reads the workspace output.
    """

    async def score(
        self,
        workspace: Path,
        environment_state: dict[str, Any],
    ) -> ExternalProgressScore: ...


# ----------------------------------------------------------- concrete grader
class ArtifactRequirementGrader:
    """Concrete grader: checks that required artifacts exist and match
    expected content patterns.

    This is a simple, deterministic grader for the controlled benchmark.
    Real-world tasks would use hidden tests, regression suites, etc.
    """

    def __init__(
        self,
        requirements: list[dict[str, Any]],
        hidden_tests: list[dict[str, Any]] | None = None,
    ):
        """``requirements`` and ``hidden_tests`` are NOT passed to the Runtime.

        Each requirement:
            {
                "id": "req-1",
                "type": "file_exists" | "file_contains" | "command_exit_zero",
                "path": "relative/path.txt",
                "content": "expected substring",  # for file_contains
                "command": "...",  # for command_exit_zero
            }
        """
        self._requirements = requirements
        self._hidden_tests = hidden_tests or []

    async def score(
        self,
        workspace: Path,
        environment_state: dict[str, Any],
    ) -> ExternalProgressScore:
        import hashlib
        import subprocess

        passed = 0
        details: list[dict[str, Any]] = []

        for req in self._requirements:
            req_id = req.get("id", "unknown")
            req_type = req.get("type", "file_exists")
            ok = False
            detail: dict[str, Any] = {"id": req_id, "type": req_type}

            if req_type == "file_exists":
                path = workspace / req.get("path", "")
                ok = path.exists()
                detail["path"] = req.get("path", "")
            elif req_type == "file_contains":
                path = workspace / req.get("path", "")
                if path.exists():
                    content = path.read_text(encoding="utf-8", errors="replace")
                    expected = req.get("content", "")
                    ok = expected in content
                detail["path"] = req.get("path", "")
                detail["expected_content"] = req.get("content", "")
            elif req_type == "command_exit_zero":
                cmd = req.get("command", "")
                try:
                    result = subprocess.run(
                        cmd,
                        shell=True,
                        cwd=str(workspace),
                        capture_output=True,
                        timeout=30,
                    )
                    ok = result.returncode == 0
                except Exception:
                    ok = False
                detail["command"] = cmd
            elif req_type == "file_hash":
                path = workspace / req.get("path", "")
                if path.exists():
                    actual = hashlib.sha256(path.read_bytes()).hexdigest()
                    ok = actual == req.get("hash", "")
                detail["path"] = req.get("path", "")

            detail["passed"] = ok
            if ok:
                passed += 1
            details.append(detail)

        total = len(self._requirements)
        score = passed / total if total > 0 else 0.0

        return ExternalProgressScore(
            score=round(score, 6),
            requirements_total=total,
            requirements_passed=passed,
            details=details,
        )


# ----------------------------------------------------------- AUPBC from external
def external_progress_budget_curve(
    events: list[Any],
    external_scores: list[tuple[float, float]],  # (budget, external_score)
) -> list[dict[str, Any]]:
    """Build a progress-budget curve from external scores.

    Unlike the VPG-based curve (which uses verified_progress from node state
    changes), this curve uses external grader scores sampled at budget
    checkpoints. This ensures AUPBC is based on external progress, not
    internal VPG state.
    """
    curve: list[dict[str, Any]] = []
    for budget, ext_score in external_scores:
        curve.append(
            {
                "budget": round(budget, 6),
                "external_progress": round(ext_score, 6),
            }
        )
    return curve


def aupbc_external(
    external_scores: list[tuple[float, float]],  # (budget, external_score)
) -> float:
    """AUPBC computed from external scores (trapezoid rule).

    Normalized by (final_budget * final_score) so curves are comparable.
    """
    if not external_scores or len(external_scores) < 2:
        return 0.0

    pts = sorted(external_scores, key=lambda t: t[0])
    area = 0.0
    for i in range(len(pts) - 1):
        x0, y0 = pts[i]
        x1, y1 = pts[i + 1]
        area += (x1 - x0) * (y0 + y1) / 2.0

    max_x = pts[-1][0]
    max_y = pts[-1][1]
    denom = max_x * max_y
    if denom <= 0.0:
        return 0.0
    return round(area / denom, 6)
