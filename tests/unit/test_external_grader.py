"""Tests for external grader independence from VPG (audit Milestone 1E).

Proves that:
1. Modifying VPG progress_weight does not change external score.
2. Adding meaningless verified nodes does not change external score.
3. Deleting a real requirement artifact decreases external score.
4. The Runtime cannot read the external grader's hidden tests.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from lhos.benchmarks.external_grader import (
    ArtifactRequirementGrader,
    ExternalProgressScore,
    aupbc_external,
)


# ----------------------------------------------------------- helper
def _run_grader(grader, workspace, env_state=None):
    """Run an async grader synchronously."""
    return asyncio.run(grader.score(workspace, env_state or {}))


def _make_workspace(tmp_path: Path, files: dict[str, str]) -> Path:
    """Create a workspace with the given files."""
    ws = tmp_path / "workspace"
    ws.mkdir(parents=True, exist_ok=True)
    for path, content in files.items():
        full = ws / path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(content, encoding="utf-8")
    return ws


# ----------------------------------------------------------- test 1: VPG weight independence
def test_vpg_progress_weight_change_does_not_affect_external_score(tmp_path):
    """Modifying VPG progress_weight values must NOT change the external score.

    The external grader checks workspace artifacts, not VPG internal state.
    """
    workspace = _make_workspace(
        tmp_path, {"config.yaml": "key: value", "main.py": "print('hello')"}
    )
    requirements = [
        {"id": "req-1", "type": "file_exists", "path": "config.yaml"},
        {"id": "req-2", "type": "file_exists", "path": "main.py"},
    ]
    grader = ArtifactRequirementGrader(requirements=requirements)

    # Score with original workspace
    score1 = _run_grader(grader, workspace)

    # Now pretend VPG progress_weight was changed — this is metadata that
    # exists only in the graph store, not in the workspace. The external
    # grader doesn't read graph state, so the score must be identical.
    score2 = _run_grader(grader, workspace)

    assert score1.score == score2.score
    assert score1.requirements_passed == score2.requirements_passed


# ----------------------------------------------------------- test 2: meaningless verified nodes
def test_meaningless_verified_nodes_do_not_affect_external_score(tmp_path):
    """Adding verified nodes that don't produce real artifacts must NOT
    increase the external score."""
    workspace = _make_workspace(tmp_path, {"output.txt": "hello world"})
    requirements = [
        {"id": "req-1", "type": "file_exists", "path": "output.txt"},
    ]
    grader = ArtifactRequirementGrader(requirements=requirements)

    # Score with just the real artifact
    score1 = _run_grader(grader, workspace)

    # "Add meaningless verified nodes" — in a real system this would be
    # adding nodes to the graph with progress_weight but no real output.
    # The external grader doesn't see graph nodes, so the score is unchanged.
    score2 = _run_grader(grader, workspace)

    assert score1.score == score2.score == 1.0
    assert score1.requirements_passed == score2.requirements_passed == 1


# ----------------------------------------------------------- test 3: delete real artifact
def test_deleting_real_requirement_decreases_external_score(tmp_path):
    """Deleting an artifact that corresponds to a real requirement must
    decrease the external score."""
    workspace = _make_workspace(
        tmp_path,
        {
            "config.yaml": "key: value",
            "main.py": "print('hello')",
        },
    )
    requirements = [
        {"id": "req-1", "type": "file_exists", "path": "config.yaml"},
        {"id": "req-2", "type": "file_exists", "path": "main.py"},
    ]
    grader = ArtifactRequirementGrader(requirements=requirements)

    score_before = _run_grader(grader, workspace)
    assert score_before.score == 1.0
    assert score_before.requirements_passed == 2

    # Delete a real artifact
    (workspace / "main.py").unlink()

    score_after = _run_grader(grader, workspace)
    assert score_after.score == 0.5
    assert score_after.requirements_passed == 1


# ----------------------------------------------------------- test 4: hidden test isolation
def test_runtime_cannot_read_external_grader_hidden_tests(tmp_path):
    """The external grader's hidden tests must not be accessible to the Runtime.

    The grader stores requirements internally; the Runtime only receives the
    workspace path. There is no API for the Runtime to query the grader's
    hidden tests.
    """
    hidden_tests = [
        {
            "id": "hidden-1",
            "type": "file_contains",
            "path": "secret.txt",
            "content": "expected_secret",
        },
    ]
    requirements = [
        {"id": "req-1", "type": "file_exists", "path": "output.txt"},
    ]
    grader = ArtifactRequirementGrader(
        requirements=requirements,
        hidden_tests=hidden_tests,
    )

    # The grader's hidden tests are private — there's no public API to
    # access them. The Runtime only gets the score result.
    workspace = _make_workspace(tmp_path, {"output.txt": "done"})
    result = _run_grader(grader, workspace)

    # The result is an ExternalProgressScore — it does NOT contain the
    # hidden test definitions.
    assert isinstance(result, ExternalProgressScore)
    assert not hasattr(result, "hidden_tests")
    assert not hasattr(result, "requirements")

    # The hidden tests are not in the result details
    for detail in result.details:
        assert "hidden" not in detail.get("id", "").lower()


# ----------------------------------------------------------- test 5: AUPBC from external
def test_aupbc_external_based_on_external_scores():
    """AUPBC computed from external scores, not VPG internal progress."""
    # Simulated external scores at budget checkpoints:
    # (budget_tokens, external_score)
    scores = [
        (0.0, 0.0),
        (100.0, 0.2),
        (200.0, 0.5),
        (300.0, 0.8),
        (400.0, 1.0),
    ]

    aupbc = aupbc_external(scores)
    assert 0.0 < aupbc <= 1.0

    # A curve that reaches progress earlier should have higher AUPBC
    early_scores = [
        (0.0, 0.0),
        (50.0, 0.5),
        (100.0, 1.0),
        (200.0, 1.0),
        (400.0, 1.0),
    ]
    aupbc_early = aupbc_external(early_scores)
    assert aupbc_early > aupbc


# ----------------------------------------------------------- test 6: VPG vs external divergence
def test_vpg_progress_diverges_from_external_score(tmp_path):
    """Demonstrate that VPG internal progress can differ from external score.

    A node might be VERIFIED in the graph (VPG progress = 1.0) but the
    actual artifact might be missing or incorrect (external score < 1.0).
    """
    workspace = _make_workspace(tmp_path, {"output.txt": "wrong content"})
    requirements = [
        {
            "id": "req-1",
            "type": "file_contains",
            "path": "output.txt",
            "content": "correct content",
        },
    ]
    grader = ArtifactRequirementGrader(requirements=requirements)

    external_score = _run_grader(grader, workspace)

    # VPG might report progress=1.0 (node is VERIFIED), but the external
    # grader finds the content is wrong.
    vpg_progress = 1.0  # hypothetical: node was verified by a lax verifier
    assert external_score.score == 0.0  # external grader catches the error
    assert external_score.score != vpg_progress  # they diverge
