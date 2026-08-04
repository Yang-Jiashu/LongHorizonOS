"""n3 root cause regression tests (Milestone 2.2 Part 2).

Verifies that FileExistsVerifier correctly handles both canonical ``path``
and legacy ``artifact_name`` parameters, rejects conflicting specs, and
produces structured failure codes instead of vague "no path" messages.

Specification:
    - canonical parameter: ``path``
    - ``artifact_name``: backward-compatible alias only
    - both present with different values: ``verification_spec_invalid``
    - both missing: ``verification_spec_invalid``
"""

from __future__ import annotations

from pathlib import Path

from lhos.domain.models import GraphNode
from lhos.domain.verification import VerificationSpec
from lhos.ports.verifier import VerificationContext
from lhos.verification.file_verifier import FileExistsVerifier


def _make_node() -> GraphNode:
    return GraphNode(
        id="n3",
        run_id="test-run",
        kind="subtask",
        title="Design config loader module",
        specification="Design a configuration loading module",
    )


def _make_context(tmp_path: Path) -> VerificationContext:
    return VerificationContext(
        run_id="test-run",
        workspace_dir=str(tmp_path),
        worker_result={},
        baseline_hashes={},
    )


class TestFileExistsAcceptsCanonicalPath:
    """file_exists with canonical 'path' parameter should work."""

    def test_file_exists_accepts_canonical_path(self, tmp_path: Path) -> None:
        # Create the target file.
        (tmp_path / "config_loader_design.md").write_text("# Config Loader Design")

        verifier = FileExistsVerifier()
        spec = VerificationSpec(
            verifier_type="file_exists",
            parameters={"path": "config_loader_design.md"},
        )
        result = verifier.verify(_make_node(), spec, _make_context(tmp_path))

        assert result.passed is True
        assert "config_loader_design.md" in result.summary
        assert len(result.evidence) == 1
        assert result.evidence[0]["metadata"]["path"] == "config_loader_design.md"


class TestFileExistsAcceptsLegacyArtifactName:
    """file_exists with legacy 'artifact_name' parameter should also work."""

    def test_file_exists_accepts_legacy_artifact_name(self, tmp_path: Path) -> None:
        # Create the target file.
        (tmp_path / "config_loader_design.md").write_text("# Config Loader Design")

        verifier = FileExistsVerifier()
        spec = VerificationSpec(
            verifier_type="file_exists",
            parameters={"artifact_name": "config_loader_design.md"},
        )
        result = verifier.verify(_make_node(), spec, _make_context(tmp_path))

        assert result.passed is True
        assert "config_loader_design.md" in result.summary

    def test_artifact_name_same_value_as_path(self, tmp_path: Path) -> None:
        """Both present with same value should work (not a conflict)."""
        (tmp_path / "output.txt").write_text("hello")

        verifier = FileExistsVerifier()
        spec = VerificationSpec(
            verifier_type="file_exists",
            parameters={"path": "output.txt", "artifact_name": "output.txt"},
        )
        result = verifier.verify(_make_node(), spec, _make_context(tmp_path))

        assert result.passed is True


class TestFileExistsRejectsConflictingPathAlias:
    """file_exists should reject specs with conflicting path and artifact_name."""

    def test_file_exists_rejects_conflicting_path_alias(self, tmp_path: Path) -> None:
        verifier = FileExistsVerifier()
        spec = VerificationSpec(
            verifier_type="file_exists",
            parameters={"path": "file_a.txt", "artifact_name": "file_b.txt"},
        )
        result = verifier.verify(_make_node(), spec, _make_context(tmp_path))

        assert result.passed is False
        assert "verification_spec_invalid" in result.summary
        assert "conflicting" in result.summary
        assert "file_a.txt" in result.summary
        assert "file_b.txt" in result.summary


class TestFileExistsMissingPathHasStructuredFailure:
    """file_exists with no path or artifact_name should return structured failure."""

    def test_file_exists_missing_path_has_structured_failure(self, tmp_path: Path) -> None:
        verifier = FileExistsVerifier()
        spec = VerificationSpec(
            verifier_type="file_exists",
            parameters={},
        )
        result = verifier.verify(_make_node(), spec, _make_context(tmp_path))

        assert result.passed is False
        assert "verification_spec_invalid" in result.summary
        # Must NOT return vague "no path" — must be structured.
        assert result.summary != "file_exists: no path"
        assert "missing" in result.summary.lower()
        assert "path" in result.summary.lower()

    def test_missing_path_feedback_classified_correctly(self) -> None:
        """The structured failure should be classified as verification_spec_invalid."""
        from lhos.runtime.verification_feedback import build_feedback_from_verification

        feedback = build_feedback_from_verification(
            verifier_type="file_exists",
            summary=(
                "verification_spec_invalid: file_exists spec is missing "
                "both 'path' (canonical) and 'artifact_name' (alias)"
            ),
            spec_params={},
            evidence=[],
        )
        assert feedback.failure_code == "verification_spec_invalid"
        assert feedback.retryable is False

    def test_conflicting_spec_feedback_classified_correctly(self) -> None:
        """Conflicting spec failure should also be classified as verification_spec_invalid."""
        from lhos.runtime.verification_feedback import build_feedback_from_verification

        feedback = build_feedback_from_verification(
            verifier_type="file_exists",
            summary=(
                "verification_spec_invalid: file_exists spec has both "
                "path='file_a.txt' and artifact_name='file_b.txt' "
                "with conflicting values"
            ),
            spec_params={"path": "file_a.txt", "artifact_name": "file_b.txt"},
            evidence=[],
        )
        assert feedback.failure_code == "verification_spec_invalid"
        assert feedback.retryable is False

    def test_file_not_found_is_retryable(self, tmp_path: Path) -> None:
        """When the file doesn't exist but the spec is valid, it should be retryable."""
        verifier = FileExistsVerifier()
        spec = VerificationSpec(
            verifier_type="file_exists",
            parameters={"path": "nonexistent.txt"},
        )
        result = verifier.verify(_make_node(), spec, _make_context(tmp_path))

        assert result.passed is False
        # This is a file-not-found, not a spec error.
        assert "verification_spec_invalid" not in result.summary
        assert "nonexistent.txt" in result.summary
