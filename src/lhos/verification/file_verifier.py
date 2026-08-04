"""File-based verifiers: file_exists, file_changed, file_contains,
artifact_exists (spec 14.2)."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

from lhos.domain.models import GraphNode
from lhos.domain.verification import VerificationResult, VerificationSpec
from lhos.ports.verifier import VerificationContext


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _resolve(workspace_dir: str, rel: str) -> Path:
    return (Path(workspace_dir).resolve() / rel).resolve()


class FileExistsVerifier:
    """Passes when the named file exists in the workspace.

    Accepts both ``path`` and ``artifact_name`` parameter names for
    backward compatibility with planner output that may use either.
    """

    verifier_type = "file_exists"

    def verify(
        self, node: GraphNode, spec: VerificationSpec, context: VerificationContext
    ) -> VerificationResult:
        rel = spec.parameters.get("path") or spec.parameters.get("artifact_name")
        if not rel:
            return VerificationResult(
                passed=False,
                summary="file_exists: no path or artifact_name parameter provided",
            )
        path = _resolve(context.workspace_dir, rel)
        passed = path.exists()
        evidence = []
        if passed:
            evidence.append(
                {
                    "evidence_type": "file_hash",
                    "uri": str(path),
                    "content_hash": _sha256(path),
                    "summary": f"file exists: {rel}",
                    "metadata": {"path": rel},
                }
            )
        return VerificationResult(
            passed=passed,
            summary=f"file_exists({rel}) -> {passed}",
            evidence=evidence,
        )


class FileChangedVerifier:
    """Passes when the file's sha256 differs from the baseline recorded before
    the worker ran (baseline None + file now exists also counts as changed)."""

    verifier_type = "file_changed"

    def verify(
        self, node: GraphNode, spec: VerificationSpec, context: VerificationContext
    ) -> VerificationResult:
        rel = spec.parameters.get("path") or spec.parameters.get("artifact_name")
        if not rel:
            return VerificationResult(
                passed=False, summary="file_changed: no path or artifact_name"
            )
        path = _resolve(context.workspace_dir, rel)
        baseline = context.baseline_hashes.get(rel)
        current = _sha256(path) if path.exists() else None
        passed = current is not None and current != baseline
        evidence = []
        if passed:
            evidence.append(
                {
                    "evidence_type": "file_hash",
                    "uri": str(path),
                    "content_hash": current,
                    "summary": f"file changed: {rel}",
                    "metadata": {"path": rel, "baseline_hash": baseline},
                }
            )
        return VerificationResult(
            passed=passed,
            summary=f"file_changed({rel}): baseline={baseline} current={current}",
            evidence=evidence,
        )


class FileContainsVerifier:
    verifier_type = "file_contains"

    def verify(
        self, node: GraphNode, spec: VerificationSpec, context: VerificationContext
    ) -> VerificationResult:
        rel = spec.parameters.get("path") or spec.parameters.get("artifact_name")
        if not rel:
            return VerificationResult(
                passed=False, summary="file_contains: no path or artifact_name"
            )
        path = _resolve(context.workspace_dir, rel)
        if not path.exists():
            return VerificationResult(passed=False, summary=f"file missing: {rel}")
        content = path.read_text(encoding="utf-8")
        substring = spec.parameters.get("substring")
        pattern = spec.parameters.get("regex")
        if substring is not None:
            passed = substring in content
            desc = f"substring {substring!r}"
        elif pattern is not None:
            passed = re.search(pattern, content) is not None
            desc = f"regex {pattern!r}"
        else:
            return VerificationResult(
                passed=False, summary="file_contains: need substring or regex"
            )
        evidence = []
        if passed:
            evidence.append(
                {
                    "evidence_type": "file_hash",
                    "uri": str(path),
                    "content_hash": _sha256(path),
                    "summary": f"{rel} contains {desc}",
                    "metadata": {"path": rel},
                }
            )
        return VerificationResult(
            passed=passed,
            summary=f"file_contains({rel}, {desc}) -> {passed}",
            evidence=evidence,
        )


class ArtifactExistsVerifier:
    """Passes when the named artifact exists as a workspace file or was
    declared in the worker's produced_artifacts."""

    verifier_type = "artifact_exists"

    def verify(
        self, node: GraphNode, spec: VerificationSpec, context: VerificationContext
    ) -> VerificationResult:
        name = spec.parameters.get("artifact_name") or spec.parameters.get("path")
        if not name:
            return VerificationResult(passed=False, summary="artifact_exists: no artifact_name")
        path = _resolve(context.workspace_dir, name)
        if path.exists():
            return VerificationResult(
                passed=True,
                summary=f"artifact file exists: {name}",
                evidence=[
                    {
                        "evidence_type": "file_hash",
                        "uri": str(path),
                        "content_hash": _sha256(path),
                        "summary": f"artifact exists: {name}",
                        "metadata": {"artifact_name": name},
                    }
                ],
            )
        for artifact in context.worker_result.get("produced_artifacts", []):
            if artifact.get("path") == name or artifact.get("artifact_name") == name:
                return VerificationResult(
                    passed=True,
                    summary=f"artifact declared by worker: {name}",
                    evidence=[
                        {
                            "evidence_type": "json_artifact",
                            "summary": f"worker declared artifact {name}",
                            "metadata": {"artifact_name": name, "artifact": artifact},
                        }
                    ],
                )
        return VerificationResult(passed=False, summary=f"artifact not found: {name}")
