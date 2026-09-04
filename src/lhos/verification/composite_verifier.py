"""Composite verifiers (spec 14.4) and the manual verifier."""

from __future__ import annotations

from lhos.domain.models import GraphNode
from lhos.domain.verification import VerificationResult, VerificationSpec
from lhos.ports.verifier import VerificationContext


class _CompositeBase:
    def __init__(self, registry):
        self._registry = registry

    def _run_children(
        self,
        node: GraphNode,
        spec: VerificationSpec,
        context: VerificationContext,
    ) -> list[VerificationResult]:
        children = spec.parameters.get("children", [])
        results = []
        for child in children:
            child_spec = VerificationSpec.from_raw(child)
            verifier = self._registry.get(child_spec.verifier_type)
            results.append(verifier.verify(node, child_spec, context))
        return results

    @staticmethod
    def _merge_evidence(results: list[VerificationResult]) -> list[dict]:
        evidence: list[dict] = []
        for r in results:
            evidence.extend(r.evidence)
        return evidence


class CompositeAndVerifier(_CompositeBase):
    verifier_type = "composite_and"

    def verify(
        self, node: GraphNode, spec: VerificationSpec, context: VerificationContext
    ) -> VerificationResult:
        results = self._run_children(node, spec, context)
        passed = bool(results) and all(r.passed for r in results)
        summary = "; ".join(r.summary for r in results)
        return VerificationResult(
            passed=passed,
            summary=f"composite_and -> {passed}: {summary}",
            evidence=self._merge_evidence(results) if passed else [],
        )


class CompositeOrVerifier(_CompositeBase):
    verifier_type = "composite_or"

    def verify(
        self, node: GraphNode, spec: VerificationSpec, context: VerificationContext
    ) -> VerificationResult:
        results = self._run_children(node, spec, context)
        passed = any(r.passed for r in results)
        summary = "; ".join(r.summary for r in results)
        return VerificationResult(
            passed=passed,
            summary=f"composite_or -> {passed}: {summary}",
            evidence=self._merge_evidence(results) if passed else [],
        )


class ManualVerifier:
    """Manual verification: the node must wait for a human. Returns a pending
    result; the verification gate parks the node in WAITING (spec 14.1 flow
    keeps the state machine legal: RUNNING -> WAITING)."""

    verifier_type = "manual"

    def verify(
        self, node: GraphNode, spec: VerificationSpec, context: VerificationContext
    ) -> VerificationResult:
        instructions = spec.parameters.get("instructions", "awaiting human confirmation")
        return VerificationResult(
            passed=False,
            summary=f"manual verification pending: {instructions}",
            pending=True,
        )
