"""Admission policies for provenance coverage.

Coverage is intentionally a policy decision, not a hidden side effect.  A
host can run in ``legacy`` mode while migrating existing executors, use
``audit`` to measure gaps without changing execution behaviour, and opt into
``strict`` once all input reads are captured.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .models import CoverageReport


class CoveragePolicy(StrEnum):
    LEGACY = "legacy"
    AUDIT = "audit"
    STRICT = "strict"


class CoverageDecision(BaseModel):
    """Result of applying a policy to one coverage report."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    policy: CoveragePolicy
    allowed: bool
    status: str
    reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    report_hash: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def denied(self) -> bool:
        return not self.allowed


class ProvenanceCoverageError(RuntimeError):
    """Raised by :func:`enforce_coverage` when strict admission fails."""

    def __init__(self, decision: CoverageDecision):
        self.decision = decision
        detail = "; ".join(decision.reasons) or decision.status
        super().__init__(f"provenance coverage denied ({decision.policy}): {detail}")


def _as_policy(policy: CoveragePolicy | str) -> CoveragePolicy:
    if isinstance(policy, CoveragePolicy):
        return policy
    try:
        return CoveragePolicy(str(policy).lower())
    except ValueError as exc:
        raise ValueError(f"unknown provenance coverage policy: {policy!r}") from exc


def evaluate_coverage(
    report: CoverageReport,
    policy: CoveragePolicy | str = CoveragePolicy.AUDIT,
) -> CoverageDecision:
    """Evaluate a report without mutating the report or runtime.

    ``legacy`` always admits for backwards compatibility.  ``audit`` also
    admits but returns warnings.  ``strict`` is fail-closed: only a
    ``COMPLETE`` report with no unknown, missing, or undeclared inputs is
    admitted.
    """

    selected = _as_policy(policy)
    reasons: list[str] = []
    warnings: list[str] = list(report.warnings)

    if report.missing_inputs:
        reasons.append("missing declared inputs: " + ", ".join(report.missing_inputs))
    if report.undeclared_inputs:
        reasons.append("undeclared observed inputs: " + ", ".join(report.undeclared_inputs))
    if report.unknown_inputs:
        reasons.append("unknown input observations: " + ", ".join(report.unknown_inputs))
    if report.missing_operations:
        reasons.append("missing required operations: " + ", ".join(report.missing_operations))
    if report.status != "COMPLETE" and not reasons:
        reasons.append(f"coverage status is {report.status}")

    if selected is CoveragePolicy.LEGACY or selected is CoveragePolicy.AUDIT:
        allowed = True
        if reasons:
            warnings.extend(reasons)
    else:
        allowed = report.status == "COMPLETE" and not reasons

    return CoverageDecision(
        policy=selected,
        allowed=allowed,
        status=report.status,
        reasons=tuple(reasons),
        warnings=tuple(dict.fromkeys(warnings)),
        report_hash=report.report_hash,
    )


def enforce_coverage(
    report: CoverageReport,
    policy: CoveragePolicy | str = CoveragePolicy.STRICT,
) -> CoverageDecision:
    """Evaluate and raise :class:`ProvenanceCoverageError` if denied."""

    decision = evaluate_coverage(report, policy)
    if not decision.allowed:
        raise ProvenanceCoverageError(decision)
    return decision


__all__ = [
    "CoverageDecision",
    "CoveragePolicy",
    "ProvenanceCoverageError",
    "enforce_coverage",
    "evaluate_coverage",
]
