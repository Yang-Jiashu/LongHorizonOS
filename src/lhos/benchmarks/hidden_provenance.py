"""Deterministic hidden-provenance safety benchmark.

This benchmark does not claim to discover arbitrary Python dependencies.  It
measures the boundary that the current runtime *can* enforce:

* a declared input with no mediated read is ``PARTIAL`` and cannot pass strict
  provenance admission;
* an executor which reports an unidentifiable input is ``UNKNOWN`` and cannot
  be promoted to ``VERIFIED`` under the strict policy;
* the audit policy remains available for compatibility/migration.

The workload is entirely in-process and uses no model/provider/network calls.
"""

from __future__ import annotations

from typing import Any

from lhos.provenance import CoveragePolicy, CoverageStatus
from lhos.sdk import (
    Task,
    build_coverage_report,
    create_execution_context,
    evaluate_verification_coverage,
)

BENCHMARK_VERSION = 1


def _missing_read_case() -> dict[str, Any]:
    task = Task(
        "missing-read",
        inputs=("workspace://declared.txt",),
        executor_api="context_v1",
        provenance_policy=CoveragePolicy.STRICT,
    )
    context = create_execution_context(
        "hidden-provenance-missing",
        task.task_id,
        executor_api="context_v1",
    )
    report = build_coverage_report(task, context)
    decision = evaluate_verification_coverage(task, report)
    return {
        "status": report.status.value,
        "missing_inputs": list(report.missing_inputs),
        "strict_allowed": decision.allowed,
        "strict_denied": not decision.allowed,
        "reasons": list(decision.reasons),
        "report_hash_present": bool(report.report_hash),
    }


def _unknown_read_case() -> dict[str, Any]:
    task = Task(
        "unknown-read",
        inputs=("workspace://declared.txt",),
        executor_api="context_v1",
        provenance_policy=CoveragePolicy.STRICT,
    )
    context = create_execution_context(
        "hidden-provenance-unknown",
        task.task_id,
        executor_api="context_v1",
    )
    # The declared input was read through the recorder, but a raw Python/tool
    # path also touched an input that cannot be identified precisely.
    context.read(
        "workspace://declared.txt",
        artifact_id="declared.txt",
        version=1,
        content_hash="a" * 64,
    )
    context.observe_unknown(resource_hint="env://hidden", channel="raw-python")
    report = build_coverage_report(task, context)
    strict = evaluate_verification_coverage(task, report)
    audit = evaluate_verification_coverage(
        task,
        report,
        policy=CoveragePolicy.AUDIT,
    )
    return {
        "status": report.status.value,
        "unknown_inputs": list(report.unknown_inputs),
        "strict_allowed": strict.allowed,
        "strict_denied": not strict.allowed,
        "audit_allowed": audit.allowed,
        "strict_reasons": list(strict.reasons),
        "audit_warnings": list(audit.warnings),
        "report_hash_present": bool(report.report_hash),
    }


def run_hidden_provenance_benchmark() -> dict[str, Any]:
    """Run the offline hidden-read safety benchmark and return JSON data."""

    missing = _missing_read_case()
    unknown = _unknown_read_case()
    checks = {
        "missing_read_is_partial": missing["status"] == CoverageStatus.PARTIAL.value,
        "missing_read_strict_denied": missing["strict_denied"],
        "unknown_read_is_unknown": unknown["status"] == CoverageStatus.UNKNOWN.value,
        "unknown_read_strict_denied": unknown["strict_denied"],
        "audit_migration_path_allowed": unknown["audit_allowed"],
    }
    return {
        "benchmark": "hidden_provenance",
        "benchmark_version": BENCHMARK_VERSION,
        "scope": {
            "offline": True,
            "tests_runtime_coverage_boundary": True,
            "does_not_discover_arbitrary_python_reads": True,
            "does_not_measure_model_quality_or_provider_cost": True,
        },
        "missing_read_case": missing,
        "unknown_read_case": unknown,
        "checks": checks,
        "valid": all(checks.values()),
    }


__all__ = ["BENCHMARK_VERSION", "run_hidden_provenance_benchmark"]
