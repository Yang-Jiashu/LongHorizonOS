"""Coverage accounting for observed runtime provenance."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

from .models import (
    CoverageReport,
    CoverageStatus,
    ProvenanceEvent,
    ProvenanceOperation,
)


def _event(value: ProvenanceEvent | Mapping[str, Any]) -> ProvenanceEvent:
    if isinstance(value, ProvenanceEvent):
        return value
    return ProvenanceEvent.model_validate(value)


def _normalise_declared(
    declared_inputs: Iterable[str] | Mapping[str, Any] | None,
) -> tuple[str, ...]:
    if declared_inputs is None:
        return ()
    values = declared_inputs.keys() if isinstance(declared_inputs, Mapping) else declared_inputs
    result = {str(item).strip() for item in values if str(item).strip()}
    return tuple(sorted(result))


def assess_coverage(
    declared_inputs: Iterable[str] | Mapping[str, Any] | None,
    events: Iterable[ProvenanceEvent | Mapping[str, Any]],
    *,
    graph_id: str = "",
    task_id: str = "",
    required_operations: Iterable[str | ProvenanceOperation] | None = None,
) -> CoverageReport:
    """Compare declared input resources with observed input events.

    Matching is exact on ``resource_uri`` (or ``artifact:<id>`` when a URI is
    unavailable).  Unknown observations are explicit ``known=False`` events;
    a recorder cannot infer an unobserved hidden dependency and callers should
    use ``observe_unknown`` when an executor detects one.
    """

    declared = _normalise_declared(declared_inputs)
    parsed = tuple(_event(item) for item in events)
    input_events = tuple(event for event in parsed if event.is_input)
    observed: set[str] = set()
    unknown: set[str] = set()
    operation_counts: Counter[str] = Counter()

    for event in input_events:
        operation_counts[event.op.value] += 1
        key = event.resource_key
        if not event.known or not key:
            unknown.add(key or f"event:{event.event_id or event.sequence}")
        else:
            observed.add(key)

    missing = set(declared) - observed
    undeclared = observed - set(declared)

    required: set[str] = set()
    for op in required_operations or ():
        required.add(op.value if isinstance(op, ProvenanceOperation) else str(op).lower())
    missing_ops = required - set(operation_counts)

    if unknown:
        status = CoverageStatus.UNKNOWN
    elif missing or undeclared or missing_ops:
        status = CoverageStatus.PARTIAL
    else:
        status = CoverageStatus.COMPLETE

    denominator = len(declared) + len(undeclared)
    ratio = 1.0 if denominator == 0 else len(set(declared) & observed) / denominator
    warnings: list[str] = []
    if not parsed:
        warnings.append("no provenance events recorded")
    if unknown:
        warnings.append("one or more input observations lack an identifiable resource")
    if not declared and observed:
        warnings.append("observed inputs were not declared by the task")

    report = CoverageReport(
        graph_id=graph_id,
        task_id=task_id,
        status=status,
        declared_inputs=declared,
        observed_inputs=tuple(sorted(observed)),
        missing_inputs=tuple(sorted(missing)),
        undeclared_inputs=tuple(sorted(undeclared)),
        unknown_inputs=tuple(sorted(unknown)),
        missing_operations=tuple(sorted(missing_ops)),
        event_count=len(parsed),
        input_event_count=len(input_events),
        operation_counts=dict(sorted(operation_counts.items())),
        coverage_ratio=round(ratio, 12),
        warnings=tuple(warnings),
    )
    return report.with_hash()


def coverage_from_events(
    events: Iterable[ProvenanceEvent | Mapping[str, Any]],
    *,
    declared_inputs: Iterable[str] | Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> CoverageReport:
    """Convenience wrapper with the event stream as the first argument."""

    return assess_coverage(declared_inputs, events, **kwargs)


__all__ = ["assess_coverage", "coverage_from_events"]
