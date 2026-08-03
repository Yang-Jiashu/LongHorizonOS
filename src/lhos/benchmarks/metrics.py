"""Benchmark metrics (spec 24) as pure functions.

Everything here operates on plain lists/dicts so it works identically for
graph-mode runs (event log + executions from SQLite) and the transcript
baseline (synthesized records). Wall-clock-dependent values are isolated in
``wall_time_seconds`` / ``*_time_seconds`` fields so the rest of a result row
is reproducible bit-for-bit across repeated runs of the same cell.

Spec 24.3 definitions implemented:

- Progress-Budget Curve: one sample per verified-progress change carrying
  timestamp, cumulative tokens / tool calls / cost / verified progress.
- AUPBC-token / AUPBC-time / AUPBC-tool-calls: normalized area under that
  curve (trapezoid), in [0, 1]-ish; 1.0 = all progress earned at zero budget.
- Useful Work Ratio: cost of the final successful attempt of every node that
  is VERIFIED at the end / total execution cost.
- Replanning Amplification: actually replanned nodes / oracle true affected.
- Invalidated Work Rate: cost of superseded attempts (every attempt except a
  node's final one) / total execution cost.
- Recovery Overhead: repeated cost after a failure / remaining task cost at
  failure time.
- Critical-path Stretch: actual wall time / oracle critical-path time.
"""

from __future__ import annotations

from itertools import pairwise as _pairwise
from typing import Any


# ------------------------------------------------------- progress-budget curve
def aupbc(curve: list[dict[str, Any]], x_key: str) -> float:
    """Normalized area under the progress-budget curve (trapezoid rule).

    ``curve`` samples carry ``verified_progress`` in [0, 1] and a budget axis
    ``x_key`` (``tokens`` | ``seconds`` | ``tool_calls``). The area is
    normalized by (final x * final progress) so curves are comparable across
    tasks; an empty/degenerate curve yields 0.0.
    """
    if not curve:
        return 0.0
    pts = [(float(p.get(x_key, 0.0)), float(p.get("verified_progress", 0.0))) for p in curve]
    pts.sort(key=lambda t: t[0])
    area = 0.0
    for (x0, y0), (x1, y1) in _pairwise(pts):
        area += (x1 - x0) * (y0 + y1) / 2.0
    max_x, max_y = pts[-1]
    denom = max_x * max_y
    if denom <= 0.0:
        return 0.0
    return round(area / denom, 6)


# ------------------------------------------------------------- work quality
def useful_work_ratio(executions: list[dict[str, Any]], final_verified_node_ids: set[str]) -> float:
    """Final-successful-attempt cost of VERIFIED nodes / total cost (24.3)."""
    total = sum(int(e.get("tokens", 0)) for e in executions)
    if total <= 0:
        return 0.0
    last_attempt: dict[str, dict[str, Any]] = {}
    for e in executions:
        node_id = e["node_id"]
        if e.get("attempt", 0) >= last_attempt.get(node_id, {}).get("attempt", -1):
            last_attempt[node_id] = e
    useful = sum(
        int(e.get("tokens", 0))
        for node_id, e in last_attempt.items()
        if node_id in final_verified_node_ids
    )
    return round(useful / total, 6)


def invalidated_work_rate(executions: list[dict[str, Any]]) -> float:
    """Superseded-attempt cost / total cost (24.3).

    Every attempt except a node's final attempt was later invalidated or
    repeated (transient failure, staleness, invalidation, crash replay).
    """
    total = sum(int(e.get("tokens", 0)) for e in executions)
    if total <= 0:
        return 0.0
    max_attempt: dict[str, int] = {}
    for e in executions:
        max_attempt[e["node_id"]] = max(max_attempt.get(e["node_id"], 0), int(e.get("attempt", 0)))
    superseded = sum(
        int(e.get("tokens", 0))
        for e in executions
        if int(e.get("attempt", 0)) < max_attempt[e["node_id"]]
    )
    return round(superseded / total, 6)


def replanning_amplification(replanned_count: int, oracle_affected_count: int) -> float:
    """Actually replanned nodes / oracle true affected nodes (24.3).

    Neutral 1.0 when no invalidation happened in either numerator or
    denominator; 0.0 when the oracle expected repair but none happened
    (e.g. repair disabled and the run stranded).
    """
    if oracle_affected_count <= 0:
        return 1.0 if replanned_count == 0 else float(replanned_count)
    return round(replanned_count / oracle_affected_count, 6)


def recovery_overhead(repeated_cost: float, remaining_cost_at_failure: float) -> float:
    """Repeated cost after failure / remaining task cost at failure (24.3).

    0.0 when there was no failure or the remaining cost is unknown/zero.
    """
    if remaining_cost_at_failure <= 0.0:
        return 0.0
    return round(repeated_cost / remaining_cost_at_failure, 6)


def critical_path_stretch(wall_seconds: float, oracle_critical_path_seconds: float) -> float:
    """Actual completion time / oracle critical-path time (24.3).

    0.0 when the oracle has no critical-path estimate (degenerate graphs).
    """
    if oracle_critical_path_seconds <= 0.0:
        return 0.0
    return round(wall_seconds / oracle_critical_path_seconds, 6)
