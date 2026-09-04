"""Deterministic provenance coverage and selective-repair demonstration.

This demo is intentionally smaller than :mod:`recovery_repair`.  It focuses on
the missing/unknown-input boundary called out in the v0.2 roadmap:

* execution records versioned reads in an append-only provenance journal;
* declared inputs are compared with observed inputs;
* strict policy fails closed when an executor reports an unknown read;
* a source-artifact version change invalidates only the graph-relative
  downstream cone, preserving an unrelated branch; and
* a durable JSONL replay verifies that the trace survived a close/re-open.

The demo does **not** claim to discover arbitrary Python reads automatically or
to replace the VPG/D3 semantic authority.  Dependencies in this scenario are
declared explicitly so that the result is honest and reproducible.
"""

from __future__ import annotations

import tempfile
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lhos.provenance import (
    CoveragePolicy,
    ExecutionContext,
    JSONLProvenanceStore,
    assess_coverage,
    evaluate_coverage,
)


class ProvenanceDemoAssertionError(RuntimeError):
    """Raised when a semantic invariant of the deterministic demo fails."""


def _fail(message: str) -> None:
    raise ProvenanceDemoAssertionError(f"provenance demo assertion failed: {message}")


@dataclass
class ProvenanceRepairSemantics:
    """Machine-readable outcome returned by :func:`run_provenance_repair`."""

    schema_version: str = "0.2"
    graph_id: str = "provenance-demo"
    initial_coverage: dict[str, str] = field(default_factory=dict)
    hidden_probe_coverage: str = ""
    hidden_probe_strict_allowed: bool = False
    strict_fail_closed: bool = False
    artifact_transition: dict[str, Any] = field(default_factory=dict)
    affected_tasks: list[str] = field(default_factory=list)
    preserved_tasks: list[str] = field(default_factory=list)
    repair_frontier: list[str] = field(default_factory=list)
    repaired_tasks: list[str] = field(default_factory=list)
    final_coverage: dict[str, str] = field(default_factory=dict)
    journal_event_count: int = 0
    durable_replay: bool = False
    graph_relative: bool = True
    automatic_dependency_discovery: bool = False
    semantic_authority: str = (
        "declared dependency graph + observed versioned provenance (demo only)"
    )
    metrics: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        """Return deterministic JSON-ready data."""

        return {
            "schema_version": self.schema_version,
            "graph_id": self.graph_id,
            "initial_coverage": dict(sorted(self.initial_coverage.items())),
            "hidden_probe_coverage": self.hidden_probe_coverage,
            "hidden_probe_strict_allowed": self.hidden_probe_strict_allowed,
            "strict_fail_closed": self.strict_fail_closed,
            "artifact_transition": self.artifact_transition,
            "affected_tasks": sorted(self.affected_tasks),
            "preserved_tasks": sorted(self.preserved_tasks),
            "repair_frontier": sorted(self.repair_frontier),
            "repaired_tasks": sorted(self.repaired_tasks),
            "final_coverage": dict(sorted(self.final_coverage.items())),
            "journal_event_count": self.journal_event_count,
            "durable_replay": self.durable_replay,
            "graph_relative": self.graph_relative,
            "automatic_dependency_discovery": self.automatic_dependency_discovery,
            "semantic_authority": self.semantic_authority,
            "metrics": dict(sorted(self.metrics.items())),
        }


def _propagate(
    seeds: set[str],
    dependencies: dict[str, tuple[str, ...]],
) -> list[str]:
    """Compute the deterministic downstream closure of ``seeds``."""

    consumers: dict[str, list[str]] = {task_id: [] for task_id in dependencies}
    for task_id, deps in dependencies.items():
        for dep in deps:
            consumers.setdefault(dep, []).append(task_id)
    for values in consumers.values():
        values.sort()

    affected: set[str] = set(seeds)
    queue: deque[str] = deque(sorted(seeds))
    while queue:
        source = queue.popleft()
        for target in consumers.get(source, ()):
            if target not in affected:
                affected.add(target)
                queue.append(target)
    return sorted(affected)


def _frontier(
    affected: set[str],
    dependencies: dict[str, tuple[str, ...]],
) -> list[str]:
    """Return affected nodes whose predecessors are not themselves affected."""

    return sorted(
        task_id
        for task_id in affected
        if not any(dep in affected for dep in dependencies.get(task_id, ()))
    )


def _coverage(
    graph_id: str,
    declared: dict[str, tuple[str, ...]],
    events_by_task: dict[str, list[Any]],
) -> dict[str, str]:
    reports = {
        task_id: assess_coverage(
            declared.get(task_id, ()),
            events,
            graph_id=graph_id,
            task_id=task_id,
        )
        for task_id, events in events_by_task.items()
    }
    return {task_id: report.status for task_id, report in reports.items()}


def run_provenance_repair(
    *,
    workspace_dir: str | None = None,
    state_path: str | None = None,
    pause: float = 0.0,
) -> tuple[Path, ProvenanceRepairSemantics]:
    """Run the deterministic provenance/repair scenario.

    ``state_path`` is the JSONL provenance journal path.  If omitted, a
    temporary workspace and journal are created.  The returned workspace is
    intentionally retained for callers that want to inspect the trace.
    """

    workspace = Path(workspace_dir or tempfile.mkdtemp(prefix="lhos_provenance_demo_"))
    workspace.mkdir(parents=True, exist_ok=True)
    journal_path = Path(state_path or workspace / "provenance.jsonl")
    journal_path.parent.mkdir(parents=True, exist_ok=True)

    graph_id = "provenance-demo"
    dependencies: dict[str, tuple[str, ...]] = {
        "FetchReport": (),
        "ComputeValuation": ("FetchReport",),
        "WriteConclusion": ("ComputeValuation",),
        "IndependentResearch": (),
    }
    declared: dict[str, tuple[str, ...]] = {
        "FetchReport": ("artifact://filing.csv",),
        "ComputeValuation": ("artifact://report.json",),
        "WriteConclusion": ("artifact://valuation.json",),
        "IndependentResearch": ("artifact://management.md",),
    }

    store = JSONLProvenanceStore(journal_path)
    events_by_task: dict[str, list[Any]] = {task_id: [] for task_id in dependencies}

    def execute(
        task_id: str,
        *,
        input_uri: str,
        input_artifact: str,
        input_version: int,
        output_uri: str,
        output_artifact: str,
        output_version: int,
    ) -> None:
        ctx = ExecutionContext(
            graph_id,
            task_id=task_id,
            attempt_id=f"{task_id.lower()}-attempt-{input_version}",
            semantic_epoch=input_version,
            store=store,
        )
        events_by_task[task_id].extend(
            [
                ctx.read(
                    input_uri,
                    artifact_id=input_artifact,
                    version=input_version,
                    content_hash=f"{input_artifact}-v{input_version}",
                ),
                ctx.write(
                    output_uri,
                    artifact_id=output_artifact,
                    version=output_version,
                    content_hash=f"{output_artifact}-v{output_version}",
                ),
            ]
        )

    # Initial closure: all four tasks have complete, explicit input coverage.
    execute(
        "FetchReport",
        input_uri="artifact://filing.csv",
        input_artifact="filing.csv",
        input_version=1,
        output_uri="artifact://report.json",
        output_artifact="report.json",
        output_version=1,
    )
    execute(
        "ComputeValuation",
        input_uri="artifact://report.json",
        input_artifact="report.json",
        input_version=1,
        output_uri="artifact://valuation.json",
        output_artifact="valuation.json",
        output_version=1,
    )
    execute(
        "WriteConclusion",
        input_uri="artifact://valuation.json",
        input_artifact="valuation.json",
        input_version=1,
        output_uri="artifact://conclusion.md",
        output_artifact="conclusion.md",
        output_version=1,
    )
    execute(
        "IndependentResearch",
        input_uri="artifact://management.md",
        input_artifact="management.md",
        input_version=1,
        output_uri="artifact://management-summary.md",
        output_artifact="management-summary.md",
        output_version=1,
    )
    sem = ProvenanceRepairSemantics(graph_id=graph_id)
    sem.initial_coverage = _coverage(graph_id, declared, events_by_task)
    if set(sem.initial_coverage.values()) != {"COMPLETE"}:
        _fail(f"initial coverage is not complete: {sem.initial_coverage}")

    # Explicitly demonstrate the hard boundary: a hidden external read is not
    # guessed.  Strict policy denies it rather than producing a false claim.
    hidden_ctx = ExecutionContext(
        graph_id,
        task_id="HiddenInputProbe",
        attempt_id="hidden-probe-1",
        store=store,
    )
    hidden_event = hidden_ctx.observe_unknown(
        resource_hint="",
        source="executor",
        note="environment variable read not declared by adapter",
    )
    hidden_report = assess_coverage(
        ("external://runtime/environment",),
        [hidden_event],
        graph_id=graph_id,
        task_id="HiddenInputProbe",
    )
    hidden_decision = evaluate_coverage(hidden_report, CoveragePolicy.STRICT)
    sem.hidden_probe_coverage = hidden_report.status
    sem.hidden_probe_strict_allowed = hidden_decision.allowed
    sem.strict_fail_closed = hidden_report.status == "UNKNOWN" and not hidden_decision.allowed
    if not sem.strict_fail_closed:
        _fail("strict policy admitted an unknown provenance input")

    # World change: filing.csv@v1 -> @v2.  Seeds are derived from observed
    # versioned input events; the rest is graph-relative causal propagation.
    sem.artifact_transition = {
        "artifact": "filing.csv",
        "old_version": 1,
        "new_version": 2,
    }
    seeds = {
        task_id
        for task_id, events in events_by_task.items()
        if any(
            event.is_input
            and event.artifact_id == "filing.csv"
            and event.version is not None
            and event.version < 2
            for event in events
        )
    }
    sem.affected_tasks = _propagate(seeds, dependencies)
    sem.preserved_tasks = sorted(set(dependencies) - set(sem.affected_tasks))
    sem.repair_frontier = _frontier(set(sem.affected_tasks), dependencies)
    if sem.affected_tasks != ["ComputeValuation", "FetchReport", "WriteConclusion"]:
        _fail(f"unexpected affected cone: {sem.affected_tasks}")
    if sem.preserved_tasks != ["IndependentResearch"]:
        _fail(f"independent branch was not preserved: {sem.preserved_tasks}")
    if sem.repair_frontier != ["FetchReport"]:
        _fail(f"unexpected repair frontier: {sem.repair_frontier}")

    # Local repair writes fresh exact-version provenance down the affected
    # chain.  Existing independent events are retained and never re-executed.
    repaired_specs = (
        (
            "FetchReport",
            "artifact://filing.csv",
            "filing.csv",
            2,
            "artifact://report.json",
            "report.json",
            2,
        ),
        (
            "ComputeValuation",
            "artifact://report.json",
            "report.json",
            2,
            "artifact://valuation.json",
            "valuation.json",
            2,
        ),
        (
            "WriteConclusion",
            "artifact://valuation.json",
            "valuation.json",
            2,
            "artifact://conclusion.md",
            "conclusion.md",
            2,
        ),
    )
    for (
        task_id,
        in_uri,
        in_artifact,
        in_version,
        out_uri,
        out_artifact,
        out_version,
    ) in repaired_specs:
        execute(
            task_id,
            input_uri=in_uri,
            input_artifact=in_artifact,
            input_version=in_version,
            output_uri=out_uri,
            output_artifact=out_artifact,
            output_version=out_version,
        )
    sem.repaired_tasks = [spec[0] for spec in repaired_specs]
    sem.final_coverage = _coverage(graph_id, declared, events_by_task)
    if set(sem.final_coverage.values()) != {"COMPLETE"}:
        _fail(f"final coverage is not complete: {sem.final_coverage}")

    # Close/re-open is a real durability check, not a counter assertion.
    before_replay = len(store.replay())
    store.close()
    reopened = JSONLProvenanceStore(journal_path)
    try:
        after_replay = len(reopened.replay())
    finally:
        reopened.close()
    sem.journal_event_count = after_replay
    sem.durable_replay = before_replay == after_replay and after_replay > 0
    if not sem.durable_replay:
        _fail("provenance journal replay did not preserve the event chain")

    sem.metrics = {
        "initial_tasks": len(dependencies),
        "initial_complete_tasks": sum(v == "COMPLETE" for v in sem.initial_coverage.values()),
        "invalidated_tasks": len(sem.affected_tasks),
        "preserved_tasks": len(sem.preserved_tasks),
        "repair_tasks": len(sem.repaired_tasks),
        "strict_unknown_probe_denied": sem.strict_fail_closed,
        "journal_events": sem.journal_event_count,
    }
    if pause > 0:
        time.sleep(pause)
    return workspace, sem


__all__ = [
    "ProvenanceDemoAssertionError",
    "ProvenanceRepairSemantics",
    "run_provenance_repair",
]
