"""LongHorizonOS Core V1 — native CLI (read-only observability, E3).

Commands: status, inspect, graph, and explicit VPG history lifecycle tools.
The normal status/inspect/graph commands remain read-only projections over the
public SDK. VPG lifecycle writes require explicit operator confirmation and
never go through the AgentOS semantic patch path.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys

from lhos.sdk import AgentOS

NL = chr(10)


def _redact(s: str) -> str:
    s = re.sub(
        r"(?i)\b(authorization\s*:\s*(?:bearer|basic)\s+)[^\s,;]+",
        r"\1***",
        s,
    )
    s = re.sub(
        r"(?i)\b((?:api[_-]?key|secret|token|password|auth(?!orization))\s*[:=]\s*)"
        r"[^\s;,&]+",
        r"\1***",
        s,
    )
    return s


def _status(os_: AgentOS, goal_id: str, as_json: bool) -> int:
    try:
        sv = os_.status_view(goal_id)
    except Exception as e:
        print(f"error: could not read goal {goal_id!r}: {e}", file=sys.stderr)
        return 1
    if as_json:
        print(json.dumps(sv.as_dict(), indent=2, sort_keys=True))
    else:
        print(_redact(sv.render_ascii()))
    return 0


def _inspect(os_: AgentOS, goal_id: str, kind: str, obj: str, as_json: bool) -> int:
    if kind == "task":
        sv = os_.status_view(goal_id)
        tv = sv.tasks.get(obj)
        if tv is None:
            print(f"error: task {obj!r} not found", file=sys.stderr)
            return 1
        lines = os_.explain(goal_id, obj)
        if as_json:
            print(
                json.dumps(
                    {"goal": goal_id, "task": obj, "view": tv, "why": lines},
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            print(NL.join(_redact(ln) for ln in lines))
    elif kind == "evidence":
        sv = os_.status_view(goal_id)
        found = [(t, v) for t, v in sv.tasks.items() if v.get("supporting_evidence") == obj]
        if not found:
            print(f"error: evidence {obj!r} not found on any task", file=sys.stderr)
            return 1
        t, v = found[0]
        if as_json:
            print(
                json.dumps(
                    {"goal": goal_id, "evidence": obj, "task": t, "view": v},
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            print(f"Evidence {obj}")
            print(f"  task: {t}")
            print(f"  bound artifact: {v.get('artifact')}@{v.get('artifact_version')}")
            print(f"  current applicable: {v.get('evidence_current_applicable')}")
    else:
        print(f"error: unknown inspect kind {kind!r} (use task|evidence)", file=sys.stderr)
        return 2
    return 0


def _graph(os_: AgentOS, goal_id: str, as_json: bool) -> int:
    try:
        lines = os_.graph_lines(goal_id)
    except Exception as e:
        print(f"error: could not render graph for {goal_id!r}: {e}", file=sys.stderr)
        return 1
    if as_json:
        sv = os_.status_view(goal_id)
        print(json.dumps({"goal": goal_id, "view": sv.as_dict()}, indent=2, sort_keys=True))
    else:
        print(NL.join(_redact(ln) for ln in lines))
    return 0


def _demo_glyphs() -> tuple[str, str, str]:
    """check / cross / boom, downgraded to ASCII when stdout cannot encode them."""
    fancy = (chr(0x2713), chr(0x2717), chr(0x1F4A5))
    enc = getattr(sys.stdout, "encoding", None) or "ascii"
    try:
        "".join(fancy).encode(enc)
    except (UnicodeEncodeError, LookupError):
        return ("v", "x", "!!")
    return fancy


def _demo_recovery_repair(as_json: bool, paced: bool) -> int:
    """Run the flagship one-command demo (deterministic, real Core)."""
    from lhos.demo.recovery_repair import DemoAssertionError, run_recovery_repair

    pause = 0.5 if paced else 0.0
    try:
        _os, ws_dir, sem = run_recovery_repair(pause=pause)
    except DemoAssertionError as e:
        print(f"demo semantic assertion failed: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"demo runtime error: {e}", file=sys.stderr)
        return 2
    T, X, SK = _demo_glyphs()
    if as_json:
        print(
            json.dumps(
                {"demo": "recovery-repair", "workspace": str(ws_dir), "result": sem.as_dict()},
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    lines = []
    tr = sem.artifact_transition
    lines.append("LONGHORIZONOS - Recovery + Semantic Reconciliation Demo")
    lines.append("")
    lines.append("===== 1. BUILD VERIFIED PROGRESS =====")
    lines.append("Goal: Ship a verified feature")
    for t in sorted(sem.initial_verified):
        lines.append(f"  {T} {t:<24} VERIFIED")
    lines.append("")
    lines.append("GOAL CLOSED")
    lines.append("")
    lines.append("===== 2. WORKER FAILURE =====")
    lines.append(
        f"{SK} coder-1 terminates  (mode: {sem.metrics.get('ownership_recovery_mode', '-')})"
    )
    lines.append("  Task: Implement")
    lines.append("  Kernel Lease: RELEASED")
    lines.append(f"  Ownership recovered {T}")
    lines.append("")
    lines.append("===== 3. WORLD CHANGED =====")
    lines.append(
        f"  Artifact: {tr.get('artifact', '?')}@v{tr.get('old_version', '?')} -> @v{tr.get('new_version', '?')}"
    )
    lines.append(f"  old Evidence historical: {sem.old_evidence_historical!s}")
    lines.append(
        "  old Evidence current applicability: " + ("NO" if sem.old_evidence_not_current else "?")
    )
    lines.append("")
    lines.append("===== 4. SEMANTIC RECONCILIATION =====")
    for t in sorted(set(sem.initial_verified)):
        if t in sem.affected_tasks:
            lines.append(f"  {X} {t:<24} STALE")
        else:
            lines.append(f"  {T} {t:<24} VERIFIED   PRESERVED")
    lines.append(f"  Invalidated tasks: {len(sem.affected_tasks)}")
    lines.append(f"  Preserved VERIFIED tasks: {len(sem.preserved_tasks)}")
    lines.append(f"  Repair Frontier: {', '.join(sem.repair_frontier) or '(empty)'}")
    lines.append("  GOAL REOPENED")
    lines.append("")
    lines.append("===== 5. LOCAL REPAIR =====")
    lines.append(
        f"  D2 schedules: {', '.join(sem.repair_frontier) or '-'} with new exact-version Evidence"
    )
    lines.append(
        f"  repair executions: {sem.repair_attempts}; new Evidence: {sem.new_evidence_count}"
    )
    lines.append("")
    lines.append("===== 6. SEMANTIC CLOSURE RESTORED =====")
    for t in sorted(sem.final_verified):
        lines.append(f"  {T} {t:<24} VERIFIED")
    lines.append("GOAL CLOSED")
    lines.append("")
    lines.append("SUMMARY")
    lines.append(f"  Worker crash recovered: {'YES' if sem.crash_recovered else 'NO'}")
    lines.append(
        f"  Artifact versions: v{tr.get('old_version', '?')} -> v{tr.get('new_version', '?')}"
    )
    lines.append(f"  Invalidated tasks: {len(sem.affected_tasks)}")
    lines.append(f"  Preserved VERIFIED tasks: {len(sem.preserved_tasks)}")
    lines.append(f"  Minimal repair used: {'YES' if sem.repair_frontier else 'NO'}")
    lines.append(f"  Full restart avoided: {'YES' if sem.full_restart_avoided else 'NO'}")
    lines.append(f"  New Evidence required: {'YES' if sem.new_evidence_count else 'NO'}")
    lines.append(f"  Semantic closure restored: {'YES' if sem.final_closed else 'NO'}")
    print(chr(10).join(lines))
    return 0


def _demo_provenance_repair(as_json: bool, paced: bool) -> int:
    """Run the deterministic provenance coverage + selective repair demo."""
    from lhos.demo.provenance_repair import (
        ProvenanceDemoAssertionError,
        run_provenance_repair,
    )

    pause = 0.5 if paced else 0.0
    try:
        workspace, semantics = run_provenance_repair(pause=pause)
    except ProvenanceDemoAssertionError as exc:
        print(f"demo semantic assertion failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"demo runtime error: {exc}", file=sys.stderr)
        return 2

    if as_json:
        print(
            json.dumps(
                {
                    "demo": "provenance-repair",
                    "workspace": str(workspace),
                    "result": semantics.as_dict(),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    print("LONGHORIZONOS - Provenance Coverage + Selective Repair Demo")
    print("")
    print("===== 1. OBSERVE VERSIONED INPUTS =====")
    print("  initial coverage: COMPLETE (all declared inputs observed)")
    print("  journal: durable JSONL + hash-chain")
    print("")
    print("===== 2. FAIL CLOSED ON HIDDEN INPUT =====")
    print(f"  coverage: {semantics.hidden_probe_coverage}")
    print("  strict policy: " + ("DENY (fail closed)" if semantics.strict_fail_closed else "ALLOW"))
    print("")
    print("===== 3. WORLD CHANGED =====")
    transition = semantics.artifact_transition
    print(
        f"  {transition.get('artifact', '?')}@v{transition.get('old_version', '?')}"
        f" -> @v{transition.get('new_version', '?')}"
    )
    print(
        f"  affected: {', '.join(semantics.affected_tasks)}"
        f"  | preserved: {', '.join(semantics.preserved_tasks)}"
    )
    print(f"  repair frontier: {', '.join(semantics.repair_frontier)}")
    print("")
    print("===== 4. LOCAL REPAIR + DURABLE REPLAY =====")
    print(f"  repaired: {', '.join(semantics.repaired_tasks)}")
    print(f"  final coverage: COMPLETE ({len(semantics.final_coverage)} tasks)")
    print(f"  journal replay: {'PASS' if semantics.durable_replay else 'FAIL'}")
    print("")
    print("SUMMARY")
    print(f"  strict unknown-input denied: {'YES' if semantics.strict_fail_closed else 'NO'}")
    print(f"  graph-relative repair: {'YES' if semantics.graph_relative else 'NO'}")
    print(
        "  automatic dependency discovery: "
        + ("YES" if semantics.automatic_dependency_discovery else "NO (explicit demo graph)")
    )
    print(f"  events durably replayed: {semantics.journal_event_count}")
    return 0


def _demo_online_supervisor(as_json: bool) -> int:
    """Run the bounded caller-owned event-supervisor vertical slice."""
    from lhos.demo.online_supervisor import (
        OnlineSupervisorDemoAssertionError,
        run_online_supervisor,
    )

    agent_os = None
    try:
        agent_os, semantics = run_online_supervisor()
    except OnlineSupervisorDemoAssertionError as exc:
        print(f"demo semantic assertion failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"demo runtime error: {exc}", file=sys.stderr)
        return 2
    finally:
        if agent_os is not None:
            agent_os.close()

    if as_json:
        print(
            json.dumps(
                {"demo": "online-supervisor", "result": semantics.as_dict()},
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    print("LONGHORIZONOS - Bounded Online Supervisor Demo")
    print("")
    print("===== 1. START =====")
    print(f"  supervisor state: {semantics.start_state.upper()}")
    print("  lifecycle: caller-owned (no daemon/background thread)")
    print("")
    print("===== 2. OBSERVE =====")
    print(f"  step status: {semantics.observation_status.upper()}")
    print(f"  graph version: {semantics.graph_versions[0] if semantics.graph_versions else '-'}")
    print("")
    print("===== 3. BOUNDED EPOCHS =====")
    print(f"  statuses: {', '.join(semantics.execution_statuses) or '-'}")
    print(f"  dispatched: {', '.join(semantics.dispatched_task_ids) or '-'}")
    print(f"  verified: {', '.join(semantics.verified_task_ids) or '-'}")
    print(f"  graph versions: {', '.join(str(v) for v in semantics.graph_versions) or '-'}")
    print("")
    print("===== 4. CLOSED =====")
    print(f"  supervisor state: {semantics.final_state.upper()}")
    print(f"  goal closed: {'YES' if semantics.final_closed else 'NO'}")
    print(f"  epochs attempted: {semantics.epochs_attempted}")
    print(f"  stop reason: {semantics.stop_reason or '-'}")
    print("")
    print("SCOPE")
    print("  controlled local executor/verifier; no LLM, GPU telemetry, or always-on daemon")
    return 0


def _benchmark(
    quick: bool,
    as_json: bool,
    *,
    live_model: bool = False,
    model: str = "gpt-5.6-sol",
    live_timeout_seconds: float = 60.0,
) -> int:
    """Run the deterministic benchmark and an optional live-model probe."""
    from lhos.benchmarks.semantic_repair.run import run_benchmark

    try:
        if live_model:
            summary = run_benchmark(
                quick=quick,
                live_model=True,
                model=model,
                live_timeout_seconds=live_timeout_seconds,
            )
        else:
            summary = run_benchmark(quick=quick)
    except Exception as e:
        print(f"benchmark error: {e}", file=sys.stderr)
        return 2
    passed = summary["valid_trials"] == summary["total_trials"]
    if as_json:
        output = dict(summary)
        output["correctness_passed"] = passed
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0 if passed else 1

    agg = summary["aggregate"].get("overall", {})
    print("LONGHORIZONOS SEMANTIC-REPAIR BENCHMARK")
    print(f"  mode: {'quick' if quick else 'full'}")
    print(
        f"  trials: {summary['total_trials']}  valid: {summary['valid_trials']}"
        f"  invalid: {summary['invalid_trials']}"
    )
    print(f"  mean preservation ratio:  {agg.get('mean_preservation_ratio')}")
    print(f"  mean recomputation ratio: {agg.get('mean_recomputation_ratio')}")
    print(
        f"  under-invalidation: {agg.get('under_invalidation_total')}"
        f"  over-invalidation: {agg.get('over_invalidation_total')}"
        f"  ownership conflicts: {agg.get('ownership_conflicts_total')}"
        f"  false verified: {agg.get('false_verified_total')}"
    )
    if summary.get("live_model"):
        live = summary["live_model"]
        live_full = live["strategies"]["full_restart"]
        live_checkpoint = live["strategies"]["task_dag_checkpoint"]
        live_lhos = live["strategies"]["longhorizonos"]
        print(
            f"  live model: {live['model']}  calls (full/checkpoint/lhos): "
            f"{live_full.get('model_calls')}/{live_checkpoint.get('model_calls')}/"
            f"{live_lhos.get('model_calls')}"
        )
        print(
            f"  live LongHorizonOS: p50 {live_lhos.get('latency_p50_ms')} ms  "
            f"usage reported {live_lhos.get('usage_reported_calls')}/"
            f"{live_lhos.get('model_calls')}"
        )
    print(
        "  correctness: "
        + ("PASS (all trials valid)" if passed else "FAIL (invalid or incorrect trials present)")
    )
    print("  raw results: " + str(summary.get("raw_sha256", "")[:12]))
    return 0 if passed else 1


def _benchmark_hidden_provenance(as_json: bool) -> int:
    """Run the offline hidden-read provenance safety benchmark."""

    from lhos.benchmarks.hidden_provenance import run_hidden_provenance_benchmark

    try:
        summary = run_hidden_provenance_benchmark()
    except Exception as e:
        print(f"benchmark error: {e}", file=sys.stderr)
        return 2
    if as_json:
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0 if summary["valid"] else 1

    missing = summary["missing_read_case"]
    unknown = summary["unknown_read_case"]
    print("LONGHORIZONOS HIDDEN-PROVENANCE BENCHMARK")
    print(f"  missing declared read: {missing['status']}")
    print(f"  missing read strict admission: {'DENY' if missing['strict_denied'] else 'ALLOW'}")
    print(f"  reported hidden read: {unknown['status']}")
    print(f"  hidden read strict admission: {'DENY' if unknown['strict_denied'] else 'ALLOW'}")
    print(f"  audit migration path: {'ALLOW' if unknown['audit_allowed'] else 'DENY'}")
    print(f"  correctness: {'PASS' if summary['valid'] else 'FAIL'}")
    print(
        "  scope: mediated provenance safety boundary; "
        "arbitrary Python dependency discovery is not implemented"
    )
    return 0 if summary["valid"] else 1


def _benchmark_async_agentos(
    as_json: bool,
    *,
    task_count: int = 24,
    delay_ms: float = 50.0,
    max_concurrency: int = 4,
    agent_concurrency: int = 2,
    agent_count: int = 2,
    min_speedup: float = 1.5,
    repetitions: int = 3,
) -> int:
    """Run the offline public-AgentOS async execution benchmark."""
    from lhos.benchmarks.async_worker_runtime import run_benchmark

    try:
        report = run_benchmark(
            task_count=task_count,
            delay_seconds=delay_ms / 1000,
            max_concurrency=max_concurrency,
            agent_concurrency=agent_concurrency,
            agent_count=agent_count,
            min_speedup=min_speedup,
            repetitions=repetitions,
        )
    except Exception as e:
        print(f"benchmark error: {e}", file=sys.stderr)
        return 2

    if as_json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["valid"] else 1

    baseline = report["baseline"]
    parallel = report["async_runtime"]
    comparison = report["comparison"]
    scope = report["scope"]
    print("LONGHORIZONOS ASYNC AGENTOS END-TO-END BENCHMARK")
    print(
        f"  workload: {report['workload']['task_count']} controlled I/O tasks, "
        f"{report['workload']['agent_count']} agents"
    )
    print(
        f"  serial: {baseline['elapsed_seconds']:.4f}s  "
        f"async: {parallel['elapsed_seconds']:.4f}s  "
        f"median paired speedup: {comparison['speedup']:.3f}x "
        f"({comparison['repetitions']} samples)"
    )
    print(
        f"  peak concurrency: {parallel['peak_concurrency']}/"
        f"{parallel['configured_max_concurrency']}"
    )
    print(f"  capacity violations: {parallel['capacity_violations']}")
    print(
        "  correctness: "
        + ("PASS" if report["valid"] else f"FAIL ({'; '.join(report['violations'])})")
    )
    print(f"  scope: {scope['measures']}")
    print(f"  excludes: {scope['does_not_measure']}")
    return 0 if report["valid"] else 1


def _benchmark_online_compute(
    as_json: bool,
    *,
    provider_id: str = "simulated-default",
    latency_multiplier: float = 1.0,
    input_token_multiplier: float = 1.0,
    output_token_multiplier: float = 1.0,
    output_cost_per_token_usd: float = 0.00001,
) -> int:
    """Run the deterministic static-vs-adaptive online-compute benchmark."""

    from lhos.benchmarks.adaptive_control import SimulatedProvider, run_benchmark

    try:
        provider = SimulatedProvider(
            provider_id=provider_id,
            latency_multiplier=latency_multiplier,
            input_token_multiplier=input_token_multiplier,
            output_token_multiplier=output_token_multiplier,
            output_cost_per_token_usd=output_cost_per_token_usd,
        )
        report = run_benchmark(provider=provider)
    except Exception as exc:
        print(f"benchmark error: {exc}", file=sys.stderr)
        return 2

    valid = bool(report.get("valid", False))
    if as_json:
        # Keep JSON machine-readable and stable; ``run_benchmark`` already
        # returns only Pydantic/native JSON-compatible values.
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if valid else 1

    comparison = report.get("comparison", {})
    static = report.get("static", {})
    adaptive = report.get("adaptive", {})
    static_metrics = static.get("metrics", {}) if isinstance(static, dict) else {}
    adaptive_metrics = adaptive.get("metrics", {}) if isinstance(adaptive, dict) else {}
    print("LONGHORIZONOS ONLINE-COMPUTE CONTROL BENCHMARK")
    print(
        f"  scope: deterministic offline simulator (provider={provider_id}; not LLM/GPU throughput)"
    )
    print(
        f"  static:   tokens={static_metrics.get('total_tokens', 0)}  "
        f"time={static_metrics.get('wall_time_seconds', 0)}s  "
        f"stale={static_metrics.get('stale_work_tokens', 0)}"
    )
    print(
        f"  adaptive: tokens={adaptive_metrics.get('total_tokens', 0)}  "
        f"time={adaptive_metrics.get('wall_time_seconds', 0)}s  "
        f"stale={adaptive_metrics.get('stale_work_tokens', 0)}"
    )
    print(
        f"  token reduction: {comparison.get('token_reduction', 0)} "
        f"({float(comparison.get('token_reduction_ratio', 0.0)):.3%})"
    )
    print(
        f"  wall-time reduction: {comparison.get('wall_time_reduction_seconds', 0)}s "
        f"({float(comparison.get('wall_time_reduction_ratio', 0.0)):.3%})"
    )
    print(f"  correctness: {'PASS' if valid else 'FAIL'}")
    print(
        "  caveat: simulated costs/durations only; no automatic provenance, "
        "physical resources, or production performance claim"
    )
    return 0 if valid else 1


def _benchmark_compute_budget(as_json: bool) -> int:
    """Run the controlled declared-estimate compute-budget benchmark."""

    from lhos.benchmarks.compute_budget import run_benchmark

    try:
        report = run_benchmark()
    except Exception as exc:
        print(f"benchmark error: {exc}", file=sys.stderr)
        return 2

    valid = bool(report.get("valid", False))
    if as_json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if valid else 1

    static = report["static_lexical"]
    budget = report["verified_progress_budget"]
    comparison = report["comparison"]
    print("LONGHORIZONOS CONTROLLED COMPUTE-BUDGET BENCHMARK")
    print(
        "  scope: deterministic declared estimates; "
        "not real LLM performance, wall-clock speed, or cost savings"
    )
    print(
        f"  static lexical: selected={static['selected_task_ids']} "
        f"expected_progress={static['expected_verified_progress_units']}"
    )
    print(
        f"  verified-progress policy: selected={budget['selected_task_ids']} "
        f"expected_progress={budget['expected_verified_progress_units']}"
    )
    print(
        "  expected verified-progress gain: "
        f"{comparison['expected_verified_progress_gain_units']} declared units"
    )
    print(
        "  safety gates: "
        f"repair={'PASS' if all(report['repair_priority'].values()) else 'FAIL'} "
        f"unknown-estimate={'PASS' if report['unknown_estimate_case']['passed'] else 'FAIL'} "
        "all-budget-dimensions="
        f"{'PASS' if all(case['passed'] for case in report['hard_budget_dimension_cases'].values()) else 'FAIL'}"
    )
    print(f"  correctness: {'PASS' if valid else 'FAIL'}")
    return 0 if valid else 1


def _benchmark_unified_control(as_json: bool) -> int:
    """Run the unified budget/conflict/resource control benchmark."""

    from lhos.benchmarks.unified_control import run_benchmark

    try:
        report = run_benchmark()
    except Exception as exc:
        print(f"benchmark error: {exc}", file=sys.stderr)
        return 2

    valid = bool(report.get("valid", False))
    if as_json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if valid else 1

    static = report["static_fifo"]
    unified = report["unified"]
    comparison = report["comparison"]
    print("LONGHORIZONOS UNIFIED COMPUTE-CONTROL BENCHMARK")
    print(f"  valid: {'PASS' if valid else 'FAIL'}")
    print(f"  same VERIFIED Goal: {'YES' if comparison['same_verified_goal'] else 'NO'}")
    print(f"  epochs: static={static['epochs']} unified={unified['epochs']}")
    print(
        "  scheduler rejections: "
        f"static={static['scheduler_rejections']} "
        f"unified={unified['scheduler_rejections']}"
    )
    print(
        f"  stale attempts: static={static['stale_attempts']} unified={unified['stale_attempts']}"
    )
    print(
        f"  declared tokens: static={static['budget_usage']['tokens']} "
        f"unified={unified['budget_usage']['tokens']}"
    )
    print(
        "  scope: deterministic declared estimates and logical resources; "
        "not real provider billing, physical scheduling, or production performance"
    )
    return 0 if valid else 1


def _benchmark_resource_aware_runtime(as_json: bool) -> int:
    """Run the deterministic logical-resource adaptive-runtime benchmark."""

    from lhos.benchmarks.resource_aware_runtime import run_benchmark

    try:
        report = run_benchmark()
    except Exception as exc:
        print(f"benchmark error: {exc}", file=sys.stderr)
        return 2

    valid = bool(report.get("valid", False))
    if as_json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if valid else 1

    static = report["static"]
    resource_aware = report["resource_aware"]
    comparison = report["comparison"]
    print("LONGHORIZONOS RESOURCE-AWARE ADAPTIVE RUNTIME BENCHMARK")
    print(
        "  path: AgentOS.run_async -> Scheduler logical admission -> "
        "Claim/Kernel Lease -> verifier -> VPG Evidence"
    )
    print(
        f"  static/conflict-only: epochs={static['epochs']} "
        f"proposal_capacity_violations={static['proposal_capacity_violations']} "
        f"scheduler_resource_rejections={static['scheduler_resource_rejections']} "
        f"closed={static['closure']}"
    )
    print(
        f"  resource-aware:       epochs={resource_aware['epochs']} "
        "proposal_capacity_violations="
        f"{resource_aware['proposal_capacity_violations']} "
        "scheduler_resource_rejections="
        f"{resource_aware['scheduler_resource_rejections']} "
        f"closed={resource_aware['closure']}"
    )
    print(
        f"  reductions: epochs={comparison['epoch_reduction']} "
        "proposal_capacity_violations="
        f"{comparison['proposal_capacity_violation_reduction']} "
        "scheduler_resource_rejections="
        f"{comparison['scheduler_resource_rejection_reduction']}"
    )
    print(f"  correctness: {'PASS' if valid else 'FAIL'}")
    print(
        "  scope: deterministic synthetic logical-resource workload; "
        "no wall-clock, physical GPU/CPU, or real-LLM acceleration claim"
    )
    return 0 if valid else 1


def _benchmark_adaptive_wallclock_runtime(
    as_json: bool,
    *,
    delay_ms: float = 20.0,
) -> int:
    """Run the bounded real-sleep adaptive AgentOS wall-clock benchmark."""

    from lhos.benchmarks.adaptive_wallclock_runtime import run_benchmark

    try:
        report = run_benchmark(delay_seconds=delay_ms / 1000.0)
    except Exception as exc:
        print(f"benchmark error: {exc}", file=sys.stderr)
        return 2

    valid = bool(report.get("valid", False))
    if as_json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if valid else 1

    static = report["static"]
    adaptive = report["adaptive"]
    comparison = report["comparison"]
    print("LONGHORIZONOS REAL-WALL-CLOCK ADAPTIVE RUNTIME BENCHMARK")
    print(
        "  path: AgentOS.run_async -> Scheduler -> Claim/Kernel Lease -> "
        "async executor -> verifier -> VPG Evidence"
    )
    print(
        f"  static:   epochs={static['epochs']} "
        f"rejections={static['scheduler_resource_rejections']} "
        f"elapsed={static['elapsed_seconds']:.6f}s closed={static['closure']}"
    )
    print(
        f"  adaptive: epochs={adaptive['epochs']} "
        f"rejections={adaptive['scheduler_resource_rejections']} "
        f"elapsed={adaptive['elapsed_seconds']:.6f}s closed={adaptive['closure']}"
    )
    print(
        f"  reductions: epochs={comparison['epoch_reduction']} "
        "scheduler_resource_rejections="
        f"{comparison['scheduler_resource_rejection_reduction']}"
    )
    print(
        f"  observed speedup: {float(comparison['observed_speedup'] or 0.0):.3f}x "
        "(informational; not a pass/fail gate)"
    )
    print(f"  correctness: {'PASS' if valid else 'FAIL'}")
    print(
        "  scope: bounded deterministic real asyncio.sleep/perf_counter workload; "
        "not an LLM/GPU/production speed claim"
    )
    return 0 if valid else 1


def _benchmark_harness_adaptive(
    as_json: bool,
    *,
    delay_ms: float = 10.0,
    max_concurrency: int = 2,
    provider_factory_spec: str | None = None,
) -> int:
    """Run the real AgentOS + exact-identity Harness benchmark."""

    from lhos.benchmarks.harness_adaptive import (
        load_provider_factory,
        run_benchmark,
    )

    try:
        provider_factory = (
            load_provider_factory(provider_factory_spec) if provider_factory_spec else None
        )
        report = run_benchmark(
            delay_seconds=delay_ms / 1000.0,
            max_concurrency=max_concurrency,
            provider_factory=provider_factory,
            provider_factory_spec=provider_factory_spec,
        )
    except Exception as exc:
        print(f"benchmark error: {exc}", file=sys.stderr)
        return 2

    valid = bool(report.get("valid", False))
    if as_json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if valid else 1

    static = report["static"]
    adaptive = report["adaptive"]
    comparison = report["comparison"]
    print("LONGHORIZONOS HARNESS ADAPTIVE INTEGRATION BENCHMARK")
    print(
        "  path: Scheduler -> Claim/Attempt -> Kernel Lease -> Harness START "
        "-> verifier -> VPG Evidence"
    )
    for label, case in (("static", static), ("adaptive", adaptive)):
        usage = case["usage"]
        print(
            f"  {label}: attempts={case['executed_attempts']} "
            f"verified={case['verified_progress']:.3f} "
            f"tokens={usage['total_tokens']} "
            f"stale={case['stale_attempts']} "
            f"time={case['elapsed_ms']:.3f}ms"
        )
    print(
        f"  adaptive reductions: attempts={comparison['attempt_reduction']} "
        f"stale_tokens={comparison['stale_work_token_reduction']} "
        f"cost=${comparison['total_cost_reduction_usd']:.6f}"
    )
    print(f"  ownership path: {'PASS' if report['scope']['ownership_path'] else 'FAIL'}")
    print(f"  correctness: {'PASS' if valid else 'FAIL'}")
    print(
        "  scope: default provider usage is synthetic/local; wall time is "
        "orientation-only and no general model/GPU claim is made"
    )
    return 0 if valid else 1


def _vpg_store(db: str, *, read_only: bool = False):
    """Open a VPG GraphStore for the explicit lifecycle command surface."""

    from lhos.runtimes.verified_progress.graph_store import GraphStore

    if not os.path.exists(db):
        raise FileNotFoundError(db)
    return GraphStore(db, read_only=read_only)


def _vpg_history(db: str, graph_id: str, as_json: bool) -> int:
    from lhos.runtimes.verified_progress.errors import VPGError

    store = None
    try:
        store = _vpg_store(db, read_only=True)
        contract = store.get_history_retention_contract(graph_id)
        record = store.get_record(graph_id)
        assert record is not None
        lifecycle_events = store.conn.execute(
            "SELECT operation, previous_earliest_version, "
            "earliest_recoverable_version, checkpoint_version, "
            "checkpoint_projection_hash, actor, reason, recorded_at "
            "FROM graph_history_lifecycle_events "
            "WHERE graph_id = ? ORDER BY recorded_at, event_id",
            (graph_id,),
        ).fetchall()
        payload = {
            "graph_id": graph_id,
            "current_version": record.current_version,
            "earliest_recoverable_version": contract.earliest_recoverable_version,
            "checkpoint_projection_hash": contract.checkpoint_projection_hash,
            "updated_at": contract.updated_at.isoformat(),
            "updated_by": contract.updated_by,
            "reason": contract.reason,
            "lifecycle_events": [dict(row) for row in lifecycle_events],
        }
    except (VPGError, OSError, sqlite3.Error, AssertionError) as exc:
        return _vpg_error(exc, as_json)
    finally:
        if store is not None:
            store.close()

    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"VPG HISTORY {graph_id}")
        print(f"  current version: {payload['current_version']}")
        print(f"  earliest recoverable version: {payload['earliest_recoverable_version']}")
        print(f"  checkpoint hash: {payload['checkpoint_projection_hash']}")
        print(f"  updated by: {payload['updated_by']}")
        print(f"  reason: {payload['reason']}")
        print(f"  lifecycle events: {len(payload['lifecycle_events'])}")
    return 0


def _vpg_compact(
    db: str,
    graph_id: str,
    retain_from: int,
    actor: str,
    reason: str,
    confirmed: bool,
    as_json: bool,
) -> int:
    from lhos.runtimes.verified_progress.errors import VPGError

    if not confirmed:
        return _vpg_error(
            ValueError("history compaction is destructive; pass --yes to confirm"),
            as_json,
        )
    store = None
    try:
        store = _vpg_store(db)
        result = store.compact_projection_history(
            graph_id,
            retain_from_version=retain_from,
            compacted_by=actor,
            reason=reason,
        )
        payload = {
            "operation": "compact",
            "graph_id": result.graph_id,
            "previous_earliest_version": result.previous_earliest_version,
            "earliest_recoverable_version": result.earliest_recoverable_version,
            "current_version": result.current_version,
            "checkpoint_projection_hash": result.checkpoint_projection_hash,
            "deleted_snapshot_headers": result.deleted_snapshot_headers,
            "deleted_node_revisions": result.deleted_node_revisions,
            "deleted_edge_revisions": result.deleted_edge_revisions,
        }
    except (VPGError, OSError, sqlite3.Error, ValueError) as exc:
        return _vpg_error(exc, as_json)
    finally:
        if store is not None:
            store.close()
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"VPG history compacted: {graph_id}")
        print(f"  earliest recoverable version: {payload['earliest_recoverable_version']}")
        print(f"  deleted snapshot headers: {payload['deleted_snapshot_headers']}")
        print(f"  deleted node revisions: {payload['deleted_node_revisions']}")
        print(f"  deleted edge revisions: {payload['deleted_edge_revisions']}")
    return 0


def _vpg_migrate_legacy(
    db: str,
    graph_id: str,
    as_json: bool,
    *,
    trust_projection: bool,
    expected_current_version: int | None,
    expected_projection_hash: str | None,
    actor: str | None,
    reason: str | None,
) -> int:
    from lhos.runtimes.verified_progress.errors import VPGError

    store = None
    try:
        # Always preview through a read-only store first. Even a malformed
        # trust invocation must not trigger schema/backfill writes.
        store = _vpg_store(db, read_only=True)
        preview = store.preview_trusted_projection_migration(graph_id)
        payload: dict[str, object] = {
            "operation": "trusted_projection_migration",
            "graph_id": preview.graph_id,
            "current_version": preview.current_version,
            "projection_hash": preview.projection_hash,
            "node_count": preview.node_count,
            "edge_count": preview.edge_count,
            "write_required": True,
            "migrated": False,
        }
        if not trust_projection:
            if as_json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                print(f"VPG legacy migration preview: {graph_id}")
                print(f"  current version: {preview.current_version}")
                print(f"  projection hash: {preview.projection_hash}")
                print(f"  nodes: {preview.node_count}  edges: {preview.edge_count}")
                print(
                    "  no changes written; repeat with --trust-projection, "
                    "--expected-current-version, --expected-projection-hash, "
                    "--actor, and --reason"
                )
            return 0

        missing = [
            name
            for name, value in (
                ("--expected-current-version", expected_current_version),
                ("--expected-projection-hash", expected_projection_hash),
                ("--actor", actor),
                ("--reason", reason),
            )
            if value is None or (isinstance(value, str) and not value.strip())
        ]
        if missing:
            raise ValueError("trusted projection migration requires " + ", ".join(missing))
        # The preview above was read-only. Reopen the database only after all
        # explicit trust-boundary arguments have been supplied.
        assert expected_current_version is not None
        assert expected_projection_hash is not None
        assert actor is not None
        assert reason is not None
        store.close()
        store = _vpg_store(db)
        result = store.migrate_snapshotless_legacy_projection(
            graph_id,
            expected_current_version=expected_current_version,
            expected_projection_hash=expected_projection_hash,
            trusted=True,
            trusted_by=actor,
            reason=reason,
        )
        payload.update(
            {
                "current_version": result.current_version,
                "projection_hash": result.projection_hash,
                "migrated": True,
                "write_required": False,
                "actor": actor,
                "reason": reason,
            }
        )
    except (VPGError, OSError, sqlite3.Error, ValueError) as exc:
        return _vpg_error(exc, as_json)
    finally:
        if store is not None:
            store.close()

    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"VPG legacy projection migrated: {graph_id}")
        print(f"  baseline version: {payload['current_version']}")
        print(f"  projection hash: {payload['projection_hash']}")
        print(f"  trusted by: {payload.get('actor', '-')}")
    return 0


def _vpg_error(exc: BaseException, as_json: bool) -> int:
    """Render lifecycle errors without exposing a traceback to CLI users."""

    message = str(exc)
    code = getattr(getattr(exc, "code", None), "value", "CLI_ERROR")
    if as_json:
        print(
            json.dumps(
                {"error": message, "code": code},
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(f"error: {message}", file=sys.stderr)
    return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lhos", description="LongHorizonOS Core V1 CLI (read-only observability)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # shared observability options via a parent
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument(
        "--state",
        default=os.environ.get("LHOS_STATE", "run.json"),
        help="run manifest path (AgentOS.save_run output)",
    )
    parent.add_argument("--goal", default="G", help="goal id")
    parent.add_argument("--json", action="store_true", help="machine-readable JSON")

    sub.add_parser("status", parents=[parent], help="show goal/task semantic status")
    p_inspect = sub.add_parser("inspect", parents=[parent], help="inspect a task or evidence")
    p_inspect.add_argument("kind", choices=["task", "evidence"], help="object kind")
    p_inspect.add_argument("obj", help="task id or evidence id")
    sub.add_parser("graph", parents=[parent], help="render the verified progress graph")

    p_demo = sub.add_parser("demo", help="run a self-contained demonstration")
    p_demo.add_argument(
        "which",
        choices=["recovery-repair", "provenance-repair", "online-supervisor"],
        nargs="?",
        default="recovery-repair",
    )
    p_demo.add_argument("--json", action="store_true", help="machine-readable JSON summary")
    p_demo.add_argument("--paced", action="store_true", help="add presentation delay (GIF/CI off)")
    p_bench = sub.add_parser("benchmark", parents=[parent], help="run a comparative benchmark")
    p_bench.add_argument(
        "which",
        choices=[
            "semantic-repair",
            "hidden-provenance",
            "async-agentos",
            "async-workers",
            "online-compute",
            "compute-budget",
            "unified-control",
            "resource-aware-runtime",
            "wallclock-adaptive-runtime",
            "harness-adaptive",
        ],
        nargs="?",
        default="semantic-repair",
        metavar=(
            "{semantic-repair,hidden-provenance,async-agentos,online-compute,compute-budget,"
            "unified-control,resource-aware-runtime,wallclock-adaptive-runtime,"
            "harness-adaptive}"
        ),
        help="benchmark family",
    )
    benchmark_mode = p_bench.add_mutually_exclusive_group()
    benchmark_mode.add_argument(
        "--quick",
        dest="quick",
        action="store_true",
        default=True,
        help="quick offline deterministic run (default)",
    )
    benchmark_mode.add_argument(
        "--full",
        dest="quick",
        action="store_false",
        help="full size/fraction sweep",
    )
    p_bench.add_argument(
        "--live-model",
        action="store_true",
        help="run the opt-in StepCode live-model probe using STEPCODE_API_KEY or STEPCODE_API_KEYS",
    )
    p_bench.add_argument(
        "--model",
        default=os.environ.get("STEPCODE_MODEL", "gpt-5.6-sol"),
        help="StepCode OpenAI-compatible model for --live-model",
    )
    p_bench.add_argument(
        "--live-timeout",
        type=float,
        default=60.0,
        help="per-request timeout in seconds for --live-model",
    )
    p_bench.add_argument("--tasks", type=int, default=24, help="async-agentos task count")
    p_bench.add_argument(
        "--delay-ms",
        type=float,
        default=50.0,
        help="controlled I/O delay per async AgentOS task",
    )
    p_bench.add_argument(
        "--max-concurrency",
        type=int,
        default=4,
        help="global AgentOS async execution limit",
    )
    p_bench.add_argument(
        "--agent-concurrency",
        type=int,
        default=2,
        help="per-agent async execution limit",
    )
    p_bench.add_argument("--agent-count", type=int, default=2)
    p_bench.add_argument(
        "--min-speedup",
        type=float,
        default=1.5,
        help="minimum median paired async AgentOS speedup required for a passing result",
    )
    p_bench.add_argument(
        "--repetitions",
        type=int,
        default=3,
        help="paired serial/parallel measurements for async-agentos",
    )
    p_bench.add_argument(
        "--provider-id",
        default="simulated-default",
        help=(
            "offline online-compute provider profile label; used only by "
            "the deterministic simulator"
        ),
    )
    p_bench.add_argument(
        "--latency-multiplier",
        type=float,
        default=1.0,
        help="simulated provider latency multiplier for online-compute",
    )
    p_bench.add_argument(
        "--input-token-multiplier",
        type=float,
        default=1.0,
        help="simulated provider context/input-token multiplier",
    )
    p_bench.add_argument(
        "--output-token-multiplier",
        type=float,
        default=1.0,
        help="simulated provider output-token multiplier",
    )
    p_bench.add_argument(
        "--output-cost-per-token-usd",
        type=float,
        default=0.00001,
        help="simulated output-token price in USD",
    )
    p_bench.add_argument(
        "--provider-factory",
        default=None,
        help=(
            "optional explicit provider factory module:callable for "
            "harness-adaptive; default is deterministic local"
        ),
    )

    p_vpg = sub.add_parser("vpg", help="explicit VPG history lifecycle tools")
    vpg_sub = p_vpg.add_subparsers(dest="vpg_command", required=True)

    p_history = vpg_sub.add_parser(
        "history",
        help="show the graph's explicit history-retention contract",
    )
    p_history.add_argument("--db", required=True, help="VPG SQLite database path")
    p_history.add_argument("--graph", required=True, dest="graph_id", help="graph id")
    p_history.add_argument("--json", action="store_true", help="machine-readable JSON")

    p_compact = vpg_sub.add_parser(
        "compact",
        help="prune projection history below a verified checkpoint",
    )
    p_compact.add_argument("--db", required=True, help="VPG SQLite database path")
    p_compact.add_argument("--graph", required=True, dest="graph_id", help="graph id")
    p_compact.add_argument(
        "--retain-from",
        required=True,
        type=int,
        help="earliest version to retain and checkpoint",
    )
    p_compact.add_argument("--actor", required=True, help="operator identity")
    p_compact.add_argument("--reason", required=True, help="retention reason")
    p_compact.add_argument(
        "--yes",
        action="store_true",
        help="confirm destructive history pruning",
    )
    p_compact.add_argument("--json", action="store_true", help="machine-readable JSON")

    p_migrate = vpg_sub.add_parser(
        "migrate-legacy",
        aliases=["migrate", "trusted-migration", "migration", "migrate-snapshotless"],
        help="preview or explicitly trust a snapshot-less legacy projection",
    )
    p_migrate.add_argument("--db", required=True, help="VPG SQLite database path")
    p_migrate.add_argument("--graph", required=True, dest="graph_id", help="graph id")
    p_migrate.add_argument("--json", action="store_true", help="machine-readable JSON")
    p_migrate.add_argument(
        "--trust-projection",
        action="store_true",
        help="explicitly bless the previewed materialized projection",
    )
    p_migrate.add_argument(
        "--expected-current-version",
        "--expected-version",
        dest="expected_current_version",
        type=int,
        help="version returned by the immediately preceding preview",
    )
    p_migrate.add_argument(
        "--expected-projection-hash",
        "--expected-hash",
        dest="expected_projection_hash",
        help="hash returned by the immediately preceding preview",
    )
    p_migrate.add_argument("--actor", help="operator identity")
    p_migrate.add_argument("--reason", help="trust decision reason")

    sub.add_parser("legacy", help="LEGACY spec-20 CLI (out of Core V1 scope)")
    return parser


def main(argv: list[str] | None = None) -> int:
    # A narrow console encoding (e.g. cp936) must not turn a rendered glyph into
    # a crash; substitute instead of raising.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    if raw_argv and raw_argv[0] == "legacy":
        from lhos.cli.main import main as legacy_main

        return legacy_main(raw_argv[1:] or ["--help"])

    parser = build_parser()
    args = parser.parse_args(raw_argv)
    if args.command == "demo":
        if args.which == "provenance-repair":
            return _demo_provenance_repair(args.json, args.paced)
        if args.which == "online-supervisor":
            return _demo_online_supervisor(args.json)
        return _demo_recovery_repair(args.json, args.paced)

    if args.command == "benchmark":
        if args.which == "hidden-provenance":
            return _benchmark_hidden_provenance(args.json)
        if args.which == "online-compute":
            return _benchmark_online_compute(
                args.json,
                provider_id=args.provider_id,
                latency_multiplier=args.latency_multiplier,
                input_token_multiplier=args.input_token_multiplier,
                output_token_multiplier=args.output_token_multiplier,
                output_cost_per_token_usd=args.output_cost_per_token_usd,
            )
        if args.which == "compute-budget":
            return _benchmark_compute_budget(args.json)
        if args.which == "unified-control":
            return _benchmark_unified_control(args.json)
        if args.which == "resource-aware-runtime":
            return _benchmark_resource_aware_runtime(args.json)
        if args.which == "wallclock-adaptive-runtime":
            return _benchmark_adaptive_wallclock_runtime(
                args.json,
                delay_ms=args.delay_ms,
            )
        if args.which == "harness-adaptive":
            return _benchmark_harness_adaptive(
                args.json,
                delay_ms=args.delay_ms,
                max_concurrency=args.max_concurrency,
                provider_factory_spec=args.provider_factory,
            )
        if args.which in {"async-agentos", "async-workers"}:
            return _benchmark_async_agentos(
                args.json,
                task_count=args.tasks,
                delay_ms=args.delay_ms,
                max_concurrency=args.max_concurrency,
                agent_concurrency=args.agent_concurrency,
                agent_count=args.agent_count,
                min_speedup=args.min_speedup,
                repetitions=args.repetitions,
            )
        return _benchmark(
            args.quick,
            args.json,
            live_model=args.live_model,
            model=args.model,
            live_timeout_seconds=args.live_timeout,
        )

    if args.command == "vpg":
        if args.vpg_command == "history":
            return _vpg_history(args.db, args.graph_id, args.json)
        if args.vpg_command == "compact":
            return _vpg_compact(
                args.db,
                args.graph_id,
                args.retain_from,
                args.actor,
                args.reason,
                args.yes,
                args.json,
            )
        if args.vpg_command in {
            "migrate-legacy",
            "migrate",
            "trusted-migration",
            "migration",
            "migrate-snapshotless",
        }:
            return _vpg_migrate_legacy(
                args.db,
                args.graph_id,
                args.json,
                trust_projection=args.trust_projection,
                expected_current_version=args.expected_current_version,
                expected_projection_hash=args.expected_projection_hash,
                actor=args.actor,
                reason=args.reason,
            )
        parser.error(f"unknown vpg command {args.vpg_command!r}")

    if not hasattr(args, "state"):
        parser.error("--state is required")
    try:
        os_ = AgentOS.open_run(args.state)
    except FileNotFoundError:
        print(
            f"error: state manifest {args.state!r} not found; create it with the SDK "
            "`AgentOS(...).save_run(...)` flow",
            file=sys.stderr,
        )
        return 3
    if args.command == "status":
        return _status(os_, args.goal, args.json)
    if args.command == "inspect":
        return _inspect(os_, args.goal, args.kind, args.obj, args.json)
    if args.command == "graph":
        return _graph(os_, args.goal, args.json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
