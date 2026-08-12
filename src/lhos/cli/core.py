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


def _benchmark_async_agentos(
    as_json: bool,
    *,
    task_count: int = 24,
    delay_ms: float = 10.0,
    max_concurrency: int = 4,
    agent_concurrency: int = 2,
    agent_count: int = 2,
    min_speedup: float = 1.5,
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
        f"speedup: {comparison['speedup']:.3f}x"
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
    p_demo.add_argument("which", choices=["recovery-repair"], nargs="?", default="recovery-repair")
    p_demo.add_argument("--json", action="store_true", help="machine-readable JSON summary")
    p_demo.add_argument("--paced", action="store_true", help="add presentation delay (GIF/CI off)")
    p_bench = sub.add_parser("benchmark", parents=[parent], help="run a comparative benchmark")
    p_bench.add_argument(
        "which",
        choices=["semantic-repair", "async-agentos", "async-workers"],
        nargs="?",
        default="semantic-repair",
        metavar="{semantic-repair,async-agentos}",
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
        default=25.0,
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
        help="minimum async AgentOS speedup required for a passing result",
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
        return _demo_recovery_repair(args.json, args.paced)

    if args.command == "benchmark":
        if args.which in {"async-agentos", "async-workers"}:
            return _benchmark_async_agentos(
                args.json,
                task_count=args.tasks,
                delay_ms=args.delay_ms,
                max_concurrency=args.max_concurrency,
                agent_concurrency=args.agent_concurrency,
                agent_count=args.agent_count,
                min_speedup=args.min_speedup,
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
