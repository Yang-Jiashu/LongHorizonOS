"""LongHorizonOS Core V1 — native CLI (read-only observability, E3).

Commands: status, inspect, graph.  All are READ-ONLY projections over the
public SDK; they never mutate semantic state (OBS-G1..G12).  `--state` points to
a run manifest (JSON) saved by `lhos.sdk.AgentOS.save_run`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
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
    p_bench.add_argument("which", choices=["semantic-repair"], nargs="?", default="semantic-repair")
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
        return _benchmark(
            args.quick,
            args.json,
            live_model=args.live_model,
            model=args.model,
            live_timeout_seconds=args.live_timeout,
        )

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
