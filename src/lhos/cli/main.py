"""lhos command line interface (spec section 20)."""

from __future__ import annotations

import argparse
import sys

from lhos.cli.benchmark import cmd_benchmark
from lhos.cli.inject import cmd_inject
from lhos.cli.inspect import cmd_graph, cmd_inspect
from lhos.cli.replay import cmd_replay
from lhos.cli.resume import cmd_resume
from lhos.cli.run import cmd_init, cmd_run


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lhos", description="LongHorizonOS runtime CLI"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="initialize a database")
    p_init.add_argument("--db", required=True)
    p_init.set_defaults(func=cmd_init)

    p_run = sub.add_parser("run", help="run a task graph")
    p_run.add_argument("--db", required=True)
    p_run.add_argument("--graph-file", required=True, help="JSON task graph (spec 8.1)")
    p_run.add_argument("--goal", default=None, help="override goal text")
    p_run.add_argument("--workspace", required=True)
    p_run.add_argument("--config", default=None)
    p_run.add_argument("--scheduler", default=None, choices=["fifo", "cost_aware"])
    p_run.add_argument("--run-id", default=None)
    p_run.set_defaults(func=cmd_run)

    p_resume = sub.add_parser("resume", help="resume an interrupted run")
    p_resume.add_argument("--db", required=True)
    p_resume.add_argument("--run-id", required=True)
    p_resume.add_argument("--workspace", default=None)
    p_resume.add_argument("--config", default=None)
    p_resume.set_defaults(func=cmd_resume)

    p_inspect = sub.add_parser("inspect", help="inspect a run")
    p_inspect.add_argument("--db", required=True)
    p_inspect.add_argument("--run-id", required=True)
    p_inspect.set_defaults(func=cmd_inspect)

    p_graph = sub.add_parser("graph", help="dump the materialized graph")
    p_graph.add_argument("--db", required=True)
    p_graph.add_argument("--run-id", required=True)
    p_graph.add_argument("--format", default="json", choices=["json"])
    p_graph.set_defaults(func=cmd_graph)

    p_replay = sub.add_parser("replay", help="rebuild the graph from the event log")
    p_replay.add_argument("--db", required=True)
    p_replay.add_argument("--run-id", required=True)
    p_replay.set_defaults(func=cmd_replay)

    p_inject = sub.add_parser("inject", help="inject an external environment event")
    p_inject.add_argument("--db", required=True)
    p_inject.add_argument("--run-id", required=True)
    p_inject.add_argument("--type", required=True, help="event type, e.g. artifact_updated")
    p_inject.add_argument("--payload", default="{}", help="JSON payload")
    p_inject.set_defaults(func=cmd_inject)

    p_bench = sub.add_parser("benchmark", help="run the controlled benchmark suite (spec 22-25)")
    p_bench.add_argument("--suite", default="controlled", choices=["controlled"])
    p_bench.add_argument("--mode", "--modes", dest="mode", default=None,
                         help="comma list of experiment modes (spec 25)")
    p_bench.add_argument("--scheduler", default=None,
                         help="comma list: fifo,cost_aware — selects modes by scheduler family")
    p_bench.add_argument("--seeds", default="1", help="comma list of seeds, e.g. 1,2,3")
    p_bench.add_argument("--size", default="small", choices=["small", "medium", "large", "xl"])
    p_bench.add_argument("--tasks", default=None, help="comma list of scenario presets (default: all 14)")
    p_bench.add_argument("--out", default="artifacts/benchmark_results")
    p_bench.add_argument("--work-root", dest="work_root", default="artifacts/benchmark_work")
    p_bench.set_defaults(func=cmd_benchmark)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args) or 0)
    except Exception as exc:  # CLI boundary: report, don't traceback by default
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
