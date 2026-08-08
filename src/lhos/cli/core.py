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
    for key in ("API_KEY", "SECRET", "TOKEN", "PASSWORD", "AUTH"):
        s = re.sub(rf"(?i)({key}=)[^\s;,&]+", r"\1***", s)
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
    sub.add_parser("legacy", help="LEGACY spec-20 CLI (out of Core V1 scope)")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "legacy":
        from lhos.cli.main import main as legacy_main

        return legacy_main(argv)

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
