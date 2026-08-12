"""CLI gate for the public ``AgentOS.run_async`` benchmark."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from lhos.benchmarks.async_worker_runtime import (
    DEFAULT_AGENT_CONCURRENCY,
    DEFAULT_AGENT_COUNT,
    DEFAULT_DELAY_SECONDS,
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_MIN_SPEEDUP,
    DEFAULT_TASKS,
    run_benchmark,
)

DEFAULT_OUTPUT = REPO_ROOT / "artifacts" / "benchmark_results" / "multi-agent-runtime.json"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark the offline AgentOS.run_async path against the same "
            "public SDK workload at max_concurrency=1."
        ),
    )
    parser.add_argument("--tasks", type=int, default=DEFAULT_TASKS)
    parser.add_argument("--delay-ms", type=float, default=DEFAULT_DELAY_SECONDS * 1000)
    parser.add_argument("--max-concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY)
    parser.add_argument("--agent-concurrency", type=int, default=DEFAULT_AGENT_CONCURRENCY)
    parser.add_argument("--agent-count", type=int, default=DEFAULT_AGENT_COUNT)
    parser.add_argument("--min-speedup", type=float, default=DEFAULT_MIN_SPEEDUP)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero when correctness, capacity, or speedup gates fail.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        report = run_benchmark(
            task_count=args.tasks,
            delay_seconds=args.delay_ms / 1000,
            max_concurrency=args.max_concurrency,
            agent_concurrency=args.agent_concurrency,
            agent_count=args.agent_count,
            min_speedup=args.min_speedup,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered + "\n", encoding="utf-8")
    return 1 if args.check and report["violations"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
