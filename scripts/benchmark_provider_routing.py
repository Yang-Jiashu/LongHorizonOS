"""Run the deterministic offline provider-routing benchmark."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from lhos.benchmarks.provider_routing import (
    DEFAULT_DELAY_SECONDS,
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_TASK_COUNT,
    run_provider_routing_benchmark,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare callback execution with opt-in bounded provider routing."
    )
    parser.add_argument("--task-count", type=int, default=DEFAULT_TASK_COUNT)
    parser.add_argument(
        "--delay-ms",
        type=float,
        default=DEFAULT_DELAY_SECONDS * 1000.0,
        help="Deterministic fake-provider delay per hook (milliseconds).",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=DEFAULT_MAX_CONCURRENCY,
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero when benchmark correctness gates fail.",
    )
    args = parser.parse_args()
    report = run_provider_routing_benchmark(
        task_count=args.task_count,
        delay_seconds=args.delay_ms / 1000.0,
        max_concurrency=args.max_concurrency,
    )
    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return 1 if args.check and report["violations"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
