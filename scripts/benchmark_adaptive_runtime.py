"""CLI gate for the controlled conflict-aware adaptive-runtime benchmark."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from lhos.benchmarks.adaptive_runtime import (
    DEFAULT_DELAY_SECONDS,
    DEFAULT_MAX_CONCURRENCY,
    run_benchmark,
)

DEFAULT_OUTPUT = REPO_ROOT / "artifacts" / "benchmark_results" / "adaptive-runtime.json"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare fixed-concurrency and conflict-aware adaptive execution "
            "on a deterministic offline workload."
        )
    )
    parser.add_argument(
        "--delay-ms",
        type=float,
        default=DEFAULT_DELAY_SECONDS * 1000.0,
        help="Synthetic per-attempt delay in milliseconds.",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=DEFAULT_MAX_CONCURRENCY,
        help="Global scheduler concurrency bound (must be >= 2).",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero when benchmark correctness/safety gates fail.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        report = run_benchmark(
            delay_seconds=args.delay_ms / 1000.0,
            max_concurrency=args.max_concurrency,
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
