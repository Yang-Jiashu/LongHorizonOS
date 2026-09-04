"""Compose independently captured static and LHOS arm summaries."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from lhos.benchmarks.dsh_dynamic_coding.experiment import _comparison


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def compose(
    static_path: Path,
    lhos_path: Path,
    output_path: Path,
    *,
    model: str,
    reasoning: str,
    provider_route: str,
    benchmark_contract_sha256: str,
) -> dict[str, Any]:
    static = _load(static_path)
    lhos = _load(lhos_path)
    comparison = _comparison(static, lhos)
    # The profiling script historically consumed regraded aliases. Keep the
    # aliases in the composed artifact so raw and regraded profiles share one
    # schema.
    static["final_grade_regraded"] = static["final_grade"]
    lhos["final_grade_regraded"] = lhos["final_grade"]
    pair = {
        "pair": 1,
        "order": ["dsh_static_restart", "dsh_lhos"],
        "static": static,
        "lhos": lhos,
        "comparison": comparison,
        "comparison_regraded": comparison,
    }
    result = {
        "benchmark": "real_dsh_dynamic_coding",
        "benchmark_version": 1,
        "benchmark_contract_sha256": benchmark_contract_sha256,
        "model": model,
        "reasoning_effort": reasoning,
        "provider_route": provider_route,
        "controller_difference_only": True,
        "same_task_prompts": True,
        "same_concurrency": 2,
        "same_retry_limit": 2,
        "credential_env": "STEPFUN_API_KEY",
        "repeat": 1,
        "pairs": [pair],
        "capture": {
            "mode": "recovered-pair-from-independent-arm-captures",
            "static_summary": str(static_path),
            "lhos_summary": str(lhos_path),
            "lhos_provider_path_neutralized": True,
        },
        "regrade": {
            "source": "independently captured static summary + neutral-path LHOS arm",
            "pair_valid": bool(comparison["pair_valid"]),
        },
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--static-summary", type=Path, required=True)
    parser.add_argument("--lhos-summary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model", default="step-3.7-flash")
    parser.add_argument("--reasoning", default="medium")
    parser.add_argument("--provider-route", default="stepfun/step_plan/openai-completions")
    parser.add_argument("--benchmark-contract-sha256", required=True)
    args = parser.parse_args()
    result = compose(
        args.static_summary.resolve(),
        args.lhos_summary.resolve(),
        args.output.resolve(),
        model=args.model,
        reasoning=args.reasoning,
        provider_route=args.provider_route,
        benchmark_contract_sha256=args.benchmark_contract_sha256,
    )
    print(
        json.dumps(
            {
                "output": str(args.output.resolve()),
                "pair_valid": result["pairs"][0]["comparison"]["pair_valid"],
            },
            ensure_ascii=True,
        )
    )
    return 0 if result["pairs"][0]["comparison"]["pair_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
