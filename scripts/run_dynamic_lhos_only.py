"""Run one LongHorizonOS arm for the real dynamic coding benchmark.

This is useful when the static arm has already completed and a provider-side
transient (for example a moderation response) invalidates only the LHOS arm.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from lhos.benchmarks.dsh_dynamic_coding.experiment import DshConfig, _run_lhos_arm_async


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--dsh", type=Path, required=True)
    parser.add_argument("--patch", type=Path, required=True)
    parser.add_argument("--credential-env", default="DEEPSEEK_API_KEY")
    parser.add_argument("--model", default="deepseek-v4-flash")
    parser.add_argument("--reasoning", default="low")
    parser.add_argument("--provider-route", default="sensenova/openai-completions")
    parser.add_argument("--timeout-seconds", type=float, default=600.0)
    parser.add_argument("--max-concurrency", type=int, default=2)
    parser.add_argument("--max-attempts", type=int, default=2)
    args = parser.parse_args()

    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=False)
    config = DshConfig(
        node=args.node.resolve(),
        dsh=args.dsh.resolve(),
        patch=args.patch.resolve(),
        timeout_seconds=float(args.timeout_seconds),
        max_concurrency=int(args.max_concurrency),
        max_attempts=int(args.max_attempts),
        credential_env=str(args.credential_env),
        model=str(args.model),
        reasoning=str(args.reasoning),
        provider_route=str(args.provider_route),
    )
    result = asyncio.run(_run_lhos_arm_async(output_root, config))
    (output_root / "arm-result.json").write_text(
        json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
