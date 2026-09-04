"""Aggregate a small paired SWE-bench Lite pilot."""

# ruff: noqa: RUF001

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _usage(arm: dict[str, Any]) -> dict[str, int]:
    usage = arm.get("dsh", {}).get("usage", {})
    return {
        "token_units": int(usage.get("total_token_units", 0) or 0),
        "model_calls": int(usage.get("model_calls", 0) or 0),
        "tool_calls": int(usage.get("tool_calls", 0) or 0),
    }


def _case(path: Path) -> dict[str, Any]:
    result = _load(path)
    static = result["static"]
    lhos = result["lhos"]
    static_usage = _usage(static)
    lhos_usage = _usage(lhos)
    static_patch = str(static.get("evaluation", {}).get("agent_patch", ""))
    lhos_patch = str(lhos.get("evaluation", {}).get("agent_patch", ""))
    return {
        "instance_id": str(result["instance_id"]),
        "result": str(path),
        "official_evaluator": bool(result.get("official_evaluator", False)),
        "static_resolved": bool(static.get("valid", False)),
        "lhos_resolved": bool(lhos.get("valid", False)),
        "pair_valid": bool(result.get("comparison", {}).get("pair_valid", False)),
        "same_patch": static_patch == lhos_patch,
        "static": {
            **static_usage,
            "elapsed_ms": float(static.get("elapsed_ms", 0.0) or 0.0),
        },
        "lhos": {
            **lhos_usage,
            "elapsed_ms": float(lhos.get("elapsed_ms", 0.0) or 0.0),
            "goal_state": str(lhos.get("run_result", {}).get("goal_state", "")),
        },
    }


def build_summary(
    result_paths: list[Path],
    *,
    model: str,
    reasoning: str,
) -> dict[str, Any]:
    cases = [_case(path.resolve()) for path in result_paths]
    paired = [case for case in cases if case["pair_valid"]]
    aggregate: dict[str, Any] = {
        "paired_cases": len(paired),
        "static": {
            "token_units": sum(case["static"]["token_units"] for case in paired),
            "model_calls": sum(case["static"]["model_calls"] for case in paired),
            "tool_calls": sum(case["static"]["tool_calls"] for case in paired),
            "elapsed_ms": round(sum(case["static"]["elapsed_ms"] for case in paired), 3),
        },
        "lhos": {
            "token_units": sum(case["lhos"]["token_units"] for case in paired),
            "model_calls": sum(case["lhos"]["model_calls"] for case in paired),
            "tool_calls": sum(case["lhos"]["tool_calls"] for case in paired),
            "elapsed_ms": round(sum(case["lhos"]["elapsed_ms"] for case in paired), 3),
        },
    }
    static_tokens = aggregate["static"]["token_units"]
    lhos_tokens = aggregate["lhos"]["token_units"]
    static_wall = aggregate["static"]["elapsed_ms"]
    lhos_wall = aggregate["lhos"]["elapsed_ms"]
    aggregate["observed"] = {
        "token_ratio_lhos_over_static": (
            None if static_tokens == 0 else round(lhos_tokens / static_tokens, 6)
        ),
        "wall_ratio_lhos_over_static": (
            None if static_wall == 0 else round(lhos_wall / static_wall, 6)
        ),
        "token_change_lhos_minus_static": lhos_tokens - static_tokens,
        "elapsed_change_ms_lhos_minus_static": round(lhos_wall - static_wall, 3),
    }
    return {
        "schema_version": "lhos-swe-lite-pilot-summary.v1",
        "benchmark": "SWE-bench Lite text-only host-native paired pilot",
        "official_score": False,
        "model": model,
        "reasoning_effort": reasoning,
        "total_cases": len(cases),
        "static_resolved": sum(int(case["static_resolved"]) for case in cases),
        "lhos_resolved": sum(int(case["lhos_resolved"]) for case in cases),
        "pair_valid_cases": len(paired),
        "cases": cases,
        "aggregate_on_pair_valid_cases": aggregate,
        "attribution": {
            "os_causal_saving": False,
            "reason": (
                "Each case contains one READY task. These paired runs measure correctness, "
                "plumbing and stochastic trajectory variance, not selective scheduling."
            ),
        },
    }


def _change(value: float) -> str:
    return f"{value:+,.0f}"


def _pct_change(lhos: float, static: float) -> str:
    return "n/a" if static == 0 else f"{100.0 * (lhos - static) / static:+.1f}%"


def render_markdown(summary: dict[str, Any]) -> str:
    aggregate = summary["aggregate_on_pair_valid_cases"]
    static = aggregate["static"]
    lhos = aggregate["lhos"]
    lines = [
        "# SWE-bench Lite 纯文本小规模 Pilot",
        "",
        "## 配置",
        "",
        "```text",
        f"model: {summary['model']}",
        f"reasoning_effort: {summary['reasoning_effort']}",
        "comparison: DSH static vs DSH + LongHorizonOS",
        "evaluator: host-native target tests",
        "```",
        "",
        "## Correctness",
        "",
        f"- Cases：{summary['total_cases']}",
        f"- DSH static resolved：{summary['static_resolved']}/{summary['total_cases']}",
        f"- DSH + LHOS resolved：{summary['lhos_resolved']}/{summary['total_cases']}",
        f"- Pair-valid：{summary['pair_valid_cases']}/{summary['total_cases']}",
        "",
        "| Case | Static | LHOS | Static token | LHOS token | Static wall | LHOS wall | Patch |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for case in summary["cases"]:
        lines.append(
            f"| `{case['instance_id']}` | "
            f"{'resolved' if case['static_resolved'] else 'unresolved'} | "
            f"{'resolved' if case['lhos_resolved'] else 'unresolved'} | "
            f"{case['static']['token_units']:,} | {case['lhos']['token_units']:,} | "
            f"{case['static']['elapsed_ms'] / 1000:.3f}s | "
            f"{case['lhos']['elapsed_ms'] / 1000:.3f}s | "
            f"{'same' if case['same_patch'] else 'different'} |"
        )
    lines.extend(
        [
            "",
            "## Pair-valid cases 的观测总量",
            "",
            "| 指标 | Static | LHOS | LHOS-Static |",
            "|---|---:|---:|---:|",
            (
                f"| Token units | {static['token_units']:,} | {lhos['token_units']:,} | "
                f"{_change(lhos['token_units'] - static['token_units'])} "
                f"({_pct_change(lhos['token_units'], static['token_units'])}) |"
            ),
            (
                f"| Model calls | {static['model_calls']} | {lhos['model_calls']} | "
                f"{_change(lhos['model_calls'] - static['model_calls'])} |"
            ),
            (
                f"| Tool calls | {static['tool_calls']} | {lhos['tool_calls']} | "
                f"{_change(lhos['tool_calls'] - static['tool_calls'])} |"
            ),
            (
                f"| Sum wall | {static['elapsed_ms'] / 1000:.3f}s | "
                f"{lhos['elapsed_ms'] / 1000:.3f}s | "
                f"{(lhos['elapsed_ms'] - static['elapsed_ms']) / 1000:+.3f}s "
                f"({_pct_change(lhos['elapsed_ms'], static['elapsed_ms'])}) |"
            ),
            "",
            "## 解释边界",
            "",
            "这不是官方 SWE-bench Lite leaderboard 分数：当前使用 host-native verifier，",
            "官方固定 Docker image 仍受 registry 拉取问题阻塞。",
            "",
            "每个 case 都只有一个 READY Task，没有 preserved branch、semantic invalidation",
            "或 selective repair。因此 token/time 正负差都只能视为随机 Agent 轨迹观测，",
            "不能归因于 LongHorizonOS。这里能报告的是 resolved correctness、执行链路和",
            "方差规模；OS 加速必须在 multi-task dynamic episode 中测。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("results", type=Path, nargs="+")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--reasoning", required=True)
    args = parser.parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = build_summary(
        args.results,
        model=args.model,
        reasoning=args.reasoning,
    )
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (output_dir / "SUMMARY.zh-CN.md").write_text(
        render_markdown(summary),
        encoding="utf-8",
    )
    print(output_dir / "summary.json")
    print(output_dir / "SUMMARY.zh-CN.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
