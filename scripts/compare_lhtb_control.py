"""Compare a raw Harbor LHTB baseline with LHOS-controlled Harbor trials."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _duration_ms(started: str | None, finished: str | None) -> float | None:
    if not started or not finished:
        return None
    start = datetime.fromisoformat(started.replace("Z", "+00:00"))
    end = datetime.fromisoformat(finished.replace("Z", "+00:00"))
    return round((end - start).total_seconds() * 1000, 3)


def _baseline(trial_result_path: Path, trajectory_path: Path) -> dict[str, Any]:
    result = _load(trial_result_path)
    trajectory = _load(trajectory_path)
    steps = trajectory.get("steps", [])
    agent_result = result.get("agent_result", {})
    verifier_result = result.get("verifier_result") or {}
    return {
        "arm": "harbor_baseline",
        "resolved": bool(verifier_result.get("reward", 0) >= 0.95),
        "reward": verifier_result.get("reward"),
        "exception_type": result.get("exception_info", {}).get("exception_type"),
        "agent_steps": sum(step.get("source") == "agent" for step in steps),
        "tool_calls": sum(
            len(step.get("tool_calls") or []) for step in steps if step.get("source") == "agent"
        ),
        "prompt_tokens": int(agent_result.get("n_input_tokens", 0) or 0),
        "cached_tokens": int(agent_result.get("n_cache_tokens", 0) or 0),
        "completion_tokens": int(agent_result.get("n_output_tokens", 0) or 0),
        "agent_elapsed_ms": _duration_ms(
            result.get("agent_execution", {}).get("started_at"),
            result.get("agent_execution", {}).get("finished_at"),
        ),
        "total_elapsed_ms": _duration_ms(result.get("started_at"), result.get("finished_at")),
        "goal_state": "n/a",
        "source": str(trial_result_path),
    }


def _lhos(path: Path, label: str) -> dict[str, Any]:
    result = _load(path)
    evaluation = result.get("evaluation", {})
    metrics = evaluation.get("trial_metrics", {})
    final_metrics = metrics.get("final_metrics", {})
    reward = evaluation.get("reward")
    return {
        "arm": label,
        "resolved": bool(
            result.get("goal_state") == "closed" and reward is not None and reward >= 0.95
        ),
        "reward": reward,
        "exception_type": metrics.get("exception_type"),
        "agent_steps": int(metrics.get("agent_steps", 0) or 0),
        "tool_calls": int(metrics.get("tool_calls", 0) or 0),
        "prompt_tokens": int(final_metrics.get("total_prompt_tokens", 0) or 0),
        "cached_tokens": int(final_metrics.get("total_cached_tokens", 0) or 0),
        "completion_tokens": int(final_metrics.get("total_completion_tokens", 0) or 0),
        "agent_elapsed_ms": _duration_ms(
            metrics.get("agent_execution", {}).get("started_at"),
            metrics.get("agent_execution", {}).get("finished_at"),
        ),
        "harbor_total_elapsed_ms": _duration_ms(
            metrics.get("started_at"),
            metrics.get("finished_at"),
        ),
        "total_elapsed_ms": float(result.get("elapsed_ms", 0.0) or 0.0),
        "goal_state": str(result.get("goal_state", "")),
        "source": str(path),
    }


def build(
    baseline_result: Path,
    baseline_trajectory: Path,
    lhos_result: Path,
    checkpoint_result: Path | None = None,
) -> dict[str, Any]:
    arms = [
        _baseline(baseline_result.resolve(), baseline_trajectory.resolve()),
        _lhos(lhos_result.resolve(), "lhos_fresh_control"),
    ]
    if checkpoint_result is not None:
        arms.append(_lhos(checkpoint_result.resolve(), "lhos_checkpoint_continuation"))
    return {
        "schema_version": "lhos-lhtb-control-comparison.v1",
        "benchmark": "LHTB langchain-version-migration local control",
        "official_score": False,
        "model": "step-3.7-flash",
        "agent_budget_seconds": 900,
        "arms": arms,
        "attribution": (
            "The fresh LHOS arm wraps one Harbor trial and cannot demonstrate selective "
            "repair. A checkpoint arm, when present, measures workspace-state reuse."
        ),
    }


def _seconds(value: float | None) -> str:
    return "n/a" if value is None else f"{value / 1000:.3f}s"


def render(data: dict[str, Any]) -> str:
    lines = [
        "# LHTB Baseline vs LongHorizonOS",
        "",
        "## Configuration",
        "",
        "```text",
        "task: langchain-version-migration",
        "model: step-3.7-flash",
        "agent budget: 900s",
        "environment: local Docker",
        "official_score: false",
        "```",
        "",
        "| Arm | Resolved | Goal | Agent steps | Tool calls | Prompt | Cached | Output | Agent time | Total time |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for arm in data["arms"]:
        lines.append(
            f"| `{arm['arm']}` | {'yes' if arm['resolved'] else 'no'} | "
            f"{arm['goal_state']} | {arm['agent_steps']} | {arm['tool_calls']} | "
            f"{arm['prompt_tokens']:,} | {arm['cached_tokens']:,} | "
            f"{arm['completion_tokens']:,} | {_seconds(arm['agent_elapsed_ms'])} | "
            f"{_seconds(arm['total_elapsed_ms'])} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "The fresh LHOS control uses the same Harbor task as the baseline. Because the",
            "task is represented as one READY node, LHOS cannot skip or repair any internal",
            "branch; this arm measures integration correctness and control-plane overhead only.",
            "",
            "A checkpoint continuation, when present, starts from the baseline's recovered",
            "workspace artifact. That is a state-reuse experiment, not an official LHTB run.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-result", type=Path, required=True)
    parser.add_argument("--baseline-trajectory", type=Path, required=True)
    parser.add_argument("--lhos-result", type=Path, required=True)
    parser.add_argument("--checkpoint-result", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    data = build(
        args.baseline_result,
        args.baseline_trajectory,
        args.lhos_result,
        args.checkpoint_result,
    )
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "comparison.json").write_text(
        json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (output_dir / "COMPARISON.zh-CN.md").write_text(render(data), encoding="utf-8")
    print(output_dir / "comparison.json")
    print(output_dir / "COMPARISON.zh-CN.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
