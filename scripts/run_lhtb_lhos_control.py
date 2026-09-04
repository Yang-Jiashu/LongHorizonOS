"""Run one LHTB Harbor trial under an LHOS Goal/Verifier control plane."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome
from lhos.sdk.subprocess_agent import subprocess_task_executor


def _find_trial(job_root: Path) -> Path | None:
    candidates = sorted(job_root.glob("*/agent/trajectory.json"))
    return candidates[0].parent.parent if candidates else None


def _trial_metrics(trial_dir: Path | None) -> dict[str, Any]:
    if trial_dir is None:
        return {}
    trajectory = trial_dir / "agent" / "trajectory.json"
    if not trajectory.exists():
        return {}
    data = json.loads(trajectory.read_text(encoding="utf-8"))
    steps = data.get("steps", [])
    trial_result_path = trial_dir / "result.json"
    trial_result = (
        json.loads(trial_result_path.read_text(encoding="utf-8"))
        if trial_result_path.exists()
        else {}
    )
    return {
        "model": data.get("agent", {}).get("model_name"),
        "agent_steps": sum(step.get("source") == "agent" for step in steps),
        "tool_calls": sum(len(step.get("tool_calls") or []) for step in steps),
        "final_metrics": data.get("final_metrics", {}),
        "trial_dir": str(trial_dir),
        "exception_type": trial_result.get("exception_info", {}).get("exception_type"),
        "exception_message": trial_result.get("exception_info", {}).get("exception_message"),
        "started_at": trial_result.get("started_at"),
        "finished_at": trial_result.get("finished_at"),
        "agent_execution": trial_result.get("agent_execution"),
        "verifier": trial_result.get("verifier"),
    }


def _reward(job_result: dict[str, Any]) -> float | None:
    evals = job_result.get("stats", {}).get("evals", {})
    for value in evals.values():
        rewards = value.get("reward_stats", {}).get("reward", {})
        if rewards:
            return max(float(key) for key in rewards)
        metrics = value.get("metrics", [])
        means = [
            float(metric["mean"])
            for metric in metrics
            if isinstance(metric, dict) and metric.get("mean") is not None
        ]
        if means:
            return max(means)
    return None


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[1]
    status_path = args.output.resolve() / "worker-status.json"
    jobs_dir = args.jobs_dir.resolve()
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    def command(_task_id: str) -> list[str]:
        return [
            sys.executable,
            str(repo_root / "scripts" / "harbor_job_worker.py"),
            "--harbor",
            str(args.harbor.resolve()),
            "--config",
            str(args.config.resolve()),
            "--jobs-dir",
            str(jobs_dir),
            "--timeout-seconds",
            str(args.timeout_seconds),
            "--status",
            str(status_path),
        ]

    executor = subprocess_task_executor(
        command,
        cwd=str(repo_root),
        env=env,
        timeout_seconds=args.timeout_seconds + 120,
        poll_seconds=0.1,
    )
    runtime = AgentOS(":memory:")
    runtime.add_agent(Agent("harbor", executor=executor, executor_api="context_v1"))
    goal = Goal(f"lhtb-lhos-{args.mode}")
    evaluation: dict[str, Any] = {}

    def verify(_context: Any, _task_id: str) -> VerificationOutcome:
        nonlocal evaluation
        job_root = jobs_dir / args.job_name
        result_path = job_root / "result.json"
        job_result = (
            json.loads(result_path.read_text(encoding="utf-8")) if result_path.exists() else {}
        )
        trial_dir = _find_trial(job_root)
        reward = _reward(job_result)
        evaluation = {
            "job_result": job_result,
            "reward": reward,
            "trial_metrics": _trial_metrics(trial_dir),
            "worker_status": (
                json.loads(status_path.read_text(encoding="utf-8")) if status_path.exists() else {}
            ),
        }
        return VerificationOutcome(
            passed=bool(reward is not None and reward >= 0.95),
            artifact_id=f"lhtb://{args.job_name}",
            version=1,
            content=json.dumps(evaluation, sort_keys=True),
            evidence_note="Harbor hidden verifier result",
            details=evaluation,
        )

    goal.task(
        "lhtb-trial",
        agent="harbor",
        verify=verify,
        executor_api="context_v1",
        max_attempts=1,
        inputs=(args.input_artifact,),
        outputs=(f"lhtb://{args.job_name}/result",),
    )
    started = time.monotonic()
    result = await runtime.run_async(
        goal,
        max_dispatches=1,
        max_steps=2,
        max_concurrency=1,
        adaptive=True,
        max_parallelism=1,
    )
    elapsed_ms = round((time.monotonic() - started) * 1000, 3)
    runtime.close()
    output = {
        "benchmark": "LHTB single-task LHOS control",
        "model": "openai/step-3.7-flash",
        "reasoning_effort": "medium",
        "job_name": args.job_name,
        "mode": args.mode,
        "input_artifact": args.input_artifact,
        "official_score": False,
        "goal_state": result.goal_state,
        "elapsed_ms": elapsed_ms,
        "evaluation": evaluation,
        "run_result": result.as_dict(),
    }
    args.output.resolve().mkdir(parents=True, exist_ok=True)
    (args.output.resolve() / "result.json").write_text(
        json.dumps(output, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--harbor", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--jobs-dir", type=Path, required=True)
    parser.add_argument("--job-name", required=True)
    parser.add_argument("--mode", choices=("fresh", "checkpoint"), default="fresh")
    parser.add_argument("--input-artifact", default="lhtb://fresh/base")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=900.0)
    args = parser.parse_args()
    output = asyncio.run(_run(args))
    print(json.dumps(output, ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if output["goal_state"] == "closed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
