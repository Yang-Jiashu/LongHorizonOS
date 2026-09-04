"""Run every LHTB task as an independently verified LongHorizonOS Task."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

import yaml

from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome
from lhos.sdk.subprocess_agent import subprocess_task_executor


def _task_names(root: Path) -> tuple[str, ...]:
    return tuple(
        sorted(
            path.name for path in root.iterdir() if path.is_dir() and (path / "task.toml").is_file()
        )
    )


def _write_configs(
    destination: Path,
    *,
    tasks_root: Path,
    task_names: tuple[str, ...],
    agent_timeout_seconds: int,
) -> dict[str, Path]:
    destination.mkdir(parents=True, exist_ok=True)
    configs: dict[str, Path] = {}
    for task_name in task_names:
        payload = {
            "job_name": f"lhtb-lhos-{task_name}",
            "jobs_dir": "./jobs",
            "n_attempts": 1,
            "n_concurrent_trials": 1,
            "timeout_multiplier": 1.0,
            "environment": {
                "type": "docker",
                "force_build": True,
                "delete": True,
            },
            "agents": [
                {
                    "name": "terminus-2",
                    "model_name": "openai/step-3.7-flash",
                    "override_timeout_sec": agent_timeout_seconds,
                    "kwargs": {
                        "parser_name": "json",
                        "temperature": 0.2,
                        "enable_summarize": True,
                        "proactive_summarization_threshold": 8000,
                        "record_terminal_session": True,
                        "llm_call_kwargs": {
                            "api_base": "https://api.stepfun.com/step_plan/v1",
                            "max_tokens": 32768,
                            "temperature": 0.2,
                            "timeout": 180,
                            "num_retries": 1,
                        },
                    },
                }
            ],
            "datasets": [
                {
                    "path": tasks_root.as_posix(),
                    "task_names": [task_name],
                }
            ],
        }
        path = destination / f"{task_name}.yaml"
        path.write_text(
            yaml.safe_dump(payload, sort_keys=False),
            encoding="utf-8",
        )
        configs[task_name] = path
    return configs


def _load(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}


def _trial_dir(job_root: Path) -> Path | None:
    candidates = sorted(job_root.glob("*/result.json"))
    return candidates[0].parent if candidates else None


def _reward(job_result: dict[str, Any]) -> float | None:
    evals = job_result.get("stats", {}).get("evals", {})
    if not isinstance(evals, dict):
        return None
    rewards: list[float] = []
    for value in evals.values():
        if not isinstance(value, dict):
            continue
        reward_stats = value.get("reward_stats", {}).get("reward", {})
        if isinstance(reward_stats, dict):
            rewards.extend(float(item) for item in reward_stats)
        for metric in value.get("metrics", ()):
            if isinstance(metric, dict) and metric.get("mean") is not None:
                rewards.append(float(metric["mean"]))
    return max(rewards) if rewards else None


def _metrics(task_name: str, jobs_root: Path) -> dict[str, Any]:
    job_root = jobs_root / f"lhtb-lhos-{task_name}"
    job_result = _load(job_root / "result.json")
    trial = _trial_dir(job_root)
    trial_result = _load(trial / "result.json") if trial is not None else {}
    trajectory = _load(trial / "agent" / "trajectory.json") if trial is not None else {}
    final = trajectory.get("final_metrics", {})
    steps = trajectory.get("steps", ())
    reward = _reward(job_result)
    return {
        "task_name": task_name,
        "reward": reward,
        "resolved": bool(reward is not None and reward >= 0.95),
        "exception_type": trial_result.get("exception_info", {}).get("exception_type"),
        "exception_message": trial_result.get("exception_info", {}).get("exception_message"),
        "agent_steps": sum(
            isinstance(step, dict) and step.get("source") == "agent" for step in steps
        ),
        "tool_calls": sum(
            len(step.get("tool_calls") or ())
            for step in steps
            if isinstance(step, dict) and step.get("source") == "agent"
        ),
        "prompt_tokens": int(final.get("total_prompt_tokens", 0) or 0),
        "cached_tokens": int(final.get("total_cached_tokens", 0) or 0),
        "completion_tokens": int(final.get("total_completion_tokens", 0) or 0),
        "trial_dir": "" if trial is None else str(trial),
    }


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[1]
    tasks_root = args.tasks.resolve()
    task_names = _task_names(tasks_root)
    if len(task_names) != 46:
        raise RuntimeError(f"expected 46 LHTB tasks, found {len(task_names)}")
    output = args.output.resolve()
    jobs_root = args.jobs_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    jobs_root.mkdir(parents=True, exist_ok=True)
    configs = _write_configs(
        output / "configs",
        tasks_root=tasks_root,
        task_names=task_names,
        agent_timeout_seconds=args.agent_timeout_seconds,
    )
    statuses = output / "worker-status"
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env["DOCKER_DEFAULT_PLATFORM"] = "linux/amd64"
    env["HB_VERIFIER_FEEDBACK_MODE"] = "binary"

    def command(task_id: str) -> list[str]:
        return [
            os.fspath(Path(os.sys.executable)),
            os.fspath(repo_root / "scripts" / "harbor_job_worker.py"),
            "--harbor-project",
            os.fspath(args.harbor_project.resolve()),
            "--config",
            os.fspath(configs[task_id]),
            "--jobs-dir",
            os.fspath(jobs_root),
            "--timeout-seconds",
            str(args.worker_timeout_seconds),
            "--status",
            os.fspath(statuses / f"{task_id}.json"),
        ]

    executor = subprocess_task_executor(
        command,
        cwd=os.fspath(repo_root),
        env=env,
        timeout_seconds=args.worker_timeout_seconds + 120,
        poll_seconds=0.1,
    )
    runtime = AgentOS(":memory:")
    runtime.add_agent(
        Agent(
            "harbor",
            executor=executor,
            executor_api="context_v1",
            max_concurrency=args.max_concurrency,
        )
    )
    goal = Goal("lhtb-full46-lhos-control", executor_api="context_v1")
    evaluations: dict[str, dict[str, Any]] = {}

    def verifier_for(task_name: str):
        def verify(_context: Any, dispatched_task_id: str) -> VerificationOutcome:
            if dispatched_task_id != task_name:
                raise RuntimeError(f"verifier {task_name!r} received {dispatched_task_id!r}")
            evaluation = _metrics(task_name, jobs_root)
            evaluations[task_name] = evaluation
            return VerificationOutcome(
                passed=bool(evaluation["resolved"]),
                artifact_id=f"lhtb://{task_name}/result",
                version=1,
                content=json.dumps(evaluation, sort_keys=True),
                evidence_note="per-task Harbor hidden verifier",
                details=evaluation,
            )

        return verify

    for task_name in task_names:
        goal.task(
            task_name,
            agent="harbor",
            verify=verifier_for(task_name),
            executor_api="context_v1",
            max_attempts=1,
            inputs=(f"lhtb://{task_name}/base",),
            outputs=(f"lhtb://{task_name}/result",),
        )

    started = time.monotonic()
    try:
        result = await runtime.run_async(
            goal,
            max_dispatches=len(task_names),
            max_steps=len(task_names) + 2,
            max_concurrency=args.max_concurrency,
            adaptive=True,
            max_parallelism=args.max_concurrency,
        )
    finally:
        runtime.close()
    for task_name in task_names:
        evaluations.setdefault(task_name, _metrics(task_name, jobs_root))
    ordered = [evaluations[item] for item in task_names]
    summary = {
        "schema_version": "lhos-lhtb-full46-control.v1",
        "benchmark": "LHTB local-pilot 46 tasks",
        "official_score": False,
        "arm": "lhos-per-task-control",
        "model": "step-3.7-flash",
        "agent_timeout_seconds": args.agent_timeout_seconds,
        "max_concurrency": args.max_concurrency,
        "goal_state": result.goal_state,
        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
        "task_count": len(task_names),
        "resolved_count": sum(bool(item["resolved"]) for item in ordered),
        "exception_count": sum(bool(item["exception_type"]) for item in ordered),
        "prompt_tokens": sum(int(item["prompt_tokens"]) for item in ordered),
        "cached_tokens": sum(int(item["cached_tokens"]) for item in ordered),
        "completion_tokens": sum(int(item["completion_tokens"]) for item in ordered),
        "tool_calls": sum(int(item["tool_calls"]) for item in ordered),
        "evaluations": ordered,
        "run_result": result.as_dict(),
        "attribution": (
            "Each official task is an independent LHOS Goal Task with its own Harbor "
            "trial and verifier. This tests task-level control and overhead, not "
            "within-task selective repair."
        ),
    }
    (output / "result.json").write_text(
        json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--harbor-project", type=Path, required=True)
    parser.add_argument("--tasks", type=Path, required=True)
    parser.add_argument("--jobs-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--agent-timeout-seconds", type=int, default=900)
    parser.add_argument("--worker-timeout-seconds", type=float, default=3600)
    parser.add_argument("--max-concurrency", type=int, default=2)
    args = parser.parse_args()
    summary = asyncio.run(_run(args))
    return 0 if summary["task_count"] == 46 else 1


if __name__ == "__main__":
    raise SystemExit(main())
