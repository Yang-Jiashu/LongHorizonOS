"""Benchmark runner: drive (task, mode, seed) cells through the same Runtime.

Every graph mode uses the identical RuntimeStack wiring as the CLI (spec 25:
只替换 Runtime 模块); the transcript mode uses the transcript baseline. Each
cell runs in its own directory (own SQLite db, own workspace), so cells are
independent and re-runnable.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from lhos.bootstrap import RuntimeStack
from lhos.benchmarks import scoring
from lhos.benchmarks.controlled.environment import ScriptedEnvironment
from lhos.benchmarks.controlled.generator import generate
from lhos.benchmarks.controlled.task_schema import ControlledTask
from lhos.benchmarks.modes import ModeConfig, make_scheduler, mode_config
from lhos.benchmarks.transcript import run_transcript
from lhos.domain.errors import SimulatedCrashError

_MAX_CRASHES_PER_RUN = 16

_ID_KEYS = ("node_id", "source_node")
_ID_LIST_KEYS = ("invalidates", "oracle_victims", "oracle_affected")


def _rewrite_environment_event_ids(graph_spec: dict[str, Any], run_id: str) -> None:
    """Scripts carry temp ids; the runtime needs real ``{run_id}:{temp}`` ids."""
    for node in graph_spec.get("nodes", []):
        script = node.get("metadata", {}).get("script", {})
        for raw in script.get("environment_events", []):
            for key in _ID_KEYS:
                if raw.get(key):
                    raw[key] = f"{run_id}:{raw[key]}"
            for key in _ID_LIST_KEYS:
                if raw.get(key):
                    raw[key] = [f"{run_id}:{t}" for t in raw[key]]


def _drive(stack: RuntimeStack, run_id: str) -> tuple[Any, int]:
    """Run / resume through crashes and environment-resolved waits."""
    env = ScriptedEnvironment(stack.graph_store, run_id)
    crashes = 0
    action = "run"
    while True:
        try:
            run = (
                stack.controller.run(run_id)
                if action == "run"
                else stack.controller.resume(run_id)
            )
        except SimulatedCrashError:
            crashes += 1
            if crashes > _MAX_CRASHES_PER_RUN:
                raise
            action = "resume"
            continue
        if run.status == "paused" and env.resolve_waiting() > 0:
            stack.graph_store.set_run_status(run_id, "running")
            action = "run"
            continue
        return run, crashes


def run_cell(
    task: ControlledTask,
    mode_name: str,
    work_root: str | Path,
    artifacts_dir: str = "artifacts",
) -> dict[str, Any]:
    mode: ModeConfig = mode_config(mode_name, artifacts_dir=artifacts_dir)
    run_id = f"bench-{task.task_id}-{mode_name}"
    cell_dir = Path(work_root) / "runs" / run_id
    workspace = cell_dir / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    if mode.engine == "transcript":
        result = run_transcript(task, workspace)
        row = scoring.score_transcript_run(result, task)
        row["run_id"] = run_id
        return row

    stack = RuntimeStack(
        db_path=cell_dir / "state.db",
        workspace_dir=workspace,
        config=dict(mode.config),
    )
    try:
        oracle_scheduler = make_scheduler(mode)
        if oracle_scheduler is not None:
            # RuntimeStack only wires fifo/cost_aware; oracle schedulers are
            # benchmark components swapped in afterwards (same interface).
            stack.scheduler = oracle_scheduler
            stack.controller._scheduler = oracle_scheduler

        graph_spec = task.graph_spec(use_oracle_priorities=mode.use_oracle_priorities)
        _rewrite_environment_event_ids(graph_spec, run_id)
        stack.graph_store.create_run(
            run_id,
            graph_spec["goal"],
            {
                "benchmark": "controlled",
                "mode": mode_name,
                "task_id": task.task_id,
                "seed": task.seed,
                "mode_config": dict(mode.config),
                "control_variables": dict(task.control_variables),
            },
        )
        stack.initial_builder.build(run_id, graph_spec)
        started = time.perf_counter()
        run, crashes = _drive(stack, run_id)
        wall = time.perf_counter() - started
        row = scoring.score_graph_run(stack, run_id, task, mode_name, wall, crashes)
        row["run_id"] = run_id
        row["db_path"] = str(cell_dir / "state.db")
        return row
    finally:
        stack.close()


def run_suite(
    modes: list[str],
    presets: list[str],
    seeds: list[int],
    size: str = "small",
    work_root: str | Path = "artifacts/benchmark_work",
    artifacts_dir: str = "artifacts",
    progress: bool = False,
) -> list[dict[str, Any]]:
    """Run every (preset, mode, seed) cell; one result row per cell."""
    rows: list[dict[str, Any]] = []
    for preset in presets:
        for seed in seeds:
            task = generate(preset, size=size, seed=seed)
            for mode_name in modes:
                if progress:
                    print(f"[benchmark] {task.task_id} mode={mode_name} ...", flush=True)
                rows.append(run_cell(task, mode_name, work_root, artifacts_dir=artifacts_dir))
    return rows
