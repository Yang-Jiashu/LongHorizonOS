"""Benchmark Adapter interface (spec 23).

Controlled, Terminal and SWE-style tasks all drive the same Runtime through
this interface. The spec sketch is async; the implementation is async over a
synchronous core (the runtime is single-threaded by design, spec 11.3).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, Field

from lhos.benchmarks.controlled.generator import generate
from lhos.benchmarks.controlled.task_schema import ControlledTask
from lhos.benchmarks.runner import run_cell


class BenchmarkScore(BaseModel):
    """Spec section 23."""

    success: bool
    verified_progress: float
    total_progress: float
    task_specific_metrics: dict[str, float] = Field(default_factory=dict)


class BenchmarkAdapter(Protocol):
    """Spec section 23 protocol."""

    async def reset(self, task_id: str, seed: int) -> None: ...
    async def get_goal(self) -> str: ...
    async def get_environment_snapshot(self) -> dict[str, Any]: ...
    async def score(self) -> BenchmarkScore: ...
    async def cleanup(self) -> None: ...


class ControlledAdapter:
    """Adapter for the controlled benchmark suite (spec 22 + 23).

    ``task_id`` encodes preset and size: ``controlled-<preset>-<size>``
    (e.g. ``controlled-serial_chain-small``); the seed comes from ``reset``.
    """

    def __init__(
        self,
        mode: str = "dynamic_graph_local_repair",
        work_root: str | Path = "artifacts/benchmark_work",
    ):
        self._mode = mode
        self._work_root = Path(work_root)
        self._task: ControlledTask | None = None
        self._row: dict[str, Any] | None = None

    async def reset(self, task_id: str, seed: int) -> None:
        parts = task_id.split("-")
        if len(parts) != 3 or parts[0] != "controlled":
            raise ValueError(
                f"controlled task ids look like controlled-<preset>-<size>, got {task_id!r}"
            )
        _, preset, size = parts
        self._task = generate(preset, size=size, seed=seed)
        self._row = None

    async def get_goal(self) -> str:
        self._require_task()
        assert self._task is not None  # for type narrowing; _require_task raises if None
        return self._task.spec.goal

    async def get_environment_snapshot(self) -> dict[str, Any]:
        self._require_task()
        assert self._task is not None  # for type narrowing; _require_task raises if None
        return {
            "preset": self._task.preset,
            "size": self._task.size,
            "seed": self._task.seed,
            "control_variables": dict(self._task.control_variables),
            "environment_events": [dict(e) for e in self._task.spec.environment_events],
            "failure_injections": [dict(f) for f in self._task.spec.failure_injections],
        }

    async def run(self) -> dict[str, Any]:
        """Drive the cell through the runtime (adapter extension)."""
        self._require_task()
        assert self._task is not None  # for type narrowing; _require_task raises if None
        self._row = run_cell(self._task, self._mode, self._work_root)
        return self._row

    async def score(self) -> BenchmarkScore:
        if self._row is None:
            await self.run()
        row = self._row or {}
        skip = {
            "task_id",
            "preset",
            "size",
            "seed",
            "mode",
            "success",
            "run_status",
            "verified_progress",
            "progress_ratio",
            "run_id",
            "db_path",
        }
        return BenchmarkScore(
            success=bool(row.get("success")),
            verified_progress=float(row.get("verified_progress", 0.0)),
            total_progress=float(row.get("progress_ratio", 0.0)),
            task_specific_metrics={
                k: float(v)
                for k, v in row.items()
                if k not in skip and isinstance(v, (int, float)) and not isinstance(v, bool)
            },
        )

    async def cleanup(self) -> None:
        # Cells are self-contained directories; nothing external to release.
        self._row = None

    def _require_task(self) -> None:
        if self._task is None:
            raise RuntimeError("reset() must be called first")
