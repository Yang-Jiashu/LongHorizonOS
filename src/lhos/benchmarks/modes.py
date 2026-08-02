"""Experiment modes (spec 25) as a mode -> runtime-config mapping.

All 8 modes reuse the same model (FakeWorker), tools, budget, seed, task,
verification and workspace initialization; only the runtime modules differ.
Honesty note: the planner is the deterministic InitialGraphBuilder in every
mode (no LLM planner noise yet), so oracle modes differ from dynamic modes
only through the oracle priority hints and scheduler — documented in the
README and reports.

Mode semantics:

- ``transcript``: no graph runtime at all — the transcript baseline.
- ``static_graph_fifo``: graph built once; environment events are logged but
  never reconciled (``features.invalidation = False``); FIFO.
- ``dynamic_graph_fifo``: invalidation reconciliation on, but local repair
  off — INVALIDATED nodes are not replanned, so a must-invalidate event
  strands the run (the intended contrast with repair modes).
- ``dynamic_graph_local_repair``: invalidation + local replan, FIFO.
- ``dynamic_graph_cost_aware``: invalidation + local replan, cost-aware
  scheduler (spec 11.2).
- ``full_lhos``: cost-aware + repair + filesystem checkpoints with
  restore-on-failure / restore-on-crash + JSONL trace.
- ``oracle_graph_fifo`` / ``oracle_graph_cost_aware``: same runtime as the
  dynamic modes, but nodes carry the generator's oracle criticality as
  ``priority`` and the schedulers exploit it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lhos.runtime.cost_aware_scheduler import CostAwareScheduler

MODES: list[str] = [
    "transcript",
    "static_graph_fifo",
    "dynamic_graph_fifo",
    "dynamic_graph_local_repair",
    "dynamic_graph_cost_aware",
    "full_lhos",
    "oracle_graph_fifo",
    "oracle_graph_cost_aware",
]


class OracleFifoScheduler:
    """FIFO tie-break, but always picks the highest oracle priority first."""

    def select(self, ready_nodes, graph, budget, resources):  # noqa: ANN001
        if not ready_nodes:
            return None
        return min(
            ready_nodes,
            key=lambda n: (
                -float(n.priority or 0.0),
                n.metadata.get("ready_at") or n.created_at.isoformat(),
                n.id,
            ),
        )


class OracleCostAwareScheduler(CostAwareScheduler):
    """Cost-aware scoring plus an oracle-priority bonus."""

    _ORACLE_WEIGHT = 1.5

    def score(self, node, graph, resources, max_token_cost, max_time_ms, max_age_seconds, now):  # noqa: ANN001
        return super().score(
            node, graph, resources, max_token_cost, max_time_ms, max_age_seconds, now
        ) + self._ORACLE_WEIGHT * float(node.priority or 0.0)


@dataclass(frozen=True)
class ModeConfig:
    name: str
    engine: str  # "transcript" | "graph"
    scheduler: str  # "fifo" | "cost_aware" | "oracle_fifo" | "oracle_cost_aware"
    use_oracle_priorities: bool
    config: dict[str, Any] = field(default_factory=dict)

    @property
    def scheduler_family(self) -> str:
        """The --scheduler CLI filter family: fifo | cost_aware."""
        return "fifo" if self.scheduler in {"fifo", "oracle_fifo"} else "cost_aware"


def mode_config(name: str, artifacts_dir: str = "artifacts") -> ModeConfig:
    if name not in MODES:
        raise ValueError(f"unknown mode {name!r}; choose from {MODES}")
    if name == "transcript":
        return ModeConfig(name, engine="transcript", scheduler="fifo",
                          use_oracle_priorities=False, config={})

    base_features = {"invalidation": True, "local_repair": True}
    table: dict[str, ModeConfig] = {
        "static_graph_fifo": ModeConfig(
            name, "graph", "fifo", False,
            config={"features": {"invalidation": False, "local_repair": False}},
        ),
        "dynamic_graph_fifo": ModeConfig(
            name, "graph", "fifo", False,
            config={"features": {"invalidation": True, "local_repair": False}},
        ),
        "dynamic_graph_local_repair": ModeConfig(
            name, "graph", "fifo", False,
            config={"features": dict(base_features)},
        ),
        "dynamic_graph_cost_aware": ModeConfig(
            name, "graph", "cost_aware", False,
            config={"features": dict(base_features), "scheduler": {"type": "cost_aware"}},
        ),
        "full_lhos": ModeConfig(
            name, "graph", "cost_aware", False,
            config={
                "features": dict(base_features),
                "scheduler": {"type": "cost_aware"},
                "checkpoint": {
                    "type": "filesystem",
                    "restore_on_failure": True,
                    "restore_on_crash": True,
                    "after_verified_node": True,
                },
                "checkpoint_root": f"{artifacts_dir}/checkpoints",
                "telemetry": {"jsonl_trace": True, "trace_directory": f"{artifacts_dir}/traces"},
            },
        ),
        "oracle_graph_fifo": ModeConfig(
            name, "graph", "oracle_fifo", True,
            config={"features": dict(base_features)},
        ),
        "oracle_graph_cost_aware": ModeConfig(
            name, "graph", "oracle_cost_aware", True,
            config={"features": dict(base_features), "scheduler": {"type": "cost_aware"}},
        ),
    }
    return table[name]


def make_scheduler(mode: ModeConfig):  # noqa: ANN001
    """Instantiate the scheduler for oracle modes; standard schedulers are
    wired by RuntimeStack from the config dict."""
    if mode.scheduler == "oracle_fifo":
        return OracleFifoScheduler()
    if mode.scheduler == "oracle_cost_aware":
        return OracleCostAwareScheduler()
    return None
