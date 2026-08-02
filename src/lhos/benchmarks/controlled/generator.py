"""Deterministic controlled-task generator (spec 22.2, 22.3).

One generator, 14 scenario presets (spec 22.3). A preset only twists the
control variables / scripts of a shared parametric DAG builder, so every
scenario stays comparable across modes and seeds.

Determinism contract: ``generate(preset, size, seed)`` is a pure function of
its arguments (one ``random.Random(seed)`` stream, no wall clock, no dict
iteration over unordered data). Same (preset, size, seed) -> identical task.

Recommended sizes (spec 22.2): Small=20, Medium=50, Large=100, XL=200 nodes.
"""

from __future__ import annotations

import random
from typing import Any

from lhos.benchmarks.controlled.oracle import compute_oracle
from lhos.benchmarks.controlled.task_schema import (
    ControlledTask,
    ControlledTaskSpec,
)

SIZES: dict[str, int] = {"small": 20, "medium": 50, "large": 100, "xl": 200}

# Spec 22.3 scenario types.
PRESETS: list[str] = [
    "serial_chain",          # 1. 纯串行链
    "wide_dag",              # 2. 宽 DAG
    "branch_join",           # 3. 多分支后汇聚
    "costly_critical_path",  # 4. 高成本关键路径
    "upstream_failure",      # 5. 某个前置节点失败
    "constraint_change",     # 6. 中途约束改变
    "artifact_modified",     # 7. 已验证 artifact 被修改
    "worker_crash",          # 8. Worker crash
    "runtime_crash",         # 9. Runtime crash
    "post_tool_crash",       # 10. 工具执行完成后、事件写入前 crash
    "alternative_paths",     # 11. 多个可选路径，成本不同
    "external_wait",         # 12. 有等待外部事件的节点
    "noop_nodes",            # 13. 存在无操作节点
    "risky_shortcut",        # 14. 存在会失败的高风险捷径
]

_TOOL_COSTS: dict[str, dict[str, float]] = {
    "filesystem": {"tokens": 20.0, "seconds": 0.001},
    "shell": {"tokens": 30.0, "seconds": 0.002},
    "fake": {"tokens": 10.0, "seconds": 0.001},
}


# --------------------------------------------------------------------- nodes
def _task_node(
    temp_id: str,
    rng: random.Random,
    *,
    token_cost: int | None = None,
    time_ms: int | None = None,
    progress_weight: float = 1.0,
    max_attempts: int = 3,
    script: dict[str, Any] | None = None,
    verification: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A schedulable subtask that writes ``<temp_id>.txt`` and is verified by
    file existence. Tokens come from the compiled context (per mode), so cost
    contrast between modes stays meaningful; estimates are the control
    variable used by the cost-aware scheduler and the oracle."""
    path = f"{temp_id}.txt"
    base_script: dict[str, Any] = {
        "produced_artifacts": [{"path": path, "content": f"output of {temp_id}\n"}],
        "output_tokens": 50,
    }
    if script:
        base_script.update(script)
    node_metadata = {"tool_type": "filesystem", "side_effect_level": "local_write"}
    if metadata:
        node_metadata.update(metadata)
    return {
        "temp_id": temp_id,
        "kind": "subtask",
        "title": f"task {temp_id}",
        "specification": f"Produce {path}",
        "schedulable": True,
        "progress_weight": progress_weight,
        "estimated_token_cost": token_cost if token_cost is not None else rng.randint(400, 1600),
        "estimated_time_ms": time_ms if time_ms is not None else rng.randint(200, 2000),
        "estimated_tool_calls": 1,
        "max_attempts": max_attempts,
        "verification_spec": verification or {"type": "file_exists", "path": path},
        "metadata": {**node_metadata, "script": base_script},
    }


def _noop_node(temp_id: str, rng: random.Random) -> dict[str, Any]:
    return _task_node(
        temp_id,
        rng,
        token_cost=rng.randint(40, 120),
        time_ms=rng.randint(20, 80),
        script={"produced_artifacts": [], "output_tokens": 10},
        verification={"type": "command", "command": "true"},
        metadata={"tool_type": "shell", "side_effect_level": "read_only", "noop": True},
    )


# ----------------------------------------------------------------- topologies
def _layered(count: int, width: int, rng: random.Random) -> tuple[list[str], list[tuple[str, str]]]:
    """Layered DAG: 1 source, middle layers of ``width``, 1 sink. Every node
    depends on 1..2 nodes of the previous layer. Returns (task temp_ids,
    depends_on edges) in execution order."""
    if count < 2:
        ids = ["n0"]
        return ids[:count], []
    middle = count - 2
    layers: list[list[str]] = [["n0"]]
    i = 0
    while i < middle:
        take = min(width, middle - i)
        layers.append([f"n{i + j + 1}" for j in range(take)])
        i += take
    layers.append([f"n{count - 1}"])
    edges: list[tuple[str, str]] = []
    for li in range(1, len(layers)):
        prev, cur = layers[li - 1], layers[li]
        for node in cur:
            edges.append((rng.choice(prev), node))
            if len(prev) > 1 and rng.random() < 0.35:
                second = rng.choice(prev)
                if (second, node) not in edges:
                    edges.append((second, node))
    ids = [n for layer in layers for n in layer]
    return ids, edges


def _chain(count: int) -> tuple[list[str], list[tuple[str, str]]]:
    ids = [f"n{i}" for i in range(count)]
    return ids, [(ids[i], ids[i + 1]) for i in range(count - 1)]


def _branches(count: int, n_branches: int, rng: random.Random) -> tuple[list[str], list[tuple[str, str]]]:
    """1 source -> n_branches parallel chains -> 1 join (多分支后汇聚)."""
    if count < 4 or n_branches < 2:
        return _chain(count)
    inner = count - 2
    per = max(1, inner // n_branches)
    ids = ["src"]
    edges: list[tuple[str, str]] = []
    tails: list[str] = []
    k = 1
    for b in range(n_branches):
        length = per if b < n_branches - 1 else max(1, count - 1 - k)
        prev = "src"
        for _ in range(length):
            node = f"n{k}"
            ids.append(node)
            edges.append((prev, node))
            prev = node
            k += 1
            if k >= count - 1:
                break
        tails.append(prev)
        if k >= count - 1:
            break
    join = f"n{count - 1}"
    ids.append(join)
    for tail in tails:
        edges.append((tail, join))
    return ids, edges


# ------------------------------------------------------------------ generator
def generate(preset: str, size: str | int = "small", seed: int = 1) -> ControlledTask:
    if preset not in PRESETS:
        raise ValueError(f"unknown preset {preset!r}; choose from {PRESETS}")
    node_count = SIZES.get(size, None) if isinstance(size, str) else int(size)
    if node_count is None:
        raise ValueError(f"unknown size {size!r}; choose from {sorted(SIZES)}")
    size_name = size if isinstance(size, str) else f"custom{node_count}"
    rng = random.Random(f"{preset}:{size_name}:{seed}")

    control: dict[str, Any] = {
        "node_count": node_count,
        "graph_depth": None,
        "graph_width": None,
        "critical_path_length": None,
        "parallelism": None,
        "tool_latency_ms": rng.randint(1, 5),
        "token_cost": "rng[400,1600]",
        "failure_probability": 0.0,
        "constraint_change_probability": 0.0,
        "artifact_invalidation_probability": 0.0,
        "crash_point": None,
        "retryability": True,
    }

    extra_nodes: list[dict[str, Any]] = []
    extra_edges: list[dict[str, str]] = []
    environment_events: list[dict[str, Any]] = []
    failure_injections: list[dict[str, Any]] = []

    # ---------------------------------------------------------- topology
    if preset == "serial_chain":
        ids, pairs = _chain(node_count)
        control["graph_width"] = 1
        control["parallelism"] = 1
    elif preset == "wide_dag":
        width = max(2, node_count - 2)
        ids, pairs = _layered(node_count, width, rng)
        control["graph_width"] = width
        control["parallelism"] = width
    elif preset == "branch_join":
        ids, pairs = _branches(node_count, max(2, min(4, node_count // 5)), rng)
        control["graph_width"] = 4
        control["parallelism"] = 4
    else:
        width = max(2, min(6, node_count // 4))
        ids, pairs = _layered(node_count, width, rng)
        control["graph_width"] = width
        control["parallelism"] = width

    nodes = [_task_node(tid, rng) for tid in ids]
    # Edge convention (spec 8.1): source = dependent, target = dependency
    # ("source depends_on target"). Topology builders return (dep, dependent).
    edges = [{"source": t, "target": s, "kind": "depends_on"} for s, t in pairs]
    by_id = {n["temp_id"]: n for n in nodes}

    def mid_task() -> dict[str, Any]:
        """A node with both predecessors and dependents (skip source/sink)."""
        candidates = ids[1:-1] or ids
        return by_id[candidates[len(candidates) // 2]]

    # ------------------------------------------------------------ twists
    if preset == "costly_critical_path":
        chain_ids, chain_pairs = _chain(node_count)
        ids, pairs = chain_ids, chain_pairs
        nodes = [
            _task_node(tid, rng, token_cost=rng.randint(4000, 8000), time_ms=rng.randint(3000, 6000))
            for tid in ids
        ]
        edges = [{"source": t, "target": s, "kind": "depends_on"} for s, t in pairs]
        by_id = {n["temp_id"]: n for n in nodes}
        control["graph_width"] = 1
        control["token_cost"] = "rng[4000,8000] on critical path"

    elif preset == "upstream_failure":
        victim = mid_task()
        victim["metadata"]["script"]["fail_times"] = 1  # retryable by default
        failure_injections.append(
            {"node": victim["temp_id"], "kind": "transient_failure", "fail_times": 1, "retryable": True}
        )
        control["failure_probability"] = 1.0
        control["retryability"] = True

    elif preset == "constraint_change":
        victim = mid_task()
        constraint = {
            "temp_id": "constraint_0",
            "kind": "constraint",
            "title": "external constraint",
            "specification": "Externally controlled constraint node.",
            "schedulable": False,
            "progress_weight": 0.0,
            "metadata": {},
        }
        extra_nodes.append(constraint)
        injector = _task_node(
            "env_injector",
            rng,
            script={
                "environment_events": [
                    {
                        "type": "constraint_changed",
                        "node_id": "constraint_0",
                        "invalidates": [victim["temp_id"]],
                        "source_node": "env_injector",
                        "reason": "benchmark: mid-run constraint change",
                    }
                ]
            },
            metadata={"env_injector": True},
        )
        extra_nodes.append(injector)
        # The injector runs after the victim (so the victim is VERIFIED when
        # the constraint changes) and is a sink outside the affected scope.
        extra_edges.append({"source": "env_injector", "target": victim["temp_id"], "kind": "depends_on"})
        environment_events.append(injector["metadata"]["script"]["environment_events"][0])
        control["constraint_change_probability"] = 1.0

    elif preset == "artifact_modified":
        producer = by_id[ids[0]]
        consumer = by_id[ids[1]] if len(ids) > 1 else producer
        artifact = {
            "temp_id": "artifact_0",
            "kind": "artifact",
            "title": "shared artifact",
            "specification": "Artifact produced by the source, consumed downstream.",
            "schedulable": False,
            "progress_weight": 0.0,
            "metadata": {"path": "shared_artifact.txt"},
        }
        extra_nodes.append(artifact)
        extra_edges.extend(
            [
                {"source": producer["temp_id"], "target": "artifact_0", "kind": "produces"},
                {"source": consumer["temp_id"], "target": "artifact_0", "kind": "consumes"},
            ]
        )
        producer["metadata"]["script"].setdefault("produced_artifacts", []).append(
            {"path": "shared_artifact.txt", "content": "v1\n"}
        )
        # The injector modifies the artifact after the consumer is VERIFIED.
        injector = _task_node(
            "env_injector",
            rng,
            script={
                "produced_artifacts": [
                    {"path": "env_injector.txt", "content": "output of env_injector\n"},
                    {"path": "shared_artifact.txt", "content": "v2 externally modified\n"},
                ]
            },
            metadata={"env_injector": True},
        )
        extra_nodes.append(injector)
        extra_edges.extend(
            [
                {"source": "env_injector", "target": consumer["temp_id"], "kind": "depends_on"},
                {"source": "env_injector", "target": "artifact_0", "kind": "produces"},
            ]
        )
        event = {
            "type": "artifact_updated",
            "node_id": "artifact_0",
            "oracle_victims": [consumer["temp_id"]],
            "source_node": "env_injector",
            "reason": "benchmark: verified artifact modified externally",
        }
        environment_events.append(event)
        control["artifact_invalidation_probability"] = 1.0

    elif preset == "worker_crash":
        victim = mid_task()
        victim["metadata"]["script"]["crash_on_attempt"] = 1
        failure_injections.append(
            {"node": victim["temp_id"], "kind": "worker_crash", "crash_point": "worker_execute", "attempt": 1}
        )
        control["crash_point"] = "worker_execute"

    elif preset == "runtime_crash":
        victim = mid_task()
        victim["metadata"]["script"]["crash_before_verification"] = True
        failure_injections.append(
            {"node": victim["temp_id"], "kind": "runtime_crash", "crash_point": "before_verification"}
        )
        control["crash_point"] = "before_verification"

    elif preset == "post_tool_crash":
        victim = mid_task()
        victim["metadata"]["script"]["crash_after_tool_calls"] = 1
        failure_injections.append(
            {"node": victim["temp_id"], "kind": "post_tool_crash", "crash_point": "after_tool_before_event"}
        )
        control["crash_point"] = "after_tool_before_event"

    elif preset == "alternative_paths":
        # AND-semantics limitation: both branches execute; the contrast is
        # cost/latency between branches (scheduling-order signal for the
        # cost-aware scheduler). True OR-path pruning is not modeled.
        if node_count >= 6:
            cheap = ids[1]
            pricey = ids[2]
            by_id[cheap]["estimated_token_cost"] = 200
            by_id[cheap]["estimated_time_ms"] = 100
            by_id[pricey]["estimated_token_cost"] = 6000
            by_id[pricey]["estimated_time_ms"] = 5000
            control["token_cost"] = "bimodal: cheap 200 / expensive 6000 branches"

    elif preset == "external_wait":
        waiter = mid_task()
        waiter["metadata"]["script"]["attempts"] = {"1": {"status": "waiting"}}
        waiter["metadata"]["waits_for_external_event"] = True

    elif preset == "noop_nodes":
        for i, tid in enumerate(ids):
            if i % 5 == 4:  # ~20% noop
                nodes[i] = _noop_node(tid, rng)
        by_id = {n["temp_id"]: n for n in nodes}

    elif preset == "risky_shortcut":
        if node_count >= 4:
            shortcut = _task_node(
                "shortcut",
                rng,
                token_cost=150,
                time_ms=100,
                max_attempts=2,
                script={"fail_times": 1},  # the risk materializes once
                metadata={"risky": True},
            )
            extra_nodes.append(shortcut)
            # The shortcut competes with the safe path into the sink region.
            extra_edges.append({"source": "shortcut", "target": ids[0], "kind": "depends_on"})
            extra_edges.append({"source": ids[-1], "target": "shortcut", "kind": "depends_on"})
            failure_injections.append(
                {"node": "shortcut", "kind": "risky_shortcut", "fail_times": 1, "retryable": True}
            )
            control["failure_probability"] = 0.5

    nodes.extend(extra_nodes)
    edges.extend(extra_edges)

    # -------------------------------------------------- control variables
    depth = 1
    level: dict[str, int] = {}
    for tid in [n["temp_id"] for n in nodes]:
        preds = [s for s, t in pairs if t == tid] + [
            e["target"] for e in extra_edges if e["source"] == tid and e["kind"] == "depends_on"
        ]
        level[tid] = 1 + max((level.get(p, 0) for p in preds), default=0)
        depth = max(depth, level[tid])
    control["graph_depth"] = depth

    task_id = f"controlled-{preset}-{size_name}-s{seed}"
    oracle = compute_oracle(nodes, edges, environment_events)
    control["critical_path_length"] = len(oracle.critical_path)

    total_weight = sum(float(n.get("progress_weight", 1.0)) for n in nodes if n.get("schedulable"))
    spec = ControlledTaskSpec(
        task_id=task_id,
        goal=f"Controlled benchmark task {task_id}: complete every subtask.",
        oracle_nodes=nodes,
        oracle_edges=edges,
        tool_costs=dict(_TOOL_COSTS),
        failure_injections=failure_injections,
        environment_events=environment_events,
        total_progress_weight=total_weight,
    )
    return ControlledTask(
        spec=spec,
        oracle=oracle,
        preset=preset,
        size=str(size_name),
        seed=seed,
        control_variables=control,
    )
