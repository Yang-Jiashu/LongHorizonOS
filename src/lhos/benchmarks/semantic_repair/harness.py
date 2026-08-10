"""Comparative semantic-repair benchmark harness.

The harness separates four policies on the same deterministic DAG:

* full restart: rerun every previously verified task;
* state-only resume: trust completed bits and do no repair;
* task-DAG checkpoint: rerun the exact downstream affected set;
* LongHorizonOS: derive invalidation through VPG, then actually run to reclose.

All LongHorizonOS execution counts are observed from scheduler attempts. Safety
metrics are derived from public semantic state and claim history, never filled
with constants.
"""

from __future__ import annotations

import random
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from lhos.sdk import Agent, AgentOS, Goal
from lhos.sdk.verification import VerificationOutcome

TASK_COSTS = (1, 5, 10)


def build_dag(n: int, topology: str, seed: int) -> tuple[list[str], list[tuple[str, str]]]:
    """Build a deterministic DAG.

    Edge ``(source, target)`` means ``source DEPENDS_ON target``.
    """
    rng = random.Random(seed)
    ids = [f"T{i}" for i in range(n)]
    edges: list[tuple[str, str]] = []
    if topology == "chain":
        for i in range(n - 1):
            edges.append((ids[i + 1], ids[i]))
    elif topology == "fan_out":
        for i in range(1, n):
            edges.append((ids[i], ids[0]))
    elif topology == "fan_in":
        tip = ids[-1]
        for i in range(n - 1):
            edges.append((tip, ids[i]))
    elif topology == "diamond":
        if n >= 4:
            edges.extend([(ids[1], ids[0]), (ids[2], ids[0]), (ids[3], ids[1]), (ids[3], ids[2])])
            for i in range(4, n):
                edges.append((ids[i], ids[i - 1]))
    else:
        for i in range(n - 1):
            edges.append((ids[i + 1], ids[i]))
        for _ in range(n):
            left = rng.randrange(n)
            right = rng.randrange(n)
            if left != right:
                low, high = min(left, right), max(left, right)
                edges.append((ids[high], ids[low]))
    seen: set[tuple[str, str]] = set()
    deduped: list[tuple[str, str]] = []
    for source, target in edges:
        if (source, target) not in seen:
            seen.add((source, target))
            deduped.append((source, target))
    return ids, deduped


def oracle(edges: list[tuple[str, str]], root: str) -> tuple[set[str], set[str], list[str]]:
    """Compute the independent causal cone, preserved set and initial frontier."""
    task_ids = {root}
    reverse: dict[str, set[str]] = {}
    dependencies: dict[str, set[str]] = {}
    for source, target in edges:
        task_ids.update((source, target))
        reverse.setdefault(target, set()).add(source)
        dependencies.setdefault(source, set()).add(target)

    affected: set[str] = set()
    queue = deque([root])
    while queue:
        current = queue.popleft()
        if current in affected:
            continue
        affected.add(current)
        queue.extend(reverse.get(current, ()))

    preserved = task_ids - affected
    frontier = [
        task_id for task_id in sorted(affected) if not (dependencies.get(task_id, set()) & affected)
    ]
    return affected, preserved, frontier


def _downstream_suffix(edges: list[tuple[str, str]], root: str) -> set[str]:
    affected, _, _ = oracle(edges, root)
    return affected


def _task_costs(ids: list[str], seed: int) -> dict[str, int]:
    rng = random.Random(f"semantic-repair:{seed}:{len(ids)}")
    return {task_id: rng.choice(TASK_COSTS) for task_id in ids}


def _versioned_executor(
    artifact_id: str,
    versions: dict[str, int],
    observer: Callable[[str, int], None] | None = None,
) -> Callable[[], VerificationOutcome]:
    def _run() -> VerificationOutcome:
        version = versions[artifact_id]
        if observer is not None:
            observer(artifact_id, version)
        return VerificationOutcome(
            passed=True,
            artifact_id=artifact_id,
            version=version,
            content=f"{artifact_id}-v{version}",
            evidence_note="deterministic-versioned-executor",
        )

    return _run


def build_lhos(
    ids: list[str],
    edges: list[tuple[str, str]],
    root: str,
    *,
    observer: Callable[[str, int], None] | None = None,
) -> tuple[AgentOS, Goal, str, dict[str, int]]:
    """Build a real VPG goal with mutable exact ArtifactVersions."""
    runtime = AgentOS(":memory:")
    runtime.add_agent(Agent("a", specializations=("python",), max_concurrency=max(len(ids), 4)))
    goal = Goal("B")
    by_id: dict[str, Any] = {}
    versions: dict[str, int] = {}
    for task_id in ids:
        dependencies = [
            by_id[target] for source, target in edges if source == task_id and target in by_id
        ]
        artifact_id = "mutation" if task_id == root else f"art_{task_id}"
        versions[artifact_id] = 1
        by_id[task_id] = goal.task(
            task_id,
            agent="a",
            depends_on=tuple(dependencies),
            verify=_versioned_executor(artifact_id, versions, observer),
        )
    return runtime, goal, "mutation", versions


def select_root_for_fraction(
    ids: list[str], edges: list[tuple[str, str]], target: float, seed: int
) -> str:
    """Pick a root whose actual causal fraction is closest to ``target``."""
    rng = random.Random(seed)
    candidates = list(rng.sample(ids, min(len(ids), 24)))
    best = ids[0]
    best_error = float("inf")
    for candidate in candidates:
        affected, _, _ = oracle(edges, candidate)
        error = abs(round(len(affected) / len(ids), 3) - target)
        if error < best_error:
            best = candidate
            best_error = error
    return best


def _ownership_conflicts(claims: list[Any]) -> int:
    """Count overlapping activated claim intervals for the same graph/task."""
    conflicts = 0
    activated = [claim for claim in claims if claim.activated_at is not None]
    for index, left in enumerate(activated):
        left_start = left.activated_at.timestamp()
        left_end = left.released_at.timestamp() if left.released_at is not None else float("inf")
        for right in activated[index + 1 :]:
            if (left.graph_id, left.task_id) != (right.graph_id, right.task_id):
                continue
            right_start = right.activated_at.timestamp()
            right_end = (
                right.released_at.timestamp() if right.released_at is not None else float("inf")
            )
            if max(left_start, right_start) < min(left_end, right_end):
                conflicts += 1
    return conflicts


def run_trial(n: int, topology: str, seed: int, root_idx: int) -> dict[str, Any]:
    ids, edges = build_dag(n, topology, seed)
    root = ids[root_idx % len(ids)]
    return run_trial_ids(ids, edges, root, seed, n=n, topology=topology)


def run_trial_ids(
    ids: list[str],
    edges: list[tuple[str, str]],
    root: str,
    seed: int,
    *,
    n: int | None = None,
    topology: str = "mixed",
) -> dict[str, Any]:
    """Run a complete close → mutate → invalidate → reclose trial."""
    exp_affected, exp_preserved, exp_frontier = oracle(edges, root)
    costs = _task_costs(ids, seed)

    full_restart_tasks = set(ids)
    checkpoint_tasks = _downstream_suffix(edges, root)

    runtime, goal, artifact_id, versions = build_lhos(ids, edges, root)
    initial = runtime.run(
        goal,
        max_dispatches=(n or len(ids)) * 4,
        max_steps=(n or len(ids)) + 8,
    )
    initial_attempts = len(runtime.scheduler.attempts)

    versions[artifact_id] = 2
    invalidation_started = time.perf_counter()
    repair = runtime.repair(goal, artifact_id=artifact_id, new_artifact_version=2)
    invalidation_wall_ms = round((time.perf_counter() - invalidation_started) * 1000, 3)

    affected = set(repair.affected)
    preserved = set(repair.preserved)
    frontier = set(repair.frontier)
    invalidated_status = runtime.status(goal)
    false_verified = len(affected & set(invalidated_status.verified))

    before_reclose = len(runtime.scheduler.attempts)
    reclose_started = time.perf_counter()
    final = runtime.run(
        goal,
        max_dispatches=(n or len(ids)) * 4,
        max_steps=(n or len(ids)) + 8,
    )
    reclose_wall_ms = round((time.perf_counter() - reclose_started) * 1000, 3)
    repair_attempts = runtime.scheduler.attempts[before_reclose:]
    repair_attempt_tasks = [attempt.task_id for attempt in repair_attempts]

    under_invalidation = len(exp_affected - affected)
    over_invalidation = len(affected - exp_affected)
    ownership_conflicts = _ownership_conflicts(runtime.scheduler.claims)
    actual_repair_work = sum(costs[task_id] for task_id in repair_attempt_tasks)
    affected_work = sum(costs[task_id] for task_id in exp_affected)
    full_restart_work = sum(costs.values())
    checkpoint_work = sum(costs[task_id] for task_id in checkpoint_tasks)

    lhos_correct = under_invalidation == 0 and over_invalidation == 0
    frontier_correct = frontier == set(exp_frontier)
    final_goal_closed = final.goal_state == "closed"
    valid_trial = (
        initial.goal_state == "closed"
        and lhos_correct
        and frontier_correct
        and false_verified == 0
        and ownership_conflicts == 0
        and final_goal_closed
    )

    prior = max(1, len(ids))
    return {
        "benchmark_id": "semantic_repair_v2",
        "strategy": "comparative",
        "n": n or len(ids),
        "topology": topology,
        "seed": seed,
        "root": root,
        "affected_fraction": round(len(exp_affected) / prior, 3),
        "task_costs": costs,
        "oracle_affected": len(exp_affected),
        "oracle_preserved": len(exp_preserved),
        "oracle_frontier": len(exp_frontier),
        "full_restart_rerun": len(full_restart_tasks),
        "full_restart_weighted_work": full_restart_work,
        "state_only_rerun": 0,
        "state_only_false_verified": len(exp_affected),
        "state_only_false_closure": bool(exp_affected),
        "checkpoint_rerun": len(checkpoint_tasks),
        "checkpoint_preserved": len(set(ids) - checkpoint_tasks),
        "checkpoint_weighted_work": checkpoint_work,
        "checkpoint_correct": checkpoint_tasks == exp_affected,
        "lhos_invalidated": len(affected),
        "lhos_rerun": len(repair_attempts),
        "lhos_reexecuted_tasks": repair_attempt_tasks,
        "lhos_preserved": len(preserved),
        "lhos_frontier": len(frontier),
        "lhos_weighted_work": actual_repair_work,
        "repair_amplification_vs_affected": round(actual_repair_work / max(1, affected_work), 6),
        "under_invalidation": under_invalidation,
        "over_invalidation": over_invalidation,
        "lhos_correct": lhos_correct,
        "lhos_matches_oracle_frontier": frontier_correct,
        "ownership_conflicts": ownership_conflicts,
        "false_verified": false_verified,
        "initial_goal_closed": initial.goal_state == "closed",
        "final_goal_closed": final_goal_closed,
        "initial_attempts": initial_attempts,
        "repair_attempts": len(repair_attempts),
        "invalidation_wall_ms": invalidation_wall_ms,
        "reclose_wall_ms": reclose_wall_ms,
        "valid_trial": valid_trial,
        "preservation_ratio": round(len(preserved) / prior, 3),
        "recomputation_ratio": round(len(repair_attempts) / prior, 3),
        "checkpoint_recomputation_ratio": round(len(checkpoint_tasks) / prior, 3),
        "weighted_saving_vs_full_restart": round(
            1.0 - (actual_repair_work / max(1, full_restart_work)), 6
        ),
        "weighted_saving_vs_checkpoint": round(
            1.0 - (actual_repair_work / max(1, checkpoint_work)), 6
        ),
    }


def measure(n: int, topology: str, seed: int, target_fraction: float) -> dict[str, Any]:
    """Run one trial with a mutation root near the target affected fraction."""
    ids, edges = build_dag(n, topology, seed)
    root = select_root_for_fraction(ids, edges, target_fraction, seed)
    trial = run_trial_ids(ids, edges, root, seed, topology=topology)
    trial["target_fraction"] = target_fraction
    return trial


def measure_real_workspace() -> dict[str, Any]:
    """Run the real Shell/Workspace/Evidence recovery-repair scenario."""
    from lhos.demo.recovery_repair import run_recovery_repair

    started = time.perf_counter()
    _, _, semantics = run_recovery_repair()
    wall_ms = round((time.perf_counter() - started) * 1000, 3)
    return {
        "benchmark_id": "semantic_repair_real_workspace",
        "strategy": "longhorizonos",
        "affected_tasks": semantics.affected_tasks,
        "preserved_tasks": semantics.preserved_tasks,
        "repair_frontier": semantics.repair_frontier,
        "repair_attempts": semantics.repair_attempts,
        "new_evidence_count": semantics.new_evidence_count,
        "wall_time_ms": wall_ms,
        "final_goal_closed": semantics.final_closed,
        "full_restart_avoided": semantics.full_restart_avoided,
    }
