"""LongHorizonOS Public SDK — AgentOS facade (E1, composition root).

`AgentOS` wires a real Agent Kernel + Verified Progress Graph + D2 Scheduler +
D3 into one object so a user can Agent/Goal/run without manual wiring.  It is a
composition/lifecycle facade — NOT a new authority.  Core owns semantic state,
ownership (Kernel Lease), and repair.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import sqlite3
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lhos.agent_os.sdk.client import create_kernel
from lhos.runtimes.multi_agent import (
    AgentDescriptor,
    AgentRegistry,
    AsyncWorkerPool,
    WorkerJob,
    create_scheduler,
)
from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.graph_store import GraphStore
from lhos.runtimes.verified_progress.models import ArtifactVersionBinding, LeaseCommitGuard
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    GraphPatchProposal,
)

from .agent import Agent, _is_async_callable  # runtime import (used by open_run)
from .errors import (
    ConfigurationError,
    ExecutionError,
    SchedulingError,
    VerificationError,
)
from .goal import Goal  # runtime import (used by save_run/_serialize_goal)
from .observability import StatusView  # re-export for CLI
from .providers import (
    FactsProvider,
    KernelCapabilityProvider,
    KernelLeaseProvider,
    KernelProcessProvider,
    VPGFacade,
)
from .result import RepairOutcome, RunResult
from .status import StatusSnapshot
from .verification import VerificationOutcome


class _ClaimFenceLost(Exception):
    """Internal control flow: this execution no longer owns the task claim."""


@dataclass(frozen=True, slots=True)
class _SDKDispatchResult:
    """Operational result passed from the SDK executor to the worker pool."""

    attempt_id: str
    executor_outcome: Any = None
    dispatched: bool = True
    error: str | None = None


class _SDKExecutorDispatcher:
    """Adapt SDK Agent executors to the AsyncWorkerPool dispatcher protocol."""

    def __init__(self, os_: AgentOS, tasks: dict[str, Any]) -> None:
        self._os = os_
        self._tasks = tasks

    async def dispatch(
        self,
        *,
        agent_id: str,
        task_id: str,
        task_kind: str,
        claim_id: str,
        execution_spec: dict[str, Any],
    ) -> _SDKDispatchResult:
        del task_kind, execution_spec
        agent = self._os._agents.get(agent_id)
        if agent is None:
            raise ConfigurationError(f"scheduled agent {agent_id!r} is not registered")
        task = self._tasks.get(task_id)
        if task is None:
            raise ConfigurationError(f"scheduled task {task_id!r} is not in the Goal")
        outcome = None
        if agent.executor is not None:
            outcome = await _invoke_executor_async(agent.executor, task_id)
        return _SDKDispatchResult(
            attempt_id=f"sdk-{claim_id}",
            executor_outcome=outcome,
        )


class _SDKWorkerLifecycle:
    """Fence worker cleanup to the exact claims submitted by this SDK batch."""

    def __init__(self, scheduler: Any, jobs: list[WorkerJob]) -> None:
        self._scheduler = scheduler
        self._claim_by_task = {(job.graph_id, job.task_id): job.claim_id for job in jobs}

    def _live_expected_claim(self, graph_id: str, task_id: str) -> Any | None:
        expected = self._claim_by_task.get((graph_id, task_id))
        claim = self._scheduler.active_claim_for_task(task_id, graph_id)
        if claim is None or claim.claim_id != expected:
            return None
        return claim

    def mark_execution_started(self, claim_id: str) -> Any | None:
        claim = next(
            (
                claim
                for claim in self._scheduler.claims
                if claim.claim_id == claim_id
                and self._live_expected_claim(claim.graph_id, claim.task_id) is not None
            ),
            None,
        )
        if claim is None:
            return None
        return self._scheduler.mark_execution_started(claim_id)

    def mark_execution_operationally_succeeded(self, claim_id: str) -> Any | None:
        claim = next(
            (
                claim
                for claim in self._scheduler.claims
                if claim.claim_id == claim_id
                and self._live_expected_claim(claim.graph_id, claim.task_id) is not None
            ),
            None,
        )
        if claim is None:
            raise _ClaimFenceLost
        return self._scheduler.mark_execution_operationally_succeeded(claim_id)

    def release_task(
        self,
        graph_id: str,
        task_id: str,
        *,
        reason: str = "execution_failed",
        retry: bool = True,
    ) -> None:
        claim = self._live_expected_claim(graph_id, task_id)
        if claim is None:
            return
        release = self._scheduler.release_task
        # Keep integrations that provide a legacy SchedulerSession-shaped
        # test double working, while the built-in API always receives the
        # fencing token.  Signature inspection avoids masking real TypeErrors
        # raised by the release implementation itself.
        try:
            signature = inspect.signature(release)
            supports_fence = "expected_claim_id" in signature.parameters or any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD
                for parameter in signature.parameters.values()
            )
        except (TypeError, ValueError):
            supports_fence = True
        kwargs = {"reason": reason, "retry": retry}
        if supports_fence:
            kwargs["expected_claim_id"] = claim.claim_id
        release(graph_id, task_id, **kwargs)


class _ReadOnlyProcessProvider:
    def get(self, pid: str) -> Any | None:
        return None

    def list_all(self) -> list[Any]:
        return []

    def spawn(self, program_id: str | None = None) -> str:
        raise ConfigurationError("read-only AgentOS cannot spawn processes")

    def set_failed(self, pid: str) -> None:
        raise ConfigurationError("read-only AgentOS cannot change process state")


class _ReadOnlyLeaseProvider:
    def acquire_exclusive(self, pid: str, resource_id: str, ttl) -> Any | None:
        return None

    def release(self, lease_id: str) -> bool:
        return False

    def release_all_for_pid(self, pid: str) -> int:
        return 0

    def get(self, lease_id: str) -> Any | None:
        return None

    def list_for_resource(self, resource_id: str) -> list[Any]:
        return []

    def list_for_pid(self, pid: str) -> list[Any]:
        return []

    def reclaim_expired(self) -> int:
        return 0


class _ReadOnlyCapabilityProvider:
    def check(self, pid: str, resource: str, operation: str) -> bool:
        return False

    def capabilities_for(self, pid: str) -> list[Any]:
        return []


class AgentOS:
    """Top-level composition root for a Core-backed LongHorizonOS instance."""

    def __init__(
        self,
        db_path: str = ":memory:",
        *,
        facts: FactsProvider | None = None,
        read_only: bool = False,
    ) -> None:
        self._db_path = db_path
        self._read_only = read_only
        self._closed = False
        self._ephemeral_db_path: Path | None = None
        if read_only and db_path == ":memory:":
            raise ConfigurationError(
                "read-only AgentOS requires an existing durable database path; "
                "':memory:' has no state to observe"
            )
        storage_db_path = db_path
        if db_path == ":memory:" and not read_only:
            fd, temp_path = tempfile.mkstemp(prefix="lhos-agentos-", suffix=".sqlite3")
            os.close(fd)
            self._ephemeral_db_path = Path(temp_path)
            storage_db_path = temp_path
        try:
            self._initialize_runtime(
                storage_db_path,
                db_path=db_path,
                facts=facts,
                read_only=read_only,
            )
        except BaseException:
            # Constructor failures must not strand a temporary SQLite backing
            # file or any handles that were opened before the failure.
            with suppress(BaseException):
                self.close()
            raise

    def _initialize_runtime(
        self,
        storage_db_path: str,
        *,
        db_path: str,
        facts: FactsProvider | None,
        read_only: bool,
    ) -> None:
        self._storage_db_path = storage_db_path
        self._kernel = None if read_only else create_kernel(storage_db_path)
        self._owns_facts = facts is None
        self._facts = facts or FactsProvider(
            storage_db_path,
            read_only=read_only,
            action_service=None if self._kernel is None else self._kernel._action_service,
        )
        self._read_only_conn: sqlite3.Connection | None = None
        vpg_store: GraphStore | str = storage_db_path
        if read_only:
            resolved_db = Path(db_path).resolve()
            if not resolved_db.exists():
                raise ConfigurationError(f"read-only database not found: {resolved_db}")
            self._read_only_conn = sqlite3.connect(
                f"file:{resolved_db.as_posix()}?mode=ro",
                uri=True,
                check_same_thread=False,
            )
            vpg_store = GraphStore(self._read_only_conn, read_only=True)
        self._vpg = VerifiedProgressRuntime(
            vpg_store, facts_artifact=self._facts, facts_kernel=self._facts
        )  # type: ignore[arg-type]
        self._vpg_surface = VPGFacade(self._vpg)
        self._proc = (
            _ReadOnlyProcessProvider() if read_only else KernelProcessProvider(self._kernel)
        )
        self._lease = _ReadOnlyLeaseProvider() if read_only else KernelLeaseProvider(self._kernel)
        self._cap = (
            _ReadOnlyCapabilityProvider() if read_only else KernelCapabilityProvider(self._kernel)
        )
        self._registry = AgentRegistry()
        self._scheduler = create_scheduler(
            self._registry,
            vpg=self._vpg_surface,
            process_provider=self._proc,
            lease_provider=self._lease,
            capability_provider=self._cap,
            # ``:memory:`` AgentOS uses a temporary SQLite backing file only
            # to let the Kernel/VPG share one connection path during this
            # process.  It is not reopenable, so do not pay the durable
            # Scheduler journal/snapshot cost for that ephemeral mode.
            state_path=(None if read_only or db_path == ":memory:" else storage_db_path),
        )
        self._agents: dict[str, Agent] = {}
        self._agent_pid: dict[str, str] = {}
        self._next_artifact_version: dict[str, int] = {}
        self._goals: dict[str, Goal] = {}
        self._goal_gid: dict[str, str] = {}
        self._last_repair: RepairOutcome | None = None

    def close(self) -> None:
        """Release the kernel and VPG database handles."""
        if self._closed:
            return
        errors: list[BaseException] = []
        closers: list[tuple[str, Any]] = [
            ("scheduler", getattr(getattr(self, "_scheduler", None), "close", None)),
            ("vpg", getattr(getattr(self, "_vpg", None), "close", None)),
            (
                "facts",
                getattr(getattr(self, "_facts", None), "close", None)
                if getattr(self, "_owns_facts", False)
                else None,
            ),
            ("read_only_conn", getattr(getattr(self, "_read_only_conn", None), "close", None)),
            ("kernel", getattr(getattr(self, "_kernel", None), "close", None)),
        ]
        for name, closer in closers:
            if not callable(closer):
                continue
            try:
                closer()
            except BaseException as exc:
                errors.append(RuntimeError(f"failed to close {name}: {exc}"))

        if self._ephemeral_db_path is not None:
            for suffix in ("", "-wal", "-shm"):
                try:
                    Path(f"{self._ephemeral_db_path}{suffix}").unlink()
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    errors.append(
                        RuntimeError(
                            f"failed to remove temporary database "
                            f"{self._ephemeral_db_path}{suffix}: {exc}"
                        )
                    )
        self._closed = True
        if errors:
            raise errors[0]

    def __enter__(self) -> AgentOS:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ── agents ───────────────────────────────────────────────────────────────
    def add_agent(self, agent: Agent) -> Agent:
        if self._read_only:
            raise ConfigurationError("read-only AgentOS cannot register agents")
        pid = self._proc.spawn(agent.name)
        agent._bind_process(pid)
        caps = (
            ("shell", "filesystem", "network") if agent.capabilities is None else agent.capabilities
        )
        self._grant_capabilities(pid, caps)
        self._registry.register(
            AgentDescriptor(
                agent_id=agent.name,
                process_id=pid,
                supported_task_kinds=agent.supported_task_kinds,
                supported_tools=(
                    tuple(caps) if agent.supported_tools is None else agent.supported_tools
                ),
                specializations=tuple(sorted(agent.specializations)),
                max_concurrency=agent.max_concurrency,
                cost_weight=max(1, round(agent.cost_weight * 100)),
                resource_capacity=agent.resource_capacity,
            )
        )
        self._agents[agent.name] = agent
        self._agent_pid[agent.name] = pid
        # A durable scheduler projection may contain claims owned by a worker
        # process from an earlier AgentOS instance.  Fence that process before
        # this descriptor becomes eligible for new work.
        self._scheduler.retire_agent_process(agent.name, pid)
        self._scheduler.refresh_registry_resources()
        return agent

    def _grant_capabilities(self, pid: str, caps: tuple[str, ...]) -> None:
        import contextlib

        from .providers import make_capability

        if self._kernel is None:
            raise ConfigurationError("read-only AgentOS cannot grant capabilities")
        for pat in caps:
            with contextlib.suppress(Exception):
                self._kernel._capability_service.grant(
                    pid, make_capability(pat, ("read", "write", "execute"))
                )

    # ── goals ────────────────────────────────────────────────────────────────
    def goal(self, goal_id: str, *, tasks: tuple = ()) -> Goal:
        from .goal import Goal

        g = Goal(goal_id, tasks=tasks)
        self._goals[goal_id] = g
        return g

    def _gid_for(self, goal_id: str, compile_if_missing: bool = False) -> str | None:
        # compiled Goal -> graph_id map is kept in _goal_gid
        gid = self._goal_gid.get(goal_id)
        if gid is None and compile_if_missing:
            g = self._goals.get(goal_id)
            if g is not None:
                gid = self._compile_goal(g)
        return gid

    def _compile_goal(self, goal: Goal) -> str:
        """Compile a Goal + Tasks into a real VPG patch; returns graph_id."""
        self._goals.setdefault(goal.goal_id, goal)
        pid = self._owner_pid()
        gid = self._vpg.create_graph(owner_pid=pid).graph_id
        ops: list[Any] = [
            AddNodeOp(
                node_id=goal.goal_id,
                graph_id=gid,
                node_type="goal",
                created_by_pid=pid,
                title=goal.goal_id,
            )
        ]
        for t in goal.tasks:
            metadata = dict(t.metadata)
            scheduler_metadata = dict(metadata.get("scheduler", {}))
            scheduler_metadata.update(
                {
                    "task_kind": t.task_kind,
                    "required_specializations": list(t.required_specializations),
                    "required_tools": list(t.required_tools),
                    "max_attempts": t.max_attempts,
                    "resources": t.resources.model_dump(mode="json"),
                }
            )
            metadata["scheduler"] = scheduler_metadata
            sdk_metadata = dict(metadata.get("sdk", {}))
            sdk_metadata["agent"] = t.agent
            metadata["sdk"] = sdk_metadata
            ops.append(
                AddNodeOp(
                    node_id=t.task_id,
                    graph_id=gid,
                    node_type="task",
                    created_by_pid=pid,
                    task_kind=t.task_kind,
                    metadata=metadata,
                ),
            )
            ops.append(
                AddEdgeOp(
                    edge_type="depends_on",
                    source_node_id=goal.goal_id,
                    target_node_id=t.task_id,
                    created_by_pid=pid,
                )
            )
            for dep in t.depends_on:
                ops.append(
                    AddEdgeOp(
                        edge_type="depends_on",
                        source_node_id=t.task_id,
                        target_node_id=dep.task_id,
                        created_by_pid=pid,
                    )
                )
        self._submit_compiled_ops(gid, pid, goal.goal_id, ops)
        self._goal_gid[goal.goal_id] = gid
        return gid

    def _submit_compiled_ops(self, gid: str, pid: str, goal_id: str, ops: list[Any]) -> None:
        """Commit compiled goal ops, batching so a large goal stays constructible.

        MAX_PATCH_OPS bounds a single proposal.  That bound exists to stop an
        *agent* from submitting an unbounded patch; goal compilation is a trusted
        path, so it batches rather than failing.  AddEdgeOp requires both
        endpoints to already exist, so we split on a boundary that keeps every
        edge behind its nodes: all node ops first, then all edge ops.
        """
        from lhos.runtimes.verified_progress.patch_validator import MAX_PATCH_OPS

        if len(ops) <= MAX_PATCH_OPS:
            self._vpg.submit_patch(
                GraphPatchProposal(
                    graph_id=gid,
                    expected_graph_version=self._vpg.get_graph(gid).current_version,
                    author_pid=pid,
                    idempotency_key=f"compile-{goal_id}",
                    operations=tuple(ops),
                )
            )
            return

        node_ops = [o for o in ops if isinstance(o, AddNodeOp)]
        edge_ops = [o for o in ops if not isinstance(o, AddNodeOp)]
        batches = [
            group[i : i + MAX_PATCH_OPS]
            for group in (node_ops, edge_ops)
            for i in range(0, len(group), MAX_PATCH_OPS)
        ]
        for index, chunk in enumerate(batches):
            self._vpg.submit_patch(
                GraphPatchProposal(
                    graph_id=gid,
                    expected_graph_version=self._vpg.get_graph(gid).current_version,
                    author_pid=pid,
                    idempotency_key=f"compile-{goal_id}-{index}",
                    operations=tuple(chunk),
                )
            )

    def _owner_pid(self) -> str:
        if self._agents:
            return next(iter(self._agent_pid.values()))
        return self._proc.spawn("sdk-root")

    # ── run ──────────────────────────────────────────────────────────────────
    def run(self, goal: Goal, *, max_dispatches: int = 8, max_steps: int = 20) -> RunResult:
        if self._read_only:
            raise ExecutionError("read-only AgentOS cannot execute goals")
        self._goals.setdefault(goal.goal_id, goal)
        gid = self._gid_for(goal.goal_id, compile_if_missing=True)
        if gid is None:
            raise ConfigurationError(f"goal {goal.goal_id!r} not registered")
        dispatched = 0
        steps = 0
        while dispatched < max_dispatches and steps < max_steps:
            res = None
            try:
                # Never acquire more claims than this invocation can execute.
                # Otherwise the unexecuted tail remains ACTIVE and is skipped by
                # every later scheduling pass.
                remaining = max_dispatches - dispatched
                res = self._scheduler.run_pass(gid, max_claims=remaining)
            except Exception as e:  # surface scheduler errors
                raise SchedulingError("scheduler pass failed", cause=e) from e
            if not res.dispatched:
                break
            async_agent_ids = sorted(
                {
                    d["agent_id"]
                    for d in res.dispatched
                    if (agent := self._agents.get(d["agent_id"])) is not None
                    and agent.executor_is_async
                }
            )
            if async_agent_ids:
                self._release_unexecuted_dispatches(
                    gid,
                    res.dispatched,
                    reason="async_executor_requires_run_async",
                )
                names = ", ".join(async_agent_ids)
                raise ConfigurationError(
                    f"Scheduled Agent executor is asynchronous ({names}); "
                    "use `await AgentOS.run_async(...)`"
                )
            for index, d in enumerate(res.dispatched):
                task_id = d["task_id"]
                agent_id = d["agent_id"]
                claim = self._scheduler.active_claim_for_task(task_id, gid)
                attempt_number = getattr(claim, "attempt_number", 0)
                try:
                    self._execute_and_verify(
                        gid,
                        task_id,
                        agent_id,
                        goal,
                        claim_id=d.get("claim_id", ""),
                        attempt_number=attempt_number,
                    )
                except ConfigurationError:
                    self._release_unexecuted_dispatches(
                        gid,
                        res.dispatched[index + 1 :],
                        reason="executor_configuration_error",
                    )
                    raise
                dispatched += 1
                if dispatched >= max_dispatches:
                    break
            steps += 1
            if steps >= max_steps:
                break
        return self.result(gid)

    async def run_async(
        self,
        goal: Goal,
        *,
        max_dispatches: int = 8,
        max_steps: int = 20,
        max_concurrency: int = 4,
    ) -> RunResult:
        """Schedule and execute ready tasks concurrently.

        Scheduler claims and Kernel leases remain the ownership authority.
        Agent executors overlap under the global and per-Agent concurrency
        bounds.  Independent synchronous verifiers run after operational
        success; Facts/Evidence/VPG commits are then serialized to avoid
        graph-version races while preserving executor concurrency.
        """
        if self._read_only:
            raise ExecutionError("read-only AgentOS cannot execute goals")
        if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
            raise ConfigurationError("max_concurrency must be an integer")
        if max_concurrency < 1:
            raise ConfigurationError("max_concurrency must be >= 1")
        if isinstance(max_dispatches, bool) or not isinstance(max_dispatches, int):
            raise ConfigurationError("max_dispatches must be an integer")
        if isinstance(max_steps, bool) or not isinstance(max_steps, int):
            raise ConfigurationError("max_steps must be an integer")
        if max_dispatches < 0 or max_steps < 0:
            raise ConfigurationError("max_dispatches and max_steps must be >= 0")

        self._goals.setdefault(goal.goal_id, goal)
        gid = self._gid_for(goal.goal_id, compile_if_missing=True)
        if gid is None:
            raise ConfigurationError(f"goal {goal.goal_id!r} not registered")

        tasks_by_id = {task.task_id: task for task in goal.tasks}
        semantic_commit_lock = asyncio.Lock()
        failures: list[str] = []
        fatal_errors: list[VerificationError] = []
        dispatched = 0
        steps = 0

        while dispatched < max_dispatches and steps < max_steps:
            remaining = max_dispatches - dispatched
            batch_limit = min(remaining, max_concurrency)
            try:
                schedule_result = self._scheduler.run_pass(
                    gid,
                    max_claims=batch_limit,
                )
            except Exception as exc:
                raise SchedulingError("scheduler pass failed", cause=exc) from exc
            if not schedule_result.dispatched:
                break

            jobs: list[WorkerJob] = []
            attempt_by_claim: dict[str, int] = {}
            for dispatch in schedule_result.dispatched:
                task_id = dispatch["task_id"]
                agent_id = dispatch["agent_id"]
                claim_id = dispatch.get("claim_id", "")
                claim = self._scheduler.active_claim_for_task(task_id, gid)
                if claim is None or claim.claim_id != claim_id:
                    continue
                task = tasks_by_id.get(task_id)
                jobs.append(
                    WorkerJob(
                        graph_id=gid,
                        task_id=task_id,
                        claim_id=claim_id,
                        agent_id=agent_id,
                        task_kind="" if task is None else task.task_kind,
                    )
                )
                attempt_by_claim[claim_id] = int(getattr(claim, "attempt_number", 0))
            if not jobs:
                break

            lifecycle = _SDKWorkerLifecycle(self._scheduler, jobs)

            async def verify_and_commit(
                job: WorkerJob,
                dispatch_result: Any,
                lifecycle: _SDKWorkerLifecycle = lifecycle,
                attempt_by_claim: dict[str, int] = attempt_by_claim,
            ) -> None:
                task = tasks_by_id.get(job.task_id)
                agent = self._agents.get(job.agent_id)
                if task is None:
                    lifecycle.release_task(
                        gid,
                        job.task_id,
                        reason="missing_task",
                    )
                    failures.append(f"{job.task_id}: missing_task")
                    return
                if agent is None:
                    lifecycle.release_task(
                        gid,
                        job.task_id,
                        reason="missing_agent",
                    )
                    failures.append(f"{job.task_id}: missing_agent")
                    return

                executor_outcome = getattr(dispatch_result, "executor_outcome", None)
                try:
                    if task.verify is not None:
                        outcome = await _invoke_verifier_async(task.verify)
                    else:
                        outcome = executor_outcome
                except Exception as exc:
                    lifecycle.release_task(
                        gid,
                        job.task_id,
                        reason=f"verifier_failed:{type(exc).__name__}",
                    )
                    failures.append(f"{job.task_id}: verifier_failed:{type(exc).__name__}")
                    return

                if not isinstance(outcome, VerificationOutcome):
                    lifecycle.release_task(
                        gid,
                        job.task_id,
                        reason="missing_verifier"
                        if outcome is None
                        else "invalid_verifier_outcome",
                    )
                    reason = "missing_verifier" if outcome is None else "invalid_verifier_outcome"
                    failures.append(f"{job.task_id}: {reason}")
                    return
                if not outcome.passed:
                    lifecycle.release_task(
                        gid,
                        job.task_id,
                        reason="verification_failed",
                    )
                    failures.append(f"{job.task_id}: verification_failed")
                    return

                async with semantic_commit_lock:
                    try:
                        committed = self._commit_verified_outcome(
                            gid,
                            job.task_id,
                            job.agent_id,
                            outcome,
                            attempt_number=attempt_by_claim.get(job.claim_id, 0),
                            claim_id=job.claim_id,
                        )
                        if not committed:
                            failures.append(f"{job.task_id}: stale_verifier_outcome")
                    except _ClaimFenceLost:
                        failures.append(f"{job.task_id}: claim_fence_lost")
                        return
                    except Exception as exc:
                        lifecycle.release_task(
                            gid,
                            job.task_id,
                            reason=f"evidence_attachment_failed:{type(exc).__name__}",
                        )
                        error = VerificationError(
                            f"failed to attach Evidence for task {job.task_id!r}",
                            cause=exc,
                        )
                        fatal_errors.append(error)
                        raise error from exc

            pool = AsyncWorkerPool(
                _SDKExecutorDispatcher(self, tasks_by_id),
                scheduler=lifecycle,
                max_concurrency=max_concurrency,
                agent_concurrency={
                    agent_id: max(1, self._agents[agent_id].max_concurrency)
                    for agent_id in sorted({job.agent_id for job in jobs})
                    if agent_id in self._agents
                },
                on_success=verify_and_commit,
            )
            try:
                outcomes = await pool.run(jobs)
            except asyncio.CancelledError:
                for job in jobs:
                    lifecycle.release_task(
                        job.graph_id,
                        job.task_id,
                        reason="worker_cancelled",
                    )
                with suppress(Exception):
                    self._scheduler.reconcile()
                raise

            dispatched += len(jobs)
            steps += 1
            for outcome in outcomes:
                if not outcome.ok:
                    failures.append(f"{outcome.task_id}: {outcome.error or outcome.status.value}")
            if fatal_errors:
                raise fatal_errors[0]

        result = self.result(gid)
        result.failures.extend(failures)
        result.meta.update(
            {
                "execution_mode": "async",
                "dispatched": dispatched,
                "max_concurrency": max_concurrency,
            }
        )
        return result

    def _release_unexecuted_dispatches(
        self,
        gid: str,
        dispatches: list[dict[str, Any]],
        *,
        reason: str,
    ) -> None:
        """Release only the exact still-live claims in an abandoned sync batch."""
        for dispatch in dispatches:
            task_id = dispatch["task_id"]
            claim_id = dispatch.get("claim_id", "")
            if claim_id:
                self._release_after_failure(
                    gid,
                    task_id,
                    claim_id,
                    reason=reason,
                )

    def _execute_and_verify(
        self,
        gid: str,
        task_id: str,
        agent_id: str,
        goal: Goal,
        *,
        claim_id: str = "",
        attempt_number: int = 0,
    ) -> None:
        """Execute a dispatched task and independently attach verified Evidence.

        A task with no verifier/executor stays unverified (VPG-G2/G3): without
        a Verification->Evidence path the SDK must NOT fabricate VERIFIED.
        """
        task = next((t for t in goal.tasks if t.task_id == task_id), None)
        agent = self._agents.get(agent_id)
        claim = self._live_claim(
            gid,
            task_id,
            claim_id=claim_id,
            agent_id=agent_id,
        )
        if claim is None:
            # A late/stale dispatch must not execute and, importantly, must not
            # release a newer owner's claim.
            return
        claim_id = claim.claim_id

        if task is None:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason="missing_task",
            )
            return
        if agent is None:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason="missing_agent",
            )
            return

        try:
            self._mark_execution_started(claim)
        except Exception as e:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason=f"execution_start_failed:{type(e).__name__}",
            )
            return

        executor_outcome: Any = None
        if agent.executor is not None:
            try:
                executor_outcome = _invoke_executor(agent.executor, task_id)
            except ConfigurationError:
                self._release_after_failure(
                    gid,
                    task_id,
                    claim_id,
                    reason="executor_configuration_error",
                )
                raise
            except Exception as e:
                self._release_after_failure(
                    gid,
                    task_id,
                    claim_id,
                    reason=f"executor_failed:{type(e).__name__}",
                )
                return

        # Task.verify is the independent semantic authority when present.
        # With no Agent.executor, retaining Task.verify as the combined legacy
        # executor/verifier preserves the existing SDK API.  An executor that
        # directly returns VerificationOutcome is also accepted when no
        # separate verifier was supplied, matching Agent's documented API.
        try:
            if task.verify is not None:
                outcome = _invoke_verifier_sync(task.verify)
            elif isinstance(executor_outcome, VerificationOutcome):
                outcome = executor_outcome
            else:
                self._release_after_failure(
                    gid,
                    task_id,
                    claim_id,
                    reason="missing_verifier",
                )
                return
        except ConfigurationError:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason="verifier_configuration_error",
            )
            raise
        except Exception as e:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason=f"verifier_failed:{type(e).__name__}",
            )
            return

        if not isinstance(outcome, VerificationOutcome):
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason="invalid_verifier_outcome",
            )
            return
        if not outcome.passed:
            # FAIL/INCONCLUSIVE => no VERIFIED; record failure
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason="verification_failed",
            )
            return

        # Fence the result again after arbitrary user code ran.  A result from
        # an expired/lost/reassigned claim must never enter Facts or the VPG.
        if (
            self._live_claim(
                gid,
                task_id,
                claim_id=claim_id,
                agent_id=agent_id,
            )
            is None
        ):
            return

        try:
            committed = self._commit_verified_outcome(
                gid,
                task_id,
                agent_id,
                outcome,
                attempt_number=attempt_number,
                claim_id=claim_id,
            )
            if not committed:
                return
        except _ClaimFenceLost:
            # Ownership was lost while user/fact-store code was running. This
            # stale result must not release or otherwise mutate a newer claim.
            return
        except Exception as e:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason=f"evidence_attachment_failed:{type(e).__name__}",
            )
            raise VerificationError(
                f"failed to attach Evidence for task {task_id!r}",
                cause=e,
            ) from e

    def _commit_verified_outcome(
        self,
        gid: str,
        task_id: str,
        agent_id: str,
        outcome: VerificationOutcome,
        *,
        attempt_number: int,
        claim_id: str,
    ) -> bool:
        """Commit one PASS outcome and let VPG observation complete its claim."""
        latest_version = self._facts.latest(outcome.artifact_id)
        if latest_version is not None and latest_version > outcome.version:
            self._release_after_failure(
                gid,
                task_id,
                claim_id,
                reason=(
                    "stale_verifier_outcome:"
                    f"{outcome.artifact_id}@{outcome.version}"
                    f"<latest@{latest_version}"
                ),
            )
            return False
        produced_pid = self._agent_pid.get(agent_id) or self._owner_pid()
        if latest_version is None or latest_version < outcome.version:
            self._require_live_claim(
                gid,
                task_id,
                claim_id=claim_id,
                agent_id=agent_id,
            )
            self._facts.add_version(
                outcome.artifact_id,
                outcome.version,
                outcome.content or "",
            )
            self._require_live_claim(
                gid,
                task_id,
                claim_id=claim_id,
                agent_id=agent_id,
            )
        self._attach_evidence(
            gid,
            task_id,
            agent_id,
            outcome,
            produced_pid,
            attempt_number=attempt_number,
            claim_id=claim_id,
        )
        self._scheduler.observe_vpg(gid)
        return True

    def _mark_execution_started(self, claim: Any) -> None:
        """Move the matching Attempt to RUNNING before invoking user code."""
        core = getattr(self._scheduler, "_s", None)
        start = getattr(core, "mark_execution_started", None)
        if callable(start):
            start(claim)
            return
        # Backward-compatible fallback while older scheduler cores lack the
        # public transition helper.
        attempt = self._scheduler.attempt_for_claim(claim.claim_id)
        manager = getattr(core, "_attempts_", None)
        mark_running = getattr(manager, "mark_running", None)
        if attempt is not None and callable(mark_running):
            mark_running(attempt)

    def _live_claim(
        self,
        gid: str,
        task_id: str,
        *,
        claim_id: str,
        agent_id: str,
    ) -> Any | None:
        """Return the exact currently-owned claim only while its lease is live."""
        claim = self._scheduler.active_claim_for_task(task_id, gid)
        if claim is None:
            return None
        if claim_id and claim.claim_id != claim_id:
            return None
        if claim.agent_id != agent_id:
            return None
        if getattr(getattr(claim, "state", None), "value", None) != "active":
            return None
        lease_id = getattr(claim, "lease_id", None)
        if not lease_id:
            return None

        core = getattr(self._scheduler, "_s", None)
        leases = getattr(core, "_leases", None)
        get_lease = getattr(leases, "get", None)
        is_active = getattr(leases, "is_lease_active", None)
        if not callable(get_lease) or not callable(is_active):
            return None
        try:
            lease = get_lease(lease_id)
        except Exception:
            return None
        if not is_active(lease):
            return None
        if getattr(lease, "owner_pid", claim.process_id) != claim.process_id:
            return None
        if getattr(lease, "resource_id", claim.lease_resource) != claim.lease_resource:
            return None
        if getattr(claim, "lease_owner_pid", None) != getattr(lease, "owner_pid", None):
            return None
        if getattr(claim, "lease_fencing_token", None) != getattr(lease, "fencing_token", None):
            return None
        return claim

    def _require_live_claim(
        self,
        gid: str,
        task_id: str,
        *,
        claim_id: str,
        agent_id: str,
    ) -> Any:
        """Return the exact live claim or stop a stale execution commit."""
        claim = self._live_claim(
            gid,
            task_id,
            claim_id=claim_id,
            agent_id=agent_id,
        )
        if claim is None:
            raise _ClaimFenceLost
        return claim

    def _claim_commit_guard(
        self,
        gid: str,
        task_id: str,
        *,
        claim_id: str,
        agent_id: str,
    ) -> LeaseCommitGuard:
        """Freeze the exact live claim lease generation for GraphStore CAS."""

        claim = self._require_live_claim(
            gid,
            task_id,
            claim_id=claim_id,
            agent_id=agent_id,
        )
        if not claim.lease_id or not claim.lease_owner_pid or claim.lease_fencing_token is None:
            raise _ClaimFenceLost
        core = getattr(self._scheduler, "_s", None)
        leases = getattr(core, "_leases", None)
        get_lease = getattr(leases, "get", None)
        is_active = getattr(leases, "is_lease_active", None)
        if not callable(get_lease) or not callable(is_active):
            raise _ClaimFenceLost
        lease = get_lease(claim.lease_id)
        if (
            lease is None
            or not is_active(lease)
            or getattr(lease, "resource_id", None) != claim.lease_resource
            or getattr(lease, "owner_pid", None) != claim.lease_owner_pid
            or getattr(lease, "fencing_token", None) != claim.lease_fencing_token
            or getattr(lease, "expires_at", None) is None
        ):
            raise _ClaimFenceLost
        return LeaseCommitGuard(
            lease_id=claim.lease_id,
            resource_id=claim.lease_resource,
            owner_pid=claim.lease_owner_pid,
            fencing_token=claim.lease_fencing_token,
            expires_at=lease.expires_at,
        )

    def _release_after_failure(
        self,
        gid: str,
        task_id: str,
        claim_id: str,
        *,
        reason: str,
    ) -> None:
        """Release only this execution's claim; never clobber a reassignment."""
        current = self._scheduler.active_claim_for_task(task_id, gid)
        if current is None or current.claim_id != claim_id:
            return
        try:
            release = self._scheduler.release_task
            try:
                signature = inspect.signature(release)
                supports_fence = "expected_claim_id" in signature.parameters or any(
                    parameter.kind == inspect.Parameter.VAR_KEYWORD
                    for parameter in signature.parameters.values()
                )
            except (TypeError, ValueError):
                supports_fence = True
            kwargs: dict[str, Any] = {"reason": reason, "retry": True}
            if supports_fence:
                kwargs["expected_claim_id"] = claim_id
            release(gid, task_id, **kwargs)
        except Exception:
            # Best-effort reconciliation is safer than allowing a cleanup
            # exception to hide the executor/verifier/evidence root cause.
            with suppress(Exception):
                self._scheduler.reconcile()

    def _attach_evidence(
        self,
        gid: str,
        task_id: str,
        agent_id: str,
        outcome: VerificationOutcome,
        pid: str,
        *,
        attempt_number: int = 0,
        claim_id: str = "",
    ) -> None:
        def fence() -> None:
            if claim_id:
                self._require_live_claim(
                    gid,
                    task_id,
                    claim_id=claim_id,
                    agent_id=agent_id,
                )

        suffix = "" if attempt_number == 0 else f"-a{attempt_number}"
        # Graph-local task ids are intentionally reusable across independent
        # Goals.  Every durable identity emitted by the SDK must therefore
        # include the graph id; otherwise a task named ``build`` in graph B can
        # reuse graph A's Kernel Action or overwrite A's VPG projection rows.
        # The attempt suffix keeps retries distinct while remaining
        # deterministic/idempotent for replay of the same attempt.
        identity = f"{gid}-{task_id}-{outcome.version}{suffix}"
        vid = f"V-{identity}"
        evid = f"E-{identity}"
        artref_id = f"AR-{identity}"
        action_id = f"sdk-act-{identity}"
        fence()
        self._facts.commit_action(action_id, pid=pid)
        fence()
        binding = ArtifactVersionBinding(
            canonical_uri=f"vpg://{outcome.artifact_id}",
            artifact_id=outcome.artifact_id,
            version=outcome.version,
            content_hash=self._facts.read_hash(pid, outcome.artifact_id, outcome.version) or "",
        )
        cur = self._vpg.get_graph(gid).current_version
        # Artifact pin + Verification + Evidence + all supporting edges are a
        # single atomic graph transition.  The validator processes operations
        # in order against one candidate projection, so newly-added nodes can
        # safely be referenced by later edge operations in the same proposal.
        fence()
        commit_guard = (
            self._claim_commit_guard(
                gid,
                task_id,
                claim_id=claim_id,
                agent_id=agent_id,
            )
            if claim_id
            else None
        )
        try:
            self._vpg.submit_patch(
                GraphPatchProposal(
                    graph_id=gid,
                    expected_graph_version=cur,
                    author_pid=pid,
                    idempotency_key=f"evidence-{identity}",
                    operations=(
                        AddNodeOp(
                            node_id=artref_id,
                            graph_id=gid,
                            node_type="artifact_ref",
                            created_by_pid=pid,
                            canonical_uri=f"vpg://{outcome.artifact_id}",
                            artifact_id=outcome.artifact_id,
                            version=outcome.version,
                            content_hash=self._facts.read_hash(
                                pid, outcome.artifact_id, outcome.version
                            )
                            or "",
                            metadata={"scheduler": {"task_kind": task_id}},
                        ),
                        AddNodeOp(
                            node_id=vid,
                            graph_id=gid,
                            node_type="verification",
                            created_by_pid=pid,
                            verification_kind="command_result",
                            obligation={"kind": "produced_artifact"},
                            source_action_id=action_id,
                            metadata={"scheduler": {"task_kind": task_id}},
                        ),
                        AddNodeOp(
                            node_id=evid,
                            graph_id=gid,
                            node_type="evidence",
                            created_by_pid=pid,
                            evidence_kind="command_result",
                            result="pass",
                            source_verification_id=vid,
                            evidence_source_action_id=action_id,
                            artifact_bindings=(binding,),
                            produced_by_pid=pid,
                        ),
                        AddEdgeOp(
                            edge_type="produces",
                            source_node_id=task_id,
                            target_node_id=artref_id,
                            created_by_pid=pid,
                        ),
                        AddEdgeOp(
                            edge_type="verifies",
                            source_node_id=vid,
                            target_node_id=task_id,
                            created_by_pid=pid,
                        ),
                        AddEdgeOp(
                            edge_type="produces",
                            source_node_id=vid,
                            target_node_id=evid,
                            created_by_pid=pid,
                        ),
                    ),
                ),
                _commit_guard=commit_guard,
            )
        except VPGError as exc:
            if exc.code == VPGCode.LEASE_FENCE_LOST:
                raise _ClaimFenceLost from exc
            raise

    # ── result / status ──────────────────────────────────────────────────────
    def result(self, gid: str) -> RunResult:
        nodes, _ = self._vpg.snapshot_projection(gid)
        tasks = {n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "task"}
        task_states = {tid: n.validity.value for tid, n in tasks.items()}
        verified = [t for t, s in task_states.items() if s == "verified"]
        stale = [t for t, s in task_states.items() if s == "stale"]
        ready = [candidate.task_id for candidate in self._vpg.query_ready_frontier(gid)]
        goal_nodes = {n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "goal"}
        goal_node = next(iter(goal_nodes.values()), None)
        owner = {}
        for claim in self._scheduler.claims:
            if claim.graph_id != gid:
                continue
            state = getattr(claim.state, "value", claim.state)
            if state in {"active", "acquiring"} or claim.task_id not in owner:
                owner[claim.task_id] = claim.agent_id
        artifacts = {}
        for aid, vs in self._facts.versions().items():
            for ver in vs:
                artifacts[aid] = (
                    ver,
                    self._facts.read_hash("sdk-observer", aid, ver) or "",
                )
        return RunResult(
            goal_id=gid,
            goal_state=(
                "closed"
                if goal_node is not None
                and getattr(goal_node.lifecycle, "value", goal_node.lifecycle) == "closed"
                else "open"
            ),
            task_states=task_states,
            verified=verified,
            stale=stale,
            ready=ready,
            owner_by_task=owner,
            artifacts=artifacts,
        )

    def status(self, goal: Goal) -> StatusSnapshot:
        self._goals.setdefault(goal.goal_id, goal)
        gid = self._gid_for(goal.goal_id)
        if gid is None:
            gid = self._compile_goal(goal)
        r = self.result(gid)
        return StatusSnapshot(
            goal_id=goal.goal_id,
            version=self._vpg.get_graph(gid).current_version,
            tasks=r.task_states,
            verified=r.verified,
            stale=r.stale,
            ready=r.ready,
            unverified=[t for t, s in r.task_states.items() if s == "unverified"],
            goal_closed=(r.goal_state == "closed"),
            owner_by_task=r.owner_by_task,
        )

    # ── E3 observability (read-only) ────────────────────────────────────────
    def status_view(self, goal_id: str) -> StatusView:
        from .observability_service import build_status_view

        return build_status_view(self, goal_id)

    def explain(self, goal_id: str, task_id: str) -> list[str]:
        sv = self.status_view(goal_id)
        tv = sv.tasks.get(task_id, {})
        lines = []
        if not tv:
            return [f"task {task_id!r} not found"]
        lines.append(f"Task {task_id}: {tv.get('validity', '?').upper()}")
        if tv.get("validity") == "verified":
            if tv.get("supporting_evidence"):
                lines.append(
                    f"  VERIFIED because: Evidence {tv['supporting_evidence']} PASS "
                    f"binds {tv.get('artifact')}@{tv.get('artifact_version')} and is current"
                )
            else:
                lines.append(
                    "  VERIFIED because: required dependencies valid + applicable Evidence exists"
                )
        elif tv.get("validity") == "stale":
            lines.append(
                f"  STALE because: {tv.get('artifact', '?')} changed version; "
                f"old Evidence not current for the new artifact version"
            )
        elif tv.get("validity") == "unverified":
            lines.append("  UNVERIFIED because: no applicable Evidence yet")
        return lines

    def graph_lines(self, goal_id: str) -> list[str]:
        sv = self.status_view(goal_id)
        gid = self._gid_for(goal_id)
        if gid is None:
            return []
        _, edges = self.vpg.snapshot_projection(gid)
        # goal -> task deps (depends_on) rendered as an indented tree
        deps: dict[str, list[str]] = {}
        roots = []
        for e in edges:
            if e.edge_type.value == "depends_on":
                deps.setdefault(e.source_node_id, []).append(e.target_node_id)
                if e.source_node_id == goal_id:
                    roots.append(e.target_node_id)
        lines = [f"Goal: {goal_id} [{sv.goal_state}]"]
        seen: set[str] = set()
        for root in sorted(roots):
            _render_tree(root, deps, sv, lines, seen, prefix="", is_last=True)
        return lines

    # ── repair (D3) ────────────────────────────────────────────────────────
    def repair(
        self, goal: Goal, *, new_artifact_version: int | None = None, artifact_id: str | None = None
    ) -> RepairOutcome:
        if self._read_only:
            raise ExecutionError("read-only AgentOS cannot mutate or repair graphs")
        """Run D3 invalidation on a goal and return affected/preserved/frontier.

        If `new_artifact_version` and `artifact_id` are given, the SDK first
        records the new ArtifactVersion (so a subsequent `run` re-verifies with
        the new Evidence).  The D3 cone marks only affected semantic descendants
        STALE and derives the minimal Repair Frontier.
        """
        self._goals.setdefault(goal.goal_id, goal)
        gid = self._gid_for(goal.goal_id, compile_if_missing=True)
        if gid is None:
            raise ConfigurationError(f"goal {goal.goal_id!r} not registered")
        from lhos.runtimes.invalidation.engine import (
            EngineInputs,
            build_invalidation_result,
            run_invalidation_engine,
        )
        from lhos.runtimes.invalidation.models import InvalidationCause

        # Snapshot semantic bindings before mutating Artifact facts.  Repair
        # must be causally grounded in an existing ArtifactRef; never invent a
        # seed task or create an orphan artifact for a typo.
        nodes, edges = self._vpg.snapshot_projection(gid)
        task_nodes = {n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "task"}
        artifact_refs_by_task: dict[str, list[Any]] = {}
        for edge in edges:
            if edge.edge_type.value != "produces" or edge.source_node_id not in task_nodes:
                continue
            ref = nodes.get(edge.target_node_id)
            if getattr(ref, "node_type", "") == "artifact_ref":
                artifact_refs_by_task.setdefault(edge.source_node_id, []).append(ref)

        artifact_ids = sorted(
            {
                str(ref.artifact_id)
                for refs in artifact_refs_by_task.values()
                for ref in refs
                if getattr(ref, "artifact_id", None)
            }
        )
        if artifact_id is None:
            if new_artifact_version is None or len(artifact_ids) != 1:
                raise ConfigurationError(
                    "repair requires an explicit artifact_id and new_artifact_version "
                    "when the invalidation cause is not uniquely identifiable"
                )
            artifact_id = artifact_ids[0]

        matching_refs = [
            ref
            for refs in artifact_refs_by_task.values()
            for ref in refs
            if getattr(ref, "artifact_id", None) == artifact_id
        ]
        if not matching_refs:
            raise ConfigurationError(
                f"cannot repair unknown or unreferenced artifact {artifact_id!r}"
            )

        cur = self._facts.latest(artifact_id) or 0
        if new_artifact_version is not None and new_artifact_version < cur:
            raise ConfigurationError(
                f"cannot repair {artifact_id!r} at stale version "
                f"{new_artifact_version}; current version is {cur}"
            )

        # If the caller omits a version, only an already-recorded newer fact is
        # unambiguous.  Otherwise fail closed rather than guessing a mutation.
        if new_artifact_version is None:
            newver = cur
            if newver <= max(int(getattr(ref, "version", 0)) for ref in matching_refs):
                raise ConfigurationError(f"no newer version recorded for artifact {artifact_id!r}")
        else:
            newver = new_artifact_version

        causes: list[InvalidationCause] = []
        for tid in sorted(task_nodes):
            refs = [
                ref
                for ref in artifact_refs_by_task.get(tid, [])
                if getattr(ref, "artifact_id", None) == artifact_id
            ]
            if not refs:
                continue
            # A task may have historical refs from multiple repairs.  The
            # highest binding strictly below the new version is the immediate
            # predecessor and therefore the auditable old_version.
            predecessor_versions = [
                int(getattr(ref, "version", 0))
                for ref in refs
                if int(getattr(ref, "version", 0)) < newver
            ]
            if not predecessor_versions:
                continue
            oldver = max(predecessor_versions)
            causes.append(
                InvalidationCause(
                    cause_id=f"c:{artifact_id}:{tid}:v{oldver}-v{newver}",
                    graph_id=gid,
                    graph_version=self._vpg.get_graph(gid).current_version,
                    cause_type="ARTIFACT_VERSION_SUPERSEDED",
                    source_node_id=tid,
                    artifact_id=artifact_id,
                    old_version=oldver,
                    new_version=newver,
                    reason=f"{artifact_id} version {oldver} superseded by {newver}",
                )
            )
        if not causes:
            raise ConfigurationError(
                f"artifact {artifact_id!r} has no prior graph binding below version {newver}"
            )

        # Register a requested new fact only after the semantic cause has been
        # validated.  Equal-to-current means the external mutation was already
        # registered and must not create a duplicate version.
        if new_artifact_version is not None and new_artifact_version > cur:
            self._facts.add_version(
                artifact_id,
                new_artifact_version,
                f"body-v{new_artifact_version}",
            )

        curver = self._vpg.get_graph(gid).current_version
        inp = EngineInputs(
            graph_id=gid,
            current_version=curver,
            task_nodes=task_nodes,
            goal_nodes={},
            evidence_nodes={
                n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "evidence"
            },
            edges=edges,
            explicit_causes=tuple(causes),
        )
        er = run_invalidation_engine(inp)
        ir = build_invalidation_result(inp, er)
        outcome = RepairOutcome(
            affected=list(ir.stale_nodes),
            preserved=list(ir.preserved_nodes),
            frontier=[c.task_id for c in ir.frontier.candidates],
            causes=[c.reason for c in ir.causes],
            cause_details=[c.model_dump(mode="json") for c in ir.causes],
        )
        self._vpg.refresh_derived_state(
            gid,
            author_pid=self._owner_pid(),
            reason="D3 artifact/invalidation refresh",
        )
        self._last_repair = outcome
        return outcome

    def clear_repair(self) -> None:
        """Clear the last D3 repair overlay (used after reclosure)."""
        self._last_repair = None

    # ── real workspace <-> ArtifactVersion bridge (E2, §15/§16) ─────────────
    def register_workspace_artifact(self, workspace, rel: str, version: int) -> str:
        """Register a real workspace file's current bytes as the exact
        ArtifactVersion.  Returns the artifact_id.  (The physical file and the
        Artifact FS authority are kept consistent; the version identity is the
        file content hash + the caller-selected version.)"""
        content = workspace.byte_content(rel)
        self._facts.add_version(rel, version, content)
        return rel

    def workspace_latest_version(self, workspace, rel: str) -> int:
        return self._facts.latest(rel) or 0

    def apply_workspace_mutation(
        self, workspace, rel: str, content: str, *, next_version: int | None = None
    ) -> int:
        if self._read_only:
            raise ExecutionError("read-only AgentOS cannot mutate workspaces")
        """Write to the real workspace, then register the new ArtifactVersion.
        Returns the new version (so D3 later sees applicability loss)."""
        written = workspace.write(rel, content)
        if written is False or getattr(written, "ok", True) is False:
            detail = getattr(written, "error", "")
            suffix = f": {detail}" if detail else ""
            raise ExecutionError(f"workspace write failed for {rel!r}{suffix}")
        ver = next_version or (self._facts.latest(rel) or 0) + 1
        self._facts.add_version(rel, ver, content)
        return ver

    # ── lower-level access (still public, for advanced users / future E3) ──
    @property
    def kernel(self):
        return self._kernel

    @property
    def vpg(self):
        return self._vpg

    @property
    def scheduler(self):
        return self._scheduler

    # ── run persistence for CLI observability (E3) ──────────────────────────
    def save_run(self, manifest_path: str) -> None:
        """Persist a run manifest so a later CLI/process can re-open it read-only.
        (Agent specs + goal graph + db path are stored; kernel/vpg state already
        durable in the db.)"""
        import json

        manifest_file = Path(manifest_path).resolve()
        if self._db_path == ":memory:":
            raise ConfigurationError(
                "cannot save a reopenable run manifest for an in-memory database"
            )
        manifest = {
            "db_path": str(Path(self._db_path).resolve()),
            "goals": {gid: self._serialize_goal(g) for gid, g in self._goals.items()},
            "agents": [
                {
                    "name": a.name,
                    "specializations": list(a.specializations),
                    "max_concurrency": a.max_concurrency,
                    "cost_weight": a.cost_weight,
                    "resource_capacity": a.resource_capacity.model_dump(mode="json"),
                }
                for a in self._agents.values()
            ],
        }
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        with open(manifest_file, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

    def _serialize_goal(self, g: Goal) -> dict:
        return {
            "goal_id": g.goal_id,
            "graph_id": self._goal_gid.get(g.goal_id),
            "tasks": [
                {
                    "task_id": t.task_id,
                    "agent": t.agent,
                    "depends_on": list(t.dependency_ids),
                    "task_kind": t.task_kind,
                    "required_specializations": list(t.required_specializations),
                    "required_tools": list(t.required_tools),
                    "max_attempts": t.max_attempts,
                    "metadata": t.metadata,
                    "resources": t.resources.model_dump(mode="json"),
                }
                for t in g.tasks
            ],
        }

    @classmethod
    def open_run(cls, manifest_path: str) -> AgentOS:
        """Re-open a saved run (read-only observability; does not re-run)."""
        import json

        with open(manifest_path, encoding="utf-8") as f:
            m = json.load(f)
        manifest = Path(manifest_path).resolve()
        raw_db_path = m.get("db_path", ":memory:")
        if raw_db_path != ":memory:":
            db_path = (
                str((manifest.parent / raw_db_path).resolve())
                if not Path(raw_db_path).is_absolute()
                else raw_db_path
            )
        else:
            db_path = raw_db_path
        os_ = cls(db_path, read_only=True)
        for agent_spec in m.get("agents", []):
            agent = Agent(
                agent_spec["name"],
                specializations=tuple(agent_spec.get("specializations", ["python"])),
                max_concurrency=agent_spec.get("max_concurrency", 4),
                cost_weight=agent_spec.get("cost_weight", 1.0),
                resource_capacity=agent_spec.get("resource_capacity"),
            )
            # Preserve manifest configuration for inspection/re-serialization,
            # but do not create a process or register a runnable descriptor.
            os_._agents[agent.name] = agent
        for gid, gm in m.get("goals", {}).items():
            g = os_.goal(gid)
            stored_gid = gm.get("graph_id")
            graph_exists = False
            if stored_gid:
                try:
                    os_.vpg.get_graph(stored_gid)
                    graph_exists = True
                except Exception:
                    graph_exists = False
            if stored_gid and graph_exists:
                # Reuse the durable graph; do NOT re-compile (keeps verified state).
                os_._goal_gid[gid] = stored_gid
                for t in gm.get("tasks", []):
                    deps = [
                        next((tt for tt in g.tasks if tt.task_id == d), None)
                        for d in t.get("depends_on", [])
                    ]
                    g.task(
                        t["task_id"],
                        agent=t.get("agent", ""),
                        depends_on=tuple(x for x in deps if x is not None),
                        task_kind=t.get("task_kind", "task"),
                        required_specializations=tuple(
                            t.get("required_specializations", ["python"])
                        ),
                        required_tools=tuple(t.get("required_tools", [])),
                        max_attempts=t.get("max_attempts", 3),
                        metadata=t.get("metadata", {}),
                        resources=t.get("resources"),
                    )
            else:
                raise ConfigurationError(
                    f"stored graph {stored_gid!r} for goal {gid!r} is missing; "
                    "read-only recovery will not rebuild or mutate the run"
                )
        return os_


# ergonomic alias
OS = AgentOS


def _render_tree(node_id: str, deps, sv, lines, seen, prefix: str, is_last: bool) -> None:
    if node_id in seen:
        return
    seen.add(node_id)
    tv = sv.tasks.get(node_id, {})
    mark = {"verified": "v", "stale": "x", "unverified": ".", "invalid": "!"}.get(
        tv.get("validity", ""), "?"
    )
    star = " * REPAIR" if tv.get("in_repair_frontier") else ""
    branch = "`-- " if is_last else "|-- "
    lines.append(f"{prefix}{branch}{mark} {node_id} [{tv.get('validity', '?').upper()}]{star}")
    children = sorted(deps.get(node_id, []))
    child_prefix = prefix + ("    " if is_last else "|   ")
    for i, c in enumerate(children):
        _render_tree(c, deps, sv, lines, seen, child_prefix, is_last=(i == len(children) - 1))


def _invoke_executor(executor: Any, task_id: str) -> Any:
    """Call documented one-arg executors or zero-arg Verifier-style executors.

    Signature binding is done before invocation so an executor's *internal*
    TypeError is never mistaken for an arity mismatch and retried.
    """
    try:
        signature = inspect.signature(executor)
    except (TypeError, ValueError):
        result = executor(task_id)
        return _require_sync_executor_result(result)
    try:
        signature.bind(task_id)
    except TypeError as one_arg_error:
        try:
            signature.bind()
        except TypeError:
            raise ConfigurationError(
                "Agent.executor must accept either task_id or no arguments",
                cause=one_arg_error,
            ) from one_arg_error
        result = executor()
    else:
        result = executor(task_id)
    return _require_sync_executor_result(result)


async def _invoke_executor_async(executor: Any, task_id: str) -> Any:
    """Invoke a documented executor without blocking the event loop."""
    try:
        signature = inspect.signature(executor)
    except (TypeError, ValueError):
        if _is_async_callable(executor):
            return await executor(task_id)
        return await asyncio.to_thread(executor, task_id)
    try:
        signature.bind(task_id)
    except TypeError as one_arg_error:
        try:
            signature.bind()
        except TypeError:
            raise ConfigurationError(
                "Agent.executor must accept either task_id or no arguments",
                cause=one_arg_error,
            ) from one_arg_error
        args: tuple[Any, ...] = ()
    else:
        args = (task_id,)

    if _is_async_callable(executor):
        return await executor(*args)
    result = await asyncio.to_thread(executor, *args)
    if inspect.isawaitable(result):
        return await result
    return result


def _invoke_verifier_sync(verifier: Any) -> Any:
    """Call a synchronous Task verifier and reject hidden awaitables.

    ``AgentOS.run`` intentionally remains a synchronous API.  A verifier that
    returns a coroutine (whether it is declared ``async`` or wrapped by a
    synchronous callable) is rejected before semantic commit and its
    coroutine is closed to avoid an un-awaited-coroutine warning.
    """
    outcome = verifier()
    if inspect.isawaitable(outcome):
        close = getattr(outcome, "close", None)
        if callable(close):
            close()
        raise ConfigurationError(
            "Task.verify returned an awaitable in AgentOS.run; use `await AgentOS.run_async(...)`"
        )
    return outcome


async def _invoke_verifier_async(verifier: Any) -> Any:
    """Invoke a sync or async Task verifier without blocking the event loop."""
    if _is_async_callable(verifier):
        return await verifier()
    outcome = await asyncio.to_thread(verifier)
    if inspect.isawaitable(outcome):
        return await outcome
    return outcome


def _require_sync_executor_result(result: Any) -> Any:
    """Reject an awaitable returned through an otherwise synchronous wrapper."""
    if inspect.isawaitable(result):
        close = getattr(result, "close", None)
        if callable(close):
            close()
        raise ConfigurationError(
            "Agent.executor returned an awaitable in synchronous AgentOS.run; "
            "use `await AgentOS.run_async(...)`"
        )
    return result
