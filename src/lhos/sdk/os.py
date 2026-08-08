"""LongHorizonOS Public SDK — AgentOS facade (E1, composition root).

`AgentOS` wires a real Agent Kernel + Verified Progress Graph + D2 Scheduler +
D3 into one object so a user can Agent/Goal/run without manual wiring.  It is a
composition/lifecycle facade — NOT a new authority.  Core owns semantic state,
ownership (Kernel Lease), and repair.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lhos.agent_os.sdk.client import create_kernel
from lhos.runtimes.multi_agent import (
    AgentDescriptor,
    AgentRegistry,
    create_scheduler,
)
from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.models import ArtifactVersionBinding
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    GraphPatchProposal,
)

from .errors import (
    ConfigurationError,
    ExecutionError,
    SchedulingError,
)
from .providers import (
    FactsProvider,
    KernelCapabilityProvider,
    KernelLeaseProvider,
    KernelProcessProvider,
    VPGFacade,
)
from .result import RepairOutcome, RunResult
from .status import StatusSnapshot

if TYPE_CHECKING:  # pragma: no cover
    from .agent import Agent
    from .goal import Goal
    from .verification import VerificationOutcome


class AgentOS:
    """Top-level composition root for a Core-backed LongHorizonOS instance."""

    def __init__(self, db_path: str = ":memory:", *, facts: FactsProvider | None = None) -> None:
        self._kernel = create_kernel(db_path)
        self._facts = facts or FactsProvider()
        self._vpg = VerifiedProgressRuntime(
            db_path, facts_artifact=self._facts, facts_kernel=self._facts
        )  # type: ignore[arg-type]
        self._vpg_surface = VPGFacade(self._vpg)
        self._proc = KernelProcessProvider(self._kernel)
        self._lease = KernelLeaseProvider(self._kernel)
        self._cap = KernelCapabilityProvider(self._kernel)
        self._registry = AgentRegistry()
        self._scheduler = create_scheduler(
            self._registry,
            vpg=self._vpg_surface,
            process_provider=self._proc,
            lease_provider=self._lease,
            capability_provider=self._cap,
        )
        self._agents: dict[str, Agent] = {}
        self._agent_pid: dict[str, str] = {}
        self._next_artifact_version: dict[str, int] = {}
        self._goals: dict[str, Goal] = {}
        self._goal_gid: dict[str, str] = {}

    # ── agents ───────────────────────────────────────────────────────────────
    def add_agent(self, agent: Agent) -> Agent:
        pid = self._proc.spawn(agent.name)
        agent._bind_process(pid)
        caps = agent.capabilities or ("shell", "filesystem", "network")
        self._grant_capabilities(pid, caps)
        self._registry.register(
            AgentDescriptor(
                agent_id=agent.name,
                process_id=pid,
                supported_task_kinds=agent.supported_task_kinds,
                supported_tools=agent.supported_task_kinds,
                specializations=tuple(sorted(agent.specializations)),
                max_concurrency=agent.max_concurrency,
                cost_weight=max(1, round(agent.cost_weight * 100)),
            )
        )
        self._agents[agent.name] = agent
        self._agent_pid[agent.name] = pid
        return agent

    def _grant_capabilities(self, pid: str, caps: tuple[str, ...]) -> None:
        import contextlib

        from .providers import make_capability

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
        pid = self._owner_pid()
        gid = self._vpg.create_graph(owner_pid=pid).graph_id
        v = self._vpg.get_graph(gid).current_version
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
            ops.append(
                AddNodeOp(
                    node_id=t.task_id,
                    graph_id=gid,
                    node_type="task",
                    created_by_pid=pid,
                    task_kind=t.task_kind,
                    metadata={
                        "scheduler": {
                            "task_kind": t.task_kind,
                            "required_specializations": list(t.required_specializations),
                            "required_tools": list(t.required_tools),
                        },
                        "sdk": {"agent": t.agent},
                    },
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
        self._vpg.submit_patch(
            GraphPatchProposal(
                graph_id=gid,
                expected_graph_version=v,
                author_pid=pid,
                idempotency_key=f"compile-{goal.goal_id}",
                operations=tuple(ops),
            )
        )
        self._goal_gid[goal.goal_id] = gid
        return gid

    def _owner_pid(self) -> str:
        if self._agents:
            return next(iter(self._agent_pid.values()))
        return self._proc.spawn("sdk-root")

    # ── run ──────────────────────────────────────────────────────────────────
    def run(self, goal: Goal, *, max_dispatches: int = 8, max_steps: int = 20) -> RunResult:
        self._goals.setdefault(goal.goal_id, goal)
        gid = self._gid_for(goal.goal_id, compile_if_missing=True)
        if gid is None:
            raise ConfigurationError(f"goal {goal.goal_id!r} not registered")
        dispatched = 0
        steps = 0
        while dispatched < max_dispatches and steps < max_steps:
            res = None
            try:
                res = self._scheduler.run_pass(gid)
            except Exception as e:  # surface scheduler errors
                raise SchedulingError("scheduler pass failed", cause=e) from e
            if not res.dispatched:
                break
            for d in res.dispatched:
                task_id = d["task_id"]
                agent_id = d["agent_id"]
                self._execute_and_verify(gid, task_id, agent_id, goal)
                dispatched += 1
                if dispatched >= max_dispatches:
                    break
            steps += 1
            if steps >= max_steps:
                break
        return self.result(gid)

    def _execute_and_verify(self, gid: str, task_id: str, agent_id: str, goal: Goal) -> None:
        """Execute a dispatched task's verifier and attach real Evidence."""
        task = next((t for t in goal.tasks if t.task_id == task_id), None)
        if task is None or task.verify is None:
            # no verifier and no executor: mark as a scripted pass with default artifact
            verifier = _default_scripted(task_id)
        else:
            verifier = task.verify
        try:
            outcome = verifier()
        except Exception as e:
            raise ExecutionError(f"executor failed for {task_id}", cause=e) from e

        if not outcome.passed:
            # FAIL/INCONCLUSIVE => no VERIFIED; record failure
            return
        produced_pid = self._agent_pid.get(agent_id) or self._owner_pid()
        self._facts.add_version(outcome.artifact_id, outcome.version, outcome.content or "")
        self._facts.commit_action(f"sdk-act-{task_id}-{outcome.version}", pid=produced_pid)
        self._attach_evidence(gid, task_id, agent_id, outcome, produced_pid)

    def _attach_evidence(
        self, gid: str, task_id: str, agent_id: str, outcome: VerificationOutcome, pid: str
    ) -> None:
        vid = f"V-{task_id}-{outcome.version}"
        evid = f"E-{task_id}-{outcome.version}"
        artref_id = f"AR-{task_id}-{outcome.version}"
        binding = ArtifactVersionBinding(
            canonical_uri=f"vpg://{outcome.artifact_id}",
            artifact_id=outcome.artifact_id,
            version=outcome.version,
            content_hash=self._facts.read_hash(pid, outcome.artifact_id, outcome.version) or "",
        )
        cur = self._vpg.get_graph(gid).current_version
        # A task must pin the exact artifact version (produces ArtifactRefNode)
        # so `task_is_verified` sees matching pins.
        self._vpg.submit_patch(
            GraphPatchProposal(
                graph_id=gid,
                expected_graph_version=cur,
                author_pid=pid,
                idempotency_key=f"pin-{task_id}-{outcome.version}",
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
                    AddEdgeOp(
                        edge_type="produces",
                        source_node_id=task_id,
                        target_node_id=artref_id,
                        created_by_pid=pid,
                    ),
                ),
            )
        )
        cur = self._vpg.get_graph(gid).current_version
        self._vpg.submit_patch(
            GraphPatchProposal(
                graph_id=gid,
                expected_graph_version=cur,
                author_pid=pid,
                idempotency_key=f"verify-{task_id}-{outcome.version}",
                operations=(
                    AddNodeOp(
                        node_id=vid,
                        graph_id=gid,
                        node_type="verification",
                        created_by_pid=pid,
                        verification_kind="command_result",
                        obligation={"kind": "produced_artifact"},
                        source_action_id=f"sdk-act-{task_id}-{outcome.version}",
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
                        evidence_source_action_id=f"sdk-act-{task_id}-{outcome.version}",
                        artifact_bindings=(binding,),
                        produced_by_pid=pid,
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
            )
        )

    # ── result / status ──────────────────────────────────────────────────────
    def result(self, gid: str) -> RunResult:
        nodes, _ = self._vpg.snapshot_projection(gid)
        tasks = {n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "task"}
        task_states = {tid: n.validity.value for tid, n in tasks.items()}
        verified = [t for t, s in task_states.items() if s == "verified"]
        stale = [t for t, s in task_states.items() if s == "stale"]
        ready = [t for t, s in task_states.items() if s in ("unverified", "stale")]
        owner = {}
        for claim in self._scheduler.claims:
            owner[claim.task_id] = claim.agent_id
        artifacts = {}
        for aid, vs in self._facts.versions().items():
            for ver in vs:
                artifacts[aid] = (ver, self._facts.read_hash(self._owner_pid(), aid, ver) or "")
        return RunResult(
            goal_id=gid,
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
            goal_closed=(not r.ready),
            owner_by_task=r.owner_by_task,
        )

    # ── repair (D3) ────────────────────────────────────────────────────────
    def repair(
        self, goal: Goal, *, new_artifact_version: int | None = None, artifact_id: str | None = None
    ) -> RepairOutcome:
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
        if artifact_id is not None and new_artifact_version is not None:
            cur = self._facts.latest(artifact_id) or 0
            if new_artifact_version <= cur:
                new_artifact_version = cur + 1
            self._facts.add_version(
                artifact_id, new_artifact_version, f"body-v{new_artifact_version}"
            )
        from lhos.runtimes.invalidation.engine import (
            EngineInputs,
            build_invalidation_result,
            run_invalidation_engine,
        )
        from lhos.runtimes.invalidation.models import InvalidationCause

        nodes, edges = self._vpg.snapshot_projection(gid)
        task_nodes = {n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "task"}
        # one seed: the task that produced the bumped artifact (if any)
        cause = None
        for tid, _tn in task_nodes.items():
            ars = [
                n
                for n in nodes.values()
                if getattr(n, "node_type", "") == "artifact_ref"
                and any(
                    (
                        e.edge_type.value == "produces"
                        and e.source_node_id == tid
                        and e.target_node_id == n.node_id
                    )
                    for e in edges
                )
            ]
            if (
                cause is None
                and ars
                and artifact_id
                and getattr(ars[0], "artifact_id", None) == artifact_id
            ):
                cause = InvalidationCause(
                    cause_id=f"c:{artifact_id}",
                    graph_id=gid,
                    graph_version=self._vpg.get_graph(gid).current_version,
                    cause_type="ARTIFACT_VERSION_SUPERSEDED",
                    source_node_id=tid,
                    artifact_id=artifact_id,
                    old_version=self._facts.latest(artifact_id) or 1,
                    new_version=new_artifact_version or (self._facts.latest(artifact_id) or 1),
                    reason=f"{artifact_id} version bumped",
                )
        if cause is None:
            cause = InvalidationCause(
                cause_id="c:change",
                graph_id=gid,
                graph_version=self._vpg.get_graph(gid).current_version,
                cause_type="ARTIFACT_VERSION_SUPERSEDED",
                source_node_id=next(iter(task_nodes)),
                artifact_id=artifact_id,
                old_version=1,
                new_version=2,
                reason="world changed",
            )
        inp = EngineInputs(
            graph_id=gid,
            current_version=self._vpg.get_graph(gid).current_version,
            task_nodes=task_nodes,
            goal_nodes={},
            evidence_nodes={
                n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "evidence"
            },
            edges=edges,
            explicit_causes=(cause,),
        )
        er = run_invalidation_engine(inp)
        ir = build_invalidation_result(inp, er)
        return RepairOutcome(
            affected=list(ir.stale_nodes),
            preserved=list(ir.preserved_nodes),
            frontier=[c.task_id for c in ir.frontier.candidates],
            causes=[c.reason for c in ir.causes],
        )

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


def _default_scripted(task_id: str):
    from .verification import VerificationOutcome

    def _run() -> VerificationOutcome:
        return VerificationOutcome(
            passed=True,
            artifact_id=task_id,
            version=1,
            content=f"{task_id}:ok",
            evidence_note="scripted",
        )

    return _run


# ergonomic alias
OS = AgentOS
