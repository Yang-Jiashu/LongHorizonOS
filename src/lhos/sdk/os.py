"""LongHorizonOS Public SDK — AgentOS facade (E1, composition root).

`AgentOS` wires a real Agent Kernel + Verified Progress Graph + D2 Scheduler +
D3 into one object so a user can Agent/Goal/run without manual wiring.  It is a
composition/lifecycle facade — NOT a new authority.  Core owns semantic state,
ownership (Kernel Lease), and repair.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Any

from lhos.agent_os.sdk.client import create_kernel
from lhos.runtimes.multi_agent import (
    AgentDescriptor,
    AgentRegistry,
    create_scheduler,
)
from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.graph_store import GraphStore
from lhos.runtimes.verified_progress.models import ArtifactVersionBinding
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    GraphPatchProposal,
)

from .agent import Agent  # runtime import (used by open_run)
from .errors import (
    ConfigurationError,
    ExecutionError,
    SchedulingError,
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

if TYPE_CHECKING:  # pragma: no cover
    from .verification import VerificationOutcome


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
        self._kernel = None if read_only else create_kernel(db_path)
        self._owns_facts = facts is None
        self._facts = facts or FactsProvider(
            db_path,
            read_only=read_only,
            action_service=None if self._kernel is None else self._kernel._action_service,
        )
        self._read_only_conn: sqlite3.Connection | None = None
        vpg_store: GraphStore | str = db_path
        if read_only and db_path != ":memory:":
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
        )
        self._agents: dict[str, Agent] = {}
        self._agent_pid: dict[str, str] = {}
        self._next_artifact_version: dict[str, int] = {}
        self._goals: dict[str, Goal] = {}
        self._goal_gid: dict[str, str] = {}
        self._last_repair: RepairOutcome | None = None

    def close(self) -> None:
        """Release the kernel and VPG database handles."""
        self._vpg.close()
        if self._owns_facts:
            self._facts.close()
        if self._read_only_conn is not None:
            self._read_only_conn.close()
        if self._kernel is not None:
            self._kernel.close()

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
            )
        )
        self._agents[agent.name] = agent
        self._agent_pid[agent.name] = pid
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
            metadata = dict(t.metadata)
            scheduler_metadata = dict(metadata.get("scheduler", {}))
            scheduler_metadata.update(
                {
                    "task_kind": t.task_kind,
                    "required_specializations": list(t.required_specializations),
                    "required_tools": list(t.required_tools),
                    "max_attempts": t.max_attempts,
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
                res = self._scheduler.run_pass(gid)
            except Exception as e:  # surface scheduler errors
                raise SchedulingError("scheduler pass failed", cause=e) from e
            if not res.dispatched:
                break
            for d in res.dispatched:
                task_id = d["task_id"]
                agent_id = d["agent_id"]
                claim = self._scheduler.active_claim_for_task(task_id, gid)
                attempt_number = getattr(claim, "attempt_number", 0)
                self._execute_and_verify(
                    gid,
                    task_id,
                    agent_id,
                    goal,
                    attempt_number=attempt_number,
                )
                dispatched += 1
                if dispatched >= max_dispatches:
                    break
            steps += 1
            if steps >= max_steps:
                break
        return self.result(gid)

    def _execute_and_verify(
        self,
        gid: str,
        task_id: str,
        agent_id: str,
        goal: Goal,
        *,
        attempt_number: int = 0,
    ) -> None:
        """Execute a dispatched task's verifier and attach real Evidence.

        A task with no verifier/executor stays unverified (VPG-G2/G3): without
        a Verification->Evidence path the SDK must NOT fabricate VERIFIED.
        """
        task = next((t for t in goal.tasks if t.task_id == task_id), None)
        if task is None or task.verify is None:
            self._scheduler.release_task(gid, task_id, reason="missing_verifier")
            return  # fail-closed: no Evidence, no VERIFIED
        verifier = task.verify
        try:
            outcome = verifier()
        except Exception as e:
            self._scheduler.release_task(
                gid,
                task_id,
                reason=f"executor_failed:{type(e).__name__}",
            )
            return

        if not outcome.passed:
            # FAIL/INCONCLUSIVE => no VERIFIED; record failure
            self._scheduler.release_task(gid, task_id, reason="verification_failed")
            return
        latest_version = self._facts.latest(outcome.artifact_id)
        if latest_version is not None and latest_version > outcome.version:
            from dataclasses import replace

            outcome = replace(outcome, version=latest_version)
        produced_pid = self._agent_pid.get(agent_id) or self._owner_pid()
        current_version = self._facts.latest(outcome.artifact_id)
        if current_version is None or current_version < outcome.version:
            self._facts.add_version(outcome.artifact_id, outcome.version, outcome.content or "")
        self._attach_evidence(
            gid,
            task_id,
            agent_id,
            outcome,
            produced_pid,
            attempt_number=attempt_number,
        )
        self._scheduler.observe_vpg(gid)

    def _attach_evidence(
        self,
        gid: str,
        task_id: str,
        agent_id: str,
        outcome: VerificationOutcome,
        pid: str,
        *,
        attempt_number: int = 0,
    ) -> None:
        suffix = "" if attempt_number == 0 else f"-a{attempt_number}"
        vid = f"V-{task_id}-{outcome.version}{suffix}"
        evid = f"E-{task_id}-{outcome.version}{suffix}"
        artref_id = f"AR-{task_id}-{outcome.version}{suffix}"
        action_id = f"sdk-act-{task_id}-{outcome.version}{suffix}"
        self._facts.commit_action(action_id, pid=pid)
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
                idempotency_key=f"pin-{task_id}-{outcome.version}{suffix}",
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
                idempotency_key=f"verify-{task_id}-{outcome.version}{suffix}",
                operations=(
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
        curver = self._vpg.get_graph(gid).current_version
        newver = (
            new_artifact_version or (self._facts.latest(artifact_id) if artifact_id else None) or 1
        )
        causes: list[InvalidationCause] = []
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
            if ars and artifact_id and getattr(ars[0], "artifact_id", None) == artifact_id:
                causes.append(
                    InvalidationCause(
                        cause_id=f"c:{artifact_id}:{tid}",
                        graph_id=gid,
                        graph_version=curver,
                        cause_type="ARTIFACT_VERSION_SUPERSEDED",
                        source_node_id=tid,
                        artifact_id=artifact_id,
                        old_version=self._facts.latest(artifact_id) or 1,
                        new_version=newver,
                        reason=f"{artifact_id} version bumped",
                    )
                )
        if not causes:
            causes.append(
                InvalidationCause(
                    cause_id="c:change",
                    graph_id=gid,
                    graph_version=curver,
                    cause_type="ARTIFACT_VERSION_SUPERSEDED",
                    source_node_id=next(iter(task_nodes)),
                    artifact_id=artifact_id,
                    old_version=1,
                    new_version=2,
                    reason="world changed",
                )
            )
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
