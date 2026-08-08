"""LongHorizonOS E3 — observability read service (projection only).

Builds StatusView / inspect / explain / graph from the frozen Core's public
surface (VPG snapshot, Scheduler claims, Kernel Lease provider, D3 last repair
result).  Never mutates semantic state (OBS-G1..G12).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .observability import StatusView, TaskView

if TYPE_CHECKING:  # pragma: no cover
    from .os import AgentOS


def _validity_str(node: Any) -> str:
    v = getattr(node, "validity", None)
    return v.value if v is not None else "unverified"


def _lifecycle_str(node: Any) -> str:
    lc = getattr(node, "lifecycle", None)
    return lc.value if lc is not None else "proposed"


def build_status_view(os_: AgentOS, goal_id: str) -> StatusView:
    """Read-only status projection for a Goal."""
    gid = os_._gid_for(goal_id)
    if gid is None:
        g = os_._goals.get(goal_id)
        if g is not None:
            gid = os_._compile_goal(g)
    version = os_.vpg.get_graph(gid).current_version
    nodes, edges = os_.vpg.snapshot_projection(gid)
    tasks = {n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "task"}
    artifacts = {
        n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "artifact_ref"
    }
    evidence = {n.node_id: n for n in nodes.values() if getattr(n, "node_type", "") == "evidence"}

    # ownership from scheduler claims + Kernel Lease authority
    owner_by_task: dict[str, str | None] = {}
    lease_active: dict[str, bool] = {}
    claim_lease: dict[str, str] = {}
    for claim in os_.scheduler.claims:
        owner_by_task[claim.task_id] = claim.agent_id
        lid = getattr(claim, "lease_id", None)
        claim_lease[claim.task_id] = lid if lid else ""
        if lid:
            lease_active[claim.task_id] = os_._lease.get(lid) is not None

    # evidence current applicability (exact-version) + supporting evidence per task
    # (task -> evidence via verification produces/verifies edges)
    task_evidence: dict[str, str] = {}
    # map verification -> task (verifies) and evidence -> verification (produces V->E)
    verif_task: dict[str, str] = {}
    for e in edges:
        if e.edge_type.value == "verifies":
            verif_task[e.source_node_id] = e.target_node_id
    for e in edges:
        if e.edge_type.value == "produces" and e.source_node_id in verif_task:
            task = verif_task[e.source_node_id]
            task_evidence.setdefault(task, e.target_node_id)
    evidence_task: dict[str, str] = {}
    for task, evid in task_evidence.items():
        evidence_task.setdefault(evid, task)

    sv = StatusView(goal_id=goal_id, goal_state="OPEN", version=version)
    affected: set[str] = set(os_._last_repair.affected) if os_._last_repair else set()
    for tid, node in tasks.items():
        tv = TaskView(task_id=tid, lifecycle=_lifecycle_str(node), validity=_validity_str(node))
        if tid in affected:
            # D3 overlay: this task's semantic validity is now STALE (projection only)
            tv.validity = "stale"
        tv.owner = owner_by_task.get(tid)
        tv.lease_active = lease_active.get(tid)
        # artifact produced (task -> artifact_ref via produces)
        for e in edges:
            if (
                e.edge_type.value == "produces"
                and e.source_node_id == tid
                and e.target_node_id in artifacts
            ):
                ar = artifacts[e.target_node_id]
                tv.artifact = getattr(ar, "artifact_id", e.target_node_id)
                tv.artifact_version = ar.version
        # supporting evidence + current applicability
        sev_evid = task_evidence.get(tid)
        if sev_evid:
            tv.supporting_evidence = sev_evid
            evnode = evidence.get(sev_evid)
            bound: tuple[Any | None, int] | None = None
            if evnode is not None and getattr(evnode, "artifact_bindings", None):
                b = evnode.artifact_bindings[0]
                bound = (getattr(b, "artifact_id", None), b.version)
            # current applicability: artifact version equals latest known
            cur = None
            bound_ver: int | None = None
            if bound and tv.artifact and bound[1] is not None:
                bound_ver = bound[1]
                cur = os_._facts.latest(tv.artifact)
            tv.evidence_current_applicable = bool(
                cur is None or (bound_ver is not None and bound_ver >= cur)
            )
        sv.tasks[tid] = tv.as_dict()
        if tv.validity == "verified":
            sv.verified.append(tid)
        elif tv.validity == "stale":
            sv.stale.append(tid)
        elif tv.validity == "unverified":
            sv.unverified.append(tid)
        if tv.owner:
            sv.owner_by_task[tid] = tv.owner
        if tv.lease_active is not None:
            sv.leases[tid] = tv.lease_active

    # ready = stale/unverified whose deps are verified (approx; real readiness
    # uses VPG, but here we expose the derived frontier + ready candidates)
    sv.ready = [t for t in sv.stale if not _depends_on_stale(t, edges, tasks, sv.stale)]
    sv.ready.sort()
    sv.verified.sort()
    sv.stale.sort()
    sv.unverified.sort()

    # preserved verified work: verified and NOT in last-repair affected
    sv.preserved_verified = [t for t in sv.verified]
    if os_._last_repair is not None:
        affected = set(os_._last_repair.affected)
        sv.preserved_verified = [t for t in sv.verified if t not in affected]
        sv.repair_frontier = sorted(os_._last_repair.frontier)
        # blocked = stale not in frontier (depends-stale)
        for t in sv.stale:
            if t not in sv.repair_frontier:
                sv.blocked[t] = [
                    {"dep": d, "reason": "depends on a STALE task"}
                    for d in _stale_deps(t, edges, tasks, sv.stale)
                ]
    else:
        # no repair seen: show any stale as frontier-ready-if-deps-verified
        for t in sv.stale:
            if _depends_on_stale(t, edges, tasks, sv.stale):
                sv.blocked[t] = [
                    {"dep": d, "reason": "depends on a STALE task"}
                    for d in _stale_deps(t, edges, tasks, sv.stale)
                ]
            else:
                sv.repair_frontier.append(t)
        sv.repair_frontier.sort()
    sv.goal_state = (
        "CLOSED" if not sv.ready else ("REOPENED" if os_._last_repair is not None else "OPEN")
    )
    return sv


def _stale_deps(tid: str, edges, tasks, stale: list[str]) -> list[str]:
    out = []
    for e in edges:
        if (
            e.edge_type.value == "depends_on"
            and e.source_node_id == tid
            and e.target_node_id in tasks
            and e.target_node_id in stale
        ):
            out.append(e.target_node_id)
    return out


def _depends_on_stale(tid: str, edges, tasks, stale: list[str]) -> bool:
    return bool(_stale_deps(tid, edges, tasks, stale))
