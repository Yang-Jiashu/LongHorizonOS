"""VerifiedProgressRuntime — SDK-facing entry point for D1.

This is the only class that user/democode imports.  It:
  - owns the GraphStore
  - exposes create_graph / submit_patch / query / inspect
  - derives VERIFIED/STALE/READY after each commit
  - pulls Kernel + Artifact facts via injected protocols
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any

from .closure import (
    goal_is_closed,
    task_should_be_closed,
)
from .errors import (
    VPGCode,
    VPGError,
    graph_not_found,
    graph_version_conflict,
)
from .events import GraphEvent, GraphEventType
from .graph_store import GraphStore
from .models import (
    AnyNode,
    ArtifactRefNode,
    EdgeType,
    GoalNode,
    GraphRecord,
    NodeLifecycle,
    NodeValidity,
    TaskDispatchCandidate,
    TaskNode,
    VPGEdge,
)
from .patches import (
    AddEdgeOp,
    AddNodeOp,
    AttachArtifactOp,
    AttachEvidenceOp,
    GraphPatchProposal,
    PatchCommitResult,
)
from .projections import rebuild_projection
from .protocols import ArtifactFactProvider, KernelEventProvider
from .readiness import compute_ready_frontier
from .verification import task_is_verified


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _uuid() -> str:
    from uuid import uuid4

    return uuid4().hex


class _InMemoryFacts:
    """No-op facts provider used in pure-graph tests with no SDK wiring.

    Allows patches to commit without SDK; artifact checks default to OK.
    """

    def artifact_exists(self, pid, canonical_uri, version):
        return True

    def read_hash(self, pid, canonical_uri, version):
        return None

    def verify_binding(self, pid, binding):
        return True

    def can_read(self, pid, artifact_id, version):
        return True

    def get_action(self, action_id):
        return None

    def has_event(self, event_id):
        return False


class VerifiedProgressRuntime:
    """D1 Verified Progress Graph Runtime.

    Construct with a GraphStore (or connection string) + optional injected
    ``ArtifactFactProvider`` and ``KernelEventProvider``.
    """

    def __init__(
        self,
        store: GraphStore | sqlite3.Connection | str,
        *,
        facts_artifact: ArtifactFactProvider | None = None,
        facts_kernel: KernelEventProvider | None = None,
    ) -> None:
        if isinstance(store, GraphStore):
            self.store = store
        else:
            self.store = GraphStore(store)
        self.facts_artifact = facts_artifact
        self.facts_kernel = facts_kernel

    # ── graph lifecycle ───────────────────────────────────────────────────
    def create_graph(
        self,
        owner_pid: str,
        *,
        graph_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> GraphRecord:
        rec = GraphRecord(
            graph_id=graph_id or _uuid(),
            owner_pid=owner_pid,
            metadata=metadata or {},
        )
        return self.store.create_graph(rec)

    def get_graph(self, graph_id: str) -> GraphRecord:
        rec = self.store.get_record(graph_id)
        if rec is None:
            raise graph_not_found(graph_id)
        return rec

    def close_graph(self, graph_id: str) -> GraphRecord:
        """Close the graph — no further agent patches allowed."""
        rec = self.get_graph(graph_id)
        if rec.closed:
            raise VPGError(VPGCode.GRAPH_CLOSED, graph_id)
        with self.store.conn:
            self.store.close_graph(graph_id)
        return self.get_graph(graph_id)

    # ── projection snapshot ───────────────────────────────────────────────
    def snapshot_projection(self, graph_id: str) -> tuple[dict[str, AnyNode], list[VPGEdge]]:
        self.get_graph(graph_id)
        nodes = {n.node_id: n for n in self.store.get_all_nodes(graph_id)}
        edges = self.store.get_all_edges(graph_id)
        return nodes, edges

    # ── patch commit ──────────────────────────────────────────────────────
    def submit_patch(self, patch: GraphPatchProposal) -> PatchCommitResult:
        """Commit a patch atomically with full D1 protocol.

        Steps:
          1. parse+validate graph
          2. check closed
          3-5. optimistic concurrency + idempotency
          6-20. validate ops, build candidate projection, commit, derive
        """
        rec = self.store.get_record(patch.graph_id)
        if rec is None:
            raise graph_not_found(patch.graph_id)
        if rec.closed:
            raise VPGError(VPGCode.GRAPH_CLOSED, patch.graph_id)

        # Idempotency replay bypasses ALL validation — the patch was already
        # successfully committed under this composite_key, so version drift
        # from intervening patches is irrelevant.
        idem = self.store.has_idempotency(patch.composite_key)
        if idem is not None:
            patch_id, committed_version = idem
            return PatchCommitResult(
                graph_id=patch.graph_id,
                patch_id=patch_id,
                committed_graph_version=committed_version,
                patch_applied=False,
                idempotent_replay=True,
            )

        if patch.expected_graph_version != rec.current_version:
            raise graph_version_conflict(
                patch.expected_graph_version, rec.current_version
            )

        # snapshot current projection — deep copy so derived-state mutations
        # during _apply_derived_after_patch don't alias the baseline and hide
        # the diff in nodes_to_upsert.
        import copy
        current_snapshot = self.snapshot_projection(patch.graph_id)
        current_nodes = {nid: copy.deepcopy(n) for nid, n in current_snapshot[0].items()}
        current_edges = [copy.deepcopy(e) for e in current_snapshot[1]]

        # build operations from patch

        # Defensive: ensure operations are concrete types
        _normalize_patch_operations(patch)

        # validate & construct candidate projection
        from .patch_validator import (
            PatchValidationRequest,
            validate_patch,
        )

        req = PatchValidationRequest()
        req.patch = patch
        req.current_nodes = current_nodes
        req.current_edges = current_edges
        req.facts_artifact = self.facts_artifact
        req.facts_kernel = self.facts_kernel
        result = validate_patch(req)

        new_version = rec.current_version + 1
        applied_at = _dt_iso_safe(_utcnow())

        # mark any STALE/derived lifecycle updates before commit
        # result.candidate_nodes are deep-copied inside validate_patch, so they
        # are independent from current_nodes — derived mutations will show up
        # in the old-vs-new model_dump() diff below.
        derived_events = self._apply_derived_after_patch(
            patch.graph_id,
            new_version,
            patch.patch_id,
            result.candidate_nodes,
            result.candidate_edges,
        )
        all_events = list(result.events) + derived_events

        # Upsert all nodes that changed state since the baseline snapshot.
        nodes_to_upsert = []
        for nid, new in result.candidate_nodes.items():
            old = current_nodes.get(nid)
            if old is None or old.model_dump() != new.model_dump():
                nodes_to_upsert.append((nid, new))

        edges_to_upsert = result.new_edges
        self.store.commit_patch(
            patch,
            patch_id=patch.patch_id,
            committed_version=new_version,
            applied_at=applied_at,
            events=all_events,
            nodes_to_upsert=nodes_to_upsert,
            edges_to_upsert=edges_to_upsert,
        )
        return PatchCommitResult(
            graph_id=patch.graph_id,
            patch_id=patch.patch_id,
            committed_graph_version=new_version,
            patch_applied=True,
            idempotent_replay=False,
        )

    def _apply_derived_after_patch(
        self,
        graph_id: str,
        new_version: int,
        patch_id: str,
        candidate_nodes: dict[str, AnyNode],
        candidate_edges: list[VPGEdge],
    ) -> list[GraphEvent]:
        """Recompute derived state (VERIFIED/STALE/READY/CLOSED) and emit events."""
        return _recompute_derived_state(
            graph_id,
            new_version,
            patch_id,
            candidate_nodes,
            candidate_edges,
            self.facts_artifact,
            self.facts_kernel,
        )

    # ── readiness ─────────────────────────────────────────────────────────
    def query_ready_frontier(
        self,
        graph_id: str,
    ) -> list[TaskDispatchCandidate]:
        rec = self.get_graph(graph_id)
        nodes, edges = self.snapshot_projection(graph_id)
        return compute_ready_frontier(
            graph_id, rec.current_version, nodes, edges
        )

    # ── inspection ────────────────────────────────────────────────────────
    def inspect_node(self, graph_id: str, node_id: str) -> AnyNode | None:
        nodes, _ = self.snapshot_projection(graph_id)
        return nodes.get(node_id)

    def inspect_edge(self, graph_id: str, edge_id: str) -> VPGEdge | None:
        edges = self.store.get_all_edges(graph_id)
        for e in edges:
            if e.edge_id == edge_id:
                return e
        return None

    def get_events(
        self, graph_id: str, since_version: int | None = None
    ) -> list[GraphEvent]:
        return self.store.get_events(graph_id, since_version)

    def rebuild_projection(self, graph_id: str) -> tuple[dict[str, AnyNode], list[VPGEdge], list[GraphEvent]]:
        """Drop + rebuild the projection from patch history."""
        self.store.delete_projection(graph_id)

        all_patches_rows = self.store.conn.execute(
            "SELECT operations_json, patch_id, committed_version "
            "FROM graph_patches WHERE graph_id = ? ORDER BY applied_at",
            (graph_id,),
        ).fetchall()

        import json as _json

        patches = []
        n_hist: dict[str, list] = {}
        e_hist: dict[str, list] = {}
        for row in all_patches_rows:
            raw = _json.loads(row["operations_json"])
            # Ensure operations are concrete (matches _normalize_patch_operations)
            raw["operations"] = _normalize_raw_ops(raw.get("operations", []))
            p = GraphPatchProposal(**raw)
            patches.append(p)
            n_row, e_row = _ops_to_nodes_edges(graph_id, p)
            n_hist[p.patch_id] = n_row
            e_hist[p.patch_id] = e_row

        nodes, edges, evs = rebuild_projection(
            graph_id,
            patches,
            e_hist,
            n_hist,
            facts_artifact=self.facts_artifact,
            facts_kernel=self.facts_kernel,
        )

        # replace materialized store
        with self.store.conn:
            ndata = [
                (n.node_id, n.graph_id, n.node_type.value, n.model_dump_json())
                for n in nodes.values()
            ]
            if ndata:
                self.store.conn.executemany(
                    "INSERT OR REPLACE INTO graph_nodes_projection "
                    "(node_id, graph_id, node_type, payload_json) "
                    "VALUES (?, ?, ?, ?)",
                    ndata,
                )
            for e in edges:
                self.store.conn.execute(
                    "INSERT OR REPLACE INTO graph_edges_projection "
                    "(edge_id, graph_id, edge_type, source_node_id, target_node_id, "
                    "created_in_version, created_by_pid, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        e.edge_id,
                        e.graph_id,
                        e.edge_type.value,
                        e.source_node_id,
                        e.target_node_id,
                        e.created_in_version,
                        e.created_by_pid,
                        _dt_iso_safe(e.created_at),
                    ),
                )

        return nodes, edges, evs

    def recover(self, graph_id: str):
        from .recovery import verify_and_recover

        return verify_and_recover(
            self.store,
            graph_id,
            facts_artifact=self.facts_artifact,
            facts_kernel=self.facts_kernel,
        )


def _recompute_derived_state(
    graph_id: str,
    graph_version: int,
    causation_patch: str,
    nodes: dict[str, AnyNode],
    edges: list[VPGEdge],
    facts_artifact: ArtifactFactProvider | None,
    facts_kernel: KernelEventProvider | None,
) -> list[GraphEvent]:
    """Full pass: task-local invalidation -> derive VERIFIED/STALE/CLOSED -> goal closure -> ready frontier."""
    out: list[GraphEvent] = []

    # 1. task-local invalidation
    for n in list(nodes.values()):
        if not isinstance(n, TaskNode):
            continue
        if n.validity != NodeValidity.VERIFIED:
            continue
        pinned_now = _pinned_versions(n.node_id, nodes, edges)
        verified_at = _verified_versions(n)
        if verified_at and pinned_now != verified_at:
            n.validity = NodeValidity.STALE
            n.updated_in_version = graph_version
            out.append(
                GraphEvent(
                    graph_id=graph_id,
                    event_type=GraphEventType.TASK_STALE_DERIVED,
                    causation_patch_id=causation_patch,
                    node_id=n.node_id,
                    graph_version=graph_version,
                )
            )
            n.metadata = dict(n.metadata)
            n.metadata.pop("__verified_artifact_versions", None)
            if n.lifecycle == NodeLifecycle.CLOSED:
                n.lifecycle = NodeLifecycle.ACTIVE
                out.append(
                    GraphEvent(
                        graph_id=graph_id,
                        event_type=GraphEventType.TASK_REOPENED_DERIVED,
                        causation_patch_id=causation_patch,
                        node_id=n.node_id,
                        graph_version=graph_version,
                    )
                )

    # 2. VERIFIED/STALE for all tasks
    for n in list(nodes.values()):
        if not isinstance(n, TaskNode):
            continue
        if n.validity == NodeValidity.VERIFIED:
            # record pinned versions snapshot for future invalidation
            n.metadata = dict(n.metadata)
            n.metadata["__verified_artifact_versions"] = [
                {"canonical_uri": u, "version": v}
                for u, v in _pinned_versions(n.node_id, nodes, edges)
            ]
            continue
        if n.validity == NodeValidity.STALE:
            # Stale tasks CAN re-verify when new evidence matches repinned
            # artifacts.  Try the VERIFIED predicate; if it passes, upgrade.
            if task_is_verified(
                n,
                nodes=nodes,
                edges=edges,
                facts_artifact=facts_artifact,
                facts_kernel=facts_kernel,
            ):
                n.validity = NodeValidity.VERIFIED
                n.updated_in_version = graph_version
                n.lifecycle = NodeLifecycle.ADMITTED
                n.metadata = dict(n.metadata)
                n.metadata["__verified_artifact_versions"] = [
                    {"canonical_uri": u, "version": v}
                    for u, v in _pinned_versions(n.node_id, nodes, edges)
                ]
                out.append(
                    GraphEvent(
                        graph_id=graph_id,
                        event_type=GraphEventType.TASK_VERIFIED_DERIVED,
                        causation_patch_id=causation_patch,
                        node_id=n.node_id,
                        graph_version=graph_version,
                    )
                )
                if task_should_be_closed(n):
                    n.lifecycle = NodeLifecycle.CLOSED
                    out.append(
                        GraphEvent(
                            graph_id=graph_id,
                            event_type=GraphEventType.TASK_CLOSED_DERIVED,
                            causation_patch_id=causation_patch,
                            node_id=n.node_id,
                            graph_version=graph_version,
                        )
                    )
            continue
        if n.validity == NodeValidity.INVALID:
            continue

        if task_is_verified(
            n,
            nodes=nodes,
            edges=edges,
            facts_artifact=facts_artifact,
            facts_kernel=facts_kernel,
        ):
            n.validity = NodeValidity.VERIFIED
            n.updated_in_version = graph_version
            n.metadata = dict(n.metadata)
            n.metadata["__verified_artifact_versions"] = [
                {"canonical_uri": u, "version": v}
                for u, v in _pinned_versions(n.node_id, nodes, edges)
            ]
            out.append(
                GraphEvent(
                    graph_id=graph_id,
                    event_type=GraphEventType.TASK_VERIFIED_DERIVED,
                    causation_patch_id=causation_patch,
                    node_id=n.node_id,
                    graph_version=graph_version,
                )
            )
            if task_should_be_closed(n):
                n.lifecycle = NodeLifecycle.CLOSED
                out.append(
                    GraphEvent(
                        graph_id=graph_id,
                        event_type=GraphEventType.TASK_CLOSED_DERIVED,
                        causation_patch_id=causation_patch,
                        node_id=n.node_id,
                        graph_version=graph_version,
                    )
                )
        else:
            # check for a STALE derivation (e.g. evidence invalidated)
            # Graph remains UNVERIFIED for a freshly admitted task.
            pass

    # 3. goal closure
    for n in list(nodes.values()):
        if not isinstance(n, GoalNode):
            continue
        closed = goal_is_closed(n, nodes, edges)
        was_closed = n.lifecycle == NodeLifecycle.CLOSED
        if closed and not was_closed:
            n.lifecycle = NodeLifecycle.CLOSED
            n.updated_in_version = graph_version
            out.append(
                GraphEvent(
                    graph_id=graph_id,
                    event_type=GraphEventType.GOAL_CLOSED_DERIVED,
                    causation_patch_id=causation_patch,
                    node_id=n.node_id,
                    graph_version=graph_version,
                )
            )
        elif not closed and was_closed:
            n.lifecycle = NodeLifecycle.ACTIVE
            n.updated_in_version = graph_version
            out.append(
                GraphEvent(
                    graph_id=graph_id,
                    event_type=GraphEventType.GOAL_REOPENED_DERIVED,
                    causation_patch_id=causation_patch,
                    node_id=n.node_id,
                    graph_version=graph_version,
                )
            )

    # 4. ready frontier
    frontier = compute_ready_frontier(graph_id, graph_version, nodes, edges)
    out.append(
        GraphEvent(
            graph_id=graph_id,
            event_type=GraphEventType.READY_FRONTIER_UPDATED,
            causation_patch_id=causation_patch,
            ready_frontier=tuple(c.task_id for c in frontier),
            graph_version=graph_version,
        )
    )
    return out


def _pinned_versions(
    task_id: str,
    nodes: dict[str, AnyNode],
    edges: list[VPGEdge],
) -> set:
    """Artifact versions currently pinned to the task.

    When multiple versions of the same canonical_uri are pinned (because
    artifact was re-pinned over time), we take only the latest version per
    canonical_uri — so a repin from v1 to v2 replaces v1 for verification
    purposes rather than accumulating.
    """
    latest: dict[str, tuple[str, int]] = {}
    for e in edges:
        if e.edge_type == EdgeType.PRODUCES and e.source_node_id == task_id:
            n = nodes.get(e.target_node_id)
            if isinstance(n, ArtifactRefNode):
                cur = latest.get(n.canonical_uri)
                if cur is None or n.version > cur[1]:
                    latest[n.canonical_uri] = (n.canonical_uri, n.version)
    return set(latest.values())


def _verified_versions(task: TaskNode) -> set | None:
    if not isinstance(task.metadata, dict):
        return None
    raw = task.metadata.get("__verified_artifact_versions")
    if not raw:
        return None
    return {(b["canonical_uri"], b["version"]) for b in raw}


def _dt_iso_safe(d: datetime) -> str:
    return d.isoformat()


def _normalize_raw_ops(ops: list) -> list:
    """JSON-decoded operations to concrete op dicts for GraphPatchProposal."""

    fixed = []
    for op in ops:
        if isinstance(op, dict):
            if "op_type" in op:
                fixed.append(op)
            elif "node_id" in op and "edge_id" not in op:
                fixed.append({**op, "op_type": "add_node"})
            elif "edge_id" in op:
                fixed.append({**op, "op_type": "add_edge"})
            elif "task_node_id" in op and "artifact" in op:
                fixed.append({**op, "op_type": "attach_artifact"})
            elif "verification_node_id" in op and "evidence_node_id" in op:
                fixed.append({**op, "op_type": "attach_evidence"})
            else:
                fixed.append(op)
        else:
            fixed.append(op)
    return fixed


def _ops_to_nodes_edges(
    graph_id: str, patch: GraphPatchProposal
) -> tuple[list, list]:
    """Derive the nodes/edges a patch added, for projection replay.

    This mirrors patch_validator._build_add_node and the per-op edge builders
    without running full admission/evidence validation (reprojection re-runs
    admit internally).
    """
    from .models import (
        ArtifactRefNode,
        EdgeType,
        EvidenceNode,
        EvidenceResult,
        NodeLifecycle,
        NodeType,
        NodeValidity,
        VerificationNode,
        VPGEdge,
    )
    from .patches import (
        AddEdgeOp,
        AddNodeOp,
        AttachArtifactOp,
        AttachEvidenceOp,
    )

    new_nodes: list = []
    new_edges: list = []
    version = (patch.expected_graph_version or 0) + 1
    by_id: dict[str, AnyNode] = {}

    def _node_for(op: AddNodeOp) -> AnyNode | None:
        from typing import cast
        # source_action_id differs per subtype: verification uses verification_tier,
        # evidence uses evidence_source_action_id.
        sa_id = (
            op.evidence_source_action_id
            if op.node_type == "evidence"
            else op.source_action_id
        )
        base: dict[str, object] = dict(
            node_id=op.node_id,
            graph_id=graph_id,
            lifecycle=NodeLifecycle.PROPOSED,
            validity=NodeValidity.UNVERIFIED,
            created_in_version=version,
            updated_in_version=version,
            created_by_pid=op.created_by_pid,
            created_at=_utcnow(),
            metadata=dict(op.metadata),
            title=op.title,
            description=op.description,
            task_kind=op.task_kind,
            execution_spec=dict(op.execution_spec),
            required_verification_count=op.required_verification_count,
            canonical_uri=op.canonical_uri,
            artifact_id=op.artifact_id,
            version=op.version if op.version is not None else -1,
            content_hash=op.content_hash,
            media_type=op.media_type,
            verification_kind=op.verification_kind,
            obligation=dict(op.obligation),
            source_action_id=sa_id,
            evidence_kind=op.evidence_kind,
            result=EvidenceResult(op.result),
            source_verification_id=op.source_verification_id,
            evidence_source_action_id=op.evidence_source_action_id,
            source_event_ids=op.source_event_ids,
            artifact_bindings=op.artifact_bindings,
            evidence_content_ref=op.evidence_content_ref,
            evidence_hash=op.evidence_hash,
            produced_by_pid=op.produced_by_pid,
        )
        if op.node_type == "goal":
            return GoalNode(**cast(Any, base))
        if op.node_type == "task":
            return TaskNode(**cast(Any, base))
        if op.node_type == "artifact_ref":
            return ArtifactRefNode(**cast(Any, base))
        if op.node_type == "verification":
            return VerificationNode(**cast(Any, base))
        if op.node_type == "evidence":
            return EvidenceNode(**cast(Any, base))
        return None

    def ensure(op: AddNodeOp | AddEdgeOp | AttachArtifactOp | AttachEvidenceOp):
        if isinstance(op, AddNodeOp):
            n = _node_for(op)
            if n is not None:
                by_id[n.node_id] = n
                new_nodes.append(n)

    # Rewrite op_type-tagged dicts so we can match below
    _normalize_patch_operations(patch)
    for op in patch.operations:
        if isinstance(op, AddNodeOp):
            ensure(op)
        elif isinstance(op, AddEdgeOp):
            new_edges.append(
                VPGEdge(
                    edge_id=op.edge_id,
                    graph_id=graph_id,
                    edge_type=EdgeType(op.edge_type),
                    source_node_id=op.source_node_id,
                    target_node_id=op.target_node_id,
                    created_in_version=version,
                    created_by_pid=op.created_by_pid,
                    created_at=_utcnow(),
                )
            )
        elif isinstance(op, AttachArtifactOp):
            art = ArtifactRefNode(
                node_id=f"{op.task_node_id}::{op.artifact.canonical_uri}@{op.artifact.version}",
                graph_id=graph_id,
                node_type=NodeType.ARTIFACT_REF,
                canonical_uri=op.artifact.canonical_uri,
                artifact_id=op.artifact.artifact_id,
                version=op.artifact.version,
                content_hash=op.artifact.content_hash,
                media_type=op.artifact.media_type,
                lifecycle=NodeLifecycle.ADMITTED,
                validity=NodeValidity.UNVERIFIED,
                created_in_version=version,
                updated_in_version=version,
                created_by_pid=op.created_by_pid,
                created_at=_utcnow(),
            )
            by_id[art.node_id] = art
            new_nodes.append(art)
            new_edges.append(
                VPGEdge(
                    edge_id=op.edge_id or f"{op.task_node_id}-produces-{art.node_id}",
                    graph_id=graph_id,
                    edge_type=EdgeType.PRODUCES,
                    source_node_id=op.task_node_id,
                    target_node_id=art.node_id,
                    created_in_version=version,
                    created_by_pid=op.created_by_pid,
                    created_at=_utcnow(),
                )
            )
        elif isinstance(op, AttachEvidenceOp):
            # EvidenceNode should already exist (created via AddNode
            # previously); edge is added only.
            n = by_id.get(op.evidence_node_id)
            if n is None and op.evidence_node_id:
                # Pre-construct placeholder if not previously admitted
                try:
                    n = EvidenceNode(
                        node_id=op.evidence_node_id,
                        graph_id=graph_id,
                        node_type=NodeType.EVIDENCE,
                        result=EvidenceResult.PASS,
                        produced_by_pid=op.created_by_pid,
                        created_by_pid=op.created_by_pid,
                        created_in_version=version,
                        updated_in_version=version,
                        lifecycle=NodeLifecycle.ADMITTED,
                        validity=NodeValidity.UNVERIFIED,
                        created_at=_utcnow(),
                    )
                    by_id[n.node_id] = n
                    new_nodes.append(n)
                except Exception:
                    n = None
            if n is not None:
                new_edges.append(
                    VPGEdge(
                        edge_id=op.edge_id or f"{op.verification_node_id}-produces-{op.evidence_node_id}",
                        graph_id=graph_id,
                        edge_type=EdgeType.PRODUCES,
                        source_node_id=op.verification_node_id,
                        target_node_id=op.evidence_node_id,
                        created_in_version=version,
                        created_by_pid=op.created_by_pid,
                        created_at=_utcnow(),
                    )
                )
    return new_nodes, new_edges


def _normalize_patch_operations(patch: GraphPatchProposal) -> None:
    """Ensure every operation is a concrete Pydantic op instance."""
    from .patches import AddEdgeOp, AddNodeOp, AttachArtifactOp, AttachEvidenceOp

    fixed = []
    for op in patch.operations:
        if isinstance(op, AddNodeOp | AddEdgeOp | AttachArtifactOp | AttachEvidenceOp):
            fixed.append(op)
        elif isinstance(op, dict):
            kind = op.get("op_type")
            if kind == "add_node":
                fixed.append(AddNodeOp(**op))
            elif kind == "add_edge":
                fixed.append(AddEdgeOp(**op))
            elif kind == "attach_artifact":
                fixed.append(AttachArtifactOp(**op))
            elif kind == "attach_evidence":
                fixed.append(AttachEvidenceOp(**op))
            else:
                raise VPGError(VPGCode.PATCH_REJECTED, f"unknown op: {kind}")
        else:
            raise VPGError(VPGCode.PATCH_REJECTED, f"bad op type: {type(op).__name__}")
    object.__setattr__(patch, "operations", tuple(fixed))


__all__ = [
    "GraphPatchProposal",
    "PatchCommitResult",
    "TaskDispatchCandidate",
    "VerifiedProgressRuntime",
]
