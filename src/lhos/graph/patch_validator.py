"""Graph patch validation and application (spec section 8.2).

Every patch must:
- check node existence;
- check expected_version;
- check legal state transitions;
- check for dependency cycles;
- check evidence existence;
- apply in a single transaction (a failed patch leaves no partial update).

Validation runs fully against an in-memory copy first; only when every op is
valid does the store get touched.
"""

from __future__ import annotations

import copy
from typing import Any

from lhos.domain.enums import EdgeKind, NodeState, PatchOperationType
from lhos.domain.errors import (
    CycleError,
    EdgeNotFoundError,
    NodeNotFoundError,
    PatchValidationError,
    VersionConflictError,
)
from lhos.domain.events import ActorType, EventType
from lhos.domain.graph_patch import GraphPatchOperation
from lhos.domain.models import EvidenceRef, GraphEdge, GraphNode
from lhos.graph.queries import ProgressGraph
from lhos.graph.state_machine import NodeStateMachine


class PatchValidator:
    def __init__(self, graph_store):  # noqa: ANN001 - SqliteGraphStore
        self._store = graph_store
        self._sm = NodeStateMachine()

    # ------------------------------------------------------------- validation
    def validate(self, run_id: str, ops: list[GraphPatchOperation]) -> None:
        graph = copy.deepcopy(self._store.load_graph(run_id))
        for op in ops:
            self._validate_op(graph, op)

    def _node(self, graph: ProgressGraph, node_id: str | None, op: str) -> GraphNode:
        if node_id is None or node_id not in graph.nodes:
            raise NodeNotFoundError(f"{op}: node {node_id!r} does not exist")
        return graph.nodes[node_id]

    def _check_version(self, node: GraphNode, expected: int | None) -> None:
        if expected is not None and node.version != expected:
            raise VersionConflictError(
                f"node {node.id}: expected version {expected}, found {node.version}"
            )

    def _validate_op(self, graph: ProgressGraph, op: GraphPatchOperation) -> None:
        if op.op == PatchOperationType.ADD_NODE:
            node_id = op.payload.get("id") or op.payload.get("temp_id")
            if not node_id:
                raise PatchValidationError("add_node requires payload.id or payload.temp_id")
            if node_id in graph.nodes:
                raise PatchValidationError(f"add_node: node {node_id!r} already exists")
            spec = op.payload
            node = GraphNode(
                id=node_id,
                run_id=graph.run_id,
                kind=spec.get("kind", "subtask"),
                title=spec.get("title", ""),
                specification=spec.get("specification", ""),
                schedulable=spec.get("schedulable", False),
                progress_weight=spec.get("progress_weight", 1.0),
                verification_spec=spec.get("verification_spec"),
                metadata=spec.get("metadata", {}),
            )
            graph.nodes[node.id] = node
            return

        if op.op == PatchOperationType.UPDATE_NODE:
            node = self._node(graph, op.target_id, "update_node")
            self._check_version(node, op.expected_version)
            # Simulate the update for downstream ops in the same patch.
            for field_name in ("title", "specification", "priority", "progress_weight",
                               "max_attempts", "verification_spec", "metadata",
                               "estimated_token_cost", "estimated_time_ms",
                               "estimated_tool_calls", "schedulable"):
                if field_name in op.payload:
                    setattr(node, field_name, op.payload[field_name])
            node.version += 1
            return

        if op.op == PatchOperationType.ADD_EDGE:
            source = op.payload.get("source") or op.payload.get("source_node_id")
            target = op.payload.get("target") or op.payload.get("target_node_id")
            kind = EdgeKind(op.payload.get("kind", "depends_on"))
            self._node(graph, source, "add_edge(source)")
            self._node(graph, target, "add_edge(target)")
            if kind == EdgeKind.DEPENDS_ON and graph.would_create_cycle(source, target):
                raise CycleError(
                    f"add_edge {source} -depends_on-> {target} would create a cycle"
                )
            graph.edges.append(
                GraphEdge(
                    run_id=graph.run_id, source_node_id=source,
                    target_node_id=target, kind=kind,
                )
            )
            return

        if op.op == PatchOperationType.REMOVE_EDGE:
            edge = self._find_edge(graph, op)
            edge.active = False
            edge.version += 1
            return

        if op.op == PatchOperationType.SET_STATE:
            node = self._node(graph, op.target_id, "set_state")
            self._check_version(node, op.expected_version)
            target = NodeState(op.payload["state"])
            if not self._sm.can_transition(node.state, target):
                raise PatchValidationError(
                    f"set_state: illegal transition {node.state} -> {target} "
                    f"for node {node.id}"
                )
            if target == NodeState.VERIFIED:
                evidence_ids = op.payload.get("evidence_ids") or []
                if not evidence_ids:
                    raise PatchValidationError(
                        "set_state to VERIFIED requires payload.evidence_ids"
                    )
                for ev_id in evidence_ids:
                    if not self._store.evidence_exists(ev_id):
                        raise PatchValidationError(
                            f"set_state: evidence {ev_id!r} does not exist"
                        )
            node.state = target
            node.version += 1
            return

        if op.op == PatchOperationType.ADD_EVIDENCE:
            node = self._node(graph, op.target_id, "add_evidence")
            self._check_version(node, op.expected_version)
            evidence_id = op.payload.get("evidence_id")
            if evidence_id is not None:
                if not self._store.evidence_exists(evidence_id):
                    raise PatchValidationError(
                        f"add_evidence: evidence {evidence_id!r} does not exist"
                    )
            elif "evidence" not in op.payload:
                raise PatchValidationError(
                    "add_evidence requires payload.evidence_id or payload.evidence"
                )
            return

        if op.op == PatchOperationType.MARK_STALE:
            node = self._node(graph, op.target_id, "mark_stale")
            self._check_version(node, op.expected_version)
            if not self._sm.can_transition(node.state, NodeState.STALE):
                raise PatchValidationError(
                    f"mark_stale: illegal transition {node.state} -> stale "
                    f"for node {node.id}"
                )
            node.state = NodeState.STALE
            node.version += 1
            return

        if op.op == PatchOperationType.INVALIDATE_NODE:
            node = self._node(graph, op.target_id, "invalidate_node")
            self._check_version(node, op.expected_version)
            if not self._sm.can_transition(node.state, NodeState.INVALIDATED):
                raise PatchValidationError(
                    f"invalidate_node: illegal transition {node.state} -> "
                    f"invalidated for node {node.id}"
                )
            node.state = NodeState.INVALIDATED
            node.version += 1
            return

        raise PatchValidationError(f"unsupported patch operation {op.op}")

    def _find_edge(self, graph: ProgressGraph, op: GraphPatchOperation) -> GraphEdge:
        edge_id = op.payload.get("edge_id") or op.target_id
        if edge_id:
            for e in graph.edges:
                if e.id == edge_id and e.active:
                    return e
            raise EdgeNotFoundError(f"remove_edge: edge {edge_id!r} not found")
        source = op.payload.get("source") or op.payload.get("source_node_id")
        target = op.payload.get("target") or op.payload.get("target_node_id")
        kind = EdgeKind(op.payload.get("kind", "depends_on"))
        for e in graph.edges:
            if (
                e.active
                and e.source_node_id == source
                and e.target_node_id == target
                and e.kind == kind
            ):
                return e
        raise EdgeNotFoundError(
            f"remove_edge: no active edge {source} -{kind}-> {target}"
        )

    # ------------------------------------------------------------ application
    def validate_and_apply(
        self,
        run_id: str,
        ops: list[GraphPatchOperation],
        actor: str = ActorType.RECONCILER,
    ) -> None:
        """Validate all ops, then apply in ONE transaction (spec 8.2)."""
        self.validate(run_id, ops)  # raises before anything is written
        db = self._store._db  # store-internal transaction composition
        with db.transaction():
            for op in ops:
                self._apply_op(run_id, op, actor)

    def _apply_op(self, run_id: str, op: GraphPatchOperation, actor: str) -> None:
        if op.op == PatchOperationType.ADD_NODE:
            spec = op.payload
            node = GraphNode(
                id=spec.get("id") or spec.get("temp_id"),
                run_id=run_id,
                kind=spec.get("kind", "subtask"),
                title=spec.get("title", ""),
                specification=spec.get("specification", ""),
                schedulable=spec.get("schedulable", False),
                progress_weight=spec.get("progress_weight", 1.0),
                verification_spec=spec.get("verification_spec"),
                metadata=spec.get("metadata", {}),
            )
            self._store.add_node(node, actor=actor)
            return

        if op.op == PatchOperationType.UPDATE_NODE:
            node = self._store.get_node(op.target_id)  # type: ignore[arg-type]
            for field_name in ("title", "specification", "priority", "progress_weight",
                               "max_attempts", "verification_spec", "metadata",
                               "estimated_token_cost", "estimated_time_ms",
                               "estimated_tool_calls", "schedulable"):
                if field_name in op.payload:
                    setattr(node, field_name, op.payload[field_name])
            self._store.update_node(node, actor=actor)
            return

        if op.op == PatchOperationType.ADD_EDGE:
            self._store.add_edge(
                GraphEdge(
                    run_id=run_id,
                    source_node_id=op.payload.get("source") or op.payload["source_node_id"],
                    target_node_id=op.payload.get("target") or op.payload["target_node_id"],
                    kind=EdgeKind(op.payload.get("kind", "depends_on")),
                    metadata=op.payload.get("metadata", {}),
                ),
                actor=actor,
            )
            return

        if op.op == PatchOperationType.REMOVE_EDGE:
            graph = self._store.load_graph(run_id)
            edge = self._find_edge(graph, op)
            self._store.remove_edge(edge.id, actor=actor)
            return

        if op.op == PatchOperationType.SET_STATE:
            self._store.set_state(
                op.target_id,  # type: ignore[arg-type]
                NodeState(op.payload["state"]),
                actor=actor,
                evidence_ids=op.payload.get("evidence_ids"),
            )
            return

        if op.op == PatchOperationType.ADD_EVIDENCE:
            evidence_id = op.payload.get("evidence_id")
            if evidence_id is None:
                raw: dict[str, Any] = dict(op.payload["evidence"])
                raw.setdefault("run_id", run_id)
                raw.setdefault("metadata", {})
                raw["metadata"].setdefault("node_id", op.target_id)
                ev = EvidenceRef(**raw)
                self._store.add_evidence(ev, actor=actor)
            return

        if op.op == PatchOperationType.MARK_STALE:
            self._store.set_state(
                op.target_id,  # type: ignore[arg-type]
                NodeState.STALE,
                actor=actor,
                event_type=EventType.NODE_MARKED_STALE,
            )
            return

        if op.op == PatchOperationType.INVALIDATE_NODE:
            self._store.set_state(
                op.target_id,  # type: ignore[arg-type]
                NodeState.INVALIDATED,
                actor=actor,
                event_type=EventType.NODE_INVALIDATED,
            )
            return

        raise PatchValidationError(f"unsupported patch operation {op.op}")
