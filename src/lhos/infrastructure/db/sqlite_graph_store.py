"""SQLite graph store: materialized progress-graph projection (spec 4, 7, 19).

Every mutation appends its event and updates the projection in the SAME
database transaction (spec section 5.3). Event payloads carry the full
node/edge dump so the projection can be rebuilt exactly from the event log
(spec section 26.2 event replay).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from lhos.domain.enums import EdgeKind, NodeState
from lhos.domain.errors import (
    EdgeNotFoundError,
    EvidenceRequiredError,
    NodeNotFoundError,
    RunNotFoundError,
    VersionConflictError,
)
from lhos.domain.events import ActorType, EventType, RuntimeEvent
from lhos.domain.models import EvidenceRef, ExecutionRecord, GraphEdge, GraphNode, Run
from lhos.graph.queries import ProgressGraph
from lhos.graph.state_machine import NodeStateMachine
from lhos.infrastructure.db.connection import Database
from lhos.infrastructure.db.sqlite_event_store import SqliteEventStore


def _now() -> datetime:
    return datetime.now().astimezone()


class SqliteGraphStore:
    def __init__(self, db: Database, event_store: SqliteEventStore):
        self._db = db
        self._events = event_store
        self._sm = NodeStateMachine()

    # ------------------------------------------------------------------ runs
    def create_run(self, run_id: str, goal: str, config: dict[str, Any]) -> Run:
        run = Run(id=run_id, goal=goal, status="pending", config=config)
        with self._db.transaction():
            self._insert_run_row(run)
            self._events.append(
                RuntimeEvent(
                    run_id=run_id,
                    event_type=EventType.RUN_CREATED,
                    actor_type=ActorType.SYSTEM,
                    payload={"run": run.model_dump(mode="json"), "goal": goal},
                )
            )
        return run

    def get_run(self, run_id: str) -> Run:
        row = self._db.conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            raise RunNotFoundError(f"run {run_id} not found")
        return self._row_to_run(row)

    def set_run_status(
        self,
        run_id: str,
        status: str,
        event_type: str | None = None,
        payload_extra: dict | None = None,
    ) -> Run:
        run = self.get_run(run_id)
        run.status = status
        run.updated_at = _now()
        with self._db.transaction():
            self._update_run_row(run)
            if event_type:
                payload: dict = {"run_id": run_id, "status": status}
                if payload_extra:
                    payload.update(payload_extra)
                self._events.append(
                    RuntimeEvent(
                        run_id=run_id,
                        event_type=event_type,
                        actor_type=ActorType.SYSTEM,
                        payload=payload,
                    )
                )
        return run

    # ----------------------------------------------------------------- nodes
    def add_node(self, node: GraphNode, actor: str = ActorType.SYSTEM) -> GraphNode:
        with self._db.transaction():
            self._upsert_node_row(node)
            self._events.append(
                RuntimeEvent(
                    run_id=node.run_id,
                    event_type=EventType.NODE_ADDED,
                    actor_type=actor,
                    payload={"node": node.model_dump(mode="json")},
                )
            )
        return node

    def get_node(self, node_id: str) -> GraphNode:
        row = self._db.conn.execute("SELECT * FROM nodes WHERE id = ?", (node_id,)).fetchone()
        if row is None:
            raise NodeNotFoundError(f"node {node_id} not found")
        return self._row_to_node(row)

    def set_state(
        self,
        node_id: str,
        target: NodeState,
        actor: str,
        evidence_ids: list[str] | None = None,
        event_type: str = EventType.NODE_STATE_CHANGED,
        payload_extra: dict[str, Any] | None = None,
        force: bool = False,
    ) -> GraphNode:
        """Transition a node's state. Enforces spec section 7 invariants:
        - legal state-machine transition (unless ``force`` for reconciler-level
          system transitions such as CLAIMED_DONE -> STALE during invalidation);
        - VERIFIED requires at least one evidence id.
        """
        node = self.get_node(node_id)
        if target == NodeState.VERIFIED and not evidence_ids:
            raise EvidenceRequiredError(f"node {node_id} cannot become VERIFIED without evidence")
        event = RuntimeEvent(
            run_id=node.run_id,
            event_type=event_type,
            actor_type=actor,
            evidence_ids=evidence_ids or [],
            payload={},
        )
        if force:
            if node.state == target:
                return node
            from_state = node.state
            node.state = target
            node.version += 1
            node.updated_at = _now()
        else:
            from_state = node.state
            node = self._sm.transition(node, target, event)
        payload: dict[str, Any] = {
            "node": node.model_dump(mode="json"),
            "node_id": node.id,
            "from_state": str(from_state),
            "to_state": str(target),
        }
        if payload_extra:
            payload.update(payload_extra)
        event.payload = payload
        with self._db.transaction():
            self._upsert_node_row(node)
            self._events.append(event)
        return node

    def update_node(
        self,
        node: GraphNode,
        actor: str,
        expected_version: int | None = None,
        event_type: str = EventType.NODE_UPDATED,
        payload_extra: dict[str, Any] | None = None,
        bump_version: bool = True,
    ) -> GraphNode:
        current = self.get_node(node.id)
        if expected_version is not None and current.version != expected_version:
            raise VersionConflictError(
                f"node {node.id}: expected version {expected_version}, found {current.version}"
            )
        if bump_version:
            node.version = current.version + 1
        else:
            node.version = current.version
        node.updated_at = _now()
        payload: dict[str, Any] = {"node": node.model_dump(mode="json")}
        if payload_extra:
            payload.update(payload_extra)
        with self._db.transaction():
            self._upsert_node_row(node)
            self._events.append(
                RuntimeEvent(
                    run_id=node.run_id,
                    event_type=event_type,
                    actor_type=actor,
                    payload=payload,
                )
            )
        return node

    # ----------------------------------------------------------------- edges
    def add_edge(self, edge: GraphEdge, actor: str = ActorType.SYSTEM) -> GraphEdge:
        # Confirm both endpoints exist.
        self.get_node(edge.source_node_id)
        self.get_node(edge.target_node_id)
        with self._db.transaction():
            self._upsert_edge_row(edge)
            self._events.append(
                RuntimeEvent(
                    run_id=edge.run_id,
                    event_type=EventType.EDGE_ADDED,
                    actor_type=actor,
                    payload={"edge": edge.model_dump(mode="json")},
                )
            )
        return edge

    def remove_edge(self, edge_id: str, actor: str = ActorType.SYSTEM) -> GraphEdge:
        row = self._db.conn.execute("SELECT * FROM edges WHERE id = ?", (edge_id,)).fetchone()
        if row is None:
            raise EdgeNotFoundError(f"edge {edge_id} not found")
        edge = self._row_to_edge(row)
        edge.active = False
        edge.version += 1
        edge.updated_at = _now()
        with self._db.transaction():
            self._upsert_edge_row(edge)
            self._events.append(
                RuntimeEvent(
                    run_id=edge.run_id,
                    event_type=EventType.EDGE_REMOVED,
                    actor_type=actor,
                    payload={"edge": edge.model_dump(mode="json"), "edge_id": edge.id},
                )
            )
        return edge

    # -------------------------------------------------------------- evidence
    def add_evidence(
        self,
        evidence: EvidenceRef,
        actor: str = ActorType.SYSTEM,
        event_type: str = EventType.ARTIFACT_CREATED,
        payload_extra: dict[str, Any] | None = None,
    ) -> EvidenceRef:
        payload: dict[str, Any] = {"evidence": [evidence.model_dump(mode="json")]}
        if payload_extra:
            payload.update(payload_extra)
        with self._db.transaction():
            self._insert_evidence_row(evidence)
            self._events.append(
                RuntimeEvent(
                    run_id=evidence.run_id,
                    event_type=event_type,
                    actor_type=actor,
                    evidence_ids=[evidence.id],
                    payload=payload,
                )
            )
        return evidence

    def add_evidence_batch(
        self,
        run_id: str,
        evidences: list[EvidenceRef],
        actor: str,
        event_type: str,
        payload_extra: dict[str, Any] | None = None,
    ) -> list[EvidenceRef]:
        payload: dict[str, Any] = {"evidence": [e.model_dump(mode="json") for e in evidences]}
        if payload_extra:
            payload.update(payload_extra)
        with self._db.transaction():
            for ev in evidences:
                self._insert_evidence_row(ev)
            self._events.append(
                RuntimeEvent(
                    run_id=run_id,
                    event_type=event_type,
                    actor_type=actor,
                    evidence_ids=[e.id for e in evidences],
                    payload=payload,
                )
            )
        return evidences

    def evidence_exists(self, evidence_id: str) -> bool:
        row = self._db.conn.execute(
            "SELECT 1 FROM evidence WHERE id = ?", (evidence_id,)
        ).fetchone()
        return row is not None

    def get_evidence(self, evidence_id: str) -> EvidenceRef:
        row = self._db.conn.execute(
            "SELECT * FROM evidence WHERE id = ?", (evidence_id,)
        ).fetchone()
        if row is None:
            from lhos.domain.errors import EvidenceNotFoundError

            raise EvidenceNotFoundError(f"evidence {evidence_id} not found")
        return self._row_to_evidence(row)

    def list_evidence(self, run_id: str) -> list[EvidenceRef]:
        rows = self._db.conn.execute(
            "SELECT * FROM evidence WHERE run_id = ? ORDER BY created_at ASC", (run_id,)
        ).fetchall()
        return [self._row_to_evidence(r) for r in rows]

    # ---------------------------------------------------------------- leases
    def acquire_lease(self, node_id: str, worker_id: str, lease_seconds: int) -> bool:
        """Acquire a lease; returns False when another worker holds a valid one.
        Expired leases are treated as free (spec section 16.3)."""
        with self._db.transaction():
            node = self.get_node(node_id)
            if (
                node.lease_owner is not None
                and node.lease_expires_at is not None
                and node.lease_expires_at > _now()
                and node.lease_owner != worker_id
            ):
                return False
            node.lease_owner = worker_id
            node.lease_expires_at = _now() + timedelta(seconds=lease_seconds)
            node.updated_at = _now()
            self._upsert_node_row(node)
            self._events.append(
                RuntimeEvent(
                    run_id=node.run_id,
                    event_type=EventType.NODE_LEASED,
                    actor_type=ActorType.SYSTEM,
                    actor_id=worker_id,
                    payload={
                        "node": node.model_dump(mode="json"),
                        "node_id": node.id,
                        "worker_id": worker_id,
                        "lease_expires_at": node.lease_expires_at.isoformat(),
                    },
                )
            )
        return True

    def release_lease(self, node_id: str, actor: str = ActorType.SYSTEM) -> None:
        with self._db.transaction():
            node = self.get_node(node_id)
            if node.lease_owner is None:
                return
            node.lease_owner = None
            node.lease_expires_at = None
            node.updated_at = _now()
            self._upsert_node_row(node)
            self._events.append(
                RuntimeEvent(
                    run_id=node.run_id,
                    event_type=EventType.NODE_LEASE_RELEASED,
                    actor_type=actor,
                    payload={"node": node.model_dump(mode="json"), "node_id": node.id},
                )
            )

    # ---------------------------------------------------------------- queries
    def list_ready_nodes(self, run_id: str) -> list[GraphNode]:
        return self.list_nodes(run_id, NodeState.READY)

    def has_waiting_nodes(self, run_id: str) -> bool:
        row = self._db.conn.execute(
            "SELECT 1 FROM nodes WHERE run_id = ? AND state = ? LIMIT 1",
            (run_id, str(NodeState.WAITING)),
        ).fetchone()
        return row is not None

    def list_nodes(self, run_id: str, state: NodeState | None = None) -> list[GraphNode]:
        if state is None:
            rows = self._db.conn.execute(
                "SELECT * FROM nodes WHERE run_id = ?", (run_id,)
            ).fetchall()
        else:
            rows = self._db.conn.execute(
                "SELECT * FROM nodes WHERE run_id = ? AND state = ?",
                (run_id, str(state)),
            ).fetchall()
        return [self._row_to_node(r) for r in rows]

    def list_edges(self, run_id: str, active_only: bool = False) -> list[GraphEdge]:
        if active_only:
            rows = self._db.conn.execute(
                "SELECT * FROM edges WHERE run_id = ? AND active = 1", (run_id,)
            ).fetchall()
        else:
            rows = self._db.conn.execute(
                "SELECT * FROM edges WHERE run_id = ?", (run_id,)
            ).fetchall()
        return [self._row_to_edge(r) for r in rows]

    def load_graph(self, run_id: str) -> ProgressGraph:
        nodes = {n.id: n for n in self.list_nodes(run_id)}
        edges = self.list_edges(run_id)
        return ProgressGraph(run_id=run_id, nodes=nodes, edges=edges)

    # ------------------------------------------------------------ executions
    def insert_execution(self, record: ExecutionRecord) -> ExecutionRecord:
        self._db.conn.execute(
            """
            INSERT INTO executions(
                id, run_id, node_id, attempt_number, context_hash, model_name,
                status, input_tokens, output_tokens, tool_calls, cost_usd,
                started_at, finished_at, result_json, error_json,
                checkpoint_before, checkpoint_after
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.id,
                record.run_id,
                record.node_id,
                record.attempt_number,
                record.context_hash,
                record.model_name,
                record.status,
                record.input_tokens,
                record.output_tokens,
                record.tool_calls,
                record.cost_usd,
                record.started_at.isoformat(),
                record.finished_at.isoformat() if record.finished_at else None,
                json.dumps(record.result) if record.result is not None else None,
                json.dumps(record.error) if record.error is not None else None,
                record.checkpoint_before,
                record.checkpoint_after,
            ),
        )
        return record

    def finish_execution(
        self,
        execution_id: str,
        status: str,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        checkpoint_after: str | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        tool_calls: int | None = None,
    ) -> None:
        self._db.conn.execute(
            """
            UPDATE executions
            SET status = ?, finished_at = ?, result_json = ?, error_json = ?,
                checkpoint_after = COALESCE(?, checkpoint_after),
                input_tokens = COALESCE(?, input_tokens),
                output_tokens = COALESCE(?, output_tokens),
                tool_calls = COALESCE(?, tool_calls)
            WHERE id = ?
            """,
            (
                status,
                _now().isoformat(),
                json.dumps(result) if result is not None else None,
                json.dumps(error) if error is not None else None,
                checkpoint_after,
                input_tokens,
                output_tokens,
                tool_calls,
                execution_id,
            ),
        )

    def list_executions(self, run_id: str, node_id: str | None = None) -> list[ExecutionRecord]:
        if node_id is None:
            rows = self._db.conn.execute(
                "SELECT * FROM executions WHERE run_id = ? ORDER BY started_at ASC",
                (run_id,),
            ).fetchall()
        else:
            rows = self._db.conn.execute(
                "SELECT * FROM executions WHERE run_id = ? AND node_id = ? "
                "ORDER BY attempt_number ASC",
                (run_id, node_id),
            ).fetchall()
        return [self._row_to_execution(r) for r in rows]

    # --------------------------------------------------------- serialization
    def _insert_run_row(self, run: Run) -> None:
        self._db.conn.execute(
            "INSERT INTO runs(id, goal, status, config_json, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                run.id,
                run.goal,
                run.status,
                json.dumps(run.config, sort_keys=True, default=str),
                run.created_at.isoformat(),
                run.updated_at.isoformat(),
            ),
        )

    def _update_run_row(self, run: Run) -> None:
        self._db.conn.execute(
            "UPDATE runs SET goal = ?, status = ?, config_json = ?, updated_at = ? WHERE id = ?",
            (
                run.goal,
                run.status,
                json.dumps(run.config, sort_keys=True, default=str),
                run.updated_at.isoformat(),
                run.id,
            ),
        )

    def _upsert_node_row(self, node: GraphNode) -> None:
        self._db.conn.execute(
            """
            INSERT OR REPLACE INTO nodes(
                id, run_id, kind, title, specification, state, version,
                schedulable, priority, progress_weight, estimated_token_cost,
                estimated_time_ms, estimated_tool_calls, actual_token_cost,
                actual_time_ms, actual_tool_calls, attempt_count, max_attempts,
                verification_attempts, parse_attempts, tool_attempts,
                verification_spec_json, metadata_json, lease_owner,
                lease_expires_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                node.id,
                node.run_id,
                str(node.kind),
                node.title,
                node.specification,
                str(node.state),
                node.version,
                1 if node.schedulable else 0,
                node.priority,
                node.progress_weight,
                node.estimated_token_cost,
                node.estimated_time_ms,
                node.estimated_tool_calls,
                node.actual_token_cost,
                node.actual_time_ms,
                node.actual_tool_calls,
                node.attempt_count,
                node.max_attempts,
                node.verification_attempts,
                node.parse_attempts,
                node.tool_attempts,
                json.dumps(node.verification_spec, sort_keys=True, default=str)
                if node.verification_spec is not None
                else None,
                json.dumps(node.metadata, sort_keys=True, default=str),
                node.lease_owner,
                node.lease_expires_at.isoformat() if node.lease_expires_at else None,
                node.created_at.isoformat(),
                node.updated_at.isoformat(),
            ),
        )

    def _upsert_edge_row(self, edge: GraphEdge) -> None:
        self._db.conn.execute(
            """
            INSERT OR REPLACE INTO edges(
                id, run_id, source_node_id, target_node_id, kind, active,
                version, metadata_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                edge.id,
                edge.run_id,
                edge.source_node_id,
                edge.target_node_id,
                str(edge.kind),
                1 if edge.active else 0,
                edge.version,
                json.dumps(edge.metadata, sort_keys=True, default=str),
                edge.created_at.isoformat(),
                edge.updated_at.isoformat(),
            ),
        )

    def _insert_evidence_row(self, evidence: EvidenceRef) -> None:
        self._db.conn.execute(
            """
            INSERT OR IGNORE INTO evidence(
                id, run_id, evidence_type, source_event_id, uri, content_hash,
                summary, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                evidence.id,
                evidence.run_id,
                evidence.evidence_type,
                evidence.source_event_id,
                evidence.uri,
                evidence.content_hash,
                evidence.summary,
                json.dumps(evidence.metadata, sort_keys=True, default=str),
                evidence.created_at.isoformat(),
            ),
        )

    @staticmethod
    def _row_to_run(row) -> Run:
        return Run(
            id=row["id"],
            goal=row["goal"],
            status=row["status"],
            config=json.loads(row["config_json"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _row_to_node(row) -> GraphNode:
        return GraphNode(
            id=row["id"],
            run_id=row["run_id"],
            kind=row["kind"],
            title=row["title"],
            specification=row["specification"],
            state=row["state"],
            version=row["version"],
            schedulable=bool(row["schedulable"]),
            priority=row["priority"],
            progress_weight=row["progress_weight"],
            estimated_token_cost=row["estimated_token_cost"],
            estimated_time_ms=row["estimated_time_ms"],
            estimated_tool_calls=row["estimated_tool_calls"],
            actual_token_cost=row["actual_token_cost"],
            actual_time_ms=row["actual_time_ms"],
            actual_tool_calls=row["actual_tool_calls"],
            attempt_count=row["attempt_count"],
            max_attempts=row["max_attempts"],
            verification_attempts=row["verification_attempts"]
            if "verification_attempts" in row
            else 0,  # noqa: SIM401
            parse_attempts=row["parse_attempts"] if "parse_attempts" in row else 0,  # noqa: SIM401
            tool_attempts=row["tool_attempts"] if "tool_attempts" in row else 0,  # noqa: SIM401
            verification_spec=json.loads(row["verification_spec_json"])
            if row["verification_spec_json"]
            else None,
            metadata=json.loads(row["metadata_json"]),
            lease_owner=row["lease_owner"],
            lease_expires_at=datetime.fromisoformat(row["lease_expires_at"])
            if row["lease_expires_at"]
            else None,
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _row_to_edge(row) -> GraphEdge:
        return GraphEdge(
            id=row["id"],
            run_id=row["run_id"],
            source_node_id=row["source_node_id"],
            target_node_id=row["target_node_id"],
            kind=EdgeKind(row["kind"]),
            active=bool(row["active"]),
            version=row["version"],
            metadata=json.loads(row["metadata_json"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _row_to_evidence(row) -> EvidenceRef:
        return EvidenceRef(
            id=row["id"],
            run_id=row["run_id"],
            evidence_type=row["evidence_type"],
            source_event_id=row["source_event_id"],
            uri=row["uri"],
            content_hash=row["content_hash"],
            summary=row["summary"],
            metadata=json.loads(row["metadata_json"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _row_to_execution(row) -> ExecutionRecord:
        return ExecutionRecord(
            id=row["id"],
            run_id=row["run_id"],
            node_id=row["node_id"],
            attempt_number=row["attempt_number"],
            context_hash=row["context_hash"],
            model_name=row["model_name"],
            status=row["status"],
            input_tokens=row["input_tokens"],
            output_tokens=row["output_tokens"],
            tool_calls=row["tool_calls"],
            cost_usd=row["cost_usd"],
            started_at=datetime.fromisoformat(row["started_at"]),
            finished_at=datetime.fromisoformat(row["finished_at"]) if row["finished_at"] else None,
            result=json.loads(row["result_json"]) if row["result_json"] else None,
            error=json.loads(row["error_json"]) if row["error_json"] else None,
            checkpoint_before=row["checkpoint_before"],
            checkpoint_after=row["checkpoint_after"],
        )
