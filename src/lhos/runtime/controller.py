"""Runtime controller: the main loop (spec section 18). Single worker,
sequential — parallelism is a later ablation (spec 11.3).
"""

from __future__ import annotations

import time
from typing import Any

from lhos.domain.enums import NodeState
from lhos.domain.errors import PatchValidationError, SimulatedCrashError
from lhos.domain.events import ActorType, EventType, RuntimeEvent
from lhos.domain.graph_patch import GraphPatchOperation
from lhos.domain.models import ExecutionRecord, GraphNode, Run
from lhos.graph.patch_validator import PatchValidator
from lhos.graph.readiness import ReadinessRefresher
from lhos.graph.reconciler import DeterministicReconciler
from lhos.runtime.budget_manager import BudgetManager
from lhos.runtime.context_compiler import ContextCompiler, ContextRequest
from lhos.runtime.recovery import RecoveryManager
from lhos.runtime.scheduler import ResourceState
from lhos.runtime.termination import TerminationDecision, TerminationEvaluator
from lhos.runtime.verification_gate import VerificationGate

_MAX_ITERATIONS = 10_000


class RuntimeController:
    def __init__(
        self,
        graph_store,
        event_store,
        scheduler,
        context_compiler: ContextCompiler,
        worker,
        verification_gate: VerificationGate,
        budget_manager: BudgetManager,
        checkpoint_manager,
        readiness: ReadinessRefresher,
        reconciler: DeterministicReconciler,
        patch_validator: PatchValidator,
        recovery: RecoveryManager,
        termination: TerminationEvaluator | None = None,
        config: dict[str, Any] | None = None,
        worker_id: str = "worker-1",
        workspace_dir: str | None = None,
    ):
        self._store = graph_store
        self._events = event_store
        self._scheduler = scheduler
        self._compiler = context_compiler
        self._worker = worker
        self._gate = verification_gate
        self._budget = budget_manager
        self._checkpoints = checkpoint_manager
        self._readiness = readiness
        self._reconciler = reconciler
        self._patches = patch_validator
        self._recovery = recovery
        self._termination = termination or TerminationEvaluator()
        self._config = config or {}
        self._worker_id = worker_id
        self._workspace_dir = workspace_dir
        self._resources = ResourceState()
        # §24.2 instrumentation: accumulated seconds, exported in the run
        # finish event payload.
        self._scheduler_seconds = 0.0
        self._checkpoint_seconds = 0.0

    # ------------------------------------------------------------ properties
    @property
    def _lease_seconds(self) -> int:
        return int(self._config.get("runtime", {}).get("lease_seconds", 600))

    @property
    def _context_config(self) -> dict[str, Any]:
        return self._config.get("context", {})

    @property
    def _checkpoint_config(self) -> dict[str, Any]:
        return self._config.get("checkpoint", {})

    @staticmethod
    def _script(node: GraphNode) -> dict[str, Any]:
        return node.metadata.get("script", {})

    def _inject_crash_once(self, run_id: str, node: GraphNode, flag: str) -> bool:
        """Fire-once crash injection (spec 26.2): the flag lives in the node
        script, but the runtime must not crash again after resume. A persisted
        CRASH_INJECTED marker event makes the injection process-safe."""
        if not self._script(node).get(flag):
            return False
        prior = [
            e
            for e in self._events.list_events(run_id)
            if e.event_type == "CRASH_INJECTED"
            and e.payload.get("node_id") == node.id
            and e.payload.get("point") == flag
        ]
        if prior:
            return False
        self._events.append(
            RuntimeEvent(
                run_id=run_id,
                event_type="CRASH_INJECTED",
                actor_type=ActorType.SYSTEM,
                payload={"node_id": node.id, "point": flag},
            )
        )
        return True

    def _checkpoint_before(self, run_id: str, node: GraphNode) -> str:
        """Pre-execution snapshot. Git records HEAD without committing
        (spec 16.2); filesystem takes a tar snapshot; noop returns an id."""
        recorder = getattr(self._checkpoints, "record", None) or self._checkpoints.create
        _t = time.perf_counter()
        checkpoint_id = recorder(run_id, reason=f"before:{node.id}")
        self._checkpoint_seconds += time.perf_counter() - _t
        self._events.append(
            RuntimeEvent(
                run_id=run_id,
                event_type=EventType.CHECKPOINT_CREATED,
                actor_type=ActorType.SYSTEM,
                payload={"checkpoint_id": checkpoint_id, "reason": f"before:{node.id}"},
            )
        )
        return checkpoint_id

    def _restore_checkpoint(
        self, run_id: str, node_id: str, checkpoint_id: str, reason: str
    ) -> None:
        _t = time.perf_counter()
        self._checkpoints.restore(checkpoint_id)
        self._checkpoint_seconds += time.perf_counter() - _t
        self._events.append(
            RuntimeEvent(
                run_id=run_id,
                event_type=EventType.CHECKPOINT_RESTORED,
                actor_type=ActorType.SYSTEM,
                payload={"checkpoint_id": checkpoint_id, "node_id": node_id, "reason": reason},
            )
        )

    # -------------------------------------------------------------- main loop
    def run(self, run_id: str) -> Run:
        run = self._store.get_run(run_id)
        if run.status == "pending":
            self._store.set_run_status(run_id, "running")
        iterations = 0
        while True:
            iterations += 1
            if iterations > _MAX_ITERATIONS:
                return self._store.set_run_status(run_id, "failed", event_type=EventType.RUN_FAILED)

            self._reconciler.project_new_events(run_id)
            self._readiness.refresh(run_id, self._budget.get_state(run_id))

            graph = self._store.load_graph(run_id)
            decision = self._termination.evaluate(graph)
            if decision.should_stop:
                return self._finish_run(run_id, decision)

            if not self._budget.can_continue(run_id):
                return self._store.set_run_status(run_id, "paused", event_type=EventType.RUN_PAUSED)

            ready_nodes = self._store.list_ready_nodes(run_id)
            if not ready_nodes:
                if self._store.has_waiting_nodes(run_id):
                    # External event required (e.g. manual verification).
                    return self._store.set_run_status(
                        run_id, "paused", event_type=EventType.RUN_PAUSED
                    )
                return self._store.set_run_status(run_id, "failed", event_type=EventType.RUN_FAILED)

            _t_sel = time.perf_counter()
            node = self._scheduler.select(
                ready_nodes=ready_nodes,
                graph=graph,
                budget=self._budget.get_state(run_id),
                resources=self._resources,
            )
            self._scheduler_seconds += time.perf_counter() - _t_sel
            if node is None:
                return self._store.set_run_status(run_id, "paused", event_type=EventType.RUN_PAUSED)

            if not self._store.acquire_lease(node.id, self._worker_id, self._lease_seconds):
                continue
            try:
                self.execute_node(run_id, node)
            finally:
                self._store.release_lease(node.id)

    def resume(self, run_id: str) -> Run:
        """Crash-safe resume (spec 16.3): recover, then continue the loop."""
        summary = self._recovery.recover(run_id)
        self._events.append(
            RuntimeEvent(
                run_id=run_id,
                event_type=EventType.RUN_RESUMED,
                actor_type=ActorType.SYSTEM,
                payload={"recovery": summary},
            )
        )
        self._store.set_run_status(run_id, "running")
        return self.run(run_id)

    # ------------------------------------------------------------- node exec
    def execute_node(self, run_id: str, node: GraphNode) -> None:
        checkpoint_id = self._checkpoint_before(run_id, node)

        if self._inject_crash_once(run_id, node, "crash_before_execution"):
            # Crash point (26.2): after lease/checkpoint, before EXECUTION_STARTED.
            raise SimulatedCrashError(f"simulated crash before execution of {node.id}")

        baselines = self._gate.pre_execute_baselines(node)
        ctx_cfg = self._context_config
        context = self._compiler.compile(
            ContextRequest(
                run_id=run_id,
                node_id=node.id,
                max_tokens=int(ctx_cfg.get("max_tokens", 12000)),
                include_last_failures=int(ctx_cfg.get("include_last_failures", 2)),
                max_dependency_hops=int(ctx_cfg.get("max_dependency_hops", 3)),
            )
        )

        started = time.monotonic()
        node = self._store.get_node(node.id)
        node.attempt_count += 1
        node = self._store.update_node(node, actor=ActorType.WORKER)
        execution = ExecutionRecord(
            run_id=run_id,
            node_id=node.id,
            attempt_number=node.attempt_count,
            context_hash=context.context_hash,
            model_name="fake-worker",
            checkpoint_before=checkpoint_id,
        )
        retry_reason = node.metadata.get("replanned_from") or (
            node.metadata.get("ready_from_state")
            if node.metadata.get("ready_from_state") in {"stale", "failed"}
            else None
        )
        with self._store._db.transaction():
            self._store.set_state(
                node.id,
                NodeState.RUNNING,
                actor=ActorType.WORKER,
                event_type=EventType.EXECUTION_STARTED,
                payload_extra={
                    "attempt": node.attempt_count,
                    "context_hash": context.context_hash,
                    "checkpoint_before": checkpoint_id,
                    "retry_reason": retry_reason,
                },
            )
            self._store.insert_execution(execution)

        result = self._worker.execute(node, context)  # may raise SimulatedCrashError
        elapsed_ms = int((time.monotonic() - started) * 1000)

        # Spec 14.1: a worker can never self-verify.
        status = "claimed_done" if result.status == "verified" else result.status
        self._budget.record_model_usage(run_id, result.input_tokens, result.output_tokens)
        execution.input_tokens = result.input_tokens
        execution.output_tokens = result.output_tokens
        execution.tool_calls = result.tool_call_count

        if status == "failed":
            self._store.set_state(
                node.id,
                NodeState.FAILED,
                actor=ActorType.WORKER,
                event_type=EventType.EXECUTION_FAILED,
                payload_extra={"summary": result.summary, "attempt": node.attempt_count},
            )
            self._store.finish_execution(
                execution.id,
                status="failed",
                result=result.model_dump(),
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                tool_calls=result.tool_call_count,
            )
            if self._checkpoint_config.get("restore_on_failure", False):
                self._restore_checkpoint(run_id, node.id, checkpoint_id, reason="execution failed")
            self._ingest_environment_events(run_id, result.environment_events)
            return

        if status == "waiting" or self._gate.is_manual(node, result.verification_request):
            self._events.append(
                RuntimeEvent(
                    run_id=run_id,
                    event_type=EventType.VERIFICATION_STARTED,
                    actor_type=ActorType.VERIFIER,
                    actor_id=node.id,
                    payload={"node_id": node.id, "pending_manual": True},
                )
            )
            self._store.set_state(
                node.id,
                NodeState.WAITING,
                actor=ActorType.WORKER,
                payload_extra={"reason": "waiting for external/manual event"},
            )
            self._store.finish_execution(
                execution.id,
                status="waiting",
                result=result.model_dump(),
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                tool_calls=result.tool_call_count,
            )
            return

        # claimed_done: CLAIM_SUBMITTED + state change in one transaction.
        claim_event: RuntimeEvent | None = None
        with self._store._db.transaction():
            node = self._store.set_state(
                node.id,
                NodeState.CLAIMED_DONE,
                actor=ActorType.WORKER,
                payload_extra={"summary": result.summary},
            )
            claim_event = self._events.append(
                RuntimeEvent(
                    run_id=run_id,
                    event_type=EventType.CLAIM_SUBMITTED,
                    actor_type=ActorType.WORKER,
                    actor_id=node.id,
                    payload={
                        "node": node.model_dump(mode="json"),
                        "node_id": node.id,
                        "summary": result.summary,
                        "produced_artifacts": result.produced_artifacts,
                    },
                )
            )

        if self._inject_crash_once(run_id, node, "crash_before_verification"):
            # Crash point (26.2): claim persisted, verification not started.
            raise SimulatedCrashError(f"simulated crash before verification of {node.id}")

        outcome = self._gate.verify(node, result, baselines=baselines, causation_id=claim_event.id)

        # Record actual costs on the node regardless of outcome.
        node = self._store.get_node(node.id)
        node.actual_token_cost += result.input_tokens + result.output_tokens
        node.actual_tool_calls += result.tool_call_count
        node.actual_time_ms += elapsed_ms
        node = self._store.update_node(node, actor=ActorType.SYSTEM)

        if outcome.passed:
            checkpoint_after = None
            if self._config.get("checkpoint", {}).get("after_verified_node", False):
                _t = time.perf_counter()
                checkpoint_after = self._checkpoints.create(
                    run_id, reason=f"verified:{node.id}:attempt{node.attempt_count}"
                )
                self._checkpoint_seconds += time.perf_counter() - _t
            self._apply_worker_patch(run_id, result)
            self._events.append(
                RuntimeEvent(
                    run_id=run_id,
                    event_type=EventType.EXECUTION_FINISHED,
                    actor_type=ActorType.SYSTEM,
                    actor_id=node.id,
                    payload={
                        "node_id": node.id,
                        "attempt": node.attempt_count,
                        "summary": result.summary,
                        "verification": outcome.summary,
                        "checkpoint_after": checkpoint_after,
                    },
                )
            )
            self._store.finish_execution(
                execution.id,
                status="verified",
                result={**result.model_dump(), "verification": outcome.summary},
                checkpoint_after=checkpoint_after,
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                tool_calls=result.tool_call_count,
            )
            tool_type = node.metadata.get("tool_type")
            if tool_type:
                self._resources.last_tool_type = tool_type
            # Phase 5: artifact version tracking — detect content changes in
            # produced artifacts and propagate staleness downstream (spec 15).
            self._update_produced_artifacts(run_id, node)
            # Mid-run external change injection (environment events emitted
            # by the environment/worker script).
            self._ingest_environment_events(run_id, result.environment_events)
            if self._inject_crash_once(run_id, node, "crash_after_verified"):
                # Crash point (26.2): immediately after the verified commit.
                raise SimulatedCrashError(f"simulated crash after verified commit of {node.id}")
        else:
            self._events.append(
                RuntimeEvent(
                    run_id=run_id,
                    event_type=EventType.EXECUTION_FINISHED,
                    actor_type=ActorType.SYSTEM,
                    actor_id=node.id,
                    payload={
                        "node_id": node.id,
                        "attempt": node.attempt_count,
                        "summary": result.summary,
                        "verification": outcome.summary,
                    },
                )
            )
            self._store.finish_execution(
                execution.id,
                status="verification_failed",
                result=result.model_dump(),
                error={"verification": outcome.summary},
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                tool_calls=result.tool_call_count,
            )
            if self._checkpoint_config.get("restore_on_failure", False):
                self._restore_checkpoint(
                    run_id, node.id, checkpoint_id, reason="verification failed"
                )
            self._ingest_environment_events(run_id, result.environment_events)

    # ------------------------------------------------- phase 5: invalidation
    @property
    def _invalidation_enabled(self) -> bool:
        """Feature flag (spec 25 ablations): static-graph benchmark modes set
        ``features.invalidation = False``; environment events and artifact
        updates are then logged but never reconciled."""
        return bool(self._config.get("features", {}).get("invalidation", True))

    def _update_produced_artifacts(self, run_id: str, node: GraphNode) -> None:
        """Artifact version tracking (spec 15): after a node is verified, hash
        the artifacts it PRODUCES; when the content hash changed, bump the
        artifact version and emit ARTIFACT_UPDATED so the reconciler marks
        verified consumers STALE / INVALIDATED along the downstream subgraph.
        """
        if not self._workspace_dir or not self._invalidation_enabled:
            return
        import hashlib
        from pathlib import Path

        graph = self._store.load_graph(run_id)
        for artifact in graph.produced_artifacts(node.id):
            rel_path = artifact.metadata.get("path")
            if not rel_path:
                continue
            full_path = (Path(self._workspace_dir).resolve() / rel_path).resolve()
            new_hash = (
                hashlib.sha256(full_path.read_bytes()).hexdigest() if full_path.exists() else None
            )
            old_hash = artifact.metadata.get("content_hash")
            if new_hash == old_hash:
                continue
            event = self._events.append(
                RuntimeEvent(
                    run_id=run_id,
                    event_type=EventType.ARTIFACT_UPDATED,
                    actor_type=ActorType.SYSTEM,
                    actor_id=node.id,
                    payload={
                        "node_id": artifact.id,
                        "producer_node_id": node.id,
                        "old_hash": old_hash,
                        "new_hash": new_hash,
                        "removed": new_hash is None,
                        "reason": "artifact content changed after re-verification",
                    },
                )
            )
            self._reconciler.reconcile_event(run_id, event)

    def _ingest_environment_events(self, run_id: str, events: list[dict[str, Any]]) -> None:
        """Mid-run external change ingestion (spec 15, 18 ingest step):
        append each event and run the deterministic reconciler immediately."""
        for raw in events:
            event_type = raw.get("type", "")
            if not event_type:
                continue
            # Scripts may write types in lower case ("artifact_updated");
            # the log uses the canonical upper-case constants (spec 5.2).
            event_type = event_type.upper()
            payload = {k: v for k, v in raw.items() if k != "type"}
            event = self._events.append(
                RuntimeEvent(
                    run_id=run_id,
                    event_type=event_type,
                    actor_type=ActorType.SYSTEM,
                    payload=payload,
                )
            )
            if self._invalidation_enabled:
                self._reconciler.reconcile_event(run_id, event)

    def _apply_worker_patch(self, run_id: str, result) -> None:
        if not result.graph_patch:
            return
        ops = [GraphPatchOperation(**raw) for raw in result.graph_patch]
        try:
            self._patches.validate_and_apply(run_id, ops, actor=ActorType.WORKER)
        except PatchValidationError as exc:
            # Rejected patches never partially apply (spec 8.2); the rejection
            # is recorded instead of silently dropping the update.
            self._events.append(
                RuntimeEvent(
                    run_id=run_id,
                    event_type=EventType.NODE_UPDATED,
                    actor_type=ActorType.SYSTEM,
                    payload={"patch_rejected": str(exc)},
                )
            )

    def _finish_run(self, run_id: str, decision: TerminationDecision) -> Run:
        event_type = {
            "completed": EventType.RUN_COMPLETED,
            "failed": EventType.RUN_FAILED,
            "paused": EventType.RUN_PAUSED,
            "aborted": EventType.RUN_ABORTED,
        }.get(decision.status, EventType.RUN_FAILED)
        # §24.2 instrumentation snapshot: aggregate execution usage plus the
        # scheduler / checkpoint wall-clock accumulated during this controller's
        # lifetime, so benchmark metrics can be computed from the event log.
        tokens_in = tokens_out = tool_calls = 0
        for ex in self._store.list_executions(run_id):
            tokens_in += int(ex.input_tokens or 0)
            tokens_out += int(ex.output_tokens or 0)
            tool_calls += int(ex.tool_calls or 0)
        nodes = self._store.list_nodes(run_id)
        verified = sum(1 for n in nodes if n.state == NodeState.VERIFIED)
        failed = sum(1 for n in nodes if n.state == NodeState.FAILED)
        invalidated = sum(1 for n in nodes if n.state == NodeState.INVALIDATED)
        payload_extra = {
            "input_tokens": tokens_in,
            "output_tokens": tokens_out,
            "total_tokens": tokens_in + tokens_out,
            "model_calls": len(self._store.list_executions(run_id)),
            "tool_calls": tool_calls,
            "model_cost_usd": 0.0,  # FakeWorker: no real model cost (spec 24.2 note)
            "verified_nodes": verified,
            "failed_nodes": failed,
            "invalidated_nodes": invalidated,
            "total_nodes": len(nodes),
            "scheduler_time_seconds": round(self._scheduler_seconds, 6),
            "checkpoint_time_seconds": round(self._checkpoint_seconds, 6),
        }
        return self._store.set_run_status(
            run_id, decision.status, event_type=event_type, payload_extra=payload_extra
        )
