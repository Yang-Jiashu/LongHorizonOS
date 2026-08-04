"""Component wiring shared by the CLI and the integration tests.

Builds the full deterministic runtime stack (Phase 0/1/3/4) against a SQLite
database file. No real LLM is ever constructed here.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from lhos.domain.budgets import BudgetLimits
from lhos.graph.initial_builder import InitialGraphBuilder
from lhos.graph.patch_validator import PatchValidator
from lhos.graph.readiness import ReadinessEvaluator, ReadinessRefresher
from lhos.graph.reconciler import DeterministicReconciler
from lhos.infrastructure.checkpoints.filesystem_checkpoint import (
    FilesystemCheckpointManager,
)
from lhos.infrastructure.checkpoints.git_checkpoint import GitCheckpointManager
from lhos.infrastructure.checkpoints.noop import NoopCheckpointManager
from lhos.infrastructure.db.connection import Database
from lhos.infrastructure.db.sqlite_event_store import SqliteEventStore
from lhos.infrastructure.db.sqlite_graph_store import SqliteGraphStore
from lhos.infrastructure.telemetry.jsonl_tracer import JsonlTracer
from lhos.infrastructure.tools.fake_tool import FAKE_METADATA, FakeTool
from lhos.infrastructure.tools.filesystem_tool import (
    FILESYSTEM_METADATA,
    FilesystemTool,
)
from lhos.infrastructure.tools.registry import ToolRegistry
from lhos.infrastructure.tools.shell_tool import SHELL_METADATA, ShellTool
from lhos.runtime.budget_manager import BudgetManager
from lhos.runtime.context_compiler import ContextCompiler
from lhos.runtime.controller import RuntimeController
from lhos.runtime.cost_aware_scheduler import CostAwareScheduler
from lhos.runtime.fifo_scheduler import FifoScheduler
from lhos.runtime.recovery import RecoveryManager
from lhos.runtime.termination import TerminationEvaluator
from lhos.runtime.tool_runtime import ToolRuntime
from lhos.runtime.verification_gate import VerificationGate
from lhos.runtime.worker import FakeWorker
from lhos.verification.registry import build_default_registry


class RuntimeStack:
    def __init__(
        self,
        db_path: str | Path,
        workspace_dir: str | Path,
        config: dict[str, Any] | None = None,
        scheduler_type: str | None = None,
        fake_tool_script: list[dict] | None = None,
    ):
        self.config = config or {}
        self.workspace_dir = str(workspace_dir)
        Path(self.workspace_dir).mkdir(parents=True, exist_ok=True)

        telemetry_cfg = self.config.get("telemetry", {})
        tracer = None
        if telemetry_cfg.get("jsonl_trace", False):
            trace_dir = telemetry_cfg.get("trace_directory", "artifacts/traces")
            tracer = JsonlTracer(trace_dir)

        self.db = Database(db_path)
        self.event_store = SqliteEventStore(self.db, tracer=tracer)
        self.graph_store = SqliteGraphStore(self.db, self.event_store)

        # Tools
        self.tool_registry = ToolRegistry()
        self.tool_registry.register(ShellTool(), SHELL_METADATA)
        self.tool_registry.register(FilesystemTool(), FILESYSTEM_METADATA)
        self.tool_registry.register(FakeTool(fake_tool_script), FAKE_METADATA)

        budget_cfg = self.config.get("budget", {})
        self.budget_limits = BudgetLimits(**{k: v for k, v in budget_cfg.items() if v is not None})
        self.budget_manager = BudgetManager(self.event_store, self.budget_limits)

        self.tool_runtime = ToolRuntime(
            self.event_store,
            self.tool_registry,
            self.workspace_dir,
            budget_manager=self.budget_manager,
        )

        # Scheduler
        scheduler_cfg = self.config.get("scheduler", {})
        chosen = scheduler_type or scheduler_cfg.get("type", "fifo")
        if chosen == "cost_aware":
            self.scheduler: Any = CostAwareScheduler(scheduler_cfg.get("weights"))
        else:
            self.scheduler = FifoScheduler()

        # Checkpoint manager
        checkpoint_cfg = self.config.get("checkpoint", {})
        checkpoint_type = checkpoint_cfg.get("type", "noop")
        checkpoint_root = self.config.get("checkpoint_root", "artifacts/checkpoints")
        if checkpoint_type == "git":
            self.checkpoint_manager: Any = GitCheckpointManager(self.workspace_dir, db=self.db)
        elif checkpoint_type == "filesystem":
            self.checkpoint_manager = FilesystemCheckpointManager(
                self.workspace_dir, checkpoint_root, db=self.db
            )
        else:
            self.checkpoint_manager = NoopCheckpointManager()

        # Graph components
        features_cfg = self.config.get("features", {})
        self.readiness = ReadinessRefresher(
            self.graph_store, ReadinessEvaluator(self.budget_limits)
        )
        self.reconciler = DeterministicReconciler(
            self.graph_store,
            local_repair=bool(features_cfg.get("local_repair", True)),
        )
        self.patch_validator = PatchValidator(self.graph_store)
        self.initial_builder = InitialGraphBuilder(self.graph_store)
        self.context_compiler = ContextCompiler(self.graph_store, self.event_store)

        verification_cfg = self.config.get("verification", {})
        self.verifier_registry = build_default_registry(
            allow_llm_judge=verification_cfg.get("allow_llm_judge", False)
        )
        self.verification_gate = VerificationGate(
            self.graph_store,
            self.event_store,
            self.verifier_registry,
            self.workspace_dir,
        )

        self.worker = FakeWorker(tool_runtime=self.tool_runtime)
        self.recovery = RecoveryManager(
            self.graph_store,
            self.event_store,
            checkpoint_manager=self.checkpoint_manager,
            restore_on_crash=checkpoint_cfg.get("restore_on_crash", False),
        )
        self.termination = TerminationEvaluator()

        self.controller = RuntimeController(
            graph_store=self.graph_store,
            event_store=self.event_store,
            scheduler=self.scheduler,
            context_compiler=self.context_compiler,
            worker=self.worker,
            verification_gate=self.verification_gate,
            budget_manager=self.budget_manager,
            checkpoint_manager=self.checkpoint_manager,
            readiness=self.readiness,
            reconciler=self.reconciler,
            patch_validator=self.patch_validator,
            recovery=self.recovery,
            termination=self.termination,
            config=self.config,
            workspace_dir=self.workspace_dir,
        )

    def close(self) -> None:
        self.db.close()
