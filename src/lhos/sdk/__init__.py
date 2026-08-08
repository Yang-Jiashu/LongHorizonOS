"""LongHorizonOS Public SDK (E1, experimental v0.x).

A thin composition/DTO/builder facade over the frozen Core V1.  The SDK
simplifies Core; it never replaces Core, never owns semantic truth, never owns
execution, never fabricates ownership, never creates a second graph, and never
bypasses Evidence.

Public surface (EXPERIMENTAL — not SDK 1.0; may change):
    OS        — composition root (AgentOS facade)
    Agent     — developer-facing agent (maps to Process + AgentDescriptor)
    Goal, Task — developer-facing goal/task builders (compile to real VPG)
    verifier  — deterministic verifier / scripted executor helpers
    RunResult — structured run result
    StatusSnapshot — read-only public state view
"""

from __future__ import annotations

from .agent import Agent
from .errors import (
    AgentOSError,
    CapabilityError,
    ConfigurationError,
    CoreInvariantError,
    ExecutionError,
    SchedulingError,
    VerificationError,
)
from .goal import Goal
from .os import OS, AgentOS
from .result import RepairOutcome, RunResult
from .status import StatusSnapshot
from .task import Task
from .verification import (
    VerificationOutcome,
    callback_verifier,
    command_verifier,
    scripted_executor,
)

__all__ = [
    "OS",
    "Agent",
    "AgentOS",
    "AgentOSError",
    "CapabilityError",
    "ConfigurationError",
    "CoreInvariantError",
    "ExecutionError",
    "Goal",
    "RepairOutcome",
    "RunResult",
    "SchedulingError",
    "StatusSnapshot",
    "Task",
    "VerificationError",
    "VerificationOutcome",
    "callback_verifier",
    "command_verifier",
    "scripted_executor",
]
