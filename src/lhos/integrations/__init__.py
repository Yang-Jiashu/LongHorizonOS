"""LongHorizonOS E2 — integrations (experimental v0.x).

Replaceable execution adapters (models, tools) that depend downward through the
public Core/SDK.  They do NOT own semantic truth; READY/VERIFIED/STALE/Goal CLOSED
remain VPG-derived.  Deleting this package must leave Core V1 semantics intact.
"""

from .models.openai_compatible import OpenAICompatibleModel
from .models.protocols import Message, ModelAdapter, ModelResponse
from .semantic import CommandVerifier
from .tools.git import GitTool
from .tools.shell import ShellTool
from .tools.workspace import WorkspaceTool

__all__ = [
    "CommandVerifier",
    "GitTool",
    "Message",
    "ModelAdapter",
    "ModelResponse",
    "OpenAICompatibleModel",
    "ShellTool",
    "WorkspaceTool",
]
