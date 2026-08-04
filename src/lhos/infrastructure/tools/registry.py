"""Tool registry. MVP allows only read_only and local_write tools (spec 13.2).

Tool name contract (Step 6): the registry uses ``ToolName`` constants and
provides ``normalize_name()`` to map common LLM-returned variants (e.g.
``filesystem.write`` → ``filesystem``) to the canonical registered name.
"""

from lhos.domain.enums import ALLOWED_SIDE_EFFECT_LEVELS, ToolName
from lhos.domain.errors import ToolError, ToolNotAllowedError
from lhos.ports.tools import Tool, ToolMetadata

# Mapping from common LLM-returned tool name variants to canonical names.
_TOOL_NAME_ALIASES: dict[str, str] = {
    # Filesystem variants
    "filesystem.read": ToolName.FILESYSTEM.value,
    "filesystem.write": ToolName.FILESYSTEM.value,
    "filesystem.list": ToolName.FILESYSTEM.value,
    "filesystem.exists": ToolName.FILESYSTEM.value,
    "filesystem.append": ToolName.FILESYSTEM.value,
    "filesystem_read": ToolName.FILESYSTEM.value,
    "filesystem_write": ToolName.FILESYSTEM.value,
    "fs": ToolName.FILESYSTEM.value,
    "fs.read": ToolName.FILESYSTEM.value,
    "fs.write": ToolName.FILESYSTEM.value,
    "file": ToolName.FILESYSTEM.value,
    "file.read": ToolName.FILESYSTEM.value,
    "file.write": ToolName.FILESYSTEM.value,
    "read_file": ToolName.FILESYSTEM.value,
    "write_file": ToolName.FILESYSTEM.value,
    # Shell variants
    "shell.exec": ToolName.SHELL.value,
    "shell.run": ToolName.SHELL.value,
    "shell_exec": ToolName.SHELL.value,
    "bash": ToolName.SHELL.value,
    "exec": ToolName.SHELL.value,
    "run": ToolName.SHELL.value,
    "terminal": ToolName.SHELL.value,
    "command": ToolName.SHELL.value,
}


def normalize_tool_name(raw: str) -> str:
    """Normalize an LLM-returned tool name to a canonical registry name.

    Returns the canonical name if a mapping exists, otherwise returns the
    input unchanged (the caller will get a clear ``ToolError`` from the
    registry if the name is not registered).
    """
    lower = raw.strip().lower()
    # Check alias map first.
    if lower in _TOOL_NAME_ALIASES:
        return _TOOL_NAME_ALIASES[lower]
    # Check if it matches a canonical ToolName (case-insensitive).
    for member in ToolName:
        if lower == member.value:
            return member.value
    return raw.strip()


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}
        self._metadata: dict[str, ToolMetadata] = {}

    def register(self, tool: Tool, metadata: ToolMetadata) -> None:
        self._tools[metadata.name] = tool
        self._metadata[metadata.name] = metadata

    def get(self, name: str) -> Tool:
        canonical = normalize_tool_name(name)
        if canonical not in self._tools:
            raise ToolError(
                f"tool {name!r} is not registered"
                + (f" (normalized to {canonical!r})" if canonical != name else "")
            )
        return self._tools[canonical]

    def metadata(self, name: str) -> ToolMetadata:
        canonical = normalize_tool_name(name)
        if canonical not in self._metadata:
            raise ToolError(
                f"tool {name!r} is not registered"
                + (f" (normalized to {canonical!r})" if canonical != name else "")
            )
        return self._metadata[canonical]

    def check_allowed(self, name: str) -> ToolMetadata:
        meta = self.metadata(name)
        if meta.side_effect_level not in ALLOWED_SIDE_EFFECT_LEVELS:
            raise ToolNotAllowedError(
                f"tool {name!r} has side_effect_level={meta.side_effect_level!r}; "
                f"MVP allows only {sorted(ALLOWED_SIDE_EFFECT_LEVELS)}"
            )
        return meta

    def names(self) -> list[str]:
        return sorted(self._tools)

    def is_registered(self, name: str) -> bool:
        """Check if a tool name (after normalization) is registered."""
        canonical = normalize_tool_name(name)
        return canonical in self._tools
