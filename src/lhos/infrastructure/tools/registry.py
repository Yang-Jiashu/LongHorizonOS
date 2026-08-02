"""Tool registry. MVP allows only read_only and local_write tools (spec 13.2)."""

from lhos.domain.enums import ALLOWED_SIDE_EFFECT_LEVELS
from lhos.domain.errors import ToolError, ToolNotAllowedError
from lhos.ports.tools import Tool, ToolMetadata


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}
        self._metadata: dict[str, ToolMetadata] = {}

    def register(self, tool: Tool, metadata: ToolMetadata) -> None:
        self._tools[metadata.name] = tool
        self._metadata[metadata.name] = metadata

    def get(self, name: str) -> Tool:
        if name not in self._tools:
            raise ToolError(f"tool {name!r} is not registered")
        return self._tools[name]

    def metadata(self, name: str) -> ToolMetadata:
        if name not in self._metadata:
            raise ToolError(f"tool {name!r} is not registered")
        return self._metadata[name]

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
