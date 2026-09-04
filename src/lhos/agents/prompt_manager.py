"""Versioned prompt management (spec Phase 2D).

Each prompt file lives in ``src/lhos/agents/prompts/`` and has:
- A version number in the filename (e.g. ``initial_planner_v1.md``).
- A header with version, input variables, output schema, prohibitions.
- At least one valid example.

At runtime, the ``PromptManager`` loads the file, computes its SHA-256 hash,
and provides ``prompt_name``, ``prompt_version``, and ``prompt_file_hash``
for LLM call logging. Prompt content is never inlined in code.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

PROMPTS_DIR = Path(__file__).parent / "prompts"


@dataclass(frozen=True)
class PromptInfo:
    """Metadata about a loaded prompt file."""

    name: str
    version: str
    file_hash: str
    content: str
    path: str


class PromptManager:
    """Loads and caches versioned prompt files.

    Usage::

        pm = PromptManager()
        info = pm.load("initial_planner", "v1")
        prompt_text = info.content
        # log: info.name, info.version, info.file_hash
    """

    def __init__(self, prompts_dir: Path | None = None):
        self._dir = prompts_dir or PROMPTS_DIR
        self._cache: dict[str, PromptInfo] = {}

    def load(self, name: str, version: str = "v1") -> PromptInfo:
        """Load a prompt file by name and version.

        The file must be at ``{prompts_dir}/{name}_{version}.md``.
        """
        cache_key = f"{name}_{version}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        filename = f"{name}_{version}.md"
        path = self._dir / filename
        if not path.exists():
            raise FileNotFoundError(f"prompt file not found: {path}")
        content = path.read_text(encoding="utf-8")
        file_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

        info = PromptInfo(
            name=name,
            version=version,
            file_hash=file_hash,
            content=content,
            path=str(path),
        )
        self._cache[cache_key] = info
        return info

    def list_prompts(self) -> list[str]:
        """List all available prompt names (without version suffix)."""
        names: set[str] = set()
        for f in self._dir.glob("*_v*.md"):
            # e.g. "initial_planner_v1.md" -> "initial_planner"
            name = f.stem.rsplit("_v", 1)[0]
            names.add(name)
        return sorted(names)
