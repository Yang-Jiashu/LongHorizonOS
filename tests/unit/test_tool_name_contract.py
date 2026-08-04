"""Unit tests for the Tool Name Contract (Step 6).

Verifies:
- ToolName enum has the correct canonical names.
- normalize_tool_name maps common variants correctly.
- ToolRegistry uses normalization.
- Unknown tools produce clear errors.
- Registered tools can be found by canonical name and common aliases.
"""

from __future__ import annotations

import pytest

from lhos.domain.enums import ToolName
from lhos.infrastructure.tools.filesystem_tool import FILESYSTEM_METADATA, FilesystemTool
from lhos.infrastructure.tools.registry import ToolRegistry, normalize_tool_name
from lhos.infrastructure.tools.shell_tool import SHELL_METADATA, ShellTool


class TestToolNameEnum:
    def test_filesystem_constant(self):
        assert ToolName.FILESYSTEM == "filesystem"

    def test_shell_constant(self):
        assert ToolName.SHELL == "shell"

    def test_fake_constant(self):
        assert ToolName.FAKE == "fake"


class TestNormalizeToolName:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("filesystem", "filesystem"),
            ("filesystem.read", "filesystem"),
            ("filesystem.write", "filesystem"),
            ("filesystem.list", "filesystem"),
            ("filesystem.exists", "filesystem"),
            ("filesystem_read", "filesystem"),
            ("filesystem_write", "filesystem"),
            ("fs", "filesystem"),
            ("fs.read", "filesystem"),
            ("fs.write", "filesystem"),
            ("file", "filesystem"),
            ("file.read", "filesystem"),
            ("file.write", "filesystem"),
            ("read_file", "filesystem"),
            ("write_file", "filesystem"),
            ("shell", "shell"),
            ("shell.exec", "shell"),
            ("shell.run", "shell"),
            ("shell_exec", "shell"),
            ("bash", "shell"),
            ("exec", "shell"),
            ("terminal", "shell"),
            ("command", "shell"),
        ],
    )
    def test_normalization(self, raw, expected):
        assert normalize_tool_name(raw) == expected

    def test_case_insensitive(self):
        assert normalize_tool_name("FILESYSTEM") == "filesystem"
        assert normalize_tool_name("Shell.Exec") == "shell"

    def test_unknown_returns_original(self):
        """Unknown tool names are returned unchanged (registry will error)."""
        assert normalize_tool_name("unknown_tool") == "unknown_tool"


class TestToolRegistryNormalization:
    @pytest.fixture
    def registry(self):
        reg = ToolRegistry()
        reg.register(ShellTool(), SHELL_METADATA)
        reg.register(FilesystemTool(), FILESYSTEM_METADATA)
        return reg

    def test_canonical_name_works(self, registry):
        tool = registry.get("filesystem")
        assert tool.name == "filesystem"

    def test_alias_works(self, registry):
        tool = registry.get("filesystem.write")
        assert tool.name == "filesystem"

    def test_metadata_normalization(self, registry):
        meta = registry.metadata("shell.exec")
        assert meta.name == "shell"

    def test_check_allowed_normalization(self, registry):
        meta = registry.check_allowed("fs.read")
        assert meta.side_effect_level == "local_write"

    def test_is_registered(self, registry):
        assert registry.is_registered("filesystem")
        assert registry.is_registered("filesystem.write")
        assert not registry.is_registered("nonexistent")

    def test_unknown_tool_clear_error(self, registry):
        from lhos.domain.errors import ToolError

        with pytest.raises(ToolError, match="not registered"):
            registry.get("nonexistent_tool")

    def test_names_returns_canonical(self, registry):
        names = registry.names()
        assert "filesystem" in names
        assert "shell" in names
        # Aliases should NOT be in the names list.
        assert "filesystem.write" not in names
