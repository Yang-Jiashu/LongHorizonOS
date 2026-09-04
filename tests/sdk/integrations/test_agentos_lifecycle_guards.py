"""Lifecycle and configuration guard tests for the public AgentOS facade."""

from __future__ import annotations

from pathlib import Path

import pytest

from lhos.sdk import AgentOS
from lhos.sdk.errors import ConfigurationError


def _ephemeral_paths() -> set[Path]:
    temp_dir = Path(__import__("tempfile").gettempdir())
    return set(temp_dir.glob("lhos-agentos-*.sqlite3"))


def test_read_only_memory_agentos_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="read-only AgentOS"):
        AgentOS(":memory:", read_only=True)


def test_constructor_failure_unwinds_ephemeral_database(monkeypatch) -> None:
    import lhos.sdk.os as sdk_os

    before = _ephemeral_paths()

    def fail_create_kernel(_db_path: str):
        raise RuntimeError("injected kernel construction failure")

    monkeypatch.setattr(sdk_os, "create_kernel", fail_create_kernel)
    with pytest.raises(RuntimeError, match="injected kernel"):
        AgentOS(":memory:")

    assert _ephemeral_paths() == before


def test_close_continues_cleanup_after_one_handle_fails() -> None:
    runtime = AgentOS(":memory:")
    backing = Path(runtime._storage_db_path)
    real_facts_close = runtime._facts.close

    def close_then_fail() -> None:
        real_facts_close()
        raise RuntimeError("injected facts close failure")

    runtime._facts.close = close_then_fail  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="failed to close facts"):
        runtime.close()

    assert runtime._closed is True
    assert not backing.exists()
    assert not Path(f"{backing}-wal").exists()
    assert not Path(f"{backing}-shm").exists()
    runtime.close()
