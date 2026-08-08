"""AgentRegistry CRUD semantics."""
from __future__ import annotations

import pytest

from lhos.runtimes.multi_agent import AgentDescriptor, AgentRegistry


def _agent(aid, pid, **kw):
    return AgentDescriptor(agent_id=aid, process_id=pid, **kw)


def test_register_and_get():
    reg = AgentRegistry()
    reg.register(_agent("a1", "p1"))
    assert reg.get("a1").process_id == "p1"
    assert len(reg) == 1


def test_register_duplicate_raises():
    reg = AgentRegistry()
    reg.register(_agent("a1", "p1"))
    with pytest.raises(ValueError):
        reg.register(_agent("a1", "p2"))


def test_list_and_enabled_filter():
    reg = AgentRegistry()
    reg.register(_agent("a1", "p1"))
    reg.register(_agent("a2", "p2"))
    reg.disable("a2")
    assert len(reg.list()) == 2
    enabled = reg.list(enabled_only=True)
    assert [a.agent_id for a in enabled] == ["a1"]


def test_enable_disable_and_remove():
    reg = AgentRegistry()
    reg.register(_agent("a1", "p1"))
    assert reg.get("a1").enabled is True
    reg.disable("a1")
    assert reg.get("a1").enabled is False
    reg.enable("a1")
    assert reg.get("a1").enabled is True
    reg.remove("a1")
    assert reg.get("a1") is None
    assert len(reg) == 0


def test_remove_missing_raises_keyerror():
    reg = AgentRegistry()
    with pytest.raises(KeyError):
        reg.remove("nope")


def test_update_immutes_agent_id():
    reg = AgentRegistry()
    reg.register(_agent("a1", "p1"))
    with pytest.raises(ValueError):
        reg.update("a1", fields={"agent_id": "a2"})


def test_update_metadata_and_capabilities():
    reg = AgentRegistry()
    reg.register(_agent("a1", "p1", max_concurrency=1))
    u = reg.update("a1", fields={"max_concurrency": 3})
    assert u.max_concurrency == 3


def test_registry_not_liveness_authority():
    reg = AgentRegistry()
    reg.register(_agent("a1", "p1"))
    reg.disable("a1")
    assert reg.get("a1") is not None
