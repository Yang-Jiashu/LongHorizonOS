"""D2-I1/I2/I3 checks on model construction and invariants."""
from __future__ import annotations

import pytest

from lhos.runtimes.multi_agent import (
    AgentDescriptor,
    ClaimState,
    TaskRequirements,
    TERMINAL_CLAIM_STATES,
)


def test_agent_descriptor_validation():
    a = AgentDescriptor(agent_id="a1", process_id="p1")
    assert a.max_concurrency == 1
    assert a.cost_weight == 100
    assert a.enabled is True


def test_agent_descriptor_rejects_empty_ids():
    with pytest.raises(Exception):
        AgentDescriptor(agent_id="", process_id="p1")
    with pytest.raises(Exception):
        AgentDescriptor(agent_id="a1", process_id="")


def test_agent_descriptor_rejects_negative_capacity():
    with pytest.raises(Exception):
        AgentDescriptor(agent_id="a1", process_id="p1", max_concurrency=-1)
    with pytest.raises(Exception):
        AgentDescriptor(agent_id="a1", process_id="p1", cost_weight=-5)


def test_agent_descriptor_no_alive_field():
    a = AgentDescriptor(agent_id="a1", process_id="p1")
    assert not hasattr(a, "alive")
    assert not hasattr(a, "running")


def test_task_requirements_defaults():
    r = TaskRequirements(task_id="t1")
    assert r.task_kind == ""
    assert r.max_attempts is None
    assert r.required_specializations == ()


def test_claim_lifecycle_states():
    assert ClaimState.PROPOSED != ClaimState.ACTIVE
    assert ClaimState.ACTIVE != ClaimState.COMPLETED
    assert ClaimState.RELEASED in TERMINAL_CLAIM_STATES
    assert ClaimState.LOST in TERMINAL_CLAIM_STATES
    assert ClaimState.ACTIVE not in TERMINAL_CLAIM_STATES
