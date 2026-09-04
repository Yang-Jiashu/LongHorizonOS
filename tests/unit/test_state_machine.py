"""Every legal and illegal state transition (spec section 6)."""

from __future__ import annotations

import pytest

from lhos.domain.enums import NodeState
from lhos.domain.errors import InvalidStateTransition
from lhos.domain.events import RuntimeEvent
from lhos.domain.models import GraphNode
from lhos.graph.state_machine import NodeStateMachine

SM = NodeStateMachine()

LEGAL = [
    (NodeState.PENDING, NodeState.READY),
    (NodeState.READY, NodeState.RUNNING),
    (NodeState.RUNNING, NodeState.CLAIMED_DONE),
    (NodeState.RUNNING, NodeState.FAILED),
    (NodeState.RUNNING, NodeState.WAITING),
    (NodeState.CLAIMED_DONE, NodeState.VERIFIED),
    (NodeState.CLAIMED_DONE, NodeState.FAILED),
    (NodeState.FAILED, NodeState.READY),
    (NodeState.WAITING, NodeState.READY),
    (NodeState.VERIFIED, NodeState.STALE),
    (NodeState.VERIFIED, NodeState.INVALIDATED),
    (NodeState.STALE, NodeState.READY),
    (NodeState.INVALIDATED, NodeState.PENDING),
]

# ANY NONTERMINAL -> ABORTED.
NONTERMINAL = [
    NodeState.PENDING,
    NodeState.READY,
    NodeState.RUNNING,
    NodeState.WAITING,
    NodeState.CLAIMED_DONE,
    NodeState.FAILED,
    NodeState.STALE,
    NodeState.INVALIDATED,
]

ILLEGAL = [
    # Explicitly forbidden by the spec: these skip execution or verification.
    (NodeState.PENDING, NodeState.VERIFIED),
    (NodeState.READY, NodeState.VERIFIED),
    (NodeState.RUNNING, NodeState.VERIFIED),
    (NodeState.FAILED, NodeState.VERIFIED),
    # Other impossible jumps.
    (NodeState.WAITING, NodeState.VERIFIED),
    (NodeState.STALE, NodeState.VERIFIED),
    (NodeState.INVALIDATED, NodeState.VERIFIED),
    (NodeState.PENDING, NodeState.RUNNING),
    (NodeState.PENDING, NodeState.CLAIMED_DONE),
    (NodeState.READY, NodeState.CLAIMED_DONE),
    (NodeState.READY, NodeState.FAILED),
    (NodeState.RUNNING, NodeState.READY),
    (NodeState.CLAIMED_DONE, NodeState.RUNNING),
    (NodeState.CLAIMED_DONE, NodeState.READY),
    (NodeState.FAILED, NodeState.RUNNING),
    (NodeState.WAITING, NodeState.RUNNING),
    (NodeState.STALE, NodeState.RUNNING),
    (NodeState.INVALIDATED, NodeState.READY),
    (NodeState.INVALIDATED, NodeState.STALE),
    # Terminal states have no outgoing transitions.
    (NodeState.VERIFIED, NodeState.READY),
    (NodeState.VERIFIED, NodeState.PENDING),
    (NodeState.VERIFIED, NodeState.RUNNING),
    (NodeState.VERIFIED, NodeState.ABORTED),
    (NodeState.ABORTED, NodeState.PENDING),
    (NodeState.ABORTED, NodeState.READY),
    (NodeState.ABORTED, NodeState.RUNNING),
    (NodeState.ABORTED, NodeState.VERIFIED),
]


def _node(state: NodeState) -> GraphNode:
    return GraphNode(
        id="n",
        run_id="r",
        kind="subtask",
        title="n",
        specification="s",
        state=state,
    )


def _event() -> RuntimeEvent:
    return RuntimeEvent(run_id="r", event_type="NODE_STATE_CHANGED", actor_type="system")


@pytest.mark.parametrize("current,target", LEGAL)
def test_legal_transitions_are_allowed(current, target):
    assert SM.can_transition(current, target)
    node = _node(current)
    version_before = node.version
    updated = SM.transition(node, target, _event())
    assert updated.state == target
    assert updated.version == version_before + 1


@pytest.mark.parametrize("current", NONTERMINAL)
def test_any_nonterminal_can_be_aborted(current):
    assert SM.can_transition(current, NodeState.ABORTED)
    node = SM.transition(_node(current), NodeState.ABORTED, _event())
    assert node.state == NodeState.ABORTED


@pytest.mark.parametrize("current,target", ILLEGAL)
def test_illegal_transitions_are_rejected(current, target):
    assert not SM.can_transition(current, target)
    with pytest.raises(InvalidStateTransition):
        SM.transition(_node(current), target, _event())


def test_terminal_states_have_no_outgoing_transitions():
    # ABORTED is fully terminal.
    for target in NodeState:
        assert not SM.can_transition(NodeState.ABORTED, target)
    # VERIFIED is terminal except for the explicit staleness edges (spec 6).
    for target in NodeState:
        expected = target in {NodeState.STALE, NodeState.INVALIDATED}
        assert SM.can_transition(NodeState.VERIFIED, target) == expected
