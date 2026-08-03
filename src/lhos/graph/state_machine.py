"""Node state machine (spec section 6).

Allowed transitions are exactly the ones listed in the spec; anything else is
rejected. In particular, no path may skip execution or verification
(PENDING/READY/RUNNING/FAILED -> VERIFIED are forbidden).
"""

from lhos.domain.enums import TERMINAL_STATES, NodeState
from lhos.domain.errors import InvalidStateTransition
from lhos.domain.events import RuntimeEvent
from lhos.domain.models import GraphNode

_ALLOWED: frozenset[tuple[NodeState, NodeState]] = frozenset(
    {
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
    }
)


class NodeStateMachine:
    """Spec section 6 interface."""

    def can_transition(self, current: NodeState, target: NodeState) -> bool:
        if (current, target) in _ALLOWED:
            return True
        # ANY NONTERMINAL -> ABORTED.
        return target == NodeState.ABORTED and current not in TERMINAL_STATES

    def transition(
        self,
        node: GraphNode,
        target: NodeState,
        event: RuntimeEvent,
    ) -> GraphNode:
        """Validate and apply a transition. Returns the updated node.

        The caller is responsible for persisting ``event`` and the updated node
        in the same database transaction (spec section 5.3). ``event`` is
        accepted as the causal record of this transition.
        """
        if not self.can_transition(node.state, target):
            raise InvalidStateTransition(
                f"illegal transition {node.state} -> {target} for node {node.id}"
            )
        from datetime import datetime

        node.state = target
        node.version += 1
        node.updated_at = datetime.now().astimezone()
        return node
