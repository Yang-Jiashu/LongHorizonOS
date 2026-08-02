"""Core enumerations (spec sections 4.1-4.3, 8.2)."""

from enum import StrEnum


class NodeKind(StrEnum):
    GOAL = "goal"
    SUBTASK = "subtask"
    FACT = "fact"
    CONSTRAINT = "constraint"
    ARTIFACT = "artifact"
    VERIFICATION = "verification"
    CHECKPOINT = "checkpoint"


class NodeState(StrEnum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    WAITING = "waiting"
    CLAIMED_DONE = "claimed_done"
    VERIFIED = "verified"
    FAILED = "failed"
    STALE = "stale"
    INVALIDATED = "invalidated"
    ABORTED = "aborted"


class EdgeKind(StrEnum):
    DEPENDS_ON = "depends_on"
    PRODUCES = "produces"
    CONSUMES = "consumes"
    VALIDATES = "validates"
    INVALIDATES = "invalidates"
    BLOCKS = "blocks"
    SUPPORTS = "supports"
    SUPERSEDES = "supersedes"


class PatchOperationType(StrEnum):
    ADD_NODE = "add_node"
    UPDATE_NODE = "update_node"
    ADD_EDGE = "add_edge"
    REMOVE_EDGE = "remove_edge"
    SET_STATE = "set_state"
    ADD_EVIDENCE = "add_evidence"
    MARK_STALE = "mark_stale"
    INVALIDATE_NODE = "invalidate_node"


# Terminal states: no outgoing transitions, not even ABORTED (spec section 6).
TERMINAL_STATES: frozenset[NodeState] = frozenset({NodeState.VERIFIED, NodeState.ABORTED})

# States that count as "remaining work" for scheduling / critical path analysis.
NON_REMAINING_STATES: frozenset[NodeState] = frozenset({NodeState.VERIFIED, NodeState.ABORTED})

# MVP allowed tool side-effect levels (spec section 13.2).
SIDE_EFFECT_READ_ONLY = "read_only"
SIDE_EFFECT_LOCAL_WRITE = "local_write"
SIDE_EFFECT_EXTERNAL_WRITE = "external_write"
SIDE_EFFECT_DESTRUCTIVE = "destructive"
ALLOWED_SIDE_EFFECT_LEVELS: frozenset[str] = frozenset(
    {SIDE_EFFECT_READ_ONLY, SIDE_EFFECT_LOCAL_WRITE}
)
