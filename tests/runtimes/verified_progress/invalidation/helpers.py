"""Shared D3 test graph builder primitives."""

from __future__ import annotations


class _Val:
    def __init__(self, value: str):
        self.value = value


class _Life:
    def __init__(self, value: str):
        self.value = value


class TNode:
    """Minimal TaskNode stand-in compatible with the cone/frontier readers."""

    def __init__(self, tid: str, validity: str = "verified", lifecycle: str = "admitted"):
        self.node_id = tid
        self.validity = _Val(validity)
        self.lifecycle = _Life(lifecycle)
        self.node_type = "task"


class GNode:
    def __init__(self, gid: str, closed: bool = True):
        self.node_id = gid
        self.node_type = "goal"
        self.closed = closed
        self.lifecycle = _Life("closed" if closed else "active")


class Bound:
    """ArtifactVersionBinding stand-in."""

    def __init__(self, artifact_id: str, version: int, content_hash: str = "h"):
        self.artifact_id = artifact_id
        self.version = version
        self.content_hash = content_hash


class FNode:
    """EvidenceNode stand-in for applicability tests."""

    def __init__(
        self,
        eid: str,
        *,
        artifact_bindings: tuple = (),
        source_action_id: str | None = None,
        source_event_ids: tuple[str, ...] = (),
    ):
        self.node_id = eid
        self.node_type = "evidence"
        self.artifact_bindings = artifact_bindings
        self.source_action_id = source_action_id
        self.source_event_ids = source_event_ids


class Edge:
    def __init__(self, etype: str, source: str, target: str):
        self.edge_type = _Val(etype)
        self.source_node_id = source
        self.target_node_id = target


def depends_on(source: str, target: str) -> Edge:
    """source depends_on target."""
    return Edge("depends_on", source, target)


def produces(source: str, target: str) -> Edge:
    return Edge("produces", source, target)


def verifies(source: str, target: str) -> Edge:
    return Edge("verifies", source, target)
