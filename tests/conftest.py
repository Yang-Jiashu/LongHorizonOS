"""Shared fixtures and builders for the LongHorizonOS test suite."""

from __future__ import annotations

import pytest

from lhos.domain.enums import EdgeKind, NodeKind, NodeState
from lhos.domain.models import GraphEdge, GraphNode
from lhos.infrastructure.db.connection import Database
from lhos.infrastructure.db.sqlite_event_store import SqliteEventStore
from lhos.infrastructure.db.sqlite_graph_store import SqliteGraphStore


@pytest.fixture
def db(tmp_path):
    database = Database(tmp_path / "lhos.db")
    yield database
    database.close()


@pytest.fixture
def event_store(db):
    return SqliteEventStore(db)


@pytest.fixture
def graph_store(db, event_store):
    return SqliteGraphStore(db, event_store)


@pytest.fixture
def run_id(graph_store):
    graph_store.create_run("run-test", goal="test goal", config={})
    return "run-test"


def make_node(
    node_id: str,
    run_id: str = "run-test",
    state: NodeState = NodeState.PENDING,
    kind: NodeKind = NodeKind.SUBTASK,
    schedulable: bool = True,
    **kwargs,
) -> GraphNode:
    return GraphNode(
        id=node_id,
        run_id=run_id,
        kind=kind,
        title=kwargs.pop("title", node_id),
        specification=kwargs.pop("specification", f"spec of {node_id}"),
        state=state,
        schedulable=schedulable,
        **kwargs,
    )


def make_edge(
    run_id: str,
    source: str,
    target: str,
    kind: EdgeKind = EdgeKind.DEPENDS_ON,
) -> GraphEdge:
    return GraphEdge(
        run_id=run_id, source_node_id=source, target_node_id=target, kind=kind
    )
