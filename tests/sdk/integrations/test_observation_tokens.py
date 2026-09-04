"""Adversarial tests for graph-bound observation/snapshot tokens."""

from __future__ import annotations

import hashlib
import sqlite3
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from typing import Any

import pytest

from lhos.integrations.tools.workspace import WorkspaceTool
from lhos.sdk import Agent, AgentOS, ConfigurationError, Goal, ObservationToken
from lhos.sdk.providers import FactsProvider
from lhos.sdk.verification import scripted_executor


class _SnapshotRaceTokens(dict[str, ObservationToken]):
    """Capture an empty token snapshot before briefly yielding to a peer."""

    def __init__(self) -> None:
        super().__init__()
        self._barrier = threading.Barrier(2)

    def values(self) -> Any:
        snapshot = tuple(super().values())
        if not snapshot:
            with suppress(threading.BrokenBarrierError):
                self._barrier.wait(timeout=0.5)
        return snapshot


class _SlowBeginConnection:
    """Expose one real sqlite connection while widening the BEGIN race."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def execute(self, sql: str, parameters: Any = ()) -> sqlite3.Cursor:
        cursor = self._connection.execute(sql, parameters)
        if sql.strip().upper() == "BEGIN IMMEDIATE":
            time.sleep(0.05)
        return cursor

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


def _issue_concurrently(
    *issuers: Callable[[], ObservationToken],
) -> tuple[ObservationToken, ...]:
    barrier = threading.Barrier(len(issuers))
    results: list[ObservationToken] = []
    errors: list[BaseException] = []

    def run(issuer: Callable[[], ObservationToken]) -> None:
        try:
            barrier.wait(timeout=5)
            results.append(issuer())
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(issuer,)) for issuer in issuers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not any(thread.is_alive() for thread in threads)
    assert not errors
    assert len(results) == len(issuers)
    return tuple(results)


def _closed_goal(os_: AgentOS, goal_id: str = "G") -> Goal:
    os_.add_agent(Agent("worker", specializations=("python",)))
    goal = Goal(goal_id)
    goal.task(
        "T",
        agent="worker",
        verify=scripted_executor(artifact_id="artifact", version=1, content="v1"),
    )
    assert "T" in os_.run(goal, max_dispatches=2).verified
    return goal


def test_graph_bound_observation_token_repairs_and_round_trips() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _closed_goal(os_)
        token = os_.observe_artifact(goal, "artifact", 2, b"v2")

        assert isinstance(token, ObservationToken)
        assert token.graph_id == os_._gid_for(goal.goal_id)
        assert token.content_hash == hashlib.sha256(b"v2").hexdigest()
        assert ObservationToken.parse(token.serialize()) == token

        repaired = os_.repair(goal, observation=token)
        assert "T" in repaired.affected
        d3 = os_.vpg.get_d3_results(token.graph_id)
        assert d3 and goal.goal_id in d3[-1]["reopened_goals"]
    finally:
        os_.close()


def test_tampered_observation_token_fails_closed() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _closed_goal(os_)
        token = os_.observe_artifact(goal, "artifact", 2, b"v2")
        payload = token.as_dict()
        payload["content_hash"] = "0" * 64

        with pytest.raises(ConfigurationError, match="invalid observation token"):
            os_.repair(goal, observation=payload)
    finally:
        os_.close()


def test_same_version_different_content_is_rejected() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _closed_goal(os_)
        os_.observe_artifact(goal, "artifact", 2, b"v2")
        with pytest.raises(ConfigurationError, match="immutable"):
            os_.observe_artifact(goal, "artifact", 2, b"tampered")
    finally:
        os_.close()


def test_token_from_another_graph_is_rejected() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal_a = _closed_goal(os_, "A")
        token = os_.observe_artifact(goal_a, "artifact", 2, b"v2")

        # A second graph references the same artifact identity, so the graph
        # binding—not merely missing dependency metadata—is what fences use.
        goal_b = Goal("B")
        goal_b.task(
            "TB",
            agent="worker",
            verify=scripted_executor(artifact_id="artifact", version=2, content="v2"),
        )
        assert "TB" in os_.run(goal_b, max_dispatches=2).verified

        with pytest.raises(ConfigurationError, match="graph mismatch"):
            os_.repair(goal_b, observation=token)
    finally:
        os_.close()


def test_unregistered_integer_cannot_mint_an_observation() -> None:
    facts = FactsProvider(":memory:")
    with pytest.raises(ValueError, match="without content"):
        facts.issue_observation("missing", 1)


def test_exact_observation_reuses_token_for_vpg_uri_alias() -> None:
    facts = FactsProvider(":memory:")
    first = facts.issue_observation("artifact", 1, graph_id="graph", content=b"payload")
    second = facts.issue_observation(
        "vpg://artifact",
        1,
        graph_id="graph",
        content=b"payload",
    )

    assert second == first


def test_concurrent_in_memory_observations_reuse_one_token() -> None:
    facts = FactsProvider(":memory:")
    facts.add_version("artifact", 1, b"payload")
    facts._observation_tokens = _SnapshotRaceTokens()

    def issue() -> ObservationToken:
        return facts.issue_observation("artifact", 1, graph_id="graph")

    tokens = _issue_concurrently(*([issue] * 8))

    assert len({token.token_id for token in tokens}) == 1
    assert len(facts._observation_tokens) == 1


def test_concurrent_observations_on_one_sqlite_connection_reuse_one_token(tmp_path) -> None:
    facts = FactsProvider(str(tmp_path / "facts.sqlite"))
    try:
        assert facts._conn is not None
        facts._conn = _SlowBeginConnection(facts._conn)

        def issue() -> ObservationToken:
            return facts.issue_observation(
                "artifact",
                1,
                graph_id="graph",
                content=b"payload",
            )

        tokens = _issue_concurrently(issue, issue)

        assert len({token.token_id for token in tokens}) == 1
    finally:
        facts.close()


def test_concurrent_observations_on_separate_sqlite_connections_reuse_one_token(
    tmp_path,
) -> None:
    db = tmp_path / "facts.sqlite"
    left = FactsProvider(str(db))
    right = FactsProvider(str(db))
    try:
        tokens = _issue_concurrently(
            lambda: left.issue_observation(
                "artifact",
                1,
                graph_id="graph",
                content=b"payload",
            ),
            lambda: right.issue_observation(
                "vpg://artifact",
                1,
                graph_id="graph",
                content=b"payload",
            ),
        )

        assert len({token.token_id for token in tokens}) == 1
        with sqlite3.connect(db) as connection:
            row = connection.execute("SELECT COUNT(*) FROM sdk_observation_tokens").fetchone()
        assert row == (1,)
    finally:
        left.close()
        right.close()


def test_workspace_observation_uses_exact_bytes_and_survives_reopen(tmp_path) -> None:
    db = tmp_path / "state.sqlite"
    workspace = WorkspaceTool(tmp_path / "workspace")
    workspace.write("input.txt", "v2")

    os_ = AgentOS(str(db))
    try:
        goal = _closed_goal(os_)
        token = os_.observe_workspace_artifact(goal, workspace, "input.txt", version=2)
        assert token.content_hash == workspace.snapshot("input.txt")["content_hash"]
        token_id = token.token_id
    finally:
        os_.close()

    reopened = AgentOS(str(db))
    try:
        loaded = reopened._facts.get_observation(token_id)
        assert loaded == token
        assert reopened._facts.validate_observation(loaded, graph_id=token.graph_id) == token
    finally:
        reopened.close()
