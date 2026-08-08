"""Shared D3 test fixtures.

Exposes the pure-engine runner as a pytest fixture.  Graph primitives live in
``.helpers`` (imported by the test modules directly).
"""

from __future__ import annotations

from typing import Any

import pytest

from lhos.runtimes.invalidation.engine import (
    EngineInputs,
    build_invalidation_result,
    run_invalidation_engine,
)
from lhos.runtimes.invalidation.models import InvalidationCause


@pytest.fixture
def cause():
    def _make(**kw) -> InvalidationCause:
        defaults = dict(
            cause_id="c1",
            graph_id="g",
            graph_version=1,
            cause_type="ARTIFACT_VERSION_SUPERSEDED",
            source_node_id=None,
            artifact_id="A",
            old_version=1,
            new_version=2,
            reason="A v1->v2",
        )
        defaults.update(kw)
        return InvalidationCause(**defaults)

    return _make


@pytest.fixture
def run_engine():
    """Run the pure D3 engine and return the atomic InvalidationResult."""

    def _run(
        *,
        graph_id: str = "g",
        version: int = 1,
        tasks: dict[str, Any] | None = None,
        goals: dict[str, Any] | None = None,
        edges: list[Any] | None = None,
        evidence_nodes: dict[str, Any] | None = None,
        explicit_causes: tuple[InvalidationCause, ...] = (),
        current_output_versions: dict[str, int] | None = None,
        has_active_claim: Any = None,
        goal_direct_tasks: dict[str, tuple[str, ...]] | None = None,
    ) -> Any:
        if tasks is None:
            tasks = {}
        if goals is None:
            goals = {}
        if edges is None:
            edges = []
        if evidence_nodes is None:
            evidence_nodes = {}
        inp = EngineInputs(
            graph_id=graph_id,
            current_version=version,
            task_nodes=tasks,
            goal_nodes=goals,
            evidence_nodes=evidence_nodes,
            edges=edges,
            current_output_versions=current_output_versions,
            explicit_causes=explicit_causes or None,
            has_active_claim=has_active_claim,
            goal_direct_tasks=goal_direct_tasks,
        )
        er = run_invalidation_engine(inp)
        return build_invalidation_result(inp, er)

    return _run
