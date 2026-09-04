"""D3 graph-version concurrency race (§19): a computation bound to an old
graph version must never silently commit; it must abort and recompute against
the newest version.  Also atomicity (§20): a failed invalidation must have
zero partial semantic effect."""

from __future__ import annotations

import pytest

from lhos.runtimes.invalidation.runtime import (
    InvalidationRuntime,
    InvalidGraphVersionRace,
)

from .helpers import TNode


# ---- §19: GraphVersion race detection ----
def test_graph_version_race_raises_not_silent_merge():
    """If graph moved to a newer version mid-computation, stale v must be
    rejected."""
    version_counter = {"v": 10}
    rt = InvalidationRuntime(
        current_version_of=lambda gid: version_counter["v"],
        task_nodes_of=lambda gid: {"T1": TNode("T1", "verified")},
        goal_nodes_of=lambda gid: {},
        evidence_nodes_of=lambda gid: {},
        edges_of=lambda gid: [],
    )
    # A Patch bumps the version WHILE the invalidation is being computed.
    version_counter["v"] = 11
    # The runtime detects the race at commit time.
    with pytest.raises(InvalidGraphVersionRace):
        rt.assert_version_is_current("g", compute_version=10)


def test_no_race_when_version_unchanged():
    rt = InvalidationRuntime(current_version_of=lambda gid: 10)
    # no exception
    rt.assert_version_is_current("g", compute_version=10)


def test_engine_binds_to_base_graph_version(run_engine, cause):
    """A cause bound to v10 and one to v11 must produce different cone hashes
    (no silent merge of stale state across versions)."""
    tasks = {"T1": TNode("T1", "verified")}
    edges = []
    c10 = cause(graph_version=10, source_node_id="T1")
    c11 = cause(graph_version=11, source_node_id="T1")
    r10 = run_engine(tasks=tasks, edges=edges, explicit_causes=(c10,), version=10)
    r11 = run_engine(tasks=tasks, edges=edges, explicit_causes=(c11,), version=11)
    assert r10.cone.cone_hash != r11.cone.cone_hash


# ---- §20: atomicity — no partial semantic effect on forced failure ----
def test_failed_invalidation_has_zero_partial_effect(run_engine, cause):
    """The pure engine must NEVER mutate the authoritative graph.  If the host
    aborts after computing (or due to a version-race exception), the graph is
    untouched => zero partial semantic effect by construction."""
    from copy import deepcopy

    from .helpers import depends_on

    tasks = {"T1": TNode("T1", "verified"), "T2": TNode("T2", "verified")}
    edges = [depends_on("T1", "T2")]
    c = cause(source_node_id="T2")

    before = deepcopy({k: (n.validity.value, n.lifecycle.value) for k, n in tasks.items()})
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))
    after = {k: (n.validity.value, n.lifecycle.value) for k, n in tasks.items()}
    assert before == after, "engine mutated the authoritative graph (atomicity violated)"
    # InvalidationResult carries the derived stale set, but the graph stays intact.
    assert res.stale_nodes == ("T1", "T2")
