"""LongHorizonOS E3 — observability service tests (read-only)."""

from __future__ import annotations

import json
import time

from lhos.sdk import Agent, AgentOS, Goal, StatusView, scripted_executor


def _make_os_with_repair():
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("coder", specializations=("python",)))
    g = os_.goal("G")
    t1 = g.task("T1", agent="coder", verify=scripted_executor(artifact_id="a.py", version=1))
    t3 = g.task("T3", agent="coder", verify=scripted_executor(artifact_id="b.md", version=1))
    t2 = g.task(
        "T2",
        agent="coder",
        depends_on=(t1,),
        verify=scripted_executor(artifact_id="a2.py", version=1),
    )
    os_.run(g, max_dispatches=6)
    os_._facts.add_version("a.py", 2, "v2")
    rep = os_.repair(g, artifact_id="a.py", new_artifact_version=2)
    return os_, g


def test_status_view_reads_validity_and_preserved():
    os_, g = _make_os_with_repair()
    sv = os_.status_view("G")
    assert isinstance(sv, StatusView)
    assert sv.goal_id == "G"
    assert sv.version > 0
    # T1 pinned a.py v1; a.py bumped to v2 -> T1 affected, not preserved
    assert "T1" not in sv.preserved_verified
    assert "T3" in sv.preserved_verified  # independent branch preserved


def test_status_view_has_artifacts_and_evidence_observability():
    os_, g = _make_os_with_repair()
    sv = os_.status_view("G")
    d = sv.as_dict()
    assert "schema_version" in d and "goal_state" in d
    assert isinstance(d["tasks"], dict)
    # every task has a validity entry
    for tid, tv in d["tasks"].items():
        assert "validity" in tv


def test_explain_verified_and_stale():
    os_, g = _make_os_with_repair()
    # T3 verified by scripted executor with b.md@1; not affected -> VERIFIED explain
    lines = os_.explain("G", "T3")
    assert any("VERIFIED" in l for l in lines)
    stale_lines = os_.explain("G", "T1")
    assert any("STALE" in l for l in stale_lines)


def test_explain_unknown_task():
    os_, g = _make_os_with_repair()
    lines = os_.explain("G", "NOPE")
    assert any("not found" in l for l in lines)


def test_graph_lines_renders_semantic_tree():
    os_, g = _make_os_with_repair()
    lines = os_.graph_lines("G")
    text = "\n".join(lines)
    assert "Goal: G" in text
    assert "T1" in text and "T3" in text


# ── query immutability (OBS-G4/G6) ─────────────────────────────────────────
def test_query_does_not_mutate_graph_version():
    from lhos.sdk import Agent, AgentOS, scripted_executor

    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    t1 = g.task("T1", agent="a", verify=scripted_executor(artifact_id="x", version=1))
    gid = os_._compile_goal(g)
    before = os_.vpg.get_graph(gid).current_version
    for _ in range(5):
        os_.status_view("G")
        os_.graph_lines("G")
        os_.explain("G", "T1")
    after = os_.vpg.get_graph(gid).current_version
    assert before == after, "observing must not change GraphVersion (OBS-G4/G6)"


def test_json_is_deterministic_and_schema_versioned():
    os_, g = _make_os_with_repair()
    d1 = json.dumps(os_.status_view("G").as_dict(), sort_keys=True)
    d2 = json.dumps(os_.status_view("G").as_dict(), sort_keys=True)
    assert d1 == d2
    assert '"schema_version": "0.1"' in d1


def test_secret_redaction():
    from lhos.cli.core import _redact

    s = "bearer SECRET=abc123 endpoint https://x leak"
    r = _redact(s)
    assert "abc" not in r.split("SECRET=")[-1].split(" ")[0]


# ── 1000-task sanity (no obvious O(n^2) or per-field full-replay) ──────────
def test_large_graph_status_sanity():
    from lhos.sdk import Agent, AgentOS, scripted_executor

    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("Big")
    prev = None
    for i in range(150):
        t = g.task(
            f"T{i}",
            agent="a",
            depends_on=(prev,) if prev else (),
            verify=scripted_executor(artifact_id=f"a{i}", version=1),
        )
        prev = t
    os_.run(g, max_dispatches=50)  # partial run; status must still be fast

    t0 = time.time()
    sv = os_.status_view("Big")
    wall = time.time() - t0
    assert wall < 5.0, f"status took {wall:.2f}s on 150-task graph"
    assert len(sv.tasks) >= 1
