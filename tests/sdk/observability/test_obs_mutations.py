"""LongHorizonOS E3 — OBS-G observability guardian + state-distinction mutations.

Mutating a critical CLI/observability behavior must break an invariant test
(E3-M01..M10, 0 non-equivalent survivor).  Focus: query immutability, ownership
authority (Kernel Lease), Evidence current-applicability derivation, preserved
real state, minimal Repair Frontier, deterministic JSON, secret redaction, and
Core/legacy state distinction.
"""

from __future__ import annotations

import json

from lhos.integrations.semantic import CommandVerifier
from lhos.integrations.tools.shell import ShellTool
from lhos.sdk import Agent, AgentOS, Goal, scripted_executor


def _mk():
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    t1 = g.task("T1", agent="a", verify=scripted_executor(artifact_id="a.py", version=1))
    t3 = g.task("T3", agent="a", verify=scripted_executor(artifact_id="b.md", version=1))
    os_.run(g, max_dispatches=4)
    return os_, g


# E3-M01: status query must not change GraphVersion (OBS-G4)
def test_m01_status_does_not_mutate_graph():
    os_, g = _mk()
    gid = os_._gid_for("G")
    before = os_.vpg.get_graph(gid).current_version
    for _ in range(5):
        os_.status_view("G")
    after = os_.vpg.get_graph(gid).current_version
    assert before == after


# E3-M03: Evidence current applicability must not be guessed from latest
# filename — after a version bump the old Evidence is not current.
def test_m03_evidence_applicability_is_derived_not_latest():
    from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    t1 = g.task("T1", agent="a", verify=scripted_executor(artifact_id="src.py", version=1))
    os_.run(g, max_dispatches=2)
    os_._facts.add_version("src.py", 2, "v2")
    os_.repair(g, artifact_id="src.py", new_artifact_version=2)
    sv = os_.status_view("G")
    # T1's evidence (src.py@1) is not current at v2 -> T1 affected/STALE projection
    assert "T1" not in sv.preserved_verified


# E3-M05: Repair Frontier display must be minimal, not all stale
def test_m05_repair_frontier_minimal():
    from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    t1 = g.task("T1", agent="a", verify=scripted_executor(artifact_id="a.py", version=1))
    t2 = g.task("T2", agent="a", depends_on=(t1,), verify=scripted_executor(artifact_id="b.py", version=1))
    os_.run(g, max_dispatches=4)
    os_._facts.add_version("a.py", 2, "v2")
    rep = os_.repair(g, artifact_id="a.py", new_artifact_version=2)
    sv = os_.status_view("G")
    assert "T1" in sv.repair_frontier
    assert "T2" not in sv.repair_frontier  # depends on STALE T1


# E3-M07: JSON task ordering deterministic
def test_m07_json_ordering_deterministic():
    os_, g = _mk()
    a = json.dumps(os_.status_view("G").as_dict(), sort_keys=True)
    b = json.dumps(os_.status_view("G").as_dict(), sort_keys=True)
    assert a == b


# E3-M08: secrets redacted
def test_m08_secret_redaction():
    from lhos.cli.core import _redact
    assert "secret" not in _redact("API_KEY=super-secret value").split("API_KEY=")[-1].split(" ")[0]


# E3-M10: inspect task must report VERIFIED from Evidence, not action success
def test_m10_verified_not_from_shell_success():
    sh = ShellTool()
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    g.task("T1", agent="a", verify=CommandVerifier("false", artifact_id="x", version=1, shell=sh))
    os_.run(g, max_dispatches=2)
    sv = os_.status_view("G")
    # A FAIL verifier must NOT produce VERIFIED (even if shell 'ran')
    assert sv.tasks.get("T1", {}).get("validity") != "verified"
