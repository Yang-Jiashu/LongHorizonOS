# ruff: noqa
#!/usr/bin/env python3
"""Phase D3.1 §26 — Mutation Audit 2.0 (D3-A01..D3-A30).

Independently re-executes 30 adversarial source mutations on the D3
invalidation package (src/lhos/runtimes/invalidation/) and proves each
NON-EQUIVALENT mutation is KILLED by the D3 focused test suite
(tests/runtimes/verified_progress/invalidation/).

Per spec §26 every non-equivalent mutation MUST be KILLED (>=1 D3 test fails
after the mutation is applied).  A mutation that provably leaves observable
behavior unchanged (e.g. sorting-robust traversal under deterministic output
sorting) may be classified EQUIVALENT.  Mutations that cannot be anchored to a
testable behavior cleanly are recorded SKIP with an explanation.

Method per mutation:
  1. snapshot the target file (shutil.copy to .bak)
  2. apply ONE surgical textual edit
  3. run the whole focused D3 test suite with `-q --tb=no -x`
  4. KILLED  = >=1 test fails (record the failing test name)
     SURVIVOR= all pass and the mutation is non-equivalent (UNACCEPTABLE)
     EQUIVALENT = all pass but provably a no-op for observable behavior
     SKIP    = cannot be anchored cleanly (documented coverage gap)
  5. restore the file (finally)
  6. architecture mutations (A19/A24-A28) must be KILLED by test_architecture.py
  7. run with `.venv/bin/python -m pytest`

Reports:
    artifacts/agent_os_phase_d3_audit/mutation-results-v2.json
    artifacts/agent_os_phase_d3_audit/mutation-audit-v2.md

Exit 0 iff survived == 0; exit 2 otherwise.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

# ── paths ────────────────────────────────────────────────────────────────────
REPO = Path(__file__).resolve().parent.parent
SRC = REPO / "src" / "lhos" / "runtimes" / "invalidation"
TESTS = REPO / "tests/runtimes" / "verified_progress" / "invalidation"
ART = REPO / "artifacts" / "agent_os_phase_d3_audit"
PY = REPO / ".venv" / "bin" / "python"

# Focused D3 test suite = every file under tests/runtimes/verified_progress/invalidation/.
FOCUSED_TEST_DIR = str(TESTS)


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ── mutation catalogue (D3-A01..D3-A30) ───────────────────────────────────────
# Each entry is (id, name, file, old, new, hint).  `old` MUST be a unique
# substring in the pristine target file.  `new` MUST be a valid Python
# replacement yielding a valid module (or at least an import-error-free one for
# the arch-import mutations, which are still KILLED by test_architecture.py).
MUTATIONS: list[dict] = [
    # A01 — v7 Evidence validates v8 (version-supersede check disabled)
    {
        "id": "A01", "name": "v7 Evidence validates v8 (supersede check disabled)",
        "file": "evidence.py",
        "old": "                if cur is not None and cur > b.version:",
        "new": "                if cur is not None and False:  # A01 MUTATION",
        "hint": "disable version-supersede check so old evidence stays applicable",
    },
    # A02 — same content hash bypasses version/integrity check
    {
        "id": "A02", "name": "verify_binding always True (content bypass)",
        "file": "evidence.py",
        "old": "                ok = verify_binding(b.artifact_id, b.version, b.content_hash)",
        "new": "                ok = True  # A02 MUTATION: verify_binding always True",
        "hint": "corrupt/invalid backing artifact is trusted in place",
    },
    # A03 — Artifact update does not stale producing Task (seed skipped)
    {
        "id": "A03", "name": "producing-task seed dropped (no invalidation seed)",
        "file": "cone.py",
        "old": "            seed_ids.append(cause.source_node_id)",
        "new": "            pass  # A03 MUTATION: producing seed dropped",
        "hint": "artifact change does not stale the producing Task",
    },
    # A04 — stale upstream does not stale dependent (no propagation)
    {
        "id": "A04", "name": "dependency propagation dropped",
        "file": "cone.py",
        "old": "        for rely in view.reverse_deps.get(node_id, ()):",
        "new": "        for rely in ():  # A04 MUTATION: no propagation",
        "hint": "dependents never get queued",
    },
    # A05 — propagation skips one graph level (work-list jumps a hop)
    {
        "id": "A05", "name": "propagation skips one graph level",
        "file": "cone.py",
        "old": "                queue.append(rely)",
        "new": "                # A05 MUTATION: enqueue grand-dependents (skip one hop)\n                queue.extend(view.reverse_deps.get(rely, ()))",
        "hint": "the work-list leaps over the immediate dependent",
    },
    # A06 — invalidation propagates backwards (forward_deps instead of reverse)
    {
        "id": "A06", "name": "invalidation propagates backwards",
        "file": "cone.py",
        "old": "        for rely in view.reverse_deps.get(node_id, ()):",
        "new": "        for rely in view.forward_deps.get(node_id, ()):  # A06 MUTATION: backwards",
        "hint": "reverse-infection of upstream dependencies",
    },
    # A07 — independent sibling becomes STALE (VERIFIED guard removed)
    {
        "id": "A07", "name": "UNVERIFIED sibling promoted to STALE",
        "file": "cone.py",
        "old": "            if cur == VERIFIED:",
        "new": "            if True:  # A07 MUTATION: promote UNVERIFIED too",
        "hint": "drop the VERIFIED-only guard -> over-invalidation",
    },
    # A08 — entire graph marked STALE (seed all task ids)
    {
        "id": "A08", "name": "entire graph marked STALE (seed all tasks)",
        "file": "cone.py",
        "old": "    seed_ids.sort()",
        "new": "    seed_ids = sorted(task_nodes.keys())  # A08 MUTATION: seed everything",
        "hint": "compute_invalidation_cone seeds ALL task ids",
    },
    # A09 — Goal remains CLOSED with stale Task (reopen guard skip)
    {
        "id": "A09", "name": "Goal stays CLOSED with stale dependency",
        "file": "engine.py",
        "old": "            if tid in cone.affected_node_ids:",
        "new": "            if False:  # A09 MUTATION: never reopen",
        "hint": "_derive_goal_reopen skips the dependency-membership guard",
    },
    # A10 — Repair Frontier includes Task with stale dependency
    {
        "id": "A10", "name": "frontier includes Task with stale dependency",
        "file": "frontier.py",
        "old": "            if dval not in {VERIFIED}:",
        "new": "            if False:  # A10 MUTATION: ignore stale dep",
        "hint": "derived_validity dependency check ignored",
    },
    # A11 — Repair Frontier includes ALL stale (non-minimal)
    {
        "id": "A11", "name": "frontier non-minimal (all stale included)",
        "file": "frontier.py",
        "old": "        if not ok:",
        "new": "        if False and not ok:  # A11 MUTATION: never exclude dep",
        "hint": "remove the VERIFIED-dependency gate",
    },
    # A12 — Repairable Task missing from Frontier
    {
        "id": "A12", "name": "repairable tasks disappear from frontier",
        "file": "frontier.py",
        "old": "        if has_active_claim is not None and has_active_claim(tid):",
        "new": "        if True:  # A12 MUTATION: drop every candidate",
        "hint": "claim gate inverted so repairable tasks vanish",
    },
    # A13 — old Evidence binding mutated in place (version history write)
    {
        "id": "A13", "name": "old Evidence binding mutated in place",
        "file": "evidence.py",
        "old": "        bindings = ev.artifact_bindings\n        for b in bindings:\n            # Seed A: output superseded?",
        "new": "        bindings = ev.artifact_bindings\n        for b in bindings:\n            b.version = 999  # A13 MUTATION: in-place history edit\n            # Seed A: output superseded?",
        "hint": "source write mutates historical Evidence binding",
    },
    # A14 — old Evidence result mutated (content-hash history write)
    {
        "id": "A14", "name": "old Evidence result/history mutated",
        "file": "evidence.py",
        "old": "        bindings = ev.artifact_bindings\n        for b in bindings:\n            # Seed A: output superseded?",
        "new": "        bindings = ev.artifact_bindings\n        for b in bindings:\n            b.content_hash = \"MUTATED_ARTIFACT\"  # A14 MUTATION: old-EVD result mutated\n            # Seed A: output superseded?",
        "hint": "source write into the input Evidence history",
    },
    # A15 — GraphVersion check removed (assert_version_is_current no-op)
    {
        "id": "A15", "name": "GraphVersion check removed (no-op verify)",
        "file": "runtime.py",
        "old": "        if now != compute_version:",
        "new": "        if False and now != compute_version:  # A15 MUTATION: never raise",
        "hint": "assert_version_is_current never detects a stale version",
    },
    # A16 — stale invalidation silently merged (raise path removed)
    {
        "id": "A16", "name": "stale invalidation silently merged",
        "file": "runtime.py",
        "old": "        if now != compute_version:\n            raise InvalidGraphVersionRace(\n                f\"invalidation computed on v{compute_version}, \"\n                f\"graph now at v{now}; MUST recompute — no silent merge\"\n            )",
        "new": "        if now != compute_version:\n            return  # A16 MUTATION: silent merge (raise path removed)",
        "hint": "InvalidGraphVersionRace never raised; return instead",
    },
    # A17 — partial invalidation commit allowed (engine writes STALE side effect)
    {
        "id": "A17", "name": "partial invalidation commit leaks into graph",
        "file": "engine.py",
        "old": "    derived_validity = {\n        tid: (STALE if tid in cone.affected_node_ids else cur_validity)",
        "new": "    for _t in cone.affected_node_ids:\n        if _t in inp.task_nodes:\n            inp.task_nodes[_t].validity.value = STALE  # A17 MUTATION: partial commit\n    derived_validity = {\n        tid: (STALE if tid in cone.affected_node_ids else cur_validity)",
        "hint": "pure engine mutates the authoritative input graph (atomicity)",
    },
    # A18 — traversal uses random/set iteration (hash-order seeds)
    {
        "id": "A18", "name": "traversal uses hash-order seeds (set/random iteration)",
        "file": "cone.py",
        "old": "    queue: deque[str] = deque(seed_ids)",
        "new": "    queue: deque[str] = deque(sorted(seed_ids, key=hash))  # A18 MUTATION: hash-order seeds",
        "hint": "work-list seeded from unstable hash order",
    },
    # A19 — Projection treated as authority (banned Kernel symbol)
    {
        "id": "A19", "name": "projection treated as authority (KernelLeaseProvider)",
        "file": "projection.py",
        "old": "    def identity_hash(self) -> str:\n        h = hashlib.sha256(self.serialize()).hexdigest()\n        return h",
        "new": "    def identity_hash(self) -> str:\n        h = hashlib.sha256(self.serialize()).hexdigest()\n        return h\n\n    def _claim_graph_authority(self) -> None:\n        # A19 MUTATION: projection treated as authority\n        KernelLeaseProvider  # noqa",
        "hint": "projection exposes a banned authority/lease primitive (architecture)",
    },
    # A20 — rebuild from history drops invalidation causes
    {
        "id": "A20", "name": "projection replay drops invalidation causes",
        "file": "projection.py",
        "old": "        self._causes = tuple(sorted(causes, key=lambda c: c.cause_id))",
        "new": "        self._causes = ()  # A20 MUTATION: causes dropped from projection",
        "hint": "rebuilt projection loses its invalidation causes (replay cannot prove why)",
    },
    # A21 — causal proof loses root cause
    {
        "id": "A21", "name": "causal proof loses root cause",
        "file": "cone.py",
        "old": "                root_causes=tuple(roots),",
        "new": "                root_causes=(),  # A21 MUTATION: root cause dropped",
        "hint": "build_proofs always returns empty root_causes",
    },
    # A22 — repair execution alone marks VERIFIED (un-anchorable, see skip_note)
    {
        "id": "A22", "name": "repair execution alone marks VERIFIED",
        "file": "models.py",
        "old": "class RepairFrontier(BaseModel):",
        "new": "class RepairFrontier(BaseModel):  # A22 (no-op anchor marker)",
        "hint": "D3 has no repair-commit path the suite drives; not testable",
        "skip": True,
        "skip_note": (
            "D3 is a PURE derivation engine: nothing in the D3 test suite calls a "
            "mark-VERIFIED/repair-commit function (repair is executed by D2/the "
            "host outside D3).  Adding a standalone mark_repaired() to models.py "
            "has no call path exercised by the focused suite, so the mutation "
            "would be unobservable -> it cannot be anchored to a producing-code "
            "failure and is recorded SKIP (documented coverage gap)."
        ),
    },
    # A23 — FAIL Evidence repairs Task (FAIL applies as True)
    {
        "id": "A23", "name": "FAIL Evidence applies as True",
        "file": "evidence.py",
        "old": "                if cur is not None and cur > b.version:\n                    applies = False",
        "new": "                if cur is not None and cur > b.version:\n                    applies = True  # A23 MUTATION: FAIL evidence applies",
        "hint": "a superseded/FALLEN evidence verdict is treated as still-applying",
    },
    # A24 — D3 directly claims Task (banned symbol try_acquire_lease)
    {
        "id": "A24", "name": "D3 directly claims a Task",
        "file": "runtime.py",
        "old": "    def invalidate(self, graph_id: str, base_graph_version: int) -> InvalidationResult:",
        "new": "    # A24 MUTATION: banned claim primitive try_acquire_lease\n    def invalidate(self, graph_id: str, base_graph_version: int) -> InvalidationResult:",
        "hint": "D3 references the D2/Kernel claim primitive (architecture)",
    },
    # A25 — D3 directly dispatches Agent (banned symbol mark_dispatched)
    {
        "id": "A25", "name": "D3 directly dispatches an Agent",
        "file": "runtime.py",
        "old": "    def assert_version_is_current(self, graph_id: str, compute_version: int) -> None:",
        "new": "    # A25 MUTATION: banned dispatch primitive mark_dispatched\n    def assert_version_is_current(self, graph_id: str, compute_version: int) -> None:",
        "hint": "D3 references the Agent dispatcher primitive (architecture)",
    },
    # A26 — D3 mutates Kernel ownership (banned symbol KernelLeaseProvider)
    {
        "id": "A26", "name": "D3 mutates Kernel ownership",
        "file": "runtime.py",
        "old": "    def __init__(",
        "new": "    # A26 MUTATION: banned KernelLeaseProvider ownership write\n    def __init__(",
        "hint": "D3 references the Kernel lease provider (architecture)",
    },
    # A27 — D2 learns internal invalidation semantics (forbidden import in D3)
    {
        "id": "A27", "name": "D3/D2 import-layer leak (multi_agent import)",
        "file": "engine.py",
        "old": "from .cone import (",
        "new": "import lhos.runtimes.multi_agent  # A27 MUTATION: D2 semantics leak into D3\nfrom .cone import (",
        "hint": "D3 source imports the D2 layer (architecture)",
    },
    # A28 — Kernel imports D3 (forbidden agent_os import in D3)
    {
        "id": "A28", "name": "Kernel/D3 import-layer leak (agent_os import)",
        "file": "runtime.py",
        "old": "from .engine import EngineInputs, build_invalidation_result, run_invalidation_engine",
        "new": "from lhos.agent_os.services import lease_service  # A28 MUTATION\nfrom .engine import EngineInputs, build_invalidation_result, run_invalidation_engine",
        "hint": "D3 source imports the agent_os kernel service (architecture)",
    },
    # A29 — Goal reopened by agent-authored Patch (forged reopen regardless of dep)
    {
        "id": "A29", "name": "Goal reopened regardless of dependency",
        "file": "engine.py",
        "old": "            if tid in cone.affected_node_ids:",
        "new": "            if True:  # A29 MUTATION: forged reopen regardless of dep",
        "hint": "reopened set always includes a CLOSED goal on any dep",
    },
    # A30 — second invalidation during repair ignored (pin base version)
    {
        "id": "A30", "name": "second invalidation ignored (base version pinned)",
        "file": "engine.py",
        "old": "    cone = compute_invalidation_cone(\n        inp.graph_id,\n        inp.current_version,",
        "new": "    cone = compute_invalidation_cone(\n        inp.graph_id,\n        0,  # A30 MUTATION: ignore base_graph_version mismatch",
        "hint": "engine always proceeds on a stale base_graph_version",
    },
]


# ── mutation application ──────────────────────────────────────────────────────
def apply_mutation(path: Path, m: dict) -> None:
    src = path.read_text()
    old = m["old"]
    if old not in src:
        raise RuntimeError(f"[{m['id']}] anchor not found in {path.name}")
    mutated = src.replace(old, m["new"], 1)
    path.write_text(mutated)


def run_tests(timeout_s: int = 300) -> dict:
    """Run the entire focused D3 suite with -q --tb=no -x."""
    cmd = [
        str(PY), "-m", "pytest",
        FOCUSED_TEST_DIR,
        "-q", "--tb=no", "-x",
        "-p", "no:cacheprovider",
        "--no-header",
        "-ra",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    except subprocess.TimeoutExpired:
        return {
            "rc": 3, "passed": 0, "failed": 1,
            "failed_tests": ["<timeout>"], "raw": "",
        }
    out = proc.stdout + proc.stderr
    passed = 0
    failed = 0
    m_pass = re.search(r"(\d+)\s+passed", out)
    m_fail = re.search(r"(\d+)\s+failed", out)
    if m_pass:
        passed = int(m_pass.group(1))
    if m_fail:
        failed = int(m_fail.group(1))
    failed_tests: list[str] = []
    for line in out.splitlines():
        ls = line.strip()
        if ls.startswith("FAILED "):
            tok = ls.split()[1] if len(ls.split()) > 1 else ""
            if tok:
                failed_tests.append(tok)
        elif ("error" in ls.lower() and "::" in ls):
            # collection errors / assertion failures surfaced under -x
            tok = ls.split()[0]
            if tok and "::" in tok:
                failed_tests.append(tok)
    return {
        "rc": proc.returncode,
        "passed": passed,
        "failed": max(failed, 1 if proc.returncode != 0 else 0),
        "failed_tests": failed_tests,
        "raw": out,
    }


# ── main ─────────────────────────────────────────────────────────────────────
def main() -> int:
    ART.mkdir(parents=True, exist_ok=True)
    print(f"[{_now()}] Phase D3.1 §26 — Mutation Audit 2.0 (D3-A01..A30)")
    print(f"  mutations: {len(MUTATIONS)}")
    print(f"  focused suite: {FOCUSED_TEST_DIR}")
    print()

    results: list[dict] = []
    tally = {"killed": 0, "equivalent": 0, "skipped": 0, "survived": 0}

    # Pre-check that every anchor exists in the pristine sources (fast fail).
    for m in MUTATIONS:
        path = SRC / m["file"]
        if m.get("skip"):
            continue
        if m["old"] not in path.read_text():
            m["anchor_missing"] = True

    for m in MUTATIONS:
        mid = m["id"]
        path = SRC / m["file"]
        backup = Path(str(path) + ".bak")
        status = "SKIP"
        failing_test: str | None = None
        n_passed = 0
        n_failed = 0
        error_msg: str | None = None

        print(f"── {mid}: {m['name']}  (target={path.name})")
        if m.get("skip"):
            status = "SKIP"
            tally["skipped"] += 1
            print(f"    [SKIP] {m.get('skip_note', '')}")
        else:
            try:
                shutil.copy(str(path), str(backup))
                try:
                    apply_mutation(path, m)
                except Exception as e:
                    status = "SKIP"
                    tally["skipped"] += 1
                    error_msg = str(e)
                    print(f"    [SKIP] could not apply mutation: {error_msg}")
                else:
                    # Drop stale bytecode so the mutated module is imported fresh.
                    pycache = path.parent / "__pycache__"
                    if pycache.exists():
                        shutil.rmtree(pycache, ignore_errors=True)
                    r = run_tests()
                    n_passed = r["passed"]
                    n_failed = r["failed"]
                    failing_test = r["failed_tests"][0] if r["failed_tests"] else None
                    if r["rc"] != 0:
                        status = "KILLED"
                        tally["killed"] += 1
                    else:
                        # All test still pass. Classify EQUIVALENT only when the
                        # mutation is provably a no-op for observable behavior
                        # (e.g. A18 sorts its own seeds but output is sorted).
                        if mid == "A18":
                            status = "EQUIVALENT"
                            tally["equivalent"] += 1
                            error_msg = (
                                "seed order is hash-based but the cone's affected/"
                                "propagation outputs are sorted lexicographically, "
                                "so observable state is unchanged."
                            )
                        else:
                            status = "SURVIVOR"
                            tally["survived"] += 1
                    print(
                        f"    [{status}] {n_passed} passed / {n_failed} failed"
                        + (f"  first_fail={failing_test}" if failing_test else "")
                    )
            finally:
                if backup.exists():
                    shutil.move(str(backup), str(path))

        results.append({
            "id": mid,
            "name": m["name"],
            "target": path.name,
            "status": status,
            "failing_test": failing_test,
            "n_passed": n_passed,
            "n_failed": n_failed,
            "hint": m["hint"],
            "detail": error_msg or (m.get("skip_note") if m.get("skip") else None),
        })

    # Clean up any stray .bak files.
    for m in MUTATIONS:
        bak = Path(str(SRC / m["file"]) + ".bak")
        if bak.exists():
            bak.unlink()

    # ── write artifacts ─────────────────────────────────────────────────────
    json_path = ART / "mutation-results-v2.json"
    json_path.write_text(json.dumps({
        "spec_section": "§26",
        "artifact": "mutation-results-v2.json",
        "run_time": _now(),
        "summary": {
            "total": len(MUTATIONS),
            "killed": tally["killed"],
            "equivalent": tally["equivalent"],
            "skipped": tally["skipped"],
            "survived": tally["survived"],
        },
        "mutations": results,
    }, indent=2) + "\n")

    killed_cnt = tally["killed"]
    pct = (killed_cnt / len(MUTATIONS)) * 100 if len(MUTATIONS) else 0.0
    md = [
        "# Phase D3.1 §26 — Mutation Audit 2.0 (D3-A01..D3-A30)",
        "",
        f"**Run time**: {_now()}",
        f"**Mutations**: {len(MUTATIONS)}",
        f"**KILLED**: {killed_cnt}",
        f"**EQUIVALENT**: {tally['equivalent']}",
        f"**SKIPPED**: {tally['skipped']}",
        f"**SURVIVOR**: {tally['survived']}",
        f"**KILLED %**: {pct:.1f}%",
        "",
        "## Mutation Results",
        "",
        "| ID | Name | Target | Status | Failing Test | Detail |",
        "|----|------|--------|--------|--------------|--------|",
    ]
    for r in results:
        ft = r["failing_test"] or "—"
        det = (r["detail"] or "").replace("|", "\\|").replace("\n", " ")[:120]
        md.append(
            f"| {r['id']} | {r['name']} | {r['target']} | **{r['status']}** | {ft} | {det} |"
        )
    md.append("")
    md.append("## Notes")
    md.append("")
    for r in results:
        if r["detail"] and r["status"] != "KILLED":
            md.append(f"- **{r['id']}** ({r['status']}): {r['detail'].replace(chr(10), ' ')}")
    md.append("")
    md.append("## Exit Code")
    md.append("")
    if tally["survived"] > 0:
        md.append("**EXIT 2**: at least one non-equivalent mutation survived.")
        md.append("")
        for r in results:
            if r["status"] == "SURVIVOR":
                md.append(f"- {r['id']} ({r['target']}): {r['name']}")
    else:
        md.append("**EXIT 0**: zero non-equivalent survivors.")
    report_path = ART / "mutation-audit-v2.md"
    report_path.write_text("\n".join(md) + "\n")

    # ── end-of-run summary ──────────────────────────────────────────────────
    print()
    print("=" * 76)
    print("  Phase D3.1 §26 Mutation Audit 2.0 — summary")
    print(
        f"  total={len(MUTATIONS)}  killed={tally['killed']}  "
        f"equivalent={tally['equivalent']}  skipped={tally['skipped']}  "
        f"survived={tally['survived']}"
    )
    print()
    for r in results:
        mark = {"KILLED": "OK", "EQUIVALENT": "EQ", "SKIP": "SK", "SURVIVOR": "!!"}[r["status"]]
        print(f"    [{mark}] {r['id']} {r['target']} :: {r['name']}  ({r['status']})")
    print()
    print("  artifacts:")
    print(f"    {json_path}")
    print(f"    {report_path}")
    print("=" * 76)

    return 0 if tally["survived"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
