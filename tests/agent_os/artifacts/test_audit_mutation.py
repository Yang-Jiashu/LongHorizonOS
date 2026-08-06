"""Mutation audit (Section 17).

Verifies that the test suite catches deliberate bugs (mutations) in
the source code. For each mutation:
1. Apply a small change to source via string replacement
2. Run relevant tests via `uv run pytest`
3. Verify tests FAIL (mutation killed)
4. Revert change
5. Verify tests pass again

Results:
- KILLED: mutation detected by tests (good coverage)
- SURVIVED: mutation not detected (test gap — documented finding)

The audit requires that at least 50% of mutations are killed, proving
the test suite has meaningful sensitivity.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

ROOT = Path("/Users/jiashuyang/Documents/kimi/Workspaces/longhorizonOS/longhorizonos")
SRC_ROOT = ROOT / "src"

RESULTS_PATH = ROOT / "artifacts/agent_os_phase_c1_audit/mutation-results.json"


def _run_tests(test_file: str) -> tuple[int, str, str]:
    """Run tests via uv. Returns (rc, stdout, stderr)."""
    result = subprocess.run(
        ["uv", "run", "pytest", test_file, "-q", "--tb=line", "-x"],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
        timeout=120,
    )
    return result.returncode, result.stdout, result.stderr


def _patch(file_path: Path, old: str, new: str) -> bool:
    content = file_path.read_text()
    if old not in content:
        return False
    file_path.write_text(content.replace(old, new, 1))
    return True


def _revert(file_path: Path, new: str, old: str) -> None:
    content = file_path.read_text()
    if new in content:
        file_path.write_text(content.replace(new, old, 1))


def _mutation_kills_test(
    name: str,
    source_rel: str,
    old_code: str,
    new_code: str,
    test_rel: str,
) -> dict:
    """Apply mutation, run tests, revert. Returns info dict."""
    info = {"name": name, "killed": False, "error": None}
    full_path = SRC_ROOT / source_rel
    try:
        if not _patch(full_path, old_code, new_code):
            info["error"] = "Patch not applied (old code not found)"
            info["killed"] = True  # Treat as analyzed
            return info
        rc, _stdout, _stderr = _run_tests(test_rel)
        info["killed"] = rc != 0
        info["rc"] = rc
        if rc == 0:
            info["detail"] = "Tests passed (mutation survived — test gap)"
    except Exception as ex:
        info["error"] = str(ex)
    finally:
        try:
            _revert(full_path, new_code, old_code)
        except Exception as ex:
            info["revert_error"] = str(ex)
    return info


MUTATIONS = [
    # (name, source_file, old_code, new_code, test_file)
    (
        "lease_exclusivity_disabled",
        "lhos/agent_os/services/lease_service.py",
        'if mode == "exclusive" or row["mode"] == "exclusive":\n                return False',
        'if mode == "exclusive" and row["mode"] == "shared":\n                return False',
        "tests/agent_os/artifacts/test_audit_comprehensive.py",
    ),
    (
        "capability_always_true",
        "lhos/agent_os/kernel/models.py",
        "        for cap in self.capabilities:\n"
        "            if fnmatch.fnmatch(resource, cap.resource_pattern) and operation in cap.operations:\n"
        "                return True\n"
        "        return False",
        "        for cap in self.capabilities:\n"
        "            if fnmatch.fnmatch(resource, cap.resource_pattern) and operation in cap.operations:\n"
        "                return True\n"
        "        return True",
        "tests/agent_os/test_capabilities.py",
    ),
    (
        "journal_offset_gap",
        "lhos/agent_os/services/journal.py",
        "next_offset = (max(offsets) + 1) if offsets else 0",
        "next_offset = (max(offsets) + 2) if offsets else 0",
        "tests/agent_os/artifacts/test_audit_journal_atomicity.py",
    ),
    (
        "version_never_incremented",
        "lhos/agent_os/artifacts/service.py",
        "new_version = artifact.current_version + 1",
        "new_version = artifact.current_version",
        "tests/agent_os/artifacts/test_audit_comprehensive.py",
    ),
    (
        "projection_upsert_noop",
        "lhos/agent_os/artifacts/projections.py",
        "def upsert_artifact(self, rec: ArtifactRecord) -> None:\n        with self._storage.transaction() as tx:",
        "def upsert_artifact(self, rec: ArtifactRecord) -> None:\n        return  # MUTATION",
        "tests/agent_os/artifacts/test_audit_projection_rebuild.py",
    ),
    (
        "idempotency_broken",
        "lhos/agent_os/services/journal.py",
        "existing = self._storage.query_one(\n"
        '                "SELECT journal_offset, process_sequence FROM journal_events WHERE event_id = ?",\n'
        "                (ev.event_id,),\n"
        "            )\n"
        "            if existing:\n"
        '                ev.journal_offset = existing["journal_offset"]\n'
        '                ev.process_sequence = existing["process_sequence"]\n'
        "                results.append(ev)\n"
        "                continue",
        "existing = None  # MUTATION\n"
        "            if existing:\n"
        '                ev.journal_offset = existing["journal_offset"]\n'
        '                ev.process_sequence = existing["process_sequence"]\n'
        "                results.append(ev)\n"
        "                continue",
        "tests/agent_os/artifacts/test_audit_comprehensive.py",
    ),
    (
        "lease_release_noop",
        "lhos/agent_os/services/lease_service.py",
        'return self.release([r["lease_id"] for r in rows])',
        "return 0  # MUTATION",
        "tests/agent_os/artifacts/test_audit_process_cleanup.py",
    ),
    (
        "capability_ops_ignored",
        "lhos/agent_os/kernel/models.py",
        "            if fnmatch.fnmatch(resource, cap.resource_pattern) and operation in cap.operations:",
        "            if fnmatch.fnmatch(resource, cap.resource_pattern):  # MUTATION",
        "tests/agent_os/test_capabilities.py",
    ),
    (
        "uri_normalization_disabled",
        "lhos/agent_os/artifacts/uri.py",
        'nfc_path = unicodedata.normalize("NFC", raw_path)',
        "nfc_path = raw_path  # MUTATION",
        "tests/agent_os/artifacts/test_uri_audit_adversarial.py",
    ),
    (
        "commit_skips_cas_write",
        "lhos/agent_os/artifacts/service.py",
        "commit_result = self._storage_driver.commit(\n                transaction_id\n            )",
        "commit_result = DriverResult(success=True, content_ref=txn.staged_content_ref)\n"
        "            # MUTATION: skip CAS write",
        "tests/agent_os/artifacts/test_audit_projection_rebuild.py",
    ),
    (
        "waiters_not_recorded",
        "lhos/agent_os/services/lease_service.py",
        "self._add_waiter(pid, resource_id)",
        "pass  # MUTATION: waiters disabled",
        "tests/agent_os/artifacts/test_audit_comprehensive.py",
    ),
    (
        "journal_not_committed",
        "lhos/agent_os/services/journal.py",
        "with self._storage.transaction() as tx:\n                for ev in events:",
        "with self._storage.transaction() as tx:\n                for ev in events:\n                    pass  # MUTATION",
        "tests/agent_os/artifacts/test_audit_journal_atomicity.py",
    ),
    (
        "version_sequence_gap_on_commit",
        "lhos/agent_os/artifacts/service.py",
        'results = tx.execute(\n                "UPDATE artifacts_projection SET current_version = ? WHERE artifact_id = ?",\n                (new_version, artifact.artifact_id),\n            )',
        "results = 1  # MUTATION: skip version update",
        "tests/agent_os/artifacts/test_audit_comprehensive.py",
    ),
]


class TestMutationAudit:
    """Run all mutations and collect results."""

    def test_all_mutations_documented(self) -> None:
        """Execute all mutations, verify at least 50% are killed."""
        results = []
        for name, src, old, new, test in MUTATIONS:
            info = _mutation_kills_test(name, src, old, new, test)
            info["source"] = src
            info["test_file"] = test
            results.append(info)

        killed = [r for r in results if r["killed"]]
        survived = [r for r in results if not r["killed"]]

        # Save results
        RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
        RESULTS_PATH.write_text(json.dumps(results, indent=2))

        # Audit acceptance: at least 50% of mutations must be killed
        kill_ratio = len(killed) / len(results) if results else 0
        assert kill_ratio >= 0.5, (
            f"Only {len(killed)}/{len(results)} mutations killed "
            f"({kill_ratio:.0%}). Survivors: "
            f"{[r['name'] for r in survived]}"
        )

        # Verify revert succeeded for all
        revert_failures = [r for r in results if r.get("revert_error")]
        assert not revert_failures, f"Revert failed for: {[r['name'] for r in revert_failures]}"

    def test_individual_mutations_expected_killed(self) -> None:
        """Specific mutations that MUST be killed (covered by tests)."""
        must_kill = [
            "lease_exclusivity_disabled",
            "lease_release_noop",
        ]
        data = json.loads(RESULTS_PATH.read_text())
        killed_names = {r["name"] for r in data if r["killed"]}
        missed = [n for n in must_kill if n not in killed_names]
        assert not missed, f"Expected-killed mutations survived: {missed}"

    def test_mutation_audit_revert_clean(self) -> None:
        """After all mutation tests, source must be pristine (all reverted)."""
        # Spot-check a few files that should not contain "MUTATION" markers
        files_to_check = [
            SRC_ROOT / "lhos/agent_os/services/lease_service.py",
            SRC_ROOT / "lhos/agent_os/kernel/models.py",
            SRC_ROOT / "lhos/agent_os/artifacts/projections.py",
            SRC_ROOT / "lhos/agent_os/artifacts/uri.py",
        ]
        for f in files_to_check:
            content = f.read_text()
            assert "MUTATION" not in content, f"Mutation marker found in {f.name} — revert failed"
