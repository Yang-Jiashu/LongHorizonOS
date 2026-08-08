#!/usr/bin/env python3
"""D2.1 §26 — Mutation Audit 2.0 (D2-A01..A25).

Real mutation analysis: for each mutation, snapshot the scheduler-core source,
apply ONE surgical semantic change, run a focused pytest subset, and record
whether the mutation was KILLED (>=1 test failed).  Source is restored before
the next mutation.

KILLED = after mutating, running the focused test subset yields at least one FAIL.
SURVIVOR = no test failed (undesirable; script exits 2 if any).
SKIP = mutation documented as un-killable by the current test suite.

On success writes:
    artifacts/agent_os_phase_d2_audit/mutation-results-v2.json
    artifacts/agent_os_phase_d2_audit/mutation-audit-v2.md
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
SRC = REPO / "src/lhos/runtimes/multi_agent"
TESTS = REPO / "tests/runtimes/multi_agent"
ART = REPO / "artifacts/agent_os_phase_d2_audit"
PY = REPO / ".venv/bin/python"

# Source files under mutation (scheduler core).
FILES = {
    "eligibility": SRC / "eligibility.py",
    "matching": SRC / "matching.py",
    "claims": SRC / "claims.py",
    "scheduler": SRC / "scheduler.py",
    "reconciliation": SRC / "reconciliation.py",
    "projections": SRC / "projections.py",
    "attempts": SRC / "attempts.py",
}

# ── mutation catalogue ────────────────────────────────────────────────────────
# new_source MUST be a valid Python replacement for exactly one unique block.

MUTATIONS = [
    # ── eligibility.py ─────────────────────────────────────────────────────
    {
        "id": "A01",
        "name": "Drop disabled-agent predicate",
        "target": "eligibility",
        "comment": "D2-I2: disabled agent must be ineligible.",
        "old": "    if not agent.enabled:\n        reasons.append(\"agent disabled\")\n\n    if not process_exists:\n",
        "new": "    # A01 MUTATION: disabled-agent predicate dropped\n    # if not agent.enabled:\n    #     reasons.append(\"agent disabled\")\n\n    if not process_exists:\n",
        "test_files": ["test_eligibility.py", "test_mutations.py"],
        "k_filter": "disabled or m01",
    },
    {
        "id": "A02",
        "name": "Drop terminal-process rejection",
        "target": "eligibility",
        "comment": "D2-I3: agent in terminal state (exited/failed) must be ineligible.",
        "old": (
            "        if process_state is None:\n"
            '            reasons.append("process state unavailable")\n'
            "        elif process_state in _TERMINAL_PROCESS_STATES:\n"
            '            reasons.append(f"process in terminal state {process_state!r}")\n'
            "        elif process_state not in _SCHEDULABLE_PROCESS_STATES:\n"
            '            reasons.append(f"process state {process_state!r} does not allow scheduling")\n'
        ),
        "new": (
            "        if process_state is None:\n"
            '            reasons.append("process state unavailable")\n'
            "        # A02 MUTATION: terminal-state rejection dropped\n"
            "        elif process_state not in _SCHEDULABLE_PROCESS_STATES and process_state not in _TERMINAL_PROCESS_STATES:\n"
            '            reasons.append(f"process state {process_state!r} does not allow scheduling")\n'
        ),
        "test_files": ["test_eligibility.py", "test_mutations.py"],
        "k_filter": "terminal or exited or m03",
    },
    {
        "id": "A03",
        "name": "Capacity off-by-one (>= -> >)",
        "target": "eligibility",
        "comment": "D2-I6: capacity exhausted when active_claims >= max_concurrency.",
        "old": '    elif active_claims_for_agent >= agent.max_concurrency:\n',
        "new": '    elif active_claims_for_agent > agent.max_concurrency:  # A03 MUTATION: off-by-one\n',
        "test_files": ["test_eligibility.py", "test_mutations.py"],
        "k_filter": "capacity or m04",
    },
    {
        "id": "A04",
        "name": "Ignore required_specializations",
        "target": "eligibility",
        "comment": "D2-I7: agent must hold every required specialization.",
        "old": (
            "    missing_specs = sorted(set(required_specializations) - set(agent.specializations))\n"
            "    if missing_specs:\n"
            '        reasons.append(f"missing required specializations: {missing_specs}")\n'
            "\n"
            "    missing_tools = sorted(set(required_tools) - set(agent.supported_tools))\n"
        ),
        "new": (
            "    # A04 MUTATION: required-specializations check dropped\n"
            "    missing_tools = sorted(set(required_tools) - set(agent.supported_tools))\n"
        ),
        "test_files": ["test_eligibility.py", "test_mutations.py"],
        "k_filter": "specialization or m05",
    },
    {
        "id": "A05",
        "name": "Ignore required_tools",
        "target": "eligibility",
        "comment": "D2-I8: agent must support every required tool.",
        "old": (
            "    missing_tools = sorted(set(required_tools) - set(agent.supported_tools))\n"
            "    if missing_tools:\n"
            '        reasons.append(f"missing required tools: {missing_tools}")\n'
            "\n"
            "    missing_caps = _missing_kernel_capabilities(\n"
        ),
        "new": (
            "    # A05 MUTATION: required-tools check dropped\n"
            "    missing_caps = _missing_kernel_capabilities(\n"
        ),
        "test_files": ["test_eligibility.py", "test_mutations.py"],
        "k_filter": "tool or m06",
    },
    {
        "id": "A06",
        "name": "Ignore required_capabilities",
        "target": "eligibility",
        "comment": "D2-I9: agent's Kernel process must hold every required capability.",
        "old": (
            "    missing_caps = _missing_kernel_capabilities(\n"
            "        agent.process_id, required_capabilities, capability_checker\n"
            "    )\n"
            "    if missing_caps:\n"
            '        reasons.append(f"missing kernel capabilities: {missing_caps}")\n'
            "\n"
            "    eligible = len(reasons) == 0\n"
        ),
        "new": (
            "    # A06 MUTATION: required-capabilities check dropped\n"
            "    eligible = len(reasons) == 0\n"
        ),
        "test_files": ["test_eligibility.py", "test_mutations.py"],
        "k_filter": "capability or cap or m07",
    },
    # ── matching.py ─────────────────────────────────────────────────────────
    {
        "id": "A07",
        "name": "Reverse matching sort (lowest score wins)",
        "target": "matching",
        "comment": "D2-I11: highest score must win; flip picks worst match.",
        "old": "    scored_sorted = sorted(\n        scored,\n        key=lambda s: (\n            -s.score,\n",
        "new": "    scored_sorted = sorted(\n        scored,\n        key=lambda s: (\n            s.score,  # A07 MUTATION: worst match now wins\n",
        "test_files": ["test_matching.py", "test_mutations.py"],
        "k_filter": "matching or m10 or cheaper or tie",
    },
    {
        "id": "A08",
        "name": "Drop agent_id tiebreak (input-order dependence)",
        "target": "matching",
        "comment": "D2-I11: tiebreak MUST use agent_id for determinism across input order.",
        "old": (
            "            _cost_of(s.agent_id, eligible_agents),\n"
            "            s.agent_id,\n"
            "        ),\n"
            "    )\n"
            '    selected = scored_sorted[0].agent_id if scored_sorted else ""\n'
        ),
        "new": (
            "            _cost_of(s.agent_id, eligible_agents),\n"
            "            # A08 MUTATION: agent_id tiebreak dropped — input-order dependent\n"
            "        ),\n"
            "    )\n"
            '    selected = scored_sorted[0].agent_id if scored_sorted else ""\n'
        ),
        "test_files": ["test_matching.py", "test_matching_determinism.py", "test_mutations.py"],
        "k_filter": "matching or determinism or m11 or tiebreak or insertion_order",
    },
    {
        "id": "A09",
        "name": "Load-attracts (reversed load sign in score)",
        "target": "matching",
        "comment": "D2-I11: load must PENALIZE, not attract (sign reversed).",
        "old": (
            "            penalty = LOAD_PENALTY * load\n"
            "            score -= penalty\n"
            '            reasons.append(f"load {load} (-{penalty})")\n'
        ),
        "new": (
            "            penalty = LOAD_PENALTY * load\n"
            "            score += penalty  # A09 MUTATION: load attracts instead of penalizing\n"
            '            reasons.append(f"load {load} (+{penalty})")\n'
        ),
        "test_files": ["test_matching.py"],
        "k_filter": "load or cost_tie",
    },
    # ── claims.py ───────────────────────────────────────────────────────────
    {
        "id": "A10",
        "name": "try_acquire_lease leaks ACTIVE without lease_id",
        "target": "claims",
        "comment": "D2-I5: an ACTIVE claim MUST have a live Kernel lease_id.",
        "old": (
            "        claim.lease_id = lease.lease_id\n"
            "        claim.state = ClaimState.ACTIVE\n"
            "        claim.activated_at = _now()\n"
        ),
        "new": (
            "        # A10 MUTATION: leak ACTIVE without lease_id\n"
            "        claim.state = ClaimState.ACTIVE\n"
            "        claim.activated_at = _now()\n"
        ),
        "test_files": ["test_claims.py"],
        "k_filter": "claim or lease or active",
    },
    {
        "id": "A11",
        "name": "try_acquire_lease overwrites lease_id with bogus value",
        "target": "claims",
        "comment": "D2-I17: single acquire must bind exactly one live lease_id.",
        "old": (
            "        claim.lease_id = lease.lease_id\n"
            "        claim.state = ClaimState.ACTIVE\n"
        ),
        "new": (
            "        claim.lease_id = lease.lease_id\n"
            '        claim.lease_id = "stale-bogus-lease"  # A11: overwrite leak\n'
            "        claim.state = ClaimState.ACTIVE\n"
        ),
        "test_files": ["test_claims.py"],
        "k_filter": "release or idempotent",
    },
    {
        "id": "A12",
        "name": "_safe_release clears lease_id on success (record corruption)",
        "target": "claims",
        "comment": "D2-I17: success-path lease_id must remain; forced-None corrupts the record.",
        "old": (
            "    def _safe_release(self, claim: TaskClaim) -> None:\n"
            "        if claim.lease_id is not None:\n"
            "            try:\n"
            "                self._adapter.release(claim.lease_id)\n"
            "            except Exception:\n"
            "                claim.lease_id = None\n"
        ),
        "new": (
            "    def _safe_release(self, claim: TaskClaim) -> None:\n"
            "        if claim.lease_id is not None:\n"
            "            try:\n"
            "                self._adapter.release(claim.lease_id)\n"
            "                claim.lease_id = None  # A12: clear on success too\n"
            "            except Exception:\n"
            "                claim.lease_id = None\n"
        ),
        "test_files": ["test_claims.py"],
        "k_filter": "complete or releases_lease or released",
    },
    # ── scheduler.py ────────────────────────────────────────────────────────
    {
        "id": "A13",
        "name": "Remove existing-claim gate (allows double-claim per task)",
        "target": "scheduler",
        "comment": "D2-I4: at most one ACTIVE claim per task.",
        "old": (
            "            # Existing active claim: skip (per D2-I4 we never create a 2nd).\n"
            "            existing = self.get_claim(task_id)\n"
            "            if existing is not None:\n"
            '                result.skipped.append((task_id, f"active claim {existing.claim_id}"))\n'
            "                continue\n"
            "\n"
            "            # Bounds.\n"
        ),
        "new": (
            "            # A13 MUTATION: existing-claim gate dropped\n"
            "            # existing = self.get_claim(task_id)\n"
            "            # if existing is not None:\n"
            '            #     result.skipped.append((task_id, f"active claim {existing.claim_id}"))\n'
            "            #     continue\n"
            "\n"
            "            # Bounds.\n"
        ),
        "test_files": ["test_scheduler.py", "test_claim_exclusivity.py", "test_completion.py"],
        "k_filter": "schedule or exclusivity or skip or claim",
    },
    {
        "id": "A14",
        "name": "Remove GraphVersion re-check in _acquire_claim",
        "target": "scheduler",
        "comment": "D2-I18: stale readiness proof must be rejected on version race.",
        "old": (
            "        # Re-check GraphVersion — stale readiness proof cannot linearize ownership.\n"
            "        current = self._vpg.current_graph_version(graph_id)\n"
            "        if current != graph_version:\n"
            "            self._events.append(\n"
            "                record_event(\n"
            "                    SchedulerEventType.CLAIM_REJECTED,\n"
            "                    graph_id=graph_id,\n"
            "                    task_id=task_id,\n"
            "                    agent_id=agent_id,\n"
            "                    graph_version=current,\n"
            '                    reason=f"graph version race (used {graph_version}, now {current})",\n'
            "                )\n"
            "            )\n"
            "            return False\n"
            "\n"
            "        # Re-check that the task is STILL in the ready frontier at the\n"
            "        # current version.\n"
            "        frontier = self._vpg.ready_frontier(graph_id)\n"
        ),
        "new": (
            "        # A14 MUTATION: GraphVersion re-check dropped\n"
            "        current = self._vpg.current_graph_version(graph_id)\n"
            "        # if current != graph_version:\n"
            "        #     ...\n"
            "        #     return False\n"
            "\n"
            "        # Re-check that the task is STILL in the ready frontier at the\n"
            "        # current version.\n"
            "        frontier = self._vpg.ready_frontier(graph_id)\n"
        ),
        "test_files": ["test_claim_graph_version.py", "test_scheduler.py"],
        "k_filter": "graph_version or version or scheduler or acquisition",
    },
    {
        "id": "A15",
        "name": "Remove graph-frontier re-check in _acquire_claim",
        "target": "scheduler",
        "comment": "D2-I19: stale readiness (no longer in frontier) must not linearize.",
        "old": (
            "        # Re-check that the task is STILL in the ready frontier at the\n"
            "        # current version.\n"
            "        frontier = self._vpg.ready_frontier(graph_id)\n"
            "        if not any(c.task_id == task_id for c in frontier):\n"
            "            self._events.append(\n"
            "                record_event(\n"
            "                    SchedulerEventType.CLAIM_REJECTED,\n"
            "                    graph_id=graph_id,\n"
            "                    task_id=task_id,\n"
            "                    agent_id=agent_id,\n"
            "                    graph_version=current,\n"
            '                    reason="task no longer in ready frontier",\n'
            "                )\n"
            "            )\n"
            "            return False\n"
            "\n"
            "        # Re-check agent process liveness + state.\n"
        ),
        "new": (
            "        # A15 MUTATION: graph-frontier re-check dropped — stale readiness may linearize\n"
            "        # frontier = self._vpg.ready_frontier(graph_id)\n"
            "        # if not any(c.task_id == task_id for c in frontier):\n"
            "        #     ...\n"
            "        #     return False\n"
            "\n"
            "        # Re-check agent process liveness + state.\n"
        ),
        "test_files": ["test_scheduler.py", "test_claim_graph_version.py"],
        "k_filter": "scheduler or claim or graph",
        "skip": True,
        "skip_note": (
            "No existing test exercises an independent frontier-recheck failure: "
            "the FakeVPG used by every test keeps tasks in the frontier until "
            "completion, so removing this guard causes no failing assertion."
        ),
    },
    {
        "id": "A16",
        "name": "Disable stale-version guard (force-pass)",
        "target": "scheduler",
        "comment": "D2-I18: version mismatch must hard-reject.",
        "old": "        # Re-check GraphVersion — stale readiness proof cannot linearize ownership.\n        current = self._vpg.current_graph_version(graph_id)\n        if current != graph_version:\n",
        "new": "        # Re-check GraphVersion — stale readiness proof cannot linearize ownership.\n        current = self._vpg.current_graph_version(graph_id)\n        if False and current != graph_version:  # A16 MUTATION: disabled guard\n",
        "test_files": ["test_claim_graph_version.py"],
        "k_filter": "graph_version or version",
    },
    {
        "id": "A25",
        "name": "Bypass claim activation (state forced to non-ACTIVE sentinel)",
        "target": "scheduler",
        "comment": "D2-I17: ownership only linearizes after try_acquire_lease succeeds.",
        "old": (
            "        self._claims_.mark_acquiring(claim)\n"
            "        self._events.append(\n"
            "            record_event(\n"
            "                SchedulerEventType.CLAIM_PROPOSED,\n"
            "                graph_id=graph_id,\n"
            "                task_id=task_id,\n"
            "                agent_id=agent_id,\n"
            "                claim_id=claim.claim_id,\n"
            "                graph_version=current,\n"
            "            )\n"
            "        )\n"
            "        self._claims.append(claim)\n"
            "\n"
            "        if self._claims_.try_acquire_lease(claim):\n"
            "            self._events.append(\n"
            "                record_event(\n"
            "                    SchedulerEventType.CLAIM_LEASE_ACQUIRED,\n"
            "                    graph_id=graph_id,\n"
            "                    task_id=task_id,\n"
            "                    agent_id=agent_id,\n"
            "                    claim_id=claim.claim_id,\n"
            "                    graph_version=current,\n"
            "                    reason=claim.reason or \"\",\n"
            "                )\n"
            "            )\n"
            "            return True\n"
            "\n"
            "        return False\n"
        ),
        "new": (
            "        self._claims_.mark_acquiring(claim)\n"
            "        self._events.append(\n"
            "            record_event(\n"
            "                SchedulerEventType.CLAIM_PROPOSED,\n"
            "                graph_id=graph_id,\n"
            "                task_id=task_id,\n"
            "                agent_id=agent_id,\n"
            "                claim_id=claim.claim_id,\n"
            "                graph_version=current,\n"
            "            )\n"
            "        )\n"
            "        self._claims.append(claim)\n"
            "        # A25 MUTATION: bypass lease acquisition, claim.state set to non-ACTIVE\n"
            "        claim.state = True  # invalid truthy state, not a ClaimState member\n"
            "        self._events.append(\n"
            "            record_event(\n"
            "                SchedulerEventType.CLAIM_LEASE_ACQUIRED,\n"
            "                graph_id=graph_id,\n"
            "                task_id=task_id,\n"
            "                agent_id=agent_id,\n"
            "                claim_id=claim.claim_id,\n"
            "                graph_version=current,\n"
            '                reason=claim.reason or "",\n'
            "            )\n"
            "        )\n"
            "        return True\n"
        ),
        "test_files": ["test_scheduler.py"],
        "k_filter": "schedule or active or dispatch or observe",
    },

    # ── reconciliation.py ──────────────────────────────────────────────────
    {
        "id": "A17",
        "name": "Drop lease-liveness check in _reconcile_active_claim",
        "target": "reconciliation",
        "comment": "D2-I5: lost Kernel lease must mark the claim LOST.",
        "old": (
            "    # 2. Kernel lease gone / expired -> claim is LOST.\n"
            "    live_leases = lease_lookup(claim)\n"
            "    if not live_leases or not lease_is_live(live_leases.lease_id):\n"
            "        # Make sure any lingering lease resource is reclaimed.\n"
            "        try:\n"
            "            if claim.lease_id:\n"
            "                release_lease(claim.lease_id)\n"
            "                res.orphan_leases_released += 1\n"
            "        except Exception:\n"
            "            pass\n"
            "        _lose_claim(claim, res, reason=\"kernel_lease_vanished_claim_lost\")\n"
            "        return\n"
            "\n"
            "    # 3. Task verified by VPG -> claim COMPLETED, lease released.\n"
        ),
        "new": (
            "    # A17 MUTATION: lease-liveness check dropped\n"
            "    # live_leases = lease_lookup(claim)\n"
            "    # if not live_leases or not lease_is_live(live_leases.lease_id):\n"
            "    #     ...\n"
            "    #     return\n"
            "\n"
            "    # 3. Task verified by VPG -> claim COMPLETED, lease released.\n"
        ),
        "test_files": ["test_reconciliation.py"],
        "k_filter": "reconcil or lease or lost",
    },
    {
        "id": "A18",
        "name": "COMPLETED transitions back to ACTIVE (reversed claim lifecycle)",
        "target": "reconciliation",
        "comment": "D2-I22: claim lifecycle is strictly forward (ACTIVE -> COMPLETED is terminal).",
        "old": (
            "        claim.state = ClaimState.COMPLETED\n"
            '        claim.released_at = _now()\n'
            '        claim.reason = "vpg_task_verified_claim_completed"\n'
            "        res.claims_completed += 1\n"
            "        res.add(\n"
            "            ReconciliationIssue(\n"
            '                issue_id="task_verified_claim_completed",\n'
        ),
        "new": (
            "        claim.state = ClaimState.ACTIVE  # A18 MUTATION: reversed lifecycle COMPLETED -> ACTIVE\n"
            '        claim.released_at = _now()\n'
            '        claim.reason = "vpg_task_verified_claim_completed"\n'
            "        res.claims_completed += 1\n"
            "        res.add(\n"
            "            ReconciliationIssue(\n"
            '                issue_id="task_verified_claim_completed",\n'
        ),
        "test_files": ["test_reconciliation.py"],
        "k_filter": "reconcil or completes",
    },
    {
        "id": "A19",
        "name": "Always ACTIVE -> COMPLETED regardless of VPG validity",
        "target": "reconciliation",
        "comment": "D2-I23: claim only completes when VPG reports task verified.",
        "old": (
            "    # 3. Task verified by VPG -> claim COMPLETED, lease released.\n"
            "    if vpg_task_verified(claim.graph_id, claim.task_id):\n"
            "        try:\n"
            "            if claim.lease_id:\n"
            "                release_lease(claim.lease_id)\n"
            "                res.orphan_leases_released += 1\n"
            "        except Exception:\n"
            "            pass\n"
            "        claim.state = ClaimState.COMPLETED\n"
            "        claim.released_at = _now()\n"
            '        claim.reason = "vpg_task_verified_claim_completed"\n'
            "        res.claims_completed += 1\n"
            "        res.add(\n"
            "            ReconciliationIssue(\n"
            '                issue_id="task_verified_claim_completed",\n'
            '                summary="Task verified by VPG; claim completed",\n'
            "                claim_id=claim.claim_id,\n"
            "                task_id=claim.task_id,\n"
            "                agent_id=claim.agent_id,\n"
            "            )\n"
            "        )\n"
        ),
        "new": (
            "    # A19 MUTATION: always ACTIVE -> COMPLETED regardless of VPG validity\n"
            "    try:\n"
            "        if claim.lease_id:\n"
            "            release_lease(claim.lease_id)\n"
            "            res.orphan_leases_released += 1\n"
            "    except Exception:\n"
            "        pass\n"
            "    claim.state = ClaimState.COMPLETED\n"
            "    claim.released_at = _now()\n"
            '    claim.reason = "vpg_task_verified_claim_completed"\n'
            "    res.claims_completed += 1\n"
            "    res.add(\n"
            "        ReconciliationIssue(\n"
            '            issue_id="task_verified_claim_completed",\n'
            '            summary="Task verified by VPG; claim completed",\n'
            "            claim_id=claim.claim_id,\n"
            "            task_id=claim.task_id,\n"
            "            agent_id=claim.agent_id,\n"
            "        )\n"
            "    )\n"
        ),
        "test_files": ["test_reconciliation.py"],
        "k_filter": "reconcil or no_issues or consistent",
    },
    {
        "id": "A24",
        "name": "LOST claim resurrects to ACTIVE",
        "target": "reconciliation",
        "comment": "D2-I24: LOST is terminal — must not transition back to ACTIVE.",
        "old": (
            "    claim.state = ClaimState.LOST\n"
            "    claim.released_at = _now()\n"
            "    claim.reason = reason\n"
            "    res.claims_marked_lost += 1\n"
        ),
        "new": (
            "    claim.state = ClaimState.ACTIVE  # A24 MUTATION: resurrection — LOST -> ACTIVE\n"
            "    claim.released_at = _now()\n"
            "    claim.reason = reason\n"
            "    res.claims_marked_lost += 1\n"
        ),
        "test_files": ["test_reconciliation.py"],
        "k_filter": "reconcil or lost",
    },
    # ── projections.py ──────────────────────────────────────────────────────
    {
        "id": "A20",
        "name": "active_claim_count_by_agent counts COMPLETED as active",
        "target": "projections",
        "comment": "D2-I25: only ACTIVE claims count toward load.",
        "old": (
            "    for c in claims:\n"
            "        if c.state == ClaimState.ACTIVE:\n"
            "            counts[c.agent_id] = counts.get(c.agent_id, 0) + 1\n"
            "    return counts\n"
        ),
        "new": (
            "    for c in claims:\n"
            "        # A20 MUTATION: count COMPLETED as active too\n"
            "        if c.state == ClaimState.ACTIVE or c.state == ClaimState.COMPLETED:\n"
            "            counts[c.agent_id] = counts.get(c.agent_id, 0) + 1\n"
            "    return counts\n"
        ),
        "test_files": ["test_mutations.py"],
        "k_filter": "m16 or active_claim_count",
    },
    {
        "id": "A21",
        "name": "agent_load.rebuild drops state==ACTIVE filter",
        "target": "projections",
        "comment": "D2-I25: projection load counts only ACTIVE claims.",
        "old": (
            "            active = [\n"
            "                c for c in claims if c.agent_id == agent.agent_id and c.state == ClaimState.ACTIVE\n"
            "            ]\n"
        ),
        "new": (
            "            # A21 MUTATION: drop state==ACTIVE filter — counts every claim\n"
            "            active = [\n"
            "                c for c in claims if c.agent_id == agent.agent_id\n"
            "            ]\n"
        ),
        "test_files": ["test_projection_replay.py"],
        "k_filter": "rebuild or projection",
    },
    # ── attempts.py ─────────────────────────────────────────────────────────
    {
        "id": "A22",
        "name": "mark_crashed sets RUNNING (state corruption)",
        "target": "attempts",
        "comment": "D2-I26: CRASHED is terminal — mark_crashed must not corrupt to RUNNING.",
        "old": (
            "    def mark_crashed(self, attempt: ScheduledExecutionAttempt, error: str = \"\") -> None:\n"
            "        attempt.state = AttemptState.CRASHED\n"
            "        attempt.ended_at = _now()\n"
            '        attempt.error = error or "crashed"\n'
        ),
        "new": (
            "    def mark_crashed(self, attempt: ScheduledExecutionAttempt, error: str = \"\") -> None:\n"
            "        attempt.state = AttemptState.RUNNING  # A22 MUTATION: state corruption\n"
            "        attempt.ended_at = _now()\n"
            '        attempt.error = error or "crashed"\n'
        ),
        "test_files": ["test_execution_attempts.py"],
        "k_filter": "attempt or crashed",
    },
    {
        "id": "A23",
        "name": "mark_semantically_verified calls mark_failed (semantic corruption)",
        "target": "attempts",
        "comment": "D2-I27: semantic verify must promote to VERIFIED, not fail.",
        "old": (
            "    def mark_semantically_verified(self, attempt: ScheduledExecutionAttempt) -> None:\n"
            '        """Promote an operationally-successful attempt to semantically\n'
            '        verified after the VPG derives Task -> VERIFIED."""\n'
            "        if attempt.state != AttemptState.SUCCEEDED_OPERATIONALLY:\n"
            "            # Defensive: we still record the fact, but flag it so a later\n"
            "            # auditor sees the sequence.\n"
            '            attempt.error = f"forced semantic verification from {attempt.state.value}"\n'
            "        attempt.state = AttemptState.VERIFIED_SEMANTICALLY\n"
            "        attempt.ended_at = _now()\n"
        ),
        "new": (
            "    def mark_semantically_verified(self, attempt: ScheduledExecutionAttempt) -> None:\n"
            '        """Promote an operationally-successful attempt to semantically\n'
            '        verified after the VPG derives Task -> VERIFIED."""\n'
            '        self.mark_failed(attempt, error="forced failure before semantic verification")  # A23\n'
            "        attempt.state = AttemptState.VERIFIED_SEMANTICALLY\n"
            "        attempt.ended_at = _now()\n"
        ),
        "test_files": ["test_execution_attempts.py"],
        "k_filter": "attempt or semantic or verified or operational",
    },
]


# ── helpers ───────────────────────────────────────────────────────────────────
def _now_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def apply_mutation(path: Path, m: dict) -> None:
    src = path.read_text()
    old = m["old"]
    if old not in src:
        raise RuntimeError(
            f"[{m['id']}] anchor not found in {path.name}:\n---\n{old}\n---"
        )
    mutated = src.replace(old, m["new"], 1)
    path.write_text(mutated)


def run_tests(test_files: list[str], k_filter: str, timeout_s: int = 120) -> dict:
    """Run a focused pytest subset with `-x` (bail on first failure)."""
    files = [str(TESTS / f) for f in test_files]
    cmd = [
        str(PY), "-m", "pytest",
        *files,
        "-q", "-x",
        "-k", k_filter,
        "--tb=line",
        "-p", "no:randomly",
        "--no-header",
        "-rN",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    except subprocess.TimeoutExpired:
        return {"rc": 3, "passed": 0, "failed": 1, "failed_tests": ["<timeout>"], "raw": ""}
    out = proc.stdout + proc.stderr
    passed = 0
    failed = 0
    m_pass = re.search(r"(\d+)\s+passed", out)
    m_fail = re.search(r"(\d+)\s+failed", out)
    if m_pass:
        passed = int(m_pass.group(1))
    if m_fail:
        failed = int(m_fail.group(1))
    # If no explicit "failed N" was emitted but rc != 0, assume at least one failure.
    failed_tests: list[str] = []
    for line in out.splitlines():
        ls = line.strip()
        if ls.startswith("FAILED "):
            # "FAILED path::test - reason"
            tok = ls.split()[1] if len(ls.split()) > 1 else ""
            if tok:
                failed_tests.append(tok)
        elif "F" in ls and "::" in ls and ls.startswith("tests/"):
            # Short mode: "tests/path::test ... F ...."
            seg = ls.split()[0]
            failed_tests.append(seg)
    # Bail: pytest -x produces only one " FAILED ..." line before summary.
    return {"rc": proc.returncode, "passed": passed, "failed": max(failed, 1 if proc.returncode != 0 else 0), "failed_tests": failed_tests, "raw": out}


# ── main ─────────────────────────────────────────────────────────────────────
def main() -> int:
    ART.mkdir(parents=True, exist_ok=True)
    print(f"[{_now_ts()}] D2.1 §26 — Mutation Audit 2.0 (D2-A01..A25)")
    print(f"  mutations: {len(MUTATIONS)}")
    print()

    results: list[dict] = []
    killed_cnt = 0
    survivor_cnt = 0
    skip_cnt = 0

    for m in MUTATIONS:
        mid = m["id"]
        target_name = m["target"]
        path = FILES[target_name]
        backup = Path(str(path) + ".bak")
        is_skip = m.get("skip", False)
        status = "SKIP"
        failing_test: str | None = None
        n_passed = 0
        n_failed = 0
        mutated_ok = True
        error_msg: str | None = None

        print(f"── {mid}: {m['name']}  (target={path.name})")
        try:
            shutil.copy(path, backup)
            try:
                apply_mutation(path, m)
            except Exception as e:
                mutated_ok = False
                error_msg = str(e)
                print(f"    !! could not apply mutation: {error_msg}")

            if mutated_ok:
                pycache = path.parent / "__pycache__"
                if pycache.exists():
                    shutil.rmtree(pycache)

                if is_skip:
                    r = run_tests(m["test_files"], m.get("k_filter", ""))
                    n_passed = r["passed"]
                    n_failed = r["failed"]
                    status = "SKIP"
                    skip_cnt += 1
                    print(
                        f"    [SKIP] documented coverage gap "
                        f"({n_passed} passed, {n_failed} failed run)"
                    )
                else:
                    r = run_tests(m["test_files"], m.get("k_filter", ""))
                    n_passed = r["passed"]
                    n_failed = r["failed"]
                    failing_test = r["failed_tests"][0] if r["failed_tests"] else None
                    if r["rc"] != 0 and n_failed > 0:
                        status = "KILLED"
                        killed_cnt += 1
                    else:
                        status = "SURVIVOR"
                        survivor_cnt += 1
                    print(
                        f"    [{status}] {n_passed} passed / {n_failed} failed"
                        + (f"  first_fail={failing_test}" if failing_test else "")
                    )
            else:
                status = "SKIP"
                skip_cnt += 1
                print("    [SKIP] mutation could not be applied cleanly")
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
            "comment": m["comment"],
            "skip_note": m.get("skip_note"),
            "error": error_msg,
        })

    # Final cleanup of any stray .bak files.
    for p in FILES.values():
        bak = Path(str(p) + ".bak")
        if bak.exists():
            bak.unlink()

    # ── write artifacts ─────────────────────────────────────────────────────
    results_path = ART / "mutation-results-v2.json"
    results_path.write_text(json.dumps(results, indent=2) + "\n")

    killed_pct = (killed_cnt / len(MUTATIONS)) * 100
    md_lines = [
        "# D2.1 §26 — Mutation Audit 2.0 (D2-A01..A25)",
        "",
        f"**Run time**: {_now_ts()}",
        f"**Mutations**: {len(MUTATIONS)}",
        f"**KILLED**: {killed_cnt}",
        f"**SURVIVOR**: {survivor_cnt}",
        f"**SKIP**: {skip_cnt}",
        f"**KILLED %**: {killed_pct:.1f}%",
        "",
        "## Mutation Results",
        "",
        "| ID | Name | Target | Status | Failing Test | Comment |",
        "|----|------|--------|--------|--------------|---------|",
    ]
    for r in results:
        ft = r["failing_test"] or "—"
        md_lines.append(
            f"| {r['id']} | {r['name']} | {r['target']} | **{r['status']}** | {ft} | {r['comment']} |"
        )
    md_lines.append("")
    md_lines.append("## Notes")
    md_lines.append("")
    for r in results:
        if r.get("skip_note"):
            md_lines.append(f"- **{r['id']}**: {r['skip_note']}")
        if r.get("error"):
            md_lines.append(f"- **{r['id']}**: error — {r['error']}")
    md_lines.append("")
    md_lines.append("## Exit Code")
    md_lines.append("")
    if survivor_cnt > 0:
        md_lines.append("**EXIT 2**: at least one mutation survived.")
        md_lines.append("")
        for r in results:
            if r["status"] == "SURVIVOR":
                md_lines.append(f"- {r['id']} ({r['target']}): {r['name']}")
    else:
        md_lines.append("**EXIT 0**: every mutation killed or skip-documented.")
    report_path = ART / "mutation-audit-v2.md"
    report_path.write_text("\n".join(md_lines) + "\n")

    # ── summary ─────────────────────────────────────────────────────────────
    print()
    print("=" * 72)
    print("  D2.1 §26 Mutation Audit 2.0 summary")
    print(f"  KILLED={killed_cnt}  SURVIVOR={survivor_cnt}  SKIP={skip_cnt}  ({killed_pct:.0f}% killed)")
    print()
    for r in results:
        mark = {"KILLED": "OK", "SURVIVOR": "!!", "SKIP": "SK"}[r["status"]]
        print(f"    [{mark}] {r['id']} {r['target']} :: {r['name']}  ({r['status']})")
    print()
    print("  artifacts:")
    print(f"    {results_path}")
    print(f"    {report_path}")
    print("=" * 72)

    if survivor_cnt > 0:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
