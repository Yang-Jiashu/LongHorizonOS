"""Capability manifest for benchmark modes (audit Milestone 1H).

Explicitly declares what each benchmark mode is allowed to access and
what capabilities it has. This serves as the fairness contract between
modes — ensuring no mode has unfair advantages or information leakage.

Architecture invariants enforced by tests:
- Non-oracle modes must NOT access oracle priorities, critical path, or
  affected sets during execution.
- The transcript mode must NOT use the graph runtime (no event log, no
  reconciliation, no checkpoints).
- All modes must use the same verifier registry.
- All modes must record token, tool-call, and time costs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lhos.benchmarks.modes import MODES, ModeConfig, mode_config


@dataclass(frozen=True)
class CapabilityManifest:
    """Declares what a benchmark mode is allowed to access and do.

    This is the fairness contract: every mode's capabilities are explicitly
    listed, and tests verify that no mode exceeds its declared capabilities.
    """

    mode: str
    engine: str  # "transcript" | "graph"
    scheduler: str  # "fifo" | "cost_aware" | "oracle_fifo" | "oracle_cost_aware"

    # ── Information access ──────────────────────────────────────────────
    can_access_graph_structure: bool
    """Can see the task graph (nodes, edges, dependencies). The transcript
    mode sees the graph for topological ordering but does not use the graph
    runtime."""

    can_access_oracle_priorities: bool
    """Can see oracle priority hints (node.priority > 0). Only oracle modes
    may access these."""

    can_access_oracle_critical_path: bool
    """Can see the true critical path. NO mode sees this during execution;
    it's only used in scoring/analysis."""

    can_access_oracle_affected_sets: bool
    """Can see true affected node sets per environment event. NO mode sees
    these during execution; only used in scoring."""

    # ── Runtime capabilities ────────────────────────────────────────────
    can_reconcile_invalidation: bool
    """Can reconcile environment events (invalidation propagation)."""

    can_local_repair: bool
    """Can replan invalidated nodes (local repair)."""

    can_checkpoint: bool
    """Can create and restore filesystem checkpoints."""

    can_crash_recover: bool
    """Can recover from crashes via persistent state (event log + graph store)."""

    can_use_cost_aware_scheduler: bool
    """Uses a cost-aware scheduler (vs. simple FIFO)."""

    # ── Cost accounting ─────────────────────────────────────────────────
    tracks_token_cost: bool
    """Records token usage (input + output)."""

    tracks_tool_calls: bool
    """Records tool call counts."""

    tracks_time_cost: bool
    """Records execution time (simulated or wall-clock)."""

    # ── Verification ────────────────────────────────────────────────────
    uses_real_verifier: bool
    """Uses the real verifier registry (same verifiers as production)."""

    # ── Documentation ───────────────────────────────────────────────────
    notes: str = ""
    """Human-readable notes about the mode's design intent."""


def build_manifest(mode_name: str, artifacts_dir: str = "artifacts") -> CapabilityManifest:
    """Build the capability manifest for a given mode."""
    mc: ModeConfig = mode_config(mode_name, artifacts_dir=artifacts_dir)
    cfg = mc.config

    features = cfg.get("features", {})
    checkpoint = cfg.get("checkpoint", {})
    has_checkpoint = bool(checkpoint.get("type", "noop") != "noop")
    restore_on_crash = bool(checkpoint.get("restore_on_crash", False))

    is_transcript = mc.engine == "transcript"
    is_oracle = mc.use_oracle_priorities

    return CapabilityManifest(
        mode=mode_name,
        engine=mc.engine,
        scheduler=mc.scheduler,
        # Information access
        can_access_graph_structure=True,  # All modes see the task graph
        can_access_oracle_priorities=is_oracle,
        can_access_oracle_critical_path=False,  # Never during execution
        can_access_oracle_affected_sets=False,  # Never during execution
        # Runtime capabilities
        can_reconcile_invalidation=not is_transcript and features.get("invalidation", True),
        can_local_repair=not is_transcript and features.get("local_repair", True),
        can_checkpoint=has_checkpoint,
        can_crash_recover=not is_transcript,  # Graph modes persist state
        can_use_cost_aware_scheduler=mc.scheduler in ("cost_aware", "oracle_cost_aware"),
        # Cost accounting
        tracks_token_cost=True,
        tracks_tool_calls=True,
        tracks_time_cost=True,
        # Verification
        uses_real_verifier=True,
        # Notes
        notes=_mode_notes(mode_name, mc),
    )


def all_manifests() -> list[CapabilityManifest]:
    """Build manifests for all 8 modes."""
    return [build_manifest(m) for m in MODES]


def manifest_summary() -> list[dict[str, Any]]:
    """Return a list of dicts suitable for JSON serialization / tabulation."""
    return [
        {
            "mode": m.mode,
            "engine": m.engine,
            "scheduler": m.scheduler,
            "graph_structure": m.can_access_graph_structure,
            "oracle_priorities": m.can_access_oracle_priorities,
            "oracle_critical_path": m.can_access_oracle_critical_path,
            "oracle_affected_sets": m.can_access_oracle_affected_sets,
            "invalidation": m.can_reconcile_invalidation,
            "local_repair": m.can_local_repair,
            "checkpoint": m.can_checkpoint,
            "crash_recover": m.can_crash_recover,
            "cost_aware": m.can_use_cost_aware_scheduler,
            "tracks_tokens": m.tracks_token_cost,
            "tracks_tools": m.tracks_tool_calls,
            "tracks_time": m.tracks_time_cost,
            "real_verifier": m.uses_real_verifier,
        }
        for m in all_manifests()
    ]


def _mode_notes(mode: str, mc: ModeConfig) -> str:
    if mode == "transcript":
        return (
            "No graph runtime. Executes in topological order with a growing "
            "transcript. Crashes discard all progress. Token cost modeled as "
            "len(context)//4. Tool calls are direct filesystem writes."
        )
    if mode == "static_graph_fifo":
        return "Graph built once. Environment events logged but never reconciled."
    if mode == "dynamic_graph_fifo":
        return "Invalidation on, local repair off. Must-invalidate strands the run."
    if mode == "dynamic_graph_local_repair":
        return "Invalidation + local replan. FIFO scheduler."
    if mode == "dynamic_graph_cost_aware":
        return "Invalidation + local replan + cost-aware scheduler."
    if mode == "full_lhos":
        return "Full system: cost-aware + repair + filesystem checkpoints + JSONL trace."
    if mode == "oracle_graph_fifo":
        return "Oracle priorities + FIFO tie-break. Upper bound for FIFO family."
    if mode == "oracle_graph_cost_aware":
        return "Oracle priorities + cost-aware scoring. Upper bound for cost-aware family."
    return ""
