"""Tests for the minimal_lhos runtime mode (Milestone 2.3)."""

from __future__ import annotations

from lhos.benchmarks.capability_manifest import build_manifest
from lhos.benchmarks.modes import MODES, mode_config


class TestMinimalLhosModeConfig:
    def test_minimal_lhos_is_registered(self):
        assert "minimal_lhos" in MODES

    def test_minimal_lhos_uses_graph_engine(self):
        mc = mode_config("minimal_lhos")
        assert mc.engine == "graph"

    def test_minimal_lhos_uses_fifo_scheduler(self):
        mc = mode_config("minimal_lhos")
        assert mc.scheduler == "fifo"
        assert mc.scheduler_family == "fifo"

    def test_minimal_lhos_has_no_mandatory_design_node(self):
        """Minimal LHoS must NOT have a mandatory design/planner node."""
        mc = mode_config("minimal_lhos")
        planner = mc.config.get("planner", {})
        assert planner.get("max_nodes", 999) <= 4
        # No "design" node requirement
        features = mc.config.get("features", {})
        assert features.get("invalidation") is False  # No invalidation cascade

    def test_minimal_lhos_uses_graph_scoped_context(self):
        """Context must be graph-scoped (limited dependency hops)."""
        mc = mode_config("minimal_lhos")
        context = mc.config.get("context", {})
        assert context.get("max_tokens", 99999) <= 8000
        assert context.get("max_dependency_hops", 999) <= 1
        assert context.get("include_last_failures", 999) <= 1

    def test_minimal_lhos_uses_verification_feedback(self):
        """Must have verification gate enabled."""
        mode_config("minimal_lhos")
        manifest = build_manifest("minimal_lhos")
        assert manifest.uses_real_verifier is True
        assert manifest.can_local_repair is True

    def test_minimal_lhos_does_not_call_reconciler_initially(self):
        """Reconciler must be disabled by default."""
        mc = mode_config("minimal_lhos")
        reconciler = mc.config.get("reconciler", {})
        assert reconciler.get("enabled") is False
        assert reconciler.get("trigger_threshold", 0) >= 2

    def test_minimal_lhos_costs_all_runtime_calls(self):
        """All runtime LLM calls must be cost-tracked."""
        manifest = build_manifest("minimal_lhos")
        assert manifest.tracks_token_cost is True
        assert manifest.tracks_tool_calls is True
        assert manifest.tracks_time_cost is True

    def test_minimal_lhos_has_no_filesystem_checkpoint(self):
        """Minimal mode uses noop checkpoint (no filesystem overhead)."""
        mc = mode_config("minimal_lhos")
        checkpoint = mc.config.get("checkpoint", {})
        assert checkpoint.get("type") == "noop"

    def test_minimal_lhos_does_not_use_cost_aware_scheduler(self):
        """Minimal mode must NOT use cost-aware scheduler."""
        manifest = build_manifest("minimal_lhos")
        assert manifest.can_use_cost_aware_scheduler is False

    def test_minimal_lhos_has_crash_recovery(self):
        """Minimal mode still has crash recovery via event log + graph store."""
        manifest = build_manifest("minimal_lhos")
        assert manifest.can_crash_recover is True

    def test_minimal_lhos_capability_manifest_complete(self):
        """Manifest must list all capability differences."""
        manifest = build_manifest("minimal_lhos")
        full_manifest = build_manifest("full_lhos")

        # Minimal has LESS than Full
        assert manifest.can_use_cost_aware_scheduler is False
        assert full_manifest.can_use_cost_aware_scheduler is True
        assert manifest.can_checkpoint is False
        assert full_manifest.can_checkpoint is True

        # Both have verification
        assert manifest.uses_real_verifier is True
        assert full_manifest.uses_real_verifier is True

        # Both track costs
        assert manifest.tracks_token_cost is True
        assert full_manifest.tracks_token_cost is True
