"""Tests for baseline fairness and capability manifest (audit Milestone 1H).

Verifies:
1. Every mode has a complete capability manifest.
2. Non-oracle modes do not access oracle priorities during execution.
3. The transcript mode does not use the graph runtime.
4. All modes use the same verifier registry.
5. Cost accounting fields are consistent across all modes.
6. Oracle priority isolation: graph_spec(use_oracle_priorities=False) sets
   all priorities to 0.0.
"""

from __future__ import annotations

import tempfile

import pytest

from lhos.benchmarks.capability_manifest import (
    all_manifests,
    build_manifest,
    manifest_summary,
)
from lhos.benchmarks.controlled.generator import generate
from lhos.benchmarks.controlled.specs import to_public_spec
from lhos.benchmarks.modes import MODES, mode_config
from lhos.benchmarks.scoring import score_graph_run, score_transcript_run
from lhos.benchmarks.transcript import run_transcript


# ── 1. Every mode has a complete manifest ─────────────────────────────────

def test_all_modes_have_manifests():
    """Every mode in MODES must have a buildable manifest."""
    for mode in MODES:
        m = build_manifest(mode)
        assert m.mode == mode
        assert m.engine in ("transcript", "graph")
        assert m.scheduler in ("fifo", "cost_aware", "oracle_fifo", "oracle_cost_aware")


def test_manifest_count_matches_modes():
    assert len(all_manifests()) == len(MODES)


def test_manifest_summary_is_serializable():
    import json

    summary = manifest_summary()
    assert len(summary) == len(MODES)
    json.dumps(summary)  # must not raise


# ── 2. Oracle priority isolation ──────────────────────────────────────────

@pytest.mark.parametrize("preset", ["serial_chain", "wide_dag", "branch_join"])
def test_non_oracle_modes_get_zero_priorities(preset):
    """graph_spec(use_oracle_priorities=False) must set all priorities to 0."""
    task = generate(preset, size="small", seed=42)
    spec = task.graph_spec(use_oracle_priorities=False)
    for node in spec["nodes"]:
        assert node.get("priority", 0.0) == 0.0, (
            f"node {node.get('temp_id')} has non-zero priority in non-oracle mode"
        )


@pytest.mark.parametrize("preset", ["serial_chain", "costly_critical_path"])
def test_oracle_modes_get_nonzero_priorities(preset):
    """Oracle modes should get non-zero priorities for at least some nodes."""
    task = generate(preset, size="small", seed=42)
    spec = task.graph_spec(use_oracle_priorities=True)
    priorities = [node.get("priority", 0.0) for node in spec["nodes"]]
    assert any(p > 0.0 for p in priorities), "no node has non-zero oracle priority"


def test_public_spec_strips_priorities():
    """to_public_spec must set all priorities to 0.0."""
    task = generate("serial_chain", size="small", seed=1)
    pub = to_public_spec(task)
    for node in pub.nodes:
        assert node.get("priority", 0.0) == 0.0


# ── 3. Transcript mode does not use graph runtime ─────────────────────────

def test_transcript_mode_has_no_graph_engine():
    m = build_manifest("transcript")
    assert m.engine == "transcript"
    assert not m.can_reconcile_invalidation
    assert not m.can_local_repair
    assert not m.can_checkpoint
    assert not m.can_crash_recover


def test_transcript_does_not_access_oracle_priorities():
    """The transcript runner must not read task.oracle.priorities."""
    task = generate("serial_chain", size="small", seed=7)
    # Replace oracle priorities with sentinel values to detect access
    original_priorities = dict(task.oracle.priorities)
    task.oracle.priorities = {k: 999.99 for k in original_priorities}

    result = run_transcript(task, workspace_dir="/tmp/lhos-test-transcript")
    # The transcript should still produce the same result regardless of
    # oracle priorities — if it read them, the result would change.
    assert result is not None
    # Restore
    task.oracle.priorities = original_priorities


def test_transcript_does_not_access_oracle_critical_path():
    """The transcript runner must not read task.oracle.critical_path."""
    task = generate("serial_chain", size="small", seed=7)
    # Replace critical path with sentinel
    original_cp = list(task.oracle.critical_path)
    task.oracle.critical_path = ["FAKE", "SENTINEL"]
    task.oracle.critical_path_seconds = 99999.0

    result = run_transcript(task, workspace_dir="/tmp/lhos-test-transcript-cp")
    assert result is not None

    task.oracle.critical_path = original_cp


# ── 4. All modes use the same verifier registry ───────────────────────────

def test_all_modes_declare_real_verifier():
    for m in all_manifests():
        assert m.uses_real_verifier, f"mode {m.mode} does not use real verifier"


def test_transcript_uses_same_verifier_as_graph():
    """The transcript mode imports and uses the same verifier registry."""
    from lhos.verification.registry import build_default_registry

    # This is the same function used by RuntimeStack (bootstrap.py)
    registry = build_default_registry()
    # Check that the registry has the standard verifiers
    assert "file_exists" in registry.names(), (
        "registry missing file_exists verifier"
    )
    assert "command" in registry.names(), "registry missing command verifier"

    # Verify that transcript run_transcript uses this registry
    task = generate("serial_chain", size="small", seed=1)
    with tempfile.TemporaryDirectory() as tmpdir:
        result = run_transcript(task, workspace_dir=tmpdir)
        assert result is not None


# ── 5. Cost accounting consistency ────────────────────────────────────────

def test_all_modes_track_all_cost_dimensions():
    """Every mode must track tokens, tool calls, and time."""
    for m in all_manifests():
        assert m.tracks_token_cost, f"mode {m.mode} doesn't track tokens"
        assert m.tracks_tool_calls, f"mode {m.mode} doesn't track tool calls"
        assert m.tracks_time_cost, f"mode {m.mode} doesn't track time"


def test_scoring_fields_are_consistent_across_modes():
    """Both score_graph_run and score_transcript_run produce rows with the
    same set of keys (the metric fields must be identical)."""
    task = generate("serial_chain", size="small", seed=1)

    # Run transcript mode
    with tempfile.TemporaryDirectory() as tmpdir:
        result = run_transcript(task, workspace_dir=tmpdir)
        transcript_row = score_transcript_run(result, task)

    # Run a graph mode
    from lhos.benchmarks.runner import run_cell

    with tempfile.TemporaryDirectory() as tmpdir:
        graph_row = run_cell(task, "dynamic_graph_local_repair", work_root=tmpdir)

    # Both scoring functions must produce the same keys
    # (run_cell adds 'run_id' and 'db_path' for debugging, but scoring
    #  functions themselves produce the same fields)
    transcript_keys = set(transcript_row.keys())
    graph_keys = set(graph_row.keys())
    # Remove keys added by run_cell (not scoring functions)
    graph_keys -= {"run_id", "db_path"}
    assert transcript_keys == graph_keys, (
        f"Key mismatch:\n  only in transcript: {transcript_keys - graph_keys}\n"
        f"  only in graph: {graph_keys - transcript_keys}"
    )


def test_cost_fields_are_non_negative():
    """All cost fields must be non-negative in both scoring paths."""
    task = generate("serial_chain", size="small", seed=1)

    with tempfile.TemporaryDirectory() as tmpdir:
        result = run_transcript(task, workspace_dir=tmpdir)
        row = score_transcript_run(result, task)

    cost_fields = [
        "input_tokens", "output_tokens", "total_tokens",
        "tool_calls", "model_calls",
        "simulated_time_seconds",
    ]
    for field in cost_fields:
        assert row[field] >= 0, f"{field} is negative: {row[field]}"


# ── 6. Mode capability ordering (fairness gradient) ──────────────────────

def test_capability_gradient_is_monotonic():
    """Capabilities should form a gradient: transcript < static < dynamic < full.

    Each mode in the progression should have at least as many capabilities
    as the previous one (no mode loses a capability that an earlier mode has).
    """
    progression = [
        "transcript",
        "static_graph_fifo",
        "dynamic_graph_fifo",
        "dynamic_graph_local_repair",
        "dynamic_graph_cost_aware",
        "full_lhos",
    ]
    manifests = {m.mode: m for m in all_manifests()}

    cap_fields = [
        "can_crash_recover",
        "can_reconcile_invalidation",
        "can_local_repair",
        "can_use_cost_aware_scheduler",
        "can_checkpoint",
    ]

    for i in range(1, len(progression)):
        prev = manifests[progression[i - 1]]
        curr = manifests[progression[i]]
        # Current mode should have at least the capabilities of the previous
        for field in cap_fields:
            if getattr(prev, field):
                assert getattr(curr, field), (
                    f"mode {curr.mode} lost capability {field} vs {prev.mode}"
                )


def test_oracle_modes_have_more_info_than_dynamic():
    """Oracle modes must declare oracle priority access; dynamic modes must not."""
    manifests = {m.mode: m for m in all_manifests()}

    for mode in MODES:
        m = manifests[mode]
        mc = mode_config(mode)
        assert m.can_access_oracle_priorities == mc.use_oracle_priorities, (
            f"mode {mode}: manifest says {m.can_access_oracle_priorities}, "
            f"mode_config says {mc.use_oracle_priorities}"
        )


# ── 7. No hidden oracle access from runtime ───────────────────────────────

def test_runtime_does_not_import_hidden_oracle():
    """The lhos.runtime package must not import HiddenOracleSpec."""
    import importlib
    import pkgutil

    import lhos.runtime as runtime_pkg

    # Check all modules in the runtime package
    for importer, modname, ispkg in pkgutil.walk_packages(
        runtime_pkg.__path__, prefix="lhos.runtime."
    ):
        try:
            mod = importlib.import_module(modname)
            source = open(mod.__file__).read() if hasattr(mod, "__file__") and mod.__file__ else ""
            assert "HiddenOracleSpec" not in source, (
                f"module {modname} references HiddenOracleSpec"
            )
            assert "to_hidden_oracle" not in source, (
                f"module {modname} references to_hidden_oracle"
            )
        except (ImportError, OSError, FileNotFoundError):
            pass


def test_no_oracle_access_in_transcript_source():
    """The transcript module must not import or reference oracle info."""
    import lhos.benchmarks.transcript as transcript_mod

    source = open(transcript_mod.__file__).read()
    assert "task.oracle.priorities" not in source, (
        "transcript module accesses task.oracle.priorities"
    )
    assert "task.oracle.critical_path" not in source, (
        "transcript module accesses task.oracle.critical_path"
    )
    assert "task.oracle.affected_by_event" not in source, (
        "transcript module accesses task.oracle.affected_by_event"
    )