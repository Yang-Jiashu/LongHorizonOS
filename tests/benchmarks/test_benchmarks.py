"""LongHorizonOS E5 benchmark tests.

Covers correctness oracle, fair baselines, falsification, small-cone repair,
reproducibility, mutations, BENCH-G and the VPG guardian.
"""

from __future__ import annotations

from lhos.benchmarks.semantic_repair.harness import (
    build_dag,
    measure,
    oracle,
    run_trial,
)


def test_oracle_is_independent_and_sound():
    ids, edges = build_dag(10, "chain", 1)
    aff, pres, front = oracle(edges, "T0")
    assert len(aff) == 10 and front == ["T0"]


def test_lhos_matches_oracle_all_topologies():
    for topo in ["chain", "fan_out", "fan_in", "diamond", "mixed"]:
        trial = run_trial(10, topo, seed=1, root_idx=0)
        assert trial["valid_trial"] and trial["lhos_correct"], topo
        assert trial["under_invalidation"] == 0, topo
        assert trial["over_invalidation"] == 0, topo


def test_100pct_affected_falsification_no_false_preservation():
    # A root that affects the whole graph must yield preservation near zero.
    trial = measure(30, "chain", seed=5, target_fraction=1.0)
    assert trial["lhos_correct"]
    assert trial["preservation_ratio"] < 0.1


def test_small_cone_preserves_majority():
    trial = measure(50, "chain", seed=3, target_fraction=0.1)
    assert trial["lhos_correct"]
    assert trial["preservation_ratio"] >= 0.5


def test_full_restart_reruns_everything():
    trial = measure(20, "mixed", seed=2, target_fraction=0.5)
    assert trial["full_restart_rerun"] == 20


def test_checkpoint_reruns_static_suffix():
    trial = measure(20, "mixed", seed=2, target_fraction=0.5)
    assert trial["checkpoint_rerun"] >= trial["lhos_rerun"]
    assert trial["checkpoint_rerun"] <= 20


def test_lhos_frontier_matches_oracle():
    trial = measure(25, "mixed", seed=1, target_fraction=0.25)
    assert trial["lhos_matches_oracle_frontier"]


def test_reproducibility_identical_results():
    first = measure(25, "mixed", seed=7, target_fraction=0.25)
    second = measure(25, "mixed", seed=7, target_fraction=0.25)
    assert first["affected_fraction"] == second["affected_fraction"]
    assert first["lhos_rerun"] == second["lhos_rerun"]
    assert first["preservation_ratio"] == second["preservation_ratio"]


def test_bench_harness_does_not_have_semantic_bypass():
    """The harness imports the SDK and never sets Core semantic state."""
    import pathlib

    src = pathlib.Path("src/lhos/benchmarks/semantic_repair/harness.py").read_text(encoding="utf-8")
    assert "verified = True" not in src
    assert "stale = " not in src.replace("stale_set", "X")


def test_bench_uses_real_core_not_fake():
    import pathlib

    src = pathlib.Path("src/lhos/benchmarks/semantic_repair/harness.py").read_text(encoding="utf-8")
    assert "runtime.run" in src and "runtime.repair" in src


def test_bench_never_imports_core_privacy():
    import pathlib

    for filename in ["harness.py", "run.py"]:
        src = pathlib.Path("src/lhos/benchmarks/semantic_repair") / filename
        assert "verified_progress.graph_store" not in src.read_text(encoding="utf-8")
        assert "invalidation.cone" not in src.read_text(encoding="utf-8")


def test_repair_metrics_observe_real_attempts_and_closure():
    trial = measure(20, "chain", seed=4, target_fraction=0.5)
    assert trial["valid_trial"]
    assert trial["repair_attempts"] == trial["lhos_rerun"]
    assert trial["final_goal_closed"]
    assert trial["false_verified"] == 0
    assert trial["ownership_conflicts"] == 0


def test_checkpoint_parity_is_reported_instead_of_hidden():
    trial = measure(20, "chain", seed=4, target_fraction=0.5)
    assert trial["checkpoint_correct"]
    assert trial["lhos_rerun"] == trial["checkpoint_rerun"]
    assert trial["weighted_saving_vs_checkpoint"] == 0
    assert trial["state_only_false_closure"]
