"""LongHorizonOS E5 — benchmark tests: correctness oracle, baselines fair,
falsification, small-cone, reproducibility, mutations, BENCH-G, VPG guardian."""

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
        t = run_trial(10, topo, seed=1, root_idx=0)
        assert t["valid_trial"] and t["lhos_correct"], topo
        assert t["under_invalidation"] == 0, topo
        assert t["over_invalidation"] == 0, topo


def test_100pct_affected_falsification_no_false_preservation():
    # A root that affects the whole graph must yield preservation ≈ 0 (no cheating).
    t = measure(30, "chain", seed=5, target_fraction=1.0)
    assert t["lhos_correct"]
    # chain root T0 affects all
    assert t["preservation_ratio"] < 0.1


def test_small_cone_preserves_majority():
    t = measure(50, "chain", seed=3, target_fraction=0.1)
    assert t["lhos_correct"]
    assert t["preservation_ratio"] >= 0.5


def test_full_restart_reruns_everything():
    t = measure(20, "mixed", seed=2, target_fraction=0.5)
    assert t["full_restart_rerun"] == 20


def test_checkpoint_reruns_static_suffix():
    t = measure(20, "mixed", seed=2, target_fraction=0.5)
    # checkpoint reruns the affected downstream suffix (>= lhos, <= full)
    assert t["checkpoint_rerun"] >= t["lhos_rerun"]
    assert t["checkpoint_rerun"] <= 20


def test_lhos_frontier_matches_oracle():
    t = measure(25, "mixed", seed=1, target_fraction=0.25)
    assert t["lhos_matches_oracle_frontier"]


def test_reproducibility_identical_results():
    a = measure(25, "mixed", seed=7, target_fraction=0.25)
    b = measure(25, "mixed", seed=7, target_fraction=0.25)
    assert a["affected_fraction"] == b["affected_fraction"]
    assert a["lhos_rerun"] == b["lhos_rerun"]
    assert a["preservation_ratio"] == b["preservation_ratio"]


def test_bench_harness_does_not_have_semantic_bypass():
    """Bench harness is measurement-only: it imports the SDK, never sets Core state."""
    import pathlib

    src = pathlib.Path("src/lhos/benchmarks/semantic_repair/harness.py").read_text(encoding="utf-8")
    assert "verified = True" not in src
    assert "stale = " not in src.replace("stale_set", "X")


def test_bench_uses_real_core_not_fake():
    import pathlib

    src = pathlib.Path("src/lhos/benchmarks/semantic_repair/harness.py").read_text(encoding="utf-8")
    assert "os_.run" in src or "os_.repair" in src  # LongHorizonOS goes through the SDK


def test_bench_never_imports_core_privacy():
    import pathlib

    for f in ["harness.py", "run.py"]:
        src = pathlib.Path("src/lhos/benchmarks/semantic_repair") / f
        assert "verified_progress.graph_store" not in src.read_text(encoding="utf-8")
        assert "invalidation.cone" not in src.read_text(encoding="utf-8")
