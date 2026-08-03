"""Architecture tests for Public/Hidden Oracle isolation (audit Milestone 1D).

These tests enforce that:
1. The runtime package does not import hidden oracle modules.
2. PublicTaskSpec strips oracle priorities.
3. Oracle modes are properly named and separated.
4. The scoring code can access oracle (for analysis) but runtime cannot.
"""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path

import pytest

from lhos.benchmarks.controlled.generator import generate
from lhos.benchmarks.controlled.specs import (
    HiddenOracleSpec,
    PublicTaskSpec,
    to_hidden_oracle,
    to_public_spec,
)


# ----------------------------------------------------------- import graph test
def test_runtime_package_does_not_import_oracle():
    """The lhos.runtime package must NOT import HiddenOracleSpec or
    to_hidden_oracle from the controlled benchmark specs."""
    runtime_path = Path(__file__).resolve().parents[2] / "src" / "lhos" / "runtime"
    assert runtime_path.exists(), f"runtime path not found: {runtime_path}"

    forbidden_imports = [
        "HiddenOracleSpec",
        "to_hidden_oracle",
        "hidden_oracle",
        "specs",
    ]

    violations: list[str] = []
    for py_file in runtime_path.rglob("*.py"):
        try:
            text = py_file.read_text(encoding="utf-8")
        except Exception:
            continue
        for forbidden in forbidden_imports:
            if f"from lhos.benchmarks.controlled.specs import" in text and forbidden in text:
                violations.append(f"{py_file.name}: imports {forbidden}")
            if f"import {forbidden}" in text and "benchmarks" in text:
                violations.append(f"{py_file.name}: imports {forbidden}")

    assert not violations, f"Runtime package imports forbidden oracle modules: {violations}"


def test_runtime_modules_do_not_reference_oracle():
    """No file in lhos/runtime/ should reference 'oracle', 'hidden', or
    'ground_truth'."""
    runtime_path = Path(__file__).resolve().parents[2] / "src" / "lhos" / "runtime"
    forbidden_patterns = ["oracle", "ground_truth", "hidden_oracle"]

    violations: list[str] = []
    for py_file in runtime_path.rglob("*.py"):
        try:
            text = py_file.read_text(encoding="utf-8")
        except Exception:
            continue
        for pattern in forbidden_patterns:
            if pattern in text.lower():
                # Allow comments and docstrings that mention "oracle" in
                # a non-access context (e.g. "oracle modes differ").
                lines = [
                    line.strip()
                    for line in text.splitlines()
                    if pattern in line.lower()
                    and not line.strip().startswith("#")
                    and not line.strip().startswith('"""')
                ]
                if lines:
                    violations.append(f"{py_file.name}: references '{pattern}'")

    assert not violations, f"Runtime package references oracle/hidden patterns: {violations}"


# ----------------------------------------------------------- public spec test
def test_public_spec_strips_oracle_priorities():
    """PublicTaskSpec must set all priorities to 0.0."""
    task = generate("serial_chain", size="small", seed=1)
    public = to_public_spec(task)

    assert isinstance(public, PublicTaskSpec)
    for node in public.nodes:
        assert node.get("priority", 0.0) == 0.0, (
            f"node {node.get('temp_id')} has non-zero priority in public spec"
        )


def test_public_spec_does_not_contain_oracle_info():
    """PublicTaskSpec must not contain critical_path, affected_by_event, etc."""
    task = generate("serial_chain", size="small", seed=1)
    public = to_public_spec(task)

    assert not hasattr(public, "critical_path")
    assert not hasattr(public, "affected_by_event")
    assert not hasattr(public, "priorities")
    assert not hasattr(public, "optimal_schedule")


def test_hidden_oracle_contains_ground_truth():
    """HiddenOracleSpec must contain the oracle ground truth."""
    task = generate("serial_chain", size="small", seed=1)
    oracle = to_hidden_oracle(task)

    assert isinstance(oracle, HiddenOracleSpec)
    assert isinstance(oracle.critical_path, list)
    assert isinstance(oracle.affected_by_event, dict)
    assert isinstance(oracle.priorities, dict)
    assert isinstance(oracle.critical_path_seconds, float)


# ----------------------------------------------------------- oracle mode naming
def test_oracle_modes_are_separately_named():
    """Oracle modes must be named 'oracle_graph_fifo' and
    'oracle_graph_cost_aware' — not mixed with main methods."""
    from lhos.benchmarks.modes import MODES

    assert "oracle_graph_fifo" in MODES
    assert "oracle_graph_cost_aware" in MODES

    # Oracle modes must not be in the main 4 experiment modes
    main_modes = {"transcript", "static_graph_fifo", "dynamic_graph_fifo", "full_lhos"}
    assert main_modes.isdisjoint({"oracle_graph_fifo", "oracle_graph_cost_aware"})


def test_oracle_modes_use_oracle_priorities():
    """Only oracle modes should set use_oracle_priorities=True."""
    from lhos.benchmarks.modes import mode_config

    for mode_name in [
        "transcript",
        "static_graph_fifo",
        "dynamic_graph_fifo",
        "dynamic_graph_local_repair",
        "dynamic_graph_cost_aware",
        "full_lhos",
    ]:
        mc = mode_config(mode_name)
        assert not mc.use_oracle_priorities, f"mode {mode_name} should not use oracle priorities"

    for mode_name in ["oracle_graph_fifo", "oracle_graph_cost_aware"]:
        mc = mode_config(mode_name)
        assert mc.use_oracle_priorities, f"mode {mode_name} should use oracle priorities"


# ----------------------------------------------------------- graph_spec isolation
def test_graph_spec_non_oracle_has_zero_priority():
    """graph_spec(use_oracle_priorities=False) must set all priorities to 0."""
    task = generate("wide_dag", size="small", seed=2)
    spec = task.graph_spec(use_oracle_priorities=False)

    for node in spec["nodes"]:
        assert node.get("priority", 0.0) == 0.0


def test_graph_spec_oracle_has_nonzero_priority():
    """graph_spec(use_oracle_priorities=True) should have at least some
    non-zero priorities (if the preset defines them)."""
    task = generate("costly_critical_path", size="small", seed=1)
    spec = task.graph_spec(use_oracle_priorities=True)

    has_nonzero = any(node.get("priority", 0.0) != 0.0 for node in spec["nodes"])
    assert has_nonzero, "oracle spec should have non-zero priorities"


# ----------------------------------------------------------- scoring access test
def test_scoring_can_access_oracle_for_analysis():
    """The scoring code (analysis) is allowed to access oracle data — this
    is for computing analysis metrics like replanning_amplification, not
    for determining success/progress."""
    task = generate("serial_chain", size="small", seed=1)
    oracle = to_hidden_oracle(task)

    # Scoring uses oracle.critical_path_seconds and oracle.affected_by_event
    # as denominators for analysis metrics — this is legitimate.
    assert oracle.critical_path_seconds >= 0.0
    assert isinstance(oracle.affected_by_event, dict)
