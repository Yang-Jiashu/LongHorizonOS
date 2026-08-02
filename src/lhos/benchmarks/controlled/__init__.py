"""Controlled benchmark: deterministic generator + oracle + environment (spec 22)."""

from lhos.benchmarks.controlled.generator import PRESETS, SIZES, generate
from lhos.benchmarks.controlled.task_schema import ControlledTask, ControlledTaskSpec

__all__ = ["PRESETS", "SIZES", "ControlledTask", "ControlledTaskSpec", "generate"]
