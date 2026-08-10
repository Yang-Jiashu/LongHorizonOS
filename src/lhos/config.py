"""YAML configuration loading (spec section 21).

Loads configs/default.yaml and deep-merges one override file on top.
"""

from __future__ import annotations

from importlib.resources import files
from pathlib import Path
from typing import Any

import yaml

_PACKAGE_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_PATH = _PACKAGE_ROOT / "configs" / "default.yaml"
_PACKAGED_DEFAULT_CONFIG = files("lhos.configs").joinpath("default.yaml")


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(override_path: str | Path | None = None) -> dict[str, Any]:
    config: dict[str, Any] = {}
    if _PACKAGED_DEFAULT_CONFIG.is_file():
        config = yaml.safe_load(_PACKAGED_DEFAULT_CONFIG.read_text(encoding="utf-8")) or {}
    elif DEFAULT_CONFIG_PATH.exists():
        config = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8")) or {}
    if override_path is not None:
        override = yaml.safe_load(Path(override_path).read_text(encoding="utf-8")) or {}
        config = _deep_merge(config, override)
    return config


def load_scheduler_config(path: str | Path) -> dict[str, Any]:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
