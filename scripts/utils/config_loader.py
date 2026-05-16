"""YAML config loading helpers for legacy scripts."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_ROOT = PROJECT_ROOT / "config"


def _load_yaml(name: str) -> dict[str, Any]:
    path = CONFIG_ROOT / f"{name}.yaml"
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


@lru_cache(maxsize=None)
def get_stocks() -> dict[str, Any]:
    return _load_yaml("stocks")


@lru_cache(maxsize=None)
def get_strategy() -> dict[str, Any]:
    return _load_yaml("strategy")


def clear_config_cache(name: str | None = None) -> None:
    get_stocks.cache_clear()
    get_strategy.cache_clear()
