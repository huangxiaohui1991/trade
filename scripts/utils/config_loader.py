"""Configuration loader for reading config/*.yaml files."""

import os
from pathlib import Path
from typing import Dict, Optional

try:
    import yaml
except ImportError:
    yaml = None  # PyYAML optional fallback

# Project root: scripts/utils/config_loader.py -> project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

_CONFIG_PATHS = {
    "strategy": _PROJECT_ROOT / "config" / "strategy.yaml",
    "stocks": _PROJECT_ROOT / "config" / "stocks.yaml",
    "notification": _PROJECT_ROOT / "config" / "notification.yaml",
}

# In-memory cache so we only read each file once
_cache: Dict[str, dict] = {}


def _load_file(name: str) -> dict:
    """Load and cache a single YAML config file."""
    if yaml is None:
        raise ImportError("PyYAML not installed. Run: pip install pyyaml")
    if name in _cache:
        return _cache[name]
    path = _CONFIG_PATHS[name]
    with open(path, "r", encoding="utf-8") as f:
        _cache[name] = yaml.safe_load(f)
    return _cache[name]


class Config:
    """Container for all configuration sections."""

    def __init__(self):
        self.strategy: dict = _load_file("strategy")
        self.stocks: dict = _load_file("stocks")
        self.notification: dict = _load_file("notification")


def load_config() -> Config:
    """Load and return all configuration sections (cached)."""
    return Config()


def get_strategy() -> dict:
    """Return the strategy configuration (cached)."""
    return _load_file("strategy")


def get_stocks() -> dict:
    """Return the stocks configuration (cached)."""
    return _load_file("stocks")


def get_notification() -> dict:
    """Return the notification configuration (cached)."""
    return _load_file("notification")


def clear_config_cache(name: Optional[str] = None) -> None:
    """Clear one cached config or the full config cache."""
    if name is None:
        _cache.clear()
        return
    _cache.pop(name, None)
