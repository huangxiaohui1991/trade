"""
platform/config.py — 配置版本化管理

每次 run 启动时 freeze 一份 config snapshot（deep copy + SHA256 hash），
写入 config_versions 表。整个 run 期间使用这份 snapshot，不再读文件。
不做配置热加载。
"""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _config_dir() -> Path:
    return Path(__file__).resolve().parent.parent.parent.parent / "config"


@dataclass(frozen=True)
class ConfigSnapshot:
    """Frozen configuration — immutable for the lifetime of a run."""

    version: str
    hash: str
    data: dict = field(repr=False)

    def get(self, *keys: str, default: Any = None) -> Any:
        """Nested key access: snapshot.get('scoring', 'weights')"""
        d = self.data
        for k in keys:
            if isinstance(d, dict):
                d = d.get(k)
            else:
                return default
            if d is None:
                return default
        return d


class ConfigRegistry:
    """Load, validate, freeze, and version configuration."""

    def __init__(self, config_dir: Optional[Path] = None, profile: str = "default"):
        self._config_dir = config_dir or _config_dir()
        self._profile = profile

    def freeze(self, conn: sqlite3.Connection) -> ConfigSnapshot:
        """
        Load all config files, merge profile overlay, validate, compute hash,
        persist to config_versions, return frozen snapshot.
        """
        data = self._load_merged()

        # JSON Schema validation (best-effort)
        errors = self._validate(data)
        if errors:
            import logging
            logging.getLogger(__name__).warning(f"Config validation warnings: {errors}")

        config_json = json.dumps(data, ensure_ascii=False, sort_keys=True, default=str)
        config_hash = hashlib.sha256(config_json.encode()).hexdigest()[:16]

        # Check if this exact config already exists
        existing = conn.execute(
            "SELECT config_version FROM config_versions WHERE config_hash = ?",
            (config_hash,),
        ).fetchone()

        if existing:
            version = existing[0]
        else:
            version = f"v{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{config_hash[:8]}"
            conn.execute(
                """INSERT INTO config_versions
                   (config_version, config_hash, config_json, created_at)
                   VALUES (?, ?, ?, ?)""",
                (version, config_hash, config_json, _now_iso()),
            )

        return ConfigSnapshot(version=version, hash=config_hash, data=copy.deepcopy(data))

    def get_version(self, conn: sqlite3.Connection, config_version: str) -> Optional[dict]:
        """Load a historical config by version string."""
        row = conn.execute(
            "SELECT config_json FROM config_versions WHERE config_version = ?",
            (config_version,),
        ).fetchone()
        if row:
            return json.loads(row[0])
        return None

    def list_versions(self, conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
        """List recent config versions."""
        rows = conn.execute(
            """SELECT config_version, config_hash, created_at, activated_at
               FROM config_versions ORDER BY created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def load_and_validate(self) -> tuple[dict, list[str]]:
        """Load config and validate against schema. Returns (data, errors)."""
        data = self._load_merged()
        errors = self._validate(data)
        return data, errors

    def _validate(self, data: dict) -> list[str]:
        """Validate config against built-in schema rules. Returns list of error messages."""
        errors: list[str] = []
        strategy = data.get("strategy", {})
        if not strategy:
            errors.append("strategy config missing")
            return errors

        # Scoring weights must sum to 10
        weights = strategy.get("scoring", {}).get("weights", {})
        if weights:
            total = sum(weights.values())
            if total != 10:
                errors.append(f"scoring weights sum to {total}, expected 10")

        # Thresholds: buy > watch > reject
        thresholds = strategy.get("scoring", {}).get("thresholds", {})
        if thresholds:
            buy = thresholds.get("buy", 0)
            watch = thresholds.get("watch", 0)
            reject = thresholds.get("reject", 0)
            if not (buy > watch > reject):
                errors.append(f"thresholds must be buy({buy}) > watch({watch}) > reject({reject})")

        # Risk: stop_loss must be positive
        risk = strategy.get("risk", {})
        for style in ("slow_bull", "momentum"):
            sl = risk.get(style, {}).get("stop_loss", 0)
            if sl and sl <= 0:
                errors.append(f"risk.{style}.stop_loss must be positive, got {sl}")

        # Position limits
        pos = risk.get("position", {})
        if pos:
            if pos.get("single_max", 0) > pos.get("total_max", 1):
                errors.append("position.single_max > position.total_max")

        return errors

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load_merged(self) -> dict:
        """Load base configs + optional profile overlay."""
        data: dict[str, Any] = {}

        for name in ("strategy", "stocks", "notification", "paths"):
            path = self._config_dir / f"{name}.yaml"
            if path.exists():
                with open(path, encoding="utf-8") as f:
                    content = yaml.safe_load(f)
                    if content:
                        data[name] = content

        # Profile overlay
        if self._profile != "default":
            overlay_path = self._config_dir / "profiles" / f"{self._profile}.yaml"
            if overlay_path.exists():
                with open(overlay_path, encoding="utf-8") as f:
                    overlay = yaml.safe_load(f) or {}
                data = _deep_merge(data, overlay)

        return data


def _deep_merge(base: dict, overlay: dict) -> dict:
    """Recursively merge overlay into base (overlay wins on conflict)."""
    result = copy.deepcopy(base)
    for key, value in overlay.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result
