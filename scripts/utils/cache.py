"""Small JSON cache helpers used by legacy scripts."""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CACHE_ROOT = PROJECT_ROOT / "data" / "cache"


def _safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip()) or "default"


def _cache_path(namespace: str, key: str) -> Path:
    return CACHE_ROOT / _safe_name(namespace) / f"{_safe_name(key)}.json"


def load_json_cache(namespace: str, key: str, max_age_seconds: int | None = None) -> Any:
    path = _cache_path(namespace, key)
    if not path.exists():
        return None
    if max_age_seconds is not None and time.time() - path.stat().st_mtime > max_age_seconds:
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload.get("data", payload)


def save_json_cache(namespace: str, key: str, data: Any, meta: dict | None = None) -> Path:
    path = _cache_path(namespace, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"data": data, "meta": meta or {}}, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    return path
