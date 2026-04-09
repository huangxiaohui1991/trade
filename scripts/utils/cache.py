#!/usr/bin/env python3
"""
本地 JSON 缓存工具

用于保存外部数据源的最新成功结果，避免自动化任务因临时接口抖动而空跑。
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "cache"


def _cache_path(namespace: str, key: str) -> Path:
    safe_namespace = namespace.strip().replace("/", "_")
    safe_key = key.strip().replace("/", "_")
    path = CACHE_DIR / safe_namespace
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{safe_key}.json"


def load_json_cache(namespace: str, key: str, max_age_seconds: Optional[int] = None) -> Optional[dict]:
    """读取 JSON 缓存；超时则返回 None。"""
    path = _cache_path(namespace, key)
    if not path.exists():
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return None

    cached_at = payload.get("cached_at", "")
    if max_age_seconds is not None and cached_at:
        try:
            age_seconds = (datetime.now() - datetime.fromisoformat(cached_at)).total_seconds()
        except ValueError:
            return None
        if age_seconds > max_age_seconds:
            return None

    return payload


def save_json_cache(namespace: str, key: str, data, meta: Optional[dict] = None) -> str:
    """写入 JSON 缓存。"""
    path = _cache_path(namespace, key)
    payload = {
        "cached_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "data": data,
        "meta": meta or {},
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return str(path)
