#!/usr/bin/env python3
"""
运行状态记录工具

将每日 pipeline 执行结果写入 data/runtime/daily_state_YYYY-MM-DD.json，
方便查看每个流程是否完成、产物路径、降级来源和错误信息。
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RUNTIME_DIR = PROJECT_ROOT / "data" / "runtime"


def _state_path(date_str: str) -> Path:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    return RUNTIME_DIR / f"daily_state_{date_str}.json"


def _json_safe(value):
    """将 Path / datetime / 容器等转换为可序列化结构。"""
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return value


def load_daily_state(date_str: Optional[str] = None) -> dict:
    """读取某天的状态文件，不存在则返回空结构。"""
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    path = _state_path(date_str)
    if not path.exists():
        return {"date": date_str, "pipelines": {}}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        data.setdefault("date", date_str)
        data.setdefault("pipelines", {})
        return data
    except Exception:
        return {"date": date_str, "pipelines": {}}


def update_pipeline_state(name: str, status: str, details: Optional[dict] = None,
                          date_str: Optional[str] = None) -> str:
    """
    更新某个 pipeline 的状态。

    status: success | warning | error | skipped
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    state = load_daily_state(date_str)
    state["updated_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    state.setdefault("pipelines", {})
    state["pipelines"][name] = {
        "status": status,
        "updated_at": state["updated_at"],
        "details": _json_safe(details or {}),
    }

    path = _state_path(date_str)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    return str(path)


def _get_state_module():
    from scripts import state
    return state


def load_portfolio_snapshot(scope: Optional[str] = None) -> dict:
    return _get_state_module().load_portfolio_snapshot(scope=scope)


def load_market_snapshot(refresh: bool = False) -> dict:
    return _get_state_module().load_market_snapshot(refresh=refresh)


def load_activity_summary(window="week", scope: str = "cn_a_system") -> dict:
    return _get_state_module().load_activity_summary(window=window, scope=scope)


def bootstrap_state(force: bool = False) -> dict:
    return _get_state_module().bootstrap_state(force=force)


def audit_state() -> dict:
    return _get_state_module().audit_state()


def sync_portfolio_state() -> dict:
    return _get_state_module().sync_portfolio_state()


def sync_activity_state() -> dict:
    return _get_state_module().sync_activity_state()


def load_pool_snapshot() -> dict:
    """兼容 pool_manager 的 {entries, metadata, updated_at} 形态。"""
    snapshot = _get_state_module().load_pool_snapshot()
    entries = list(snapshot.get("entries", []))
    if not entries:
        entries.extend(snapshot.get("core_pool", []))
        entries.extend(snapshot.get("watch_pool", []))
        entries.extend(snapshot.get("other_entries", []))
    return {
        "entries": entries,
        "metadata": snapshot.get("metadata", {}),
        "updated_at": snapshot.get("updated_at", ""),
        "summary": snapshot.get("summary", {}),
        "snapshot_date": snapshot.get("snapshot_date", ""),
        "source": snapshot.get("source", ""),
    }


def save_pool_snapshot(entries: list, metadata: Optional[dict] = None) -> str:
    result = _get_state_module().save_pool_snapshot(entries, metadata or {})
    return str(result.get("db_path", _get_state_module().LEDGER_DB_PATH))
