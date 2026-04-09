#!/usr/bin/env python3
"""
统一运行上下文

职责：
  - 生成 run_id
  - 运行锁
  - 运行结果 JSON 落盘
  - daily_state 摘要联动
"""

import json
import os
import socket
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from scripts.utils.runtime_state import update_pipeline_state


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOCK_DIR = PROJECT_ROOT / "data" / "locks"
RUNS_DIR = PROJECT_ROOT / "data" / "runs"

VALID_STATUSES = {"success", "warning", "error", "skipped", "blocked"}


def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _safe_json(value):
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if hasattr(value, "to_dict"):
        try:
            return value.to_dict()
        except Exception:
            pass
    if hasattr(value, "tolist"):
        try:
            return value.tolist()
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(k): _safe_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_safe_json(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def sanitize_for_json(value):
    return _safe_json(value)


def make_run_id(name: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{name}_{stamp}_{os.getpid()}"


def run_output_dir(date_str: Optional[str] = None) -> Path:
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    path = RUNS_DIR / date_str
    path.mkdir(parents=True, exist_ok=True)
    return path


def result_path(name: str, run_id: str, date_str: Optional[str] = None) -> Path:
    return run_output_dir(date_str) / f"{name}_{run_id}.json"


def write_run_result(payload: dict, date_str: Optional[str] = None) -> str:
    name = payload.get("pipeline", "run")
    run_id = payload.get("run_id", make_run_id(name))
    path = result_path(name, run_id, date_str)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(_safe_json(payload), f, ensure_ascii=False, indent=2)
    return str(path)


def summarize_run_result(payload: dict) -> dict:
    details = {
        "run_id": payload.get("run_id", ""),
        "result_path": payload.get("result_path", ""),
        "started_at": payload.get("started_at", ""),
        "finished_at": payload.get("finished_at", ""),
        "duration_seconds": payload.get("duration_seconds", 0),
        "retryable": payload.get("retryable", False),
        "error": payload.get("error"),
    }
    if isinstance(payload.get("details"), dict):
        details.update(payload["details"])
    return details


def sync_run_to_daily_state(payload: dict, date_str: Optional[str] = None) -> str:
    status = payload.get("status", "error")
    if status not in VALID_STATUSES:
        status = "error"
    return update_pipeline_state(
        payload.get("pipeline", "run"),
        status,
        summarize_run_result(payload),
        date_str=date_str,
    )


@contextmanager
def pipeline_lock(name: str):
    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    path = LOCK_DIR / f"{name}.lock"
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                lock_info = json.load(f)
        except Exception:
            lock_info = {"message": "lock_exists"}
        raise RuntimeError(json.dumps(lock_info, ensure_ascii=False))

    info = {
        "pipeline": name,
        "pid": os.getpid(),
        "host": socket.gethostname(),
        "started_at": now_ts(),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(info, f, ensure_ascii=False, indent=2)

    try:
        yield str(path)
    finally:
        try:
            path.unlink(missing_ok=True)
        except TypeError:
            if path.exists():
                path.unlink()
