#!/usr/bin/env python3
"""
池子状态管理

职责：
  - 记录候选股票连续高分/低分/veto 状态
  - 基于连续天数给出更严格的池子晋级/降级建议
  - 将状态持久化到 data/runtime/pool_state.json
"""

import json
from datetime import datetime
from pathlib import Path

from scripts.utils.config_loader import get_strategy
from scripts.utils.runtime_state import RUNTIME_DIR
from scripts.engine.scorer import split_veto_signals


POOL_STATE_PATH = RUNTIME_DIR / "pool_state.json"
WARNING_ONLY_SIGNALS = {"consecutive_outflow_warn"}


def _safe_float(value, default=0.0) -> float:
    try:
        if value in [None, ""]:
            return default
        if isinstance(value, str):
            value = value.replace("**", "").replace(",", "").strip()
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_state() -> dict:
    if not POOL_STATE_PATH.exists():
        return {"updated_at": "", "codes": {}}
    try:
        with open(POOL_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        data.setdefault("codes", {})
        return data
    except Exception:
        return {"updated_at": "", "codes": {}}


def load_pool_state() -> dict:
    return _load_state()


def _save_state(state: dict) -> str:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    with open(POOL_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    return str(POOL_STATE_PATH)


def _json_safe(value):
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return value


def _runtime_snapshot_io():
    """尽量接入统一状态接口；不存在时回退到本模块实现。"""
    try:
        from scripts.utils import runtime_state
    except Exception:
        return None, None
    return (
        getattr(runtime_state, "load_pool_snapshot", None),
        getattr(runtime_state, "save_pool_snapshot", None),
    )


def _safe_score(value, default=0.0) -> float:
    try:
        if value in [None, ""]:
            return default
        if isinstance(value, str):
            value = value.replace("**", "").replace(",", "").strip()
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_bucket(bucket: str | None, score: float, hard_veto: bool,
                      watch_min_score: float, promote_min_score: float) -> str:
    bucket = str(bucket or "").strip().lower()
    if bucket in {"core", "watch", "avoid"}:
        return bucket
    if hard_veto or score < watch_min_score:
        return "avoid"
    if score >= promote_min_score:
        return "core"
    return "watch"


def _build_note(hard_veto: list, warning_signals: list, bucket: str, fallback: str = "") -> str:
    if fallback:
        return fallback
    if hard_veto:
        return f"veto:{','.join(hard_veto)}"
    if warning_signals:
        return f"预警:{','.join(warning_signals)}"
    return {"core": "核心池", "watch": "观察池", "avoid": "规避"}.get(bucket, "")


def _normalize_entry(entry: dict, fallback_bucket: str = "avoid", source: str = "") -> dict:
    score = _safe_score(entry.get("total_score", 0))
    hard_veto, warning_signals = split_veto_signals(entry.get("veto_signals", []))
    bucket = _normalize_bucket(
        entry.get("bucket"),
        score,
        bool(entry.get("veto_triggered", False)) or bool(hard_veto),
        5.0,
        7.0,
    )
    note = str(entry.get("note", "") or "").strip()
    note = _build_note(hard_veto, warning_signals, bucket, note)
    normalized = {
        "bucket": bucket,
        "code": str(entry.get("code", "")).strip(),
        "name": str(entry.get("name", "")).strip(),
        "total_score": round(score, 1),
        "technical_score": _safe_score(entry.get("technical_score", 0)),
        "fundamental_score": _safe_score(entry.get("fundamental_score", 0)),
        "flow_score": _safe_score(entry.get("flow_score", 0)),
        "sentiment_score": _safe_score(entry.get("sentiment_score", 0)),
        "veto_triggered": bool(entry.get("veto_triggered", False)) or bool(hard_veto),
        "veto_signals": list(entry.get("veto_signals", [])) if entry.get("veto_signals") else [],
        "warning_signals": list(entry.get("warning_signals", [])) if entry.get("warning_signals") else warning_signals,
        "hard_veto_signals": list(entry.get("hard_veto_signals", [])) if entry.get("hard_veto_signals") else hard_veto,
        "note": note,
        "source": str(entry.get("source", source) or source),
    }
    if "previous_bucket" in entry:
        normalized["previous_bucket"] = entry.get("previous_bucket")
    if "updated_at" in entry:
        normalized["updated_at"] = entry.get("updated_at")
    return normalized


def _pool_membership_maps(snapshot: dict | None, stocks_cfg: dict | None = None) -> tuple[set, set]:
    snapshot = snapshot or {}
    entries = snapshot.get("entries", []) if isinstance(snapshot, dict) else []
    core_codes = {str(item.get("code", "")).strip() for item in entries if str(item.get("bucket", "")).strip() == "core"}
    watch_codes = {str(item.get("code", "")).strip() for item in entries if str(item.get("bucket", "")).strip() == "watch"}

    if core_codes or watch_codes:
        return core_codes, watch_codes

    stocks_cfg = stocks_cfg or {}
    core_codes = {str(item.get("code", "")).strip() for item in stocks_cfg.get("core_pool", [])}
    watch_codes = {str(item.get("code", "")).strip() for item in stocks_cfg.get("watch_pool", [])}
    return core_codes, watch_codes


def normalize_pool_snapshot(snapshot) -> dict:
    """将各种 snapshot 形态归一化为 {entries, metadata, ...}。"""
    if not snapshot:
        return {"entries": [], "metadata": {}, "updated_at": ""}
    if isinstance(snapshot, list):
        return {
            "entries": [_normalize_entry(item) for item in snapshot],
            "metadata": {},
            "updated_at": "",
        }
    if not isinstance(snapshot, dict):
        return {"entries": [], "metadata": {}, "updated_at": ""}

    entries = snapshot.get("entries") or snapshot.get("data") or []
    if not isinstance(entries, list):
        entries = []
    metadata = snapshot.get("metadata") or {}
    normalized = [_normalize_entry(item) for item in entries]
    return {
        "entries": normalized,
        "metadata": metadata if isinstance(metadata, dict) else {},
        "updated_at": snapshot.get("updated_at", ""),
    }


def load_pool_snapshot() -> dict:
    """读取结构化 pool snapshot；优先统一状态接口，否则回退本地 JSON。"""
    runtime_load, _ = _runtime_snapshot_io()
    if runtime_load:
        try:
            return normalize_pool_snapshot(runtime_load())
        except Exception:
            pass

    if not POOL_STATE_PATH.exists():
        return {"entries": [], "metadata": {}, "updated_at": ""}
    try:
        with open(POOL_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return normalize_pool_snapshot(data)
    except Exception:
        return {"entries": [], "metadata": {}, "updated_at": ""}


def save_pool_snapshot(entries: list, metadata: dict | None = None) -> str:
    """写入结构化 pool snapshot；优先统一状态接口，否则落到本地 JSON。"""
    runtime_load, runtime_save = _runtime_snapshot_io()
    normalized_entries = [_normalize_entry(item) for item in entries or []]
    metadata = _json_safe(metadata or {})
    summary = {
        "core": sum(1 for item in normalized_entries if item.get("bucket") == "core"),
        "watch": sum(1 for item in normalized_entries if item.get("bucket") == "watch"),
        "avoid": sum(1 for item in normalized_entries if item.get("bucket") == "avoid"),
    }
    payload = {
        "entries": normalized_entries,
        "metadata": metadata,
        "updated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }
    payload["summary"] = summary

    if runtime_save:
        try:
            result = runtime_save(normalized_entries, metadata)
            if result:
                return str(result)
        except Exception:
            pass

    state = _load_state()
    state["updated_at"] = payload["updated_at"]
    state["last_eval_date"] = datetime.now().strftime("%Y-%m-%d")
    state["last_summary"] = summary
    state["entries"] = normalized_entries
    state["metadata"] = metadata
    state["codes"] = {
        item["code"]: {
            "name": item["name"],
            "last_date": state["last_eval_date"],
            "last_score": item["total_score"],
            "last_veto": item["veto_triggered"],
            "last_veto_signals": item["veto_signals"],
            "membership": item["bucket"],
            "updated_at": payload["updated_at"],
        }
        for item in normalized_entries
        if item.get("code")
    }
    return _save_state(state)


def _merge_snapshot_entries(results: list, current_snapshot: dict | None,
                            strategy_cfg: dict | None = None, source: str = "") -> tuple[list, dict]:
    """把最新评分结果合并到现有 snapshot，并输出结构化条目。"""
    current_snapshot = normalize_pool_snapshot(current_snapshot)
    strategy_cfg = strategy_cfg or get_strategy()
    pool_cfg = strategy_cfg.get("pool_management", {})
    watch_min_score = float(pool_cfg.get("watch_min_score", 5))
    promote_min_score = float(pool_cfg.get("promote_min_score", 7))

    current_entries = current_snapshot.get("entries", [])
    merged = {str(item.get("code", "")).strip(): dict(item) for item in current_entries if str(item.get("code", "")).strip()}

    for row in results or []:
        code = str(row.get("code", "")).strip()
        if not code:
            continue
        score = _safe_score(row.get("total_score", 0))
        veto_signals = list(row.get("veto_signals", []) or [])
        hard_veto, warning_signals = split_veto_signals(veto_signals)
        bucket = _normalize_bucket(
            row.get("bucket"),
            score,
            bool(row.get("veto_triggered", False)) or bool(hard_veto),
            watch_min_score,
            promote_min_score,
        )
        previous_bucket = merged.get(code, {}).get("bucket", "")
        note = _build_note(hard_veto, warning_signals, bucket, str(row.get("note", "") or "").strip())
        entry = {
            "bucket": bucket,
            "code": code,
            "name": str(row.get("name", "")).strip(),
            "total_score": round(score, 1),
            "technical_score": _safe_score(row.get("technical_score", 0)),
            "fundamental_score": _safe_score(row.get("fundamental_score", 0)),
            "flow_score": _safe_score(row.get("flow_score", 0)),
            "sentiment_score": _safe_score(row.get("sentiment_score", 0)),
            "veto_triggered": bool(row.get("veto_triggered", False)) or bool(hard_veto),
            "veto_signals": veto_signals,
            "warning_signals": list(row.get("warning_signals", [])) if row.get("warning_signals") else warning_signals,
            "hard_veto_signals": list(row.get("hard_veto_signals", [])) if row.get("hard_veto_signals") else hard_veto,
            "note": note,
            "source": str(row.get("source", source) or source),
            "previous_bucket": previous_bucket,
        }
        merged[code] = entry

    entries = sorted(
        merged.values(),
        key=lambda item: (
            {"core": 0, "watch": 1, "avoid": 2}.get(item.get("bucket", "avoid"), 3),
            -_safe_score(item.get("total_score", 0)),
            item.get("code", ""),
        ),
    )
    metadata = {
        "source": source,
        "summary": {
            "core": sum(1 for item in entries if item.get("bucket") == "core"),
            "watch": sum(1 for item in entries if item.get("bucket") == "watch"),
            "avoid": sum(1 for item in entries if item.get("bucket") == "avoid"),
        },
        "current_snapshot_summary": current_snapshot.get("metadata", {}).get("summary", {}),
        "current_snapshot_updated_at": current_snapshot.get("updated_at", ""),
    }
    return entries, metadata


def build_pool_snapshot_entries(results: list, current_snapshot: dict | None = None,
                                strategy_cfg: dict | None = None, source: str = "") -> tuple[list, dict]:
    """对外的 snapshot 合并入口。"""
    return _merge_snapshot_entries(results, current_snapshot, strategy_cfg, source)


def evaluate_pool_actions(results: list, stocks_cfg: dict | None, strategy_cfg: dict | None = None,
                          current_snapshot: dict | None = None, source: str = "") -> tuple[dict, dict]:
    """
    基于当前结果和历史状态生成严格池子建议。

    Returns:
        (suggestions, metadata)
    """
    strategy_cfg = strategy_cfg or get_strategy()
    pool_cfg = strategy_cfg.get("pool_management", {})

    watch_min_score = float(pool_cfg.get("watch_min_score", 5))
    promote_min_score = float(pool_cfg.get("promote_min_score", 7))
    promote_streak_days = int(pool_cfg.get("promote_streak_days", 2))
    demote_max_score = float(pool_cfg.get("demote_max_score", 5))
    demote_streak_days = int(pool_cfg.get("demote_streak_days", 2))
    remove_max_score = float(pool_cfg.get("remove_max_score", 4))
    remove_streak_days = int(pool_cfg.get("remove_streak_days", 2))
    veto_immediate_demote = bool(pool_cfg.get("veto_immediate_demote", True))
    add_to_watch_streak_days = int(pool_cfg.get("add_to_watch_streak_days", 1))

    current_snapshot = normalize_pool_snapshot(current_snapshot or load_pool_snapshot())
    core_codes, watch_codes = _pool_membership_maps(current_snapshot, stocks_cfg or {})

    state = _load_state()
    code_state = state.setdefault("codes", {})
    today = datetime.now().strftime("%Y-%m-%d")

    suggestions = {
        "promote_to_core": [],
        "keep_watch": [],
        "add_to_watch": [],
        "demote_from_core": [],
        "remove_or_avoid": [],
    }

    for row in results:
        code = str(row.get("code", "")).strip()
        name = str(row.get("name", "")).strip()
        if not code:
            continue

        score = _safe_float(row.get("total_score", 0))
        veto_signals = row.get("veto_signals", []) or []
        hard_veto, warning_signals = split_veto_signals(veto_signals)
        veto = bool(row.get("veto_triggered", False)) or bool(hard_veto)

        prev = code_state.get(code, {})
        if prev.get("last_date") == today:
            high_streak = int(prev.get("high_streak", 0))
            low_streak = int(prev.get("low_streak", 0))
            watch_streak = int(prev.get("watch_streak", 0))
            veto_streak = int(prev.get("veto_streak", 0))
        else:
            high_streak = int(prev.get("high_streak", 0)) + 1 if score >= promote_min_score and not veto else 0
            low_streak = int(prev.get("low_streak", 0)) + 1 if score < demote_max_score or veto else 0
            watch_streak = int(prev.get("watch_streak", 0)) + 1 if score >= watch_min_score and not veto else 0
            veto_streak = int(prev.get("veto_streak", 0)) + 1 if veto else 0

        membership = "core" if code in core_codes else "watch" if code in watch_codes else "other"
        code_state[code] = {
            "name": name,
            "last_date": today,
            "last_score": round(score, 2),
            "last_veto": veto,
            "last_veto_signals": veto_signals,
            "membership": membership,
            "high_streak": high_streak,
            "low_streak": low_streak,
            "watch_streak": watch_streak,
            "veto_streak": veto_streak,
            "updated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        }

        def entry(reason: str) -> dict:
            return {
                "code": code,
                "name": name,
                "score": round(score, 1),
                "reason": reason,
                "high_streak": high_streak,
                "low_streak": low_streak,
                "watch_streak": watch_streak,
                "veto_streak": veto_streak,
            }

        if membership == "core":
            if veto and veto_immediate_demote:
                reason = f"触发 veto: {','.join(hard_veto) or 'unknown'}" if hard_veto else f"预警: {','.join(warning_signals) or 'unknown'}"
                suggestions["demote_from_core"].append(entry(reason))
            elif low_streak >= demote_streak_days:
                suggestions["demote_from_core"].append(entry(f"连续{low_streak}天分数<{demote_max_score:.1f}"))
            elif score >= watch_min_score and not veto:
                suggestions["keep_watch"].append(entry("核心池保留观察"))
            else:
                suggestions["remove_or_avoid"].append(entry("核心池暂不加仓"))
        elif membership == "watch":
            if not veto and score >= promote_min_score and high_streak >= promote_streak_days:
                suggestions["promote_to_core"].append(entry(f"连续{high_streak}天分数>={promote_min_score:.1f}"))
            elif veto or low_streak >= remove_streak_days or score < remove_max_score:
                if veto and hard_veto:
                    reason = f"触发 veto: {','.join(hard_veto)}"
                elif warning_signals:
                    reason = f"预警: {','.join(warning_signals)}"
                else:
                    reason = f"连续{low_streak}天分数<{demote_max_score:.1f}"
                suggestions["remove_or_avoid"].append(entry(reason))
            else:
                suggestions["keep_watch"].append(entry("观察池继续跟踪"))
        else:
            if not veto and score >= watch_min_score and watch_streak >= add_to_watch_streak_days:
                suggestions["add_to_watch"].append(entry(f"连续{watch_streak}天分数>={watch_min_score:.1f}"))
            else:
                suggestions["remove_or_avoid"].append(entry("暂不纳入池子"))

    snapshot_entries, snapshot_meta = build_pool_snapshot_entries(
        results,
        current_snapshot=current_snapshot,
        strategy_cfg=strategy_cfg,
        source=source or "pool_manager",
    )

    state["updated_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    state["last_eval_date"] = today
    state["last_summary"] = {
        key: len(value)
        for key, value in suggestions.items()
    }
    state["entries"] = snapshot_entries
    state["snapshot_meta"] = snapshot_meta
    state_path = _save_state(state)

    metadata = {
        "state_path": state_path,
        "summary": state["last_summary"],
        "snapshot_entries": snapshot_entries,
        "snapshot_summary": snapshot_meta.get("summary", {}),
        "snapshot_meta": snapshot_meta,
        "rules": {
            "watch_min_score": watch_min_score,
            "promote_min_score": promote_min_score,
            "promote_streak_days": promote_streak_days,
            "demote_max_score": demote_max_score,
            "demote_streak_days": demote_streak_days,
            "remove_max_score": remove_max_score,
            "remove_streak_days": remove_streak_days,
            "veto_immediate_demote": veto_immediate_demote,
            "add_to_watch_streak_days": add_to_watch_streak_days,
        },
    }
    return suggestions, metadata
