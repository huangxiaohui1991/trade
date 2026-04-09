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


POOL_STATE_PATH = RUNTIME_DIR / "pool_state.json"


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


def evaluate_pool_actions(results: list, stocks_cfg: dict, strategy_cfg: dict | None = None) -> tuple[dict, dict]:
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

    core_codes = {str(item.get("code", "")).strip() for item in stocks_cfg.get("core_pool", [])}
    watch_codes = {str(item.get("code", "")).strip() for item in stocks_cfg.get("watch_pool", [])}

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
        veto = bool(row.get("veto_triggered", False))
        veto_signals = row.get("veto_signals", []) or []

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
                suggestions["demote_from_core"].append(entry(f"触发 veto: {','.join(veto_signals) or 'unknown'}"))
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
                reason = f"触发 veto: {','.join(veto_signals)}" if veto else f"连续{low_streak}天分数<{demote_max_score:.1f}"
                suggestions["remove_or_avoid"].append(entry(reason))
            else:
                suggestions["keep_watch"].append(entry("观察池继续跟踪"))
        else:
            if not veto and score >= watch_min_score and watch_streak >= add_to_watch_streak_days:
                suggestions["add_to_watch"].append(entry(f"连续{watch_streak}天分数>={watch_min_score:.1f}"))
            else:
                suggestions["remove_or_avoid"].append(entry("暂不纳入池子"))

    state["updated_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    state["last_eval_date"] = today
    state["last_summary"] = {
        key: len(value)
        for key, value in suggestions.items()
    }
    state_path = _save_state(state)

    metadata = {
        "state_path": state_path,
        "summary": state["last_summary"],
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
