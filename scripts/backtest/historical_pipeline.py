"""
backtest/historical_pipeline.py — 真实策略历史回填引擎

职责：
  把 601869（或其他股票）在指定时间区间内的每日行情数据
  → 按真实策略逻辑计算大盘信号 + 技术面评分 + veto 检查
  → 生成与 run_strategy_replay 兼容的 daily_data fixture

用法：
  python -c "
    from scripts.backtest.historical_pipeline import build_replay_fixture
    fixture = build_replay_fixture(
        stock_code='601869',
        start='2025-04-10',
        end='2026-04-10',
        index_code='000001',      # 上证指数
    )
    # 写入 fixture 文件供 strategy_replay 消费
  "
"""

from __future__ import annotations

import atexit
from collections import Counter, defaultdict
import contextlib
import io
import json
import math
import re
import statistics
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import baostock as bs
import pandas as pd

from scripts.engine.stock_classifier import classify_style
from scripts.state import (
    load_candidate_snapshot_history,
    load_daily_signal_snapshot_bundle,
    load_decision_snapshot_history,
    load_market_snapshot_history,
    load_pool_snapshot_history,
)
from scripts.state.reason_codes import veto_reason_to_label
from scripts.utils.config_loader import get_strategy


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

_FETCH_DAILY_CACHE: dict[tuple[str, str, str, str], pd.DataFrame] = {}
_BS_SESSION_OPEN = False


def _ensure_bs_session() -> None:
    global _BS_SESSION_OPEN
    if _BS_SESSION_OPEN:
        return
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        lg = bs.login()
    assert lg.error_code == "0", lg.error_msg
    _BS_SESSION_OPEN = True


def _close_bs_session() -> None:
    global _BS_SESSION_OPEN
    if not _BS_SESSION_OPEN:
        return
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            bs.logout()
    finally:
        _BS_SESSION_OPEN = False


atexit.register(_close_bs_session)


def _ensure_bs_code(code: str) -> str:
    """规范化为 baostock 格式"""
    c = code.strip().lower().replace(".sh", "").replace(".sz", "")
    if c.startswith("sh") or c.startswith("sz"):
        return c
    if c.startswith("6"):
        return f"sh.{c}"
    return f"sz.{c}"


def _to_bs_code(std_code: str) -> str:
    """个股标准码 → baostock 格式"""
    return _ensure_bs_code(std_code)


def _fetch_daily(
    bs_code: str,
    start: str,
    end: str,
    fields: str = "date,open,high,low,close,volume,amount,turn",
) -> pd.DataFrame:
    """拉取日线数据（前复权）"""
    cache_key = (bs_code, start, end, fields)
    cached = _FETCH_DAILY_CACHE.get(cache_key)
    if cached is not None:
        return cached.copy()

    _ensure_bs_session()
    rs = bs.query_history_k_data_plus(
        bs_code, fields,
        start_date=start, end_date=end,
        frequency="d", adjustflag="2",
    )
    assert rs.error_code == "0", rs.error_msg
    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    df = pd.DataFrame(rows, columns=rs.fields)

    for col in ["open", "high", "low", "close", "volume", "amount", "turn"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    _FETCH_DAILY_CACHE[cache_key] = df
    return df.copy()


# ---------------------------------------------------------------------------
# 大盘信号计算（与 market_timer.py 逻辑一致）
# ---------------------------------------------------------------------------

def compute_market_signal(df_index: pd.DataFrame) -> pd.DataFrame:
    """
    输入指数日线，追加 signal 列。
    GREEN: 连续3日收盘 > MA20
    RED:   连续5日收盘 < MA20
    CLEAR: 连续15日收盘 < MA60
    YELLOW: 其他
    """
    GREEN_DAYS = 3
    RED_DAYS = 5
    CLEAR_DAYS = 15

    df = df_index.copy()
    df["MA20"] = df["close"].rolling(20).mean()
    df["MA60"] = df["close"].rolling(60).mean()

    # 是否在 MA20 上方
    df["above_ma20"] = df["close"] > df["MA20"]
    df["below_ma20"] = df["close"] < df["MA20"]
    df["below_ma60"] = df["close"] < df["MA60"]

    # 连续 N 日计数（逐行累加）
    def rolling_count(series: pd.Series, min_periods: int) -> pd.Series:
        counts = []
        count = 0
        for val in series:
            if val:
                count += 1
            else:
                count = 0
            counts.append(count)
        return pd.Series(counts, index=series.index)

    df["conseq_above_20"] = rolling_count(df["above_ma20"], GREEN_DAYS)
    df["conseq_below_20"] = rolling_count(df["below_ma20"], RED_DAYS)
    df["conseq_below_60"] = rolling_count(df["below_ma60"], CLEAR_DAYS)

    def row_signal(row):
        if row["conseq_below_60"] >= CLEAR_DAYS:
            return "CLEAR"
        if row["conseq_below_20"] >= RED_DAYS:
            return "RED"
        if row["conseq_above_20"] >= GREEN_DAYS:
            return "GREEN"
        return "YELLOW"

    df["signal"] = df.apply(row_signal, axis=1)
    return df[["date", "close", "MA20", "MA60", "signal"]]


_SYSTEM_INDEX_CODES = ["000001", "399001", "399006", "000688"]


def _resolve_strategy_params(overrides: dict | None = None) -> dict:
    """Flatten strategy.yaml into replay params, then apply preset + caller overrides."""
    strategy = get_strategy()
    scoring = strategy.get("scoring", {})
    weights = scoring.get("weights", {})
    thresholds = scoring.get("thresholds", {})
    risk = strategy.get("risk", {})
    entry_cfg = strategy.get("entry_signal", {})
    position = risk.get("position", {})
    portfolio = risk.get("portfolio", {})
    momentum = risk.get("momentum", {})
    slow_bull = risk.get("slow_bull", {})
    overrides = dict(overrides or {})
    preset_name = str(overrides.pop("preset", "")).strip()
    presets = strategy.get("backtest_presets", {}) or {}

    params = {
        "use_system_strategy": True,
        "require_entry_signal": True,
        "entry_mode": entry_cfg.get("entry_mode", "hybrid"),
        "entry_volume_ratio_min": entry_cfg.get("volume_ratio_min", 1.5),
        "entry_rsi_max": entry_cfg.get("rsi_max", 70),
        "entry_trend_rsi_max": entry_cfg.get("rsi_max", 70),
        "entry_pullback_rsi_max": entry_cfg.get("rsi_max", 70),
        "entry_pullback_volume_ratio_min": entry_cfg.get("volume_ratio_min", 1.5),
        "entry_deviation_max": entry_cfg.get("deviation_max", 999.0),
        "entry_require_bullish_candle": bool(entry_cfg.get("require_bullish_candle", False)),
        "buy_threshold": thresholds.get("buy", 7),
        "watch_threshold": thresholds.get("watch", 5),
        "reject_threshold": thresholds.get("reject", 4),
        "total_max": position.get("total_max", 0.60),
        "single_max": position.get("single_max", 0.20),
        "weekly_max": position.get("weekly_max", 2),
        "holding_max": position.get("holding_max", 4),
        "consecutive_loss_days_limit": portfolio.get("consecutive_loss_days_limit", 0),
        "cooldown_days": portfolio.get("cooldown_days", 0),
        "veto_rules": scoring.get("veto", [
            "below_ma20", "limit_up_today", "consecutive_outflow",
            "red_market", "earnings_bomb", "ma20_trend_down",
        ]),
        "technical_weight": weights.get("technical", 3),
        "fundamental_weight": weights.get("fundamental", 2),
        "flow_weight": weights.get("flow", 2),
        "sentiment_weight": weights.get("sentiment", 3),
        "technical_denom": 3,
        "fundamental_denom": 2,
        "flow_denom": 2,
        "sentiment_denom": 3,
        "momentum_stop_loss": momentum.get("stop_loss", 0.05),
        "momentum_trailing_stop": momentum.get("trailing_stop", 0.08),
        "momentum_time_stop_days": momentum.get("time_stop_days", 10),
        "momentum_exit_ma": momentum.get("exit_ma", 20),
        "slow_bull_stop_loss": slow_bull.get("stop_loss", 0.08),
        "slow_bull_time_stop_days": slow_bull.get("time_stop_days", 30),
        "slow_bull_exit_ma": slow_bull.get("exit_ma", 20),
        "slow_bull_absolute_stop_ma": slow_bull.get("absolute_stop_ma", 60),
        # Legacy replay fallback values.
        "stop_loss": momentum.get("stop_loss", 0.05),
        "take_profit": momentum.get("trailing_stop", 0.08),
        "time_stop_days": momentum.get("time_stop_days", 10),
    }

    preset_values: dict[str, Any] = {}
    preset_sets_entry_requirement = False
    if preset_name:
        preset = presets.get(preset_name)
        if not isinstance(preset, dict):
            available = ", ".join(sorted(str(name) for name in presets)) or "<none>"
            raise ValueError(f"Unknown backtest preset '{preset_name}'. Available presets: {available}")
        preset_values = {key: value for key, value in preset.items() if key != "description"}
        preset_sets_entry_requirement = "require_entry_signal" in preset_values
        params["preset"] = preset_name
        params.update(preset_values)

    override_sets_entry_requirement = "require_entry_signal" in overrides
    params.update(overrides)

    if not preset_sets_entry_requirement and not override_sets_entry_requirement:
        params["require_entry_signal"] = str(params.get("entry_mode", entry_cfg.get("entry_mode", "hybrid"))).strip().lower() != "score_only"

    if "stop_loss" not in preset_values and "stop_loss" not in overrides:
        params["stop_loss"] = params.get("momentum_stop_loss", 0.05)
    if "take_profit" not in preset_values and "take_profit" not in overrides:
        params["take_profit"] = params.get("momentum_trailing_stop", 0.08)
    if "time_stop_days" not in preset_values and "time_stop_days" not in overrides:
        params["time_stop_days"] = params.get("momentum_time_stop_days", 10)
    return params


def _normalize_replay_code(code: str) -> str:
    value = str(code or "").strip().lower()
    for prefix in ("sh.", "sz."):
        if value.startswith(prefix):
            value = value[len(prefix):]
    for suffix in (".sh", ".sz"):
        if value.endswith(suffix):
            value = value[:-len(suffix)]
    return value


def _history_candidate_for_code(bundle: dict[str, Any], stock_code: str) -> dict[str, Any]:
    normalized_target = _normalize_replay_code(stock_code)
    for candidate in bundle.get("scored_candidates", []) or []:
        if _normalize_replay_code(candidate.get("code", "")) == normalized_target:
            return dict(candidate)
    return {}


def _proxy_candidate_snapshot(
    *,
    stock_code: str,
    row: pd.Series,
    tech: dict[str, Any],
    params: dict[str, Any],
    strategy: dict[str, Any],
    tech_score: float,
    fundamental_score: float,
    flow_score: float,
    sentiment_score: float,
    total_score: float,
    vetoes: list[str],
    entry_signal: bool,
    entry_reasons: list[str],
    code_name: str,
    df_stock: pd.DataFrame,
    idx: int,
) -> dict[str, Any]:
    close_price = float(row["close"])
    closes = [float(v) for v in df_stock.iloc[max(0, idx - 80):idx + 1]["close"].tolist()]
    style_info = classify_style(closes, rsi=tech.get("rsi"), strategy=strategy) if len(closes) >= 30 else {}
    return {
        "code": stock_code,
        "name": code_name,
        "score": total_score,
        "total_score": total_score,
        "price": round(close_price, 4),
        "technical_score": tech_score,
        "fundamental_score": fundamental_score,
        "flow_score": flow_score,
        "sentiment_score": sentiment_score,
        "veto_signals": list(vetoes),
        "entry_signal": entry_signal,
        "entry_reasons": list(entry_reasons),
        "entry_mode": params.get("entry_mode", "hybrid"),
        "golden_cross": tech.get("golden_cross", False),
        "close_above_prev": tech.get("close_above_prev", False),
        "deviation_pct": tech.get("deviation_pct"),
        "volume_ratio": round(float(tech.get("volume_ratio") or 0), 2),
        "rsi": round(float(tech["rsi"]), 1) if tech.get("rsi") is not None else None,
        "style": style_info.get("style", "momentum"),
        "style_confidence": style_info.get("confidence", 0),
        "ma20": round(float(row["MA20"]), 4) if not pd.isna(row.get("MA20")) else None,
        "ma5": round(float(row["MA5"]), 4) if not pd.isna(row.get("MA5")) else None,
        "ma10": round(float(row["MA10"]), 4) if not pd.isna(row.get("MA10")) else None,
        "ma60": round(float(row["MA60"]), 4) if not pd.isna(row.get("MA60")) else None,
        "low": round(float(row["low"]), 4),
        "high": round(float(row["high"]), 4),
        "turn": round(float(row["turn"]), 4) if not pd.isna(row.get("turn")) else 0,
        "snapshot_source": "proxy_replay",
    }


def _merge_history_candidate(
    proxy_candidate: dict[str, Any],
    history_candidate: dict[str, Any],
    *,
    history_group_id: str,
) -> dict[str, Any]:
    merged = dict(proxy_candidate)
    merged.update(history_candidate or {})

    raw_score = history_candidate.get(
        "score",
        history_candidate.get("total_score", merged.get("score", merged.get("total_score", proxy_candidate.get("score", 0)))),
    )
    try:
        normalized_score = round(float(raw_score or 0), 2)
    except Exception:
        normalized_score = float(proxy_candidate.get("score", 0) or 0)

    merged["score"] = normalized_score
    merged["total_score"] = normalized_score
    merged["name"] = str(merged.get("name", proxy_candidate.get("name", "")) or proxy_candidate.get("name", "")).strip()
    merged["price"] = round(float(merged.get("price", proxy_candidate.get("price", 0)) or proxy_candidate.get("price", 0) or 0), 4)
    merged["technical_score"] = round(float(merged.get("technical_score", proxy_candidate.get("technical_score", 0)) or 0), 4)
    merged["fundamental_score"] = round(float(merged.get("fundamental_score", proxy_candidate.get("fundamental_score", 0)) or 0), 4)
    merged["flow_score"] = round(float(merged.get("flow_score", proxy_candidate.get("flow_score", 0)) or 0), 4)
    merged["sentiment_score"] = round(float(merged.get("sentiment_score", proxy_candidate.get("sentiment_score", 0)) or 0), 4)
    merged["entry_signal"] = bool(merged.get("entry_signal", proxy_candidate.get("entry_signal", False)))
    if "entry_reasons" in merged and isinstance(merged.get("entry_reasons"), list):
        merged["entry_reasons"] = list(merged.get("entry_reasons") or [])
    elif merged["entry_signal"]:
        merged["entry_reasons"] = list(proxy_candidate.get("entry_reasons", []) or [])
    else:
        merged["entry_reasons"] = []
    merged["veto_signals"] = [str(item).strip() for item in merged.get("veto_signals", proxy_candidate.get("veto_signals", [])) or [] if str(item).strip()]
    merged["entry_mode"] = str(merged.get("entry_mode", proxy_candidate.get("entry_mode", "")) or proxy_candidate.get("entry_mode", "hybrid"))
    merged["style"] = str(merged.get("style", proxy_candidate.get("style", "")) or proxy_candidate.get("style", "momentum"))
    merged["style_confidence"] = float(merged.get("style_confidence", proxy_candidate.get("style_confidence", 0)) or 0)
    merged["snapshot_source"] = "history_signal_snapshot"
    merged["history_group_id"] = history_group_id
    return merged


def _summarize_data_fidelity(day_sources: list[dict[str, Any]]) -> dict[str, Any]:
    history_days = sum(1 for item in day_sources if item.get("source") == "history_signal_snapshot")
    proxy_days = sum(1 for item in day_sources if item.get("source") == "proxy_replay")
    history_candidate_hit_days = sum(1 for item in day_sources if item.get("source") == "history_signal_snapshot" and item.get("candidate_present"))
    history_candidate_absent_days = sum(1 for item in day_sources if item.get("source") == "history_signal_snapshot" and not item.get("candidate_present"))
    if history_days and not proxy_days:
        mode = "historical_signal_mirror"
    elif history_days:
        mode = "hybrid_signal_mirror"
    else:
        mode = "proxy_replay"
    return {
        "mode": mode,
        "history_days": history_days,
        "proxy_days": proxy_days,
        "history_candidate_hit_days": history_candidate_hit_days,
        "history_candidate_absent_days": history_candidate_absent_days,
        "day_sources": day_sources,
    }


def _entry_signal_for_mode(
    row: pd.Series,
    tech: dict[str, Any],
    params: dict[str, Any],
) -> tuple[bool, list[str]]:
    """Compute entry eligibility using the selected entry mode."""
    mode = str(params.get("entry_mode", "hybrid") or "hybrid").strip().lower()
    volume_ratio = float(tech.get("volume_ratio") or 0.0)
    rsi = tech.get("rsi")

    ma5 = float(row["MA5"]) if "MA5" in row and not pd.isna(row["MA5"]) else None
    ma10 = float(row["MA10"]) if "MA10" in row and not pd.isna(row["MA10"]) else None
    ma20 = float(row["MA20"]) if "MA20" in row and not pd.isna(row["MA20"]) else None
    ma60 = float(row["MA60"]) if "MA60" in row and not pd.isna(row["MA60"]) else None
    close = float(row["close"])

    golden_cross = tech.get("golden_cross", False)
    close_above_prev = tech.get("close_above_prev", False)
    deviation_pct = tech.get("deviation_pct")

    # 金叉收阳线：增强版要求 golden_cross 且今日收阳
    require_bullish = params.get("entry_require_bullish_candle", False)
    golden_cross_ok = golden_cross and (not require_bullish or close_above_prev)

    # 乖离率过滤：超过阈值则不入
    deviation_max = params.get("entry_deviation_max", 999.0)
    deviation_ok = deviation_pct is None or deviation_pct < deviation_max

    strict_entry = bool(
        golden_cross_ok
        and deviation_ok
        and volume_ratio >= float(params.get("entry_volume_ratio_min", 1.5) or 1.5)
        and rsi is not None
        and float(rsi) < float(params.get("entry_rsi_max", 70) or 70)
    )
    trend_follow_entry = bool(
        rsi is not None
        and ma10 is not None
        and ma20 is not None
        and ma60 is not None
        and ma10 > ma20 > ma60
        and deviation_ok
        and float(rsi) < float(params.get("entry_trend_rsi_max", 70) or 70)
    )
    pullback_entry = bool(
        rsi is not None
        and ma5 is not None
        and ma10 is not None
        and ma20 is not None
        and close >= ma20
        and ma5 > ma10 > ma20
        and deviation_ok
        and volume_ratio >= float(params.get("entry_pullback_volume_ratio_min", 1.5) or 1.5)
        and float(rsi) < float(params.get("entry_pullback_rsi_max", 70) or 70)
    )

    if mode == "score_only":
        return True, ["score_only"]
    if mode == "trend_follow":
        return trend_follow_entry, ["trend_follow"] if trend_follow_entry else []
    if mode == "hybrid":
        reasons: list[str] = []
        if strict_entry:
            reasons.append("strict")
        if trend_follow_entry:
            reasons.append("trend_follow")
        if pullback_entry:
            reasons.append("pullback")
        return bool(reasons), reasons
    return strict_entry, ["strict"] if strict_entry else []


def _composite_market_signal(
    start: str,
    end: str,
    *,
    warmup_start: str,
) -> pd.DataFrame:
    """Compute the historical market signal with the same multi-index voting idea as MarketTimer."""
    signal_frames = []
    close_frames = []
    sh_amount_frames = []
    sz_amount_frames = []
    for code in _SYSTEM_INDEX_CODES:
        df_raw = _fetch_daily(_to_bs_code(code), warmup_start, end)
        df_signal = compute_market_signal(df_raw).rename(columns={"signal": f"signal_{code}", "close": f"close_{code}"})
        signal_frames.append(df_signal[["date", f"signal_{code}"]])
        close_frames.append(df_signal[["date", f"close_{code}"]])
        # 收集上证和深证的成交额用于两市成交额过滤
        if code == "000001" and "amount" in df_raw.columns:
            sh_amount_frames.append(df_signal[["date"]].assign(amount=df_raw["amount"]))
        if code == "399001" and "amount" in df_raw.columns:
            sz_amount_frames.append(df_signal[["date"]].assign(amount=df_raw["amount"]))

    merged = signal_frames[0]
    for frame in signal_frames[1:]:
        merged = merged.merge(frame, on="date", how="outer")
    for frame in close_frames:
        merged = merged.merge(frame, on="date", how="left")
    # 合并上证成交额
    if sh_amount_frames:
        sh_df = sh_amount_frames[0].rename(columns={"amount": "amount_000001"})
        merged = merged.merge(sh_df[["date", "amount_000001"]], on="date", how="left")
    # 合并深成成交额
    if sz_amount_frames:
        sz_df = sz_amount_frames[0].rename(columns={"amount": "amount_399001"})
        merged = merged.merge(sz_df[["date", "amount_399001"]], on="date", how="left")
    merged = merged.sort_values("date").reset_index(drop=True)

    signal_cols = [f"signal_{code}" for code in _SYSTEM_INDEX_CODES]

    def row_signal(row):
        values = [str(row[col]) for col in signal_cols if pd.notna(row.get(col))]
        if not values:
            return "CLEAR"
        clear_pct = sum(1 for value in values if value == "CLEAR") / len(values)
        green_pct = sum(1 for value in values if value == "GREEN") / len(values)
        if clear_pct >= 0.6:
            return "CLEAR"
        if green_pct >= 0.6:
            return "GREEN"
        if green_pct >= 0.3:
            return "YELLOW"
        return "RED"

    merged["signal"] = merged.apply(row_signal, axis=1)
    merged["close"] = merged.get("close_000001")

    # 两市成交额 = 上证成交额 + 深成成交额（单位万元，baostock 的 AMOUNT 单位就是元）
    if "amount_000001" in merged.columns and "amount_399001" in merged.columns:
        merged["total_amount"] = merged["amount_000001"].fillna(0) + merged["amount_399001"].fillna(0)
    else:
        merged["total_amount"] = None

    return merged[["date", "close", "signal", "total_amount"]]


# ---------------------------------------------------------------------------
# 技术面评分（与 scorer.py / technical.py 逻辑对齐）
# ---------------------------------------------------------------------------

def compute_technical_score(
    df_stock: pd.DataFrame,
    row: pd.Series,
    idx: int,
) -> dict:
    """
    根据当日及历史数据计算技术面评分（满分 3 分）。
    V1.0 策略因子：
      - 金叉信号（MA10 上穿 MA20）：+1 分
      - 量比 > 1.5：+0.5 分
      - RSI < 70（不过热）：+0.5 分
      - 均线多头排列（MA5>MA10>MA20）：+0.5 分
      - 动量（近5日涨幅 > 3%）：+0.5 分
    """
    close = float(row["close"])
    ma = {}
    lookback = df_stock.iloc[max(0, idx - 60):idx + 1]
    for window in [5, 10, 20, 60]:
        if len(lookback) >= window:
            ma[f"MA{window}"] = lookback["close"].iloc[-window:].mean()
        else:
            ma[f"MA{window}"] = close

    # 金叉：前一日 MA10 <= MA20，当日 MA10 > MA20
    score = 0.0
    details = []

    if idx >= 1:
        prev_ma10 = (
            df_stock.iloc[idx - 10:idx]["close"].mean()
            if idx >= 10
            else float(df_stock.iloc[idx - 1]["close"])
        )
        prev_ma20 = (
            df_stock.iloc[idx - 20:idx]["close"].mean()
            if idx >= 20
            else float(df_stock.iloc[idx - 1]["close"])
        )
        curr_ma10 = ma["MA10"]
        curr_ma20 = ma["MA20"]
        if prev_ma10 <= prev_ma20 and curr_ma10 > curr_ma20:
            score += 1.0
            details.append("golden_cross")

    # 量比
    if idx >= 4:
        avg_vol5 = df_stock.iloc[idx - 4:idx + 1]["volume"].mean()
        today_vol = float(row["volume"])
        vol_ratio = today_vol / avg_vol5 if avg_vol5 > 0 else 0
        if vol_ratio >= 1.5:
            score += 0.5
            details.append(f"vol_ratio={vol_ratio:.2f}")

    # RSI (14日)
    rsi = _compute_rsi(df_stock, idx, period=14)
    if rsi is not None and rsi < 70:
        score += 0.5
        details.append(f"rsi={rsi:.1f}")

    # 均线多头排列
    if ma["MA5"] > ma["MA10"] > ma["MA20"]:
        score += 0.5
        details.append("ma_bullish")

    # 动量：近5日涨幅
    if idx >= 4:
        price_5d_ago = float(df_stock.iloc[idx - 4]["close"])
        momentum = (close - price_5d_ago) / price_5d_ago if price_5d_ago > 0 else 0
        if momentum > 0.03:
            score += 0.5
            details.append(f"momentum={momentum*100:.1f}%")

    # 收阳线：今日收盘 > 昨日收盘
    close_above_prev = False
    if idx >= 1:
        prev_close = float(df_stock.iloc[idx - 1]["close"])
        close_above_prev = close > prev_close

    # 乖离率：(收盘价 - MA20) / MA20 * 100
    deviation_pct = None
    if ma["MA20"] and ma["MA20"] > 0:
        deviation_pct = round((close - ma["MA20"]) / ma["MA20"] * 100, 2)

    return {
        "score": round(score, 2),
        "details": details,
        "technical_score": round(score, 2),
        "golden_cross": "golden_cross" in details,
        "volume_ratio": next(
            (float(item.split("=", 1)[1]) for item in details if item.startswith("vol_ratio=")),
            0.0,
        ),
        "rsi": rsi,
        "close_above_prev": close_above_prev,
        "deviation_pct": deviation_pct,
    }


def _compute_rsi(df: pd.DataFrame, idx: int, period: int = 14) -> float | None:
    if idx < period:
        return None
    window = df.iloc[idx - period:idx]["close"]
    deltas = window.diff().dropna()
    if deltas.empty:
        return None
    gains = deltas.clip(lower=0).mean()
    losses = (-deltas.clip(upper=0)).mean()
    if losses == 0:
        return 100.0
    rs = gains / losses
    return 100 - (100 / (1 + rs))


# ---------------------------------------------------------------------------
# Veto 检查（与 strategy.yaml v2 一致）
# ---------------------------------------------------------------------------

def check_veto(
    row: pd.Series,
    idx: int,
    df_stock: pd.DataFrame,
    market_signal: str,
    ma20_history: list,
    total_market_amount: float | None = None,
) -> list[str]:
    """
    返回触发的 veto 列表。无 veto = 可以买入。
    """
    vetoes: list[str] = []
    close = float(row["close"])

    # 1. below_ma20：收盘价 < MA20
    if idx >= 20:
        ma20 = float(row["MA20"]) if "MA20" in row and not pd.isna(row["MA20"]) else None
        if ma20 and close < ma20:
            vetoes.append("below_ma20")

    # 2. red_market：大盘信号 RED 或 CLEAR
    if market_signal in ("RED", "CLEAR"):
        vetoes.append("red_market")

    # 3. limit_up_today：涨停（涨幅 > 9.5%）
    if idx >= 1:
        prev_close = float(df_stock.iloc[idx - 1]["close"])
        if prev_close > 0:
            change = (close - prev_close) / prev_close
            if change > 0.095:
                vetoes.append("limit_up_today")

    # 4. ma20_trend_down：MA20 斜率向下（近10日）
    if len(ma20_history) >= 10:
        ma20_slice = ma20_history[-10:]
        if len(ma20_slice) >= 2 and ma20_slice[-1] < ma20_slice[0]:
            vetoes.append("ma20_trend_down")

    # 5. consecutive_outflow：近3日成交量连续萎缩
    if idx >= 3:
        vols = [float(df_stock.iloc[idx - i]["volume"]) for i in range(3)]
        if vols[0] < vols[1] < vols[2]:
            vetoes.append("consecutive_outflow")

    # 6. 两市成交额不足（大盘择时增强版：两市成交 > 6000亿，baostock AMOUNT 单位是元）
    # 6000亿 = 6000 * 1亿 = 6000 * 100,000,000 = 600,000,000,000 元
    if total_market_amount is not None:
        min_amount = 600_000_000_000  # 6000亿元
        if total_market_amount < min_amount:
            vetoes.append("insufficient_market_amount")

    return vetoes


# ---------------------------------------------------------------------------
# 完整 daily_data 生成
# ---------------------------------------------------------------------------

def build_replay_fixture(
    stock_code: str,
    start: str,
    end: str,
    index_code: str = "000001",
    total_capital: float = 450286.0,
    strategy_params: dict | None = None,
    use_history_snapshots: bool = True,
) -> dict:
    """
    生成完整的 replay fixture。

    Returns:
        dict with keys: daily_data, total_capital, params, strategy_snapshot
    """
    params = _resolve_strategy_params(strategy_params)
    use_history_snapshots = bool(params.get("use_history_snapshots", use_history_snapshots))
    strategy = get_strategy()

    bs_code = _to_bs_code(stock_code)
    bs_index = _to_bs_code(index_code)

    # 预拉额外60日数据用于计算MA等指标
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    warmup_start = (start_dt - timedelta(days=90)).strftime("%Y-%m-%d")

    # 拉指数数据。index_code="system"/"composite" 时按当前系统多指数投票。
    if str(index_code).lower() in {"system", "composite", "all"}:
        df_index = _composite_market_signal(start, end, warmup_start=warmup_start)
    else:
        df_index_raw = _fetch_daily(bs_index, warmup_start, end)
        df_index = compute_market_signal(df_index_raw)
    if "total_amount" not in df_index.columns:
        df_index = df_index.copy()
        df_index["total_amount"] = None

    # 拉个股数据
    df_stock = _fetch_daily(bs_code, warmup_start, end)

    # 预计算个股 MA20 序列
    df_stock["MA20"] = df_stock["close"].rolling(20).mean()
    df_stock["MA5"] = df_stock["close"].rolling(5).mean()
    df_stock["MA10"] = df_stock["close"].rolling(10).mean()
    df_stock["MA60"] = df_stock["close"].rolling(60).mean()

    # 合并信号：个股日期在指数上对齐
    index_map = df_index.set_index("date")[["signal", "close", "total_amount"]].to_dict("index")

    daily_data: dict[str, dict] = {}
    day_sources: list[dict[str, Any]] = []
    code_name = str(stock_code).strip()

    for idx, row in df_stock.iterrows():
        day_str = row["date"].strftime("%Y-%m-%d")

        # 跳过回测区间外
        if day_str < start:
            continue
        if day_str > end:
            break

        idx_info = index_map.get(row["date"], {})
        market_signal = idx_info.get("signal", "GREEN")
        index_close = idx_info.get("close", row["close"])
        total_market_amount = idx_info.get("total_amount")

        # 技术评分
        tech = compute_technical_score(df_stock, row, idx)
        tech_score = tech["technical_score"]
        entry_signal, entry_reasons = _entry_signal_for_mode(row, tech, params)

        # Veto
        ma20_hist = list(df_stock["MA20"].iloc[max(0, idx - 20):idx + 1])
        vetoes = check_veto(row, idx, df_stock, market_signal, ma20_hist, total_market_amount=total_market_amount)

        # 综合评分：技术3 + 资金2 + 舆情3 + 基本面2 简化
        # 由于历史回测无法重建资金流/舆情，用技术面代理 + 固定基础分
        # 真实策略中基本面/舆情在实盘中起作用，这里做合理近似
        flow_score = 2.0  # 近似：量比高则加分
        sentiment_score = 1.5  # 无法回测，设为中性
        fundamental_score = 1.0  # 无法重建季度财务快照，设为中性

        weights = {
            "technical": params.get("technical_weight", 3),
            "fundamental": params.get("fundamental_weight", 2),
            "flow": params.get("flow_weight", 2),
            "sentiment": params.get("sentiment_weight", 3),
        }
        denoms = {
            "technical": params.get("technical_denom", 3),
            "fundamental": params.get("fundamental_denom", 2),
            "flow": params.get("flow_denom", 2),
            "sentiment": params.get("sentiment_denom", 3),
        }

        total_score = round(
            tech_score * weights["technical"] / denoms["technical"]
            + fundamental_score * weights["fundamental"] / denoms["fundamental"]
            + flow_score * weights["flow"] / denoms["flow"]
            + sentiment_score * weights["sentiment"] / denoms["sentiment"],
            2,
        )
        proxy_candidate = _proxy_candidate_snapshot(
            stock_code=stock_code,
            row=row,
            tech=tech,
            params=params,
            strategy=strategy,
            tech_score=tech_score,
            fundamental_score=fundamental_score,
            flow_score=flow_score,
            sentiment_score=sentiment_score,
            total_score=total_score,
            vetoes=vetoes,
            entry_signal=entry_signal,
            entry_reasons=entry_reasons,
            code_name=code_name,
            df_stock=df_stock,
            idx=idx,
        )
        close_price = float(row["close"])
        history_bundle: dict[str, Any] = {}
        history_market = {}
        history_candidate_snapshot = {}
        history_candidate = {}
        history_group_id = ""
        source = "proxy_replay"
        missing_components: list[str] = []

        if use_history_snapshots:
            history_bundle = load_daily_signal_snapshot_bundle(day_str)
            history_group_id = str(history_bundle.get("history_group_id", "")).strip()
            history_market = history_bundle.get("market_snapshot", {}) if isinstance(history_bundle.get("market_snapshot", {}), dict) else {}
            history_candidate_snapshot = history_bundle.get("candidate_snapshot", {}) if isinstance(history_bundle.get("candidate_snapshot", {}), dict) else {}
            history_candidate = _history_candidate_for_code(history_bundle, stock_code) if history_candidate_snapshot else {}
            if history_market and history_candidate_snapshot:
                market_signal = str(history_market.get("signal", market_signal) or market_signal).upper()
                source = "history_signal_snapshot"
            else:
                if not history_market:
                    missing_components.append("market_snapshot")
                if not history_candidate_snapshot:
                    missing_components.append("candidate_snapshot")

        if history_market and source == "history_signal_snapshot":
            market_signal = str(history_market.get("signal", market_signal) or market_signal).upper()

        candidates: list[dict[str, Any]]
        if source == "history_signal_snapshot":
            if history_candidate:
                candidates = [
                    _merge_history_candidate(
                        proxy_candidate,
                        history_candidate,
                        history_group_id=history_group_id,
                    )
                ]
            else:
                candidates = []
        else:
            candidates = [proxy_candidate]

        day_sources.append({
            "date": day_str,
            "source": source,
            "history_group_id": history_group_id,
            "candidate_present": bool(history_candidate),
            "missing_components": missing_components,
        })

        daily_data[day_str] = {
            "market_signal": market_signal,
            "market_index_close": round(float(index_close), 2) if index_close else None,
            "total_market_amount": round(float(total_market_amount), 0) if total_market_amount else None,
            "history_group_id": history_group_id,
            "snapshot_source": source,
            "candidates": candidates,
            "prices": {stock_code: round(close_price, 4)},
            "bars": {
                stock_code: {
                    "open": round(float(row["open"]), 4),
                    "high": round(float(row["high"]), 4),
                    "low": round(float(row["low"]), 4),
                    "close": round(close_price, 4),
                    "ma5": round(float(row["MA5"]), 4) if not pd.isna(row.get("MA5")) else None,
                    "ma10": round(float(row["MA10"]), 4) if not pd.isna(row.get("MA10")) else None,
                    "ma20": round(float(row["MA20"]), 4) if not pd.isna(row.get("MA20")) else None,
                    "ma60": round(float(row["MA60"]), 4) if not pd.isna(row.get("MA60")) else None,
                }
            },
        }

    strategy_source = "config/strategy.yaml"
    if params.get("preset"):
        strategy_source += f"[preset={params['preset']}]"
    strategy_source += " + historical_overrides"
    data_fidelity = _summarize_data_fidelity(day_sources)

    return {
        "daily_data": daily_data,
        "total_capital": total_capital,
        "params": params,
        "_meta": {
            "stock_code": stock_code,
            "index_code": index_code,
            "start": start,
            "end": end,
            "preset": params.get("preset"),
            "entry_mode": params.get("entry_mode", "hybrid"),
            "use_history_snapshots": bool(use_history_snapshots),
            "strategy_source": strategy_source,
            "data_fidelity": data_fidelity,
            "generated_at": datetime.now().isoformat(),
        },
        "strategy_snapshot": strategy,
    }


# ---------------------------------------------------------------------------
# ATR 跟踪止盈止损专用回测（不依赖 strategy_replay）
# ---------------------------------------------------------------------------

def _compute_atr(df: pd.DataFrame, idx: int, period: int = 14) -> float | None:
    """计算 ATR(14)"""
    if idx < period:
        return None
    trs = []
    for i in range(idx - period + 1, idx + 1):
        high = float(df.iloc[i]["high"])
        low = float(df.iloc[i]["low"])
        prev_close = float(df.iloc[i - 1]["close"]) if i > 0 else high
        close = float(df.iloc[i]["close"])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    return sum(trs) / len(trs) if trs else None


def _compute_ma(df: pd.DataFrame, idx: int, window: int) -> float | None:
    if idx < window - 1:
        return None
    return float(df.iloc[idx - window + 1:idx + 1]["close"].mean())


def _volume_ratio(df: pd.DataFrame, idx: int, window: int = 5) -> float:
    if idx < window:
        return 0.0
    avg = float(df.iloc[idx - window:idx]["volume"].mean())
    today = float(df.iloc[idx]["volume"])
    return today / avg if avg > 0 else 0.0


def _is_golden_cross(df: pd.DataFrame, idx: int) -> bool:
    """MA10 上穿 MA20（金叉）"""
    if idx < 20:
        return False
    ma10_prev = _compute_ma(df, idx - 1, 10)
    ma20_prev = _compute_ma(df, idx - 1, 20)
    ma10_curr = _compute_ma(df, idx, 10)
    ma20_curr = _compute_ma(df, idx, 20)
    if None in (ma10_prev, ma20_prev, ma10_curr, ma20_curr):
        return False
    return ma10_prev <= ma20_prev and ma10_curr > ma20_curr


def run_atr_strategy_replay(
    stock_code: str,
    start: str,
    end: str,
    index_code: str = "000001",
    total_capital: float = 450286.0,
    atr_period: int = 14,
    stop_loss_atr_mult: float = 2.0,    # ATR×2
    stop_loss_pct: float = 0.05,        # 5% 固定止损
    stop_loss_low_mult: float = 0.97,   # 入场低点×0.97
    trailing_stop_pct: float = 0.08,    # 跟踪止盈 -8%
) -> dict:
    """
    ATR 跟踪止盈止损策略回测。

    入场规则：
      - MA10 金叉 MA20（金叉当天）
      - 量比 > 1.5
      - 大盘信号非 RED/CLEAR

    离场规则（优先级）：
      1. 跟踪止盈触发（从入场后高点回撤 > trailing_stop_pct）
      2. ATR 止损（从入场价跌 > ATR×2）
      3. 固定止损（从入场价跌 > stop_loss_pct）
      4. ATR 低点止损（入场低点 × stop_loss_low_mult）
      5. 大盘 RED/CLEAR 清仓
    """
    bs_code = _to_bs_code(stock_code)
    bs_index = _to_bs_code(index_code)

    start_dt = datetime.strptime(start, "%Y-%m-%d")
    warmup = (start_dt - timedelta(days=120)).strftime("%Y-%m-%d")

    df_index_raw = _fetch_daily(bs_index, warmup, end)
    df_index = compute_market_signal(df_index_raw)
    index_map = df_index.set_index("date")[["signal"]].to_dict("index")

    df = _fetch_daily(bs_code, warmup, end)
    # 预计算 MA10/MA20
    df["MA10"] = df["close"].rolling(10).mean()
    df["MA20"] = df["close"].rolling(20).mean()

    cash = total_capital
    peak_exposure = 0.0
    min_cash = cash
    cumulative_pnl = 0.0
    max_capital_deployed = 0.0
    constrained_count = 0

    closed_trades: list[dict] = []
    open_positions: list[dict] = []
    timeline: list[dict] = []
    rejected_entries: list[dict] = []

    # 持仓状态
    position: dict | None = None

    for idx, row in df.iterrows():
        day_str = row["date"].strftime("%Y-%m-%d")
        date_val = row["date"]
        if day_str < start or day_str > end:
            continue

        idx_info = index_map.get(row["date"], {})
        market_signal = idx_info.get("signal", "GREEN")
        close_price = float(row["close"])
        high_price = float(row["high"])
        low_price = float(row["low"])
        atr = _compute_atr(df, idx, atr_period) if idx >= atr_period else None
        vol_ratio = _volume_ratio(df, idx)
        golden_cross = _is_golden_cross(df, idx)

        exits_today = []
        entries_today = []

        # ── 持仓管理 ──
        if position:
            entry_price = position["entry_price"]
            entry_low = position["entry_low"]
            shares = position["shares"]
            capital = position["capital"]

            # 更新峰值（先更新，再用最新峰值计算止盈）
            if high_price > position["peak_price"]:
                position["peak_price"] = high_price
            peak_price = position["peak_price"]

            exit_reason = None
            exit_price = close_price

            # 1. 跟踪止盈（从峰值回落 > trailing_stop_pct）
            trailing_trigger = peak_price * (1 - trailing_stop_pct)
            if close_price <= trailing_trigger:
                exit_reason = "trailing_stop"
                exit_price = trailing_trigger

            # 2. ATR 止损
            if atr and exit_reason is None:
                atr_stop = entry_price - atr * stop_loss_atr_mult
                if close_price <= atr_stop:
                    exit_reason = "atr_stop"
                    exit_price = atr_stop

            # 3. 固定止损
            if exit_reason is None:
                fixed_stop = entry_price * (1 - stop_loss_pct)
                if close_price <= fixed_stop:
                    exit_reason = "fixed_stop"
                    exit_price = fixed_stop

            # 4. ATR 低点止损
            if exit_reason is None:
                low_stop = entry_low * stop_loss_low_mult
                if close_price <= low_stop:
                    exit_reason = "low_stop"
                    exit_price = low_stop

            # 5. 大盘止损
            if exit_reason is None and market_signal in ("RED", "CLEAR"):
                exit_reason = "market_signal_exit"
                exit_price = close_price

            if exit_reason:
                # 以触发止盈/止损时的价格出局（非收盘价）
                exit_price = round(exit_price, 4)
                realized_pnl = round((exit_price - entry_price) * shares, 2)
                cash = round(cash + capital + realized_pnl, 2)
                cumulative_pnl = round(cumulative_pnl + realized_pnl, 2)
                holding_days = (datetime.strptime(day_str, "%Y-%m-%d") -
                                datetime.strptime(position["entry_date"], "%Y-%m-%d")).days
                closed_trades.append({
                    "code": stock_code,
                    "name": "长飞光纤",
                    "entry_date": position["entry_date"],
                    "exit_date": day_str,
                    "entry_price": round(entry_price, 4),
                    "exit_price": exit_price,
                    "shares": shares,
                    "capital": round(capital, 2),
                    "realized_pnl": realized_pnl,
                    "exit_reason": exit_reason,
                    "entry_score": 10,
                    "holding_days": holding_days,
                    "atr": round(atr, 4) if atr else None,
                    "peak_price": round(peak_price, 4),
                    "trailing_trigger": round(trailing_trigger, 4),
                })
                exits_today.append({
                    "code": stock_code,
                    "capital_released": capital,
                    "realized_pnl": realized_pnl,
                    "exit_reason": exit_reason,
                })
                position = None

        # ── 入场判断 ──
        entry_today = None
        if not position and market_signal not in ("RED", "CLEAR"):
            # 大盘允许开仓
            if golden_cross and vol_ratio > 1.5:
                # 检查 ATR 存在（预热足够）
                if atr and atr > 0:
                    entry_price = close_price
                    entry_low = low_price
                    # 计算入场股数（单笔上限 20% 仓位）
                    single_cap_limit = round(total_capital * 0.20, 2)
                    capital = min(single_cap_limit, cash)
                    shares = max(int(capital / entry_price // 100) * 100, 100)
                    actual_capital = round(entry_price * shares, 2)
                    if actual_capital > cash:
                        shares = max(shares - 100, 100)
                        actual_capital = round(entry_price * shares, 2)
                    if actual_capital > 0 and actual_capital <= cash:
                        position = {
                            "code": stock_code,
                            "name": "长飞光纤",
                            "entry_date": day_str,
                            "entry_price": entry_price,
                            "entry_low": low_price,
                            "shares": shares,
                            "capital": actual_capital,
                            "peak_price": high_price,
                            "atr": atr,
                            "stop_atr": round(entry_price - atr * stop_loss_atr_mult, 4),
                            "stop_fixed": round(entry_price * (1 - stop_loss_pct), 4),
                            "stop_low": round(low_price * stop_loss_low_mult, 4),
                            "trailing_trigger": round(high_price * (1 - trailing_stop_pct), 4),
                        }
                        cash = round(cash - actual_capital, 2)
                        entries_today.append({
                            "code": stock_code,
                            "capital": actual_capital,
                            "atr": round(atr, 4),
                            "stop_atr": position["stop_atr"],
                            "stop_fixed": position["stop_fixed"],
                        })

        # ── 当日快照 ──
        capital_deployed = 0.0
        if position:
            # 盯市：当前亏损
            mark_pnl = round((close_price - position["entry_price"]) * position["shares"], 2)
            capital_deployed = position["capital"]
            peak_exposure = max(peak_exposure, capital_deployed / total_capital)
            max_capital_deployed = max(max_capital_deployed, capital_deployed)

        realized_today = sum(e["realized_pnl"] for e in exits_today)
        timeline.append({
            "date": day_str,
            "market_signal": market_signal,
            "close": round(close_price, 2),
            "atr": round(atr, 4) if atr else None,
            "vol_ratio": round(vol_ratio, 2),
            "golden_cross": golden_cross,
            "open_position_count": 1 if position else 0,
            "entry_count": len(entries_today),
            "exit_count": len(exits_today),
            "capital_deployed": round(capital_deployed, 2),
            "cash_available": round(cash, 2),
            "realized_pnl_today": round(realized_today, 2),
            "cumulative_pnl": round(cumulative_pnl, 2),
            "position": dict(position) if position else None,
        })

    # ── 汇总 ──
    win_trades = [t for t in closed_trades if t["realized_pnl"] > 0]
    loss_trades = [t for t in closed_trades if t["realized_pnl"] < 0]

    return {
        "command": "backtest",
        "action": "atr_strategy",
        "status": "ok",
        "summary": {
            "capital": round(total_capital, 2),
            "atr_period": atr_period,
            "stop_loss_atr_mult": stop_loss_atr_mult,
            "stop_loss_pct": stop_loss_pct,
            "stop_loss_low_mult": stop_loss_low_mult,
            "trailing_stop_pct": trailing_stop_pct,
            "timeline_days": len(timeline),
            "max_capital_deployed": round(max_capital_deployed, 2),
            "peak_exposure_pct": round(peak_exposure, 4),
            "min_cash_available": round(min(min_cash, cash), 2),
            "ending_cash": round(cash, 2),
            "total_realized_pnl": round(cumulative_pnl, 2),
            "closed_trade_count": len(closed_trades),
            "win_count": len(win_trades),
            "loss_count": len(loss_trades),
            "win_rate": round(len(win_trades) / len(closed_trades) * 100, 1) if closed_trades else 0.0,
            "open_position_count": 1 if position else 0,
        },
        "closed_trades": closed_trades,
        "open_positions": [dict(position)] if position else [],
        "timeline": timeline,
    }


def render_atr_strategy_report(result: dict) -> str:
    """格式化 ATR 策略回测报告"""
    s = result["summary"]
    lines = [
        "",
        "=" * 58,
        "  ATR 跟踪止盈止损策略回测",
        f"  区间: {result.get('_period', 'N/A')}",
        "=" * 58,
        "",
        f"  止损体系: ATR×{s['stop_loss_atr_mult']} | 固定 -%5 | 低点×0.97",
        f"  止盈体系: 跟踪 -%8（从峰值回落）",
        f"  ATR周期: {s['atr_period']}",
        "",
        f"  {'已平仓交易':>10}   {'胜率':>8}   {'累计盈亏':>12}",
        f"  {s['closed_trade_count']:>10}   {s['win_rate']:>7.1f}%   {s['total_realized_pnl']:>+12.2f}",
        "",
        "  逐笔明细:",
        f"  {'入场日期':<12} {'入场价':>8} {'出场日期':<12} {'出场价':>8} {'盈亏':>10} {'原因':>16} {'ATR':>8} {'峰值':>8} {'跟踪触发':>10}",
    ]

    for t in result.get("closed_trades", []):
        lines.append(
            f"  {t['entry_date']:<12} {t['entry_price']:>8.2f} "
            f"{t['exit_date']:<12} {t['exit_price']:>8.2f} "
            f"{t['realized_pnl']:>+10.2f}  {t['exit_reason']:>16} "
            f"{t.get('atr', 0) or 0:>8.2f} {t.get('peak_price', 0):>8.2f} "
            f"{t.get('trailing_trigger', 0):>8.2f}"
        )

    if result.get("open_positions"):
        lines.append(f"\n  未平仓 ({len(result['open_positions'])} 笔):")
        for p in result["open_positions"]:
            lines.append(
                f"  {p['entry_date']} 买入 @¥{p['entry_price']} "
                f"ATR={p.get('atr', 0):.2f} "
                f"峰值={p.get('peak_price', 0):.2f}"
            )

    lines.append("")
    return "\n".join(lines)


def run_system_strategy_backtest(
    stock_code: str,
    start: str,
    end: str,
    index_code: str = "system",
    total_capital: float | None = None,
    strategy_params: dict | None = None,
) -> dict:
    """Build a system-strategy historical fixture and immediately replay it."""
    from scripts.backtest.strategy_replay import run_strategy_replay

    params = _resolve_strategy_params(strategy_params)
    capital = total_capital if total_capital is not None else float(get_strategy().get("capital", 450286))
    fixture = build_replay_fixture(
        stock_code=stock_code,
        start=start,
        end=end,
        index_code=index_code,
        total_capital=capital,
        strategy_params=params,
    )
    result = run_strategy_replay(
        daily_data=fixture["daily_data"],
        start=start,
        end=end,
        total_capital=fixture["total_capital"],
        params=fixture["params"],
    )
    result["fixture_meta"] = fixture.get("_meta", {})
    result["params"] = fixture["params"]
    return result


def _candidate_row_for_code(snapshot: dict[str, Any], stock_code: str) -> dict[str, Any]:
    for cand in snapshot.get("candidates", []) or []:
        if str(cand.get("code", "")).strip() == stock_code:
            return cand
    return {}


def _build_single_stock_daily_rows(stock_code: str, daily_data: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day in sorted(daily_data):
        snapshot = daily_data.get(day, {}) or {}
        candidate = _candidate_row_for_code(snapshot, stock_code)
        prices = snapshot.get("prices", {}) if isinstance(snapshot.get("prices", {}), dict) else {}
        bars = snapshot.get("bars", {}) if isinstance(snapshot.get("bars", {}), dict) else {}
        bar = bars.get(stock_code, {}) if isinstance(bars.get(stock_code, {}), dict) else {}
        close = candidate.get("price", prices.get(stock_code, bar.get("close", 0)))
        high = bar.get("high", candidate.get("high", close))
        low = bar.get("low", candidate.get("low", close))
        rows.append({
            "date": day,
            "close": float(close or 0),
            "high": float(high or close or 0),
            "low": float(low or close or 0),
            "market_signal": str(snapshot.get("market_signal", "GREEN") or "GREEN").upper(),
            "score": float(candidate.get("score", 0) or 0),
            "entry_signal": bool(candidate.get("entry_signal", False)),
            "entry_reasons": list(candidate.get("entry_reasons", []) or []),
            "veto_signals": list(candidate.get("veto_signals", []) or []),
            "style": str(candidate.get("style", "") or ""),
            "volume_ratio": float(candidate.get("volume_ratio", 0) or 0),
            "rsi": candidate.get("rsi"),
            "snapshot_source": str(snapshot.get("snapshot_source", "proxy_replay") or "proxy_replay"),
            "history_group_id": str(snapshot.get("history_group_id", "") or ""),
            "candidate_present": bool(candidate),
        })
    return rows


def _summarize_reason_counts(items: list[str]) -> list[dict[str, Any]]:
    counter = Counter(str(item).strip() for item in items if str(item).strip())
    return [
        {"reason": veto_reason_to_label(reason), "count": count}
        for reason, count in sorted(counter.items(), key=lambda pair: (-pair[1], pair[0]))
    ]


def _trade_ranges(result: dict[str, Any], end: str) -> list[dict[str, Any]]:
    ranges: list[dict[str, Any]] = []
    for trade in result.get("closed_trades", []):
        ranges.append({
            "entry_date": str(trade.get("entry_date", "")).strip(),
            "exit_date": str(trade.get("exit_date", "")).strip(),
            "exit_reason": str(trade.get("exit_reason", "")).strip(),
            "realized_pnl": float(trade.get("realized_pnl", 0) or 0),
        })
    for pos in result.get("open_positions", []):
        ranges.append({
            "entry_date": str(pos.get("entry_date", "")).strip(),
            "exit_date": end,
            "exit_reason": "open_position",
            "realized_pnl": 0.0,
        })
    return ranges


def _date_in_ranges(day: str, ranges: list[dict[str, Any]]) -> bool:
    return any(item["entry_date"] <= day <= item["exit_date"] for item in ranges if item.get("entry_date") and item.get("exit_date"))


def _classify_missed_reason(row: dict[str, Any], params: dict[str, Any]) -> str:
    if row.get("snapshot_source") == "history_signal_snapshot" and not bool(row.get("candidate_present", False)):
        return "not_in_scored_candidates"
    if row.get("market_signal") in {"RED", "CLEAR"}:
        return f"market_signal_{str(row['market_signal']).lower()}"
    if float(row.get("score", 0) or 0) < float(params.get("buy_threshold", 7) or 7):
        return "score_below_threshold"
    if bool(params.get("require_entry_signal", False)) and not bool(row.get("entry_signal", False)):
        return "entry_signal_missing"
    veto_signals = [str(item).strip() for item in row.get("veto_signals", []) if str(item).strip()]
    if veto_signals:
        return f"veto:{','.join(veto_signals)}"
    return "portfolio_or_execution_constraint"


def _find_opportunity_windows(
    rows: list[dict[str, Any]],
    params: dict[str, Any],
    holding_ranges: list[dict[str, Any]],
    *,
    lookahead_days: int = 20,
    min_gain_pct: float = 0.15,
) -> list[dict[str, Any]]:
    if len(rows) < 2:
        return []

    raw_candidates: list[dict[str, Any]] = []
    for idx, row in enumerate(rows[:-1]):
        close = float(row.get("close", 0) or 0)
        if close <= 0:
            continue
        end_idx = min(len(rows) - 1, idx + lookahead_days)
        peak_idx = idx
        peak_price = close
        for j in range(idx + 1, end_idx + 1):
            future_close = float(rows[j].get("close", 0) or 0)
            if future_close > peak_price:
                peak_price = future_close
                peak_idx = j
        if peak_idx <= idx:
            continue
        gain_pct = peak_price / close - 1
        if gain_pct >= min_gain_pct:
            raw_candidates.append({
                "anchor_idx": idx,
                "peak_idx": peak_idx,
                "gain_pct": gain_pct,
            })

    if not raw_candidates:
        return []

    blocks: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for candidate in raw_candidates:
        if current is None or candidate["anchor_idx"] > current["anchor_end_idx"] + 1:
            if current is not None:
                blocks.append(current)
            current = {
                "anchor_start_idx": candidate["anchor_idx"],
                "anchor_end_idx": candidate["anchor_idx"],
                "best": candidate,
            }
            continue
        current["anchor_end_idx"] = candidate["anchor_idx"]
        if candidate["gain_pct"] > current["best"]["gain_pct"]:
            current["best"] = candidate
    if current is not None:
        blocks.append(current)

    windows: list[dict[str, Any]] = []
    for block in blocks:
        best = block["best"]
        anchor = rows[best["anchor_idx"]]
        peak = rows[best["peak_idx"]]
        start_date = str(anchor["date"])
        peak_date = str(peak["date"])
        overlapping = [
            item for item in holding_ranges
            if item.get("entry_date") and item.get("exit_date")
            and not (item["exit_date"] < start_date or item["entry_date"] > peak_date)
        ]
        captured = bool(overlapping)
        windows.append({
            "start_date": start_date,
            "peak_date": peak_date,
            "start_price": round(float(anchor.get("close", 0) or 0), 4),
            "peak_price": round(float(peak.get("close", 0) or 0), 4),
            "gain_pct": round(float(best["gain_pct"] * 100), 2),
            "window_days": int(best["peak_idx"] - best["anchor_idx"]),
            "captured": captured,
            "capture_mode": "held_during_run" if captured else "missed",
            "miss_reason": "" if captured else _classify_missed_reason(anchor, params),
            "market_signal": anchor.get("market_signal", ""),
            "score": round(float(anchor.get("score", 0) or 0), 2),
            "entry_signal": bool(anchor.get("entry_signal", False)),
            "veto_signals": list(anchor.get("veto_signals", []) or []),
        })
    return windows


def _find_premature_exits(
    rows: list[dict[str, Any]],
    result: dict[str, Any],
    *,
    lookahead_days: int = 20,
    min_extra_gain_pct: float = 0.08,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    index_by_date = {row["date"]: idx for idx, row in enumerate(rows)}
    premature: list[dict[str, Any]] = []
    for trade in result.get("closed_trades", []):
        exit_date = str(trade.get("exit_date", "")).strip()
        exit_price = float(trade.get("exit_price", 0) or 0)
        if not exit_date or exit_price <= 0 or exit_date not in index_by_date:
            continue
        exit_idx = index_by_date[exit_date]
        end_idx = min(len(rows) - 1, exit_idx + lookahead_days)
        future_rows = rows[exit_idx + 1:end_idx + 1]
        if not future_rows:
            continue
        future_peak = max(future_rows, key=lambda item: float(item.get("close", 0) or 0))
        future_peak_price = float(future_peak.get("close", 0) or 0)
        if future_peak_price <= 0:
            continue
        extra_gain_pct = future_peak_price / exit_price - 1
        if extra_gain_pct < min_extra_gain_pct:
            continue
        premature.append({
            "entry_date": str(trade.get("entry_date", "")).strip(),
            "exit_date": exit_date,
            "exit_reason": str(trade.get("exit_reason", "")).strip(),
            "exit_price": round(exit_price, 4),
            "future_peak_date": str(future_peak["date"]),
            "future_peak_price": round(future_peak_price, 4),
            "missed_gain_pct": round(extra_gain_pct * 100, 2),
            "realized_pnl": round(float(trade.get("realized_pnl", 0) or 0), 2),
        })
    return premature


def _signal_statistics(rows: list[dict[str, Any]], params: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    buy_threshold = float(params.get("buy_threshold", 7) or 7)
    market_positive_days = sum(1 for row in rows if row.get("market_signal") in {"GREEN", "YELLOW"})
    score_ready_days = sum(1 for row in rows if float(row.get("score", 0) or 0) >= buy_threshold)
    entry_signal_days = sum(1 for row in rows if bool(row.get("entry_signal", False)))
    veto_free_days = sum(1 for row in rows if not row.get("veto_signals"))
    buy_ready_days = sum(
        1 for row in rows
        if row.get("market_signal") in {"GREEN", "YELLOW"}
        and float(row.get("score", 0) or 0) >= buy_threshold
        and (not bool(params.get("require_entry_signal", False)) or bool(row.get("entry_signal", False)))
        and not row.get("veto_signals")
    )
    actual_entry_days = sum(int(item.get("entry_count", 0) or 0) for item in result.get("timeline", []))
    return {
        "market_positive_days": market_positive_days,
        "score_ready_days": score_ready_days,
        "entry_signal_days": entry_signal_days,
        "veto_free_days": veto_free_days,
        "buy_ready_days": buy_ready_days,
        "actual_entry_days": actual_entry_days,
    }


def _summarize_exit_reasons(closed_trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "count": 0,
        "total_realized_pnl": 0.0,
        "win_count": 0,
        "loss_count": 0,
        "total_holding_days": 0,
    })
    for trade in closed_trades:
        reason = str(trade.get("exit_reason", "")).strip() or "unknown"
        bucket = grouped[reason]
        pnl = float(trade.get("realized_pnl", 0) or 0)
        bucket["count"] += 1
        bucket["total_realized_pnl"] += pnl
        bucket["total_holding_days"] += int(trade.get("holding_days", 0) or 0)
        if pnl > 0:
            bucket["win_count"] += 1
        elif pnl < 0:
            bucket["loss_count"] += 1
    items = []
    for reason, bucket in grouped.items():
        count = max(bucket["count"], 1)
        items.append({
            "reason": reason,
            "count": bucket["count"],
            "total_realized_pnl": round(bucket["total_realized_pnl"], 2),
            "avg_realized_pnl": round(bucket["total_realized_pnl"] / count, 2),
            "avg_holding_days": round(bucket["total_holding_days"] / count, 1),
            "win_count": bucket["win_count"],
            "loss_count": bucket["loss_count"],
        })
    return sorted(items, key=lambda item: (-item["count"], item["reason"]))


def _build_validation_findings(
    opportunities: list[dict[str, Any]],
    premature_exits: list[dict[str, Any]],
    rejection_breakdown: list[dict[str, Any]],
    signal_stats: dict[str, Any],
) -> list[str]:
    findings: list[str] = []
    total_windows = len(opportunities)
    captured_windows = sum(1 for item in opportunities if item.get("captured"))
    if total_windows:
        findings.append(
            f"主升窗口 {captured_windows}/{total_windows} 个被持仓覆盖，捕获率 {captured_windows / total_windows * 100:.1f}%"
        )
    if premature_exits:
        top_exit = max(premature_exits, key=lambda item: float(item.get("missed_gain_pct", 0) or 0))
        findings.append(
            f"存在 {len(premature_exits)} 次提前离场，最大卖飞发生在 {top_exit['exit_date']} 后少赚 {top_exit['missed_gain_pct']:.1f}%"
        )
    if rejection_breakdown:
        top_reject = rejection_breakdown[0]
        findings.append(f"最常见的错过原因是 {top_reject['reason']}，共 {top_reject['count']} 次")
    if signal_stats.get("buy_ready_days", 0) == 0 and signal_stats.get("score_ready_days", 0) > 0:
        findings.append("出现过分数达标，但没有形成完整买点，说明入场触发条件偏严")
    return findings


def _validation_data_fidelity(meta: dict[str, Any]) -> dict[str, Any]:
    fidelity = dict(meta.get("data_fidelity", {}) if isinstance(meta.get("data_fidelity", {}), dict) else {})
    mode = str(fidelity.get("mode", "")).strip() or "proxy_replay"
    history_days = int(fidelity.get("history_days", 0) or 0)
    proxy_days = int(fidelity.get("proxy_days", 0) or 0)
    history_candidate_absent_days = int(fidelity.get("history_candidate_absent_days", 0) or 0)

    if mode == "historical_signal_mirror":
        notes = [
            "交易日优先使用每日持久化的 market snapshot 与 scored candidates",
            "当日股票不在历史候选池时，按真实快照保留为空候选，仅继续管理已有持仓",
        ]
    elif mode == "hybrid_signal_mirror":
        notes = [
            f"{history_days} 个交易日使用历史信号快照，{proxy_days} 个交易日回退到价格代理重建",
            "缺快照日期仍按当前策略逻辑重建，不会中断整段回放",
        ]
    else:
        notes = [
            "技术面、风格和大盘择时按历史价格重建",
            "基本面、资金流和舆情仍为历史近似，不是当日完整实盘镜像",
        ]

    fidelity["mode"] = mode
    fidelity["history_days"] = history_days
    fidelity["proxy_days"] = proxy_days
    fidelity["history_candidate_absent_days"] = history_candidate_absent_days
    fidelity["notes"] = notes
    return fidelity


def _available_signal_history_groups(snapshot_date: str, *, limit: int = 20) -> list[dict[str, Any]]:
    def _group_timepoint(history_group_id: str, pipeline: str, metadata: dict[str, Any]) -> str:
        timepoint = str(metadata.get("timepoint", "") if isinstance(metadata, dict) else "").strip()
        if timepoint:
            return timepoint
        parts = [part.strip() for part in str(history_group_id or "").split(":") if part.strip()]
        if len(parts) >= 4 and parts[0] in {"morning", "noon", "evening"}:
            return parts[2]
        if pipeline in {"morning", "noon", "evening", "stock_screener", "core_pool_scoring"}:
            return {
                "morning": "preopen",
                "noon": "midday",
                "evening": "close",
                "stock_screener": "screener",
                "core_pool_scoring": "core_pool_scoring",
            }.get(pipeline, pipeline)
        return parts[0] if parts else ""

    group_rows: dict[str, dict[str, Any]] = {}
    sources = [
        ("market_snapshot", load_market_snapshot_history(snapshot_date, limit=max(1, int(limit or 20)))),
        ("pool_snapshot", load_pool_snapshot_history(snapshot_date, limit=max(1, int(limit or 20)))),
        ("today_decision", load_decision_snapshot_history(snapshot_date, limit=max(1, int(limit or 20)))),
        ("scored_candidates", load_candidate_snapshot_history(snapshot_date, limit=max(1, int(limit or 20)))),
    ]
    for component, payload in sources:
        for item in payload.get("items", []) or []:
            history_group_id = str(item.get("history_group_id", "")).strip()
            if not history_group_id:
                continue
            metadata = item.get("metadata", {}) if isinstance(item.get("metadata", {}), dict) else {}
            row = group_rows.setdefault(
                history_group_id,
                {
                    "history_group_id": history_group_id,
                    "snapshot_date": snapshot_date,
                    "updated_at": str(item.get("updated_at", "") or ""),
                    "pipeline": str(item.get("pipeline", "") or ""),
                    "source": str(item.get("source", "") or ""),
                    "timepoint": _group_timepoint(history_group_id, str(item.get("pipeline", "") or ""), metadata),
                    "components": set(),
                },
            )
            row["components"].add(component)
            updated_at = str(item.get("updated_at", "") or "")
            if updated_at > str(row.get("updated_at", "") or ""):
                row["updated_at"] = updated_at
            if not row.get("pipeline") and item.get("pipeline"):
                row["pipeline"] = str(item.get("pipeline", "") or "")
            if not row.get("source") and item.get("source"):
                row["source"] = str(item.get("source", "") or "")
            if not row.get("timepoint"):
                row["timepoint"] = _group_timepoint(history_group_id, str(item.get("pipeline", "") or ""), metadata)

    items = []
    for row in group_rows.values():
        items.append(
            {
                "history_group_id": row["history_group_id"],
                "snapshot_date": snapshot_date,
                "updated_at": row.get("updated_at", ""),
                "pipeline": row.get("pipeline", ""),
                "source": row.get("source", ""),
                "timepoint": row.get("timepoint", ""),
                "component_count": len(row.get("components", set())),
                "components": sorted(row.get("components", set())),
            }
        )
    items.sort(key=lambda item: (str(item.get("updated_at", "") or ""), str(item.get("history_group_id", "") or "")), reverse=True)
    return items[: max(1, int(limit or 20))]


def _pool_entry_for_code(snapshot: dict[str, Any], stock_code: str) -> dict[str, Any]:
    normalized_target = _normalize_replay_code(stock_code)
    entries = snapshot.get("entries", []) if isinstance(snapshot, dict) else []
    for entry in entries if isinstance(entries, list) else []:
        if _normalize_replay_code(entry.get("code", "")) == normalized_target:
            return dict(entry)
    return {}


def _diagnose_signal_snapshot_code(stock_code: str, bundle: dict[str, Any]) -> dict[str, Any]:
    code = str(stock_code or "").strip()
    candidate = _history_candidate_for_code(bundle, code)
    pool_entry = _pool_entry_for_code(bundle.get("pool_snapshot", {}) if isinstance(bundle.get("pool_snapshot", {}), dict) else {}, code)
    market_snapshot = bundle.get("market_snapshot", {}) if isinstance(bundle.get("market_snapshot", {}), dict) else {}
    candidate_snapshot = bundle.get("candidate_snapshot", {}) if isinstance(bundle.get("candidate_snapshot", {}), dict) else {}
    missing_components = list(bundle.get("missing_components", []) or [])
    candidates = candidate_snapshot.get("candidates", []) if isinstance(candidate_snapshot.get("candidates", []), list) else []

    candidate_rank = 0
    if candidate:
        normalized_target = _normalize_replay_code(code)
        for idx, item in enumerate(candidates, start=1):
            if _normalize_replay_code(item.get("code", "")) == normalized_target:
                candidate_rank = idx
                break

    status = "unknown"
    reason = ""
    if pool_entry:
        bucket = str(pool_entry.get("bucket", "")).strip() or "other"
        status = f"selected_{bucket}"
        reason = str(pool_entry.get("note", "") or "").strip() or f"stock landed in {bucket} pool"
    elif candidate:
        veto_signals = [str(item).strip() for item in candidate.get("veto_signals", []) or [] if str(item).strip()]
        if veto_signals:
            status = "scored_but_vetoed"
            reason = ",".join(veto_signals)
        else:
            status = "scored_but_not_in_pool"
            reason = "candidate was scored in this run but not projected into pool snapshot"
    elif "candidate_snapshot" in missing_components or "scored_candidates" in missing_components:
        status = "candidate_snapshot_missing"
        reason = "candidate snapshot missing for this history group"
    else:
        status = "not_in_scored_candidates"
        reason = "stock was not present in scored candidates for this run"

    return {
        "code": code,
        "history_group_id": str(bundle.get("history_group_id", "") or ""),
        "market_signal": str(market_snapshot.get("signal", "") or ""),
        "candidate_present": bool(candidate),
        "candidate_rank": candidate_rank,
        "pool_present": bool(pool_entry),
        "pool_bucket": str(pool_entry.get("bucket", "") or ""),
        "status": status,
        "reason": reason,
        "candidate": candidate,
        "pool_entry": pool_entry,
    }


def _build_market_timepoint_timeline(snapshot_date: str, groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_market = load_market_snapshot_history(snapshot_date, limit=100).get("items", [])
    latest_by_group: dict[str, dict[str, Any]] = {}
    for item in latest_market:
        history_group_id = str(item.get("history_group_id", "")).strip()
        if history_group_id and history_group_id not in latest_by_group:
            latest_by_group[history_group_id] = item

    timeline = []
    for group in groups:
        history_group_id = str(group.get("history_group_id", "")).strip()
        market_item = latest_by_group.get(history_group_id, {})
        timeline.append(
            {
                "history_group_id": history_group_id,
                "timepoint": str(group.get("timepoint", "") or ""),
                "pipeline": str(group.get("pipeline", "") or ""),
                "updated_at": str(group.get("updated_at", "") or ""),
                "signal": str(market_item.get("signal", "") or ""),
                "source": str(market_item.get("source", group.get("source", "")) or group.get("source", "")),
                "components": list(group.get("components", []) or []),
            }
        )

    order = {"preopen": 0, "midday": 1, "screener": 2, "core_pool_scoring": 3, "close": 4}
    timeline.sort(
        key=lambda item: (
            order.get(str(item.get("timepoint", "") or ""), 99),
            str(item.get("updated_at", "") or ""),
            str(item.get("history_group_id", "") or ""),
        )
    )
    return timeline


def _diagnosis_across_group_row(group: dict[str, Any], diagnosis: dict[str, Any]) -> dict[str, Any]:
    return {
        "history_group_id": str(group.get("history_group_id", "")).strip(),
        "timepoint": str(group.get("timepoint", "") or ""),
        "pipeline": str(group.get("pipeline", "") or ""),
        "updated_at": str(group.get("updated_at", "") or ""),
        "status": diagnosis.get("status", ""),
        "reason": diagnosis.get("reason", ""),
        "market_signal": diagnosis.get("market_signal", ""),
        "candidate_present": bool(diagnosis.get("candidate_present", False)),
        "pool_present": bool(diagnosis.get("pool_present", False)),
        "pool_bucket": str(diagnosis.get("pool_bucket", "") or ""),
    }


def _diagnose_snapshot_code_across_groups(
    stock_code: str,
    *,
    snapshot_date: str,
    available_groups: list[dict[str, Any]],
    bundle_cache: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    across_groups = []
    for group in available_groups[:20]:
        group_id = str(group.get("history_group_id", "")).strip()
        if not group_id:
            continue
        bundle = bundle_cache.get(group_id)
        if bundle is None:
            bundle = load_daily_signal_snapshot_bundle(snapshot_date, history_group_id=group_id)
            bundle_cache[group_id] = bundle
        across_groups.append(_diagnosis_across_group_row(group, _diagnose_signal_snapshot_code(stock_code, bundle)))
    return across_groups


def _build_code_scan_item(
    stock_code: str,
    *,
    selected_group_id: str,
    across_groups: list[dict[str, Any]],
) -> dict[str, Any]:
    current = next(
        (item for item in across_groups if str(item.get("history_group_id", "")).strip() == selected_group_id),
        across_groups[0] if across_groups else {},
    )
    screener = next((item for item in across_groups if item.get("timepoint") == "screener"), {})
    timepoint_statuses = {
        str(item.get("timepoint", "") or ""): str(item.get("status", "") or "")
        for item in across_groups
        if str(item.get("timepoint", "") or "")
    }
    selected_count = sum(1 for item in across_groups if str(item.get("status", "")).startswith("selected_"))
    candidate_count = sum(1 for item in across_groups if bool(item.get("candidate_present", False)))
    pool_count = sum(1 for item in across_groups if bool(item.get("pool_present", False)))
    return {
        "code": str(stock_code or "").strip(),
        "current_group_status": str(current.get("status", "") or ""),
        "current_group_reason": str(current.get("reason", "") or ""),
        "current_group_bucket": str(current.get("pool_bucket", "") or ""),
        "current_group_market_signal": str(current.get("market_signal", "") or ""),
        "screener_status": str(screener.get("status", "") or ""),
        "screener_bucket": str(screener.get("pool_bucket", "") or ""),
        "selected_group_count": selected_count,
        "candidate_group_count": candidate_count,
        "pool_group_count": pool_count,
        "timepoint_statuses": timepoint_statuses,
        "across_groups": across_groups,
    }


def diagnose_signal_snapshot(
    snapshot_date: str,
    *,
    history_group_id: str | None = None,
    stock_code: str | None = None,
    stock_codes: list[str] | None = None,
    candidate_limit: int = 20,
) -> dict[str, Any]:
    """Inspect one historical signal snapshot bundle and optionally explain a stock's outcome."""
    resolved_date = str(snapshot_date or "").strip()
    if not resolved_date:
        raise ValueError("snapshot_date is required")

    available_groups = _available_signal_history_groups(resolved_date, limit=50)
    requested_group_id = str(history_group_id or "").strip()
    resolved_group_id = requested_group_id or (available_groups[0]["history_group_id"] if available_groups else "")
    bundle = load_daily_signal_snapshot_bundle(resolved_date, history_group_id=resolved_group_id or None)
    bundle_cache = {
        str(bundle.get("history_group_id", resolved_group_id) or resolved_group_id): bundle,
    } if str(bundle.get("history_group_id", resolved_group_id) or resolved_group_id).strip() else {}

    pool_snapshot = dict(bundle.get("pool_snapshot", {}) if isinstance(bundle.get("pool_snapshot", {}), dict) else {})
    candidate_snapshot = dict(bundle.get("candidate_snapshot", {}) if isinstance(bundle.get("candidate_snapshot", {}), dict) else {})
    candidates = list(candidate_snapshot.get("candidates", []) if isinstance(candidate_snapshot.get("candidates", []), list) else [])
    pool_entries = list(pool_snapshot.get("entries", []) if isinstance(pool_snapshot.get("entries", []), list) else [])

    candidate_snapshot["candidates"] = candidates[: max(1, int(candidate_limit or 20))]
    candidate_snapshot["candidates_truncated"] = len(candidates) > len(candidate_snapshot["candidates"])
    pool_snapshot["entries"] = pool_entries[: max(1, int(candidate_limit or 20))]
    pool_snapshot["entries_truncated"] = len(pool_entries) > len(pool_snapshot["entries"])
    market_timeline = _build_market_timepoint_timeline(resolved_date, available_groups)

    result = {
        "command": "backtest",
        "action": "signal_snapshot_diagnosis",
        "status": str(bundle.get("status", "missing") or "missing"),
        "snapshot_date": resolved_date,
        "requested_history_group_id": requested_group_id,
        "history_group_id": str(bundle.get("history_group_id", resolved_group_id) or resolved_group_id),
        "resolved_from_latest": bool(not requested_group_id and str(bundle.get("history_group_id", "") or "")),
        "missing_components": list(bundle.get("missing_components", []) or []),
        "available_history_groups": available_groups,
        "market_timeline": market_timeline,
        "market_snapshot": bundle.get("market_snapshot", {}),
        "today_decision": bundle.get("today_decision", {}),
        "decision_snapshot": bundle.get("decision_snapshot", {}),
        "pool_snapshot": pool_snapshot,
        "candidate_snapshot": candidate_snapshot,
        "candidate_count": int(candidate_snapshot.get("candidate_count", len(candidates)) or 0),
        "pool_entry_count": len(pool_entries),
    }
    if stock_code:
        result["code_diagnosis"] = _diagnose_signal_snapshot_code(stock_code, bundle)
        across_groups = _diagnose_snapshot_code_across_groups(
            stock_code,
            snapshot_date=resolved_date,
            available_groups=available_groups,
            bundle_cache=bundle_cache,
        )
        result["code_diagnosis_across_groups"] = across_groups
    requested_codes = []
    if stock_codes:
        requested_codes.extend([str(code).strip() for code in stock_codes if str(code).strip()])
    if stock_code and str(stock_code).strip() not in requested_codes:
        requested_codes.append(str(stock_code).strip())
    if requested_codes:
        code_scan = []
        summary: dict[str, int] = {}
        for code in requested_codes:
            across_groups = _diagnose_snapshot_code_across_groups(
                code,
                snapshot_date=resolved_date,
                available_groups=available_groups,
                bundle_cache=bundle_cache,
            )
            item = _build_code_scan_item(
                code,
                selected_group_id=str(result.get("history_group_id", "") or ""),
                across_groups=across_groups,
            )
            code_scan.append(item)
            current_status = str(item.get("current_group_status", "") or "unknown")
            summary[current_status] = summary.get(current_status, 0) + 1
        result["requested_codes"] = requested_codes
        result["code_scan"] = code_scan
        result["code_scan_summary"] = {
            "requested_code_count": len(requested_codes),
            "status_counts": summary,
        }
    return result


def run_single_stock_strategy_validation(
    stock_code: str,
    start: str,
    end: str,
    index_code: str = "system",
    total_capital: float | None = None,
    strategy_params: dict | None = None,
    *,
    opportunity_lookahead_days: int = 20,
    opportunity_min_gain_pct: float = 0.15,
    premature_exit_min_gain_pct: float = 0.08,
) -> dict[str, Any]:
    """Run single-stock replay and return a structured diagnostics report."""
    from scripts.backtest.strategy_replay import run_strategy_replay

    params = _resolve_strategy_params(strategy_params)
    capital = total_capital if total_capital is not None else float(get_strategy().get("capital", 450286))
    fixture = build_replay_fixture(
        stock_code=stock_code,
        start=start,
        end=end,
        index_code=index_code,
        total_capital=capital,
        strategy_params=params,
    )
    replay = run_strategy_replay(
        daily_data=fixture["daily_data"],
        start=start,
        end=end,
        total_capital=fixture["total_capital"],
        params=fixture["params"],
    )

    rows = _build_single_stock_daily_rows(stock_code, fixture["daily_data"])
    holding_ranges = _trade_ranges(replay, end)
    opportunities = _find_opportunity_windows(
        rows,
        fixture["params"],
        holding_ranges,
        lookahead_days=opportunity_lookahead_days,
        min_gain_pct=opportunity_min_gain_pct,
    )
    premature_exits = _find_premature_exits(
        rows,
        replay,
        lookahead_days=opportunity_lookahead_days,
        min_extra_gain_pct=premature_exit_min_gain_pct,
    )
    signal_stats = _signal_statistics(rows, fixture["params"], replay)
    rejection_breakdown = _summarize_reason_counts([
        item.get("reason", "") for item in replay.get("rejected_entries", [])
    ])
    exit_reason_breakdown = _summarize_exit_reasons(replay.get("closed_trades", []))
    opportunity_miss_reason_breakdown = _summarize_reason_counts([
        item.get("miss_reason", "") for item in opportunities if not item.get("captured")
    ])
    captured_gain = sum(float(item.get("gain_pct", 0) or 0) for item in opportunities if item.get("captured"))
    total_gain = sum(float(item.get("gain_pct", 0) or 0) for item in opportunities)
    captured_windows = sum(1 for item in opportunities if item.get("captured"))
    findings = _build_validation_findings(opportunities, premature_exits, opportunity_miss_reason_breakdown, signal_stats)

    return {
        "command": "backtest",
        "action": "single_stock_strategy_validation",
        "status": "ok",
        "stock_code": stock_code,
        "start": start,
        "end": end,
        "index_code": index_code,
        "data_fidelity": _validation_data_fidelity(fixture.get("_meta", {})),
        "params": fixture["params"],
        "fixture_meta": fixture.get("_meta", {}),
        "performance": replay.get("summary", {}),
        "diagnostics": {
            "signal_statistics": signal_stats,
            "exit_reason_breakdown": exit_reason_breakdown,
            "rejected_reason_breakdown": rejection_breakdown,
            "opportunity_statistics": {
                "total_opportunity_windows": len(opportunities),
                "captured_opportunity_windows": captured_windows,
                "missed_opportunity_windows": len(opportunities) - captured_windows,
                "capture_rate_pct": round(captured_windows / len(opportunities) * 100, 1) if opportunities else 0.0,
                "weighted_capture_rate_pct": round(captured_gain / total_gain * 100, 1) if total_gain > 0 else 0.0,
            },
            "opportunity_miss_reason_breakdown": opportunity_miss_reason_breakdown,
            "premature_exit_count": len(premature_exits),
            "findings": findings,
        },
        "opportunity_windows": opportunities[:20],
        "premature_exits": premature_exits[:20],
        "closed_trades": replay.get("closed_trades", []),
        "open_positions": replay.get("open_positions", []),
        "rejected_entries": replay.get("rejected_entries", []),
    }


def render_single_stock_validation_report(report: dict[str, Any]) -> str:
    """Render a concise text report for single-stock validation."""
    perf = report.get("performance", {})
    diag = report.get("diagnostics", {})
    opp = diag.get("opportunity_statistics", {})
    lines = [
        "",
        "=" * 64,
        f"  单股策略验证报告  {report.get('stock_code', '')}",
        f"  区间: {report.get('start', '')} ~ {report.get('end', '')}",
        "=" * 64,
        "",
        f"  模式: {report.get('data_fidelity', {}).get('mode', 'unknown')}",
        f"  收益: {perf.get('total_realized_pnl', 0):+.2f}    期末权益: {perf.get('ending_equity', 0):.2f}",
        f"  交易: {perf.get('closed_trade_count', 0)} 笔    胜率: {perf.get('win_rate', 0):.1f}%",
        f"  最大回撤: {perf.get('max_drawdown_pct', 0):+.2f}% @ {perf.get('max_drawdown_date', '')}",
        "",
        "  信号统计:",
        f"    市场可交易日 {diag.get('signal_statistics', {}).get('market_positive_days', 0)} / "
        f"分数达标 {diag.get('signal_statistics', {}).get('score_ready_days', 0)} / "
        f"完整买点 {diag.get('signal_statistics', {}).get('buy_ready_days', 0)} / "
        f"实际入场 {diag.get('signal_statistics', {}).get('actual_entry_days', 0)}",
        "",
        "  机会窗口:",
        f"    总数 {opp.get('total_opportunity_windows', 0)} / 捕获 {opp.get('captured_opportunity_windows', 0)} / "
        f"捕获率 {opp.get('capture_rate_pct', 0):.1f}% / 加权捕获率 {opp.get('weighted_capture_rate_pct', 0):.1f}%",
        "",
        "  主要结论:",
    ]
    for item in diag.get("findings", [])[:5]:
        lines.append(f"    - {item}")
    if not diag.get("findings"):
        lines.append("    - 暂无显著结论")
    lines.append("")
    lines.append("  主要出场原因:")
    for item in diag.get("exit_reason_breakdown", [])[:5]:
        lines.append(
            f"    - {item['reason']}: {item['count']} 次, "
            f"累计 {item['total_realized_pnl']:+.2f}, 平均持有 {item['avg_holding_days']:.1f} 天"
        )
    lines.append("")
    lines.append("  错过主升的主要原因:")
    for item in diag.get("opportunity_miss_reason_breakdown", [])[:5]:
        lines.append(f"    - {item['reason']}: {item['count']} 次")
    lines.append("")
    lines.append("  提前离场样本:")
    for item in report.get("premature_exits", [])[:3]:
        lines.append(
            f"    - {item['exit_date']} {item['exit_reason']} 后，"
            f"{item['future_peak_date']} 前还少赚 {item['missed_gain_pct']:.1f}%"
        )
    lines.append("")
    return "\n".join(lines)


def render_signal_snapshot_diagnosis_report(report: dict[str, Any]) -> str:
    """Render a concise text report for one historical signal snapshot."""
    market = report.get("market_snapshot", {}) if isinstance(report.get("market_snapshot", {}), dict) else {}
    decision = report.get("today_decision", {}) if isinstance(report.get("today_decision", {}), dict) else {}
    pool_snapshot = report.get("pool_snapshot", {}) if isinstance(report.get("pool_snapshot", {}), dict) else {}
    candidate_snapshot = report.get("candidate_snapshot", {}) if isinstance(report.get("candidate_snapshot", {}), dict) else {}
    lines = [
        "",
        "=" * 64,
        f"  历史信号镜像诊断  {report.get('snapshot_date', '')}",
        f"  history_group_id: {report.get('history_group_id', '') or '<auto/latest>'}",
        "=" * 64,
        "",
        f"  状态: {report.get('status', 'missing')}",
        f"  市场信号: {market.get('signal', '')}    决策: {decision.get('action', decision.get('decision', ''))}",
        f"  候选数: {report.get('candidate_count', 0)}    池条目数: {report.get('pool_entry_count', 0)}",
        "",
    ]
    if report.get("missing_components"):
        lines.append(f"  缺失组件: {', '.join(report.get('missing_components', []))}")
        lines.append("")

    summary = pool_snapshot.get("summary", {}) if isinstance(pool_snapshot.get("summary", {}), dict) else {}
    if summary:
        lines.append("  池子摘要:")
        lines.append(
            f"    core={summary.get('core_count', summary.get('core', 0))} "
            f"watch={summary.get('watch_count', summary.get('watch', 0))} "
            f"other={summary.get('other_count', summary.get('avoid', 0))}"
        )
        lines.append("")

    code_diagnosis = report.get("code_diagnosis", {}) if isinstance(report.get("code_diagnosis", {}), dict) else {}
    if code_diagnosis:
        lines.append(f"  个股诊断: {code_diagnosis.get('code', '')}")
        lines.append(
            f"    状态={code_diagnosis.get('status', '')} "
            f"候选命中={code_diagnosis.get('candidate_present', False)} "
            f"池命中={code_diagnosis.get('pool_present', False)} "
            f"bucket={code_diagnosis.get('pool_bucket', '') or '-'}"
        )
        lines.append(f"    结论: {code_diagnosis.get('reason', '') or '无'}")
        candidate = code_diagnosis.get("candidate", {}) if isinstance(code_diagnosis.get("candidate", {}), dict) else {}
        if candidate:
            score = candidate.get("score", candidate.get("total_score", 0))
            lines.append(
                f"    候选详情: rank={code_diagnosis.get('candidate_rank', 0)} "
                f"score={float(score or 0):.2f} veto={','.join(candidate.get('veto_signals', []) or []) or '-'}"
            )
        pool_entry = code_diagnosis.get("pool_entry", {}) if isinstance(code_diagnosis.get("pool_entry", {}), dict) else {}
        if pool_entry:
            lines.append(
                f"    池条目: bucket={pool_entry.get('bucket', '')} "
                f"score={float(pool_entry.get('total_score', 0) or 0):.2f} "
                f"note={pool_entry.get('note', '') or '-'}"
            )
        lines.append("")

    available_groups = report.get("available_history_groups", []) or []
    timeline = report.get("market_timeline", []) if isinstance(report.get("market_timeline", []), list) else []
    if timeline:
        lines.append("  时点摘要:")
        for item in timeline[:8]:
            lines.append(
                f"    - {item.get('timepoint', '') or '-'} "
                f"{item.get('pipeline', '') or '-'} "
                f"signal={item.get('signal', '') or '-'} "
                f"{item.get('updated_at', '')}"
            )
        lines.append("")

    if available_groups:
        lines.append("  当日可选运行:")
        for item in available_groups[:5]:
            lines.append(
                f"    - {item.get('history_group_id', '')} "
                f"({item.get('timepoint', '') or '-'}) "
                f"[{','.join(item.get('components', []) or [])}] "
                f"{item.get('updated_at', '')}"
            )
        lines.append("")

    across_groups = report.get("code_diagnosis_across_groups", []) if isinstance(report.get("code_diagnosis_across_groups", []), list) else []
    if across_groups:
        lines.append("  单股跨组对比:")
        for item in across_groups[:8]:
            lines.append(
                f"    - {item.get('timepoint', '') or '-'} "
                f"{item.get('status', '')} "
                f"signal={item.get('market_signal', '') or '-'} "
                f"bucket={item.get('pool_bucket', '') or '-'}"
            )
        lines.append("")

    code_scan = report.get("code_scan", []) if isinstance(report.get("code_scan", []), list) else []
    if code_scan:
        lines.append("  批量代码扫描:")
        for item in code_scan[:10]:
            lines.append(
                f"    - {item.get('code', '')} "
                f"current={item.get('current_group_status', '') or '-'} "
                f"screener={item.get('screener_status', '') or '-'} "
                f"selected={item.get('selected_group_count', 0)} "
                f"candidate={item.get('candidate_group_count', 0)}"
            )
        lines.append("")

    candidates = candidate_snapshot.get("candidates", []) if isinstance(candidate_snapshot.get("candidates", []), list) else []
    if candidates:
        lines.append("  候选预览:")
        for item in candidates[:5]:
            score = item.get("score", item.get("total_score", 0))
            lines.append(
                f"    - {item.get('code', '')} {item.get('name', '')} "
                f"score={float(score or 0):.2f} "
                f"veto={','.join(item.get('veto_signals', []) or []) or '-'}"
            )
        if candidate_snapshot.get("candidates_truncated", False):
            lines.append("    - ...")
        lines.append("")
    return "\n".join(lines)


def _safe_pct(numerator: int | float, denominator: int | float) -> float:
    if float(denominator or 0) <= 0:
        return 0.0
    return round(float(numerator or 0) / float(denominator or 0) * 100, 1)


def _dedupe_text_list(items: list[Any]) -> list[str]:
    seen: set[str] = set()
    values: list[str] = []
    for item in items or []:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        values.append(value)
    return values


def _active_hard_veto_signals(row: dict[str, Any], params: dict[str, Any]) -> list[str]:
    active_rules = {
        str(item).strip()
        for item in params.get("veto_rules", []) or []
        if str(item).strip()
    }
    return [
        signal
        for signal in _dedupe_text_list(row.get("veto_signals", []) if isinstance(row, dict) else [])
        if signal in active_rules and signal != "consecutive_outflow_warn"
    ]


def _evaluate_future_path(
    rows: list[dict[str, Any]],
    anchor_idx: int,
    *,
    lookahead_days: int,
) -> dict[str, Any]:
    anchor = rows[anchor_idx]
    anchor_close = float(anchor.get("close", 0) or 0)
    if anchor_close <= 0:
        return {}
    end_idx = min(len(rows) - 1, anchor_idx + max(1, int(lookahead_days or 1)))
    if end_idx <= anchor_idx:
        return {}

    peak_gain_ratio = -math.inf
    peak_date = ""
    trough_drawdown_ratio = math.inf
    trough_date = ""

    for row in rows[anchor_idx + 1:end_idx + 1]:
        high = float(row.get("high", row.get("close", 0)) or 0)
        low = float(row.get("low", row.get("close", 0)) or 0)
        if high > 0:
            gain_ratio = high / anchor_close - 1
            if gain_ratio > peak_gain_ratio:
                peak_gain_ratio = gain_ratio
                peak_date = str(row.get("date", "") or "")
        if low > 0:
            drawdown_ratio = low / anchor_close - 1
            if drawdown_ratio < trough_drawdown_ratio:
                trough_drawdown_ratio = drawdown_ratio
                trough_date = str(row.get("date", "") or "")

    if math.isinf(peak_gain_ratio) and peak_gain_ratio < 0:
        peak_gain_ratio = 0.0
    if math.isinf(trough_drawdown_ratio) and trough_drawdown_ratio > 0:
        trough_drawdown_ratio = 0.0

    end_close = float(rows[end_idx].get("close", anchor_close) or anchor_close)
    end_close_ratio = end_close / anchor_close - 1 if anchor_close > 0 else 0.0
    return {
        "window_end_date": str(rows[end_idx].get("date", "") or ""),
        "peak_date": peak_date,
        "trough_date": trough_date,
        "peak_gain_ratio": float(peak_gain_ratio),
        "worst_drawdown_ratio": float(trough_drawdown_ratio),
        "end_close_ratio": float(end_close_ratio),
        "peak_gain_pct": round(float(peak_gain_ratio) * 100, 2),
        "worst_drawdown_pct": round(float(trough_drawdown_ratio) * 100, 2),
        "end_close_return_pct": round(float(end_close_ratio) * 100, 2),
    }


def _categorize_veto_outcome(*, opportunity_hit: bool, risk_hit: bool) -> str:
    if opportunity_hit and risk_hit:
        return "mixed"
    if opportunity_hit:
        return "missed_opportunity"
    if risk_hit:
        return "risk_blocked"
    return "neutral"


def _summarize_veto_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    trigger_count = len(samples)
    opportunity_hit_count = sum(1 for item in samples if item.get("opportunity_hit"))
    risk_hit_count = sum(1 for item in samples if item.get("risk_hit"))
    opportunity_only_count = sum(1 for item in samples if item.get("outcome") == "missed_opportunity")
    risk_only_count = sum(1 for item in samples if item.get("outcome") == "risk_blocked")
    both_hit_count = sum(1 for item in samples if item.get("outcome") == "mixed")
    neutral_count = sum(1 for item in samples if item.get("outcome") == "neutral")
    avg_peak_gain_pct = round(
        sum(float(item.get("peak_gain_pct", 0) or 0) for item in samples) / trigger_count,
        2,
    ) if trigger_count else 0.0
    avg_worst_drawdown_pct = round(
        sum(float(item.get("worst_drawdown_pct", 0) or 0) for item in samples) / trigger_count,
        2,
    ) if trigger_count else 0.0
    pure_false_kill_rate_pct = _safe_pct(opportunity_only_count, trigger_count)
    pure_risk_intercept_rate_pct = _safe_pct(risk_only_count, trigger_count)
    return {
        "trigger_count": trigger_count,
        "opportunity_hit_count": opportunity_hit_count,
        "risk_hit_count": risk_hit_count,
        "opportunity_only_count": opportunity_only_count,
        "risk_only_count": risk_only_count,
        "both_hit_count": both_hit_count,
        "neutral_count": neutral_count,
        "false_kill_rate_pct": _safe_pct(opportunity_hit_count, trigger_count),
        "pure_false_kill_rate_pct": pure_false_kill_rate_pct,
        "risk_intercept_rate_pct": _safe_pct(risk_hit_count, trigger_count),
        "pure_risk_intercept_rate_pct": pure_risk_intercept_rate_pct,
        "mixed_rate_pct": _safe_pct(both_hit_count, trigger_count),
        "neutral_rate_pct": _safe_pct(neutral_count, trigger_count),
        "avg_peak_gain_pct": avg_peak_gain_pct,
        "avg_worst_drawdown_pct": avg_worst_drawdown_pct,
        "net_value_pct": round(pure_risk_intercept_rate_pct - pure_false_kill_rate_pct, 1),
    }


def _build_veto_analysis_findings(
    summary: dict[str, Any],
    effective_rules: list[dict[str, Any]],
    too_strict_rules: list[dict[str, Any]],
) -> list[str]:
    findings: list[str] = []
    trigger_count = int(summary.get("trigger_count", 0) or 0)
    if trigger_count:
        findings.append(
            f"共分析 {trigger_count} 次 veto 触发，纯风险拦截 {summary.get('pure_risk_intercept_rate_pct', 0):.1f}% ，"
            f"纯误杀机会 {summary.get('pure_false_kill_rate_pct', 0):.1f}%"
        )
    if effective_rules:
        top = effective_rules[0]
        findings.append(
            f"{top['rule']} 最有效：{top['trigger_count']} 次触发里，纯拦截风险 {top['pure_risk_intercept_rate_pct']:.1f}% ，"
            f"纯误杀 {top['pure_false_kill_rate_pct']:.1f}%"
        )
    if too_strict_rules:
        top = too_strict_rules[0]
        findings.append(
            f"{top['rule']} 偏严：{top['trigger_count']} 次触发里，纯误杀机会 {top['pure_false_kill_rate_pct']:.1f}% ，"
            f"纯拦截风险 {top['pure_risk_intercept_rate_pct']:.1f}%"
        )
    return findings


def run_veto_rule_analysis(
    stock_codes: list[str],
    start: str,
    end: str,
    *,
    index_code: str = "system",
    total_capital: float | None = None,
    strategy_params: dict | None = None,
    lookahead_days: int = 20,
    opportunity_gain_pct: float = 0.15,
    risk_drawdown_pct: float = 0.08,
    sample_limit: int = 5,
) -> dict[str, Any]:
    """Analyze how effective each hard veto rule is across one or more stocks."""
    codes = [str(code).strip() for code in stock_codes if str(code).strip()]
    if not codes:
        raise ValueError("stock_codes must not be empty")

    lookahead_days = max(1, int(lookahead_days or 20))
    opportunity_gain_pct = float(opportunity_gain_pct or 0)
    risk_drawdown_pct = abs(float(risk_drawdown_pct or 0))

    trigger_samples: list[dict[str, Any]] = []
    stock_summaries: list[dict[str, Any]] = []
    mode_counter: Counter[str] = Counter()
    history_days = 0
    proxy_days = 0
    candidate_day_count = 0
    trading_day_count = 0
    veto_day_count = 0
    sample_params: dict[str, Any] | None = None

    for code in codes:
        fixture = build_replay_fixture(
            stock_code=code,
            start=start,
            end=end,
            index_code=index_code,
            total_capital=total_capital if total_capital is not None else float(get_strategy().get("capital", 450286)),
            strategy_params=strategy_params,
        )
        params = dict(fixture.get("params", {}) if isinstance(fixture.get("params", {}), dict) else {})
        rows = _build_single_stock_daily_rows(code, fixture.get("daily_data", {}))
        if sample_params is None:
            sample_params = params

        fidelity = fixture.get("_meta", {}).get("data_fidelity", {}) if isinstance(fixture.get("_meta", {}), dict) else {}
        mode = str(fidelity.get("mode", "") or "proxy_replay")
        mode_counter[mode] += 1
        history_days += int(fidelity.get("history_days", 0) or 0)
        proxy_days += int(fidelity.get("proxy_days", 0) or 0)

        stock_candidate_days = 0
        stock_veto_days = 0
        stock_trigger_count = 0
        stock_rules: set[str] = set()

        for idx, row in enumerate(rows):
            if row.get("candidate_present"):
                stock_candidate_days += 1
            active_vetoes = _active_hard_veto_signals(row, params)
            if not active_vetoes:
                continue
            future_path = _evaluate_future_path(rows, idx, lookahead_days=lookahead_days)
            if not future_path:
                continue
            stock_veto_days += 1
            stock_trigger_count += len(active_vetoes)
            stock_rules.update(active_vetoes)

            opportunity_hit = bool(future_path["peak_gain_ratio"] >= opportunity_gain_pct)
            risk_hit = bool(future_path["worst_drawdown_ratio"] <= -risk_drawdown_pct)
            for rule in active_vetoes:
                trigger_samples.append(
                    {
                        "rule": rule,
                        "code": code,
                        "date": str(row.get("date", "") or ""),
                        "market_signal": str(row.get("market_signal", "") or ""),
                        "score": round(float(row.get("score", 0) or 0), 2),
                        "entry_signal": bool(row.get("entry_signal", False)),
                        "triggered_vetoes": list(active_vetoes),
                        "snapshot_source": str(row.get("snapshot_source", "") or ""),
                        "history_group_id": str(row.get("history_group_id", "") or ""),
                        "peak_date": future_path.get("peak_date", ""),
                        "trough_date": future_path.get("trough_date", ""),
                        "window_end_date": future_path.get("window_end_date", ""),
                        "peak_gain_pct": future_path.get("peak_gain_pct", 0.0),
                        "worst_drawdown_pct": future_path.get("worst_drawdown_pct", 0.0),
                        "end_close_return_pct": future_path.get("end_close_return_pct", 0.0),
                        "opportunity_hit": opportunity_hit,
                        "risk_hit": risk_hit,
                        "outcome": _categorize_veto_outcome(
                            opportunity_hit=opportunity_hit,
                            risk_hit=risk_hit,
                        ),
                    }
                )

        stock_summaries.append(
            {
                "code": code,
                "data_fidelity_mode": mode,
                "trading_day_count": len(rows),
                "candidate_day_count": stock_candidate_days,
                "veto_day_count": stock_veto_days,
                "trigger_count": stock_trigger_count,
                "history_days": int(fidelity.get("history_days", 0) or 0),
                "proxy_days": int(fidelity.get("proxy_days", 0) or 0),
                "active_veto_rules": sorted(stock_rules),
            }
        )
        trading_day_count += len(rows)
        candidate_day_count += stock_candidate_days
        veto_day_count += stock_veto_days

    rule_samples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in trigger_samples:
        rule_samples[str(item.get("rule", "")).strip()].append(item)

    rule_stats: list[dict[str, Any]] = []
    for rule, items in rule_samples.items():
        stat = _summarize_veto_samples(items)
        stat.update(
            {
                "rule": rule,
                "stock_count": len({str(item.get('code', '')).strip() for item in items if str(item.get("code", "")).strip()}),
                "sample_codes": sorted({str(item.get("code", "")).strip() for item in items if str(item.get("code", "")).strip()})[:10],
            }
        )
        rule_stats.append(stat)

    rule_stats.sort(
        key=lambda item: (
            -float(item.get("net_value_pct", 0) or 0),
            -float(item.get("pure_risk_intercept_rate_pct", 0) or 0),
            int(item.get("trigger_count", 0) or 0),
            str(item.get("rule", "") or ""),
        )
    )

    effective_rules = sorted(
        rule_stats,
        key=lambda item: (
            -float(item.get("net_value_pct", 0) or 0),
            -float(item.get("pure_risk_intercept_rate_pct", 0) or 0),
            -int(item.get("trigger_count", 0) or 0),
            str(item.get("rule", "") or ""),
        ),
    )
    too_strict_rules = sorted(
        rule_stats,
        key=lambda item: (
            float(item.get("net_value_pct", 0) or 0),
            -float(item.get("pure_false_kill_rate_pct", 0) or 0),
            -int(item.get("trigger_count", 0) or 0),
            str(item.get("rule", "") or ""),
        ),
    )

    summary = _summarize_veto_samples(trigger_samples)
    top_missed_opportunities = sorted(
        [item for item in trigger_samples if item.get("outcome") == "missed_opportunity"],
        key=lambda item: (
            -float(item.get("peak_gain_pct", 0) or 0),
            float(item.get("worst_drawdown_pct", 0) or 0),
            str(item.get("date", "") or ""),
        ),
    )[: max(1, int(sample_limit or 5))]
    top_blocked_losses = sorted(
        [item for item in trigger_samples if item.get("outcome") == "risk_blocked"],
        key=lambda item: (
            float(item.get("worst_drawdown_pct", 0) or 0),
            float(item.get("peak_gain_pct", 0) or 0),
            str(item.get("date", "") or ""),
        ),
    )[: max(1, int(sample_limit or 5))]

    return {
        "command": "backtest",
        "action": "veto_rule_analysis",
        "status": "ok",
        "codes": codes,
        "start": start,
        "end": end,
        "index_code": index_code,
        "params": sample_params or {},
        "analysis_window": {
            "lookahead_days": lookahead_days,
            "opportunity_gain_pct": opportunity_gain_pct,
            "risk_drawdown_pct": risk_drawdown_pct,
        },
        "coverage": {
            "stock_count": len(codes),
            "trading_day_count": trading_day_count,
            "candidate_day_count": candidate_day_count,
            "veto_day_count": veto_day_count,
            "trigger_count": len(trigger_samples),
            "history_days": history_days,
            "proxy_days": proxy_days,
            "data_fidelity_mode_counts": dict(sorted(mode_counter.items())),
        },
        "summary": summary,
        "rule_stats": rule_stats,
        "effective_rules": effective_rules[:5],
        "too_strict_rules": too_strict_rules[:5],
        "top_missed_opportunities": top_missed_opportunities,
        "top_blocked_losses": top_blocked_losses,
        "stock_summaries": stock_summaries,
        "findings": _build_veto_analysis_findings(summary, effective_rules, too_strict_rules),
    }


def render_veto_rule_analysis_report(report: dict[str, Any]) -> str:
    """Render a concise text report for veto rule effectiveness."""
    coverage = report.get("coverage", {}) if isinstance(report.get("coverage", {}), dict) else {}
    summary = report.get("summary", {}) if isinstance(report.get("summary", {}), dict) else {}
    analysis_window = report.get("analysis_window", {}) if isinstance(report.get("analysis_window", {}), dict) else {}
    lines = [
        "",
        "=" * 64,
        f"  Veto 规则分析  {report.get('start', '')} ~ {report.get('end', '')}",
        "=" * 64,
        "",
        f"  股票数: {coverage.get('stock_count', 0)}    veto 触发日: {coverage.get('veto_day_count', 0)}    veto 触发次数: {coverage.get('trigger_count', 0)}",
        f"  窗口: {analysis_window.get('lookahead_days', 0)} 天    机会阈值: +{float(analysis_window.get('opportunity_gain_pct', 0) or 0) * 100:.1f}%    "
        f"风险阈值: -{float(analysis_window.get('risk_drawdown_pct', 0) or 0) * 100:.1f}%",
        f"  数据覆盖: history_days={coverage.get('history_days', 0)} proxy_days={coverage.get('proxy_days', 0)}",
        "",
        "  总体结论:",
        f"    - 纯风险拦截 {summary.get('pure_risk_intercept_rate_pct', 0):.1f}%    纯误杀机会 {summary.get('pure_false_kill_rate_pct', 0):.1f}%    "
        f"混合 {summary.get('mixed_rate_pct', 0):.1f}%",
        f"    - 平均未来最高涨幅 {summary.get('avg_peak_gain_pct', 0):+.2f}%    平均未来最大回撤 {summary.get('avg_worst_drawdown_pct', 0):+.2f}%",
        "",
        "  主要发现:",
    ]
    for item in report.get("findings", [])[:5]:
        lines.append(f"    - {item}")
    if not report.get("findings"):
        lines.append("    - 暂无显著结论")

    lines.append("")
    lines.append("  最有效规则:")
    for item in report.get("effective_rules", [])[:5]:
        lines.append(
            f"    - {item.get('rule', '')}: 触发 {item.get('trigger_count', 0)} 次, "
            f"纯拦截 {item.get('pure_risk_intercept_rate_pct', 0):.1f}%, "
            f"纯误杀 {item.get('pure_false_kill_rate_pct', 0):.1f}%, "
            f"净值 {item.get('net_value_pct', 0):+.1f}pct"
        )
    if not report.get("effective_rules"):
        lines.append("    - 暂无数据")

    lines.append("")
    lines.append("  偏严规则:")
    for item in report.get("too_strict_rules", [])[:5]:
        lines.append(
            f"    - {item.get('rule', '')}: 触发 {item.get('trigger_count', 0)} 次, "
            f"纯误杀 {item.get('pure_false_kill_rate_pct', 0):.1f}%, "
            f"纯拦截 {item.get('pure_risk_intercept_rate_pct', 0):.1f}%, "
            f"净值 {item.get('net_value_pct', 0):+.1f}pct"
        )
    if not report.get("too_strict_rules"):
        lines.append("    - 暂无数据")

    lines.append("")
    lines.append("  误杀样本:")
    for item in report.get("top_missed_opportunities", [])[:3]:
        lines.append(
            f"    - {item.get('code', '')} {item.get('date', '')} "
            f"rule={item.get('rule', '')} "
            f"未来最高 {item.get('peak_gain_pct', 0):+.2f}% "
            f"最大回撤 {item.get('worst_drawdown_pct', 0):+.2f}%"
        )
    if not report.get("top_missed_opportunities"):
        lines.append("    - 暂无样本")

    lines.append("")
    lines.append("  拦截风险样本:")
    for item in report.get("top_blocked_losses", [])[:3]:
        lines.append(
            f"    - {item.get('code', '')} {item.get('date', '')} "
            f"rule={item.get('rule', '')} "
            f"未来最高 {item.get('peak_gain_pct', 0):+.2f}% "
            f"最大回撤 {item.get('worst_drawdown_pct', 0):+.2f}%"
        )
    if not report.get("top_blocked_losses"):
        lines.append("    - 暂无样本")
    lines.append("")
    return "\n".join(lines)


def _coerce_holding_windows(values: list[int] | tuple[int, ...] | None) -> list[int]:
    windows: list[int] = []
    seen: set[int] = set()
    for item in values or [5, 10, 20]:
        try:
            day = int(item)
        except Exception:
            continue
        if day <= 0 or day in seen:
            continue
        seen.add(day)
        windows.append(day)
    return sorted(windows) or [5, 10, 20]


def _bucket_targets(bucket: str) -> set[str]:
    normalized = str(bucket or "core").strip().lower()
    if normalized == "all":
        return {"core", "watch"}
    if normalized == "other":
        return {"avoid", "other"}
    return {normalized or "core"}


def _load_pool_snapshots_for_range(
    start: str,
    end: str,
    *,
    pipeline: str = "stock_screener",
    history_limit: int | None = None,
) -> list[dict[str, Any]]:
    start_date = datetime.strptime(start, "%Y-%m-%d").date()
    end_date = datetime.strptime(end, "%Y-%m-%d").date()
    estimated_days = max(1, (end_date - start_date).days + 1)
    resolved_limit = max(5000, estimated_days * 20) if history_limit is None else max(1, int(history_limit))
    payload = load_pool_snapshot_history(limit=resolved_limit)
    latest_per_day: dict[str, dict[str, Any]] = {}
    for item in payload.get("items", []) or []:
        snapshot_date = str(item.get("snapshot_date", "") or "").strip()
        if not snapshot_date or snapshot_date < start or snapshot_date > end:
            continue
        if pipeline and str(item.get("pipeline", "") or "").strip() != pipeline:
            continue
        latest_per_day.setdefault(snapshot_date, item)
    return [latest_per_day[day] for day in sorted(latest_per_day)]


def _extract_pool_entry_events(
    snapshots: list[dict[str, Any]],
    *,
    bucket: str,
    stock_codes: list[str] | None = None,
) -> list[dict[str, Any]]:
    target_buckets = _bucket_targets(bucket)
    code_filter = {
        _normalize_replay_code(code)
        for code in stock_codes or []
        if _normalize_replay_code(code)
    }
    previous_bucket_by_code: dict[str, str] = {}
    events: list[dict[str, Any]] = []

    for snapshot in snapshots:
        current_bucket_by_code: dict[str, str] = {}
        current_entry_by_code: dict[str, dict[str, Any]] = {}
        for entry in snapshot.get("entries", []) if isinstance(snapshot.get("entries", []), list) else []:
            code = _normalize_replay_code(entry.get("code", ""))
            if not code:
                continue
            if code_filter and code not in code_filter:
                continue
            bucket_name = str(entry.get("bucket", "") or "").strip().lower() or "other"
            current_bucket_by_code[code] = bucket_name
            current_entry_by_code[code] = dict(entry)

        for code, bucket_name in current_bucket_by_code.items():
            if bucket_name not in target_buckets:
                continue
            previous_bucket = previous_bucket_by_code.get(code, "")
            if previous_bucket in target_buckets:
                continue
            entry = current_entry_by_code.get(code, {})
            events.append(
                {
                    "code": str(entry.get("code", code) or code).strip(),
                    "name": str(entry.get("name", code) or code).strip(),
                    "bucket": bucket_name,
                    "entry_date": str(snapshot.get("snapshot_date", "") or ""),
                    "history_group_id": str(snapshot.get("history_group_id", "") or ""),
                    "pipeline": str(snapshot.get("pipeline", "") or ""),
                    "updated_at": str(snapshot.get("updated_at", "") or ""),
                    "entry_score": round(float(entry.get("total_score", 0) or 0), 2),
                    "technical_score": round(float(entry.get("technical_score", 0) or 0), 2),
                    "fundamental_score": round(float(entry.get("fundamental_score", 0) or 0), 2),
                    "flow_score": round(float(entry.get("flow_score", 0) or 0), 2),
                    "sentiment_score": round(float(entry.get("sentiment_score", 0) or 0), 2),
                    "note": str(entry.get("note", "") or "").strip(),
                    "data_quality": str(entry.get("data_quality", "ok") or "ok").strip(),
                    "veto_signals": _dedupe_text_list(entry.get("veto_signals", [])),
                }
            )
        previous_bucket_by_code = current_bucket_by_code
    return events


def _load_price_frames_for_codes(
    codes: list[str],
    *,
    start: str,
    end: str,
    max_window: int,
) -> dict[str, pd.DataFrame]:
    end_date = datetime.strptime(end, "%Y-%m-%d") + timedelta(days=max_window * 3 + 30)
    fetch_end = end_date.strftime("%Y-%m-%d")
    frames: dict[str, pd.DataFrame] = {}
    for code in codes:
        frame = _fetch_daily(_to_bs_code(code), start, fetch_end)
        if frame.empty:
            continue
        frame = frame.copy().sort_values("date").reset_index(drop=True)
        frame["_date_str"] = frame["date"].dt.strftime("%Y-%m-%d")
        frame.attrs["date_index"] = {value: idx for idx, value in enumerate(frame["_date_str"].tolist())}
        frames[code] = frame
    return frames


def _event_window_metrics(
    price_frame: pd.DataFrame,
    entry_date: str,
    *,
    holding_windows: list[int],
) -> tuple[float | None, dict[int, dict[str, Any]]]:
    if price_frame.empty:
        return None, {}
    date_index = price_frame.attrs.get("date_index", {})
    idx = date_index.get(entry_date)
    if idx is None:
        return None, {}
    entry_close = float(price_frame.iloc[idx]["close"] or 0)
    if entry_close <= 0:
        return None, {}

    metrics: dict[int, dict[str, Any]] = {}
    for window_days in holding_windows:
        target_idx = idx + int(window_days)
        if target_idx >= len(price_frame):
            metrics[window_days] = {"available": False, "window_days": window_days}
            continue
        future = price_frame.iloc[idx + 1:target_idx + 1]
        if future.empty:
            metrics[window_days] = {"available": False, "window_days": window_days}
            continue
        target_row = price_frame.iloc[target_idx]
        target_close = float(target_row["close"] or 0)
        max_high = float(future["high"].max() or target_close or entry_close)
        min_low = float(future["low"].min() or target_close or entry_close)
        metrics[window_days] = {
            "available": True,
            "window_days": window_days,
            "end_date": str(target_row["_date_str"] or ""),
            "return_pct": round((target_close / entry_close - 1) * 100, 2),
            "max_gain_pct": round((max_high / entry_close - 1) * 100, 2),
            "max_drawdown_pct": round((min_low / entry_close - 1) * 100, 2),
        }
    return round(entry_close, 4), metrics


def _build_pool_window_statistics(events: list[dict[str, Any]], window_days: int) -> dict[str, Any]:
    window_metrics = [
        item["metrics_by_window"][window_days]
        for item in events
        if window_days in item.get("metrics_by_window", {})
        and item["metrics_by_window"][window_days].get("available")
    ]
    sample_count = len(window_metrics)
    if not sample_count:
        return {
            "window_days": window_days,
            "sample_count": 0,
            "positive_count": 0,
            "positive_rate_pct": 0.0,
            "avg_return_pct": 0.0,
            "median_return_pct": 0.0,
            "avg_max_gain_pct": 0.0,
            "avg_max_drawdown_pct": 0.0,
            "gain_10pct_hit_rate_pct": 0.0,
            "drawdown_8pct_hit_rate_pct": 0.0,
        }

    returns = [float(item.get("return_pct", 0) or 0) for item in window_metrics]
    max_gains = [float(item.get("max_gain_pct", 0) or 0) for item in window_metrics]
    max_drawdowns = [float(item.get("max_drawdown_pct", 0) or 0) for item in window_metrics]
    positive_count = sum(1 for value in returns if value > 0)
    gain_10_count = sum(1 for value in max_gains if value >= 10)
    drawdown_8_count = sum(1 for value in max_drawdowns if value <= -8)
    return {
        "window_days": window_days,
        "sample_count": sample_count,
        "positive_count": positive_count,
        "positive_rate_pct": _safe_pct(positive_count, sample_count),
        "avg_return_pct": round(sum(returns) / sample_count, 2),
        "median_return_pct": round(float(statistics.median(returns)), 2),
        "avg_max_gain_pct": round(sum(max_gains) / sample_count, 2),
        "avg_max_drawdown_pct": round(sum(max_drawdowns) / sample_count, 2),
        "gain_10pct_hit_rate_pct": _safe_pct(gain_10_count, sample_count),
        "drawdown_8pct_hit_rate_pct": _safe_pct(drawdown_8_count, sample_count),
    }


def _aggregate_pool_stock_summaries(events: list[dict[str, Any]], primary_window_days: int) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for event in events:
        metrics = event.get("metrics_by_window", {}).get(primary_window_days, {})
        if not metrics.get("available"):
            continue
        code = str(event.get("code", "") or "").strip()
        if not code:
            continue
        bucket = grouped.setdefault(
            code,
            {
                "code": code,
                "name": str(event.get("name", code) or code).strip(),
                "sample_count": 0,
                "total_return_pct": 0.0,
                "total_max_gain_pct": 0.0,
                "total_max_drawdown_pct": 0.0,
                "positive_count": 0,
            },
        )
        bucket["sample_count"] += 1
        bucket["total_return_pct"] += float(metrics.get("return_pct", 0) or 0)
        bucket["total_max_gain_pct"] += float(metrics.get("max_gain_pct", 0) or 0)
        bucket["total_max_drawdown_pct"] += float(metrics.get("max_drawdown_pct", 0) or 0)
        if float(metrics.get("return_pct", 0) or 0) > 0:
            bucket["positive_count"] += 1

    items = []
    for bucket in grouped.values():
        count = max(int(bucket["sample_count"] or 0), 1)
        items.append(
            {
                "code": bucket["code"],
                "name": bucket["name"],
                "sample_count": bucket["sample_count"],
                "avg_return_pct": round(bucket["total_return_pct"] / count, 2),
                "avg_max_gain_pct": round(bucket["total_max_gain_pct"] / count, 2),
                "avg_max_drawdown_pct": round(bucket["total_max_drawdown_pct"] / count, 2),
                "positive_rate_pct": _safe_pct(bucket["positive_count"], count),
            }
        )
    return sorted(items, key=lambda item: (-float(item.get("avg_return_pct", 0) or 0), -int(item.get("sample_count", 0) or 0), str(item.get("code", "") or "")))


def _build_pool_performance_findings(
    *,
    bucket: str,
    window_stats: list[dict[str, Any]],
    top_stocks: list[dict[str, Any]],
    bottom_stocks: list[dict[str, Any]],
) -> list[str]:
    label = {"core": "核心池", "watch": "观察池", "all": "入池"}.get(str(bucket or "").strip().lower(), str(bucket or "入池"))
    findings: list[str] = []
    if window_stats:
        primary = max(window_stats, key=lambda item: int(item.get("window_days", 0) or 0))
        if int(primary.get("sample_count", 0) or 0) > 0:
            findings.append(
                f"{label}后 {primary['window_days']} 日平均收益 {primary['avg_return_pct']:+.2f}% ，正收益占比 {primary['positive_rate_pct']:.1f}%"
            )
            findings.append(
                f"{label}后 {primary['window_days']} 日平均最高上冲 {primary['avg_max_gain_pct']:+.2f}% ，平均最大回撤 {primary['avg_max_drawdown_pct']:+.2f}%"
            )
    if top_stocks:
        top = top_stocks[0]
        findings.append(
            f"{top['code']} 表现最好，样本 {top['sample_count']} 次，平均收益 {top['avg_return_pct']:+.2f}%"
        )
    if bottom_stocks:
        bottom = bottom_stocks[0]
        findings.append(
            f"{bottom['code']} 表现最弱，样本 {bottom['sample_count']} 次，平均收益 {bottom['avg_return_pct']:+.2f}%"
        )
    return findings


def run_pool_entry_performance_analysis(
    *,
    start: str,
    end: str,
    bucket: str = "core",
    holding_windows: list[int] | tuple[int, ...] | None = None,
    stock_codes: list[str] | None = None,
    pipeline: str = "stock_screener",
    history_limit: int | None = None,
    sample_limit: int = 5,
) -> dict[str, Any]:
    """Analyze forward N-day performance after a stock newly enters the pool."""
    windows = _coerce_holding_windows(list(holding_windows or [5, 10, 20]))
    snapshots = _load_pool_snapshots_for_range(
        start,
        end,
        pipeline=str(pipeline or "stock_screener").strip(),
        history_limit=history_limit,
    )
    result = {
        "command": "backtest",
        "action": "pool_entry_performance",
        "status": "ok",
        "start": start,
        "end": end,
        "bucket": str(bucket or "core").strip().lower() or "core",
        "pipeline": str(pipeline or "stock_screener").strip(),
        "holding_windows": windows,
        "stock_codes": [str(code).strip() for code in stock_codes or [] if str(code).strip()],
        "coverage": {
            "snapshot_count": len(snapshots),
            "entry_event_count": 0,
            "priced_event_count": 0,
            "missing_price_event_count": 0,
            "stock_count": 0,
            "history_limit_used": max(5000, (datetime.strptime(end, "%Y-%m-%d").date() - datetime.strptime(start, "%Y-%m-%d").date()).days * 20 + 20) if history_limit is None else max(1, int(history_limit)),
        },
        "window_statistics": [],
        "findings": [],
        "top_stocks": [],
        "bottom_stocks": [],
        "top_events": [],
        "bottom_events": [],
        "events_preview": [],
    }
    if not snapshots:
        result["status"] = "warning"
        result["error"] = "no_pool_snapshots_in_range"
        return result

    events = _extract_pool_entry_events(
        snapshots,
        bucket=result["bucket"],
        stock_codes=stock_codes,
    )
    result["coverage"]["entry_event_count"] = len(events)
    result["coverage"]["stock_count"] = len({str(item.get("code", "")).strip() for item in events if str(item.get("code", "")).strip()})
    if not events:
        result["status"] = "warning"
        result["error"] = "no_pool_entry_events"
        return result

    price_frames = _load_price_frames_for_codes(
        sorted({str(item.get("code", "")).strip() for item in events if str(item.get("code", "")).strip()}),
        start=start,
        end=end,
        max_window=max(windows),
    )

    enriched_events: list[dict[str, Any]] = []
    missing_price_count = 0
    for event in events:
        code = str(event.get("code", "")).strip()
        frame = price_frames.get(code)
        if frame is None:
            missing_price_count += 1
            continue
        entry_price, metrics_by_window = _event_window_metrics(
            frame,
            str(event.get("entry_date", "") or ""),
            holding_windows=windows,
        )
        if entry_price is None:
            missing_price_count += 1
            continue
        enriched = dict(event)
        enriched["entry_price"] = entry_price
        enriched["metrics_by_window"] = metrics_by_window
        enriched_events.append(enriched)

    result["coverage"]["priced_event_count"] = len(enriched_events)
    result["coverage"]["missing_price_event_count"] = missing_price_count
    if not enriched_events:
        result["status"] = "warning"
        result["error"] = "no_price_samples_for_entry_events"
        return result

    result["window_statistics"] = [
        _build_pool_window_statistics(enriched_events, window_days)
        for window_days in windows
    ]
    primary_window = max(windows)
    stock_summaries = _aggregate_pool_stock_summaries(enriched_events, primary_window)
    result["top_stocks"] = stock_summaries[: max(1, int(sample_limit or 5))]
    result["bottom_stocks"] = sorted(
        stock_summaries,
        key=lambda item: (
            float(item.get("avg_return_pct", 0) or 0),
            -int(item.get("sample_count", 0) or 0),
            str(item.get("code", "") or ""),
        ),
    )[: max(1, int(sample_limit or 5))]

    primary_events = []
    for event in enriched_events:
        metrics = event.get("metrics_by_window", {}).get(primary_window, {})
        if not metrics.get("available"):
            continue
        primary_events.append(
            {
                "code": event.get("code", ""),
                "name": event.get("name", ""),
                "bucket": event.get("bucket", ""),
                "entry_date": event.get("entry_date", ""),
                "entry_score": event.get("entry_score", 0),
                "entry_price": event.get("entry_price", 0),
                "data_quality": event.get("data_quality", "ok"),
                **metrics,
            }
        )
    result["top_events"] = sorted(
        primary_events,
        key=lambda item: (
            -float(item.get("return_pct", 0) or 0),
            -float(item.get("max_gain_pct", 0) or 0),
            str(item.get("entry_date", "") or ""),
        ),
    )[: max(1, int(sample_limit or 5))]
    result["bottom_events"] = sorted(
        primary_events,
        key=lambda item: (
            float(item.get("return_pct", 0) or 0),
            float(item.get("max_drawdown_pct", 0) or 0),
            str(item.get("entry_date", "") or ""),
        ),
    )[: max(1, int(sample_limit or 5))]
    result["events_preview"] = primary_events[: max(1, int(sample_limit or 5))]
    result["findings"] = _build_pool_performance_findings(
        bucket=result["bucket"],
        window_stats=result["window_statistics"],
        top_stocks=result["top_stocks"],
        bottom_stocks=result["bottom_stocks"],
    )
    return result


def render_pool_entry_performance_report(report: dict[str, Any]) -> str:
    """Render a concise text report for pool entry forward performance."""
    coverage = report.get("coverage", {}) if isinstance(report.get("coverage", {}), dict) else {}
    bucket = str(report.get("bucket", "core") or "core")
    label = {"core": "核心池", "watch": "观察池", "all": "入池"}.get(bucket, bucket)
    lines = [
        "",
        "=" * 64,
        f"  {label} N日表现  {report.get('start', '')} ~ {report.get('end', '')}",
        "=" * 64,
        "",
        f"  快照数: {coverage.get('snapshot_count', 0)}    入池事件: {coverage.get('entry_event_count', 0)}    有价格样本: {coverage.get('priced_event_count', 0)}",
        f"  pipeline: {report.get('pipeline', '') or '-'}    窗口: {','.join(str(item) for item in report.get('holding_windows', []) or [])}",
        "",
        "  主要发现:",
    ]
    for item in report.get("findings", [])[:5]:
        lines.append(f"    - {item}")
    if not report.get("findings"):
        lines.append("    - 暂无显著结论")

    lines.append("")
    lines.append("  窗口统计:")
    for item in report.get("window_statistics", [])[:10]:
        lines.append(
            f"    - {item.get('window_days', 0)}日: 样本 {item.get('sample_count', 0)} "
            f"平均收益 {item.get('avg_return_pct', 0):+.2f}% "
            f"正收益占比 {item.get('positive_rate_pct', 0):.1f}% "
            f"平均最大回撤 {item.get('avg_max_drawdown_pct', 0):+.2f}%"
        )

    lines.append("")
    lines.append("  最强股票:")
    for item in report.get("top_stocks", [])[:5]:
        lines.append(
            f"    - {item.get('code', '')} {item.get('name', '')} "
            f"样本 {item.get('sample_count', 0)} "
            f"平均收益 {item.get('avg_return_pct', 0):+.2f}%"
        )
    if not report.get("top_stocks"):
        lines.append("    - 暂无样本")

    lines.append("")
    lines.append("  最弱股票:")
    for item in report.get("bottom_stocks", [])[:5]:
        lines.append(
            f"    - {item.get('code', '')} {item.get('name', '')} "
            f"样本 {item.get('sample_count', 0)} "
            f"平均收益 {item.get('avg_return_pct', 0):+.2f}%"
        )
    if not report.get("bottom_stocks"):
        lines.append("    - 暂无样本")

    lines.append("")
    lines.append("  最佳样本:")
    for item in report.get("top_events", [])[:3]:
        lines.append(
            f"    - {item.get('entry_date', '')} {item.get('code', '')} "
            f"收益 {item.get('return_pct', 0):+.2f}% "
            f"最大上冲 {item.get('max_gain_pct', 0):+.2f}% "
            f"最大回撤 {item.get('max_drawdown_pct', 0):+.2f}%"
        )
    if not report.get("top_events"):
        lines.append("    - 暂无样本")

    lines.append("")
    lines.append("  最弱样本:")
    for item in report.get("bottom_events", [])[:3]:
        lines.append(
            f"    - {item.get('entry_date', '')} {item.get('code', '')} "
            f"收益 {item.get('return_pct', 0):+.2f}% "
            f"最大上冲 {item.get('max_gain_pct', 0):+.2f}% "
            f"最大回撤 {item.get('max_drawdown_pct', 0):+.2f}%"
        )
    if not report.get("bottom_events"):
        lines.append("    - 暂无样本")
    lines.append("")
    return "\n".join(lines)


def _collect_pool_codes_from_snapshots(
    snapshots: list[dict[str, Any]],
    *,
    bucket: str,
) -> list[str]:
    target_buckets = _bucket_targets(bucket)
    codes: set[str] = set()
    for snapshot in snapshots:
        for entry in snapshot.get("entries", []) if isinstance(snapshot.get("entries", []), list) else []:
            code = _normalize_replay_code(entry.get("code", ""))
            if not code:
                continue
            bucket_name = str(entry.get("bucket", "") or "").strip().lower() or "other"
            if bucket_name in target_buckets:
                codes.add(str(entry.get("code", code) or code).strip())
    return sorted(codes)


def _build_strategy_health_findings(
    *,
    batch_result: dict[str, Any],
    veto_result: dict[str, Any],
    pool_result: dict[str, Any],
) -> list[str]:
    findings: list[str] = []
    aggregate = batch_result.get("aggregate", {}) if isinstance(batch_result.get("aggregate", {}), dict) else {}
    if aggregate:
        findings.append(
            f"批量回放 {aggregate.get('stock_count', 0)} 只股票，累计收益 {aggregate.get('total_realized_pnl', 0):+.2f}，"
            f"混合胜率 {aggregate.get('blended_win_rate', 0):.1f}% ，最差回撤 {aggregate.get('worst_max_drawdown_pct', 0):+.2f}%"
        )
    findings.extend(pool_result.get("findings", [])[:2] if isinstance(pool_result.get("findings", []), list) else [])
    findings.extend(veto_result.get("findings", [])[:2] if isinstance(veto_result.get("findings", []), list) else [])
    return findings[:6]


def run_strategy_health_report(
    *,
    start: str,
    end: str,
    bucket: str = "core",
    holding_windows: list[int] | tuple[int, ...] | None = None,
    stock_codes: list[str] | None = None,
    pipeline: str = "stock_screener",
    code_limit: int = 30,
    total_capital: float | None = None,
    strategy_params: dict | None = None,
    veto_lookahead_days: int = 20,
    veto_opportunity_gain_pct: float = 0.15,
    veto_risk_drawdown_pct: float = 0.08,
    sample_limit: int = 5,
) -> dict[str, Any]:
    """Combine pool performance, veto analysis, and batch replay into one health report."""
    snapshots = _load_pool_snapshots_for_range(start, end, pipeline=str(pipeline or "stock_screener").strip())
    explicit_codes = [str(code).strip() for code in stock_codes or [] if str(code).strip()]
    derived_codes = _collect_pool_codes_from_snapshots(snapshots, bucket=bucket) if not explicit_codes else explicit_codes
    selected_codes = derived_codes[: max(1, int(code_limit or 30))]

    result = {
        "command": "backtest",
        "action": "strategy_health_report",
        "status": "ok",
        "start": start,
        "end": end,
        "bucket": str(bucket or "core").strip().lower() or "core",
        "pipeline": str(pipeline or "stock_screener").strip(),
        "holding_windows": _coerce_holding_windows(list(holding_windows or [5, 10, 20])),
        "selected_codes": selected_codes,
        "code_source": "explicit_codes" if explicit_codes else "pool_snapshot_history",
        "code_limit": max(1, int(code_limit or 30)),
        "coverage": {
            "snapshot_count": len(snapshots),
            "available_code_count": len(derived_codes),
            "selected_code_count": len(selected_codes),
        },
    }
    if not selected_codes:
        result["status"] = "warning"
        result["error"] = "no_codes_for_strategy_health_report"
        result["pool_performance"] = run_pool_entry_performance_analysis(
            start=start,
            end=end,
            bucket=bucket,
            holding_windows=holding_windows,
            stock_codes=explicit_codes or None,
            pipeline=pipeline,
            sample_limit=sample_limit,
        )
        result["veto_analysis"] = {}
        result["batch_backtest"] = {}
        result["findings"] = result["pool_performance"].get("findings", [])[:3]
        return result

    pool_performance = run_pool_entry_performance_analysis(
        start=start,
        end=end,
        bucket=bucket,
        holding_windows=holding_windows,
        stock_codes=explicit_codes or None,
        pipeline=pipeline,
        sample_limit=sample_limit,
    )
    veto_analysis = run_veto_rule_analysis(
        stock_codes=selected_codes,
        start=start,
        end=end,
        total_capital=total_capital,
        strategy_params=strategy_params,
        lookahead_days=veto_lookahead_days,
        opportunity_gain_pct=veto_opportunity_gain_pct,
        risk_drawdown_pct=veto_risk_drawdown_pct,
        sample_limit=sample_limit,
    )
    batch_backtest = run_multi_stock_system_backtest(
        stock_codes=selected_codes,
        start=start,
        end=end,
        total_capital=total_capital,
        strategy_params=strategy_params,
    )

    statuses = {
        str(item.get("status", "ok") or "ok")
        for item in (pool_performance, veto_analysis, batch_backtest)
        if isinstance(item, dict)
    }
    result["status"] = "warning" if "warning" in statuses else "ok"
    result["pool_performance"] = pool_performance
    result["veto_analysis"] = veto_analysis
    result["batch_backtest"] = batch_backtest
    result["findings"] = _build_strategy_health_findings(
        batch_result=batch_backtest,
        veto_result=veto_analysis,
        pool_result=pool_performance,
    )
    return result


def render_strategy_health_report(report: dict[str, Any]) -> str:
    """Render a concise text report for strategy health."""
    coverage = report.get("coverage", {}) if isinstance(report.get("coverage", {}), dict) else {}
    batch_backtest = report.get("batch_backtest", {}) if isinstance(report.get("batch_backtest", {}), dict) else {}
    aggregate = batch_backtest.get("aggregate", {}) if isinstance(batch_backtest.get("aggregate", {}), dict) else {}
    pool_report = report.get("pool_performance", {}) if isinstance(report.get("pool_performance", {}), dict) else {}
    veto_report = report.get("veto_analysis", {}) if isinstance(report.get("veto_analysis", {}), dict) else {}
    lines = [
        "",
        "=" * 64,
        f"  Strategy Health  {report.get('start', '')} ~ {report.get('end', '')}",
        "=" * 64,
        "",
        f"  代码来源: {report.get('code_source', '')}    可用代码: {coverage.get('available_code_count', 0)}    选中代码: {coverage.get('selected_code_count', 0)}",
        f"  bucket: {report.get('bucket', '')}    pipeline: {report.get('pipeline', '')}",
        "",
        "  核心结论:",
    ]
    for item in report.get("findings", [])[:6]:
        lines.append(f"    - {item}")
    if not report.get("findings"):
        lines.append("    - 暂无显著结论")

    lines.append("")
    lines.append("  Batch Backtest:")
    lines.append(
        f"    - 收益 {aggregate.get('total_realized_pnl', 0):+.2f} 交易 {aggregate.get('closed_trade_count', 0)} 笔 "
        f"胜率 {aggregate.get('blended_win_rate', 0):.1f}% 最差回撤 {aggregate.get('worst_max_drawdown_pct', 0):+.2f}%"
    )

    pool_window_stats = pool_report.get("window_statistics", []) if isinstance(pool_report.get("window_statistics", []), list) else []
    lines.append("")
    lines.append("  Pool Performance:")
    for item in pool_window_stats[:3]:
        lines.append(
            f"    - {item.get('window_days', 0)}日: 平均收益 {item.get('avg_return_pct', 0):+.2f}% "
            f"正收益占比 {item.get('positive_rate_pct', 0):.1f}%"
        )
    if not pool_window_stats:
        lines.append("    - 暂无样本")

    lines.append("")
    lines.append("  Veto Analysis:")
    for item in veto_report.get("effective_rules", [])[:3]:
        lines.append(
            f"    - 有效 {item.get('rule', '')}: 纯拦截 {item.get('pure_risk_intercept_rate_pct', 0):.1f}% "
            f"纯误杀 {item.get('pure_false_kill_rate_pct', 0):.1f}%"
        )
    for item in veto_report.get("too_strict_rules", [])[:2]:
        lines.append(
            f"    - 偏严 {item.get('rule', '')}: 纯误杀 {item.get('pure_false_kill_rate_pct', 0):.1f}% "
            f"纯拦截 {item.get('pure_risk_intercept_rate_pct', 0):.1f}%"
        )
    if not veto_report.get("effective_rules") and not veto_report.get("too_strict_rules"):
        lines.append("    - 暂无样本")
    lines.append("")
    return "\n".join(lines)


def _parse_symbol_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in re.split(r"[,/，、\s]+", str(value)) if item.strip()]


def _stock_backtest_row(code: str, result: dict[str, Any]) -> dict[str, Any]:
    summary = result.get("summary", {})
    params = result.get("params", {})
    return {
        "code": code,
        "closed_trade_count": int(summary.get("closed_trade_count", 0) or 0),
        "win_count": int(summary.get("win_count", 0) or 0),
        "loss_count": int(summary.get("loss_count", 0) or 0),
        "win_rate": float(summary.get("win_rate", 0) or 0),
        "total_realized_pnl": round(float(summary.get("total_realized_pnl", 0) or 0), 2),
        "ending_equity": round(float(summary.get("ending_equity", 0) or 0), 2),
        "max_drawdown_pct": round(float(summary.get("max_drawdown_pct", 0) or 0), 4),
        "max_drawdown_date": str(summary.get("max_drawdown_date", "") or ""),
        "simulation_mode": str(summary.get("simulation_mode", "") or ""),
        "entry_mode": str(params.get("entry_mode", "") or ""),
        "preset": params.get("preset"),
    }


def run_multi_stock_system_backtest(
    stock_codes: list[str],
    start: str,
    end: str,
    index_code: str = "system",
    total_capital: float | None = None,
    strategy_params: dict | None = None,
) -> dict[str, Any]:
    """Run the system strategy replay for multiple stocks and return an aggregate summary."""
    codes = [str(code).strip() for code in stock_codes if str(code).strip()]
    if not codes:
        raise ValueError("stock_codes must not be empty")

    rows: list[dict[str, Any]] = []
    total_pnl = 0.0
    total_trades = 0
    total_win_count = 0
    worst_drawdown_pct = 0.0
    total_ending_equity = 0.0
    sample_params: dict[str, Any] | None = None

    for code in codes:
        result = run_system_strategy_backtest(
            stock_code=code,
            start=start,
            end=end,
            index_code=index_code,
            total_capital=total_capital,
            strategy_params=strategy_params,
        )
        row = _stock_backtest_row(code, result)
        rows.append(row)
        total_pnl += row["total_realized_pnl"]
        total_trades += row["closed_trade_count"]
        total_win_count += row["win_count"]
        worst_drawdown_pct = min(worst_drawdown_pct, row["max_drawdown_pct"])
        total_ending_equity += row["ending_equity"]
        if sample_params is None:
            sample_params = dict(result.get("params", {}))

    aggregate = {
        "stock_count": len(codes),
        "closed_trade_count": total_trades,
        "total_realized_pnl": round(total_pnl, 2),
        "avg_ending_equity": round(total_ending_equity / len(codes), 2),
        "worst_max_drawdown_pct": round(worst_drawdown_pct, 4),
        "blended_win_rate": round(total_win_count / total_trades * 100, 1) if total_trades else 0.0,
    }
    if sample_params:
        aggregate["entry_mode"] = sample_params.get("entry_mode")
        aggregate["preset"] = sample_params.get("preset")

    return {
        "command": "backtest",
        "action": "system_strategy_batch_replay",
        "status": "ok",
        "codes": codes,
        "start": start,
        "end": end,
        "index_code": index_code,
        "aggregate": aggregate,
        "results": rows,
        "params": sample_params or {},
    }


def compare_system_strategy_presets(
    stock_codes: list[str],
    preset_names: list[str],
    *,
    start: str,
    end: str,
    index_code: str = "system",
    total_capital: float | None = None,
    strategy_params: dict | None = None,
) -> dict[str, Any]:
    """Compare multiple presets on the same stock basket."""
    codes = [str(code).strip() for code in stock_codes if str(code).strip()]
    presets = [str(name).strip() for name in preset_names if str(name).strip()]
    if not codes:
        raise ValueError("stock_codes must not be empty")
    if not presets:
        raise ValueError("preset_names must not be empty")

    base_params = dict(strategy_params or {})
    ranked: list[dict[str, Any]] = []
    code_breakdown: dict[str, list[dict[str, Any]]] = {}

    for preset in presets:
        params = dict(base_params)
        params["preset"] = preset
        batch_result = run_multi_stock_system_backtest(
            stock_codes=codes,
            start=start,
            end=end,
            index_code=index_code,
            total_capital=total_capital,
            strategy_params=params,
        )
        aggregate = batch_result["aggregate"]
        resolved_params = batch_result.get("params", {})
        ranked.append({
            "preset": preset,
            "entry_mode": resolved_params.get("entry_mode"),
            "buy_threshold": resolved_params.get("buy_threshold"),
            "momentum_stop_loss": resolved_params.get("momentum_stop_loss"),
            "momentum_trailing_stop": resolved_params.get("momentum_trailing_stop"),
            "momentum_time_stop_days": resolved_params.get("momentum_time_stop_days"),
            "closed_trade_count": aggregate.get("closed_trade_count", 0),
            "total_realized_pnl": aggregate.get("total_realized_pnl", 0),
            "avg_ending_equity": aggregate.get("avg_ending_equity", 0),
            "worst_max_drawdown_pct": aggregate.get("worst_max_drawdown_pct", 0),
            "blended_win_rate": aggregate.get("blended_win_rate", 0),
        })
        code_breakdown[preset] = batch_result.get("results", [])

    ranked.sort(
        key=lambda item: (
            -float(item.get("total_realized_pnl", 0) or 0),
            -float(item.get("blended_win_rate", 0) or 0),
            float(item.get("worst_max_drawdown_pct", 0) or 0),
        )
    )

    return {
        "command": "backtest",
        "action": "system_strategy_preset_compare",
        "status": "ok",
        "codes": codes,
        "presets": presets,
        "start": start,
        "end": end,
        "index_code": index_code,
        "ranked": ranked,
        "code_breakdown": code_breakdown,
    }


def _parse_params_json(value: str | None) -> dict:
    if not value:
        return {}
    path = Path(value)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return json.loads(value)


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="生成系统策略历史回填 fixture，可选直接运行策略回放")
    parser.add_argument("--code", default="601869", help="股票代码")
    parser.add_argument("--codes", default=None, help="批量股票代码，支持逗号/斜杠/空格分隔")
    parser.add_argument("--index", default="system", help="指数代码；system/composite=按当前系统多指数投票")
    parser.add_argument("--start", default="2025-04-10")
    parser.add_argument("--end", default="2026-04-10")
    parser.add_argument("--capital", type=float, default=None, help="覆盖初始资金")
    parser.add_argument("--preset", default=None, help="回测 preset 名称，例如 aggressive_high_return")
    parser.add_argument("--compare-presets", default=None, help="横向比较多个 preset，支持逗号分隔")
    parser.add_argument("--validation-report", action="store_true", help="生成单股策略验证报告")
    parser.add_argument("--params-json", default=None, help="JSON 字符串或 JSON 文件路径，用于动态覆盖策略参数")
    parser.add_argument("--run-replay", action="store_true", help="生成 fixture 后立即运行系统策略回放")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    codes = _parse_symbol_list(args.codes) or [args.code]
    compare_presets = _parse_symbol_list(args.compare_presets)
    params_override = _parse_params_json(args.params_json)
    if args.preset:
        params_override["preset"] = args.preset
    capital = args.capital if args.capital is not None else float(get_strategy().get("capital", 450286))

    if args.validation_report:
        if len(codes) != 1:
            raise SystemExit("--validation-report 只支持单只股票，请使用 --code 或仅传 1 个 --codes")
        print(f"正在生成单股验证报告...")
        validation = run_single_stock_strategy_validation(
            stock_code=codes[0],
            start=args.start,
            end=args.end,
            index_code=args.index,
            total_capital=capital,
            strategy_params=params_override,
        )
        out_path = Path(args.output) if args.output else (
            _PROJECT_ROOT / "data" / "backtest" / f"validation_{codes[0]}_{args.end}.json"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8")
        print(render_single_stock_validation_report(validation))
        print(f"Validation report 已写入: {out_path}")
        raise SystemExit(0)

    if compare_presets:
        print(f"正在比较 {len(compare_presets)} 个 preset，股票数 {len(codes)}...")
        compare_result = compare_system_strategy_presets(
            stock_codes=codes,
            preset_names=compare_presets,
            start=args.start,
            end=args.end,
            index_code=args.index,
            total_capital=capital,
            strategy_params=params_override,
        )
        out_path = Path(args.output) if args.output else (
            _PROJECT_ROOT / "data" / "backtest" / f"preset_compare_{len(codes)}stocks_{args.end}.json"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(compare_result, ensure_ascii=False, indent=2), encoding="utf-8")
        print("")
        for item in compare_result["ranked"]:
            print(
                f"{item['preset']}: "
                f"收益{item['total_realized_pnl']:+.2f} "
                f"交易{item['closed_trade_count']}笔 "
                f"胜率{item['blended_win_rate']:.1f}% "
                f"最差回撤{item['worst_max_drawdown_pct']:+.2f}%"
            )
        print(f"\nPreset compare 已写入: {out_path}")
        raise SystemExit(0)

    if len(codes) > 1:
        print(f"正在批量回放 {len(codes)} 只股票...")
        batch_result = run_multi_stock_system_backtest(
            stock_codes=codes,
            start=args.start,
            end=args.end,
            index_code=args.index,
            total_capital=capital,
            strategy_params=params_override,
        )
        out_path = Path(args.output) if args.output else (
            _PROJECT_ROOT / "data" / "backtest" / f"system_batch_{len(codes)}stocks_{args.end}.json"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(batch_result, ensure_ascii=False, indent=2), encoding="utf-8")
        agg = batch_result["aggregate"]
        print(
            "批量回放: "
            f"收益{agg.get('total_realized_pnl', 0):+.2f} "
            f"交易{agg.get('closed_trade_count', 0)}笔 "
            f"胜率{agg.get('blended_win_rate', 0):.1f}% "
            f"最差回撤{agg.get('worst_max_drawdown_pct', 0):+.2f}%"
        )
        for row in batch_result["results"]:
            print(
                f"  {row['code']}: "
                f"{row['total_realized_pnl']:+.2f} / "
                f"{row['closed_trade_count']}笔 / "
                f"DD {row['max_drawdown_pct']:+.2f}%"
            )
        print(f"\nBatch result 已写入: {out_path}")
        raise SystemExit(0)

    print(f"正在拉取数据...")
    fixture = build_replay_fixture(
        stock_code=codes[0],
        start=args.start,
        end=args.end,
        index_code=args.index,
        total_capital=capital,
        strategy_params=params_override,
    )

    out_path = Path(args.output) if args.output else (
        _PROJECT_ROOT / "data" / "backtest" / f"fixture_real_{codes[0]}_{args.end}.json"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(fixture, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # 打印摘要
    signals = {}
    for day, d in sorted(fixture["daily_data"].items()):
        sig = d["market_signal"]
        signals[sig] = signals.get(sig, 0) + 1
    print(f"\n大盘信号分布: {signals}")
    veto_stats = {}
    for day, d in sorted(fixture["daily_data"].items()):
        for cand in d["candidates"]:
            for v in cand.get("veto_signals", []):
                veto_stats[v] = veto_stats.get(v, 0) + 1
    print(f"Veto 分布: {veto_stats}")
    trade_days = len(fixture["daily_data"])
    no_veto = sum(
        1 for d in fixture["daily_data"].values()
        if not d["candidates"][0].get("veto_signals")
        and d["market_signal"] in ("GREEN", "YELLOW")
    )
    print(f"可交易天数: {no_veto}/{trade_days}")
    print(f"\nFixture 已写入: {out_path}")

    if args.run_replay:
        from scripts.backtest.strategy_replay import run_strategy_replay

        result = run_strategy_replay(
            daily_data=fixture["daily_data"],
            start=args.start,
            end=args.end,
            total_capital=fixture["total_capital"],
            params=fixture["params"],
        )
        summary = result.get("summary", {})
        result_path = out_path.with_name(out_path.stem.replace("fixture", "replay") + ".json")
        result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(
            "策略回放: "
            f"交易{summary.get('closed_trade_count', 0)}笔 "
            f"胜率{summary.get('win_rate', 0)}% "
            f"累计盈亏{summary.get('total_realized_pnl', 0):+.2f} "
            f"最大回撤{summary.get('max_drawdown_pct', 0):+.2f}% "
            f"期末权益{summary.get('ending_equity', 0):.2f}"
        )
        print(f"Replay 已写入: {result_path}")
