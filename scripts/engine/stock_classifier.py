#!/usr/bin/env python3
"""
engine/stock_classifier.py — 股票风格判定模块

根据 strategy.yaml 的 style_classifier 配置，将股票分为：
  - slow_bull（慢牛成长股）：低波动、RSI 温和、MA20 斜率平缓上行
  - momentum（题材趋势股）：高波动、RSI 易冲高、MA20 斜率陡峭

同时提供动态风格切换检测（防回撤保险丝）。
"""

import os
import sys
from typing import Optional, Union, Optional

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from scripts.utils.common import _safe_float
from scripts.utils.config_loader import get_strategy
from scripts.utils.logger import get_logger

_logger = get_logger("stock_classifier")

STYLE_SLOW_BULL = "slow_bull"
STYLE_MOMENTUM = "momentum"
STYLE_UNKNOWN = "unknown"


def calc_ma20_slope(closes: list[float], lookback: int = 10) -> float:
    """
    计算 MA20 在最近 lookback 天的平均日斜率（百分比）。

    需要至少 20 + lookback 根 K 线。
    返回每日平均变化率，如 0.005 表示每天 0.5%。
    """
    if len(closes) < 20 + lookback:
        return 0.0

    ma20_series = []
    for i in range(len(closes) - 19):
        ma20_series.append(sum(closes[i:i + 20]) / 20)

    if len(ma20_series) < lookback + 1:
        return 0.0

    recent = ma20_series[-lookback:]
    if recent[0] <= 0:
        return 0.0

    daily_changes = []
    for i in range(1, len(recent)):
        daily_changes.append((recent[i] - recent[i - 1]) / recent[i - 1])

    return sum(daily_changes) / len(daily_changes) if daily_changes else 0.0


def calc_daily_volatility(closes: list[float], lookback: int = 20) -> float:
    """
    计算最近 lookback 天的平均日波动率（绝对值百分比）。
    """
    if len(closes) < lookback + 1:
        return 0.0

    recent = closes[-(lookback + 1):]
    daily_returns = []
    for i in range(1, len(recent)):
        if recent[i - 1] > 0:
            daily_returns.append(abs((recent[i] - recent[i - 1]) / recent[i - 1]))

    return sum(daily_returns) / len(daily_returns) if daily_returns else 0.0


def calc_rsi(closes: list[float], period: int = 14) -> float:
    """计算 RSI。"""
    if len(closes) < period + 1:
        return 50.0

    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent = deltas[-period:]

    gains = [d for d in recent if d > 0]
    losses = [-d for d in recent if d < 0]

    avg_gain = sum(gains) / period if gains else 0.0
    avg_loss = sum(losses) / period if losses else 0.0001

    rs = avg_gain / avg_loss if avg_loss > 0 else 100.0
    return 100.0 - (100.0 / (1.0 + rs))


def classify_style(
    closes: list[float],
    rsi: Optional[float] = None,
    strategy: Optional[dict] = None,
) -> dict:
    """
    判定股票风格。

    Args:
        closes: 至少 60 根日 K 收盘价（越多越好）
        rsi: 当前 RSI（可选，不传则自动计算）
        strategy: 策略配置（可选）

    Returns:
        {
            "style": "slow_bull" | "momentum" | "unknown",
            "confidence": float,  # 0~1
            "metrics": {
                "daily_volatility": float,
                "rsi": float,
                "ma20_slope": float,
            },
            "reason": str,
        }
    """
    strategy = strategy or get_strategy()
    cfg = strategy.get("style_classifier", {})
    sb_cfg = cfg.get("slow_bull", {})
    mm_cfg = cfg.get("momentum", {})

    if rsi is None:
        rsi = calc_rsi(closes)

    lookback = sb_cfg.get("ma20_slope_lookback_days", 10)
    volatility = calc_daily_volatility(closes)
    slope = calc_ma20_slope(closes, lookback)

    metrics = {
        "daily_volatility": round(volatility, 4),
        "rsi": round(rsi, 1),
        "ma20_slope": round(slope, 5),
    }

    # 慢牛判定
    sb_vol_max = sb_cfg.get("daily_volatility_max", 0.02)
    sb_rsi_range = sb_cfg.get("rsi_range", [50, 65])
    sb_slope_min = sb_cfg.get("ma20_slope_min", 0.005)

    # 题材判定
    mm_vol_min = mm_cfg.get("daily_volatility_min", 0.03)
    mm_rsi_high = mm_cfg.get("rsi_high_threshold", 75)
    mm_slope_min = mm_cfg.get("ma20_slope_min", 0.02)

    sb_score = 0
    mm_score = 0

    # 波动率
    if volatility <= sb_vol_max:
        sb_score += 1
    if volatility >= mm_vol_min:
        mm_score += 1

    # RSI
    if sb_rsi_range[0] <= rsi <= sb_rsi_range[1]:
        sb_score += 1
    if rsi >= mm_rsi_high:
        mm_score += 1

    # MA20 斜率
    if slope >= sb_slope_min:
        sb_score += 1
    if slope >= mm_slope_min:
        mm_score += 1

    if sb_score >= 2 and sb_score > mm_score:
        return {
            "style": STYLE_SLOW_BULL,
            "confidence": round(sb_score / 3, 2),
            "metrics": metrics,
            "reason": f"低波动({volatility:.1%}) + RSI温和({rsi:.0f}) + MA20斜率平缓({slope:.3%})",
        }
    elif mm_score >= 2:
        return {
            "style": STYLE_MOMENTUM,
            "confidence": round(mm_score / 3, 2),
            "metrics": metrics,
            "reason": f"高波动({volatility:.1%}) + RSI冲高({rsi:.0f}) + MA20陡峭({slope:.3%})",
        }
    else:
        # 默认按波动率二分
        if volatility >= mm_vol_min:
            return {
                "style": STYLE_MOMENTUM,
                "confidence": 0.5,
                "metrics": metrics,
                "reason": f"波动率偏高({volatility:.1%})，默认归为题材",
            }
        else:
            return {
                "style": STYLE_SLOW_BULL,
                "confidence": 0.5,
                "metrics": metrics,
                "reason": f"波动率偏低({volatility:.1%})，默认归为慢牛",
            }


def check_style_switch(
    style: str,
    daily_change_pct: float,
    rsi: float,
    rsi_history: Optional[list[float]] = None,
    strategy: Optional[dict] = None,
) -> dict:
    """
    检测是否需要从慢牛切换到题材模式（防回撤保险丝）。

    Args:
        style: 当前风格
        daily_change_pct: 今日涨幅（如 0.08 = 8%）
        rsi: 当前 RSI
        rsi_history: 最近 N 日 RSI 列表（用于检测连续过热）
        strategy: 策略配置

    Returns:
        {
            "should_switch": bool,
            "trigger": Optional[str],
            "new_style": str,
        }
    """
    if style != STYLE_SLOW_BULL:
        return {"should_switch": False, "trigger": None, "new_style": style}

    strategy = strategy or get_strategy()
    sw_cfg = strategy.get("style_switch", {}).get("triggers", {})

    surge_pct = sw_cfg.get("single_day_surge_pct", 0.07)
    rsi_threshold = sw_cfg.get("rsi_overheat_threshold", 75)
    rsi_days = sw_cfg.get("rsi_overheat_consecutive_days", 3)

    # 单日暴涨
    if daily_change_pct >= surge_pct:
        return {
            "should_switch": True,
            "trigger": f"单日涨幅 {daily_change_pct:.1%} >= {surge_pct:.0%}",
            "new_style": STYLE_MOMENTUM,
        }

    # RSI 连续过热
    if rsi_history and len(rsi_history) >= rsi_days:
        recent = rsi_history[-rsi_days:]
        if all(r >= rsi_threshold for r in recent):
            return {
                "should_switch": True,
                "trigger": f"RSI 连续 {rsi_days} 日 >= {rsi_threshold}",
                "new_style": STYLE_MOMENTUM,
            }

    return {"should_switch": False, "trigger": None, "new_style": style}


def get_risk_params(style: str, strategy: Optional[dict] = None) -> dict:
    """
    根据风格返回对应的风控参数。

    Returns:
        {
            "stop_loss": float,
            "time_stop_days": int,
            "trailing_stop": Optional[float],
            "exit_ma": int,
            ...
        }
    """
    strategy = strategy or get_strategy()
    risk_cfg = strategy.get("risk", {})

    if style == STYLE_SLOW_BULL:
        sb = risk_cfg.get("slow_bull", {})
        return {
            "style": STYLE_SLOW_BULL,
            "stop_loss": sb.get("stop_loss", 0.08),
            "absolute_stop_ma": sb.get("absolute_stop_ma", 60),
            "take_profit": None,
            "trailing_stop": None,
            "exit_ma": sb.get("exit_ma", 20),
            "time_stop_days": sb.get("time_stop_days", 30),
        }
    else:
        mm = risk_cfg.get("momentum", {})
        return {
            "style": STYLE_MOMENTUM,
            "stop_loss": mm.get("stop_loss", 0.05),
            "stop_loss_anchor": mm.get("stop_loss_anchor", "entry_day_low"),
            "trailing_stop": mm.get("trailing_stop", 0.08),
            "trailing_ma_cross": mm.get("trailing_ma_cross", {"fast": 5, "slow": 10}),
            "exit_ma": mm.get("exit_ma", 20),
            "time_stop_days": mm.get("time_stop_days", 10),
        }
