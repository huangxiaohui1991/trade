"""
strategy/classifier.py — 风格判定（纯函数）

不做任何 IO。输入 K 线数据 + RSI，输出风格判定结果。
从 V1 scripts/engine/stock_classifier.py 提取纯逻辑。
"""

from __future__ import annotations

from typing import Optional

from hermes.strategy.models import Style, StyleResult, SwitchResult


def calc_ma20_slope(closes: list[float], lookback: int = 10) -> float:
    """
    计算 MA20 在最近 lookback 天的平均日斜率（百分比）。
    需要至少 20 + lookback 根 K 线。
    """
    if len(closes) < 20 + lookback:
        return 0.0

    ma20_series = []
    for i in range(len(closes) - 19):
        ma20_series.append(sum(closes[i : i + 20]) / 20)

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
    """计算最近 lookback 天的平均日波动率（绝对值百分比）。"""
    if len(closes) < lookback + 1:
        return 0.0

    recent = closes[-(lookback + 1) :]
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
    config: Optional[dict] = None,
) -> StyleResult:
    """
    纯函数：判定股票风格。

    Args:
        closes: 至少 60 根日 K 收盘价
        rsi: 当前 RSI（可选，不传则自动计算）
        config: style_classifier 配置段（可选）

    Returns:
        StyleResult
    """
    config = config or {}
    sb_cfg = config.get("slow_bull", {})
    mm_cfg = config.get("momentum", {})

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

    # 慢牛阈值
    sb_vol_max = sb_cfg.get("daily_volatility_max", 0.02)
    sb_rsi_range = sb_cfg.get("rsi_range", [50, 65])
    sb_slope_min = sb_cfg.get("ma20_slope_min", 0.005)

    # 题材阈值
    mm_vol_min = mm_cfg.get("daily_volatility_min", 0.03)
    mm_rsi_high = mm_cfg.get("rsi_high_threshold", 75)
    mm_slope_min = mm_cfg.get("ma20_slope_min", 0.02)

    sb_score = 0
    mm_score = 0

    if volatility <= sb_vol_max:
        sb_score += 1
    if volatility >= mm_vol_min:
        mm_score += 1

    if sb_rsi_range[0] <= rsi <= sb_rsi_range[1]:
        sb_score += 1
    if rsi >= mm_rsi_high:
        mm_score += 1

    if slope >= sb_slope_min:
        sb_score += 1
    if slope >= mm_slope_min:
        mm_score += 1

    if sb_score >= 2 and sb_score > mm_score:
        return StyleResult(
            style=Style.SLOW_BULL,
            confidence=round(sb_score / 3, 2),
            metrics=metrics,
            reason=f"低波动({volatility:.1%}) + RSI温和({rsi:.0f}) + MA20斜率平缓({slope:.3%})",
        )
    elif mm_score >= 2:
        return StyleResult(
            style=Style.MOMENTUM,
            confidence=round(mm_score / 3, 2),
            metrics=metrics,
            reason=f"高波动({volatility:.1%}) + RSI冲高({rsi:.0f}) + MA20陡峭({slope:.3%})",
        )
    else:
        if volatility >= mm_vol_min:
            return StyleResult(
                style=Style.MOMENTUM,
                confidence=0.5,
                metrics=metrics,
                reason=f"波动率偏高({volatility:.1%})，默认归为题材",
            )
        else:
            return StyleResult(
                style=Style.SLOW_BULL,
                confidence=0.5,
                metrics=metrics,
                reason=f"波动率偏低({volatility:.1%})，默认归为慢牛",
            )


def check_style_switch(
    style: Style,
    daily_change_pct: float,
    rsi: float,
    rsi_history: Optional[list[float]] = None,
    config: Optional[dict] = None,
) -> SwitchResult:
    """
    纯函数：检测是否需要从慢牛切换到题材模式（防回撤保险丝）。

    Args:
        style: 当前风格
        daily_change_pct: 今日涨幅（如 0.08 = 8%）
        rsi: 当前 RSI
        rsi_history: 最近 N 日 RSI 列表
        config: style_switch.triggers 配置段
    """
    if style != Style.SLOW_BULL:
        return SwitchResult(should_switch=False, new_style=style)

    config = config or {}
    surge_pct = config.get("single_day_surge_pct", 0.07)
    rsi_threshold = config.get("rsi_overheat_threshold", 75)
    rsi_days = config.get("rsi_overheat_consecutive_days", 3)

    # 单日暴涨
    if daily_change_pct >= surge_pct:
        return SwitchResult(
            should_switch=True,
            trigger=f"单日涨幅 {daily_change_pct:.1%} >= {surge_pct:.0%}",
            new_style=Style.MOMENTUM,
        )

    # RSI 连续过热
    if rsi_history and len(rsi_history) >= rsi_days:
        recent = rsi_history[-rsi_days:]
        if all(r >= rsi_threshold for r in recent):
            return SwitchResult(
                should_switch=True,
                trigger=f"RSI 连续 {rsi_days} 日 >= {rsi_threshold}",
                new_style=Style.MOMENTUM,
            )

    return SwitchResult(should_switch=False, new_style=style)
