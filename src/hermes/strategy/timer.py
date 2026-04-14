"""
strategy/timer.py — 大盘择时（纯函数）

不做任何 IO。输入指数数据 dict，输出 MarketState。
数据获取由 market context 负责，这里只做信号计算。
"""

from __future__ import annotations

from typing import Optional

from hermes.strategy.models import MarketSignal, MarketState


def compute_market_signal(
    index_data: dict[str, dict],
    config: Optional[dict] = None,
) -> MarketState:
    """
    纯函数：根据指数数据计算大盘信号。

    Args:
        index_data: {
            "上证指数": {"above_ma20": bool, "below_ma60_days": int, "change_pct": float, ...},
            "深证成指": {...},
            "创业板指": {...},
        }
        config: market_timer 配置段（可选）

    Returns:
        MarketState(signal, multiplier, detail)
    """
    config = config or {}
    clear_days = config.get("clear_days_ma60", 15)

    green_count = 0
    red_count = 0
    clear_count = 0
    total = 0

    for name, data in index_data.items():
        if "error" in data:
            continue
        total += 1

        above_ma20 = data.get("above_ma20", False)
        below_ma60_days = data.get("below_ma60_days", 0)

        if above_ma20:
            green_count += 1
        else:
            red_count += 1

        if below_ma60_days >= clear_days:
            clear_count += 1

    if total == 0:
        return MarketState(
            signal=MarketSignal.CLEAR,
            multiplier=0.0,
            detail={"reason": "无有效指数数据", "indices": index_data},
        )

    green_pct = green_count / total
    clear_pct = clear_count / total

    # 优先级：CLEAR > RED > YELLOW > GREEN
    if clear_pct >= 0.6:
        signal = MarketSignal.CLEAR
    elif green_pct >= 0.6:
        signal = MarketSignal.GREEN
    elif green_pct >= 0.3:
        signal = MarketSignal.YELLOW
    else:
        signal = MarketSignal.RED

    multiplier = _signal_to_multiplier(signal)

    return MarketState(
        signal=signal,
        multiplier=multiplier,
        detail={
            "green_count": green_count,
            "red_count": red_count,
            "clear_count": clear_count,
            "total": total,
            "green_pct": round(green_pct, 2),
            "clear_pct": round(clear_pct, 2),
            "indices": index_data,
        },
    )


def _signal_to_multiplier(signal: MarketSignal) -> float:
    return {
        MarketSignal.GREEN: 1.0,
        MarketSignal.YELLOW: 0.5,
        MarketSignal.RED: 0.0,
        MarketSignal.CLEAR: 0.0,
    }.get(signal, 0.0)
