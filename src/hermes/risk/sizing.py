"""
risk/sizing.py — 仓位计算（纯函数）
"""

from __future__ import annotations

from hermes.risk.models import PositionSize


def calc_position_size(
    total_capital: float,
    current_exposure_pct: float,
    price: float,
    market_multiplier: float = 1.0,
    single_max_pct: float = 0.20,
    total_max_pct: float = 0.60,
) -> PositionSize:
    """
    纯函数：计算建议仓位。

    Args:
        total_capital: 总资金
        current_exposure_pct: 当前仓位占比
        price: 当前价格
        market_multiplier: 大盘仓位系数 (GREEN=1.0, YELLOW=0.5, RED=0.0)
        single_max_pct: 单股仓位上限
        total_max_pct: 总仓位上限
    """
    if market_multiplier <= 0 or price <= 0:
        return PositionSize(shares=0, amount=0, pct=0, market_multiplier=market_multiplier)

    base_pct = single_max_pct * market_multiplier
    remaining = max(0, total_max_pct - current_exposure_pct)
    final_pct = min(base_pct, remaining)

    amount = total_capital * final_pct
    shares = int(amount / price / 100) * 100  # 整手

    if shares <= 0:
        return PositionSize(shares=0, amount=0, pct=0, market_multiplier=market_multiplier)

    actual_amount = shares * price
    actual_pct = actual_amount / total_capital if total_capital > 0 else 0

    return PositionSize(
        shares=shares,
        amount=round(actual_amount, 2),
        pct=round(actual_pct, 4),
        market_multiplier=market_multiplier,
    )
