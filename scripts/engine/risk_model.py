#!/usr/bin/env python3
"""
engine/risk_model.py — 风控校验模块

职责：
  - calc_stop_loss / calc_take_profit: 止损止盈计算
  - check_risk: 风控校验（总仓位/单只仓位/每周次数）
  - should_buy / should_add / should_exit: 买卖决策辅助
  - check_time_stop: 时间止损检查
  - check_absolute_stop: 绝对止损检查

所有风控参数从 strategy.yaml 读取，不硬编码。
"""

import os
import sys
import warnings
from typing import Optional, Tuple

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

from scripts.utils.config_loader import get_strategy
from scripts.utils.logger import get_logger

_logger = get_logger("risk_model")


# ---------------------------------------------------------------------------
# 止损止盈计算
# ---------------------------------------------------------------------------

def calc_stop_loss(cost: float, ma20: float = 0) -> dict:
    """
    计算动态止损价

    规则（从 strategy.yaml 读取）：
      - 正常：成本价 × (1 - stop_loss_pct)
      - MA20 支撑：如果 ma20 > 成本×0.96，以 ma20 为动态止损（更宽松）
      - 绝对止损：成本价 × (1 - absolute_stop_pct)（不可跌破）

    Args:
        cost: 持仓成本
        ma20: 20日均线（可选）

    Returns:
        {
            "stop_loss": float,       # 动态止损价
            "absolute_stop": float,   # 绝对止损价（-7%）
            "method": str,            # "ma20_anchor" | "cost_based"
        }
    """
    strategy = get_strategy()
    risk = strategy.get("risk", {})
    stop_loss_pct = risk.get("stop_loss", 0.04)
    absolute_stop_pct = risk.get("absolute_stop", 0.07)

    stop_loss = round(cost * (1 - stop_loss_pct), 2)
    absolute_stop = round(cost * (1 - absolute_stop_pct), 2)

    # 如果 MA20 在成本的 96% 以上，以 MA20 为动态止损
    method = "cost_based"
    if ma20 and ma20 > cost * 0.96:
        stop_loss = round(min(stop_loss, ma20), 2)
        method = "ma20_anchor"

    return {
        "stop_loss": stop_loss,
        "absolute_stop": absolute_stop,
        "method": method,
    }


def calc_take_profit(cost: float, first_buy_price: float = 0) -> dict:
    """
    计算止盈目标价（从 strategy.yaml 读取参数）

    Args:
        cost: 平均成本
        first_buy_price: 首次买入价（用于计算真实盈亏）

    Returns:
        {
            "batch_1_price": float,   # 第一批止盈价（+15%）
            "batch_1_pct": float,     # 第一批止盈比例
            "batch_2_drawdown": float, # 第二批回撤触发（-5%）
            "batch_3_drawdown": float, # 第三批清仓回撤（-8%）
        }
    """
    strategy = get_strategy()
    risk = strategy.get("risk", {})
    tp = risk.get("take_profit", {})

    t1_pct = tp.get("t1_pct", 0.15)
    t1_drawdown = tp.get("t1_drawdown", 0.05)
    t2_drawdown = tp.get("t2_drawdown", 0.08)

    base = first_buy_price if first_buy_price > 0 else cost
    batch_1_price = round(base * (1 + t1_pct), 2)

    return {
        "batch_1_price": batch_1_price,
        "batch_1_pct": t1_pct,
        "batch_2_drawdown": t1_drawdown,
        "batch_3_drawdown": t2_drawdown,
    }


# ---------------------------------------------------------------------------
# 风控校验
# ---------------------------------------------------------------------------

def check_risk(current_exposure: float, this_week_buys: int,
                holding_count: int, proposed_amount: float = 0) -> dict:
    """
    综合风控校验

    Args:
        current_exposure: 当前已用仓位占总资金比例（0-1）
        this_week_buys: 本周已买次数
        holding_count: 当前持仓只数
        proposed_amount: 拟买入金额（0=不计）

    Returns:
        {
            "can_buy": bool,
            "reasons": [str],          # 不允许的原因
            "limits": {
                "total_exposure_max": float,
                "current_exposure": float,
                "remaining_pct": float,
                "weekly_max": int,
                "this_week_buys": int,
                "weekly_remaining": int,
                "holding_max": int,
                "holding_count": int,
            }
        }
    """
    strategy = get_strategy()
    risk = strategy.get("risk", {})
    pos = risk.get("position", {})

    total_max = pos.get("total_max", 0.60)
    single_max = pos.get("single_max", 0.20)
    weekly_max = pos.get("weekly_max", 2)
    holding_max = pos.get("holding_max", 4)

    reasons = []
    if current_exposure >= total_max:
        reasons.append(f"总仓位已达上限 ({current_exposure:.0%} >= {total_max:.0%})")
    if proposed_amount > 0:
        proposed_pct = proposed_amount / strategy.get("capital", 450286)
        if proposed_pct > single_max:
            reasons.append(f"单只仓位超限 ({proposed_pct:.0%} > {single_max:.0%})")
        if current_exposure + proposed_pct > total_max:
            reasons.append(f"总仓位将超限 ({current_exposure + proposed_pct:.0%} > {total_max:.0%})")
    if this_week_buys >= weekly_max:
        reasons.append(f"本周买入次数已满 ({this_week_buys}/{weekly_max})")
    if holding_count >= holding_max:
        reasons.append(f"持仓只数已达上限 ({holding_count}/{holding_max})")

    can_buy = len(reasons) == 0

    return {
        "can_buy": can_buy,
        "reasons": reasons,
        "limits": {
            "total_exposure_max": total_max,
            "current_exposure": round(current_exposure, 3),
            "remaining_pct": round(max(total_max - current_exposure, 0), 3),
            "weekly_max": weekly_max,
            "this_week_buys": this_week_buys,
            "weekly_remaining": max(weekly_max - this_week_buys, 0),
            "holding_max": holding_max,
            "holding_count": holding_count,
        }
    }


def should_exit(position: dict, current_price: float) -> Tuple[bool, str]:
    """
    判断是否应该止损/卖出

    Args:
        position: dict with keys cost, shares, first_buy_price, ...
        current_price: 最新价格

    Returns:
        (should_exit: bool, reason: str)
    """
    cost = float(position.get("平均成本", 0))
    if cost <= 0:
        return False, ""

    change_pct = (current_price - cost) / cost
    stops = calc_stop_loss(cost)

    # 绝对止损
    if current_price <= stops["absolute_stop"]:
        return True, f"绝对止损（{current_price} <= {stops['absolute_stop']}）"

    # 动态止损
    if current_price <= stops["stop_loss"]:
        return True, f"动态止损（{current_price} <= {stops['stop_loss']}）"

    # 时间止损（15个交易日）
    hold_days = int(position.get("持有天数", 0))
    time_stop_days = get_strategy().get("risk", {}).get("time_stop_days", 15)
    if hold_days >= time_stop_days > 0 and change_pct < 0.02:
        return True, f"时间止损（已持有{hold_days}日，涨幅不足2%）"

    return False, ""


def calc_position_size(total_capital: float, price: float,
                        risk_pct: float = 0.04) -> dict:
    """
    计算仓位尺寸（按 4% 风险公式）

    Args:
        total_capital: 总资金
        price: 买入价格
        risk_pct: 单次最大风险比例（默认 4%）

    Returns:
        {
            "max_loss": float,      # 最大亏损金额
            "shares": int,           # 可买入股数
            "amount": float,         # 买入金额
            "position_pct": float,  # 仓位占比
        }
    """
    strategy = get_strategy()
    risk = strategy.get("risk", {})
    stop_loss_pct = risk.get("stop_loss", 0.04)
    single_max_pct = risk.get("position", {}).get("single_max", 0.20)

    # 最大亏损金额
    max_loss = total_capital * risk_pct

    # 按止损幅度计算股数（确保亏损不超过 max_loss）
    stop_distance = price * stop_loss_pct
    if stop_distance > 0:
        shares = int(max_loss / stop_distance)
        # 必须是100的倍数
        shares = (shares // 100) * 100
        if shares < 100:
            return {
                "max_loss": 0, "shares": 0, "amount": 0, "position_pct": 0,
                "reason": "资金不足以开最小仓位"
            }
    else:
        shares = 0

    amount = round(shares * price, 2)
    position_pct = amount / total_capital if total_capital > 0 else 0

    # 不超过单只上限
    if position_pct > single_max_pct:
        shares = int(total_capital * single_max_pct / price)
        shares = (shares // 100) * 100
        amount = round(shares * price, 2)
        position_pct = amount / total_capital

    return {
        "max_loss": round(max_loss, 2),
        "shares": shares,
        "amount": amount,
        "position_pct": round(position_pct, 3),
    }


# ---------------------------------------------------------------------------
# 验证
# ------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== risk_model.py 验证 ===")

    # 止损止盈
    stops = calc_stop_loss(cost=100.0, ma20=98.0)
    print(f"\n成本=100, MA20=98:")
    print(f"  动态止损={stops['stop_loss']} 绝对止损={stops['absolute_stop']} method={stops['method']}")

    stops2 = calc_stop_loss(cost=100.0, ma20=0)
    print(f"\n成本=100, 无MA20:")
    print(f"  动态止损={stops2['stop_loss']} 绝对止损={stops2['absolute_stop']}")

    tp = calc_take_profit(cost=100.0, first_buy_price=98.0)
    print(f"\n止盈目标: {tp}")

    # 风控校验
    risk = check_risk(current_exposure=0.3, this_week_buys=1, holding_count=2)
    print(f"\n当前仓位30%, 本周1次, 持仓2只:")
    print(f"  can_buy={risk['can_buy']} reasons={risk['reasons']}")
    print(f"  remaining={risk['limits']['remaining_pct']:.0%} weekly_remaining={risk['limits']['weekly_remaining']}")

    # 仓位计算
    size = calc_position_size(total_capital=450286, price=10.5)
    print(f"\n总资金45万, 买入价10.5:")
    print(f"  可买{size['shares']}股 金额¥{size['amount']:,.0f} 仓位{size['position_pct']:.1%}")
