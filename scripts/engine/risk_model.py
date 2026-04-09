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
from datetime import date, datetime, timedelta
from typing import Optional, Tuple

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

from scripts.utils.config_loader import get_strategy
from scripts.utils.logger import get_logger

_logger = get_logger("risk_model")


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value in [None, ""]:
            return default
        if isinstance(value, str):
            value = value.replace("¥", "").replace("%", "").replace(",", "").strip()
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_trade_date(value) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) >= 10:
        text = text[:10]
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _event_trade_date(event: dict) -> date | None:
    return _parse_trade_date(
        event.get("trade_date")
        or event.get("event_date")
        or event.get("date")
    )


def _daily_realized_pnl(trade_events: list[dict]) -> dict[str, float]:
    by_day = {}
    for event in trade_events or []:
        trade_day = _event_trade_date(event)
        if not trade_day:
            continue
        action = str(event.get("action") or event.get("side") or "").strip().upper()
        if action not in {"SELL", "SELL_PARTIAL"} and str(event.get("side", "")).strip().lower() != "sell":
            continue
        day_key = trade_day.isoformat()
        by_day[day_key] = round(
            by_day.get(day_key, 0.0) + _safe_float(event.get("realized_pnl", event.get("pnl", 0.0)), 0.0),
            2,
        )
    return by_day


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


def check_portfolio_risk(trade_events: list[dict], positions: list[dict],
                         total_capital: float,
                         today: Optional[date] = None,
                         strategy: Optional[dict] = None) -> dict:
    """
    组合级风控：连续亏损冷却 + 单日亏损上限 + 持仓集中度预警。

    Returns:
        {
            "can_trade": bool,
            "state": "ok"|"warning"|"block",
            "reason_codes": [str],
            "reasons": [str],
            "metrics": {...},
        }
    """
    strategy = strategy or get_strategy()
    today = today or date.today()
    portfolio_cfg = strategy.get("risk", {}).get("portfolio", {})

    daily_loss_limit_pct = _safe_float(portfolio_cfg.get("daily_loss_limit_pct", 0.03), 0.03)
    consecutive_loss_days_limit = int(portfolio_cfg.get("consecutive_loss_days_limit", 2) or 2)
    cooldown_days = int(portfolio_cfg.get("cooldown_days", 2) or 2)
    max_single_warn_pct = _safe_float(portfolio_cfg.get("max_single_position_warn_pct", 0.25), 0.25)

    day_pnl = _daily_realized_pnl(trade_events or [])
    today_key = today.isoformat()
    today_realized_pnl = round(day_pnl.get(today_key, 0.0), 2)
    today_realized_loss = abs(min(today_realized_pnl, 0.0))
    today_loss_pct = (today_realized_loss / total_capital) if total_capital else 0.0

    sorted_days = sorted(day_pnl.keys())
    consecutive_loss_days = 0
    streak_end_date = None
    if sorted_days:
        for day_key in reversed(sorted_days):
            pnl = _safe_float(day_pnl.get(day_key, 0.0), 0.0)
            if pnl < 0:
                consecutive_loss_days += 1
                if streak_end_date is None:
                    streak_end_date = _parse_trade_date(day_key)
            else:
                break

    cooldown_active = False
    cooldown_until = ""
    if (
        streak_end_date
        and consecutive_loss_days >= consecutive_loss_days_limit > 0
        and cooldown_days > 0
    ):
        cooldown_until_date = streak_end_date + timedelta(days=cooldown_days)
        cooldown_until = cooldown_until_date.isoformat()
        cooldown_active = today <= cooldown_until_date

    largest_position_pct = 0.0
    largest_position_code = ""
    for position in positions or []:
        market_value = _safe_float(position.get("market_value", 0.0), 0.0)
        if total_capital <= 0 or market_value <= 0:
            continue
        pct = market_value / total_capital
        if pct > largest_position_pct:
            largest_position_pct = pct
            largest_position_code = str(position.get("code", "")).strip()

    reason_codes = []
    reasons = []
    if today_loss_pct >= daily_loss_limit_pct > 0:
        reason_codes.append("TRADE_PORTFOLIO_DAILY_LOSS_LIMIT")
        reasons.append(
            f"单日已实现亏损达上限 (¥{today_realized_loss:,.2f} / {today_loss_pct:.1%})"
        )
    if cooldown_active:
        reason_codes.append("TRADE_CONSECUTIVE_LOSS_COOLDOWN")
        reasons.append(
            f"连续亏损冷却中 ({consecutive_loss_days} 个亏损交易日，冷却至 {cooldown_until})"
        )
    if largest_position_pct >= max_single_warn_pct > 0:
        reason_codes.append("TRADE_POSITION_CONCENTRATION_WARNING")
        reasons.append(
            f"持仓集中度预警 ({largest_position_code or 'unknown'} 占总资金 {largest_position_pct:.1%})"
        )

    state = "ok"
    if any(code in {"TRADE_PORTFOLIO_DAILY_LOSS_LIMIT", "TRADE_CONSECUTIVE_LOSS_COOLDOWN"} for code in reason_codes):
        state = "block"
    elif "TRADE_POSITION_CONCENTRATION_WARNING" in reason_codes:
        state = "warning"

    return {
        "can_trade": state != "block",
        "state": state,
        "reason_codes": reason_codes,
        "reasons": reasons,
        "metrics": {
            "today_realized_pnl": today_realized_pnl,
            "today_realized_loss": today_realized_loss,
            "today_loss_pct": round(today_loss_pct, 4),
            "daily_loss_limit_pct": round(daily_loss_limit_pct, 4),
            "consecutive_loss_days": consecutive_loss_days,
            "consecutive_loss_days_limit": consecutive_loss_days_limit,
            "cooldown_days": cooldown_days,
            "cooldown_active": cooldown_active,
            "cooldown_until": cooldown_until,
            "largest_position_pct": round(largest_position_pct, 4),
            "largest_position_code": largest_position_code,
            "max_single_position_warn_pct": round(max_single_warn_pct, 4),
        },
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
