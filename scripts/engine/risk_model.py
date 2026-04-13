#!/usr/bin/env python3
from __future__ import annotations

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
from typing import Optional, Union, Optional, Tuple

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

from scripts.utils.common import _safe_float
from scripts.utils.config_loader import get_strategy
from scripts.utils.logger import get_logger

_logger = get_logger("risk_model")


def _parse_trade_date(value) -> Optional[date]:
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


def _event_trade_date(event: dict) -> Optional[date]:
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


def _position_code(position: dict) -> str:
    return str(position.get("code", position.get("stock_code", "")) or "").strip()


def _position_market_value(position: dict) -> float:
    market_value = _safe_float(position.get("market_value", position.get("marketValue", 0.0)), 0.0)
    if market_value > 0:
        return market_value
    shares = _safe_float(position.get("shares", position.get("totalQty", 0)), 0.0)
    price = _safe_float(position.get("current_price", position.get("price", position.get("lastPrice", 0.0))), 0.0)
    return round(shares * price, 2) if shares > 0 and price > 0 else 0.0


def _position_attr(position: dict, key: str, aliases: tuple[str, ...] = ()) -> str:
    for candidate in (key, *aliases):
        value = position.get(candidate)
        if value not in (None, ""):
            return str(value).strip()
    metadata = position.get("metadata", {}) if isinstance(position.get("metadata", {}), dict) else {}
    for candidate in (key, *aliases):
        value = metadata.get(candidate)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def _resolve_sector(position: dict, portfolio_cfg: dict) -> str:
    code = _position_code(position)
    sector = _position_attr(position, "sector", ("industry", "板块", "行业"))
    if sector:
        return sector
    sector_map = portfolio_cfg.get("sector_map", {}) if isinstance(portfolio_cfg.get("sector_map", {}), dict) else {}
    return str(sector_map.get(code, "")).strip()


def _resolve_correlation_group(position: dict, portfolio_cfg: dict) -> str:
    code = _position_code(position)
    group = _position_attr(position, "correlation_group", ("theme", "concept", "题材", "相关性分组"))
    if group:
        return group
    groups = portfolio_cfg.get("correlation_groups", {})
    if isinstance(groups, dict):
        if code in groups and isinstance(groups.get(code), str):
            return str(groups.get(code, "")).strip()
        for group_name, codes in groups.items():
            if isinstance(codes, (list, tuple, set)) and code in {str(item).strip() for item in codes}:
                return str(group_name).strip()
    return ""


def _event_risks_for_today(portfolio_cfg: dict, today: date) -> list[dict]:
    raw_events = portfolio_cfg.get("event_risk_dates", []) or portfolio_cfg.get("event_risks", [])
    if isinstance(raw_events, dict):
        raw_events = [
            {"date": key, **(value if isinstance(value, dict) else {"name": str(value)})}
            for key, value in raw_events.items()
        ]
    result = []
    for item in raw_events if isinstance(raw_events, list) else []:
        if not isinstance(item, dict):
            continue
        event_date = _parse_trade_date(item.get("date", item.get("event_date", "")))
        if event_date != today:
            continue
        result.append({
            "date": today.isoformat(),
            "name": str(item.get("name", item.get("label", "event_risk"))).strip() or "event_risk",
            "severity": str(item.get("severity", "warning")).strip() or "warning",
            "codes": list(item.get("codes", [])) if isinstance(item.get("codes", []), list) else [],
        })
    return result


# ---------------------------------------------------------------------------
# 止损止盈计算
# ---------------------------------------------------------------------------

def calc_stop_loss(cost: float, ma20: float = 0, style: str = "momentum",
                   entry_day_low: float = 0, ma60: float = 0) -> dict:
    """
    计算动态止损价（V1.0 双轨策略）

    慢牛：-8% 或跌破 MA60
    题材：-5% 或跌破买入日最低价

    Args:
        cost: 持仓成本
        ma20: 20日均线
        style: "slow_bull" | "momentum"
        entry_day_low: 买入日最低价（题材股用）
        ma60: 60日均线（慢牛股用）

    Returns:
        {
            "stop_loss": float,
            "absolute_stop": float,
            "method": str,
            "style": str,
        }
    """
    from scripts.engine.stock_classifier import get_risk_params, STYLE_SLOW_BULL
    params = get_risk_params(style)
    stop_loss_pct = params["stop_loss"]

    cost_stop = round(cost * (1 - stop_loss_pct), 2)

    if style == STYLE_SLOW_BULL:
        # 慢牛：成本 -8% 或 MA60，取更宽松的（更低的）
        absolute_stop = round(cost * (1 - stop_loss_pct), 2)
        if ma60 and ma60 > 0:
            # MA60 作为绝对止损线
            absolute_stop = min(absolute_stop, round(ma60 * 0.98, 2))
        method = "slow_bull_ma60" if ma60 else "slow_bull_cost"
        return {
            "stop_loss": cost_stop,
            "absolute_stop": absolute_stop,
            "method": method,
            "style": style,
        }
    else:
        # 题材：成本 -5% 或买入日最低价，取更严格的（更高的）
        stop_loss = cost_stop
        if entry_day_low and entry_day_low > 0:
            stop_loss = max(stop_loss, round(entry_day_low * 0.99, 2))
        absolute_stop = round(cost * (1 - stop_loss_pct - 0.02), 2)  # 额外 2% 绝对线
        method = "momentum_entry_low" if entry_day_low else "momentum_cost"
        return {
            "stop_loss": stop_loss,
            "absolute_stop": absolute_stop,
            "method": method,
            "style": style,
        }


def calc_take_profit(cost: float, first_buy_price: float = 0,
                     style: str = "momentum", highest_price: float = 0) -> dict:
    """
    计算止盈规则（V1.0 双轨策略）

    慢牛：不主动止盈，跌破 MA20 离场
    题材：移动止盈（最高点回撤 -8%）或 MA5 死叉 MA10

    Args:
        cost: 平均成本
        first_buy_price: 首次买入价
        style: "slow_bull" | "momentum"
        highest_price: 持仓期间最高价（题材股移动止盈用）

    Returns:
        {
            "mode": str,
            "trailing_stop_price": Optional[float],
            "trailing_stop_pct": Optional[float],
            "exit_ma": int,
        }
    """
    from scripts.engine.stock_classifier import get_risk_params, STYLE_SLOW_BULL
    params = get_risk_params(style)

    if style == STYLE_SLOW_BULL:
        return {
            "mode": "ma_exit_only",
            "trailing_stop_price": None,
            "trailing_stop_pct": None,
            "exit_ma": params.get("exit_ma", 20),
            "style": style,
        }
    else:
        trailing_pct = params.get("trailing_stop", 0.08)
        trailing_price = None
        if highest_price and highest_price > 0:
            trailing_price = round(highest_price * (1 - trailing_pct), 2)
        return {
            "mode": "trailing_stop",
            "trailing_stop_price": trailing_price,
            "trailing_stop_pct": trailing_pct,
            "exit_ma": params.get("exit_ma", 20),
            "trailing_ma_cross": params.get("trailing_ma_cross", {"fast": 5, "slow": 10}),
            "style": style,
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
    max_sector_warn_pct = _safe_float(portfolio_cfg.get("max_sector_exposure_warn_pct", 0.40), 0.40)
    max_corr_warn_pct = _safe_float(portfolio_cfg.get("max_correlation_group_exposure_warn_pct", 0.50), 0.50)

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
    sector_exposure: dict[str, float] = {}
    correlation_group_exposure: dict[str, float] = {}
    for position in positions or []:
        market_value = _position_market_value(position)
        if total_capital <= 0 or market_value <= 0:
            continue
        pct = market_value / total_capital
        if pct > largest_position_pct:
            largest_position_pct = pct
            largest_position_code = str(position.get("code", "")).strip()
        sector = _resolve_sector(position, portfolio_cfg)
        if sector:
            sector_exposure[sector] = round(sector_exposure.get(sector, 0.0) + market_value, 2)
        corr_group = _resolve_correlation_group(position, portfolio_cfg)
        if corr_group:
            correlation_group_exposure[corr_group] = round(correlation_group_exposure.get(corr_group, 0.0) + market_value, 2)

    largest_sector = ""
    largest_sector_pct = 0.0
    for sector, market_value in sector_exposure.items():
        pct = market_value / total_capital if total_capital else 0.0
        if pct > largest_sector_pct:
            largest_sector = sector
            largest_sector_pct = pct

    largest_correlation_group = ""
    largest_correlation_group_pct = 0.0
    for group, market_value in correlation_group_exposure.items():
        pct = market_value / total_capital if total_capital else 0.0
        if pct > largest_correlation_group_pct:
            largest_correlation_group = group
            largest_correlation_group_pct = pct

    event_risks = _event_risks_for_today(portfolio_cfg, today)

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
    if largest_sector_pct >= max_sector_warn_pct > 0:
        reason_codes.append("TRADE_SECTOR_CONCENTRATION_WARNING")
        reasons.append(
            f"板块集中度预警 ({largest_sector or 'unknown'} 占总资金 {largest_sector_pct:.1%})"
        )
    if largest_correlation_group_pct >= max_corr_warn_pct > 0:
        reason_codes.append("TRADE_CORRELATION_CONCENTRATION_WARNING")
        reasons.append(
            f"相关性集中度预警 ({largest_correlation_group or 'unknown'} 占总资金 {largest_correlation_group_pct:.1%})"
        )
    if event_risks:
        reason_codes.append("TRADE_EVENT_RISK_DAY_WARNING")
        reasons.append(
            "事件风险日: " + "、".join(item["name"] for item in event_risks)
        )

    state = "ok"
    if any(code in {"TRADE_PORTFOLIO_DAILY_LOSS_LIMIT", "TRADE_CONSECUTIVE_LOSS_COOLDOWN"} for code in reason_codes):
        state = "block"
    elif any(code.endswith("_WARNING") for code in reason_codes):
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
            "sector_exposure": {key: round(value, 2) for key, value in sector_exposure.items()},
            "largest_sector": largest_sector,
            "largest_sector_pct": round(largest_sector_pct, 4),
            "max_sector_exposure_warn_pct": round(max_sector_warn_pct, 4),
            "correlation_group_exposure": {key: round(value, 2) for key, value in correlation_group_exposure.items()},
            "largest_correlation_group": largest_correlation_group,
            "largest_correlation_group_pct": round(largest_correlation_group_pct, 4),
            "max_correlation_group_exposure_warn_pct": round(max_corr_warn_pct, 4),
            "event_risks": event_risks,
        },
    }


def should_exit(position: dict, current_price: float,
                ma20: float = 0, ma60: float = 0,
                highest_price: float = 0,
                ma5: float = 0, ma10: float = 0) -> Tuple[bool, str]:
    """
    判断是否应该止损/卖出（V1.0 双轨策略）

    Args:
        position: dict with keys cost, shares, style, entry_day_low, ...
        current_price: 最新价格
        ma20: 当前 MA20
        ma60: 当前 MA60
        highest_price: 持仓期间最高价
        ma5: 当前 MA5
        ma10: 当前 MA10

    Returns:
        (should_exit: bool, reason: str)
    """
    cost = _safe_float(position.get("平均成本", position.get("cost", 0)))
    if cost <= 0:
        return False, ""

    style = str(position.get("style", position.get("stock_style", "momentum")))
    hold_days = int(position.get("持有天数", position.get("hold_days", 0)))
    entry_day_low = _safe_float(position.get("entry_day_low", 0))
    change_pct = (current_price - cost) / cost

    from scripts.engine.stock_classifier import get_risk_params, STYLE_SLOW_BULL
    params = get_risk_params(style)

    # ── 通用：收盘价跌破 MA20 ──
    exit_ma = params.get("exit_ma", 20)
    if exit_ma == 20 and ma20 and ma20 > 0 and current_price < ma20:
        return True, f"跌破MA20（{current_price} < {ma20:.2f}）"

    if style == STYLE_SLOW_BULL:
        # 慢牛止损：-8% 或跌破 MA60
        stop_pct = params["stop_loss"]
        if change_pct <= -stop_pct:
            return True, f"慢牛止损（{change_pct:.1%} <= -{stop_pct:.0%}）"
        if ma60 and ma60 > 0 and current_price < ma60:
            return True, f"慢牛跌破MA60（{current_price} < {ma60:.2f}）"
        # 时间止损：30天不涨
        time_stop = params.get("time_stop_days", 30)
        if hold_days >= time_stop > 0 and change_pct < 0.02:
            return True, f"慢牛时间止损（{hold_days}日，涨幅{change_pct:.1%}）"
    else:
        # 题材止损：-5% 或跌破买入日最低价
        stop_pct = params["stop_loss"]
        if change_pct <= -stop_pct:
            return True, f"题材止损（{change_pct:.1%} <= -{stop_pct:.0%}）"
        if entry_day_low and entry_day_low > 0 and current_price < entry_day_low:
            return True, f"题材跌破买入日低点（{current_price} < {entry_day_low:.2f}）"
        # 移动止盈：最高点回撤 -8%
        trailing_pct = params.get("trailing_stop", 0.08)
        if highest_price and highest_price > cost and current_price < highest_price * (1 - trailing_pct):
            return True, f"题材移动止盈（最高{highest_price:.2f}回撤{trailing_pct:.0%}）"
        # MA5 死叉 MA10
        if ma5 and ma10 and ma5 < ma10 and highest_price and highest_price > cost * 1.03:
            return True, f"题材MA5死叉MA10（MA5={ma5:.2f} < MA10={ma10:.2f}）"
        # 时间止损：10天不创新高
        time_stop = params.get("time_stop_days", 10)
        if hold_days >= time_stop > 0 and change_pct < 0.02:
            return True, f"题材时间止损（{hold_days}日，涨幅{change_pct:.1%}）"

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
